"""Unified encumbrance extraction service — dispatch table + cache + orchestration.

This module replaces the single-purpose ``PgMortgageExtractionService`` with a
unified service that can extract structured data from ANY ORI document type
(mortgages, liens, lis pendens, satisfactions, assignments, NOCs, deeds, etc.).

Architecture
------------
The core idea is a **dispatch table** (``EXTRACTION_DISPATCH``) that maps each
``encumbrance_type`` enum value to:

1. A VisionService method name (e.g. ``"extract_mortgage"``)
2. A Pydantic model class that validates the LLM's JSON output

This eliminates the need for per-type service classes.  The ``run()`` method:

1. Queries ``ori_encumbrances`` for rows missing ``extracted_data``
2. For each row, checks the on-disk JSON cache (``{stem}_extracted.json``)
3. If no cache hit, downloads the PDF, renders pages to images, and calls
   the appropriate VisionService method via the dispatch table
4. Validates the result against the Pydantic model
5. Writes the JSON cache and persists to PG

Cache helpers (``_cache_path_for``, ``_load_cache``, ``_write_cache``) follow
the same ``{stem}_extracted.json`` convention used by ``FinalJudgmentProcessor``
and ``PgMortgageExtractionService``, so existing cached extractions are reused.

Downstream consumers:
    - ``pg_survival_service`` reads ``extracted_data`` for lien priority analysis
    - ``pg_title_chain_service`` uses party names + recording dates for chain building
    - ``properties.py`` + Jinja templates render extracted data in the web dashboard
"""

from __future__ import annotations

import asyncio
import json
import tempfile as _tempfile
import time
import urllib.parse
from pathlib import Path
from typing import TYPE_CHECKING, Any

import fitz as _fitz
from loguru import logger
from pydantic import ValidationError
from sqlalchemy import text

from src.models.assignment_extraction import AssignmentExtraction
from src.models.deed_extraction import DeedExtraction
from src.models.lien_extraction import LienExtraction
from src.models.lis_pendens_extraction import LisPendensExtraction
from src.models.mortgage_extraction import MortgageExtraction
from src.models.noc_extraction import NOCExtraction
from src.models.satisfaction_extraction import SatisfactionExtraction
from src.services.scraper_storage import ScraperStorage
from src.services.vision_service import VisionService
from sunbiz.db import get_engine, resolve_pg_dsn

if TYPE_CHECKING:
    from collections.abc import Sequence

    from playwright.async_api import Page

    from src.models.extraction_base import BaseDocumentExtraction

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PAV_BASE = "https://publicaccess.hillsclerk.com"
_MAX_PAGES = 3
_RENDER_DPI = 150

# ---------------------------------------------------------------------------
# Dispatch table: encumbrance_type → (VisionService method name, Pydantic model)
#
# Each entry tells the service which VisionService method to call and which
# Pydantic model to validate the returned JSON against.  Adding a new doc
# type is a one-line addition here (plus the model + vision prompt).
# ---------------------------------------------------------------------------

EXTRACTION_DISPATCH: dict[str, tuple[str, type[BaseDocumentExtraction]]] = {
    "mortgage": ("extract_mortgage", MortgageExtraction),
    "lis_pendens": ("extract_lis_pendens", LisPendensExtraction),
    "lien": ("extract_lien", LienExtraction),
    "satisfaction": ("extract_satisfaction", SatisfactionExtraction),
    "assignment": ("extract_assignment", AssignmentExtraction),
    "noc": ("extract_noc", NOCExtraction),
    "easement": ("extract_deed", DeedExtraction),
    "other": ("extract_deed", DeedExtraction),
}


# ---------------------------------------------------------------------------
# Cache helpers
#
# Follow the same {stem}_extracted.json convention used by
# FinalJudgmentProcessor and PgMortgageExtractionService so that
# existing cached extractions are automatically picked up.
# ---------------------------------------------------------------------------


def _cache_path_for(pdf_path: Path) -> Path:
    """Return the JSON cache path for a given PDF path."""
    return pdf_path.with_name(f"{pdf_path.stem}_extracted.json")


def _load_cache(pdf_path: Path) -> dict[str, Any] | None:
    """Load cached extraction JSON for a PDF, or None if absent/corrupt."""
    cache = _cache_path_for(pdf_path)
    if not cache.exists():
        return None
    try:
        data = json.loads(cache.read_text(encoding="utf-8"))
        if isinstance(data, dict) and data:
            return data
        return None
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning(f"Bad cache file {cache}, will re-extract: {exc}")
        return None


def _write_cache(pdf_path: Path, data: dict[str, Any]) -> None:
    """Write extraction JSON to the cache file next to the PDF."""
    cache = _cache_path_for(pdf_path)
    try:
        cache.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
        logger.debug(f"Cached extraction to {cache}")
    except OSError as exc:
        logger.warning(f"Failed to write cache {cache}: {exc}")


# ---------------------------------------------------------------------------
# Service class
# ---------------------------------------------------------------------------


class PgEncumbranceExtractionService:
    """Unified extraction service for all ORI encumbrance document types.

    Replaces the single-purpose PgMortgageExtractionService with a dispatch-
    table-driven approach that handles mortgages, liens, lis pendens,
    satisfactions, assignments, NOCs, deeds, and any future doc type.
    """

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = resolve_pg_dsn(dsn)
        self.engine = get_engine(self.dsn)
        self.storage = ScraperStorage()
        self.vision = VisionService()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        *,
        limit: int | None = None,
        straps: Sequence[str] | None = None,
        enc_types: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """Find unprocessed encumbrances, extract via Vision, push to PG.

        Parameters
        ----------
        limit:
            Maximum number of encumbrances to process.
        straps:
            If provided, restrict to these property straps.
        enc_types:
            If provided, restrict to these encumbrance types (must be keys
            in ``EXTRACTION_DISPATCH``).

        Returns
        -------
        dict with extraction statistics.
        """
        started = time.monotonic()
        stats = asyncio.run(self._run_async(limit=limit, straps=straps, enc_types=enc_types))
        elapsed = round(time.monotonic() - started, 2)
        stats["elapsed_seconds"] = elapsed
        logger.info(
            "Encumbrance extraction complete: "
            f"extracted={stats['extracted']}, cached={stats['cached']}, "
            f"errors={stats['errors']}, skipped={stats['skipped']} "
            f"in {elapsed}s"
        )
        return stats

    # ------------------------------------------------------------------
    # Internal async orchestration
    # ------------------------------------------------------------------

    async def _run_async(
        self,
        *,
        limit: int | None = None,
        straps: Sequence[str] | None = None,
        enc_types: Sequence[str] | None = None,
    ) -> dict[str, int]:
        """Async entry point — find rows, launch browser, extract, close."""
        rows = self._find_unextracted(limit=limit, straps=straps, enc_types=enc_types)
        if not rows:
            logger.info("No unextracted encumbrances found.")
            return {"extracted": 0, "cached": 0, "errors": 0, "skipped": 0}

        logger.info(f"Found {len(rows)} encumbrances needing extraction")
        stats: dict[str, int] = {"extracted": 0, "cached": 0, "errors": 0, "skipped": 0}

        from playwright.async_api import async_playwright

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(accept_downloads=True)
            page = await context.new_page()
            # Pre-navigate to establish cookies/session
            await page.goto(
                f"{_PAV_BASE}/oripublicaccess/",
                wait_until="domcontentloaded",
            )

            for row in rows:
                enc_type = row["encumbrance_type"]
                if enc_type not in EXTRACTION_DISPATCH:
                    logger.debug(
                        "No dispatch for type={}, skipping id={}",
                        enc_type,
                        row["id"],
                    )
                    stats["skipped"] += 1
                    continue
                try:
                    result = await self._process_one(page, row)
                    if result:
                        key = "extracted" if result.get("_from_vision") else "cached"
                        stats[key] += 1
                    else:
                        stats["skipped"] += 1
                except Exception:
                    logger.exception("Error extracting id={}", row["id"])
                    stats["errors"] += 1

            await browser.close()

        logger.info("Extraction complete: {}", stats)
        return stats

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def _find_unextracted(
        self,
        *,
        limit: int | None = None,
        straps: Sequence[str] | None = None,
        enc_types: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Find encumbrances with no extracted_data and a downloadable ori_id."""
        sql = """
            SELECT id, strap, folio, ori_id, ori_uuid, instrument_number,
                   encumbrance_type, raw_document_type, case_number
            FROM ori_encumbrances
            WHERE extracted_data IS NULL
              AND ori_id IS NOT NULL
              AND encumbrance_type != 'release'
        """
        params: dict[str, Any] = {}
        if straps:
            sql += " AND strap = ANY(:straps)"
            params["straps"] = list(straps)
        if enc_types:
            sql += " AND encumbrance_type = ANY(:enc_types)"
            params["enc_types"] = list(enc_types)
        sql += " ORDER BY recording_date ASC NULLS LAST"
        if limit:
            sql += " LIMIT :lim"
            params["lim"] = limit

        with self.engine.connect() as conn:
            rows = conn.execute(text(sql), params).mappings().all()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # PDF path + download
    # ------------------------------------------------------------------

    @staticmethod
    def _pdf_path_for(row: dict[str, Any]) -> Path:
        """Build local PDF path from encumbrance metadata."""
        case = row.get("case_number") or "unknown"
        inst = row.get("instrument_number") or str(row["id"])
        doc_type = row.get("raw_document_type") or row.get("encumbrance_type") or "doc"
        filename = f"{doc_type.lower()}_{inst}.pdf"
        return Path(f"data/Foreclosure/{case}/documents/{filename}")

    async def _download_pdf(self, page: Page, row: dict[str, Any]) -> Path | None:
        """Download document PDF from PAV API.  Returns local path or None."""
        pdf_path = self._pdf_path_for(row)
        if pdf_path.exists() and pdf_path.stat().st_size > 0:
            return pdf_path

        ori_id = row["ori_id"]
        encoded = urllib.parse.quote(str(ori_id))
        url = f"{_PAV_BASE}/PAVDirectSearch/api/Document/{encoded}/?OverlayMode=View"

        try:
            pdf_path.parent.mkdir(parents=True, exist_ok=True)
            dl_page = await page.context.new_page()
            try:
                async with dl_page.expect_download(timeout=60_000) as dl_info:
                    await dl_page.evaluate(f"window.location.href = '{url}'")
                download = await dl_info.value
                await download.save_as(str(pdf_path))
                logger.debug("Downloaded {} -> {}", ori_id, pdf_path)
                return pdf_path
            finally:
                await dl_page.close()
        except Exception:
            logger.exception("Download failed for ori_id={}", ori_id)
            return None

    # ------------------------------------------------------------------
    # PDF rendering
    # ------------------------------------------------------------------

    @staticmethod
    def _render_pages(pdf_path: Path) -> list[str]:
        """Render first N pages of PDF to temp PNG files.  Returns paths."""
        doc = _fitz.open(str(pdf_path))
        images: list[str] = []
        try:
            for i in range(min(len(doc), _MAX_PAGES)):
                pg = doc[i]
                pix = pg.get_pixmap(dpi=_RENDER_DPI)
                with _tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                    pix.save(tmp.name)
                    images.append(tmp.name)
        finally:
            doc.close()
        return images

    # ------------------------------------------------------------------
    # Vision extraction + validation
    # ------------------------------------------------------------------

    async def _extract_from_images(
        self, images: list[str], enc_type: str
    ) -> dict[str, Any] | None:
        """Send page images to vision and merge results across pages."""
        method_name, _model_cls = EXTRACTION_DISPATCH[enc_type]
        vision_fn = getattr(self.vision, method_name)

        result: dict[str, Any] | None = None
        for img in images:
            try:
                page_result = vision_fn(img)
                if not isinstance(page_result, dict):
                    continue
                if result is None:
                    result = page_result
                else:
                    # Fill in missing fields from subsequent pages
                    for k, v in page_result.items():
                        if v and not result.get(k):
                            result[k] = v
            except Exception:
                logger.exception("Vision call failed for {}", img)
            await asyncio.sleep(1)

        return result

    @staticmethod
    def _validate(
        data: dict[str, Any], enc_type: str
    ) -> dict[str, Any] | None:
        """Validate extraction against the Pydantic model.

        Returns cleaned dict on success, or the raw dict if validation fails
        (we still persist partial data so downstream can use whatever was extracted).
        """
        _, model_cls = EXTRACTION_DISPATCH[enc_type]
        try:
            validated = model_cls.model_validate(data)
            return validated.model_dump(mode="json")
        except ValidationError as exc:
            logger.warning("Validation failed for {}: {}", enc_type, exc.error_count())
            return data  # store raw extraction even if validation fails

    # ------------------------------------------------------------------
    # Database persistence
    # ------------------------------------------------------------------

    def _save_to_pg(self, encumbrance_id: int, data: dict[str, Any]) -> None:
        """UPDATE ori_encumbrances SET extracted_data for this row."""
        sql = text("""
            UPDATE ori_encumbrances
            SET extracted_data = CAST(:jdata AS JSONB),
                updated_at = NOW()
            WHERE id = :id
        """)
        with self.engine.begin() as conn:
            conn.execute(sql, {"jdata": json.dumps(data, default=str), "id": encumbrance_id})

    # ------------------------------------------------------------------
    # Single-row orchestration
    # ------------------------------------------------------------------

    async def _process_one(
        self, page: Page, row: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Process a single encumbrance: cache check -> download -> extract -> save."""
        enc_type = row["encumbrance_type"]
        pdf_path = self._pdf_path_for(row)

        # 1. Check cache
        cached = _load_cache(pdf_path)
        if cached:
            self._save_to_pg(row["id"], cached)
            logger.debug("Loaded from cache: id={}", row["id"])
            return {**cached, "_from_vision": False}

        # 2. Download
        downloaded = await self._download_pdf(page, row)
        if not downloaded:
            return None

        # 3. Render
        images = self._render_pages(downloaded)
        if not images:
            logger.warning("No pages rendered from {}", downloaded)
            return None

        try:
            # 4. Extract
            raw = await self._extract_from_images(images, enc_type)
            if not raw:
                return None

            # 5. Validate
            validated = self._validate(raw, enc_type)
            if not validated:
                return None

            # 6. Cache
            _write_cache(downloaded, validated)

            # 7. Save to DB
            self._save_to_pg(row["id"], validated)
            logger.info(
                "Extracted id={} type={} inst={}",
                row["id"],
                enc_type,
                row.get("instrument_number"),
            )
            return {**validated, "_from_vision": True}

        finally:
            for img in images:
                Path(img).unlink(missing_ok=True)
