"""Unified encumbrance extraction service — dispatch table + cache + orchestration.

This module replaces the single-purpose ``PgMortgageExtractionService`` with a
unified service that can extract structured data from ANY ORI document type
(mortgages, liens, lis pendens, satisfactions, assignments, NOCs, deeds, etc.).

Architecture
------------
The core idea is a **dispatch table** (``EXTRACTION_DISPATCH``) that maps each
``encumbrance_type`` enum value to:

1. A prompt constant from ``vision_service`` (e.g. ``MORTGAGE_PROMPT``)
2. A Pydantic model class that validates the LLM's JSON output

The extraction flow follows the same pattern as ``FinalJudgmentProcessor``:

1. Queries ``ori_encumbrances`` for rows missing ``extracted_data``
2. For each row, checks the on-disk JSON cache (``{stem}_extracted.json``)
3. If no cache hit, downloads the PDF, renders every page to PNG (PyMuPDF)
4. **pytesseract** OCRs every page image to raw text
5. Combines OCR text with the doc-type prompt and sends to the LLM via
   ``VisionService.analyze_text()`` (text-only, NOT image-based)
6. Parses JSON from the LLM response, validates against the Pydantic model
7. Writes the JSON cache and persists to PG

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
import pytesseract
from loguru import logger
from PIL import Image
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
from src.services.vision_service import (
    ASSIGNMENT_PROMPT,
    DEED_PROMPT,
    LIS_PENDENS_PROMPT,
    LIEN_PROMPT,
    MORTGAGE_PROMPT,
    NOC_PROMPT,
    SATISFACTION_PROMPT,
    VisionService,
    robust_json_parse,
)
from sunbiz.db import get_engine, resolve_pg_dsn

if TYPE_CHECKING:
    from collections.abc import Sequence

    from playwright.async_api import Page

    from src.models.extraction_base import BaseDocumentExtraction

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PAV_BASE = "https://publicaccess.hillsclerk.com"
_RENDER_DPI = 150

# ---------------------------------------------------------------------------
# Dispatch table: encumbrance_type → (prompt constant, Pydantic model)
#
# Each entry tells the service which LLM prompt to use and which Pydantic
# model to validate the returned JSON against.  Adding a new doc type is a
# one-line addition here (plus the model + prompt in vision_service.py).
# ---------------------------------------------------------------------------

EXTRACTION_DISPATCH: dict[str, tuple[str, type[BaseDocumentExtraction]]] = {
    "mortgage": (MORTGAGE_PROMPT, MortgageExtraction),
    "lis_pendens": (LIS_PENDENS_PROMPT, LisPendensExtraction),
    "lien": (LIEN_PROMPT, LienExtraction),
    "satisfaction": (SATISFACTION_PROMPT, SatisfactionExtraction),
    "assignment": (ASSIGNMENT_PROMPT, AssignmentExtraction),
    "noc": (NOC_PROMPT, NOCExtraction),
    "easement": (DEED_PROMPT, DeedExtraction),
    "other": (DEED_PROMPT, DeedExtraction),
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
                    self._tally_result(stats, result)
                except Exception:
                    logger.exception("Error extracting id={}", row["id"])
                    stats["errors"] += 1

            await browser.close()

        logger.info("Extraction complete: {}", stats)
        return stats

    @staticmethod
    def _tally_result(stats: dict[str, int], result: dict[str, Any] | None) -> None:
        """Map per-row outcome markers into controller-visible stats.

        The service used to return ``None`` for both benign skips and real OCR /
        LLM / validation failures, which hid operational problems inside the
        ``skipped`` counter. Keep the row result explicit so downstream metrics
        tell reviewers whether a document was actually processed, cached, or
        failed.
        """
        status = (result or {}).get("_status", "skipped")
        if status == "extracted":
            stats["extracted"] += 1
        elif status == "cached":
            stats["cached"] += 1
        elif status == "error":
            stats["errors"] += 1
        else:
            stats["skipped"] += 1

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
        """Render every PDF page to temp PNG files.

        Encumbrance documents routinely split critical facts across distant
        pages: page 1 may have parties / recording refs, middle pages may hold
        legal descriptions, and trailing riders often contain HOA names or other
        downstream-critical terms. Rendering only the first few pages defeats
        the whole "single combined OCR context" design.
        """
        doc = _fitz.open(str(pdf_path))
        images: list[str] = []
        try:
            for i in range(len(doc)):
                pg = doc[i]
                pix = pg.get_pixmap(dpi=_RENDER_DPI)
                with _tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                    pix.save(tmp.name)
                    images.append(tmp.name)
        finally:
            doc.close()
        return images

    # ------------------------------------------------------------------
    # OCR + text-based LLM extraction
    # ------------------------------------------------------------------

    @staticmethod
    def _ocr_images_to_text(image_paths: list[str]) -> tuple[str, list[int]]:
        """OCR all page images with pytesseract and combine into one string.

        Each page is prefixed with ``--- PAGE N ---`` for context, matching
        the same convention used by ``FinalJudgmentProcessor``.

        Returns ``(text, missing_pages)``. A missing page means OCR raised or
        returned no text for that rendered image. The caller treats that as an
        extraction failure because the "single combined LLM call with full
        document context" contract is no longer true once pages are missing.
        """
        page_texts: list[str] = []
        missing_pages: list[int] = []
        for idx, image_path in enumerate(image_paths, start=1):
            try:
                with Image.open(image_path) as image:
                    page_text = pytesseract.image_to_string(image).strip()
            except Exception as exc:
                logger.warning(
                    "Tesseract OCR failed for page {} ({}): {}",
                    idx,
                    image_path,
                    exc,
                )
                missing_pages.append(idx)
                continue
            if not page_text:
                logger.warning(
                    "Tesseract OCR returned no text for page {} ({})",
                    idx,
                    image_path,
                )
                missing_pages.append(idx)
                continue
            page_texts.append(f"--- PAGE {idx} ---\n{page_text}")

        return "\n\n".join(page_texts), missing_pages

    def _extract_from_ocr_text(
        self, ocr_text: str, enc_type: str
    ) -> dict[str, Any] | None:
        """Send combined OCR text to the LLM with the doc-type prompt.

        All pages are sent in a single LLM call so the model has full
        document context (e.g. parties on page 1, amounts on page 3).
        """
        if not ocr_text.strip():
            return None

        prompt_template, model_cls = EXTRACTION_DISPATCH[enc_type]

        # Build the full prompt: doc-type prompt + OCR text. The prompt gives
        # domain guidance; the response_format below is the actual schema
        # contract. Both are necessary because prompt drift alone is not a safe
        # structured-output strategy.
        full_prompt = (
            f"{prompt_template}\n\n"
            f"## DOCUMENT TEXT (OCR)\n\n{ocr_text}\n\n"
            "Return a JSON object matching the schema above. "
            "Use null for any field you cannot determine from the text."
        )

        raw_response = self.vision.analyze_text(
            full_prompt,
            max_tokens=4000,
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": f"{enc_type}_extraction",
                    "schema": model_cls.model_json_schema(),
                },
            },
        )
        if not raw_response:
            logger.warning("LLM returned empty response for {} extraction", enc_type)
            return None

        parsed = robust_json_parse(raw_response, f"{enc_type}_extraction")
        if parsed is None:
            logger.warning("Failed to parse JSON from {} extraction response", enc_type)
            return None

        parsed["raw_text"] = ocr_text
        return parsed

    @staticmethod
    def _validation_messages(exc: ValidationError) -> list[str]:
        """Flatten Pydantic validation errors into readable log strings."""
        messages: list[str] = []
        for err in exc.errors():
            msg = str(err.get("msg") or "validation error")
            if msg.startswith("Value error, "):
                msg = msg.removeprefix("Value error, ")
            messages.append(msg)
        return messages

    @staticmethod
    def _validate(
        data: dict[str, Any],
        enc_type: str,
        *,
        row_context: dict[str, Any] | None = None,
        source: str = "extraction",
    ) -> tuple[dict[str, Any] | None, list[str]]:
        """Validate extraction against the Pydantic model.

        Returns cleaned dict on success. Validation failures are treated as real
        extraction failures instead of being silently persisted as partial data,
        because downstream survival / title workflows assume ``extracted_data``
        is structurally trustworthy.
        """
        _, model_cls = EXTRACTION_DISPATCH[enc_type]
        try:
            validated = model_cls.model_validate(data)
            return validated.model_dump(mode="json"), []
        except ValidationError as exc:
            messages = PgEncumbranceExtractionService._validation_messages(exc)
            preview = "; ".join(messages[:5]) if messages else "unknown validation failure"
            if len(messages) > 5:
                preview = f"{preview}; ... ({len(messages) - 5} more)"
            logger.warning(
                "Validation failed for {} {} id={} type={} inst={} with {} issue(s): {}",
                source,
                enc_type,
                (row_context or {}).get("id"),
                enc_type,
                (row_context or {}).get("instrument_number"),
                exc.error_count(),
                preview,
            )
            return None, messages

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
            validated_cache, cache_errors = self._validate(
                cached,
                enc_type,
                row_context=row,
                source="cache",
            )
            if validated_cache is None:
                logger.warning(
                    "Ignoring invalid cache for id={} type={} inst={} path={} and re-extracting after validation errors: {}",
                    row["id"],
                    enc_type,
                    row.get("instrument_number"),
                    pdf_path,
                    "; ".join(cache_errors[:3]) if cache_errors else "unknown validation failure",
                )
            else:
                self._save_to_pg(row["id"], validated_cache)
                logger.debug("Loaded validated cache: id={}", row["id"])
                return {**validated_cache, "_status": "cached"}

        # 2. Download
        downloaded = await self._download_pdf(page, row)
        if not downloaded:
            return {
                "_status": "error",
                "_reason": "download_failed",
            }

        # 3. Render
        images = self._render_pages(downloaded)
        if not images:
            logger.warning("No pages rendered from {}", downloaded)
            return {
                "_status": "error",
                "_reason": "render_failed",
            }

        try:
            # 4. OCR all pages → combined text → single LLM call
            ocr_text, missing_pages = self._ocr_images_to_text(images)
            if missing_pages:
                logger.warning(
                    "OCR missed {}/{} page(s) for id={} type={} inst={}: pages={}",
                    len(missing_pages),
                    len(images),
                    row["id"],
                    enc_type,
                    row.get("instrument_number"),
                    missing_pages,
                )
                return {
                    "_status": "error",
                    "_reason": "ocr_incomplete",
                }
            if not ocr_text.strip():
                logger.warning(
                    "OCR produced no text for id={} type={} inst={}",
                    row["id"],
                    enc_type,
                    row.get("instrument_number"),
                )
                return {
                    "_status": "error",
                    "_reason": "ocr_empty",
                }

            raw = self._extract_from_ocr_text(ocr_text, enc_type)
            if not raw:
                logger.warning(
                    "LLM returned no usable structured extraction for id={} type={} inst={}",
                    row["id"],
                    enc_type,
                    row.get("instrument_number"),
                )
                return {
                    "_status": "error",
                    "_reason": "llm_no_structured_output",
                }

            # 5. Validate
            validated, validation_errors = self._validate(
                raw,
                enc_type,
                row_context=row,
                source="fresh extraction",
            )
            if not validated:
                logger.warning(
                    "Skipping persistence for invalid extraction id={} type={} inst={} because validation failed: {}",
                    row["id"],
                    enc_type,
                    row.get("instrument_number"),
                    "; ".join(validation_errors[:3]) if validation_errors else "unknown validation failure",
                )
                return {
                    "_status": "error",
                    "_reason": "validation_failed",
                }

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
            return {**validated, "_status": "extracted"}

        finally:
            for img in images:
                Path(img).unlink(missing_ok=True)
