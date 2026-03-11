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
from copy import deepcopy
from datetime import date, datetime
import json
import re
import tempfile as _tempfile
import time
import urllib.parse
from pathlib import Path
from typing import TYPE_CHECKING, Any, get_args, get_origin

import fitz as _fitz
import pytesseract
from loguru import logger
from PIL import Image
from pydantic import ValidationError
from pydantic.fields import PydanticUndefined
from sqlalchemy import text

from src.models.assignment_extraction import AssignmentExtraction
from src.models.deed_extraction import DeedExtraction
from src.models.extraction_base import StrictExtractionModel
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

_HILLSBOROUGH_ZIPS = (
    "33503, 33510, 33511, 33527, 33534, 33544, 33547, 33548, 33549, "
    "33556, 33558, 33559, 33563, 33565, 33566, 33567, 33569, 33570, "
    "33572, 33573, 33575, 33578, 33579, 33584, 33592, 33594, 33596, "
    "33598, 33601, 33602, 33603, 33604, 33605, 33606, 33607, 33609, "
    "33610, 33611, 33612, 33613, 33614, 33615, 33616, 33617, 33618, "
    "33619, 33624, 33625, 33626, 33629, 33634, 33635, 33636, 33637, 33647"
)

_REPAIR_PROMPT_TEMPLATE = """You previously extracted data from this Hillsborough County document, but the \
property address you returned does not match any known parcel in the county.

YOUR PREVIOUS EXTRACTION:
{previous_json}

THE ERROR:
{error_description}

VALID HILLSBOROUGH COUNTY ZIP CODES:
{zips}

COMMON MISTAKES TO CORRECT:
- The property address is in the GRANTING CLAUSE or LEGAL DESCRIPTION section, \
NOT the "Return To" / "Prepared By" / "After Recording" header block
- If the zip code is not in the list above, you have the WRONG address \
(likely the lender's, attorney's, or servicer's office)
- The BORROWER/MORTGAGOR grants the mortgage — the return-to contact is NOT the borrower
- The RECORDING DATE is on the clerk's stamp (top-right, "RECORDED" or "INSTR #"), \
not the form print date at the bottom of the page
- For UCC/Consensual Liens: always extract parties and amounts — \
secured party = creditor, debtor = lienee

ORIGINAL OCR TEXT:
{ocr_text}

Return a corrected JSON object with the same schema. Fix the property_address \
and any other fields that were wrong. Use null only if the information truly \
does not appear anywhere in the document."""

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
    "release": (SATISFACTION_PROMPT, SatisfactionExtraction),
    "assignment": (ASSIGNMENT_PROMPT, AssignmentExtraction),
    "noc": (NOC_PROMPT, NOCExtraction),
    "easement": (DEED_PROMPT, DeedExtraction),
    "other": (DEED_PROMPT, DeedExtraction),
}


_LEGACY_OUTPUT_FORMAT_RE = re.compile(
    r"\n## OUTPUT FORMAT\b.*?(?=\n## [A-Z]|\Z)",
    flags=re.DOTALL,
)

_ROW_FIELD_FALLBACKS: dict[str, dict[str, str]] = {
    "mortgage": {
        "mortgagor": "party1",
        "mortgagee": "party2",
    },
    "assignment": {
        "assignor": "party1",
        "assignee": "party2",
    },
    "easement": {
        "grantor": "party1",
        "grantee": "party2",
    },
    "other": {
        "grantor": "party1",
        "grantee": "party2",
    },
}


def _strip_legacy_output_format(prompt: str) -> str:
    """Remove stale JSON examples that drifted away from the live schemas.

    The old prompt constants in ``vision_service`` were written before the
    strict Pydantic contracts existed. Some local OpenAI-compatible endpoints
    follow the prompt examples more strongly than ``response_format``, which
    caused them to emit keys like ``debtor``/``creditor`` instead of the live
    schema keys ``lienee``/``lienor``. We keep the domain guidance but strip the
    obsolete output examples so the schema contract becomes the single source of
    truth.
    """

    cleaned = _LEGACY_OUTPUT_FORMAT_RE.sub("\n", prompt or "")
    return cleaned.strip()


def _schema_contract_text(model_cls: type[BaseDocumentExtraction]) -> str:
    """Render the live Pydantic schema as prompt text for weak local endpoints."""

    schema = model_cls.model_json_schema()
    top_level_keys = ", ".join(schema.get("properties", {}).keys())
    schema_json = json.dumps(schema, indent=2, ensure_ascii=True, sort_keys=True)
    return (
        "## JSON CONTRACT\n"
        "Return exactly one JSON object that matches this schema.\n"
        "Use the exact field names from the schema.\n"
        "Do not invent aliases or shorthand keys from older prompts.\n"
        "Include every required key even when the value is null.\n"
        f"Top-level keys: {top_level_keys}\n\n"
        "Schema:\n"
        f"{schema_json}"
    )


def _allows_none(annotation: Any) -> bool:
    """Return True when the annotation explicitly allows ``None``."""

    return any(arg is type(None) for arg in get_args(annotation))


def _submodel_from_annotation(annotation: Any) -> type[StrictExtractionModel] | None:
    """Extract a nested Pydantic model class from a field annotation."""

    if isinstance(annotation, type) and issubclass(annotation, StrictExtractionModel):
        return annotation
    for arg in get_args(annotation):
        if isinstance(arg, type) and issubclass(arg, StrictExtractionModel):
            return arg
    return None


def _list_submodel_from_annotation(annotation: Any) -> type[StrictExtractionModel] | None:
    """Return the nested model class when the field is ``list[Model]``."""

    if get_origin(annotation) is not list:
        return None
    args = get_args(annotation)
    if not args:
        return None
    item = args[0]
    if isinstance(item, type) and issubclass(item, StrictExtractionModel):
        return item
    return None


def _default_value_for_field(field: Any) -> Any:
    """Choose the explicit-null/default value used when the LLM omits a key."""

    if field.default_factory is not None:
        return field.default_factory()
    if field.default is not PydanticUndefined:
        return deepcopy(field.default)
    if get_origin(field.annotation) is list:
        return []
    if _allows_none(field.annotation):
        return None
    return None


def _normalize_model_payload(
    model_cls: type[StrictExtractionModel],
    payload: dict[str, Any],
    *,
    path: str = "",
) -> tuple[dict[str, Any], list[str], list[str]]:
    """Normalize a parsed JSON object to the declared schema keys only.

    Some endpoints return a nearly-correct object but omit nullable keys or add
    spurious OCR spill keys. We normalize those responses into the declared
    contract, then validate the repaired object. The repair is always logged so
    reviewers can see exactly what was missing or dropped.
    """

    missing: list[str] = []
    extras: list[str] = []
    normalized: dict[str, Any] = {}

    valid_names = {
        name
        for name, field in model_cls.model_fields.items()
        if name != "raw_text" and field.exclude is not True
    }
    for key in payload:
        if key not in valid_names:
            extras.append(f"{path}{key}")

    for name, field in model_cls.model_fields.items():
        if name == "raw_text" or field.exclude is True:
            continue
        field_path = f"{path}{name}"
        value = payload.get(name, PydanticUndefined)
        if value is PydanticUndefined:
            normalized[name] = _default_value_for_field(field)
            missing.append(field_path)
            continue

        nested_model = _submodel_from_annotation(field.annotation)
        list_model = _list_submodel_from_annotation(field.annotation)
        if nested_model and isinstance(value, dict):
            child, child_missing, child_extras = _normalize_model_payload(
                nested_model,
                value,
                path=f"{field_path}.",
            )
            normalized[name] = child
            missing.extend(child_missing)
            extras.extend(child_extras)
        elif list_model and isinstance(value, list):
            normalized_list: list[Any] = []
            for idx, item in enumerate(value):
                if isinstance(item, dict):
                    child, child_missing, child_extras = _normalize_model_payload(
                        list_model,
                        item,
                        path=f"{field_path}[{idx}].",
                    )
                    normalized_list.append(child)
                    missing.extend(child_missing)
                    extras.extend(child_extras)
                else:
                    normalized_list.append(item)
            normalized[name] = normalized_list
        else:
            normalized[name] = value

    return normalized, missing, extras


def _json_scalar(value: Any) -> Any:
    """Convert DB/date values into JSON-serializable scalars for schema fill."""

    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


# ---------------------------------------------------------------------------
# Legacy key remapping
#
# Old cache files use different field names than the current Pydantic models.
# When loaded, mismatched keys are silently dropped, losing data.  This
# remapping converts old-format keys to current-schema keys so cached
# extractions survive model renames.  The function is idempotent — if keys
# are already in the new format, it's a no-op.
# ---------------------------------------------------------------------------

_CONFIDENCE_WORD_TO_SCORE: dict[str, float] = {
    "high": 0.95,
    "medium": 0.80,
    "low": 0.60,
}

_LEGACY_KEY_MAPS: dict[str, dict[str, str | None]] = {
    "mortgage": {
        "borrower": "mortgagor",
        "lender": "mortgagee",
        "book": "recording_book",
        "page": "recording_page",
        "is_mers": "is_mers_nominee",
        "confidence": "confidence_score",
        "document_type": None,   # remove — not in new schema
        "red_flags": None,       # remove
        "prior_assignments": None,  # remove
    },
    "assignment": {
        "confidence": "confidence_score",
        "red_flags": None,
    },
    "lien": {
        "creditor": "lienor",
        "debtor": "lienee",
        "amount": "lien_amount",
        "document_type": "lien_type",
        "confidence": "confidence_score",
    },
}

_ASSIGNMENT_NESTED_REMAP: dict[str, str | None] = {
    "book": "recording_book",
    "page": "recording_page",
    "original_amount": None,  # remove
}


def _remap_legacy_keys(payload: dict[str, Any], enc_type: str) -> dict[str, Any]:
    """Remap legacy cache field names to current Pydantic model keys.

    Old cache files (written before schema renames) use different field names.
    When loaded, mismatched keys are silently dropped by Pydantic's
    ``extra="forbid"`` policy, losing data.  This function translates legacy
    keys into the current schema so cached extractions survive model renames.

    The function is idempotent: if keys are already in the new format, the
    payload passes through unchanged.
    """
    key_map = _LEGACY_KEY_MAPS.get(enc_type)
    if not key_map:
        return payload

    remapped: dict[str, Any] = {}
    for key, value in payload.items():
        target = key_map.get(key, key)  # default: keep the key as-is
        if target is None:
            # Explicitly removed legacy key
            continue
        # Special handling: convert word confidence to numeric score
        if key == "confidence" and target == "confidence_score" and isinstance(value, str):
            value = _CONFIDENCE_WORD_TO_SCORE.get(value.lower(), 0.80)
        remapped[target] = value

    # Assignment: remap nested original_mortgage → parent_instrument
    if enc_type == "assignment" and "original_mortgage" in remapped:
        nested = remapped.pop("original_mortgage")
        if isinstance(nested, dict):
            child: dict[str, Any] = {}
            for k, v in nested.items():
                child_target = _ASSIGNMENT_NESTED_REMAP.get(k, k)
                if child_target is None:
                    continue
                child[child_target] = v
            # Only set if the key isn't already present (idempotent)
            if "parent_instrument" not in remapped:
                remapped["parent_instrument"] = child
        elif nested is not None and "parent_instrument" not in remapped:
            remapped["parent_instrument"] = nested

    return remapped


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


def _delete_cache(downloaded_path: Path) -> None:
    """Remove the ``_extracted.json`` cache file next to a downloaded PDF."""
    cache = _cache_path_for(downloaded_path)
    try:
        if cache.exists():
            cache.unlink()
            logger.debug("Deleted stale cache {}", cache)
    except OSError as exc:
        logger.warning("Failed to delete cache {}: {}", cache, exc)


def _normalize_case_number(raw: str) -> str | None:
    """Convert ORI clerk case number format to standard civil case format.

    ORI stores case numbers like ``292025CA006599A001HC`` which encode:
    - ``29`` = county prefix (always 29 for Hillsborough)
    - ``YYYY`` = filing year
    - ``XX`` = case type (CA, CC, DR, etc.)
    - ``NNNNNN`` = sequence number
    - ``A001HC`` = suffix

    This converts to standard format: ``YY-XX-NNNNNN`` (e.g. ``25-CA-006599``).
    Returns None if the input doesn't match the expected pattern.
    """
    if not raw or len(raw) < 14:
        return None
    # Pattern: 29YYYYXXNNNNNNA001HC
    match = re.match(r"^29(\d{4})([A-Z]{2})(\d{6})", raw)
    if not match:
        return None
    year = match.group(1)
    case_type = match.group(2)
    number = match.group(3)
    return f"{year[2:]}-{case_type}-{number}"


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

        logger.info(
            "Found {} encumbrances needing extraction (filtered to extracted_data IS NULL; persisted rows are excluded before cache/OCR/Vision)",
            len(rows),
        )
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
        """Find encumbrances that still need extraction work.

        ``extracted_data IS NULL`` is the primary database-backed dedupe gate
        for this service. Rows that already have validated structured payloads
        in PostgreSQL are excluded here, before any cache lookup, OCR, or
        VisionService call happens.
        """
        sql = """
            SELECT id, strap, folio, ori_id, ori_uuid, instrument_number,
                   encumbrance_type, raw_document_type, case_number,
                   book, page, recording_date, party1, party2
            FROM ori_encumbrances
            WHERE extracted_data IS NULL
              AND ori_id IS NOT NULL
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
        schema = model_cls.model_json_schema()
        schema_guidance = _schema_contract_text(model_cls)
        prompt_guidance = _strip_legacy_output_format(prompt_template)

        # Build the full prompt: preserve the doc-type domain guidance, but
        # replace any stale example JSON with the live schema contract. Local
        # endpoints have already shown they can follow the old examples more
        # strongly than ``response_format`` alone, so both layers must agree.
        full_prompt = (
            f"{prompt_guidance}\n\n"
            f"{schema_guidance}\n\n"
            f"## DOCUMENT TEXT (OCR)\n\n{ocr_text}\n\n"
            "Use null for any field you cannot determine from the text. "
            "Do not omit required keys. Do not return commentary or markdown."
        )

        raw_response = self.vision.analyze_text(
            full_prompt,
            max_tokens=4000,
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": f"{enc_type}_extraction",
                    "schema": schema,
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
    def _repair_extraction_payload(
        data: dict[str, Any],
        enc_type: str,
        *,
        row_context: dict[str, Any] | None = None,
        source: str = "extraction",
    ) -> tuple[dict[str, Any], list[str]]:
        """Repair weak-but-salvageable JSON shape issues before validation.

        The strict model contract is still the source of truth. This helper only
        fixes transport/decoder shape problems:

        - omitted nullable/defaultable keys -> explicit null/default values
        - undeclared keys -> dropped
        - a small set of safe ORI metadata fallbacks for base identifiers and
          two-party document roles

        This function is intentionally pure/local. It must never trigger a new
        OCR or VisionService request. Re-running the model belongs at the row
        orchestration level, not inside validation repair.
        """

        _, model_cls = EXTRACTION_DISPATCH[enc_type]
        valid_keys = {
            name
            for name, field in model_cls.model_fields.items()
            if name != "raw_text" and field.exclude is not True
        }
        overlap = len(set(data) & valid_keys)
        if overlap < 4:
            return data, []

        repaired, missing_keys, extra_keys = _normalize_model_payload(model_cls, data)
        notes: list[str] = []

        if missing_keys:
            preview = ", ".join(missing_keys[:12])
            if len(missing_keys) > 12:
                preview = f"{preview}, ... ({len(missing_keys) - 12} more)"
            notes.append(f"filled omitted schema key(s): {preview}")

        if extra_keys:
            preview = ", ".join(extra_keys[:12])
            if len(extra_keys) > 12:
                preview = f"{preview}, ... ({len(extra_keys) - 12} more)"
            notes.append(f"dropped unexpected key(s): {preview}")

        filled_from_row: list[str] = []
        row = row_context or {}
        base_fallbacks = {
            "instrument_number": row.get("instrument_number"),
            "recording_book": row.get("book"),
            "recording_page": row.get("page"),
            "recording_date": row.get("recording_date"),
        }
        parcel_fallback = row.get("strap") or row.get("folio")
        if parcel_fallback and not repaired.get("parcel_id"):
            repaired["parcel_id"] = _json_scalar(parcel_fallback)
            filled_from_row.append("parcel_id")

        for field_name, fallback in base_fallbacks.items():
            if fallback and not repaired.get(field_name):
                repaired[field_name] = _json_scalar(fallback)
                filled_from_row.append(field_name)

        for field_name, row_key in _ROW_FIELD_FALLBACKS.get(enc_type, {}).items():
            row_value = row.get(row_key)
            if row_value and not repaired.get(field_name):
                repaired[field_name] = _json_scalar(row_value)
                filled_from_row.append(field_name)

        if filled_from_row:
            notes.append(
                "filled from ORI row metadata: " + ", ".join(sorted(set(filled_from_row)))
            )

        if notes:
            logger.warning(
                "Repaired {} {} id={} type={} inst={} before validation: {}",
                source,
                enc_type,
                row.get("id"),
                enc_type,
                row.get("instrument_number"),
                " | ".join(notes),
            )

        return repaired, notes

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
        data, _ = PgEncumbranceExtractionService._repair_extraction_payload(
            data,
            enc_type,
            row_context=row_context,
            source=source,
        )
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

    def _address_resolves(self, address: str | None) -> bool:
        """Check if extracted address matches any HCPA parcel."""
        if not address or len(address.strip()) < 5:
            return False
        normalized = address.upper().strip().split(",")[0].strip()
        if not normalized:
            return False
        sql = text("""
            SELECT 1 FROM hcpa_bulk_parcels
            WHERE property_address = :addr
            LIMIT 1
        """)
        with self.engine.connect() as conn:
            return conn.execute(sql, {"addr": normalized}).fetchone() is not None

    def _build_repair_error_description(self, address: str | None) -> str:
        """Build a human-readable error for the repair prompt."""
        if not address:
            return (
                "No property address was extracted. Look for it in the granting "
                "clause, legal description, or sale paragraph."
            )
        # Check if zip is outside Hillsborough
        zip_match = re.search(r"\b(\d{5})\b", address)
        if zip_match:
            zip_code = zip_match.group(1)
            if zip_code not in _HILLSBOROUGH_ZIPS:
                return (
                    f"You extracted address '{address}' with zip code {zip_code}, "
                    f"which is not in Hillsborough County. This is likely the "
                    f"lender's or attorney's office address."
                )
        return (
            f"Address '{address}' does not match any known parcel in "
            f"Hillsborough County. Check for OCR errors in the street name or number."
        )

    def _save_raw_to_pg(self, encumbrance_id: int, ocr_text: str) -> None:
        """Persist raw OCR text before LLM extraction."""
        sql = text("""
            UPDATE ori_encumbrances
            SET raw = :ocr_text,
                updated_at = NOW()
            WHERE id = :id
        """)
        with self.engine.begin() as conn:
            conn.execute(sql, {"ocr_text": ocr_text, "id": encumbrance_id})

    # ------------------------------------------------------------------
    # Repair pass
    # ------------------------------------------------------------------

    def _attempt_repair(
        self,
        ocr_text: str,
        validated: dict[str, Any],
        enc_type: str,
    ) -> dict[str, Any] | None:
        """Fire a repair prompt when the extracted address doesn't resolve."""
        address = validated.get("property_address")
        error_desc = self._build_repair_error_description(address)

        prompt = _REPAIR_PROMPT_TEMPLATE.format(
            previous_json=json.dumps(validated, indent=2, default=str),
            error_description=error_desc,
            zips=_HILLSBOROUGH_ZIPS,
            ocr_text=ocr_text,
        )

        _, model_cls = EXTRACTION_DISPATCH[enc_type]
        schema_guidance = _schema_contract_text(model_cls)
        full_prompt = f"{prompt}\n\n{schema_guidance}"

        raw = self.vision.analyze_text(full_prompt, max_tokens=4000)
        if not raw:
            return None

        parsed = robust_json_parse(raw, f"{enc_type}_repair")
        if not parsed:
            return None

        repaired, _ = self._validate(parsed, enc_type, row_context={}, source="repair")
        return repaired

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
                logger.debug(
                    "Loaded validated cache for id={} type={} inst={}; skipping download/OCR/Vision",
                    row["id"],
                    enc_type,
                    row.get("instrument_number"),
                )
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

            # Persist raw OCR text before LLM call
            self._save_raw_to_pg(row["id"], ocr_text)

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

            # 6. Repair if address doesn't resolve
            address = validated.get("property_address")
            if not self._address_resolves(address):
                logger.info(
                    "Address '{}' for id={} type={} inst={} does not resolve; attempting repair",
                    address,
                    row["id"],
                    enc_type,
                    row.get("instrument_number"),
                )
                repaired = self._attempt_repair(ocr_text, validated, enc_type)
                if repaired and self._address_resolves(repaired.get("property_address")):
                    logger.info(
                        "Repair succeeded for id={}: '{}' -> '{}'",
                        row["id"],
                        address,
                        repaired.get("property_address"),
                    )
                    validated = repaired
                else:
                    logger.info(
                        "Repair did not improve address for id={}; keeping original",
                        row["id"],
                    )

            # 7. Cache
            _write_cache(downloaded, validated)

            # 8. Save to DB
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
