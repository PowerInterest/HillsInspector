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
import time
from typing import TYPE_CHECKING, Any

from loguru import logger

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
    from pathlib import Path

    from src.models.extraction_base import BaseDocumentExtraction

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
        """Async entry point for extraction orchestration.

        Currently a stub — full implementation in Task 4.
        """
        rows = self._find_unextracted(limit=limit, straps=straps, enc_types=enc_types)
        if not rows:
            logger.info("No unextracted encumbrances found.")
            return {"extracted": 0, "cached": 0, "errors": 0, "skipped": 0}

        logger.info(f"Found {len(rows)} encumbrances needing extraction")

        # TODO(Task 4): iterate rows, download PDFs, run vision, validate, save
        return {"extracted": 0, "cached": 0, "errors": 0, "skipped": 0}

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
        """Find encumbrances that need extraction.

        Currently a stub returning an empty list — full implementation
        in Task 4.
        """
        # TODO(Task 4): query ori_encumbrances WHERE extracted_data IS NULL
        return []
