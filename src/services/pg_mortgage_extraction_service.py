"""Phase B Step 5: Extract mortgage data from PDFs → PG ori_encumbrances.mortgage_data.

Finds encumbrances that are mortgages but have no extracted JSON, downloads the PDF
from the Clerk's PAVDirectSearch API using the instrument number, and runs the
Vision Service to extract loan amounts, interest rates, and loan seniority.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
import urllib.parse
from pathlib import Path
from typing import TYPE_CHECKING, Any


from loguru import logger
from playwright.async_api import async_playwright, Page
from playwright_stealth import Stealth
from sqlalchemy import text

from src.services.scraper_storage import ScraperStorage
from src.services.vision_service import VisionService, MORTGAGE_PROMPT
from src.utils.amount_validator import validate_mortgage_amount
from sunbiz.db import get_engine, resolve_pg_dsn

if TYPE_CHECKING:
    from collections.abc import Sequence


USER_AGENT_DESKTOP = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
)


async def apply_stealth(page: Page) -> None:
    """Apply stealth settings to a page to avoid bot detection."""
    await Stealth().apply_stealth_async(page)


class PgMortgageExtractionService:
    """Process mortgage PDFs and push extracted data to PG."""

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = resolve_pg_dsn(dsn)
        self.engine = get_engine(self.dsn)
        self.storage = ScraperStorage()
        self.vision = VisionService()

    @staticmethod
    def _has_value(value: Any) -> bool:
        """Return True when a parsed field is meaningfully populated."""
        if value is None:
            return False
        if isinstance(value, str):
            return bool(value.strip())
        if isinstance(value, (list, dict, tuple, set)):
            return len(value) > 0
        if isinstance(value, (int, float)):
            return value != 0
        return True

    @classmethod
    def _is_cache_complete(cls, data: Any) -> bool:
        """Gate mortgage cache reads/writes to avoid persisting partial extracts."""
        if not isinstance(data, dict):
            return False
        has_principal = cls._has_value(data.get("principal_amount"))
        has_lender = cls._has_value(data.get("lender"))
        has_borrower = cls._has_value(data.get("borrower"))
        return has_principal and (has_lender or has_borrower)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        *,
        limit: int | None = None,
        straps: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """Find unprocessed mortgages, download PDFs, extract via Vision, push to PG."""
        started = time.monotonic()
        target_straps = [strap.strip() for strap in (straps or []) if strap and strap.strip()]
        if straps is not None and not target_straps:
            return {"skipped": True, "reason": "no_target_straps"}

        needs_extract = self._find_unextracted_mortgages(limit, straps=target_straps or None)
        if not needs_extract:
            logger.info("No unextracted mortgages found.")
            return {"skipped": True, "reason": "no_mortgages"}

        logger.info(f"Found {len(needs_extract)} mortgages needing extraction")

        extracted = asyncio.run(self._process_mortgages_async(needs_extract))

        elapsed = round(time.monotonic() - started, 2)
        logger.info(f"Extracted {extracted}/{len(needs_extract)} mortgages in {elapsed}s")

        return {
            "mortgages_found": len(needs_extract),
            "mortgages_extracted": extracted,
            "elapsed_seconds": elapsed,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _find_unextracted_mortgages(
        self,
        limit: int | None,
        *,
        straps: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Find mortgages in the DB that don't have mortgage_data extracted."""
        query = """
            SELECT
                id,
                ori_id,
                instrument_number,
                case_number,
                folio,
                strap
            FROM ori_encumbrances
            WHERE encumbrance_type = 'mortgage'
              AND mortgage_data IS NULL
              AND ori_id IS NOT NULL
        """
        params: dict[str, Any] = {}
        if straps:
            query += "\n              AND strap = ANY(:straps)"
            params["straps"] = list(straps)
        query += "\n            ORDER BY id DESC"
        if limit is not None:
            query += "\nLIMIT :limit"
            params["limit"] = limit

        with self.engine.connect() as conn:
            rows = conn.execute(text(query), params).mappings().fetchall()

        return [dict(r) for r in rows]

    async def _process_mortgages_async(self, mortgages: list[dict[str, Any]]) -> int:
        """Run the playwright downloader and vision processor sequentially for safety."""
        extracted_count = 0

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=USER_AGENT_DESKTOP,
                accept_downloads=True,
            )
            page = await context.new_page()
            await apply_stealth(page)
            # Pre-navigate to the domain to establish cookies / session
            await page.goto("https://publicaccess.hillsclerk.com/PAVDirectSearch/", timeout=30000)

            try:
                for mtg in mortgages:
                    try:
                        success = await self._process_single(page, mtg)
                        if success:
                            extracted_count += 1
                    except Exception as exc:
                        logger.error(f"Error processing mortgage {mtg['instrument_number']}: {exc}")

                    # Small sleep between records to avoid hammering Clerk
                    await asyncio.sleep(2)
            finally:
                await browser.close()

        return extracted_count

    async def _process_single(self, page: Page, mtg: dict[str, Any]) -> bool:
        """Download PDF for the given mortgage and run Vision extraction."""
        instrument = mtg["instrument_number"]
        case_num = mtg["case_number"] or "unknown_case"
        enc_id = mtg["id"]
        ori_id = mtg["ori_id"]

        logger.info(f"Processing Mortgage Instrument {instrument} (ID {enc_id})")

        # 1. Check if PDF already on disk
        storage_id = case_num if case_num and case_num != "unknown_case" else instrument

        pdf_path = self.storage.document_exists(
            property_id=storage_id,
            doc_type="mortgage",
            doc_id=instrument,
            extension="pdf",
        )

        # 2. Download from PAV if not found
        if not pdf_path:
            logger.info(f"PDF not found locally, downloading for {instrument}...")
            pdf_path = await self._download_mortgage_pdf(page, instrument, ori_id, storage_id)

        if not pdf_path:
            logger.warning(f"Could not secure PDF for Mortgage {instrument}")
            return False

        # 3. Check JSON cache (same pattern as FinalJudgmentProcessor)
        cache_path = Path(pdf_path).parent / f"{Path(pdf_path).stem}_extracted.json"
        if cache_path.exists():
            try:
                cached = json.loads(cache_path.read_text(encoding="utf-8"))
                if cached and self._is_cache_complete(cached):
                    logger.info(f"Loaded cached mortgage extraction for {instrument}")
                    result = cached
                    # Jump straight to DB save
                    try:
                        self._save_to_pg(enc_id, result)
                        return True
                    except Exception as exc:
                        logger.error(f"Failed to save cached mortgage to DB for {instrument}: {exc}")
                        return False
                elif cached:
                    logger.warning(
                        f"Incomplete mortgage cache for {instrument}; re-extracting from PDF"
                    )
            except Exception as e:
                logger.warning(f"Bad mortgage cache file {cache_path}, re-extracting: {e}")

        # 4. Render PDF pages to images and extract via Vision
        #    VisionService expects image paths, not PDF paths
        #    (same pattern as FinalJudgmentProcessor)
        logger.info(f"Extracting JSON via Vision for {instrument}...")
        import fitz as _fitz
        import tempfile as _tempfile

        page_images: list[str] = []
        result = None
        try:
            doc = _fitz.open(str(pdf_path))
            num_pages = min(len(doc), 3)  # Mortgages: first 3 pages have the key data
            for page_num in range(num_pages):
                pg = doc[page_num]
                pix = pg.get_pixmap(dpi=150)
                with _tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                    pix.save(tmp.name)
                    page_images.append(tmp.name)
            doc.close()

            # Extract from each page, merge results
            for idx, img_path in enumerate(page_images):
                if idx > 0:
                    await asyncio.sleep(1)  # Pace API calls without blocking the event loop
                page_result = self.vision.extract_json(img_path, MORTGAGE_PROMPT)
                if page_result:
                    if result is None:
                        result = page_result
                    else:
                        # Fill in missing fields from subsequent pages
                        for k, v in page_result.items():
                            if v and not result.get(k):
                                result[k] = v
        finally:
            # Clean up temp images
            for img_path in page_images:
                with contextlib.suppress(OSError):
                    Path(img_path).unlink(missing_ok=True)

        if not result:
            logger.warning(f"Vision extraction failed or returned NULL for {instrument}")
            return False

        # 5. Write JSON cache to disk (survives DB rebuilds)
        if self._is_cache_complete(result):
            try:
                cache_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
                logger.debug(f"Cached mortgage extraction to {cache_path}")
            except OSError as exc:
                logger.warning(f"Failed to write mortgage cache {cache_path}: {exc}")
        else:
            logger.warning(
                f"Mortgage extraction for {instrument} is partial; skipping cache write so future runs can retry"
            )

        # 6. Save to Database
        try:
            self._save_to_pg(enc_id, result)
            return True
        except Exception as exc:
            logger.error(f"Failed to save extracted mortgage to DB for {instrument}: {exc}")
            return False

    async def _download_mortgage_pdf(self, page: Page, instrument: str, doc_id: str, storage_id: str) -> Path | None:
        """Download the PDF using the known doc_id."""

        # Perform download using the doc_id
        encoded_id = urllib.parse.quote(doc_id)
        download_url = f"https://publicaccess.hillsclerk.com/PAVDirectSearch/api/Document/{encoded_id}/?OverlayMode=View"

        logger.info(f"Triggering download: {download_url}")
        try:
            # We open a new throwaway page to handle the download so we don't navigate away
            dl_page = await page.context.new_page()
            try:
                async with dl_page.expect_download(timeout=60000) as download_info:
                    await dl_page.evaluate(f"window.location.href = '{download_url}'")

                download = await download_info.value
                temp_path = await download.path()
                pdf_bytes = Path(temp_path).read_bytes()

                saved_path = self.storage.save_document(
                    property_id=storage_id,
                    file_data=pdf_bytes,
                    doc_type="mortgage",
                    doc_id=instrument,
                    extension="pdf",
                )
                full_path = self.storage.get_full_path(storage_id, saved_path)
                logger.info(f"Saved mortgage PDF to {full_path}")
                return full_path
            finally:
                await dl_page.close()

        except Exception as e:
            logger.error(f"Error downloading PDF for {instrument}: {e}")
            return None

    def _save_to_pg(self, encumbrance_id: int, mortgage_data: dict[str, Any]) -> None:
        """Update the ori_encumbrances row with the JSON and amounts."""
        principal = mortgage_data.get("principal_amount")
        v = validate_mortgage_amount(principal)
        amount_val = v["amount"]
        if v["flags"]:
            logger.debug(
                "Mortgage amount flags for encumbrance {}: {} (confidence={})",
                encumbrance_id,
                ", ".join(v["flags"]),
                v["confidence"],
            )

        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE ori_encumbrances
                    SET mortgage_data = CAST(:jdata AS JSONB),
                        amount = COALESCE(NULLIF(amount, 0), :amt),
                        updated_at = now()
                    WHERE id = :id
                """),
                {"jdata": json.dumps(mortgage_data), "amt": amount_val, "id": encumbrance_id},
            )
