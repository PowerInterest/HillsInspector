"""Test script: Vision OCR extraction on 10 unlinked SAT/REL documents.

Downloads PDFs from PAVDirectSearch using ori_id, runs Vision extraction,
and checks whether the extracted parent_instrument reference resolves to
a known encumbrance in ori_encumbrances.

Usage:
    uv run python scripts/test_sat_vision.py
"""

from __future__ import annotations

import asyncio
import json
import re
import sys
import urllib.parse
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from loguru import logger
from playwright.async_api import async_playwright, Page
from playwright_stealth import Stealth
from sqlalchemy import text

from src.services.vision_service import VisionService
from sunbiz.db import get_engine, resolve_pg_dsn

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TEST_LIMIT = 10
DOWNLOAD_DIR = Path("data/cache/sat_vision_test")

SAT_VISION_PROMPT = """Extract the following data from this Satisfaction, Release, or Termination document into JSON:

1. instrument_number: The instrument number of THIS document.
2. recording_date: The recording date of THIS document (YYYY-MM-DD).
3. party_1: The Releasor/Creditor/Lender filing the satisfaction.
4. party_2: The Releasee/Debtor/Borrower whose debt is being released.
5. parent_instrument: The instrument number of the ORIGINAL Mortgage or Lien being satisfied/released. Look for references like "Instrument No.", "CLK #", "Document #", or explicit instrument numbers.
6. parent_book_page: The Official Records Book and Page of the original Mortgage or Lien. Look for "O.R. Book", "Official Records Book", "recorded in Book X, Page Y".
7. parent_case_number: Any civil case number referenced (format like XX-CA-NNNNNN or XX-CC-NNNNNN).
8. satisfaction_type: "full" if this fully satisfies the debt, "partial" if partial release.
9. original_amount: The original mortgage/lien amount if mentioned.

Expected JSON Schema:
{
  "type": "object",
  "properties": {
    "instrument_number": { "type": ["string", "null"] },
    "recording_date": { "type": ["string", "null"], "description": "YYYY-MM-DD" },
    "party_1": { "type": ["string", "null"], "description": "Releasor/Creditor" },
    "party_2": { "type": ["string", "null"], "description": "Releasee/Debtor" },
    "parent_instrument": { "type": ["string", "null"], "description": "Instrument # of original mortgage/lien" },
    "parent_book_page": { "type": ["string", "null"], "description": "Book and Page of original mortgage/lien" },
    "parent_case_number": { "type": ["string", "null"] },
    "satisfaction_type": { "type": ["string", "null"], "description": "full or partial" },
    "original_amount": { "type": ["number", "null"] }
  },
  "required": ["instrument_number", "party_1", "party_2", "parent_instrument", "parent_book_page"],
  "additionalProperties": false
}
"""

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
)

# Reuse existing regex patterns for matching extracted refs
_INST_RE = re.compile(r"\d{7,10}")
_BKPG_RE = re.compile(r"(\d+)\s*[,/]\s*(\d+)")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_test_candidates(engine) -> list[dict[str, Any]]:
    """Get unlinked SAT/REL docs that have ori_id for download."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT oe.id, oe.instrument_number, oe.ori_id, oe.party1, oe.party2,
                       oe.recording_date, oe.legal_description, oe.strap,
                       oe.raw_document_type, oe.encumbrance_type
                FROM ori_encumbrances oe
                WHERE oe.encumbrance_type IN ('satisfaction', 'release')
                  AND oe.satisfies_encumbrance_id IS NULL
                  AND oe.ori_id IS NOT NULL
                ORDER BY oe.recording_date DESC NULLS LAST
                LIMIT :lim
            """),
            {"lim": TEST_LIMIT},
        ).mappings().all()
    return [dict(r) for r in rows]


def _find_parent_encumbrance(
    engine, strap: str, extracted: dict[str, Any]
) -> dict[str, Any] | None:
    """Try to match extracted parent reference to a known encumbrance."""
    parent_inst = (extracted.get("parent_instrument") or "").strip()
    parent_bkpg = (extracted.get("parent_book_page") or "").strip()

    with engine.connect() as conn:
        encs = conn.execute(
            text("""
                SELECT id, instrument_number, book, page, encumbrance_type,
                       party1, amount, recording_date
                FROM ori_encumbrances
                WHERE strap = :strap
                  AND encumbrance_type IN ('mortgage', 'lien', 'judgment')
            """),
            {"strap": strap},
        ).mappings().all()

    if not encs:
        return None

    # Strategy 1: Match instrument number
    if parent_inst:
        inst_digits = _INST_RE.findall(parent_inst)
        for enc in encs:
            enc_inst = (enc["instrument_number"] or "").strip()
            if enc_inst and enc_inst in inst_digits:
                return {"match": "instrument", "enc": dict(enc)}

    # Strategy 2: Match book/page
    if parent_bkpg:
        bkpg_pairs = _BKPG_RE.findall(parent_bkpg)
        for enc in encs:
            enc_bk = (enc["book"] or "").strip()
            enc_pg = (enc["page"] or "").strip()
            if enc_bk and enc_pg:
                for bk, pg in bkpg_pairs:
                    if bk == enc_bk and pg == enc_pg:
                        return {"match": "book_page", "enc": dict(enc)}

    return None


async def _download_pdf(page: Page, ori_id: str, instrument: str) -> Path | None:
    """Download a single ORI document PDF."""
    pdf_path = DOWNLOAD_DIR / f"{instrument}.pdf"
    if pdf_path.exists() and pdf_path.stat().st_size > 1000:
        logger.info("PDF already cached: {}", pdf_path)
        return pdf_path

    encoded_id = urllib.parse.quote(ori_id)
    url = f"https://publicaccess.hillsclerk.com/PAVDirectSearch/api/Document/{encoded_id}/?OverlayMode=View"

    try:
        dl_page = await page.context.new_page()
        try:
            async with dl_page.expect_download(timeout=60000) as download_info:
                await dl_page.evaluate(f"window.location.href = '{url}'")
            download = await download_info.value
            temp_path = await download.path()
            pdf_bytes = Path(temp_path).read_bytes()
            pdf_path.write_bytes(pdf_bytes)
            logger.info("Downloaded {} ({} bytes)", pdf_path.name, len(pdf_bytes))
            return pdf_path
        finally:
            await dl_page.close()
    except Exception as e:
        logger.error("Download failed for {}: {}", instrument, e)
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main() -> None:
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    engine = get_engine(resolve_pg_dsn())
    vision = VisionService()

    candidates = _get_test_candidates(engine)
    logger.info("Selected {} SAT/REL candidates for vision test", len(candidates))

    results: list[dict[str, Any]] = []

    # Download PDFs via Playwright
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=USER_AGENT,
            accept_downloads=True,
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)

        # Pre-navigate to establish session
        await page.goto(
            "https://publicaccess.hillsclerk.com/PAVDirectSearch/",
            timeout=30000,
        )
        await asyncio.sleep(2)

        for i, sat in enumerate(candidates):
            instrument = sat["instrument_number"]
            ori_id = sat["ori_id"]
            strap = sat["strap"]
            logger.info(
                "[{}/{}] Processing {} (ID={} type={} raw={})",
                i + 1, len(candidates), instrument,
                sat["id"], sat["encumbrance_type"], sat["raw_document_type"],
            )

            # Download
            pdf_path = await _download_pdf(page, ori_id, instrument)
            if not pdf_path:
                results.append({
                    "id": sat["id"],
                    "instrument": instrument,
                    "status": "download_failed",
                })
                continue

            # Vision extraction
            extracted = vision.extract_json(str(pdf_path), SAT_VISION_PROMPT)
            if not extracted:
                results.append({
                    "id": sat["id"],
                    "instrument": instrument,
                    "status": "vision_failed",
                })
                continue

            # Check if we can resolve the parent
            parent = _find_parent_encumbrance(engine, strap, extracted)

            result = {
                "id": sat["id"],
                "instrument": instrument,
                "strap": strap,
                "raw_type": sat["raw_document_type"],
                "existing_legal": (sat["legal_description"] or "")[:100],
                "status": "matched" if parent else "extracted_no_match",
                "extracted": {
                    "parent_instrument": extracted.get("parent_instrument"),
                    "parent_book_page": extracted.get("parent_book_page"),
                    "parent_case_number": extracted.get("parent_case_number"),
                    "party_1": extracted.get("party_1"),
                    "party_2": extracted.get("party_2"),
                    "satisfaction_type": extracted.get("satisfaction_type"),
                    "original_amount": extracted.get("original_amount"),
                },
            }
            if parent:
                result["matched_via"] = parent["match"]
                result["parent_enc"] = {
                    "id": parent["enc"]["id"],
                    "instrument": parent["enc"]["instrument_number"],
                    "type": parent["enc"]["encumbrance_type"],
                    "party1": (parent["enc"]["party1"] or "")[:60],
                    "amount": str(parent["enc"]["amount"]) if parent["enc"]["amount"] else None,
                }

            results.append(result)
            logger.info(
                "  → {} | parent_inst={} parent_bkpg={}",
                result["status"],
                extracted.get("parent_instrument"),
                extracted.get("parent_book_page"),
            )

            await asyncio.sleep(2)  # Rate limit

        await browser.close()

    # Summary
    print("\n" + "=" * 80)
    print("SAT/REL VISION TEST RESULTS")
    print("=" * 80)

    matched = [r for r in results if r["status"] == "matched"]
    extracted = [r for r in results if r["status"] == "extracted_no_match"]
    failed = [r for r in results if r["status"] in ("download_failed", "vision_failed")]

    print(f"\nTotal tested:      {len(results)}")
    print(f"Matched parent:    {len(matched)}")
    print(f"Extracted no match:{len(extracted)}")
    print(f"Failed:            {len(failed)}")

    if matched:
        print(f"\nVision link rate:  {len(matched)}/{len(results)} = {len(matched)/len(results)*100:.0f}%")

    for r in results:
        print(f"\n--- {r['instrument']} (ID={r['id']}) ---")
        print(f"  Status: {r['status']}")
        if "extracted" in r:
            ext = r["extracted"]
            print(f"  Vision found: inst={ext.get('parent_instrument')} bkpg={ext.get('parent_book_page')}")
            print(f"  Party1={ext.get('party_1', '')[:60]}")
            print(f"  Amount={ext.get('original_amount')}")
        if "parent_enc" in r:
            pe = r["parent_enc"]
            print(f"  MATCHED → enc_id={pe['id']} type={pe['type']} inst={pe['instrument']}")
        if "existing_legal" in r:
            print(f"  Existing legal: {r['existing_legal']}")

    # Save full results
    out_path = DOWNLOAD_DIR / "test_results.json"
    out_path.write_text(json.dumps(results, indent=2, default=str))
    print(f"\nFull results saved to {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
