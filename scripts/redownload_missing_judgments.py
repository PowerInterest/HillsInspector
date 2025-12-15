"""
Re-download Final Judgment PDFs for auctions that have valid parcel IDs but no PDF files.

This script:
1. Finds auctions missing extracted_judgment_data with valid parcel_ids
2. Re-scrapes the auction page to get the case href with CQID=320
3. Downloads the PDF using the existing download method
4. Triggers extraction via FinalJudgmentProcessor
"""

import asyncio
import json
import urllib.parse
from datetime import date
from pathlib import Path

import duckdb
from loguru import logger
from playwright.async_api import async_playwright

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.services.final_judgment_processor import FinalJudgmentProcessor
from src.services.scraper_storage import ScraperStorage
from src.db.operations import PropertyDB


USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"


def get_auctions_missing_pdfs(conn) -> list[dict]:
    """Get auctions with valid parcel_id but no extracted judgment data."""
    result = conn.execute('''
        SELECT case_number, folio, parcel_id, auction_date, final_judgment_amount
        FROM auctions
        WHERE extracted_judgment_data IS NULL
          AND parcel_id IS NOT NULL
          AND parcel_id != ''
          AND parcel_id NOT IN ('Property Appraiser', 'property appraiser', 'N/A')
        ORDER BY auction_date
    ''').fetchdf()
    return result.to_dict('records')


def pdf_exists(folio: str) -> bool:
    """Check if a final judgment PDF already exists for this folio."""
    doc_dir = Path(f"data/properties/{folio}/documents")
    if not doc_dir.exists():
        return False
    pdfs = list(doc_dir.glob("final_judgment*.pdf"))
    return len(pdfs) > 0


async def download_pdf_for_case(
    page,
    case_number: str,
    parcel_id: str,
    auction_date: date,
    storage: ScraperStorage
) -> Path | None:
    """Navigate to auction page and download PDF for a specific case."""

    date_str = auction_date.strftime("%m/%d/%Y")
    url = f"https://hillsborough.realforeclose.com/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE={date_str}"

    try:
        logger.info(f"Loading auction page for {date_str}...")
        await page.goto(url, timeout=60000)
        await page.wait_for_load_state("networkidle")

        # Find the specific case
        items = page.locator("div.AUCTION_ITEM")
        count = await items.count()

        for i in range(count):
            item = items.nth(i)
            details = item.locator("table.ad_tab")

            # Check case #
            case_row = details.locator("tr:has-text('Case #:')")
            case_link = case_row.locator("a")
            if await case_link.count() == 0:
                continue

            found_case = (await case_link.inner_text()).strip()
            if found_case != case_number:
                continue

            # Found our case - get the href
            case_href = await case_link.get_attribute("href")
            if not case_href or "CQID=320" not in case_href:
                logger.warning(f"No CQID=320 in href for {case_number}")
                return None

            # Extract instrument number
            instrument_number = None
            if "OBKey__1006_1=" in case_href:
                instrument_number = case_href.split("OBKey__1006_1=")[-1]

            # Download PDF
            return await _download_final_judgment(
                page, case_href, case_number, parcel_id, instrument_number, storage
            )

        logger.warning(f"Case {case_number} not found on auction page for {date_str}")
        return None

    except Exception as e:
        logger.error(f"Error processing {case_number}: {e}")
        return None


async def _download_final_judgment(
    page,
    onbase_url: str,
    case_number: str,
    parcel_id: str,
    instrument_number: str | None,
    storage: ScraperStorage
) -> Path | None:
    """Download Final Judgment PDF from OnBase."""

    new_context = None
    new_page = None

    try:
        new_context = await page.context.browser.new_context(
            user_agent=USER_AGENT,
            accept_downloads=True
        )
        new_page = await new_context.new_page()

        # Future to capture Document ID from API response
        doc_id_future = asyncio.Future()

        async def handle_response(response):
            if "KeywordSearch" in response.url and not doc_id_future.done():
                try:
                    json_data = await response.json()
                    if "Data" in json_data and len(json_data["Data"]) > 0:
                        doc_id = json_data["Data"][0].get("ID")
                        if doc_id:
                            doc_id_future.set_result(doc_id)
                except Exception as exc:
                    logger.debug(f"Failed to parse OnBase response for {case_number}: {exc}")

        new_page.on("response", handle_response)

        logger.info(f"Navigating to OnBase for {case_number}...")
        await new_page.goto(onbase_url, timeout=30000)

        try:
            onbase_doc_id = await asyncio.wait_for(doc_id_future, timeout=15.0)
        except TimeoutError:
            logger.warning(f"Could not find Document ID for {case_number}")
            return None

        # Construct download URL
        encoded_id = urllib.parse.quote(onbase_doc_id)
        download_url = f"https://publicaccess.hillsclerk.com/PAVDirectSearch/api/Document/{encoded_id}/?OverlayMode=View"

        logger.info(f"Downloading PDF for {case_number}...")

        async with new_page.expect_download(timeout=60000) as download_info:
            await new_page.evaluate(f"window.location.href = '{download_url}'")

        download = await download_info.value

        # Read bytes
        pdf_path = await download.path()
        with open(pdf_path, "rb") as f:
            pdf_bytes = f.read()

        # Save using storage
        doc_id = instrument_number if instrument_number else case_number
        saved_path = storage.save_document(
            property_id=parcel_id,
            file_data=pdf_bytes,
            doc_type="final_judgment",
            doc_id=doc_id,
            extension="pdf"
        )

        full_path = storage.get_full_path(parcel_id, saved_path)
        logger.success(f"Saved PDF to {full_path}")
        return full_path

    except Exception as e:
        logger.error(f"Error downloading PDF for {case_number}: {e}")
        return None
    finally:
        if new_page:
            await new_page.close()
        if new_context:
            await new_context.close()


def extract_and_save_judgment(pdf_path: Path, case_number: str, parcel_id: str, db: PropertyDB, storage: ScraperStorage):
    """Extract data from PDF and save to database."""
    processor = FinalJudgmentProcessor()

    result = processor.process_pdf(str(pdf_path), case_number)
    if not result:
        logger.warning(f"No data extracted from {pdf_path}")
        return False

    amounts = processor.extract_key_amounts(result)
    payload = {
        **result,
        **amounts,
        "extracted_judgment_data": json.dumps(result),
        "raw_judgment_text": result.get("raw_text", ""),
    }

    # Save vision output
    vision_path = storage.save_vision_output(
        property_id=parcel_id,
        scraper="final_judgment",
        vision_data=result,
        context=case_number
    )

    storage.record_scrape(
        property_id=parcel_id,
        scraper="final_judgment",
        vision_output_path=vision_path,
        vision_data=result,
        success=True
    )

    updated = db.update_judgment_data(case_number, payload)
    if updated:
        logger.success(f"Stored Final Judgment data for {case_number}")
        return True

    logger.warning(f"No fields updated for {case_number}")
    return False


async def main():
    logger.info("=" * 60)
    logger.info("RE-DOWNLOADING MISSING FINAL JUDGMENT PDFs")
    logger.info("=" * 60)

    conn = duckdb.connect('data/property_master.db', read_only=False)
    db = PropertyDB()
    storage = ScraperStorage()

    # Get auctions needing PDFs
    auctions = get_auctions_missing_pdfs(conn)

    # Filter to those without existing PDFs
    auctions_needing_download = []
    for auction in auctions:
        folio = auction['folio'] or auction['parcel_id']
        if not pdf_exists(folio):
            auctions_needing_download.append(auction)
        else:
            logger.info(f"PDF already exists for {auction['case_number']}, will extract")

    logger.info(f"Found {len(auctions)} auctions missing extraction")
    logger.info(f"  - {len(auctions_needing_download)} need PDF download")
    logger.info(f"  - {len(auctions) - len(auctions_needing_download)} have PDFs (just need extraction)")

    if not auctions_needing_download:
        logger.info("No PDFs to download!")
    else:
        # Download PDFs
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=USER_AGENT)
            page = await context.new_page()

            downloaded = 0
            for i, auction in enumerate(auctions_needing_download, 1):
                case_number = auction['case_number']
                folio = auction['folio'] or auction['parcel_id']
                auction_date = auction['auction_date']

                logger.info(f"\n[{i}/{len(auctions_needing_download)}] {case_number}")
                logger.info(f"  Folio: {folio}")
                logger.info(f"  Date: {auction_date}")

                pdf_path = await download_pdf_for_case(
                    page, case_number, folio, auction_date, storage
                )

                if pdf_path:
                    downloaded += 1
                    # Extract immediately
                    extract_and_save_judgment(pdf_path, case_number, folio, db, storage)

                # Small delay between downloads
                await asyncio.sleep(1)

            await browser.close()
            logger.info(f"\nDownloaded {downloaded}/{len(auctions_needing_download)} PDFs")

    # Process any existing PDFs that weren't extracted
    for auction in auctions:
        folio = auction['folio'] or auction['parcel_id']
        case_number = auction['case_number']

        doc_dir = Path(f"data/properties/{folio}/documents")
        pdfs = list(doc_dir.glob("final_judgment*.pdf")) if doc_dir.exists() else []

        if pdfs and not auction.get('extracted_judgment_data'):
            logger.info(f"Extracting existing PDF for {case_number}...")
            extract_and_save_judgment(pdfs[0], case_number, folio, db, storage)

    conn.close()
    logger.success("Done!")


if __name__ == "__main__":
    asyncio.run(main())
