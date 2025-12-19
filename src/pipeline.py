"""
Full end-to-end pipeline for property analysis.

Steps:
1. Scrape auctions (foreclosure + tax deed) - calendar-based skip logic
1.5. Scrape tax deed auctions
2. Extract Final Judgment data from PDFs (includes legal description)
3. Property Enrichment from BULK DATA (owner, legal description from 4 fields)
4. HCPA GIS - Sales history & additional property details
5. Ingest ORI data & build chain of title (uses legal description permutations)
6. Analyze lien survival
7. Sunbiz - Business entity lookup (if LLC/Corp)
8. Scrape building permits
9. FEMA flood zone lookup
10. Market data - Zillow (always refresh)
11. Market data - Realtor.com
12. Property enrichment - HCPA (fallback for missing data)
13. Tax payment status
"""

import asyncio
import contextlib
import json
import re
import urllib.parse
from datetime import date, timedelta, datetime, UTC
from pathlib import Path
from typing import Optional
from loguru import logger
import sys

# Add parent directory to path for imports if running directly
if str(Path(__file__).parent.parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent))

from src.scrapers.auction_scraper import AuctionScraper
from src.scrapers.tax_deed_scraper import TaxDeedScraper
from src.scrapers.hcpa_scraper import HCPAScraper
from src.scrapers.hcpa_gis_scraper import scrape_hcpa_property
from src.scrapers.permit_scraper import PermitScraper
from src.scrapers.fema_flood_scraper import FEMAFloodChecker
from src.scrapers.sunbiz_scraper import SunbizScraper
from src.scrapers.market_scraper import MarketScraper
from src.scrapers.realtor_scraper import RealtorScraper
from src.scrapers.tax_scraper import TaxScraper
from src.scrapers.ori_scraper import ORIScraper
from src.services.final_judgment_processor import FinalJudgmentProcessor
from src.services.lien_survival_analyzer import LienSurvivalAnalyzer
from src.services.ingestion_service import IngestionService
from src.services.scraper_storage import ScraperStorage
from src.ingest.bulk_parcel_ingest import enrich_auctions_from_bulk
from src.services.data_linker import link_permits_to_nocs
from src.db.operations import PropertyDB
from src.models.property import Property
from playwright.async_api import async_playwright

from src.services.homeharvest_service import HomeHarvestService


# Invalid folio values to skip - these are often scraped incorrectly from the auction site
INVALID_FOLIO_VALUES = {
    'property appraiser', 'n/a', 'none', '', 'unknown', 'pending',
    'see document', 'multiple', 'various', 'tbd', 'na'
}


def is_valid_folio(folio: str) -> bool:
    """
    Validate that a folio/parcel ID is a real parcel number, not garbage data.

    Hillsborough County folios are typically 21+ character alphanumeric strings
    like "182811104000055000300U" or similar patterns.

    Returns False for:
    - Empty/None values
    - Known invalid values like "Property Appraiser"
    - Values that are too short (< 6 chars)
    - Values that are all letters (likely labels, not IDs)
    """
    if not folio:
        return False

    folio_clean = folio.strip().lower()

    # Check against known invalid values
    if folio_clean in INVALID_FOLIO_VALUES:
        return False

    # Folio should have at least some minimum length
    if len(folio_clean) < 6:
        return False

    # Folio should contain at least some digits (not all letters)
    return any(c.isdigit() for c in folio_clean)


class PipelineDB(PropertyDB):
    """Extended DB class for pipeline operations."""

    def execute_query(self, query: str, params: tuple = ()):
        conn = self.connect()
        results = conn.execute(query, params).fetchall()
        columns = [desc[0] for desc in conn.description]
        return [dict(zip(columns, row, strict=False)) for row in results]

    def ensure_last_analyzed_column(self):
        """Add last_analyzed_case_number column if missing."""
        conn = self.connect()
        conn.execute(
            "ALTER TABLE parcels ADD COLUMN IF NOT EXISTS last_analyzed_case_number VARCHAR"
        )

    def get_auction_count_by_date(self, auction_date: date) -> int:
        """Get count of auctions we have for a specific date."""
        conn = self.connect()
        result = conn.execute(
            "SELECT COUNT(*) FROM auctions WHERE auction_date = ?", [auction_date]
        ).fetchone()
        return result[0] if result else 0

    def folio_has_sales_history(self, folio: str) -> bool:
        """Check if folio has sales history data."""
        conn = self.connect()
        result = conn.execute(
            "SELECT COUNT(*) FROM sales_history WHERE folio = ?", [folio]
        ).fetchone()
        return result[0] > 0 if result else False

    def folio_has_chain_of_title(self, folio: str) -> bool:
        """Check if folio has chain of title data."""
        conn = self.connect()
        result = conn.execute(
            "SELECT COUNT(*) FROM chain_of_title WHERE folio = ?", [folio]
        ).fetchone()
        return result[0] > 0 if result else False

    def folio_has_encumbrances(self, folio: str) -> bool:
        """Check if folio has encumbrances."""
        conn = self.connect()
        result = conn.execute(
            "SELECT COUNT(*) FROM encumbrances WHERE folio = ?", [folio]
        ).fetchone()
        return result[0] > 0 if result else False

    def folio_has_survival_analysis(self, folio: str) -> bool:
        """Check if folio has survival status set on encumbrances."""
        conn = self.connect()
        result = conn.execute(
            "SELECT COUNT(*) FROM encumbrances WHERE folio = ? AND survival_status IS NOT NULL AND survival_status != 'UNKNOWN'",
            [folio],
        ).fetchone()
        return result[0] > 0 if result else False

    def get_last_analyzed_case(self, folio: str) -> Optional[str]:
        """Get the last analyzed case number for a folio."""
        conn = self.connect()
        self.ensure_last_analyzed_column()
        result = conn.execute(
            "SELECT last_analyzed_case_number FROM parcels WHERE folio = ?", [folio]
        ).fetchone()
        return result[0] if result else None

    def set_last_analyzed_case(self, folio: str, case_number: str):
        """Set the last analyzed case number for a folio."""
        conn = self.connect()
        self.ensure_last_analyzed_column()
        # Ensure parcel exists
        conn.execute(
            "INSERT OR IGNORE INTO parcels (folio) VALUES (?)", [folio]
        )
        conn.execute(
            "UPDATE parcels SET last_analyzed_case_number = ?, updated_at = CURRENT_TIMESTAMP WHERE folio = ?",
            [case_number, folio],
        )

    def folio_has_permits(self, folio: str) -> bool:
        """Check if folio has permit data."""
        conn = self.connect()
        result = conn.execute(
            "SELECT COUNT(*) FROM permits WHERE folio = ?", [folio]
        ).fetchone()
        return result[0] > 0 if result else False

    def folio_has_flood_data(self, folio: str) -> bool:
        """Check if parcel has flood zone data by checking for flood fields."""
        conn = self.connect()
        # Check if we have a parcel with flood data
        # We'll store flood data in parcels table
        try:
            result = conn.execute(
                """SELECT COUNT(*) FROM parcels
                   WHERE folio = ? AND flood_zone IS NOT NULL""",
                [folio],
            ).fetchone()
            return result[0] > 0 if result else False
        except Exception:
            # Column might not exist yet
            return False

    def save_flood_data(self, folio: str, flood_zone: str, flood_risk: str, insurance_required: bool):
        """Save flood zone data to parcels table."""
        conn = self.connect()
        # Ensure columns exist
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS flood_zone VARCHAR")
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS flood_risk VARCHAR")
        conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS flood_insurance_required BOOLEAN")
        # Ensure parcel exists
        conn.execute("INSERT OR IGNORE INTO parcels (folio) VALUES (?)", [folio])
        conn.execute(
            """UPDATE parcels SET
               flood_zone = ?, flood_risk = ?, flood_insurance_required = ?,
               updated_at = CURRENT_TIMESTAMP
               WHERE folio = ?""",
            [flood_zone, flood_risk, insurance_required, folio],
        )

    def folio_has_realtor_data(self, folio: str) -> bool:
        """Check if folio has realtor.com data."""
        conn = self.connect()
        result = conn.execute(
            "SELECT COUNT(*) FROM market_data WHERE folio = ? AND source = 'Realtor'",
            [folio],
        ).fetchone()
        return result[0] > 0 if result else False

    def folio_has_owner_name(self, folio: str) -> bool:
        """Check if folio has owner name in parcels."""
        conn = self.connect()
        result = conn.execute(
            "SELECT owner_name FROM parcels WHERE folio = ?", [folio]
        ).fetchone()
        return result is not None and result[0] is not None

    def folio_has_tax_data(self, folio: str) -> bool:
        """Check if folio has tax data."""
        conn = self.connect()
        # Check if we have tax data - stored in liens table with type TAX
        result = conn.execute(
            "SELECT COUNT(*) FROM liens WHERE folio = ? AND document_type = 'TAX'",
            [folio],
        ).fetchone()
        return result[0] > 0 if result else False

    def folio_has_sunbiz_data(self, folio: str) -> bool:
        """Check if folio has sunbiz entity data (stored in scraper_results)."""
        storage = ScraperStorage()
        return not storage.needs_refresh(folio, "sunbiz", max_age_days=30)


def is_entity_name(name: str) -> bool:
    """Check if a name appears to be a business entity rather than a person."""
    if not name:
        return False
    name_upper = name.upper()
    entity_keywords = [
        "LLC", "L.L.C.", "INC", "INC.", "CORP", "CORPORATION",
        "TRUST", "BANK", "N.A.", "NA", "FSB", "ASSOCIATION",
        "PARTNERS", "PARTNERSHIP", "LP", "L.P.", "LLP", "L.L.P.",
        "HOLDINGS", "INVESTMENTS", "PROPERTIES", "CAPITAL",
        "MORTGAGE", "LENDING", "FINANCIAL", "SERVICES",
        "D/B/A", "DBA", "A/K/A", "AKA",
    ]
    return any(kw in name_upper for kw in entity_keywords)


async def _download_missing_judgment_pdfs(
    auctions: list[dict],
    storage: ScraperStorage,
    db: "PipelineDB"
) -> list[tuple[dict, Path]]:
    """Download Final Judgment PDFs for auctions that are missing them.

    Args:
        auctions: List of auction dicts needing PDF download
        storage: ScraperStorage instance for saving files
        db: Database connection for updating auction dates

    Returns:
        List of (auction, pdf_path) tuples for successfully downloaded PDFs
    """
    if not auctions:
        return []

    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    downloaded = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=USER_AGENT)
        page = await context.new_page()

        # Group auctions by date to minimize page loads
        auctions_by_date: dict[date, list[dict]] = {}
        for auction in auctions:
            auction_date = auction.get("auction_date")
            if auction_date:
                if isinstance(auction_date, str):
                    auction_date = datetime.strptime(auction_date[:10], "%Y-%m-%d").date()
                elif hasattr(auction_date, "date"):
                    auction_date = auction_date.date()
                auctions_by_date.setdefault(auction_date, []).append(auction)

        for auction_date, date_auctions in auctions_by_date.items():
            date_str = auction_date.strftime("%m/%d/%Y")
            url = f"https://hillsborough.realforeclose.com/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE={date_str}"

            try:
                logger.debug(f"Loading auction page for {date_str}...")
                await page.goto(url, timeout=60000)
                await page.wait_for_load_state("networkidle")

                items = page.locator("div.AUCTION_ITEM")
                item_count = await items.count()

                # Build a map of case_number -> href from the page
                case_hrefs = {}
                for i in range(item_count):
                    item = items.nth(i)
                    details = item.locator("table.ad_tab")
                    case_row = details.locator("tr:has-text('Case #:')")
                    case_link = case_row.locator("a")
                    if await case_link.count():
                        case_text = (await case_link.inner_text()).strip()
                        case_href = await case_link.get_attribute("href")
                        if case_href and "CQID=320" in case_href:
                            case_hrefs[case_text] = case_href

                # Download PDFs for each auction
                for auction in date_auctions:
                    case_number = auction["case_number"]
                    parcel_id = auction.get("parcel_id") or auction.get("folio")

                    if case_number not in case_hrefs:
                        logger.warning(f"Case {case_number} not found on page for {date_str}")
                        continue

                    case_href = case_hrefs[case_number]
                    instrument_number = None
                    if "OBKey__1006_1=" in case_href:
                        instrument_number = case_href.split("OBKey__1006_1=")[-1]

                    try:
                        pdf_path = await _download_single_judgment_pdf(
                            page, case_href, case_number, parcel_id, instrument_number, storage
                        )
                        if pdf_path:
                            downloaded.append((auction, pdf_path))
                            await asyncio.sleep(0.5)  # Rate limiting
                    except Exception as e:
                        logger.error(f"Failed to download PDF for {case_number}: {e}")

            except Exception as e:
                logger.error(f"Error loading auction page for {date_str}: {e}")

        await browser.close()

    return downloaded


async def _download_single_judgment_pdf(
    page,
    onbase_url: str,
    case_number: str,
    parcel_id: str,
    instrument_number: str | None,
    storage: ScraperStorage
) -> Path | None:
    """Download a single Final Judgment PDF from OnBase."""
    # Check if PDF already exists
    doc_id = instrument_number if instrument_number else case_number
    existing_path = storage.get_full_path(parcel_id, f"documents/final_judgment_{doc_id}.pdf")
    if existing_path.exists():
        logger.debug(f"PDF already exists for {case_number}: {existing_path}")
        return existing_path

    new_context = None
    new_page = None

    try:
        new_context = await page.context.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            accept_downloads=True
        )
        new_page = await new_context.new_page()

        # Capture Document ID from API response
        doc_id_future = asyncio.get_event_loop().create_future()

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

        logger.debug(f"Navigating to OnBase for {case_number}...")
        await new_page.goto(onbase_url, timeout=30000)

        try:
            onbase_doc_id = await asyncio.wait_for(doc_id_future, timeout=15.0)
        except TimeoutError:
            logger.warning(f"Could not find Document ID for {case_number}")
            return None

        # Construct download URL
        encoded_id = urllib.parse.quote(onbase_doc_id)
        download_url = f"https://publicaccess.hillsclerk.com/PAVDirectSearch/api/Document/{encoded_id}/?OverlayMode=View"

        logger.debug(f"Downloading PDF for {case_number}...")

        async with new_page.expect_download(timeout=60000) as download_info:
            await new_page.evaluate(f"window.location.href = '{download_url}'")

        download = await download_info.value
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
        logger.info(f"Downloaded PDF for {case_number}: {full_path.name}")
        return full_path

    except Exception as e:
        logger.error(f"Error downloading PDF for {case_number}: {e}")
        return None
    finally:
        if new_page:
            await new_page.close()
        if new_context:
            await new_context.close()


async def run_full_pipeline(
    max_auctions: int = 10,
    property_limit: Optional[int] = None,
    start_step: int = 1,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    geocode_missing_parcels: bool = False,
    geocode_limit: int | None = 25,
):
    """Run the complete property analysis pipeline with smart skip logic.

    Args:
        max_auctions: Limit applied to later market data steps.
        property_limit: Optional cap on total auctions ingested (foreclosure + tax deed).
        start_step: Step number to start from (1-15). Use to resume after failures.
        start_date: Date to start scraping from. Defaults to tomorrow.
        end_date: Date to stop scraping (inclusive). Defaults to 30 days after start_date.
        geocode_missing_parcels: Whether to geocode parcels missing latitude/longitude.
        geocode_limit: Maximum number of parcels to geocode per run (None = no limit).
    """

    logger.info("=" * 60)
    logger.info(f"STARTING FULL PIPELINE (from step {start_step})")
    logger.info("=" * 60)

    db = PipelineDB()
    db.create_chain_tables()
    db.ensure_last_analyzed_column()
    db.initialize_pipeline_flags()

    today = datetime.now(UTC).date()
    # Default to tomorrow if no start_date specified
    if start_date is None:
        start_date = today + timedelta(days=1)
    # Use provided end_date, otherwise default to 30 days after start
    if end_date is None:
        end_date = start_date + timedelta(days=30)
    if property_limit:
        end_date = start_date  # constrain debug runs to the nearest auction day

    # =========================================================================
    # STEP 1: Scrape Foreclosure Auctions (Calendar-based skip logic)
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1: SCRAPING FORECLOSURE AUCTIONS")
    logger.info("=" * 60)

    properties = []
    if start_step > 1:
        logger.info("Skipping step 1 (start_step > 1)")
    else:
        foreclosure_scraper = AuctionScraper()

        try:
            logger.info(f"Scraping foreclosures from {start_date} to {end_date}...")

            # Check day-by-day if we already have data
            dates_to_scrape = []
            current = start_date
            while current <= end_date:
                if current.weekday() < 5:  # Skip weekends
                    count = db.get_auction_count_by_date(current)
                    if count > 0:
                        logger.info(f"Skipping {current}: {count} auctions already in DB")
                    else:
                        dates_to_scrape.append(current)
                current += timedelta(days=1)

            if dates_to_scrape:
                # We scrape date-by-date to allow precise skipping
                for target_date in dates_to_scrape:
                    remaining = None
                    if property_limit:
                        remaining = max(property_limit - len(properties), 0)
                        if remaining <= 0:
                            break

                    # Use scrape_date directly instead of scrape_all
                    daily_props = await foreclosure_scraper.scrape_date(target_date, fast_fail=True, max_properties=remaining)
                    properties.extend(daily_props)

                    # Save immediately
                    for p in daily_props:
                        db.upsert_auction(p)

                logger.success(f"Scraped {len(properties)} new foreclosure auctions")
            else:
                logger.success("All dates in range already have auction data. Skipping scrape.")

        except Exception as e:
            logger.error(f"Foreclosure scrape failed: {e}")

    # =========================================================================
    # STEP 1.5: Scrape Tax Deed Auctions
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1.5: SCRAPING TAX DEED AUCTIONS")
    logger.info("=" * 60)

    tax_properties: list[Property] = []
    if start_step > 1:
        logger.info("Skipping step 1.5 (start_step > 1)")
    elif property_limit and len(properties) >= property_limit:
        logger.info("Property limit reached; skipping tax deed scrape")
    else:
        tax_deed_scraper = TaxDeedScraper()

        try:
            logger.info(f"Scraping tax deeds from {start_date} to {end_date}...")
            tax_properties = await tax_deed_scraper.scrape_all(start_date, end_date)
            logger.success(f"Scraped {len(tax_properties)} tax deed auctions")

            # Save to DB
            for p in tax_properties:
                db.upsert_auction(p)
            logger.success(f"Saved {len(tax_properties)} tax deed auctions to DB")
        except Exception as e:
            logger.error(f"Tax deed scrape failed: {e}")

    # =========================================================================
    # STEP 2: Download & Extract Final Judgment Data
    # Skip if: case_number already has extracted_judgment_data
    # Now downloads missing PDFs before extraction
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: DOWNLOADING & EXTRACTING FINAL JUDGMENT DATA")
    logger.info("=" * 60)

    if start_step > 2:
        logger.info("Skipping step 2 (start_step > 2)")
    else:
        judgment_processor = FinalJudgmentProcessor()
        storage = ScraperStorage()

        # Invalid parcel_id values that should be treated as missing
        INVALID_PARCEL_IDS = {"property appraiser", "n/a", "none", ""}

        try:
            auctions = db.execute_query(
                "SELECT * FROM auctions WHERE needs_judgment_extraction = TRUE AND parcel_id IS NOT NULL"
            )
            logger.info(f"Found {len(auctions)} auctions needing judgment extraction")
        except Exception as e:
            logger.warning(f"Could not query auctions: {e}")
            auctions = []

        # Separate auctions into those with PDFs and those needing download
        auctions_with_pdf = []
        auctions_needing_download = []

        for auction in auctions:
            case_number = auction["case_number"]
            parcel_id = auction.get("parcel_id") or ""

            # Skip invalid parcel IDs
            if parcel_id.lower() in INVALID_PARCEL_IDS:
                logger.debug(f"Skipping {case_number}: invalid parcel_id '{parcel_id}'")
                db.mark_step_complete(case_number, "needs_judgment_extraction") # Skip future runs
                continue

            # Check if PDF exists
            sanitized_folio = parcel_id.replace("/", "_").replace("\\", "_").replace(":", "_")
            base_dir = Path("data/properties") / sanitized_folio / "documents"
            potential_paths = list(base_dir.glob("final_judgment*.pdf")) if base_dir.exists() else []

            legacy_path = Path(f"data/pdfs/final_judgments/{case_number}_final_judgment.pdf")
            if legacy_path.exists():
                potential_paths.append(legacy_path)

            if potential_paths:
                auctions_with_pdf.append((auction, potential_paths[0]))
            else:
                auctions_needing_download.append(auction)

        logger.info(f"  - {len(auctions_with_pdf)} have PDFs ready for extraction")
        logger.info(f"  - {len(auctions_needing_download)} need PDF download")

        # Download missing PDFs
        downloaded_count = 0
        if auctions_needing_download:
            logger.info("Downloading missing Final Judgment PDFs...")
            downloaded_pdfs = await _download_missing_judgment_pdfs(
                auctions_needing_download, storage, db
            )
            downloaded_count = len(downloaded_pdfs)
            # Add downloaded PDFs to extraction queue
            auctions_with_pdf.extend(downloaded_pdfs)
            logger.success(f"Downloaded {downloaded_count} PDFs")

        # Extract data from all PDFs
        extracted_count = 0
        for auction, pdf_path in auctions_with_pdf:
            case_number = auction["case_number"]
            folio = auction.get("parcel_id", "")
            with logger.contextualize(folio=folio, case_number=case_number, step="judgment_extraction"):
                logger.info(f"Processing judgment from {pdf_path}...")

                try:
                    result = judgment_processor.process_pdf(str(pdf_path), case_number)
                    if result:
                        amounts = judgment_processor.extract_key_amounts(result)
                        payload = {
                            **result,
                            **amounts,
                            "extracted_judgment_data": json.dumps(result),
                            "raw_judgment_text": result.get("raw_text", ""),
                        }
                        db.update_judgment_data(case_number, payload)
                        db.mark_step_complete(case_number, "needs_judgment_extraction")
                        extracted_count += 1
                except Exception as e:
                    logger.exception(f"Failed to process judgment: {e}")

        logger.success(f"Downloaded {downloaded_count} PDFs, extracted data from {extracted_count} Final Judgments")

    # =========================================================================
    # STEP 3: BULK DATA ENRICHMENT
    # Enrich parcels table from bulk_parcels (owner, legal description, etc.)
    # This MUST run before ORI ingestion which needs legal descriptions
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: BULK DATA ENRICHMENT")
    logger.info("=" * 60)

    if start_step > 3:
        logger.info("Skipping step 3 (start_step > 3)")
    else:
        try:
            enrichment_stats = enrich_auctions_from_bulk()
            logger.success(f"Bulk enrichment: {enrichment_stats}")
        except Exception as e:
            logger.error(f"Bulk enrichment failed: {e}")

        # Also update legal descriptions from Final Judgment extractions
        # (more authoritative than bulk data for specific properties)
        try:
            auctions_with_judgment = db.execute_query(
                """SELECT parcel_id, extracted_judgment_data FROM auctions
                   WHERE parcel_id IS NOT NULL AND extracted_judgment_data IS NOT NULL"""
            )
            for row in auctions_with_judgment:
                folio = row["parcel_id"]
                try:
                    judgment_data = json.loads(row["extracted_judgment_data"])
                    legal_desc = judgment_data.get("legal_description")
                    if legal_desc:
                        conn = db.connect()
                        # Only update if better than what we have (judgment > bulk)
                        conn.execute("""
                            UPDATE parcels SET
                                judgment_legal_description = ?,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE folio = ?
                        """, [legal_desc, folio])
                        logger.debug(f"Updated judgment legal for {folio}")
                except Exception as exc:
                    logger.debug(f"Could not update judgment legal for {folio}: {exc}")
        except Exception as e:
            logger.warning(f"Could not update judgment legal descriptions: {e}")

    # =========================================================================
    # STEP 4: HCPA GIS - Sales History & Property Details
    # Skip if: folio already has sales_history records
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 4: HCPA GIS - SALES HISTORY")
    logger.info("=" * 60)

    if start_step > 4:
        logger.info("Skipping step 4 (start_step > 4)")
    else:
        try:
            pending = db.execute_query(
                """SELECT DISTINCT parcel_id, case_number FROM auctions
                   WHERE needs_hcpa_enrichment = TRUE AND parcel_id IS NOT NULL"""
            )
        except Exception:
            pending = []

        gis_count = 0
        gis_errors = 0
        max_consecutive_errors = 5  # Skip GIS if website appears down
        gis_limit = 10  # Limit GIS lookups per run to avoid timeouts

        for row in pending:
            # Stop if we hit the GIS limit
            if gis_count >= gis_limit:
                logger.info(f"Reached GIS limit ({gis_limit}), moving on...")
                break

            # Skip GIS entirely if website appears down
            if gis_errors >= max_consecutive_errors:
                logger.warning(f"HCPA GIS website appears down ({gis_errors} consecutive failures), skipping remaining GIS lookups")
                break

            folio = row["parcel_id"]
            case_number = row["case_number"]
            if not folio:
                continue

            # Skip if already has sales history
            # (Double check in case flags were manual)
            if db.folio_has_sales_history(folio):
                logger.debug(f"Skipping GIS for {folio} - already has sales history")
                db.mark_step_complete(case_number, "needs_hcpa_enrichment")
                continue

            logger.info(f"Fetching HCPA GIS for {folio}...")
            try:
                # scrape_hcpa_property is an async function with timeout protection
                # Use parcel_id parameter for direct URL access (more reliable than search)
                result = await scrape_hcpa_property(parcel_id=folio, timeout_seconds=90)

                # Check for timeout/error in result
                if result and result.get("error"):
                    logger.warning(f"GIS scrape returned error for {folio}: {result['error']}")
                    gis_errors += 1
                    # Add a small delay after errors to let system stabilize
                    await asyncio.sleep(2)
                    continue

                if result and result.get("sales_history"):
                    db.save_sales_history(
                        folio, result.get("strap", ""), result["sales_history"]
                    )
                    gis_count += 1
                    gis_errors = 0  # Reset error count on success
                    # Mark ALL auctions with this folio as complete (avoid duplicate scrapes)
                    db.mark_step_complete_by_folio(folio, "needs_hcpa_enrichment")

                # Save legal description for ORI search in Step 4
                if result and result.get("legal_description"):
                    conn = db.connect()
                    conn.execute("""
                        ALTER TABLE parcels ADD COLUMN IF NOT EXISTS legal_description VARCHAR
                    """)
                    conn.execute("""
                        INSERT OR IGNORE INTO parcels (folio) VALUES (?)
                    """, [folio])
                    conn.execute("""
                        UPDATE parcels SET legal_description = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE folio = ?
                    """, [result["legal_description"], folio])
                    logger.info(f"Saved legal description for {folio}")

                # Small delay between successful scrapes to avoid overwhelming the server
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"GIS scrape failed for {folio}: {e}")
                gis_errors += 1
                # Add a delay after errors to let system stabilize
                await asyncio.sleep(3)

        logger.success(f"Scraped GIS data for {gis_count} properties")

    # =========================================================================
    # STEP 5: Ingest ORI Data & Build Chain of Title
    # Skip if: folio has chain data AND last_analyzed_case_number = current case
    # PRIORITY: Use HCPA-scraped legal_description (clean subdivision format)
    # FALLBACK: Skip property if no HCPA legal description (mark for manual review)
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 5: ORI INGESTION & CHAIN OF TITLE")
    logger.info("=" * 60)

    ingestion_service = IngestionService()

    # Ensure hcpa_scrape_failed column exists for tracking failures
    try:
        conn = db.connect()
        conn.execute("ALTER TABLE auctions ADD COLUMN IF NOT EXISTS hcpa_scrape_failed BOOLEAN DEFAULT FALSE")
        conn.execute("ALTER TABLE auctions ADD COLUMN IF NOT EXISTS hcpa_scrape_error VARCHAR")
    except Exception as e:
        logger.warning(f"Could not add hcpa tracking columns: {e}")

    try:
        pending_auctions = db.execute_query(
            """SELECT * FROM auctions
               WHERE needs_ori_ingestion = TRUE AND parcel_id IS NOT NULL"""
        )
    except Exception as e:
        logger.error(f"Failed to fetch auctions: {e}")
        pending_auctions = []

    ingested_count = 0
    invalid_folio_count = 0
    party_search_count = 0
    no_hcpa_legal_count = 0

    for row in pending_auctions:
        folio = row.get("parcel_id")
        case_number = row.get("case_number")
        if not folio or not case_number:
            continue

        # Validate folio is a real parcel ID (not "Property Appraiser" or similar garbage)
        if not is_valid_folio(folio):
            # Try party-based search as fallback for invalid folios
            plaintiff = row.get("plaintiff")
            defendant = row.get("defendant")

            if plaintiff or defendant:
                logger.info(f"Invalid folio '{folio}' for case {case_number}, trying party-based ORI search")
                try:
                    prop = Property(
                        case_number=case_number,
                        parcel_id=folio,  # Keep original for reference
                        address=row.get("property_address"),
                        auction_date=row.get("auction_date"),
                        plaintiff=plaintiff,
                        defendant=defendant,
                    )
                    ingestion_service.ingest_property_by_party(prop, plaintiff, defendant)
                    party_search_count += 1
                    db.mark_step_complete(case_number, "needs_ori_ingestion")
                except Exception as e:
                    logger.error(f"Party-based ingestion failed for {case_number}: {e}")
            else:
                logger.warning(f"Invalid folio '{folio}' for case {case_number}, no party data for fallback")
                invalid_folio_count += 1
                # Can't do anything, mark complete so we don't retry forever
                db.mark_step_complete(case_number, "needs_ori_ingestion")
            continue

        # Skip logic: folio has chain AND same case number
        last_case = db.get_last_analyzed_case(folio)
        if db.folio_has_chain_of_title(folio) and last_case == case_number:
            logger.debug(f"Skipping ORI for {folio} - already analyzed for {case_number}")
            db.mark_step_complete(case_number, "needs_ori_ingestion")
            continue

        # Get legal description - PRIORITIZE HCPA-scraped legal_description
        # This is the clean subdivision format like "LORENE TERRACE LOT 11 BLOCK B"
        # NOT the raw_legal1..4 fields which contain metes and bounds measurements
        hcpa_legal_desc = None
        judgment_legal = None

        try:
            parcel_data = db.connect().execute(
                """SELECT legal_description, judgment_legal_description
                   FROM parcels WHERE folio = ?""", [folio]
            ).fetchone()
            if parcel_data:
                hcpa_legal_desc = parcel_data[0]  # HCPA-scraped clean legal description
                judgment_legal = parcel_data[1]   # From final judgment extraction
        except Exception as exc:
            logger.debug(f"Failed to load legal descriptions for {folio}: {exc}")

        # Use HCPA legal description as primary source, fall back to:
        # - Judgment legal (if present)
        # - Bulk parcel raw_legal fields (metes/bounds or platted)
        primary_legal = hcpa_legal_desc or judgment_legal

        legal_source = "HCPA" if hcpa_legal_desc else ("JUDGMENT" if judgment_legal else None)

        if not primary_legal:
            # Fallback to bulk_parcels raw legal fields when HCPA scrape didn't populate parcels.legal_description.
            try:
                bp = db.connect().execute(
                    """
                    SELECT raw_legal1, raw_legal2, raw_legal3, raw_legal4
                    FROM bulk_parcels
                    WHERE strap = ?
                    """,
                    [folio],
                ).fetchone()
                if bp:
                    from src.utils.legal_description import combine_legal_fields

                    primary_legal = combine_legal_fields(bp[0], bp[1], bp[2], bp[3])
                    if primary_legal:
                        legal_source = "BULK_RAW_LEGAL"
            except Exception as exc:
                logger.debug(f"Failed to load bulk_parcels raw_legal for {folio}: {exc}")

        if not primary_legal:
            # No legal description available anywhere - mark for manual review and skip
            logger.warning(f"No usable legal description for {folio} (case {case_number}), marking for manual review")
            try:
                conn = db.connect()
                conn.execute(
                    """
                    UPDATE auctions SET
                        hcpa_scrape_failed = TRUE,
                        hcpa_scrape_error = 'No usable legal description (HCPA/judgment/bulk)'
                    WHERE case_number = ?
                    """,
                    [case_number],
                )
            except Exception as exc:
                logger.debug(f"Failed to mark missing legal for {case_number}: {exc}")
            no_hcpa_legal_count += 1
            # Mark complete so we don't loop forever; this requires manual intervention.
            db.mark_step_complete(case_number, "needs_ori_ingestion")
            continue

        # Build ORI-optimized search terms using the legal description utilities
        # The key insight: ORI indexes documents with LOT/BLOCK FIRST
        # So "L 44 B 2 SYMPHONY*" finds the specific lot
        # While "SYMPHONY ISLES" returns random lots from the subdivision
        from src.utils.legal_description import parse_legal_description, generate_search_permutations

        parsed_legal = parse_legal_description(primary_legal)
        search_terms = generate_search_permutations(parsed_legal)

        # Add filter info for post-search filtering (for browser-based searches
        # that might return broader results)
        lot_filter = parsed_legal.lots or ([parsed_legal.lot] if parsed_legal.lot else None)
        if lot_filter or parsed_legal.block:
            filter_info = {
                "lot": lot_filter,
                "block": parsed_legal.block,
                "subdivision": parsed_legal.subdivision,
                "require_all_lots": isinstance(lot_filter, list) and len(lot_filter) > 1,
            }
            search_terms.append(("__filter__", filter_info))

        # Fallback if no search terms generated: use a longer prefix to keep metes-and-bounds searches specific.
        if not search_terms or (len(search_terms) == 1 and isinstance(search_terms[0], tuple)):
            prefix = primary_legal.upper().strip()[:60]
            if prefix:
                search_terms.insert(0, f"{prefix}*")

        logger.info(f"Ingesting ORI for {folio} (case {case_number})...")
        logger.info(f"  Legal ({legal_source}): {primary_legal}")
        logger.info(f"  Search terms: {search_terms}")

        try:
            prop = Property(
                case_number=case_number,
                parcel_id=folio,
                address=row.get("property_address"),
                auction_date=row.get("auction_date"),
                legal_description=primary_legal,
            )
            # Pass search terms to ingestion service
            prop.legal_search_terms = search_terms
            ingestion_service.ingest_property(prop)
            ingested_count += 1
            db.mark_step_complete(case_number, "needs_ori_ingestion")
        except Exception as e:
            logger.error(f"Ingestion failed for {case_number}: {e}")

    logger.success(f"Ingested ORI data for {ingested_count} properties")
    if party_search_count > 0:
        logger.info(f"  Party-based searches: {party_search_count}")
    if invalid_folio_count > 0:
        logger.info(f"  Skipped (invalid folio, no party data): {invalid_folio_count}")
    if no_hcpa_legal_count > 0:
        logger.warning(f"  Skipped (no HCPA legal desc, needs manual review): {no_hcpa_legal_count}")

    # =========================================================================
    # STEP 6: Analyze Lien Survival
    # Skip if: folio has survival_status AND last_analyzed_case_number = current case
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 6: LIEN SURVIVAL ANALYSIS")
    logger.info("=" * 60)

    survival_analyzer = LienSurvivalAnalyzer()

    try:
        auctions_to_analyze = db.execute_query(
            """SELECT * FROM auctions
               WHERE needs_lien_survival = TRUE AND parcel_id IS NOT NULL"""
        )
    except Exception:
        auctions_to_analyze = []

    analyzed_count = 0
    for auction in auctions_to_analyze:
        folio = auction.get("parcel_id")
        case_number = auction.get("case_number")
        if not folio or not case_number:
            continue

        # Skip logic
        last_case = db.get_last_analyzed_case(folio)
        if db.folio_has_survival_analysis(folio) and last_case == case_number:
            logger.debug(f"Skipping survival for {folio} - already analyzed for {case_number}")
            db.mark_step_complete(case_number, "needs_lien_survival")
            continue

        with logger.contextualize(folio=folio, case_number=case_number, step="lien_survival"):
            logger.info("Analyzing lien survival...")

            # Fetch encumbrances with full details for new analyzer
            try:
                encs_rows = db.execute_query(
                    f"""SELECT id, encumbrance_type, recording_date, creditor, debtor,
                               amount, instrument, book, page, is_satisfied
                        FROM encumbrances
                        WHERE folio = '{folio}'"""
                )
            except Exception as e:
                logger.exception(f"Failed to fetch encumbrances: {e}")
                continue

            # Build encumbrances list for new analyzer
            encumbrances = []
            enc_id_map = {}  # Map instrument/recording_date combo to DB id

            for row in encs_rows:
                rec_date = None
                if row["recording_date"]:
                    with contextlib.suppress(ValueError):
                        rec_date = datetime.strptime(
                            str(row["recording_date"]), "%Y-%m-%d"
                        ).date()

                enc = {
                    "encumbrance_type": row["encumbrance_type"],
                    "recording_date": rec_date,
                    "creditor": row.get("creditor"),
                    "debtor": row.get("debtor"),
                    "amount": row["amount"],
                    "instrument": row.get("instrument"),
                    "book": row.get("book"),
                    "page": row.get("page"),
                    "is_satisfied": row.get("is_satisfied", False),
                }
                encumbrances.append(enc)
                # Use instrument as key, fall back to recording_date + type
                key = row.get("instrument") or f"{rec_date}_{row['encumbrance_type']}"
                enc_id_map[key] = row["id"]

            # Get current owner acquisition date from chain_of_title
            current_owner_acquisition_date = None
            try:
                chain_rows = db.execute_query(
                    f"""SELECT acquisition_date
                        FROM chain_of_title
                        WHERE folio = '{folio}'
                        ORDER BY acquisition_date DESC
                        LIMIT 1"""
                )
                if chain_rows and chain_rows[0].get("acquisition_date"):
                    acq_str = str(chain_rows[0]["acquisition_date"])
                    with contextlib.suppress(ValueError):
                        current_owner_acquisition_date = datetime.strptime(
                            acq_str, "%Y-%m-%d"
                        ).date()
            except Exception as e:
                logger.warning(f"Failed to get chain_of_title: {e}")

            # Get judgment data for lis pendens date
            judgment_data = {}
            if auction.get("extracted_judgment_data"):
                with contextlib.suppress(json.JSONDecodeError):
                    judgment_data = json.loads(auction["extracted_judgment_data"])

            # Create foreclosing mortgage encumbrance from judgment data if not already present
            foreclosed_mtg = judgment_data.get("foreclosed_mortgage", {})
            mtg_book = foreclosed_mtg.get("recording_book")
            mtg_page = foreclosed_mtg.get("recording_page")
            
            # Prepare foreclosing refs for analyzer
            foreclosing_refs = {
                "instrument": foreclosed_mtg.get("instrument_number"),
                "book": mtg_book,
                "page": mtg_page
            }
            
            if mtg_book and mtg_page and not db.encumbrance_exists(folio, mtg_book, mtg_page):
                # Use book/page to fetch full document details from ORI (bypasses 25-result limit)
                mtg_instrument = foreclosed_mtg.get("instrument_number")
                mtg_record_date = foreclosed_mtg.get("recording_date")
                if not mtg_instrument:
                    try:
                        ori_scraper = ORIScraper()
                        ori_results = ori_scraper.search_by_book_page_sync(mtg_book, mtg_page)
                        if ori_results:
                            # Get first result with mortgage-related doc type
                            for ori_doc in ori_results:
                                doc_type = ori_doc.get("ORI - Doc Type", "")
                                if "MTG" in doc_type or "MORTGAGE" in doc_type.upper():
                                    mtg_instrument = ori_doc.get("Instrument #")
                                    if not mtg_record_date:
                                        mtg_record_date = ori_doc.get("Recording Date Time", "").split()[0] if ori_doc.get("Recording Date Time") else None
                                    logger.info(f"  Found mortgage instrument {mtg_instrument} via book/page lookup")
                                    break
                            # If no mortgage type found, use first result
                            if not mtg_instrument and ori_results:
                                mtg_instrument = ori_results[0].get("Instrument #")
                                if not mtg_record_date:
                                    mtg_record_date = ori_results[0].get("Recording Date Time", "").split()[0] if ori_results[0].get("Recording Date Time") else None
                                logger.info(f"  Found instrument {mtg_instrument} via book/page lookup")
                    except Exception as e:
                        logger.warning(f"  Failed to lookup mortgage by book/page: {e}")
                
                # Update refs with found instrument if available
                if mtg_instrument:
                    foreclosing_refs["instrument"] = mtg_instrument

                # Get amount from judgment - principal_amount or original_amount
                mtg_amount = (
                    judgment_data.get("principal_amount")
                    or foreclosed_mtg.get("original_amount")
                    or None
                )
                # Get creditor from plaintiff
                mtg_creditor = auction.get("plaintiff")

                enc_id = db.insert_encumbrance(
                    folio=folio,
                    encumbrance_type="(MTG) MORTGAGE",
                    creditor=mtg_creditor,
                    amount=mtg_amount,
                    recording_date=mtg_record_date,
                    book=mtg_book,
                    page=mtg_page,
                    instrument=mtg_instrument,
                    survival_status="FORECLOSING",
                )
                logger.info(f"  Created foreclosing mortgage encumbrance (id={enc_id}) from judgment data")

                # Add to encumbrances list for survival analysis
                encumbrances.append({
                    "id": enc_id,
                    "type": "(MTG) MORTGAGE",
                    "creditor": mtg_creditor,
                    "amount": mtg_amount,
                    "recording_date": mtg_record_date,
                    "book": mtg_book,
                    "page": mtg_page,
                    "instrument": mtg_instrument,
                })
                # Add to ID map
                enc_id_map[mtg_instrument or f"{mtg_record_date}_(MTG) MORTGAGE"] = enc_id

            lis_pendens_date = None
            lp_str = judgment_data.get("lis_pendens_date")
            if lp_str:
                with contextlib.suppress(ValueError):
                    lis_pendens_date = datetime.strptime(lp_str, "%Y-%m-%d").date()

            # Get plaintiff from auction
            plaintiff = auction.get("plaintiff")

            # Extract defendant names list from judgment data for "Joined" check
            defendant_names = []
            if judgment_data:
                # Try explicit defendants list first
                defs = judgment_data.get("defendants", [])
                if isinstance(defs, list):
                    defendant_names = [d.get("name") for d in defs if d.get("name")]
                
                # Fallback to single string split if list missing
                if not defendant_names and judgment_data.get("defendant"):
                    defendant_names = [judgment_data.get("defendant")]

            # Run the new analyzer
            survival_result = survival_analyzer.analyze(
                encumbrances=encumbrances,
                foreclosure_type=auction.get("foreclosure_type")
                or judgment_data.get("foreclosure_type"),
                lis_pendens_date=lis_pendens_date,
                current_owner_acquisition_date=current_owner_acquisition_date,
                plaintiff=plaintiff,
                original_mortgage_amount=auction.get("original_mortgage_amount"),
                foreclosing_refs=foreclosing_refs,
                defendants=defendant_names or None,
            )

            # Update survival status for each category
            results = survival_result.get("results", {})

            # Process each status category
            status_mapping = {
                "survived": "SURVIVED",
                "extinguished": "EXTINGUISHED",
                "expired": "EXPIRED",
                "satisfied": "SATISFIED",
                "historical": "HISTORICAL",
                "foreclosing": "FORECLOSING",
            }

            for category, status in status_mapping.items():
                for enc in results.get(category, []):
                    # Find the DB id using instrument or recording_date + type
                    key = enc.get("instrument") or f"{enc.get('recording_date')}_{enc.get('type')}"
                    db_id = enc_id_map.get(key)
                    if db_id:
                        kwargs = {}
                        if enc.get("is_joined") is not None:
                            kwargs["is_joined"] = enc.get("is_joined")
                        if enc.get("is_inferred"):
                            kwargs["is_inferred"] = True
                        db.update_encumbrance_survival(db_id, status, **kwargs)

            # Mark as analyzed and record case number
            db.mark_as_analyzed(case_number)
            db.set_last_analyzed_case(folio, case_number)
            db.mark_step_complete(case_number, "needs_lien_survival")
            analyzed_count += 1

            summary = survival_result.get("summary", {})
            logger.info(
                f"  Survived: {summary.get('survived_count', 0)}, "
                f"Extinguished: {summary.get('extinguished_count', 0)}, "
                f"Historical: {summary.get('historical_count', 0)}, "
                f"Foreclosing: {summary.get('foreclosing_count', 0)}"
            )

    logger.success(f"Analyzed {analyzed_count} properties")

    # =========================================================================
    # STEP 7: Sunbiz - Business Entity Lookup
    # Only if: party name is LLC/Corp/Trust
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 7: SUNBIZ ENTITY LOOKUP")
    logger.info("=" * 60)

    sunbiz_scraper = SunbizScraper(headless=True)

    try:
        # Get parties from auctions (plaintiff/defendant) and chain
        auctions_with_parties = db.execute_query(
            """SELECT DISTINCT parcel_id, case_number, plaintiff, defendant FROM auctions
               WHERE needs_sunbiz_search = TRUE AND parcel_id IS NOT NULL"""
        )
    except Exception:
        auctions_with_parties = []

    sunbiz_count = 0
    for row in auctions_with_parties:
        folio = row["parcel_id"]
        case_number = row["case_number"]
        if not folio:
            continue

        # Skip if already has sunbiz data
        if db.folio_has_sunbiz_data(folio):
            logger.debug(f"Skipping Sunbiz for {folio} - already has data")
            db.mark_step_complete(case_number, "needs_sunbiz_search")
            continue

        # Check if plaintiff or defendant is an entity
        parties_to_check = []
        for party in [row.get("plaintiff"), row.get("defendant")]:
            if party and is_entity_name(party):
                parties_to_check.append(party)

        if not parties_to_check:
            # No entities to check, mark complete
            db.mark_step_complete(case_number, "needs_sunbiz_search")
            continue

        logger.info(f"Looking up entities for {folio}: {parties_to_check}")
        try:
            for party_name in parties_to_check[:2]:  # Limit to 2 per property
                await sunbiz_scraper.search_for_property(folio, party_name)
            sunbiz_count += 1
            db.mark_step_complete(case_number, "needs_sunbiz_search")
        except Exception as e:
            logger.error(f"Sunbiz lookup failed for {folio}: {e}")

    logger.success(f"Sunbiz lookups for {sunbiz_count} properties")

    # =========================================================================
    # STEP 8: Scrape Building Permits
    # Skip if: folio has permit data
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 8: BUILDING PERMITS")
    logger.info("=" * 60)

    permit_scraper = PermitScraper()

    try:
        auctions_for_permits = db.execute_query(
            """SELECT DISTINCT parcel_id, case_number, property_address FROM auctions
               WHERE needs_permit_check = TRUE AND parcel_id IS NOT NULL AND property_address IS NOT NULL"""
        )
    except Exception:
        auctions_for_permits = []

    permit_count = 0
    for row in auctions_for_permits:
        folio = row["parcel_id"]
        case_number = row["case_number"]
        address = row["property_address"]
        if not folio or not address:
            continue

        # Skip if already has permits
        if db.folio_has_permits(folio):
            logger.debug(f"Skipping permits for {folio} - already has data")
            db.mark_step_complete(case_number, "needs_permit_check")
            continue

        logger.info(f"Scraping permits for {folio}...")
        try:
            # Parse city from address and extract street address only for search
            # Full address format: "3101 E 29TH AVE, TAMPA, FL- 33610"
            # Accela search works best with just street: "3101 E 29TH AVE"
            parts = address.split(",")
            street_address = parts[0].strip() if parts else address
            city = parts[1].strip() if len(parts) > 1 else "Tampa"
            permits = await permit_scraper.get_permits(street_address, city)
            
            if permits:
                # Fetch ORI docs for NOC linking
                try:
                    ori_docs = db.execute_query(
                        "SELECT document_type as doc_type, recording_date as record_date, instrument_number as instrument FROM documents WHERE folio = ?", 
                        [folio]
                    )
                    # Helper to format date for linker if needed
                    for d in ori_docs:
                        if isinstance(d['record_date'], (date, datetime)):
                             d['record_date'] = d['record_date'].strftime("%m/%d/%Y")
                    
                    permits = link_permits_to_nocs(permits, ori_docs)
                except Exception as ex:
                    logger.warning(f"Failed to link NOCs for {folio}: {ex}")

                conn = db.connect()
                for p in permits:
                    conn.execute(
                        """INSERT OR IGNORE INTO permits
                           (folio, permit_number, issue_date, status, permit_type,
                            description, contractor, estimated_cost, url, noc_instrument)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        [
                            folio,
                            p.permit_number,
                            p.issue_date,
                            p.status,
                            p.permit_type,
                            p.description,
                            p.contractor,
                            p.estimated_cost,
                            p.url,
                            p.noc_instrument
                        ],
                    )
                permit_count += 1
            db.mark_step_complete(case_number, "needs_permit_check")
        except Exception as e:
            logger.error(f"Permit scrape failed for {folio}: {e}")

    logger.success(f"Scraped permits for {permit_count} properties")

    # =========================================================================
    # STEP 9: FEMA Flood Zone Lookup
    # Skip if: folio has flood data
    # Requires: lat/lon coordinates
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 9: FEMA FLOOD ZONE")
    logger.info("=" * 60)

    flood_checker = FEMAFloodChecker()

    try:
        parcels_with_coords = db.execute_query(
            """SELECT a.case_number, p.folio, p.latitude, p.longitude 
               FROM auctions a
               JOIN parcels p ON a.parcel_id = p.folio
               WHERE a.needs_flood_check = TRUE
               AND p.latitude IS NOT NULL AND p.longitude IS NOT NULL"""
        )
    except Exception:
        parcels_with_coords = []

    flood_count = 0
    for row in parcels_with_coords:
        folio = row["folio"]
        case_number = row["case_number"]
        lat = row["latitude"]
        lon = row["longitude"]

        if not folio or not lat or not lon:
            continue

        # Skip if already has flood data
        if db.folio_has_flood_data(folio):
            logger.debug(f"Skipping flood for {folio} - already has data")
            db.mark_step_complete(case_number, "needs_flood_check")
            continue

        logger.info(f"Looking up flood zone for {folio}...")
        try:
            result = flood_checker.get_flood_zone_for_property(folio, lat, lon)
            if result:
                db.save_flood_data(
                    folio, result.flood_zone, result.risk_level, result.insurance_required
                )
                flood_count += 1
                db.mark_step_complete(case_number, "needs_flood_check")
        except Exception as e:
            logger.error(f"Flood lookup failed for {folio}: {e}")

    logger.success(f"Flood zone lookups for {flood_count} properties")

    # =========================================================================
    # STEP 10: Market Data - Zillow (ALWAYS REFRESH)
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 10: MARKET DATA - ZILLOW")
    logger.info("=" * 60)

    market_scraper = MarketScraper(headless=False)

    try:
        auctions_for_market = db.execute_query(
            """SELECT DISTINCT parcel_id, case_number, property_address FROM auctions
               WHERE needs_market_data = TRUE AND parcel_id IS NOT NULL AND property_address IS NOT NULL
               LIMIT ?""",
            (max_auctions,),
        )
    except Exception:
        auctions_for_market = []

    market_count = 0
    for auction in auctions_for_market:
        folio = auction.get("parcel_id")
        case_number = auction.get("case_number")
        address_str = auction.get("property_address")
        if not address_str or not folio:
            continue

        # Parse address
        try:
            parts = address_str.split(",")
            if len(parts) >= 3:
                street = parts[0].strip()
                city = parts[1].strip()
                state_zip = parts[2].strip().split(" ")
                state = state_zip[0]
                zip_code = state_zip[1] if len(state_zip) > 1 else ""
            else:
                continue

            logger.info(f"Fetching Zillow data for {address_str}...")
            listing = await market_scraper.get_listing_details(
                street, city, state, zip_code, property_id=folio
            )

            if listing:
                data = {
                    "price": listing.price,
                    "listing_status": listing.status,
                    "zestimate": listing.estimates.get("Zillow"),
                    "rent_zestimate": listing.estimates.get("Rent Zestimate"),
                }
                db.save_market_data(
                    folio=folio,
                    source="Zillow",
                    data=data,
                    screenshot_path=listing.screenshot_path,
                )
                market_count += 1
                # Mark step complete only if we get data
                # But wait, Realtor also needs to run. 
                # Let's not mark complete here, wait for Realtor step.
                # Or better: mark individually if we have granular flags, 
                # but we have one flag for "market data". 
                # Decision: Mark complete after BOTH run, or check if both exist.
                # For simplicity, we'll mark complete at end of Step 11.

        except Exception as e:
            logger.error(f"Zillow scrape failed for {folio}: {e}")

    logger.success(f"Zillow data for {market_count} properties")

    # =========================================================================
    # STEP 11: Market Data - Realtor.com
    # Skip if: folio has realtor data (7-day cache)
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 11: MARKET DATA - REALTOR.COM")
    logger.info("=" * 60)

    realtor_scraper = RealtorScraper(headless=False)

    realtor_count = 0
    for auction in auctions_for_market:
        folio = auction.get("parcel_id")
        case_number = auction.get("case_number")
        address_str = auction.get("property_address")
        if not address_str or not folio:
            continue

        # Skip if already has realtor data
        if db.folio_has_realtor_data(folio):
            logger.debug(f"Skipping Realtor for {folio} - already has data")
            db.mark_step_complete(case_number, "needs_market_data")
            continue

        try:
            parts = address_str.split(",")
            if len(parts) >= 3:
                street = parts[0].strip()
                city = parts[1].strip()
                state_zip = parts[2].strip().split(" ")
                state = state_zip[0]
                zip_code = state_zip[1] if len(state_zip) > 1 else ""
            else:
                continue

            logger.info(f"Fetching Realtor data for {address_str}...")
            listing = await realtor_scraper.get_listing_for_property(
                folio, street, city, state, zip_code
            )

            if listing:
                data = {
                    "list_price": listing.list_price,
                    "listing_status": listing.listing_status,
                    "hoa_fee": listing.hoa_fee,
                    "hoa_frequency": listing.hoa_frequency,
                    "days_on_market": listing.days_on_market,
                }
                db.save_market_data(
                    folio=folio,
                    source="Realtor",
                    data=data,
                    screenshot_path=listing.screenshot_path,
                )
                realtor_count += 1
                db.mark_step_complete(case_number, "needs_market_data")

        except Exception as e:
            logger.error(f"Realtor scrape failed for {folio}: {e}")

    logger.success(f"Realtor data for {realtor_count} properties")

    # =========================================================================
    # STEP 12: Property Enrichment - HCPA (Fallback for missing data)
    # Skip if: folio has owner_name (bulk enrichment already ran in Step 3)
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 12: PROPERTY ENRICHMENT (HCPA FALLBACK)")
    logger.info("=" * 60)

    hcpa_scraper = HCPAScraper()

    try:
        auctions_need_enrichment = db.execute_query(
            """SELECT DISTINCT a.parcel_id, a.property_address, a.case_number
               FROM auctions a
               LEFT JOIN parcels p ON a.parcel_id = p.folio
               WHERE a.parcel_id IS NOT NULL
               AND (p.owner_name IS NULL OR p.folio IS NULL)"""
        )
    except Exception:
        auctions_need_enrichment = []

    enriched_count = 0
    for row in auctions_need_enrichment:
        folio = row["parcel_id"]
        address = row["property_address"]
        case_number = row["case_number"]

        logger.info(f"Enriching {folio}...")

        prop = Property(parcel_id=folio, address=address, case_number=case_number)

        try:
            enriched_prop = await hcpa_scraper.enrich_property(prop)
            if enriched_prop.owner_name or enriched_prop.year_built:
                logger.success(f"  Enriched: {enriched_prop.owner_name}")
                db.upsert_parcel(enriched_prop)
                enriched_count += 1
            else:
                logger.warning(f"  No enrichment data found")
        except Exception as e:
            logger.error(f"  Enrichment error: {e}")

    logger.success(f"Enriched {enriched_count} properties")

    # =========================================================================
    # STEP 13: Tax Payment Status
    # Skip if: folio has tax data
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 13: TAX PAYMENT STATUS")
    logger.info("=" * 60)

    tax_scraper = TaxScraper()

    try:
        auctions_for_tax = db.execute_query(
            """SELECT DISTINCT a.parcel_id, a.case_number, p.property_address
               FROM auctions a
               LEFT JOIN parcels p ON a.parcel_id = p.folio
               WHERE a.needs_tax_check = TRUE AND a.parcel_id IS NOT NULL"""
        )
    except Exception:
        auctions_for_tax = []

    tax_count = 0
    for row in auctions_for_tax:
        folio = row["parcel_id"]
        case_number = row["case_number"]
        property_address = row.get("property_address")
        if not folio:
            continue

        # Skip if already has tax data
        if db.folio_has_tax_data(folio):
            logger.debug(f"Skipping tax for {folio} - already has data")
            db.mark_step_complete(case_number, "needs_tax_check")
            continue

        # Skip if no address - can't search tax collector without it
        if not property_address:
            logger.warning(f"Skipping tax check for {folio} - no property address available")
            continue

        logger.info(f"Checking tax status for {folio} ({property_address})...")
        try:
            liens = await tax_scraper.get_tax_liens(folio, property_address)
            if liens:
                for lien in liens:
                    db.save_liens(folio, [lien])
                tax_count += 1
            # Mark complete even if no liens found (it's a valid check)
            db.mark_step_complete(case_number, "needs_tax_check")
        except Exception as e:
            logger.error(f"Tax check failed for {folio}: {e}")

        logger.success(f"Tax checks for {tax_count} properties")
    
    # =========================================================================
    # STEP 14: HomeHarvest Enrichment
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 14: HOMEHARVEST ENRICHMENT")
    logger.info("=" * 60)
    
    if start_step > 14:
        logger.info("Skipping step 14 (start_step > 14)")
    else:
        hh_service = HomeHarvestService()
        # Process properties that need enrichment, filtered by auction_date
        hh_props = hh_service.get_pending_properties(limit=max_auctions, auction_date=start_date)
    
        if hh_props:
            logger.info(f"Found {len(hh_props)} properties for HomeHarvest enrichment.")
            for prop_data in hh_props:
                folio = prop_data["folio"]
                case_number = prop_data["case_number"]
                try:
                    hh_service._process_single_property(folio, prop_data["location"])  # noqa: SLF001
                    db.mark_step_complete(case_number, "needs_homeharvest_enrichment")
                except Exception as e:
                    logger.error(f"HomeHarvest enrichment failed for {folio}: {e}")
            logger.success(f"Enriched {len(hh_props)} properties with HomeHarvest data.")
        else:
            logger.info("No properties found needing HomeHarvest enrichment.")
    
    # =========================================================================
    # DONE
    # =========================================================================
    # =========================================================================
    # STEP 15: Geocode Missing Parcel Coordinates (Nominatim, cached)
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 15: GEOCODE MISSING PARCELS")
    logger.info("=" * 60)

    if start_step > 15:
        logger.info("Skipping step 15 (start_step > 15)")
    elif not geocode_missing_parcels:
        logger.info("Skipping step 15 (disabled)")
    else:
        from src.services.geocoder import geocode_address

        db.ensure_geocode_columns()

        query = """
            SELECT DISTINCT
                p.folio,
                p.property_address,
                p.city,
                p.zip_code
            FROM parcels p
            JOIN auctions a ON a.parcel_id = p.folio
            WHERE (p.latitude IS NULL OR p.longitude IS NULL)
              AND p.property_address IS NOT NULL
              AND p.property_address != ''
              AND a.auction_date >= ?
              AND a.auction_date <= ?
        """
        params: list[object] = [start_date, end_date]
        if geocode_limit is not None:
            query += " LIMIT ?"
            params.append(geocode_limit)

        try:
            rows = db.execute_query(query, params)
        except Exception as exc:
            logger.error(f"Failed to query parcels needing geocode: {exc}")
            rows = []

        logger.info(f"Found {len(rows)} parcels needing geocode")
        updated = 0
        for row in rows:
            folio = row.get("folio")
            address = (row.get("property_address") or "").strip()
            if not folio or not address:
                continue

            # Check if address already contains state abbreviation (FL)
            # Pattern: ", FL" or ", FL-" or ", FL " typically indicates full address
            if re.search(r',\s*FL[\s\-]', address, re.IGNORECASE):
                # Address already has city/state/zip - normalize "FL-" to "FL " and use as-is
                full_address = re.sub(r'FL-\s*', 'FL ', address)
            else:
                # Address needs city/state/zip appended
                city = (row.get("city") or "Tampa").strip()
                zip_code = (row.get("zip_code") or "").strip()
                full_address = f"{address}, {city}, FL {zip_code}".strip()

            coords = geocode_address(full_address)
            if not coords:
                continue

            lat, lon = coords
            db.update_parcel_coordinates(str(folio), lat, lon)
            updated += 1
            logger.info(f"Geocoded {folio}: ({lat}, {lon})")

        logger.success(f"Geocoded {updated}/{len(rows)} parcels")

    logger.info("\n" + "=" * 60)
    logger.success("PIPELINE COMPLETE")
    logger.info("=" * 60)

if __name__ == "__main__":
    asyncio.run(run_full_pipeline(max_auctions=5))
