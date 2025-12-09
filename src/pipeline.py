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
from src.services.final_judgment_processor import FinalJudgmentProcessor
from src.services.lien_survival_analyzer import LienSurvivalAnalyzer
from src.services.ingestion_service import IngestionService
from src.services.scraper_storage import ScraperStorage
from src.ingest.bulk_parcel_ingest import enrich_auctions_from_bulk
from src.utils.legal_description import build_ori_search_terms
from src.services.data_linker import link_permits_to_nocs
from src.db.operations import PropertyDB
from src.models.property import Lien, Property
from playwright.async_api import async_playwright


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
            "SELECT COUNT(*) FROM encumbrances WHERE folio = ? AND survival_status IS NOT NULL",
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
                except Exception:
                    pass

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


async def run_full_pipeline(max_auctions: int = 10, property_limit: Optional[int] = None):
    """Run the complete property analysis pipeline with smart skip logic.

    Args:
        max_auctions: Limit applied to later market data steps.
        property_limit: Optional cap on total auctions ingested (foreclosure + tax deed).
    """

    logger.info("=" * 60)
    logger.info("STARTING FULL PIPELINE")
    logger.info("=" * 60)

    db = PipelineDB()
    db.create_chain_tables()
    db.ensure_last_analyzed_column()

    today = datetime.now(UTC).date()
    start_date = today
    end_date = start_date + timedelta(days=60)
    if property_limit:
        end_date = start_date  # constrain debug runs to the nearest auction day

    # =========================================================================
    # STEP 1: Scrape Foreclosure Auctions (Calendar-based skip logic)
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1: SCRAPING FORECLOSURE AUCTIONS")
    logger.info("=" * 60)

    foreclosure_scraper = AuctionScraper()

    try:
        logger.info(f"Scraping foreclosures from {start_date} to {end_date}...")
        # The scraper handles calendar logic internally via scrape_all
        properties = await foreclosure_scraper.scrape_all(start_date, end_date, max_properties=property_limit)
        logger.success(f"Scraped {len(properties)} foreclosure auctions")

        # Save to DB
        for p in properties:
            db.upsert_auction(p)
        logger.success(f"Saved {len(properties)} foreclosure auctions to DB")
    except Exception as e:
        logger.error(f"Foreclosure scrape failed: {e}")
        properties = []

    # =========================================================================
    # STEP 1.5: Scrape Tax Deed Auctions
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1.5: SCRAPING TAX DEED AUCTIONS")
    logger.info("=" * 60)

    tax_properties: list[Property] = []
    if property_limit and len(properties) >= property_limit:
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

    judgment_processor = FinalJudgmentProcessor()
    storage = ScraperStorage()

    # Invalid parcel_id values that should be treated as missing
    INVALID_PARCEL_IDS = {"property appraiser", "n/a", "none", ""}

    try:
        auctions = db.execute_query(
            "SELECT * FROM auctions WHERE extracted_judgment_data IS NULL"
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
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Could not update judgment legal descriptions: {e}")

    # =========================================================================
    # STEP 4: HCPA GIS - Sales History & Property Details
    # Skip if: folio already has sales_history records
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 4: HCPA GIS - SALES HISTORY")
    logger.info("=" * 60)

    try:
        pending = db.execute_query(
            """SELECT DISTINCT parcel_id, case_number FROM auctions
               WHERE parcel_id IS NOT NULL"""
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
        if not folio:
            continue

        # Skip if already has sales history
        if db.folio_has_sales_history(folio):
            logger.debug(f"Skipping GIS for {folio} - already has sales history")
            continue

        logger.info(f"Fetching HCPA GIS for {folio}...")
        try:
            # scrape_hcpa_property is an async function
            result = await scrape_hcpa_property(folio=folio)
            if result and result.get("sales_history"):
                db.save_sales_history(
                    folio, result.get("strap", ""), result["sales_history"]
                )
                gis_count += 1
                gis_errors = 0  # Reset error count on success

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
        except Exception as e:
            logger.error(f"GIS scrape failed for {folio}: {e}")
            gis_errors += 1

    logger.success(f"Scraped GIS data for {gis_count} properties")

    # =========================================================================
    # STEP 5: Ingest ORI Data & Build Chain of Title
    # Skip if: folio has chain data AND last_analyzed_case_number = current case
    # Uses legal description permutations for robust ORI search
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 5: ORI INGESTION & CHAIN OF TITLE")
    logger.info("=" * 60)

    ingestion_service = IngestionService()

    try:
        pending_auctions = db.execute_query(
            """SELECT * FROM auctions
               WHERE parcel_id IS NOT NULL"""
        )
    except Exception as e:
        logger.error(f"Failed to fetch auctions: {e}")
        pending_auctions = []

    ingested_count = 0
    for row in pending_auctions:
        folio = row.get("parcel_id")
        case_number = row.get("case_number")
        if not folio or not case_number:
            continue

        # Skip logic: folio has chain AND same case number
        last_case = db.get_last_analyzed_case(folio)
        if db.folio_has_chain_of_title(folio) and last_case == case_number:
            logger.debug(f"Skipping ORI for {folio} - already analyzed for {case_number}")
            continue

        # Get legal descriptions from multiple sources for permutation search
        legal_desc = None
        judgment_legal = None
        raw_legal1 = None
        raw_legal2 = None
        raw_legal3 = None
        raw_legal4 = None

        try:
            parcel_data = db.connect().execute(
                """SELECT legal_description, judgment_legal_description,
                          raw_legal1, raw_legal2, raw_legal3, raw_legal4
                   FROM parcels WHERE folio = ?""", [folio]
            ).fetchone()
            if parcel_data:
                legal_desc = parcel_data[0]
                judgment_legal = parcel_data[1]
                raw_legal1 = parcel_data[2]
                raw_legal2 = parcel_data[3]
                raw_legal3 = parcel_data[4]
                raw_legal4 = parcel_data[5]
        except Exception:
            pass

        # Build search terms using permutation logic
        search_terms = build_ori_search_terms(
            folio=folio,
            legal1=raw_legal1,
            legal2=raw_legal2,
            legal3=raw_legal3,
            legal4=raw_legal4,
            judgment_legal=judgment_legal or legal_desc,
        )

        if not search_terms:
            logger.warning(f"No legal description available for {folio}, skipping ORI")
            continue

        logger.info(f"Ingesting ORI for {folio} (case {case_number})...")
        logger.debug(f"  Search terms: {search_terms[:3]}...")

        try:
            prop = Property(
                case_number=case_number,
                parcel_id=folio,
                address=row.get("property_address"),
                auction_date=row.get("auction_date"),
                legal_description=judgment_legal or legal_desc,
            )
            # Pass search terms to ingestion service for permutation search
            prop.legal_search_terms = search_terms
            ingestion_service.ingest_property(prop)
            ingested_count += 1
        except Exception as e:
            logger.error(f"Ingestion failed for {case_number}: {e}")

    logger.success(f"Ingested ORI data for {ingested_count} properties")

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
               WHERE parcel_id IS NOT NULL"""
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
            continue

        with logger.contextualize(folio=folio, case_number=case_number, step="lien_survival"):
            logger.info("Analyzing lien survival...")

            try:
                encs_rows = db.execute_query(
                    f"""SELECT id, encumbrance_type, recording_date, book, page, amount
                        FROM encumbrances
                        WHERE folio = '{folio}' AND is_satisfied = FALSE"""
                )
            except Exception as e:
                logger.exception(f"Failed to fetch encumbrances: {e}")
                continue

            liens_for_analysis = []
            lien_id_map = {}

            for row in encs_rows:
                rec_date = None
                if row["recording_date"]:
                    with contextlib.suppress(ValueError):
                        rec_date = datetime.strptime(
                            str(row["recording_date"]), "%Y-%m-%d"
                        ).date()

                lien_obj = Lien(
                    document_type=row["encumbrance_type"],
                    recording_date=rec_date or date.min,
                    amount=row["amount"],
                    book=row["book"],
                    page=row["page"],
                )
                liens_for_analysis.append(lien_obj)
                lien_id_map[id(lien_obj)] = row["id"]

            # Get judgment data for lis pendens date
            judgment_data = {}
            if auction.get("extracted_judgment_data"):
                with contextlib.suppress(json.JSONDecodeError):
                    judgment_data = json.loads(auction["extracted_judgment_data"])

            lis_pendens_date = None
            lp_str = judgment_data.get("lis_pendens_date")
            if lp_str:
                with contextlib.suppress(ValueError):
                    lis_pendens_date = datetime.strptime(lp_str, "%Y-%m-%d").date()

            survival_result = survival_analyzer.analyze(
                liens=liens_for_analysis,
                foreclosure_type=auction.get("foreclosure_type")
                or judgment_data.get("foreclosure_type"),
                lis_pendens_date=lis_pendens_date,
                original_mortgage_amount=auction.get("original_mortgage_amount"),
            )

            # Update survival status
            surviving_ids = []
            expired_ids = []

            for surviving_lien in survival_result["surviving_liens"]:
                lid = lien_id_map.get(id(surviving_lien))
                if lid:
                    db.update_encumbrance_survival(lid, "SURVIVED")
                    surviving_ids.append(lid)

            for expired_lien in survival_result.get("expired_liens", []):
                lid = lien_id_map.get(id(expired_lien))
                if lid:
                    db.update_encumbrance_survival(lid, "EXPIRED")
                    expired_ids.append(lid)

            for row in encs_rows:
                lid = row["id"]
                if lid not in surviving_ids and lid not in expired_ids:
                    db.update_encumbrance_survival(lid, "WIPED_OUT")

            # Mark as analyzed and record case number
            db.mark_as_analyzed(case_number)
            db.set_last_analyzed_case(folio, case_number)
            analyzed_count += 1

            logger.info(f"  Surviving: {len(survival_result['surviving_liens'])}")

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
            """SELECT DISTINCT parcel_id, plaintiff, defendant FROM auctions
               WHERE parcel_id IS NOT NULL"""
        )
    except Exception:
        auctions_with_parties = []

    sunbiz_count = 0
    for row in auctions_with_parties:
        folio = row["parcel_id"]
        if not folio:
            continue

        # Skip if already has sunbiz data
        if db.folio_has_sunbiz_data(folio):
            logger.debug(f"Skipping Sunbiz for {folio} - already has data")
            continue

        # Check if plaintiff or defendant is an entity
        parties_to_check = []
        for party in [row.get("plaintiff"), row.get("defendant")]:
            if party and is_entity_name(party):
                parties_to_check.append(party)

        if not parties_to_check:
            continue

        logger.info(f"Looking up entities for {folio}: {parties_to_check}")
        try:
            for party_name in parties_to_check[:2]:  # Limit to 2 per property
                await sunbiz_scraper.search_for_property(folio, party_name)
            sunbiz_count += 1
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
            """SELECT DISTINCT parcel_id, property_address FROM auctions
               WHERE parcel_id IS NOT NULL AND property_address IS NOT NULL"""
        )
    except Exception:
        auctions_for_permits = []

    permit_count = 0
    for row in auctions_for_permits:
        folio = row["parcel_id"]
        address = row["property_address"]
        if not folio or not address:
            continue

        # Skip if already has permits
        if db.folio_has_permits(folio):
            logger.debug(f"Skipping permits for {folio} - already has data")
            continue

        logger.info(f"Scraping permits for {folio}...")
        try:
            # Parse city from address
            parts = address.split(",")
            city = parts[1].strip() if len(parts) > 1 else "Tampa"
            permits = await permit_scraper.get_permits(address, city)
            
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
            """SELECT folio, latitude, longitude FROM parcels
               WHERE latitude IS NOT NULL AND longitude IS NOT NULL"""
        )
    except Exception:
        parcels_with_coords = []

    flood_count = 0
    for row in parcels_with_coords:
        folio = row["folio"]
        lat = row["latitude"]
        lon = row["longitude"]

        if not folio or not lat or not lon:
            continue

        # Skip if already has flood data
        if db.folio_has_flood_data(folio):
            logger.debug(f"Skipping flood for {folio} - already has data")
            continue

        logger.info(f"Looking up flood zone for {folio}...")
        try:
            result = flood_checker.get_flood_zone_for_property(folio, lat, lon)
            if result:
                db.save_flood_data(
                    folio, result.flood_zone, result.risk_level, result.insurance_required
                )
                flood_count += 1
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
            """SELECT DISTINCT parcel_id, property_address FROM auctions
               WHERE parcel_id IS NOT NULL AND property_address IS NOT NULL
               LIMIT ?""",
            (max_auctions,),
        )
    except Exception:
        auctions_for_market = []

    market_count = 0
    for auction in auctions_for_market:
        folio = auction.get("parcel_id")
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
        address_str = auction.get("property_address")
        if not address_str or not folio:
            continue

        # Skip if already has realtor data
        if db.folio_has_realtor_data(folio):
            logger.debug(f"Skipping Realtor for {folio} - already has data")
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
            """SELECT DISTINCT parcel_id FROM auctions
               WHERE parcel_id IS NOT NULL"""
        )
    except Exception:
        auctions_for_tax = []

    tax_count = 0
    for row in auctions_for_tax:
        folio = row["parcel_id"]
        if not folio:
            continue

        # Skip if already has tax data
        if db.folio_has_tax_data(folio):
            logger.debug(f"Skipping tax for {folio} - already has data")
            continue

        logger.info(f"Checking tax status for {folio}...")
        try:
            liens = await tax_scraper.get_tax_liens(folio)
            if liens:
                for lien in liens:
                    db.save_liens(folio, [lien])
                tax_count += 1
        except Exception as e:
            logger.error(f"Tax check failed for {folio}: {e}")

    logger.success(f"Tax checks for {tax_count} properties")

    # =========================================================================
    # DONE
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.success("PIPELINE COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(run_full_pipeline(max_auctions=5))
