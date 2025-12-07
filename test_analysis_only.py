"""
Test script to run ONLY Steps 4 & 5 (ORI Ingestion + Lien Survival) on existing auction data.
Skips all scraping and judgment extraction.
"""
import asyncio
import sys
from pathlib import Path
from loguru import logger

# Configure logging
logger.remove()
logger.add(sys.stderr, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}")

# Add parent to path
if str(Path(__file__).parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent))

from src.db.operations import PropertyDB
from src.services.ingestion_service import IngestionService
from src.services.lien_survival_analyzer import LienSurvivalAnalyzer
from src.models.property import Property, Lien
from datetime import datetime, date
import json
import contextlib

async def main():
    """Run Steps 4 & 5 on first auction only."""
    logger.info("=" * 60)
    logger.info("TESTING ORI INGESTION + LIEN SURVIVAL (Steps 4 & 5)")
    logger.info("=" * 60)

    db = PropertyDB()
    db.create_chain_tables()

    # Get specific auction that we have legal description for
    conn = db.connect()
    auction = conn.execute("""
        SELECT * FROM auctions
        WHERE parcel_id = '2029132AX000004000710U'
        LIMIT 1
    """).fetchone()

    if not auction:
        logger.error("No auctions found in database")
        return

    columns = [desc[0] for desc in conn.description]
    row = dict(zip(columns, auction))

    folio = row["parcel_id"]
    case_number = row["case_number"]

    logger.info(f"Testing with: {case_number} | {folio}")

    # =========================================================================
    # STEP 3: HCPA GIS - Get Legal Description
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: HCPA GIS - LEGAL DESCRIPTION")
    logger.info("=" * 60)

    from src.scrapers.hcpa_gis_scraper import scrape_hcpa_property

    # Check if already has legal description
    legal_desc = None
    try:
        parcel_data = conn.execute(
            "SELECT legal_description FROM parcels WHERE folio = ?", [folio]
        ).fetchone()
        if parcel_data:
            legal_desc = parcel_data[0]
    except Exception:
        pass

    if not legal_desc:
        logger.info(f"Fetching legal description for {folio} from HCPA GIS...")
        try:
            result = scrape_hcpa_property(folio=folio)
            if result and result.get("legal_description"):
                legal_desc = result["legal_description"]
                # Save to database
                conn.execute("ALTER TABLE parcels ADD COLUMN IF NOT EXISTS legal_description VARCHAR")
                conn.execute("INSERT OR IGNORE INTO parcels (folio) VALUES (?)", [folio])
                conn.execute("""
                    UPDATE parcels SET legal_description = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE folio = ?
                """, [legal_desc, folio])
                logger.success(f"✓ Saved legal description: {legal_desc[:100]}...")
            else:
                logger.warning("No legal description found")
        except Exception as e:
            logger.error(f"Failed to get legal description: {e}")
    else:
        logger.info(f"✓ Legal description already in DB: {legal_desc[:100]}...")

    if not legal_desc:
        logger.error("Cannot proceed without legal description")
        return

    # =========================================================================
    # STEP 4: ORI INGESTION
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 4: ORI INGESTION & CHAIN OF TITLE")
    logger.info("=" * 60)

    ingestion_service = IngestionService()

    # Launch browser in main async context
    await ingestion_service.ori_scraper._ensure_browser(headless=False)
    logger.info("✓ Browser launched and ready")

    try:
        prop = Property(
            case_number=case_number,
            parcel_id=folio,
            address=row.get("property_address"),
            auction_date=row.get("auction_date"),
            legal_description=legal_desc,
        )
        logger.info(f"Ingesting ORI for {folio} (case {case_number})...")

        # Search ORI directly using the browser
        search_term = legal_desc[:60] if legal_desc else None
        if search_term:
            logger.info(f"Searching ORI for: {search_term}")
            docs = await ingestion_service.ori_scraper.search_by_legal_browser(search_term, headless=False)
            logger.info(f"Found {len(docs)} documents")

        logger.success(f"✓ ORI search complete")
    except Exception as e:
        logger.error(f"✗ Ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        return

    # Verify chain data was saved
    chain_count = conn.execute("SELECT COUNT(*) FROM chain_of_title WHERE folio = ?", [folio]).fetchone()[0]
    enc_count = conn.execute("SELECT COUNT(*) FROM encumbrances WHERE folio = ?", [folio]).fetchone()[0]
    logger.info(f"Chain records: {chain_count} | Encumbrances: {enc_count}")

    if chain_count == 0 and enc_count == 0:
        logger.warning("No chain or encumbrance data saved!")
        # Don't return - keep browser alive for inspection
        logger.info("\n" + "=" * 60)
        logger.info("Browser is still running. Waiting 120 seconds for inspection...")
        logger.info("=" * 60)
        await asyncio.sleep(120)
        return

    # =========================================================================
    # STEP 5: LIEN SURVIVAL ANALYSIS
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 5: LIEN SURVIVAL ANALYSIS")
    logger.info("=" * 60)

    survival_analyzer = LienSurvivalAnalyzer()

    try:
        encs_rows = conn.execute(f"""
            SELECT id, encumbrance_type, recording_date, book, page, amount
            FROM encumbrances
            WHERE folio = '{folio}' AND is_satisfied = FALSE
        """).fetchall()

        logger.info(f"Found {len(encs_rows)} unsatisfied encumbrances")

        if len(encs_rows) == 0:
            logger.warning("No unsatisfied encumbrances to analyze")
            db.mark_as_analyzed(case_number)
            logger.success("✓ Marked as analyzed")
            return

        liens_for_analysis = []
        lien_id_map = {}

        for row_data in encs_rows:
            rec_date = None
            if row_data[2]:  # recording_date
                with contextlib.suppress(ValueError):
                    rec_date = datetime.strptime(str(row_data[2]), "%Y-%m-%d").date()

            lien_obj = Lien(
                document_type=row_data[1],  # encumbrance_type
                recording_date=rec_date or date.min,
                amount=row_data[5],  # amount
                book=row_data[3],    # book
                page=row_data[4],    # page
            )
            liens_for_analysis.append(lien_obj)
            lien_id_map[id(lien_obj)] = row_data[0]  # id

        # Get judgment data for lis pendens date
        judgment_data = {}
        if row.get("extracted_judgment_data"):
            with contextlib.suppress(json.JSONDecodeError):
                judgment_data = json.loads(row["extracted_judgment_data"])

        lis_pendens_date = None
        lp_str = judgment_data.get("lis_pendens_date")
        if lp_str:
            with contextlib.suppress(ValueError):
                lis_pendens_date = datetime.strptime(lp_str, "%Y-%m-%d").date()

        logger.info(f"Analyzing liens with lis_pendens_date={lis_pendens_date}")

        survival_result = survival_analyzer.analyze(
            liens=liens_for_analysis,
            foreclosure_type=row.get("foreclosure_type") or judgment_data.get("foreclosure_type"),
            lis_pendens_date=lis_pendens_date,
            original_mortgage_amount=row.get("original_mortgage_amount"),
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

        for row_data in encs_rows:
            lid = row_data[0]
            if lid not in surviving_ids and lid not in expired_ids:
                db.update_encumbrance_survival(lid, "WIPED_OUT")

        # Mark as analyzed
        db.mark_as_analyzed(case_number)
        db.set_last_analyzed_case(folio, case_number)

        logger.success(f"✓ Analysis complete!")
        logger.info(f"  Surviving: {len(survival_result['surviving_liens'])}")
        logger.info(f"  Wiped out: {len(encs_rows) - len(survival_result['surviving_liens'])}")

    except Exception as e:
        logger.error(f"✗ Survival analysis failed: {e}")
        import traceback
        traceback.print_exc()

    # =========================================================================
    # VERIFY DATABASE
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("DATABASE VERIFICATION")
    logger.info("=" * 60)

    chain_count = conn.execute("SELECT COUNT(*) FROM chain_of_title").fetchone()[0]
    enc_count = conn.execute("SELECT COUNT(*) FROM encumbrances").fetchone()[0]
    enc_with_status = conn.execute("SELECT COUNT(*) FROM encumbrances WHERE survival_status IS NOT NULL").fetchone()[0]

    logger.info(f"Total chain_of_title: {chain_count}")
    logger.info(f"Total encumbrances: {enc_count}")
    logger.info(f"Encumbrances with survival_status: {enc_with_status}")

    if chain_count > 0 and enc_count > 0 and enc_with_status > 0:
        logger.success("✓✓✓ ALL ANALYSIS COMPLETE ✓✓✓")
    else:
        logger.error("✗✗✗ ANALYSIS INCOMPLETE ✗✗✗")

    # Keep browser alive for inspection
    logger.info("\n" + "=" * 60)
    logger.info("Browser is still running. Waiting 120 seconds for inspection...")
    logger.info("=" * 60)
    await asyncio.sleep(120)

if __name__ == "__main__":
    asyncio.run(main())
