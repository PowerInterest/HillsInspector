"""
Run just Step 5 (ORI Ingestion & Chain of Title) from the pipeline.

This script processes properties that need chain of title building:
- Properties with valid folios
- Properties with legal descriptions (from judgment extraction or bulk data)
- Skips properties that already have up-to-date chain data
"""

import asyncio
import sys
from pathlib import Path
from loguru import logger

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db.operations import PropertyDB
from src.services.ingestion_service import IngestionService
from src.scrapers.ori_api_scraper import ORIApiScraper
from src.utils.legal_description import build_ori_search_terms
from src.models.property import Property
from src.pipeline import is_valid_folio


# Configure logging
logger.remove()
logger.add(
    sys.stderr,
    level="INFO",
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
)
logger.add(
    "logs/hills_inspector.log",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{function}:{line} - {message}",
)


def run_step_5(max_properties: int = 100, analyze_pdfs: bool = False):
    """
    Run Step 5: ORI Ingestion & Chain of Title.

    Args:
        max_properties: Maximum number of properties to process
        analyze_pdfs: Whether to download and analyze PDFs with vision service
    """
    logger.info("=" * 60)
    logger.info("STEP 5: ORI INGESTION & CHAIN OF TITLE (with fixes)")
    logger.info("=" * 60)

    db = PropertyDB()
    db.connect()

    # Get auctions that have valid parcel IDs
    auctions = db.conn.execute("""
        SELECT
            a.case_number,
            a.parcel_id,
            p.raw_legal1,
            p.raw_legal2,
            p.raw_legal3,
            p.raw_legal4,
            p.last_analyzed_case_number
        FROM auctions a
        LEFT JOIN parcels p ON a.parcel_id = p.folio
        WHERE a.parcel_id IS NOT NULL
        ORDER BY a.auction_date ASC
    """).fetchall()

    logger.info(f"Found {len(auctions)} auctions with parcel IDs")

    # Filter to properties needing ingestion
    to_process = []
    for row in auctions:
        case_number, folio, legal1, legal2, legal3, legal4, last_case = row

        # Skip invalid folios
        if not is_valid_folio(folio):
            logger.warning(f"Invalid folio '{folio}' for case {case_number}, skipping")
            continue

        # Skip if already analyzed for this case
        if last_case == case_number:
            continue

        # Need a legal description to search ORI
        if not any([legal1, legal2, legal3, legal4]):
            logger.debug(f"No legal description for {folio}, skipping")
            continue

        # Build search terms
        search_terms = build_ori_search_terms(folio, legal1, legal2, legal3, legal4)
        if not search_terms:
            logger.debug(f"Could not build search terms for {folio}, skipping")
            continue

        to_process.append({
            'case_number': case_number,
            'folio': folio,
            'search_terms': search_terms,
            'legal': f"{legal1} {legal2} {legal3} {legal4}".strip(),
        })

    logger.info(f"Found {len(to_process)} properties needing ORI ingestion")

    if not to_process:
        logger.info("No properties to process")
        return

    # Limit to max_properties
    to_process = to_process[:max_properties]
    logger.info(f"Processing {len(to_process)} properties (limit: {max_properties})")

    # Create shared ORI scraper and ingestion service
    ori_scraper = ORIApiScraper()
    ingestion_service = IngestionService(ori_scraper=ori_scraper, analyze_pdfs=analyze_pdfs)

    processed = 0
    errors = 0

    try:
        for item in to_process:
            case_number = item['case_number']
            folio = item['folio']
            search_terms = item['search_terms']

            logger.info(f"Ingesting ORI for {folio} (case {case_number})...")
            logger.info(f"  Search terms: {search_terms[:3]}")  # Show first 3 terms

            try:
                # Build Property object
                prop = Property(
                    case_number=case_number,
                    parcel_id=folio,
                    legal_description=item['legal'],
                    address="",  # Required field but not used for ORI search
                )
                prop.legal_search_terms = search_terms

                # Run ingestion (browser search has its own 60s timeout)
                ingestion_service.ingest_property(prop)

                # Update last_analyzed_case_number in parcels table
                db.conn.execute("""
                    UPDATE parcels
                    SET last_analyzed_case_number = ?
                    WHERE folio = ?
                """, [case_number, folio])
                db.conn.commit()

                processed += 1
                logger.success(f"Processed {processed}/{len(to_process)}: {folio}")

            except Exception as e:
                errors += 1
                logger.error(f"Failed to process {folio}: {e}")
                continue

    finally:
        # Clean up browser
        if ori_scraper.browser:
            asyncio.run(ori_scraper.close_browser())

    logger.info("=" * 60)
    logger.success(f"Step 5 complete: {processed} processed, {errors} errors")
    logger.info("=" * 60)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run Step 5: ORI Ingestion")
    parser.add_argument("--max", type=int, default=100, help="Max properties to process")
    parser.add_argument("--analyze-pdfs", action="store_true", help="Download and analyze PDFs with vision")

    args = parser.parse_args()

    run_step_5(max_properties=args.max, analyze_pdfs=args.analyze_pdfs)
