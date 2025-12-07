"""
Batch process all properties that need chain of title analysis.
Skips properties that already have chain data unless --force is used.
"""
import asyncio
import sys
import duckdb
from datetime import datetime
from loguru import logger
from pathlib import Path

from src.db.operations import PropertyDB
from src.services.ingestion_service import IngestionService
from src.models.property import Property
from src.scrapers.ori_api_scraper import ORIApiScraper
from src.utils.legal_description import build_ori_search_terms

# Configure logging
logger.remove()
logger.add(sys.stderr, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}")
logger.add("logs/batch_run_{time}.log", level="DEBUG", rotation="100 MB")

async def get_properties_to_process(force: bool = False) -> list:
    """Get list of properties that need processing."""
    conn = duckdb.connect('data/property_master.db', read_only=True)

    if force:
        # Get all unique folios from auctions
        query = """
            SELECT DISTINCT a.folio, a.case_number, p.legal_description, p.address
            FROM auctions a
            LEFT JOIN parcels p ON a.folio = p.folio
            WHERE a.folio IS NOT NULL
            ORDER BY a.auction_date DESC
        """
    else:
        # Get folios that don't have chain of title yet
        query = """
            SELECT DISTINCT a.folio, a.case_number, p.legal_description, p.address
            FROM auctions a
            LEFT JOIN parcels p ON a.folio = p.folio
            LEFT JOIN chain_of_title c ON a.folio = c.folio
            WHERE a.folio IS NOT NULL AND c.folio IS NULL
            ORDER BY a.auction_date DESC
        """

    results = conn.execute(query).fetchall()
    conn.close()

    properties = []
    for row in results:
        folio, case_number, legal_desc, address = row
        if folio and legal_desc:
            properties.append({
                'folio': folio,
                'case_number': case_number or f'BATCH-{folio}',
                'legal_description': legal_desc,
                'address': address or ''
            })

    return properties

async def process_property(prop_data: dict, ori_scraper: ORIApiScraper,
                          ingestion_service: IngestionService) -> dict:
    """Process a single property through the pipeline."""
    folio = prop_data['folio']
    result = {
        'folio': folio,
        'success': False,
        'error': None,
        'docs_found': 0,
        'chain_periods': 0,
        'search_term': None
    }

    try:
        # Build search terms from legal description
        search_terms = build_ori_search_terms(prop_data['legal_description'])

        if not search_terms:
            result['error'] = 'No search terms generated from legal description'
            return result

        # Search ORI for documents
        docs = []
        successful_term = None

        for term in search_terms:
            logger.debug(f"Trying search term: {term}")
            try:
                docs = ori_scraper.search_by_legal_sync(term, headless=True)
                if docs:
                    successful_term = term
                    break
            except Exception as e:
                logger.debug(f"Search failed for '{term}': {e}")
                continue

        if not docs:
            result['error'] = f'No documents found with terms: {search_terms}'
            return result

        result['search_term'] = successful_term
        result['docs_found'] = len(docs)

        # Create Property object
        prop = Property(
            case_number=prop_data['case_number'],
            parcel_id=folio,
            legal_description=prop_data['legal_description'],
            address=prop_data['address']
        )
        prop.legal_search_terms = [successful_term]

        # Ingest with pre-fetched documents
        ingestion_service.ingest_property(prop, raw_docs=docs)

        result['success'] = True

        # Get chain period count
        conn = duckdb.connect('data/property_master.db', read_only=True)
        count = conn.execute(
            "SELECT COUNT(*) FROM chain_of_title WHERE folio = ?", [folio]
        ).fetchone()[0]
        conn.close()
        result['chain_periods'] = count

    except Exception as e:
        result['error'] = str(e)
        logger.error(f"Error processing {folio}: {e}")

    return result

async def run_batch(force: bool = False, limit: int = None):
    """Run the batch processing pipeline."""
    start_time = datetime.now()

    logger.info("=" * 70)
    logger.info("BATCH PROPERTY PROCESSING")
    logger.info("=" * 70)

    # Get properties to process
    properties = await get_properties_to_process(force)

    if limit:
        properties = properties[:limit]

    logger.info(f"Found {len(properties)} properties to process")

    if not properties:
        logger.info("No properties need processing")
        return

    # Initialize services (reuse browser across properties)
    ori_scraper = ORIApiScraper()
    ingestion_service = IngestionService()

    results = {
        'success': [],
        'failed': [],
        'no_docs': [],
        'no_search_terms': []
    }

    for i, prop_data in enumerate(properties, 1):
        folio = prop_data['folio']
        logger.info(f"\n[{i}/{len(properties)}] Processing {folio}")
        logger.info(f"  Address: {prop_data.get('address', 'N/A')}")
        logger.info(f"  Legal: {prop_data['legal_description'][:60]}...")

        result = await process_property(prop_data, ori_scraper, ingestion_service)

        if result['success']:
            results['success'].append(result)
            logger.success(f"  SUCCESS: {result['docs_found']} docs -> {result['chain_periods']} chain periods")
            logger.info(f"  Search: {result['search_term']}")
        elif result['error'] and 'No search terms' in result['error']:
            results['no_search_terms'].append(result)
            logger.warning(f"  SKIP: {result['error']}")
        elif result['error'] and 'No documents found' in result['error']:
            results['no_docs'].append(result)
            logger.warning(f"  SKIP: No ORI documents found")
        else:
            results['failed'].append(result)
            logger.error(f"  FAILED: {result['error']}")

    # Summary
    elapsed = datetime.now() - start_time

    logger.info("\n" + "=" * 70)
    logger.info("BATCH PROCESSING COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Total processed: {len(properties)}")
    logger.info(f"Successful: {len(results['success'])} ({100*len(results['success'])/len(properties):.1f}%)")
    logger.info(f"Failed: {len(results['failed'])}")
    logger.info(f"No ORI docs: {len(results['no_docs'])}")
    logger.info(f"No search terms: {len(results['no_search_terms'])}")
    logger.info(f"Elapsed time: {elapsed}")

    if results['failed']:
        logger.info("\nFailed properties:")
        for r in results['failed']:
            logger.info(f"  {r['folio']}: {r['error']}")

    if results['no_docs']:
        logger.info("\nProperties with no ORI documents:")
        for r in results['no_docs'][:10]:  # Show first 10
            logger.info(f"  {r['folio']}")
        if len(results['no_docs']) > 10:
            logger.info(f"  ... and {len(results['no_docs']) - 10} more")

    return results

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Batch process properties")
    parser.add_argument("--force", action="store_true", help="Reprocess all properties")
    parser.add_argument("--limit", type=int, help="Limit number of properties to process")

    args = parser.parse_args()

    asyncio.run(run_batch(force=args.force, limit=args.limit))
