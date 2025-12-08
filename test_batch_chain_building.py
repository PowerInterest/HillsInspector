"""
Batch test script to verify chain building fix across properties.
"""

import asyncio
import sys
from datetime import datetime
from loguru import logger

# Configure logging
logger.remove()
logger.add(sys.stdout, level="INFO", format="{time:HH:mm:ss} | {level:<7} | {message}")

from src.services.ingestion_service import IngestionService
from src.scrapers.ori_api_scraper import ORIApiScraper
from src.models.property import Property
from src.db.operations import PropertyDB
from src.utils.legal_description import build_ori_search_terms
import duckdb


def get_test_properties(limit: int = 20):
    """Get properties with legal descriptions for testing."""
    conn = duckdb.connect('data/property_master.db', read_only=True)

    # Get properties that have legal descriptions but no chain of title yet
    result = conn.execute('''
        SELECT
            a.case_number,
            a.parcel_id as folio,
            a.property_address,
            p.legal_description,
            p.judgment_legal_description,
            p.raw_legal1,
            p.raw_legal2,
            p.raw_legal3,
            p.raw_legal4
        FROM auctions a
        JOIN parcels p ON a.parcel_id = p.folio
        LEFT JOIN chain_of_title c ON a.parcel_id = c.folio
        WHERE p.legal_description IS NOT NULL
        AND c.folio IS NULL
        AND a.parcel_id NOT IN ('Property Appraiser', 'MULTIPLE PARCEL', 'MULTIPLEPARCEL')
        LIMIT ?
    ''', [limit]).fetchall()

    conn.close()

    properties = []
    for r in result:
        properties.append({
            'case_number': r[0],
            'folio': r[1],
            'address': r[2],
            'legal_description': r[3],
            'judgment_legal': r[4],
            'raw_legal1': r[5],
            'raw_legal2': r[6],
            'raw_legal3': r[7],
            'raw_legal4': r[8],
        })

    return properties


async def test_single_property(service: IngestionService, prop_data: dict, scraper: ORIApiScraper) -> dict:
    """Test chain building for a single property."""
    folio = prop_data['folio']
    case_number = prop_data['case_number']

    result = {
        'folio': folio,
        'case_number': case_number,
        'address': prop_data['address'],
        'success': False,
        'search_term': None,
        'raw_records': 0,
        'grouped_docs': 0,
        'chain_periods': 0,
        'encumbrances': 0,
        'error': None,
    }

    try:
        # Build search terms
        search_terms = build_ori_search_terms(
            folio=folio,
            legal1=prop_data['raw_legal1'],
            legal2=prop_data['raw_legal2'],
            legal3=prop_data['raw_legal3'],
            legal4=prop_data['raw_legal4'],
            judgment_legal=prop_data['judgment_legal'] or prop_data['legal_description'],
        )

        if not search_terms:
            result['error'] = "No search terms generated"
            return result

        # Try each search term
        raw_docs = []
        successful_term = None

        for term in search_terms[:5]:  # Limit to first 5 terms
            try:
                docs = await scraper.search_by_legal_browser(term, headless=True)
                if docs and len(docs) < 500:  # Skip overly broad results
                    raw_docs = docs
                    successful_term = term
                    break
                elif docs and len(docs) >= 500:
                    logger.warning(f"  Skipping term '{term}' - too many results ({len(docs)})")
            except Exception as e:
                continue

        result['search_term'] = successful_term
        result['raw_records'] = len(raw_docs)

        if not raw_docs:
            result['error'] = f"No documents found with terms: {search_terms[:5]}"
            return result

        # Group by instrument (for reporting)
        grouped = service._group_ori_records_by_instrument(raw_docs)
        result['grouped_docs'] = len(grouped)

        # Create property and ingest - PASS THE RAW DOCS to avoid re-searching!
        prop = Property(
            case_number=case_number,
            parcel_id=folio,
            address=prop_data['address'],
            legal_description=prop_data['judgment_legal'] or prop_data['legal_description'],
        )
        prop.legal_search_terms = [successful_term]

        # Pass raw_docs to avoid duplicate browser search
        service.ingest_property(prop, raw_docs=raw_docs)

        # Check results in DB
        db = PropertyDB()
        conn = db.connect()

        chain_count = conn.execute(
            'SELECT COUNT(*) FROM chain_of_title WHERE folio = ?', [folio]
        ).fetchone()[0]

        enc_count = conn.execute(
            'SELECT COUNT(*) FROM encumbrances WHERE folio = ?', [folio]
        ).fetchone()[0]

        result['chain_periods'] = chain_count
        result['encumbrances'] = enc_count
        result['success'] = chain_count > 0 or enc_count > 0

        # If no chain or encumbrances, set descriptive error
        if not result['success'] and result['raw_records'] > 0:
            result['error'] = f"Found {result['raw_records']} raw records ({result['grouped_docs']} grouped) but 0 deeds/encumbrances"

    except Exception as e:
        result['error'] = str(e)

    return result


async def run_batch_test(num_properties: int = 20):
    """Run batch test on multiple properties."""
    print("=" * 80)
    print(f"BATCH CHAIN BUILDING TEST - {num_properties} PROPERTIES")
    print("=" * 80)
    print()

    # Get test properties
    properties = get_test_properties(num_properties)
    print(f"Found {len(properties)} properties to test")
    print()

    if not properties:
        print("No properties available for testing!")
        return

    # Initialize services
    service = IngestionService()
    scraper = ORIApiScraper()

    results = []

    for i, prop_data in enumerate(properties):
        print(f"[{i+1}/{len(properties)}] Testing {prop_data['folio']}...")
        print(f"         Address: {prop_data['address']}")
        legal_preview = (prop_data['legal_description'] or '')[:50]
        print(f"         Legal: {legal_preview}...")

        result = await test_single_property(service, prop_data, scraper)
        results.append(result)

        if result['success']:
            print(f"         ✅ SUCCESS: {result['chain_periods']} chain periods, {result['encumbrances']} encumbrances")
            print(f"            Search: {result['search_term']} ({result['raw_records']} raw → {result['grouped_docs']} docs)")
        else:
            print(f"         ❌ FAILED: {result['error']}")
        print()

    # Close scraper
    await scraper.close_browser()

    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)

    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]

    print(f"Total tested: {len(results)}")
    print(f"Successful: {len(successful)} ({len(successful)/len(results)*100:.1f}%)")
    print(f"Failed: {len(failed)} ({len(failed)/len(results)*100:.1f}%)")
    print()

    if successful:
        total_chain = sum(r['chain_periods'] for r in successful)
        total_enc = sum(r['encumbrances'] for r in successful)
        print(f"Total chain periods created: {total_chain}")
        print(f"Total encumbrances created: {total_enc}")
        print()

    if failed:
        print("Failed properties:")
        for r in failed:
            print(f"  {r['folio']}: {r['error']}")

    print()
    print("=" * 80)

    # Check final DB state
    conn = duckdb.connect('data/property_master.db', read_only=True)
    chain_total = conn.execute('SELECT COUNT(*), COUNT(DISTINCT folio) FROM chain_of_title').fetchone()
    enc_total = conn.execute('SELECT COUNT(*), COUNT(DISTINCT folio) FROM encumbrances').fetchone()
    doc_total = conn.execute('SELECT COUNT(*), COUNT(DISTINCT folio) FROM documents').fetchone()
    conn.close()

    print("FINAL DATABASE STATE:")
    print(f"  Chain of Title: {chain_total[0]} periods ({chain_total[1]} properties)")
    print(f"  Encumbrances: {enc_total[0]} records ({enc_total[1]} properties)")
    print(f"  Documents: {doc_total[0]} records ({doc_total[1]} properties)")
    print("=" * 80)


if __name__ == "__main__":
    import sys
    num = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    asyncio.run(run_batch_test(num))
