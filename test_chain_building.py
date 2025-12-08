"""
Test script to trace through chain of title building step by step.
Uses: 1827349TP000000000370U - "RETREAT AT CARROLLWOOD LOT 37"
"""

import asyncio
import json
from datetime import datetime
from loguru import logger
import sys

# Configure detailed logging
logger.remove()
logger.add(sys.stdout, level="DEBUG", format="{time:HH:mm:ss} | {level} | {message}")

from src.scrapers.ori_api_scraper import ORIApiScraper
from src.services.title_chain_service import TitleChainService
from src.services.ingestion_service import IngestionService
from src.db.operations import PropertyDB
from src.models.property import Property

# Test property
FOLIO = "1827349TP000000000370U"
CASE_NUMBER = "292022CA007547A001HC"
ADDRESS = "14320 AVON FARMS DR, TAMPA, FL- 33618"
LEGAL_DESC = "RETREAT AT CARROLLWOOD LOT 37"
JUDGMENT_LEGAL = "LOT 37, RETREAT AT CARROLLWOOD"


async def test_full_chain():
    """Test the complete chain building process with fixed grouping"""
    print("\n" + "=" * 80)
    print("CHAIN OF TITLE BUILDING TEST (WITH FIX)")
    print(f"Property: {FOLIO}")
    print(f"Address: {ADDRESS}")
    print(f"Legal: {LEGAL_DESC}")
    print("=" * 80)

    # Step 1: Search ORI
    print("\n" + "=" * 60)
    print("STEP 1: SEARCHING ORI")
    print("=" * 60)

    scraper = ORIApiScraper()
    search_term = "L 37 RETREAT*"
    url = f"https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=321&OBKey__1011_1={search_term.replace(' ', '%20')}"
    print(f"URL: {url}")
    print(f"Search term: {search_term}")

    raw_docs = await scraper.search_by_legal_browser(search_term, headless=True)
    await scraper.close_browser()

    print(f"Raw records returned: {len(raw_docs)}")

    if not raw_docs:
        print("❌ NO DOCUMENTS FOUND")
        return

    # Step 2: Group by instrument (THE FIX)
    print("\n" + "=" * 60)
    print("STEP 2: GROUPING BY INSTRUMENT NUMBER")
    print("=" * 60)

    service = IngestionService()
    grouped_docs = service._group_ori_records_by_instrument(raw_docs)

    print(f"Grouped into {len(grouped_docs)} unique documents")

    # Show all grouped documents
    print("\n--- Grouped Documents ---")
    for i, doc in enumerate(grouped_docs):
        print(f"\n[{i+1}] {doc.get('doc_type')}")
        print(f"    Date: {doc.get('record_date')}")
        print(f"    Instrument: {doc.get('instrument')}")
        print(f"    Book/Page: {doc.get('book_num')}/{doc.get('page_num')}")
        print(f"    Party 1 (Grantor/Debtor): {doc.get('party1_names')}")
        print(f"    Party 2 (Grantee/Creditor): {doc.get('party2_names')}")

    # Step 3: Map to our schema
    print("\n" + "=" * 60)
    print("STEP 3: MAPPING TO SCHEMA")
    print("=" * 60)

    prop = Property(
        case_number=CASE_NUMBER,
        parcel_id=FOLIO,
        address=ADDRESS,
        legal_description=JUDGMENT_LEGAL,
    )

    mapped_docs = []
    for doc in grouped_docs:
        mapped = service._map_grouped_ori_doc(doc, prop)
        mapped_docs.append(mapped)

    # Count by type
    doc_types = {}
    for d in mapped_docs:
        dt = d.get('document_type', 'UNKNOWN')
        doc_types[dt] = doc_types.get(dt, 0) + 1

    print("\nDocument Types:")
    for dt, count in sorted(doc_types.items(), key=lambda x: -x[1]):
        print(f"  {dt}: {count}")

    # Step 4: Build chain of title
    print("\n" + "=" * 60)
    print("STEP 4: BUILDING CHAIN OF TITLE")
    print("=" * 60)

    chain_service = TitleChainService()
    analysis = chain_service.build_chain_and_analyze(mapped_docs)

    # Show chain
    chain = analysis.get('chain', [])
    print(f"\n--- Ownership Chain ({len(chain)} transfers) ---")
    for i, period in enumerate(chain):
        print(f"\n[{i+1}] {period.get('date')}")
        print(f"    From: {period.get('grantor')}")
        print(f"    To: {period.get('grantee')}")
        print(f"    Type: {period.get('doc_type')}")
        print(f"    Book/Page: {period.get('book_page')}")
        if period.get('notes'):
            for note in period.get('notes'):
                print(f"    ⚠️  {note}")

    # Show encumbrances
    encumbrances = analysis.get('encumbrances', [])
    print(f"\n--- Encumbrances ({len(encumbrances)} found) ---")
    for i, enc in enumerate(encumbrances):
        status_icon = "✅" if enc.get('status') == 'SATISFIED' else "🔴"
        print(f"\n{status_icon} [{i+1}] {enc.get('type')}")
        print(f"    Date: {enc.get('date')}")
        print(f"    Creditor: {enc.get('creditor')}")
        print(f"    Debtor: {enc.get('debtor')}")
        print(f"    Amount: {enc.get('amount')}")
        print(f"    Status: {enc.get('status')}")
        if enc.get('satisfaction_ref'):
            print(f"    Satisfied by: {enc.get('satisfaction_ref')}")
        if enc.get('match_method'):
            print(f"    Match method: {enc.get('match_method')}")

    # Summary
    summary = analysis.get('summary', {})
    print(f"\n--- Summary ---")
    print(f"    Total Deeds: {summary.get('total_deeds')}")
    print(f"    Active Liens: {summary.get('active_liens')}")
    print(f"    Current Owner: {summary.get('current_owner')}")

    # Step 5: Transform for DB
    print("\n" + "=" * 60)
    print("STEP 5: TRANSFORMING FOR DATABASE")
    print("=" * 60)

    db_data = service._transform_analysis_for_db(analysis)
    timeline = db_data.get('ownership_timeline', [])

    print(f"\nOwnership Timeline ({len(timeline)} periods):")
    for i, period in enumerate(timeline):
        print(f"\n[{i+1}] Owner: {period.get('owner')}")
        print(f"    Acquired from: {period.get('acquired_from')}")
        print(f"    Acquired on: {period.get('acquisition_date')}")
        print(f"    Sold on: {period.get('disposition_date')}")
        enc_count = len(period.get('encumbrances', []))
        print(f"    Encumbrances during ownership: {enc_count}")
        for enc in period.get('encumbrances', []):
            print(f"      - {enc.get('type')}: ${enc.get('amount', 0):,.0f} ({enc.get('survival_status')})")

    print("\n" + "=" * 80)
    print("TEST COMPLETE")
    if chain:
        print(f"✅ Successfully built chain with {len(chain)} ownership periods")
        print(f"✅ Found {len(encumbrances)} encumbrances ({summary.get('active_liens')} active)")
    else:
        print("❌ Chain building failed - no ownership periods")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(test_full_chain())
