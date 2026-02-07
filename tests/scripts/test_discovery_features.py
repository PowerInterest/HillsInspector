#!/usr/bin/env python
"""
Test script for new discovery features:
- Instrument reference extraction
- Adjacent instrument search
- Chain gap detection
- Gap-bounded searches

Tests on 3 real properties from the database.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
from loguru import logger

from config.step4v2 import V2_DB_PATH
from src.services.step4v2.discovery import ChainGap, IterativeDiscovery


# Test properties
TEST_FOLIOS = [
    ("192935B6Y000008000040U", "TOUCHSTONE PHASE 2 - 7407 EVENING PRIMROSE CT"),
    ("21300836P000002000020U", "RIVER RIDGE RESERVE - 2560 REGAL RIVER RD"),
    ("20271089U000029000050A", "HERITAGE ISLES PHASE 2A - 18122 PALM BEACH DR"),
]


def test_instrument_reference_extraction():
    """Test _extract_instrument_references() with sample documents."""
    print("\n" + "=" * 80)
    print("TEST 1: Instrument Reference Extraction")
    print("=" * 80)

    conn = duckdb.connect(V2_DB_PATH)
    discovery = IterativeDiscovery(conn)

    # Test cases
    test_docs = [
        {
            "Legal": "CLK #2019437669 L 4 B 8 TOUCHSTONE PH 2",
            "expected": ["2019437669"],
        },
        {
            "Legal": "INST #2020123456 REFERENCE TO CLK #2019999999",
            "expected": ["2020123456", "2019999999"],
        },
        {
            "Legal": "L 4 B 8 TOUCHSTONE PH 2",  # No references
            "expected": [],
        },
        {
            "Legal": "O.R. 2021555555 SOME OTHER TEXT",
            "expected": ["2021555555"],
        },
    ]

    passed = 0
    for i, test in enumerate(test_docs):
        doc = {"Legal": test["Legal"], "Instrument": "9999999999"}  # Own instrument
        result = discovery._extract_instrument_references(doc)
        expected = set(test["expected"])
        actual = set(result)

        if actual == expected:
            print(f"  ✓ Test {i+1}: PASSED")
            passed += 1
        else:
            print(f"  ✗ Test {i+1}: FAILED")
            print(f"    Input: {test['Legal']}")
            print(f"    Expected: {expected}")
            print(f"    Actual: {actual}")

    print(f"\n  Results: {passed}/{len(test_docs)} tests passed")
    conn.close()
    return passed == len(test_docs)


def test_chain_gap_detection(folio: str, description: str):
    """Test _get_chain_gaps() for a specific property."""
    print(f"\n  Testing: {description}")
    print(f"  Folio: {folio}")

    conn = duckdb.connect(V2_DB_PATH)
    discovery = IterativeDiscovery(conn)

    # Get chain gaps
    gaps = discovery._get_chain_gaps(folio)

    if not gaps:
        print("    No gaps detected (chain may be complete or no data)")
    else:
        print(f"    Found {len(gaps)} gap(s):")
        for gap in gaps:
            print(f"      - {gap.gap_type}: {gap.start_date} → {gap.end_date} ({gap.days} days)")
            if gap.expected_grantor:
                print(f"        Expected grantor: {gap.expected_grantor}")
            if gap.expected_grantee:
                print(f"        Expected grantee: {gap.expected_grantee}")

    # Also show deeds found
    deeds = discovery._get_deeds(folio)
    print(f"    Deeds found: {len(deeds)}")
    for deed in deeds[:5]:  # Show first 5
        print(f"      - {deed['recording_date']}: {deed.get('grantor', 'N/A')[:30]} → {deed.get('grantee', 'N/A')[:30]}")
    if len(deeds) > 5:
        print(f"      ... and {len(deeds) - 5} more")

    conn.close()
    return gaps


def test_full_discovery(folio: str, description: str):
    """Run full discovery on a property and check results."""
    import json

    print(f"\n  Running discovery: {description}")
    print(f"  Folio: {folio}")

    # Get auction data from v1 database
    v1_conn = duckdb.connect("data/property_master.db", read_only=True)
    auction_row = v1_conn.execute(
        "SELECT * FROM auctions WHERE folio = ? LIMIT 1", [folio]
    ).fetchone()

    if not auction_row:
        print("    ERROR: Auction not found in v1 database")
        v1_conn.close()
        return None

    # Get column names
    columns = [desc[0] for desc in v1_conn.description]
    auction = dict(zip(columns, auction_row))
    v1_conn.close()

    # Parse extracted_judgment_data if it's a string
    if auction.get("extracted_judgment_data"):
        ejd = auction["extracted_judgment_data"]
        if isinstance(ejd, str):
            try:
                auction["extracted_judgment_data"] = json.loads(ejd)
            except json.JSONDecodeError:
                auction["extracted_judgment_data"] = None

    # Connect to v2 database
    v2_conn = duckdb.connect(V2_DB_PATH)
    discovery = IterativeDiscovery(v2_conn)

    # Run discovery
    try:
        result = discovery.run(
            folio=folio,
            auction=auction,
            hcpa_data=None,  # Will be fetched if needed
            final_judgment=auction.get("extracted_judgment_data"),
        )

        print(f"    Result:")
        print(f"      Iterations: {result.iterations}")
        print(f"      Documents found: {result.documents_found}")
        print(f"      Chain years: {result.chain_years:.1f}")
        print(f"      Is complete: {result.is_complete}")
        print(f"      Stopped reason: {result.stopped_reason}")

        # Check for instrument searches queued
        inst_searches = v2_conn.execute(
            """
            SELECT COUNT(*) FROM ori_search_queue
            WHERE folio = ? AND search_type = 'instrument'
            """,
            [folio],
        ).fetchone()[0]
        print(f"      Instrument searches queued: {inst_searches}")

        # Check for gap-bounded searches
        gap_searches = v2_conn.execute(
            """
            SELECT COUNT(*) FROM ori_search_queue
            WHERE folio = ? AND triggered_by_instrument LIKE 'gap:%'
            """,
            [folio],
        ).fetchone()[0]
        print(f"      Gap-bounded searches queued: {gap_searches}")

        v2_conn.close()
        return result

    except Exception as e:
        print(f"    ERROR: {e}")
        import traceback
        traceback.print_exc()
        v2_conn.close()
        return None


def check_existing_data():
    """Check what data already exists in v2 database."""
    print("\n" + "=" * 80)
    print("EXISTING DATA CHECK")
    print("=" * 80)

    try:
        conn = duckdb.connect(V2_DB_PATH, read_only=True)

        for folio, desc in TEST_FOLIOS:
            doc_count = conn.execute(
                "SELECT COUNT(*) FROM documents WHERE folio = ?", [folio]
            ).fetchone()[0]

            search_count = conn.execute(
                "SELECT COUNT(*) FROM ori_search_queue WHERE folio = ?", [folio]
            ).fetchone()[0]

            print(f"  {desc[:40]:<40}: {doc_count} docs, {search_count} searches")

        conn.close()
    except Exception as e:
        print(f"  Could not check v2 database: {e}")


def clear_folio_data(folio: str):
    """Clear all v2 data for a folio (fresh start)."""
    conn = duckdb.connect(V2_DB_PATH)

    # Delete in order to respect foreign keys
    conn.execute("DELETE FROM ori_search_queue WHERE folio = ?", [folio])
    conn.execute("DELETE FROM property_parties WHERE folio = ?", [folio])
    conn.execute("DELETE FROM documents WHERE folio = ?", [folio])

    conn.close()
    print(f"    Cleared all data for {folio}")


def main():
    """Run all tests."""
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    print("=" * 80)
    print("DISCOVERY FEATURE TESTS")
    print("=" * 80)

    # Check existing data
    check_existing_data()

    # Test 1: Instrument reference extraction
    test1_passed = test_instrument_reference_extraction()

    # Test 2: Run full discovery on CLEAN property (HERITAGE ISLES - 0 docs)
    print("\n" + "=" * 80)
    print("TEST 2: Full Discovery on Clean Property (HERITAGE ISLES)")
    print("=" * 80)

    folio, desc = TEST_FOLIOS[2]  # HERITAGE ISLES - 0 docs
    result1 = test_full_discovery(folio, desc)

    # Test 3: Clear and re-run TOUCHSTONE with fresh data
    print("\n" + "=" * 80)
    print("TEST 3: Fresh Discovery on TOUCHSTONE (after clearing old data)")
    print("=" * 80)

    folio, desc = TEST_FOLIOS[0]  # TOUCHSTONE
    print(f"\n  Clearing old contaminated data for {folio}...")
    clear_folio_data(folio)

    result2 = test_full_discovery(folio, desc)

    # Test 4: Chain gap detection after fresh discovery
    print("\n" + "=" * 80)
    print("TEST 4: Chain Gap Detection (after fresh discovery)")
    print("=" * 80)

    test_chain_gap_detection(folio, desc)

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"  Instrument extraction tests: {'PASSED' if test1_passed else 'FAILED'}")
    print(f"  HERITAGE ISLES discovery: {'COMPLETED' if result1 else 'FAILED'}")
    print(f"  TOUCHSTONE fresh discovery: {'COMPLETED' if result2 else 'FAILED'}")
    print(f"  Chain gap detection: See results above")


if __name__ == "__main__":
    main()
