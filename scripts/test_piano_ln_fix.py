"""
Test script for the ORI search fix on Piano Ln property.

This property had 0 mortgages because ORI search was returning documents
for the entire subdivision instead of just LOT 44 BLOCK 2.

The fix generates ORI-optimized search terms with lot/block first:
    "L 44 B 2 SYMPHONY*" instead of "SYMPHONY ISLES UNIT TWO"
"""

import sys
sys.path.insert(0, "/mnt/c/code/HillsInspector")

from loguru import logger
from src.utils.legal_description import parse_legal_description, generate_search_permutations
from src.scrapers.ori_api_scraper import ORIApiScraper

# Configure logging
logger.remove()
logger.add(sys.stderr, level="INFO")

# Piano Ln property details
FOLIO = "1931201T7000002000440U"
LEGAL_DESCRIPTION = "SYMPHONY ISLES UNIT TWO LOT 44 BLOCK 2"
EXPECTED_LOT = "44"
EXPECTED_BLOCK = "2"

def test_legal_parsing():
    """Test that legal description parsing extracts lot/block correctly."""
    print("\n" + "="*60)
    print("Step 1: Testing legal description parsing")
    print("="*60)

    parsed = parse_legal_description(LEGAL_DESCRIPTION)
    print(f"Input: {LEGAL_DESCRIPTION}")
    print(f"Parsed lot: {parsed.lot} (expected: {EXPECTED_LOT})")
    print(f"Parsed block: {parsed.block} (expected: {EXPECTED_BLOCK})")
    print(f"Parsed subdivision: {parsed.subdivision}")

    assert parsed.lot == EXPECTED_LOT, f"Lot mismatch: got {parsed.lot}"
    assert parsed.block == EXPECTED_BLOCK, f"Block mismatch: got {parsed.block}"
    print("✓ Legal parsing works correctly!")
    return parsed

def test_search_term_generation(parsed):
    """Test that search terms are generated in ORI-optimized format."""
    print("\n" + "="*60)
    print("Step 2: Testing search term generation")
    print("="*60)

    terms = generate_search_permutations(parsed)
    print(f"Generated {len(terms)} search terms:")
    for i, term in enumerate(terms, 1):
        print(f"  {i}. {term}")

    # First term should have lot/block first
    first_term = terms[0] if terms else ""
    assert "L 44" in first_term, f"First term should start with 'L 44', got: {first_term}"
    assert "B 2" in first_term, f"First term should include 'B 2', got: {first_term}"
    assert "*" in first_term, f"First term should be a wildcard search, got: {first_term}"

    print("✓ Search term generation works correctly!")
    return terms

def test_browser_search(terms):
    """Test actual browser search with the new terms."""
    print("\n" + "="*60)
    print("Step 3: Testing browser search")
    print("="*60)

    scraper = ORIApiScraper()

    for term in terms[:2]:  # Test first 2 terms
        print(f"\nSearching ORI browser for: {term}")
        try:
            docs = scraper.search_by_legal_sync(term, headless=True)
            print(f"Found {len(docs)} documents")

            if docs:
                # Count document types
                doc_types = {}
                for doc in docs:
                    dtype = doc.get("DocType") or doc.get("doc_type") or "UNKNOWN"
                    doc_types[dtype] = doc_types.get(dtype, 0) + 1

                print("Document types found:")
                for dtype, count in sorted(doc_types.items(), key=lambda x: -x[1]):
                    print(f"  {dtype}: {count}")

                # Count mortgages - types may include description like "(MTG) MORTGAGE"
                mortgages = 0
                for dtype, count in doc_types.items():
                    if 'MTG' in dtype.upper() or 'MORTGAGE' in dtype.upper() or 'DOT' in dtype.upper() or 'HELOC' in dtype.upper():
                        mortgages += count
                print(f"\nTotal mortgages: {mortgages}")

                if mortgages > 0:
                    print("✓ Found mortgages! Fix is working.")
                    return docs
                print("⚠ No mortgages found with this term, trying next...")
        except Exception as e:
            print(f"Error: {e}")

    return []

def filter_by_lot_block(docs, lot, block):
    """Filter documents by lot/block."""
    import re

    filtered = []
    for doc in docs:
        legal = doc.get("Legal") or doc.get("legal") or ""
        legal_upper = legal.upper()

        # Check lot match
        lot_pattern = rf'\bL(?:OT)?\s*{re.escape(lot)}\b'
        lot_match = bool(re.search(lot_pattern, legal_upper))

        # Check block match
        block_pattern = rf'\bB(?:LK|LOCK)?\s*{re.escape(block)}\b'
        block_match = bool(re.search(block_pattern, legal_upper))

        if lot_match and block_match:
            filtered.append(doc)

    return filtered

def main():
    print("Testing ORI search fix for Piano Ln property")
    print(f"Folio: {FOLIO}")
    print(f"Legal: {LEGAL_DESCRIPTION}")

    # Step 1: Parse legal description
    parsed = test_legal_parsing()

    # Step 2: Generate search terms
    terms = test_search_term_generation(parsed)

    # Step 3: Test browser search
    docs = test_browser_search(terms)

    if docs:
        print("\n" + "="*60)
        print("Step 4: Verifying lot/block filtering")
        print("="*60)

        filtered = filter_by_lot_block(docs, EXPECTED_LOT, EXPECTED_BLOCK)
        print(f"Before filtering: {len(docs)} documents")
        print(f"After filtering: {len(filtered)} documents for LOT {EXPECTED_LOT} BLOCK {EXPECTED_BLOCK}")

        if filtered:
            # Count mortgages in filtered results - doc_type may include description
            mortgages = 0
            for doc in filtered:
                dtype = doc.get("DocType") or doc.get("doc_type") or ""
                if 'MTG' in dtype.upper() or 'MORTGAGE' in dtype.upper() or 'DOT' in dtype.upper() or 'HELOC' in dtype.upper():
                    mortgages += 1

            print(f"Mortgages in filtered results: {mortgages}")

            if mortgages > 0:
                print("\n✓✓✓ SUCCESS! The fix is working correctly. ✓✓✓")
                print(f"Expected: 50+ mortgages (from manual browser test)")
                print(f"Found: {mortgages} mortgages for LOT 44 BLOCK 2")
            else:
                print("\n⚠ Warning: No mortgages found after filtering")
        else:
            print("\n⚠ Warning: No documents after lot/block filtering")
    else:
        print("\n✗ Failed: No documents found via browser search")

if __name__ == "__main__":
    main()
