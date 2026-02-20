"""
Test suite for legal description parser.

Uses real examples from the database to ensure:
1. Improvements to poor-performing cases
2. No regressions in good-performing cases
"""

import json
import re
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from utils.legal_description import (
    parse_legal_description,
    generate_search_permutations,
)


def load_test_cases() -> dict:
    """Load test cases from JSON file."""
    test_file = Path(__file__).parent.parent / "docs" / "legal_test_cases.json"
    with open(test_file) as f:
        return json.load(f)


def analyze_legal_description(legal: str) -> dict:
    """Parse and analyze a legal description, returning structured results."""
    parsed = parse_legal_description(legal)
    perms = generate_search_permutations(parsed)

    return {
        "subdivision": parsed.subdivision,
        "lot": parsed.lot,
        "lots": parsed.lots,
        "block": parsed.block,
        "unit": parsed.unit,
        "phase": parsed.phase,
        "section": parsed.section,
        "permutations": perms,
    }


def test_multiple_lots_extraction():
    """Test that LOTS X Y AND Z patterns extract ALL lot numbers."""
    test_cases = [
        # (input, expected_lots)
        ("LOTS 1, 2 AND 3, LESS THE WEST 50 FEET THEREOF, IN BLOCK 100, MAP OF PART OF PORT TAMPA CITY", ["1", "2", "3"]),
        ("LOTS 1 AND 2, BLOCK R, MAP OF CASTLE HEIGHTS", ["1", "2"]),
        ("LOT 11 AND THE WEST 15.00 FEET OF LOT 12, BLOCK 1, ANADELL SUBDIVISION", ["11", "12"]),
        ("The West 52 feet of Lot 19 and the East 13 feet of Lot 18, Block 25, TOWN N COUNTRY PARK", ["19", "18"]),
        # Single lot should still work
        ("LOT 9, BLOCK 2, BRANDON LAKES", ["9"]),
        ("LOT 48, BLOCK 1, OAK FOREST ADDITION", ["48"]),
    ]

    print("\n" + "=" * 80)
    print("TEST: Multiple Lots Extraction")
    print("=" * 80)

    passed = 0
    failed = 0

    for legal, expected_lots in test_cases:
        result = analyze_legal_description(legal)
        actual_lots = result["lots"]

        # Check if all expected lots are found
        expected_set = set(expected_lots)
        actual_set = set(actual_lots)

        if expected_set == actual_set:
            print(f"✓ PASS: {legal[:60]}...")
            print(f"       Found lots: {actual_lots}")
            passed += 1
        else:
            print(f"✗ FAIL: {legal[:60]}...")
            print(f"       Expected: {expected_lots}")
            print(f"       Got:      {actual_lots}")
            failed += 1

    print(f"\nResults: {passed} passed, {failed} failed")
    return failed == 0


def test_subdivision_extraction():
    """Test that subdivision names are extracted correctly."""
    test_cases = [
        # (input, expected_subdivision_contains)
        ("LOT 11, BLOCK 10, GANDY MANOR ADDITION", "GANDY MANOR"),
        ("LOT 9 OF FLOWERS & STUART OAK GROVE SUBDIVISION", "OAK GROVE"),
        ("LOT 40, BLOCK 1, TEMPLE OAKS", "TEMPLE OAKS"),
        ("LOT 15, BLOCK 17, NORTHDALE, SECTION B, UNIT NO. 2", "NORTHDALE"),
        ("1	TURMAN'S EAST YBOR LOT 13 BLOCK 30", "TURMAN"),  # Should handle apostrophe
        ("1	MUNRO'S AND CLEWIS'S ADDITION TO WEST TAMPA LOT 11 BLOCK 8", "MUNRO"),
        # Good cases - should still work
        ("LOT 48, BLOCK 1, OAK FOREST ADDITION", "OAK FOREST"),
        ("LOT 9 IN BLOCK 2 OF BRANDON LAKES", "BRANDON LAKES"),
        ("LOT 2, BLOCK 2, RIVER RIDGE RESERVE", "RIVER RIDGE"),
    ]

    print("\n" + "=" * 80)
    print("TEST: Subdivision Extraction")
    print("=" * 80)

    passed = 0
    failed = 0

    for legal, expected_contains in test_cases:
        result = analyze_legal_description(legal)
        subdiv = result["subdivision"] or ""

        if expected_contains.upper() in subdiv.upper():
            print(f"✓ PASS: Subdivision contains '{expected_contains}'")
            print(f"       Full: {subdiv}")
            passed += 1
        else:
            print(f"✗ FAIL: Expected subdivision containing '{expected_contains}'")
            print(f"       Got: {subdiv}")
            print(f"       Legal: {legal[:80]}...")
            failed += 1

    print(f"\nResults: {passed} passed, {failed} failed")
    return failed == 0


def test_permutation_generation():
    """Test that search permutations are generated correctly."""
    print("\n" + "=" * 80)
    print("TEST: Search Permutation Generation")
    print("=" * 80)

    # Test that good cases generate sensible permutations
    good_cases = [
        "LOT 48, BLOCK 1, OAK FOREST ADDITION",
        "LOT 9 IN BLOCK 2 OF BRANDON LAKES",
        "LOT 2, BLOCK 2, RIVER RIDGE RESERVE",
    ]

    for legal in good_cases:
        result = analyze_legal_description(legal)
        print(f"\nLegal: {legal}")
        print(f"Lot: {result['lot']}, Block: {result['block']}")
        print(f"Subdivision: {result['subdivision']}")
        print(f"Permutations ({len(result['permutations'])}):")
        for p in result['permutations'][:5]:
            print(f"  - {p}")


def run_regression_tests():
    """Run tests on all good-performing cases to ensure no regressions."""
    print("\n" + "=" * 80)
    print("REGRESSION TESTS: Good Parsing Cases (10+ docs)")
    print("=" * 80)

    data = load_test_cases()

    # Store baseline results for good cases
    results = []
    for case in data["good"]:
        legal = case["legal_description"]
        folio = case["folio"]
        doc_count = case["doc_count"]

        result = analyze_legal_description(legal)

        print(f"\n[{doc_count:3} docs] {folio}")
        print(f"Legal: {legal[:80]}...")
        print(f"  Lot: {result['lot']}, Lots: {result['lots']}, Block: {result['block']}")
        print(f"  Subdivision: {result['subdivision']}")
        print(f"  Permutations: {result['permutations'][:3]}")

        results.append({
            "folio": folio,
            "doc_count": doc_count,
            "lot": result["lot"],
            "lots": result["lots"],
            "block": result["block"],
            "subdivision": result["subdivision"],
            "permutation_count": len(result["permutations"]),
        })

    return results


def analyze_poor_cases():
    """Analyze poor-performing cases to identify patterns."""
    print("\n" + "=" * 80)
    print("ANALYSIS: Poor Parsing Cases (<3 docs)")
    print("=" * 80)

    data = load_test_cases()

    categories = {
        "multiple_lots": [],
        "partial_lots": [],
        "metes_bounds": [],
        "complex_phase": [],
        "apostrophe": [],
        "simple_fail": [],
    }

    for case in data["poor"]:
        legal = case["legal_description"]
        folio = case["folio"]
        legal_upper = legal.upper()

        # Categorize
        if "LOTS " in legal_upper or " AND LOT " in legal_upper.replace("THE ", "") or ("LOT " in legal_upper and legal_upper.count("LOT ") > 1):
            categories["multiple_lots"].append(case)
        elif re.search(r'\b[NSEW]\s+\d+\s+(?:FT|FEET)\s+OF', legal_upper):
            categories["partial_lots"].append(case)
        elif folio[6:9] == "ZZZ" or legal_upper.startswith(("BEGIN", "BEG", "COM", "THE NORTH", "THE SOUTH", "THE EAST", "THE WEST", "A TRACT")):
            categories["metes_bounds"].append(case)
        elif "SECTION" in legal_upper and "UNIT" in legal_upper:
            categories["complex_phase"].append(case)
        elif "'" in legal:
            categories["apostrophe"].append(case)
        else:
            categories["simple_fail"].append(case)

    for cat_name, cases in categories.items():
        if cases:
            print(f"\n### {cat_name.upper()} ({len(cases)} cases)")
            for case in cases[:3]:  # Show first 3 of each category
                result = analyze_legal_description(case["legal_description"])
                print(f"  [{case['doc_count']} docs] {case['folio']}")
                print(f"    Legal: {case['legal_description'][:70]}...")
                print(f"    Extracted: lot={result['lot']}, lots={result['lots']}, block={result['block']}")
                print(f"    Subdiv: {result['subdivision']}")

if __name__ == "__main__":
    print("=" * 80)
    print("LEGAL DESCRIPTION PARSER TEST SUITE")
    print("=" * 80)

    # Run specific tests
    test_multiple_lots_extraction()
    test_subdivision_extraction()
    test_permutation_generation()

    # Run regression tests
    run_regression_tests()

    # Analyze poor cases
    analyze_poor_cases()
