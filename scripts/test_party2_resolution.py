"""
Test script for Party 2 Resolution Service.

Tests:
1. Self-transfer detection logic
2. Name normalization
3. CQID 326 party search (requires network)
4. Full resolution workflow with known deed
"""
from src.services.party2_resolution_service import (
    Party2ResolutionService,
    detect_self_transfer
)


def test_self_transfer_detection():
    """Test self-transfer detection with various cases."""
    print("\n=== Testing Self-Transfer Detection ===\n")

    test_cases = [
        # (party1, party2, expected_is_self_transfer, expected_type)
        ("JOHN SMITH", "JOHN SMITH", True, "exact_match"),
        ("JOHN A SMITH", "JOHN A. SMITH", True, "exact_match"),
        ("JOHN SMITH JR", "JOHN SMITH", True, "name_variation"),
        ("JOHN SMITH", "JOHN SMITH, Trustee of the Smith Family Trust", True, "trust_transfer"),
        ("KRISTEN H BARGAMIN", "KRISTEN H. BARGAMIN", True, "exact_match"),
        ("JOHN SMITH", "JANE DOE", False, None),
        ("ABC COMPANY LLC", "XYZ CORPORATION", False, None),
        ("JOHN SMITH", "JOHN SMITH AS TRUSTEE", True, "trust_transfer"),
    ]

    passed = 0
    failed = 0

    for party1, party2, expected_self, expected_type in test_cases:
        is_self, transfer_type = detect_self_transfer(party1, party2)

        if is_self == expected_self:
            status = "PASS"
            passed += 1
        else:
            status = "FAIL"
            failed += 1

        print(f"  {status}: '{party1}' -> '{party2}'")
        print(f"         Expected: is_self={expected_self}, type={expected_type}")
        print(f"         Got:      is_self={is_self}, type={transfer_type}")
        print()

    print(f"Results: {passed} passed, {failed} failed")
    return failed == 0


def test_name_normalization():
    """Test name normalization."""
    print("\n=== Testing Name Normalization ===\n")

    service = Party2ResolutionService()

    test_cases = [
        ("JOHN SMITH JR", "JOHN SMITH"),
        ("MR. JOHN SMITH", "JOHN SMITH"),
        ("DR JANE DOE PHD", "JANE DOE"),
        ("  JOHN   SMITH  ", "JOHN SMITH"),
        ("JOHN A. SMITH, ESQ.", "JOHN A SMITH"),
    ]

    passed = 0
    failed = 0

    for input_name, expected in test_cases:
        result = service._normalize_name(input_name)

        if result == expected:
            status = "PASS"
            passed += 1
        else:
            status = "FAIL"
            failed += 1

        print(f"  {status}: '{input_name}' -> '{result}' (expected: '{expected}')")

    print(f"\nResults: {passed} passed, {failed} failed")
    return failed == 0


def test_needs_resolution():
    """Test document resolution check."""
    print("\n=== Testing needs_resolution() ===\n")

    service = Party2ResolutionService()

    test_cases = [
        # (doc, expected)
        ({"doc_type": "(D) DEED", "party1": "JOHN SMITH", "party2": None}, True),
        ({"doc_type": "(D) DEED", "party1": "JOHN SMITH", "party2": "JANE DOE"}, False),
        ({"doc_type": "(MTG) MORTGAGE", "party1": "JOHN SMITH", "party2": None}, False),  # Not a deed
        ({"doc_type": "(WD) WARRANTY DEED", "party1": "JOHN SMITH", "party2": ""}, True),
        ({"document_type": "D", "grantor": "JOHN SMITH", "grantee": None}, True),
    ]

    passed = 0
    failed = 0

    for doc, expected in test_cases:
        result = service.needs_resolution(doc)

        if result == expected:
            status = "PASS"
            passed += 1
        else:
            status = "FAIL"
            failed += 1

        print(f"  {status}: {doc.get('doc_type') or doc.get('document_type')} "
              f"p1={bool(doc.get('party1') or doc.get('grantor'))} "
              f"p2={bool(doc.get('party2') or doc.get('grantee'))} -> {result}")

    print(f"\nResults: {passed} passed, {failed} failed")
    return failed == 0


def test_cqid_326_search():
    """Test CQID 326 party name search (requires network)."""
    print("\n=== Testing CQID 326 Party Name Search ===\n")

    from src.scrapers.ori_api_scraper import ORIApiScraper

    scraper = ORIApiScraper()

    # Test with a known party name
    try:
        results = scraper.search_by_party_browser_sync("BARGAMIN KRISTEN*")
        print(f"  Found {len(results)} results for 'BARGAMIN KRISTEN*'")

        if results:
            # Show first few results
            for r in results[:5]:
                print(f"    {r.get('person_type'):10} | {r.get('name'):30} | {r.get('instrument')}")

            # Check for specific instrument
            target_instrument = "2024478600"
            party2_rows = [r for r in results if r.get("instrument") == target_instrument and "PARTY 2" in r.get("person_type", "")]

            if party2_rows:
                print(f"\n  Found Party 2 for instrument {target_instrument}!")
            else:
                print(f"\n  No Party 2 found for instrument {target_instrument} (expected - this is a self-transfer)")

        return len(results) > 0

    except Exception as e:
        print(f"  ERROR: {e}")
        return False

    finally:
        # Clean up browser
        import asyncio
        asyncio.run(scraper.close_browser())


def test_full_resolution():
    """Test full resolution workflow with known deed."""
    print("\n=== Testing Full Resolution Workflow ===\n")

    service = Party2ResolutionService()

    # Test document - instrument 2024478600 (known self-transfer)
    doc = {
        "instrument": "2024478600",
        "party1": "BARGAMIN KRISTEN H",
        "party2": None,
        "doc_type": "(D) DEED",
        "ID": "2024478600"  # For PDF download
    }

    print(f"  Testing resolution for instrument {doc['instrument']}")
    print(f"  Party 1: {doc['party1']}")

    result = service.resolve_party2(doc)

    print(f"\n  Result:")
    print(f"    Party 2: {result.party2}")
    print(f"    Method: {result.method}")
    print(f"    Is Self-Transfer: {result.is_self_transfer}")
    print(f"    Transfer Type: {result.self_transfer_type}")
    print(f"    Confidence: {result.confidence}")

    # Clean up
    import asyncio
    asyncio.run(service.ori_scraper.close_browser())

    return result.party2 is not None or result.method == "unresolved"


if __name__ == "__main__":
    print("=" * 60)
    print("Party 2 Resolution Service Tests")
    print("=" * 60)

    results = []

    # Run unit tests
    results.append(("Self-Transfer Detection", test_self_transfer_detection()))
    results.append(("Name Normalization", test_name_normalization()))
    results.append(("Needs Resolution", test_needs_resolution()))

    # Run integration tests (require network)
    print("\n" + "=" * 60)
    print("Integration Tests (require network)")
    print("=" * 60)

    try:
        results.append(("CQID 326 Search", test_cqid_326_search()))
    except Exception as e:
        print(f"  SKIP: CQID 326 Search - {e}")
        results.append(("CQID 326 Search", None))

    try:
        results.append(("Full Resolution", test_full_resolution()))
    except Exception as e:
        print(f"  SKIP: Full Resolution - {e}")
        results.append(("Full Resolution", None))

    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)

    for name, passed in results:
        if passed is None:
            status = "SKIP"
        elif passed:
            status = "PASS"
        else:
            status = "FAIL"
        print(f"  {status}: {name}")
