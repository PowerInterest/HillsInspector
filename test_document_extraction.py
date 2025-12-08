"""
Test script to download and analyze documents of each type to verify extraction works.
"""
import json
from pathlib import Path
from loguru import logger
import sys

from src.scrapers.ori_api_scraper import ORIApiScraper
from src.services.document_analyzer import DocumentAnalyzer

# Configure logging
logger.remove()
logger.add(sys.stderr, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}")

# Test documents - instrument, folio, doc_type
TEST_DOCS = [
    # ("2021173566", "172817060000007000090U", "D"),  # Already tested DEED
    ("2025313552", "202935ZZZ000002717700U", "MTG"),     # MORTGAGE
    ("2022312028", "1827349TP000000000370U", "LN"),      # LIEN
    ("2023308942", "202935ZZZ000002717700U", "SAT"),     # SATISFACTION
    ("99057542", "213005650000007000250U", "ASG"),       # ASSIGNMENT (older)
    ("2022438726", "1827349TP000000000370U", "LP"),      # LIS PENDENS
    ("2024531053", "1827349TP000000000370U", "NOC"),     # NOTICE OF COMMENCEMENT
    ("2025441733", "202935ZZZ000002717700U", "AFF"),     # AFFIDAVIT
]

def test_single_document(analyzer: DocumentAnalyzer, instrument: str, folio: str, doc_type: str) -> dict:
    """Test extraction for a single document."""
    logger.info(f"Testing {doc_type} document: {instrument}")

    result = {
        "instrument": instrument,
        "folio": folio,
        "doc_type": doc_type,
        "success": False,
        "data": None,
        "error": None
    }

    try:
        data = analyzer.download_and_analyze(instrument, folio, doc_type)
        if data:
            result["success"] = True
            result["data"] = data
            logger.success(f"  Extracted data for {doc_type}")
        else:
            result["error"] = "No data returned"
            logger.warning(f"  No data returned for {doc_type}")
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"  Error: {e}")

    return result

def main():
    """Run tests for all document types."""
    analyzer = DocumentAnalyzer()
    results = []

    logger.info("=" * 70)
    logger.info("DOCUMENT EXTRACTION TEST")
    logger.info("=" * 70)

    for instrument, folio, doc_type in TEST_DOCS:
        result = test_single_document(analyzer, instrument, folio, doc_type)
        results.append(result)

        # Print extracted data summary
        if result["success"] and result["data"]:
            data = result["data"]
            logger.info(f"  Key fields extracted:")

            # Common fields
            if "document_type" in data:
                logger.info(f"    document_type: {data.get('document_type')}")
            if "amount" in data:
                logger.info(f"    amount: ${data.get('amount'):,.2f}" if isinstance(data.get('amount'), (int, float)) else f"    amount: {data.get('amount')}")
            if "principal_amount" in data:
                logger.info(f"    principal_amount: ${data.get('principal_amount'):,.2f}" if isinstance(data.get('principal_amount'), (int, float)) else f"    principal_amount: {data.get('principal_amount')}")

            # Party information
            for field in ['grantor', 'grantee', 'lender', 'creditor', 'plaintiff',
                         'assignor', 'assignee', 'contractor', 'releasing_party']:
                if data.get(field):
                    logger.info(f"    {field}: {data.get(field)}")

            if "confidence" in data:
                logger.info(f"    confidence: {data.get('confidence')}")

        print()  # Blank line between tests

    # Summary
    logger.info("=" * 70)
    logger.info("TEST SUMMARY")
    logger.info("=" * 70)

    success_count = sum(1 for r in results if r["success"])
    logger.info(f"Total: {len(results)}, Success: {success_count}, Failed: {len(results) - success_count}")

    for result in results:
        status = "✓" if result["success"] else "✗"
        logger.info(f"  {status} {result['doc_type']}: {result['instrument']}")
        if result["error"]:
            logger.info(f"      Error: {result['error']}")

    # Save detailed results
    output_path = Path("data/test_results/document_extraction_results.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Convert to serializable format
    serializable_results = []
    for r in results:
        sr = r.copy()
        if sr["data"]:
            # Remove metadata that might have non-serializable items
            sr["data"] = {k: v for k, v in sr["data"].items() if not k.startswith("_")}
        serializable_results.append(sr)

    with open(output_path, "w") as f:
        json.dump(serializable_results, f, indent=2, default=str)

    logger.info(f"\nDetailed results saved to: {output_path}")

    return results

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Test document extraction")
    parser.add_argument("--type", type=str, help="Test specific doc type (MTG, LN, SAT, etc.)")
    parser.add_argument("--instrument", type=str, help="Test specific instrument number")
    parser.add_argument("--folio", type=str, default="test", help="Folio for organizing output")

    args = parser.parse_args()

    if args.type and args.instrument:
        # Test single document
        analyzer = DocumentAnalyzer()
        result = test_single_document(analyzer, args.instrument, args.folio, args.type)
        if result["data"]:
            print(json.dumps(result["data"], indent=2, default=str))
    else:
        # Run all tests
        main()
