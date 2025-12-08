"""Test PDF download with instrument number lookup."""
from pathlib import Path
from src.scrapers.ori_api_scraper import ORIApiScraper

def test_download_by_instrument():
    """Test downloading a PDF using just instrument number (no ID)."""
    scraper = ORIApiScraper()

    # Test instrument from a known document
    test_instrument = "2024478600"  # A deed from the database

    output_dir = Path("data/temp/test_downloads")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create a doc dict like browser scraping would produce (no ID)
    browser_doc = {
        "instrument": test_instrument,
        "doc_type": "(D) DEED",
        "record_date": "11/25/2024 12:03 PM",
    }

    print(f"Testing download for instrument: {test_instrument}")
    print(f"Doc has no ID - should look up via API...")

    result = scraper.download_pdf(browser_doc, output_dir)

    if result:
        print(f"SUCCESS: Downloaded to {result}")
        print(f"File size: {result.stat().st_size} bytes")
    else:
        print("FAILED: Could not download PDF")

    return result

if __name__ == "__main__":
    test_download_by_instrument()
