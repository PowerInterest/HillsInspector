"""Test downloading a single PDF with the new API-based flow."""
from pathlib import Path
from src.scrapers.ori_api_scraper import ORIApiScraper

scraper = ORIApiScraper()

# Search for a specific deed
print("Searching for WESTCHASE SECTION 110 deeds...")
results = scraper.search_by_legal("WESTCHASE SECTION 110")
print(f"Found {len(results)} documents")

# Find a deed
deed = None
for doc in results:
    if "(D)" in str(doc.get("DocType", "")):
        deed = doc
        break

if deed:
    print(f"\nFound deed:")
    print(f"  Instrument: {deed.get('Instrument')}")
    print(f"  DocType: {deed.get('DocType')}")
    print(f"  ID: {deed.get('ID', '')[:50]}...")
    print(f"  PartiesOne: {deed.get('PartiesOne', [])}")
    print(f"  PartiesTwo: {deed.get('PartiesTwo', [])}")

    # Try to download
    output_dir = Path("data/temp/test_downloads")
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nDownloading PDF...")
    pdf_path = scraper.download_pdf(deed, output_dir)

    if pdf_path and pdf_path.exists():
        print(f"SUCCESS: Downloaded to {pdf_path}")
        print(f"File size: {pdf_path.stat().st_size} bytes")

        # Test vLLM analysis
        print("\nAnalyzing with vLLM...")
        from src.services.document_analyzer import DocumentAnalyzer
        analyzer = DocumentAnalyzer()
        result = analyzer.analyze_document(str(pdf_path), "D", str(deed.get("Instrument")))
        if result:
            print(f"Extraction result:")
            print(f"  Grantor: {result.get('grantor')}")
            print(f"  Grantee: {result.get('grantee')}")
            print(f"  Consideration: {result.get('consideration')}")
    else:
        print("FAILED: Could not download PDF")
else:
    print("No deed found!")
