"""Test full ingestion with PDF analysis on a limited set of documents."""
from src.services.ingestion_service import IngestionService
from src.models.property import Property
from src.scrapers.ori_api_scraper import ORIApiScraper

# Create a test property
prop = Property(
    case_number="24-CA-007587",
    parcel_id="19301393I000014000280U",
    address="9903 Bennington Dr, Tampa FL 33626",
    legal_description="LOT 9, BLOCK 7, WESTCHASE SECTION 110",
)
prop.legal_search_terms = ["WESTCHASE SECTION 110"]

print(f"Testing full ingestion with PDF analysis for: {prop.case_number}")
print("-" * 60)

# First, get documents via API to limit the test
scraper = ORIApiScraper()
all_docs = scraper.search_by_legal("WESTCHASE SECTION 110")
print(f"API returned {len(all_docs)} total documents")

# Filter to just deeds and mortgages for this test (limit to 5)
test_docs = []
for doc in all_docs:
    doc_type = doc.get("DocType", "")
    if "(D)" in doc_type or "(MTG)" in doc_type:
        test_docs.append(doc)
        if len(test_docs) >= 5:
            break

print(f"Testing with {len(test_docs)} documents (deeds + mortgages)")

# Create ingestion service WITH PDF analysis
svc = IngestionService(analyze_pdfs=True)

# Manually ingest with limited docs
print("\nStarting ingestion with PDF analysis...")
svc.ingest_property(prop, raw_docs=test_docs)

# Check results
from src.db.operations import PropertyDB
with PropertyDB() as db:
    # Check documents with extracted data
    docs_with_extraction = db.conn.execute(f"""
        SELECT document_type, instrument_number, extracted_data, file_path
        FROM documents
        WHERE folio = '{prop.parcel_id}'
        AND extracted_data IS NOT NULL
        ORDER BY recording_date DESC
        LIMIT 5
    """).fetchall()

    print(f"\nDocuments with vLLM extraction: {len(docs_with_extraction)}")
    for doc in docs_with_extraction:
        print(f"\n  {doc[0]}: {doc[1]}")
        print(f"    PDF: {doc[3] or 'None'}")
        if doc[2]:
            import json
            data = json.loads(doc[2]) if isinstance(doc[2], str) else doc[2]
            if "grantor" in data:
                print(f"    Extracted grantor: {data.get('grantor')}")
                print(f"    Extracted grantee: {data.get('grantee')}")
                print(f"    Consideration: {data.get('consideration')}")
            elif "principal_amount" in data:
                print(f"    Principal: {data.get('principal_amount')}")
                print(f"    Lender: {data.get('lender')}")
