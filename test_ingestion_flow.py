"""Test the updated ingestion flow with API search and PDF download."""
from src.services.ingestion_service import IngestionService
from src.models.property import Property

# Create a test property with WESTCHASE legal description
# This is the property from the previous tests
prop = Property(
    case_number="24-CA-007587",
    parcel_id="19301393I000014000280U",
    address="9903 Bennington Dr, Tampa FL 33626",
    legal_description="LOT 9, BLOCK 7, WESTCHASE SECTION 110",
)

# Set up search terms
prop.legal_search_terms = ["WESTCHASE SECTION 110", "WESTCHASE"]

print(f"Testing ingestion for property: {prop.case_number}")
print(f"Folio: {prop.parcel_id}")
print(f"Legal description: {prop.legal_description}")
print(f"Search terms: {prop.legal_search_terms}")
print("-" * 60)

# Create ingestion service with PDF analysis disabled first (faster test)
# We'll test PDF download separately
svc = IngestionService(analyze_pdfs=False)

# Run ingestion
print("\nStarting ingestion...")
svc.ingest_property(prop)

# Check results
from src.db.operations import PropertyDB
with PropertyDB() as db:
    # Count documents
    doc_count = db.conn.execute(f"""
        SELECT COUNT(*) FROM documents WHERE folio = '{prop.parcel_id}'
    """).fetchone()[0]
    print(f"\nDocuments saved: {doc_count}")

    # Check parties
    docs_with_party2 = db.conn.execute(f"""
        SELECT COUNT(*) FROM documents
        WHERE folio = '{prop.parcel_id}'
        AND party2 IS NOT NULL AND party2 != ''
    """).fetchone()[0]
    print(f"Documents with Party2: {docs_with_party2}")

    # Check chain of title
    chain_count = db.conn.execute(f"""
        SELECT COUNT(*) FROM chain_of_title WHERE folio = '{prop.parcel_id}'
    """).fetchone()[0]
    print(f"Chain of title entries: {chain_count}")

    # Sample some documents
    print("\nSample documents:")
    samples = db.conn.execute(f"""
        SELECT document_type, instrument_number, party1, party2, file_path
        FROM documents
        WHERE folio = '{prop.parcel_id}'
        AND document_type LIKE '%DEED%'
        LIMIT 3
    """).fetchall()
    for doc in samples:
        print(f"  {doc[0]}: {doc[1]}")
        print(f"    Party1: {doc[2]}")
        print(f"    Party2: {doc[3]}")
        print(f"    PDF: {doc[4] or 'None'}")
