
import duckdb
import os
import json
from pathlib import Path

def audit_database(db_path="data/property_master.db"):
    print(f"Auditing database: {db_path}")
    if not os.path.exists(db_path):
        print("Database not found.")
        return

    conn = duckdb.connect(db_path, read_only=True)
    
    # 1. Pipeline Status
    print("\n--- Pipeline Status ---")
    auctions = conn.execute("SELECT count(*) FROM auctions").fetchone()[0]
    print(f"Total Auctions: {auctions}")
    
    steps = [
        "needs_judgment_extraction",
        "needs_hcpa_enrichment",
        "needs_tax_check",
        "needs_market_data"
    ]
    for step in steps:
        try:
            pending = conn.execute(f"SELECT count(*) FROM auctions WHERE {step} = TRUE").fetchone()[0]
            print(f"  Pending {step}: {pending}")
        except Exception:
            pass

    # 2. Data Quality
    print("\n--- Data Quality ---")
    
    # Legal Descriptions
    parcels_count = conn.execute("SELECT count(*) FROM parcels").fetchone()[0]
    legal_desc_count = conn.execute("SELECT count(*) FROM parcels WHERE legal_description IS NOT NULL AND length(legal_description) > 10").fetchone()[0]
    print(f"Parcels with high-quality legal description: {legal_desc_count}/{parcels_count} ({legal_desc_count/parcels_count*100:.1f}%)" if parcels_count > 0 else "No parcels.")

    # Judgment Data
    judgment_count = conn.execute("SELECT count(*) FROM auctions WHERE extracted_judgment_data IS NOT NULL").fetchone()[0]
    print(f"Auctions with extracted judgment data: {judgment_count}/{auctions} ({judgment_count/auctions*100:.1f}%)" if auctions > 0 else "No auctions.")

    # Tax Data
    tax_data_count = conn.execute("SELECT count(DISTINCT folio) FROM liens WHERE document_type LIKE 'TAX%'").fetchone()[0]
    print(f"Properties with tax collector data: {tax_data_count}/{parcels_count} ({tax_data_count/parcels_count*100:.1f}%)" if parcels_count > 0 else "No properties.")

    # 3. Sample Check
    print("\n--- Recent Legal Descriptions (Head) ---")
    try:
        samples = conn.execute("SELECT folio, legal_description FROM parcels WHERE legal_description IS NOT NULL ORDER BY updated_at DESC LIMIT 3").fetchall()
        for folio, legal in samples:
            clean_legal = legal.replace('\n', ' ')[:100] + "..." if legal else "None"
            print(f"  {folio}: {clean_legal}")
    except Exception as e:
        print(f"  Error fetching samples: {e}")

    conn.close()

if __name__ == "__main__":
    audit_database()
