import duckdb
from pathlib import Path
import json

def inspect_data():
    db_path = "data/property_master.db"
    if not Path(db_path).exists():
        print(f"Database not found at {db_path}")
        return

    conn = duckdb.connect(db_path)
    
    print("=== Data Quality Inspection ===\n")
    
    # 1. Auctions Completeness
    print("--- Auctions Table ---")
    try:
        total = conn.execute("SELECT COUNT(*) FROM auctions").fetchone()[0]
        print(f"Total Auctions: {total}")
        
        # Check key fields
        missing_parcel = conn.execute("SELECT COUNT(*) FROM auctions WHERE parcel_id IS NULL OR parcel_id = ''").fetchone()[0]
        print(f"  Missing Parcel ID: {missing_parcel}")
        
        missing_address = conn.execute("SELECT COUNT(*) FROM auctions WHERE property_address IS NULL OR property_address = ''").fetchone()[0]
        print(f"  Missing Address: {missing_address}")
        
        missing_judgment_data = conn.execute("SELECT COUNT(*) FROM auctions WHERE extracted_judgment_data IS NULL").fetchone()[0]
        print(f"  Missing Extracted Judgment Data (PDF Analysis): {missing_judgment_data}")
        
        missing_judgment_amt = conn.execute("SELECT COUNT(*) FROM auctions WHERE final_judgment_amount IS NULL").fetchone()[0]
        print(f"  Missing Final Judgment Amount: {missing_judgment_amt}")

        # Show a sample of what IS there for judgment data
        sample_judgment = conn.execute("SELECT case_number, extracted_judgment_data FROM auctions WHERE extracted_judgment_data IS NOT NULL LIMIT 1").fetchone()
        if sample_judgment:
            print(f"  Sample Judgment Data (Case {sample_judgment[0]}): {sample_judgment[1][:100]}...")
        else:
            print("  NO Judgment Data found for any auction.")

    except Exception as e:
        print(f"Error querying auctions: {e}")

    # 2. Documents (ORI)
    print("\n--- Documents Table (ORI) ---")
    try:
        # Check if table exists first
        table_exists = conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'documents'").fetchone()[0]
        if table_exists:
            total_docs = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
            print(f"Total Documents Ingested: {total_docs}")
            
            # Check unique folios
            unique_folios = conn.execute("SELECT COUNT(DISTINCT folio) FROM documents").fetchone()[0]
            print(f"Properties with Documents: {unique_folios}")
        else:
            print("Table 'documents' does not exist.")
    except Exception as e:
        print(f"Error querying documents: {e}")

    # 3. Chain of Title
    print("\n--- Chain of Title ---")
    try:
        table_exists = conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'chain_of_title'").fetchone()[0]
        if table_exists:
            total_chains = conn.execute("SELECT COUNT(*) FROM chain_of_title").fetchone()[0]
            print(f"Total Ownership Periods: {total_chains}")
            
            unique_chains = conn.execute("SELECT COUNT(DISTINCT folio) FROM chain_of_title").fetchone()[0]
            print(f"Properties with Chain Analysis: {unique_chains}")
        else:
            print("Table 'chain_of_title' does not exist.")
    except Exception as e:
        print(f"Error querying chain_of_title: {e}")

    conn.close()

if __name__ == "__main__":
    inspect_data()
