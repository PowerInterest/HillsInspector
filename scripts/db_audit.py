import duckdb
import pandas as pd
from pathlib import Path

DB_PATH = "data/property_master.db"

def audit_database():
    if not Path(DB_PATH).exists():
        print(f"Database not found at {DB_PATH}")
        return

    conn = duckdb.connect(DB_PATH, read_only=True)
    
    print("=== Database Audit Report ===\n")
    
    # 1. Auction Counts
    try:
        total_auctions = conn.execute("SELECT COUNT(*) FROM auctions").fetchone()[0]
        print(f"Total Auctions: {total_auctions}")
    except Exception as e:
        print(f"Error querying auctions: {e}")

    # 2. Completeness Checks
    print("\n--- Completeness ---")
    
    # Check Parcels (should match auctions roughly, or be unique parcels)
    try:
        total_parcels = conn.execute("SELECT COUNT(*) FROM parcels").fetchone()[0]
        print(f"Total Parcels in DB: {total_parcels}")
        
        # Missing Tax Info
        # Check for NULL tax_status or 'UNKNOWN'
        # Assuming tax_status column exists in parcels based on previous edits
        missing_tax = conn.execute("""
            SELECT COUNT(*) FROM parcels 
            WHERE tax_status IS NULL OR tax_status = 'UNKNOWN'
        """).fetchone()[0]
        print(f"Parcels with Missing/Unknown Tax Status: {missing_tax}")

        # Missing Market Data
        # Join with market_data table if possible or check if attributes are in parcels
        # Based on previous edits, market data is in 'market_data' table linked by folio?
        # Let's check if the table exists first
        has_market_table = conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'market_data'").fetchone()[0]
        if has_market_table:
            # Check how many parcels have entries in market_data
            parcels_with_market = conn.execute("""
                SELECT COUNT(DISTINCT folio) FROM market_data
            """).fetchone()[0]
            print(f"Parcels with Market Data: {parcels_with_market} (Missing: {total_parcels - parcels_with_market})")
        else:
            print("Market Data table not found.")

    except Exception as e:
        print(f"Error checking completeness: {e}")

    # 3. Chain of Title Analysis
    print("\n--- Chain of Title ---")
    try:
        # Check if table exists
        has_chain = conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'chain_of_title'").fetchone()[0]
        if has_chain:
            # Count parcels with ANY chain info
            parcels_with_chain = conn.execute("""
                SELECT COUNT(DISTINCT folio) FROM chain_of_title
            """).fetchone()[0]
            print(f"Parcels with Chain of Title Entries: {parcels_with_chain}")
            print(f"Parcels Missing Chain History: {total_parcels - parcels_with_chain}")
            
            # "Broken Chains" - Heuristic:
            # 1. Current owner (from parcels) not found in chain (timeline gap at end)
            # 2. Gap > X years between transfers? (Hard to do in SQL easily without complex window functions)
            # Let's list parcels with very few chain entries (e.g., < 2, suggesting incomplete)
            
            shallow_chains = conn.execute("""
                SELECT folio, COUNT(*) as entries 
                FROM chain_of_title 
                GROUP BY folio 
                HAVING COUNT(*) < 2
            """).fetchall()
            print(f"Parcels with < 2 Chain Entries (Potentially Broken/Shallow): {len(shallow_chains)}")
            
        else:
            print("Chain of Title table not found.")
            
    except Exception as e:
        print(f"Error checking chain of title: {e}")

    # 4. Enriched Data Check
    print("\n--- Enriched Data ---")
    try:
       # Check for Lat/Lon
       missing_coords = conn.execute("SELECT COUNT(*) FROM parcels WHERE latitude IS NULL OR longitude IS NULL").fetchone()[0]
       print(f"Parcels Missing Coordinates: {missing_coords}")
       
       # Check for Legal Description
       missing_legal = conn.execute("SELECT COUNT(*) FROM parcels WHERE legal_description IS NULL OR legal_description = ''").fetchone()[0]
       print(f"Parcels Missing Legal Description: {missing_legal}")
       
    except Exception as e:
        print(f"Error checking enriched data: {e}")

    conn.close()

if __name__ == "__main__":
    audit_database()
