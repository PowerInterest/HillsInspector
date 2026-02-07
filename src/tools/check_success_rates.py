
import sqlite3
from pathlib import Path
from loguru import logger

DB_PATH = "data/property_master_sqlite.db"

def check_stats():
    if not Path(DB_PATH).exists():
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    
    try:
        # 1. Total Auctions (Base)
        total_cursor = conn.execute("SELECT COUNT(*) FROM auctions")
        total_auctions = total_cursor.fetchone()[0]
        
        if total_auctions == 0:
            print("No auctions found in DB.")
            return

        print(f"Total Auctions: {total_auctions}")

        # 2. Step Completion Rates from 'status' table
        try:
            # Table is likely 'status' not 'property_status' based on operations.py
            # Check if table exists
            table_check = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='status'").fetchone()
            if not table_check:
                print("Status table not found.")
                return

            # Columns are like 'step_auction_scraped', 'step_hcpa_enriched', etc.
            # We want to count non-nulls for these columns.
            steps = [
                "step_auction_scraped",
                "step_pdf_downloaded",
                "step_judgment_extracted",
                "step_hcpa_enriched",
                "step_permits_checked",
                "step_survival_analyzed" # or similar
            ]
            
            # Verify columns exist first to avoid crash
            cursor = conn.execute("SELECT * FROM status LIMIT 0")
            columns = [description[0] for description in cursor.description]
            valid_steps = [s for s in steps if s in columns]

            if not valid_steps:
                print("No step columns found in status table.")
                print(f"Available columns: {columns}")
                return

            print("\n--- Step Success Rates ---")
            for step in valid_steps:
                # Count non-null timestamps
                count = conn.execute(f"SELECT COUNT(*) FROM status WHERE {step} IS NOT NULL").fetchone()[0]
                pct = (count / total_auctions) * 100
                print(f"{step}: {count}/{total_auctions} ({pct:.1f}%)")

        except Exception as e:
            print(f"Error checking status table: {e}")

    finally:
        conn.close()

if __name__ == "__main__":
    check_stats()
