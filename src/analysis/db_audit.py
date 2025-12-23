import duckdb
import os
from src.utils.time import ensure_duckdb_utc

DB_PATH = "data/property_master.db"


def _fetch_count(conn: duckdb.DuckDBPyConnection, query: str) -> int:
    row = conn.execute(query).fetchone()
    return row[0] if row else 0


def run_audit():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    conn = duckdb.connect(DB_PATH, read_only=True)
    ensure_duckdb_utc(conn)
    
    print("=== DATABASE AUDIT REPORT ===")
    
    # 1. Auction Counts per Day
    print("\n--- Auction Counts per Day (Last 60 Days) ---")
    
    query_daily = """
        SELECT auction_date, COUNT(*) as count 
        FROM auctions 
        WHERE auction_date >= (CURRENT_DATE - INTERVAL '60 days')
        GROUP BY auction_date 
        ORDER BY auction_date
    """
    days = conn.execute(query_daily).fetchall()
    for d, c in days:
        print(f"{d}: {c}")

    # 2. Missing Critical Fields
    print("\n--- Missing Critical Fields ---")
    
    # Missing Legal Description
    missing_legal = _fetch_count(
        conn,
        "SELECT COUNT(*) FROM parcels WHERE legal_description IS NULL OR legal_description = ''",
    )
    total_parcels = _fetch_count(conn, "SELECT COUNT(*) FROM parcels")
    print(f"Missing Legal Descriptions: {missing_legal} / {total_parcels} ({(missing_legal/total_parcels)*100:.1f}%)")

    # Missing Extracted Judgment Data (for properties that have final judgments)
    # Checking auctions table for final_judgment_amount > 0 but no extracted data? 
    # Actually, we store extracted text in parcels or use a separate check.
    # Let's check how many auctions have a lat/lon (geocoded)
    missing_geo = _fetch_count(conn, "SELECT COUNT(*) FROM parcels WHERE latitude IS NULL")
    print(f"Missing Geocoding: {missing_geo} / {total_parcels}")
    
    # 3. Tax Scrape Coverage
    print("\n--- Tax & HCPA Scrape Coverage ---")
    # Check if we have tax info
    # We can check if `last_tax_scrape_date` is populated in parcels if that column exists, 
    # or infer from other fields. 
    # Let's check 'market_value' which comes from HCPA/Realtor.
    missing_market = _fetch_count(conn, "SELECT COUNT(*) FROM parcels WHERE market_value IS NULL")
    print(f"Missing Market Value: {missing_market} / {total_parcels}")

    conn.close()

if __name__ == "__main__":
    run_audit()
