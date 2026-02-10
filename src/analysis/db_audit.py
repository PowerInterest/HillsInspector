import sqlite3
import os

from src.db.sqlite_paths import resolve_sqlite_db_path_str

DB_PATH = resolve_sqlite_db_path_str()


def _fetch_count(conn: sqlite3.Connection, query: str) -> int:
    row = conn.execute(query).fetchone()
    return row[0] if row else 0


def run_audit():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    print("=== DATABASE AUDIT REPORT ===")

    # 1. Auction Counts per Day
    print("\n--- Auction Counts per Day (Last 60 Days) ---")

    query_daily = """
        SELECT auction_date, COUNT(*) as count
        FROM auctions
        WHERE auction_date >= date('now', '-60 days')
        GROUP BY auction_date
        ORDER BY auction_date
    """
    days = conn.execute(query_daily).fetchall()
    for row in days:
        print(f"{row[0]}: {row[1]}")

    # 2. Missing Critical Fields
    print("\n--- Missing Critical Fields ---")

    # Missing Legal Description
    missing_legal = _fetch_count(
        conn,
        "SELECT COUNT(*) FROM parcels WHERE legal_description IS NULL OR legal_description = ''",
    )
    total_parcels = _fetch_count(conn, "SELECT COUNT(*) FROM parcels")
    if total_parcels > 0:
        print(f"Missing Legal Descriptions: {missing_legal} / {total_parcels} ({(missing_legal/total_parcels)*100:.1f}%)")
    else:
        print(f"Missing Legal Descriptions: {missing_legal} / {total_parcels}")

    # Missing geocoding
    missing_geo = _fetch_count(conn, "SELECT COUNT(*) FROM parcels WHERE latitude IS NULL")
    print(f"Missing Geocoding: {missing_geo} / {total_parcels}")

    # 3. Tax Scrape Coverage
    print("\n--- Tax & HCPA Scrape Coverage ---")
    missing_market = _fetch_count(conn, "SELECT COUNT(*) FROM parcels WHERE market_value IS NULL")
    print(f"Missing Market Value: {missing_market} / {total_parcels}")

    conn.close()

if __name__ == "__main__":
    run_audit()
