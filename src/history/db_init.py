import sqlite3
from pathlib import Path
from src.db.sqlite_paths import resolve_sqlite_db_path_str

DB_PATH = resolve_sqlite_db_path_str()

def ensure_history_schema(db_path: str = DB_PATH) -> None:
    """Create history tables/columns without destructive resets."""
    # Ensure parent directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS history_auctions (
                auction_id TEXT PRIMARY KEY,
                auction_date TEXT,
                case_number TEXT,
                parcel_id TEXT,
                property_address TEXT,

                -- Financials at Auction
                winning_bid REAL,
                final_judgment_amount REAL,
                assessed_value REAL,

                -- The Buyer
                sold_to TEXT,
                buyer_normalized TEXT,
                buyer_type TEXT,

                -- Source Data
                auction_url TEXT,
                pdf_url TEXT,
                pdf_path TEXT,

                status TEXT,
                scraped_at TEXT DEFAULT (datetime('now')),

                -- Pipeline Scans
                last_resale_scan_at TEXT,
                last_judgment_scan_at TEXT,

                -- Judgment Fields
                pdf_judgment_amount REAL,
                pdf_principal_amount REAL,
                pdf_interest_amount REAL,
                pdf_attorney_fees REAL,
                pdf_court_costs REAL,
                judgment_red_flags TEXT,
                judgment_data_json TEXT
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS history_resales (
                resale_id TEXT PRIMARY KEY,
                parcel_id TEXT,
                auction_id TEXT,

                -- Sale Details
                sale_date TEXT,
                sale_price REAL,
                sale_type TEXT,

                -- Performance Metrics
                hold_time_days INTEGER,
                gross_profit REAL,
                roi REAL,

                -- Validation
                source TEXT,

                FOREIGN KEY (auction_id) REFERENCES history_auctions(auction_id)
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS history_property_details (
                parcel_id TEXT PRIMARY KEY,

                -- Comparables
                est_market_value REAL,
                est_resale_value REAL,
                value_delta REAL,

                -- Media
                primary_image_url TEXT,
                gallery_json TEXT,
                description TEXT,

                updated_at TEXT DEFAULT (datetime('now'))
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS history_scraped_dates (
                auction_date TEXT PRIMARY KEY,
                scraped_at TEXT DEFAULT (datetime('now')),
                status TEXT
            );
        """)

        # Safely add columns that may not exist yet.
        # SQLite doesn't support ADD COLUMN IF NOT EXISTS,
        # so we check pragma table_info first.
        _safe_add_columns(conn, "history_auctions", [
            ("pdf_path", "TEXT"),
            ("status", "TEXT"),
            ("scraped_at", "TEXT"),
            ("last_resale_scan_at", "TEXT"),
            ("last_judgment_scan_at", "TEXT"),
            ("pdf_judgment_amount", "REAL"),
            ("pdf_principal_amount", "REAL"),
            ("pdf_interest_amount", "REAL"),
            ("pdf_attorney_fees", "REAL"),
            ("pdf_court_costs", "REAL"),
            ("judgment_red_flags", "TEXT"),
            ("judgment_data_json", "TEXT"),
        ])
        _safe_add_columns(conn, "history_resales", [
            ("roi", "REAL"),
        ])
        _safe_add_columns(conn, "history_scraped_dates", [
            ("status", "TEXT"),
        ])

        conn.commit()
    finally:
        conn.close()


def _safe_add_columns(conn: sqlite3.Connection, table: str, columns: list[tuple[str, str]]) -> None:
    """Add columns to a table only if they don't already exist."""
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for col_name, col_type in columns:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")


def init_history_db():
    """Initialize the historical auctions database schema."""
    ensure_history_schema(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    try:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'history_%'"
        ).fetchall()
        print(f"History tables in {DB_PATH}: {[t[0] for t in tables]}")
    finally:
        conn.close()

if __name__ == "__main__":
    init_history_db()
