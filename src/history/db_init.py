import duckdb
from pathlib import Path
import os
from src.utils.time import ensure_duckdb_utc

DB_PATH = Path("data/history.db")

def ensure_history_schema(db_path: Path = DB_PATH) -> None:
    """Create history tables/columns without destructive resets."""
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(db_path))
    ensure_duckdb_utc(conn)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS auctions (
                auction_id VARCHAR PRIMARY KEY, -- (case_number + date)
                auction_date DATE,
                case_number VARCHAR,
                parcel_id VARCHAR,
                property_address VARCHAR,

                -- Financials at Auction
                winning_bid DOUBLE,             -- Acquisition Cost
                final_judgment_amount DOUBLE,   -- Debt Load
                assessed_value DOUBLE,          -- Gov Value at Auction Time

                -- The Buyer
                sold_to VARCHAR,                -- Raw Name
                buyer_normalized VARCHAR,       -- Cleaned Name
                buyer_type VARCHAR,             -- 'Third Party', 'Plaintiff', 'Individual'

                -- Source Data
                auction_url VARCHAR,
                pdf_url VARCHAR,
                pdf_path VARCHAR,

                status VARCHAR,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                -- Pipeline Scans
                last_resale_scan_at TIMESTAMP,
                last_judgment_scan_at TIMESTAMP,

                -- Judgment Fields
                pdf_judgment_amount DOUBLE,
                pdf_principal_amount DOUBLE,
                pdf_interest_amount DOUBLE,
                pdf_attorney_fees DOUBLE,
                pdf_court_costs DOUBLE,
                judgment_red_flags JSON,
                judgment_data_json JSON
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS resales (
                resale_id VARCHAR PRIMARY KEY,
                parcel_id VARCHAR,
                auction_id VARCHAR, -- Foreign Key to auctions.auction_id

                -- Sale Details
                sale_date DATE,
                sale_price DOUBLE,
                sale_type VARCHAR,

                -- Performance Metrics
                hold_time_days INTEGER,
                gross_profit DOUBLE,
                roi DOUBLE,

                -- Validation
                source VARCHAR,

                FOREIGN KEY (auction_id) REFERENCES auctions(auction_id)
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS property_details (
                parcel_id VARCHAR PRIMARY KEY,

                -- Comparables
                est_market_value DOUBLE,
                est_resale_value DOUBLE,
                value_delta DOUBLE,

                -- Media
                primary_image_url VARCHAR,
                gallery_json JSON,
                description TEXT,

                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS scraped_dates (
                auction_date DATE PRIMARY KEY,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR
            );
        """)

        columns = [
            ("auctions", "pdf_path", "VARCHAR"),
            ("auctions", "status", "VARCHAR"),
            ("auctions", "scraped_at", "TIMESTAMP"),
            ("auctions", "last_resale_scan_at", "TIMESTAMP"),
            ("auctions", "last_judgment_scan_at", "TIMESTAMP"),
            ("auctions", "pdf_judgment_amount", "DOUBLE"),
            ("auctions", "pdf_principal_amount", "DOUBLE"),
            ("auctions", "pdf_interest_amount", "DOUBLE"),
            ("auctions", "pdf_attorney_fees", "DOUBLE"),
            ("auctions", "pdf_court_costs", "DOUBLE"),
            ("auctions", "judgment_red_flags", "JSON"),
            ("auctions", "judgment_data_json", "JSON"),
            ("resales", "roi", "DOUBLE"),
            ("scraped_dates", "status", "VARCHAR"),
        ]

        for table, column, col_type in columns:
            conn.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {col_type}"
            )
    finally:
        conn.close()

def init_history_db():
    """Initialize the historical auctions database schema."""
    # Ensure fresh start for schema change
    if DB_PATH.exists():
        try:
            os.remove(DB_PATH)
            print(f"Removed existing database at {DB_PATH}")
        except Exception as e:
            print(f"Warning: Could not remove existing DB: {e}")

    ensure_history_schema(DB_PATH)

    conn = duckdb.connect(str(DB_PATH))
    try:
        tables = conn.execute("SHOW TABLES").fetchall()
        print(f"Tables created in {DB_PATH}: {tables}")
    finally:
        conn.close()

if __name__ == "__main__":
    init_history_db()
