import duckdb
from pathlib import Path
import os
from src.utils.time import ensure_duckdb_utc

DB_PATH = Path("data/history.db")

def init_history_db():
    """Initialize the historical auctions database schema."""
    # Ensure fresh start for schema change
    if DB_PATH.exists():
        try:
            os.remove(DB_PATH)
            print(f"Removed existing database at {DB_PATH}")
        except Exception as e:
            print(f"Warning: Could not remove existing DB: {e}")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    conn = duckdb.connect(str(DB_PATH))
    ensure_duckdb_utc(conn)
    
    # 1. THE BUY: Auction Results
    conn.execute("""
        CREATE TABLE auctions (
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
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # 2. THE EXIT: Resale Events
    conn.execute("""
        CREATE TABLE resales (
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
    
    # 3. THE ASSET: Market Data & Media
    conn.execute("""
        CREATE TABLE property_details (
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
    
    # Verify table creation
    tables = conn.execute("SHOW TABLES").fetchall()
    print(f"Tables created in {DB_PATH}: {tables}")
    
    conn.close()

if __name__ == "__main__":
    init_history_db()
