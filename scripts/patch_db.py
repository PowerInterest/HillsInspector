import duckdb
from src.db.operations import PropertyDB

def patch_market_data_schema():
    print("Patching market_data schema...")
    db = PropertyDB()
    conn = db.connect()
    
    try:
        # Create sequence if not exists
        conn.execute("CREATE SEQUENCE IF NOT EXISTS market_data_id_seq START 1")
        
        # Check if id column has default value
        # DuckDB doesn't easily support ALTER COLUMN SET DEFAULT in all versions, 
        # but we can try.
        try:
            conn.execute("ALTER TABLE market_data ALTER COLUMN id SET DEFAULT nextval('market_data_id_seq')")
            print("Successfully updated market_data.id default value.")
        except Exception as e:
            print(f"Could not alter column directly: {e}")
            print("Recreating table...")
            
            # Recreate table strategy
            conn.execute("BEGIN TRANSACTION")
            conn.execute("ALTER TABLE market_data RENAME TO market_data_old")
            conn.execute("""
                CREATE TABLE market_data (
                    id INTEGER PRIMARY KEY DEFAULT nextval('market_data_id_seq'),
                    folio VARCHAR,
                    source VARCHAR,
                    capture_date DATE,
                    listing_status VARCHAR,
                    list_price FLOAT,
                    zestimate FLOAT,
                    rent_estimate FLOAT,
                    hoa_monthly FLOAT,
                    days_on_market INTEGER,
                    price_history VARCHAR,
                    raw_json VARCHAR,
                    screenshot_path VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("INSERT INTO market_data SELECT * FROM market_data_old")
            conn.execute("DROP TABLE market_data_old")
            conn.execute("COMMIT")
            print("Table recreated with sequence.")
            
    except Exception as e:
        print(f"Error patching schema: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    patch_market_data_schema()
