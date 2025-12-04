import duckdb
from pathlib import Path

def check_status():
    db_path = "data/property_master.db"
    if not Path(db_path).exists():
        print(f"Database not found at {db_path}")
        return

    conn = duckdb.connect(db_path)
    
    try:
        # 1. Auction Stats
        print("--- Auction Stats ---")
        total_auctions = conn.execute("SELECT COUNT(*) FROM auctions").fetchone()[0]
        print(f"Total Auctions: {total_auctions}")
        
        status_counts = conn.execute("SELECT status, COUNT(*) FROM auctions GROUP BY status").fetchall()
        for status, count in status_counts:
            print(f"  Status '{status}': {count}")
            
        # 2. Chain of Title Stats
        print("\n--- Chain of Title Stats ---")
        try:
            total_chains = conn.execute("SELECT COUNT(DISTINCT folio) FROM chain_of_title").fetchone()[0]
            print(f"Properties with Chain of Title: {total_chains}")
        except Exception as e:
            print(f"Could not query chain_of_title: {e}")

        # 3. Encumbrance/Lien Stats
        print("\n--- Encumbrance Stats ---")
        try:
            total_encumbrances = conn.execute("SELECT COUNT(*) FROM encumbrances").fetchone()[0]
            print(f"Total Encumbrances: {total_encumbrances}")
            
            survival_counts = conn.execute("SELECT survival_status, COUNT(*) FROM encumbrances GROUP BY survival_status").fetchall()
            for status, count in survival_counts:
                print(f"  Survival '{status}': {count}")
        except Exception as e:
            print(f"Could not query encumbrances: {e}")
            
        # 4. Market Data Stats
        print("\n--- Market Data Stats ---")
        try:
            total_market = conn.execute("SELECT COUNT(*) FROM market_data").fetchone()[0]
            print(f"Properties with Market Data: {total_market}")
        except Exception as e:
             print(f"Could not query market_data: {e}")

    except Exception as e:
        print(f"Error querying database: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_status()
