import duckdb
from typing import List, Dict, Any

class DatabaseManager:
    def __init__(self, db_path: str = "property_data.duckdb"):
        self.con = duckdb.connect(db_path)
        self.initialize_schema()

    def initialize_schema(self):
        """Creates the properties table if it doesn't exist."""
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS properties (
                folio_number VARCHAR PRIMARY KEY,
                owner_name VARCHAR,
                address VARCHAR,
                legal_description VARCHAR,
                market_value DOUBLE,
                assessed_value DOUBLE,
                status VARCHAR,
                data_source VARCHAR,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # We can add more tables (e.g., for auctions, liens) as needed.

    def save_property(self, data: Dict[str, Any]):
        """
        Upserts a property record.
        """
        # DuckDB's UPSERT syntax: INSERT OR REPLACE INTO...
        # Note: keys in `data` must match column names.

        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        values = list(data.values())

        query = f"INSERT OR REPLACE INTO properties ({columns}) VALUES ({placeholders})"
        self.con.execute(query, values)
        print(f"Saved property: {data.get('folio_number', 'Unknown')}")

    def get_property(self, folio_number: str):
        return self.con.execute("SELECT * FROM properties WHERE folio_number = ?", [folio_number]).fetchone()

    def close(self):
        self.con.close()
