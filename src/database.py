import duckdb
from typing import List, Dict, Any
import json

class DatabaseManager:
    def __init__(self, db_path: str = "property_data.duckdb"):
        self.con = duckdb.connect(db_path)
        self.initialize_schema()

    def initialize_schema(self):
        """Creates the properties table if it doesn't exist."""
        # Using CREATE OR REPLACE TABLE might destroy data, using IF NOT EXISTS is safer.
        # Ideally, migrations should handle schema changes, but for this scale we can check columns.

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS properties (
                folio_number VARCHAR PRIMARY KEY,
                owner_name VARCHAR,
                address VARCHAR,
                legal_description VARCHAR,
                market_value DOUBLE,
                assessed_value DOUBLE,
                status VARCHAR,
                source_url VARCHAR,
                raw_data JSON,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def save_property(self, data: Dict[str, Any]):
        """
        Upserts a property record.
        """
        # Prepare data for insertion
        # Ensure we only insert columns that exist in the table schema
        # For simplicity in this demo, we assume the dict matches the schema or we align it.

        # We need to handle the dict/JSON conversion for raw_data if it's a dict
        if 'raw_data' in data and isinstance(data['raw_data'], dict):
            data['raw_data'] = json.dumps(data['raw_data'])

        # Align keys with schema (rudimentary check)
        valid_columns = [
            "folio_number", "owner_name", "address", "legal_description",
            "market_value", "assessed_value", "status", "source_url", "raw_data"
        ]

        filtered_data = {k: v for k, v in data.items() if k in valid_columns}

        columns = ', '.join(filtered_data.keys())
        placeholders = ', '.join(['?'] * len(filtered_data))
        values = list(filtered_data.values())

        query = f"INSERT OR REPLACE INTO properties ({columns}) VALUES ({placeholders})"
        self.con.execute(query, values)
        print(f"Saved property: {filtered_data.get('folio_number', 'Unknown')}")

    def get_property(self, folio_number: str):
        return self.con.execute("SELECT * FROM properties WHERE folio_number = ?", [folio_number]).fetchone()

    def close(self):
        self.con.close()
