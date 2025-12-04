import duckdb
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "property_master.db"

def get_connection():
    """Return a DuckDB connection, creating tables if they don't exist."""
    con = duckdb.connect(database=str(DB_PATH), read_only=False)
    _create_tables(con)
    return con

def _create_tables(con: duckdb.DuckDBPyConnection):
    # Simple schema for demonstration
    con.execute("""
    CREATE TABLE IF NOT EXISTS properties (
        folio VARCHAR PRIMARY KEY,
        address VARCHAR,
        owner VARCHAR,
        value DOUBLE,
        auction_date DATE,
        status VARCHAR
    )
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS documents (
        id BIGINT PRIMARY KEY,
        folio VARCHAR,
        doc_type VARCHAR,
        doc_date DATE,
        content TEXT
    )
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS auctions (
        folio VARCHAR PRIMARY KEY,
        auction_date DATE,
        status VARCHAR,
        equity DOUBLE
    )
    """)
