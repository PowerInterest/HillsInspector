import sqlite3
from contextlib import suppress
from loguru import logger

from src.db.sqlite_paths import resolve_sqlite_db_path_str


def migrate():
    db_path = resolve_sqlite_db_path_str()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        # Create table if not exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS permits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                folio TEXT,
                permit_number TEXT,
                issue_date TEXT,
                status TEXT,
                permit_type TEXT,
                description TEXT,
                contractor TEXT,
                estimated_cost REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Add new columns (SQLite doesn't support ADD COLUMN IF NOT EXISTS)
        for col_name, col_type in [("url", "TEXT"), ("noc_instrument", "TEXT")]:
            try:
                conn.execute(f"ALTER TABLE permits ADD COLUMN {col_name} {col_type}")
                logger.info(f"Added {col_name} column")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    logger.warning(f"Could not add {col_name} column: {e}")

        conn.commit()
        logger.success("Permits table migration complete")

    except Exception as e:
        logger.error(f"Migration failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
