
import sqlite3

from pathlib import Path

from loguru import logger

from src.db.sqlite_paths import resolve_sqlite_db_path_str

DB_PATH = resolve_sqlite_db_path_str()


def check_stats():
    if not Path(DB_PATH).exists():
        logger.error("Database not found at {}", DB_PATH)
        return

    conn = sqlite3.connect(DB_PATH)

    try:
        # 1. Total Auctions (Base)
        total_cursor = conn.execute("SELECT COUNT(*) FROM auctions")
        total_auctions = total_cursor.fetchone()[0]

        if total_auctions == 0:
            logger.warning("No auctions found in DB")
            return

        logger.info("Total auctions in DB: {}", total_auctions)

        # 2. Step Completion Rates from 'status' table
        try:
            # Table is likely 'status' not 'property_status' based on operations.py
            # Check if table exists
            table_check = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='status'").fetchone()
            if not table_check:
                logger.warning("Status table not found")
                return

            # Columns are like 'step_auction_scraped', 'step_hcpa_enriched', etc.
            # We want to count non-nulls for these columns.
            steps = [
                "step_auction_scraped",
                "step_pdf_downloaded",
                "step_judgment_extracted",
                "step_hcpa_enriched",
                "step_permits_checked",
                "step_survival_analyzed",  # or similar
            ]

            # Verify columns exist first to avoid crash
            cursor = conn.execute("SELECT * FROM status LIMIT 0")
            columns = [description[0] for description in cursor.description]
            valid_steps = [s for s in steps if s in columns]

            if not valid_steps:
                logger.warning("No step columns found in status table")
                logger.info("Available status columns: {}", columns)
                return

            logger.info("--- Step Success Rates ---")
            for step in valid_steps:
                # Count non-null timestamps
                count = conn.execute(f"SELECT COUNT(*) FROM status WHERE {step} IS NOT NULL").fetchone()[0]
                pct = (count / total_auctions) * 100
                logger.info("{}: {}/{} ({:.1f}%)", step, count, total_auctions, pct)

        except Exception as e:
            logger.exception("Error checking status table: {}", e)

    finally:
        conn.close()


if __name__ == "__main__":
    check_stats()
