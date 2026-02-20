"""
Migration Script: Legacy History SQLite -> PostgreSQL.
Merges data from history.db into historical_auctions table.
"""

from __future__ import annotations

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import sqlite3
from loguru import logger
from sqlalchemy.orm import Session
from src.services.history import HistoryService, ExtractedHistoryRecord

def migrate_legacy_sqlite(sqlite_path: Path, pg_dsn: str | None = None):
    if not sqlite_path.exists():
        logger.error(f"Legacy SQLite not found: {sqlite_path}")
        return

    service = HistoryService(dsn=pg_dsn)
    
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    
    try:
        rows = conn.execute("SELECT * FROM auctions").fetchall()
        logger.info(f"Found {len(rows)} legacy records in SQLite.")
        
        records = []
        with Session(service.engine) as session:
            for row in rows:
                # Map legacy SQLite to Pydantic/PG schema
                # We prioritize the unmasked case_number if available from HTML,
                # but for legacy data we take what we have.
                record = ExtractedHistoryRecord(
                    listing_id=str(row["auction_id"]),
                    case_number=row["case_number"],
                    auction_date=row["auction_date"],
                    auction_status=row["status"],
                    folio=row["folio"],
                    strap=row["strap"],
                    property_address=row["property_address"],
                    winning_bid=row["winning_bid"],
                    final_judgment_amount=row["final_judgment_amount"],
                    bedrooms=row["bedrooms"],
                    bathrooms=row["bathrooms"],
                    sqft_total=int(row["sqft"]) if row["sqft"] else None,
                    sold_to=row["sold_to"]
                )
                records.append(record)
                
                if len(records) >= 100:
                    service.upsert_records(session, records)
                    records = []
            
            if records:
                service.upsert_records(session, records)
                
        logger.success(f"Migrated {len(rows)} records to PostgreSQL.")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    sqlite_db = Path("data/history.db")
    migrate_legacy_sqlite(sqlite_db)
