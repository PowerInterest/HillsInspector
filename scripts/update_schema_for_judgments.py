"""
Update database schema to store Final Judgment extraction data.

Adds columns to the auctions table for:
- Plaintiff/defendant information
- Foreclosure type and dates
- Financial amounts (judgment, principal, interest, fees)
- Raw extracted data (JSON and text)
"""

import duckdb
from pathlib import Path
from loguru import logger
import sys

logger.remove()
logger.add(sys.stderr, level="INFO")


def update_schema():
    """Add Final Judgment columns to auctions table."""
    
    db_path = Path("data/property_master.db")
    
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        logger.info("Run setup_db.py first to create the database")
        return False
    
    logger.info(f"Connecting to database: {db_path}")
    conn = duckdb.connect(str(db_path))
    
    # List of columns to add
    columns_to_add = [
        ("plaintiff", "TEXT", "Name of foreclosing party"),
        ("defendant", "TEXT", "Name(s) of property owner(s)"),
        ("foreclosure_type", "TEXT", "FIRST MORTGAGE | SECOND MORTGAGE | HOA | TAX | OTHER"),
        ("judgment_date", "DATE", "Date the judgment was entered"),
        ("lis_pendens_date", "DATE", "Date Lis Pendens was filed"),
        ("foreclosure_sale_date", "DATE", "Scheduled auction/sale date"),
        ("total_judgment_amount", "REAL", "Total amount awarded"),
        ("principal_amount", "REAL", "Original loan/debt amount"),
        ("interest_amount", "REAL", "Accrued interest"),
        ("attorney_fees", "REAL", "Attorney fees"),
        ("court_costs", "REAL", "Court costs"),
        ("original_mortgage_amount", "REAL", "Original mortgage principal"),
        ("original_mortgage_date", "DATE", "Date of original mortgage"),
        ("monthly_payment", "REAL", "Monthly payment amount"),
        ("default_date", "DATE", "Date of default"),
        ("extracted_judgment_data", "JSON", "Full extracted data as JSON"),
        ("raw_judgment_text", "TEXT", "Raw OCR text from Final Judgment"),
        ("judgment_extracted_at", "TIMESTAMP", "When extraction was performed"),
    ]
    
    logger.info(f"Adding {len(columns_to_add)} columns to auctions table...")
    
    added_count = 0
    skipped_count = 0
    
    for col_name, col_type, description in columns_to_add:
        try:
            # Check if column already exists
            result = conn.execute(f"""
                SELECT COUNT(*) 
                FROM information_schema.columns 
                WHERE table_name = 'auctions' 
                AND column_name = '{col_name}'
            """).fetchone()
            
            if result[0] > 0:
                logger.debug(f"Column '{col_name}' already exists, skipping")
                skipped_count += 1
                continue
            
            # Add the column
            conn.execute(f"""
                ALTER TABLE auctions 
                ADD COLUMN {col_name} {col_type}
            """)
            
            logger.success(f"✓ Added column: {col_name} ({col_type}) - {description}")
            added_count += 1
            
        except Exception as e:
            logger.error(f"Failed to add column '{col_name}': {e}")
    
    conn.close()
    
    logger.info("\n" + "=" * 60)
    logger.info("SCHEMA UPDATE COMPLETE")
    logger.info("=" * 60)
    logger.success(f"Added: {added_count} columns")
    logger.info(f"Skipped (already exist): {skipped_count} columns")
    logger.info("=" * 60)
    
    return True


if __name__ == "__main__":
    success = update_schema()
    sys.exit(0 if success else 1)
