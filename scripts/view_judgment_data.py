"""
View extracted Final Judgment data from the database.
"""
import duckdb
from pathlib import Path
import json
from loguru import logger
import sys

logger.remove()
logger.add(sys.stderr, level="INFO")

def view_data():
    db_path = Path("data/property_master.db")
    if not db_path.exists():
        logger.error("Database not found")
        return

    conn = duckdb.connect(str(db_path))
    
    # Query auctions with extracted data
    query = """
    SELECT 
        case_number,
        foreclosure_type,
        judgment_date,
        total_judgment_amount,
        principal_amount,
        plaintiff,
        judgment_extracted_at
    FROM auctions
    WHERE extracted_judgment_data IS NOT NULL
    ORDER BY judgment_extracted_at DESC
    LIMIT 10
    """
    
    results = conn.execute(query).fetchall()
    
    if not results:
        logger.warning("No extracted judgment data found in database.")
        return

    print("\n=== Extracted Final Judgment Data ===")
    print(f"{'Case Number':<25} | {'Type':<15} | {'Date':<12} | {'Amount':<15} | {'Plaintiff'}")
    print("-" * 100)
    
    for row in results:
        case_num, f_type, j_date, amount, principal, plaintiff, extracted_at = row
        amount_str = f"${amount:,.2f}" if amount else "N/A"
        date_str = str(j_date) if j_date else "N/A"
        type_str = (f_type or "Unknown")[:15]
        plaintiff_str = (plaintiff or "Unknown")[:30]
        
        print(f"{case_num:<25} | {type_str:<15} | {date_str:<12} | {amount_str:<15} | {plaintiff_str}")

    print(f"\nTotal records found: {len(results)}")
    conn.close()

if __name__ == "__main__":
    view_data()
