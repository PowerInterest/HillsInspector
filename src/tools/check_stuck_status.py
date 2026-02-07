
import sys
from datetime import date, timedelta
from src.orchestrator import _display_status_summary
from src.db.operations import PropertyDB

def main():
    try:
        # Default range usually covers "upcoming" auctions
        start_date = date.today()
        # But since the log was from 2025-12-31, maybe we should check that range
        # Actually, let's just check the last few days and next 30 days
        start_check = date(2025, 12, 30)
        end_check = start_check + timedelta(days=60)
        
        print(f"Checking status from {start_check} to {end_check}")
        
        with PropertyDB() as db:
            _display_status_summary(db, start_check, end_check)
            
    except Exception as e:
        print(f"Error checking status: {e}")

if __name__ == "__main__":
    main()
