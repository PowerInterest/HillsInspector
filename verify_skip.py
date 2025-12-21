
import asyncio
from datetime import date
from src.db.operations import PropertyDB
from main import handle_update

# Setup DB to ensure count is > 0
db = PropertyDB()
check_date = date(2026, 1, 15)
count = db.get_auction_count_by_date(check_date)
print(f"Pre-check count for {check_date}: {count}")

if count > 0:
    print("Row exists. Running handle_update expecting SKIP...")
    asyncio.run(handle_update(start_date=check_date, end_date=check_date))
else:
    print("Row missing! Cannot test skip logic.")
