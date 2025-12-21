
import duckdb
from datetime import datetime, date, timedelta, UTC
from src.db.operations import PropertyDB

db = PropertyDB()
# Mocking the loop in main.py
start_date = datetime.now(tz=UTC).date() # Current date 2025-12-20
# We want to check 2025-12-30 specifically
target_date = date(2025, 12, 30)

print(f"Target Date: {target_date} (Type: {type(target_date)})")

# Direct call
try:
    count = db.get_auction_count_by_date(target_date)
    print(f"Count for {target_date}: {count}")
except Exception as e:
    print(f"Error: {e}")

# Check what's actually in DB for that date to be sure format matches
conn = duckdb.connect("data/property_master.db", read_only=True)
rows = conn.execute("SELECT auction_date FROM auctions WHERE auction_date = '2025-12-30'").fetchall()
print(f"DB Rows value: {rows}")
