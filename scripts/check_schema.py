import duckdb
conn = duckdb.connect("data/property_master.db", read_only=True)
print("--- Parcels Columns ---")
print(conn.execute("DESCRIBE parcels").fetchall())
print("\n--- Market Data Columns ---")
try:
    print(conn.execute("DESCRIBE market_data").fetchall())
except Exception as e:
    print(e)
