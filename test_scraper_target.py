import sys
import asyncio
from pathlib import Path

# Fix sys path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from sunbiz.db import resolve_pg_dsn
from sqlalchemy import create_engine, text
from services.market_data_service import MarketDataService


async def run():
    print("Starting manual test run...")

    engine = create_engine(resolve_pg_dsn())
    with engine.connect() as conn:
        test_target_rows = conn.execute(
            text("""
                SELECT f.strap, f.property_address, pm.zillow_json 
                FROM property_market pm
                JOIN foreclosures f ON f.strap = pm.strap
                WHERE pm.zillow_json IS NULL 
                  AND f.property_address IS NOT NULL 
                  AND f.property_address != ''
                LIMIT 10
            """)
        ).fetchall()

    if not test_target_rows:
        print("No valid Zillow-missing targets found.")
        return

    test_targets = [{"strap": r.strap, "property_address": r.property_address} for r in test_target_rows]
    print(f"Found {len(test_targets)} valid properties missing Zillow data. Starting scrape...")

    # Fire the market service loop for these properties
    print("Initializing market service batch worker...")
    service = MarketDataService()
    await service.run_batch(test_targets, sources=["zillow"])

    # Check the database results
    print("DONE. Checking DB lengths:")
    with engine.connect() as conn:
        for t in test_targets:
            res = conn.execute(
                text(
                    "SELECT jsonb_array_length(CASE WHEN jsonb_typeof(photo_cdn_urls)='array' THEN photo_cdn_urls ELSE '[]'::jsonb END), zillow_json->>'_attempted' FROM property_market WHERE strap = :strap"
                ),
                {"strap": t["strap"]},
            ).fetchone()
            print(f"{t['strap']} -> Photos Length: {res[0]}, Zillow Attempted Flag: {res[1]}")


if __name__ == "__main__":
    asyncio.run(run())
