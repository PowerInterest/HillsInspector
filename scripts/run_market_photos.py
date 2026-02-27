import asyncio
import sqlite3
from loguru import logger
from src.services.market_data_service import MarketDataService

async def main():
    conn = sqlite3.connect('/home/user/hills_data/property_master_sqlite.db')
    conn.row_factory = sqlite3.Row
    
    # Get properties that need market photos (from status table)
    rows = conn.execute("""
        SELECT s.parcel_id as strap, s.case_number, bp.property_address as address
        FROM status s
        LEFT JOIN bulk_parcels bp ON s.parcel_id = bp.strap
        WHERE s.step_homeharvest_enriched IS NOT NULL
          AND s.step_market_fetched IS NULL
    """).fetchall()
    
    properties = []
    for r in rows:
        properties.append({
            'strap': r['strap'],
            'folio': None,  # Can look up if needed, but strap handles MarketDataService
            'case_number': r['case_number'],
            'property_address': r['address']
        })
        
    logger.info(f"Loaded {len(properties)} properties needing Zillow/Realtor check.")
    
    if properties:
        svc = MarketDataService()
        # Only run zillow and realtor to fetch missing photos
        await svc.run_batch(properties, sources=["zillow", "realtor"])
        
        # After success, mark them in sqlite
        for p in properties:
             conn.execute("UPDATE status SET step_market_fetched = CURRENT_TIMESTAMP WHERE case_number = ?", (p['case_number'],))
        conn.commit()

if __name__ == '__main__':
    asyncio.run(main())
