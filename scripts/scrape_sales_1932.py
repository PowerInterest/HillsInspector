from src.scrapers.hcpa_gis_scraper import scrape_hcpa_property
from src.services.scraper_storage import ScraperStorage
from src.db.operations import PropertyDB
import asyncio

async def main():
    folio = '193206C07000000005160U'
    
    # Try using the folio/strap directly as it seems to match the R-T-S format
    # PIN: U-06-32-19... -> 193206...
    parcel_id = "193206C07000000005160U"
    
    print(f"Scraping sales history for {folio} (Parcel: {parcel_id})...")
    
    storage = ScraperStorage()
    # Pass parcel_id to skip search page
    result = await scrape_hcpa_property(parcel_id=parcel_id, storage=storage)
    
    print("Sales History found:", len(result.get('sales_history', [])))
    for sale in result.get('sales_history', []):
        print(sale)
        
    # Save to DB if found (the scraper returns dict, doesn't save to DB directly? 
    # Wait, the pipeline calls db.save_sales_history. The scraper just returns data + saves raw JSON.)
    
    if result.get('sales_history'):
        db = PropertyDB()
        # The scraper returns 'folio' (normalized?) and 'strap'. 
        # We need to ensure we use the correct keys.
        # Check result keys.
        res_folio = result.get('folio') or folio
        res_strap = result.get('strap') or folio # Fallback
        
        print(f"Saving to DB for folio {res_folio}")
        db.save_sales_history(res_folio, res_strap, result['sales_history'])
        print("Saved.")

if __name__ == "__main__":
    asyncio.run(main())
