import asyncio
from datetime import date
from src.history.scrape_history import HistoricalScraper

async def test_single_date():
    scraper = HistoricalScraper()
    # Try a date likely to have data
    target = date(2025, 12, 10) # a weekday
    print(f"Testing scrape for {target}...")
    results = await scraper.scrape_single_date(target)
    print(f"Found {len(results)} auctions.")
    if results:
        scraper.save_batch(results)
        print("Saved to DB.")

if __name__ == "__main__":
    asyncio.run(test_single_date())
