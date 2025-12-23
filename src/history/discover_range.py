import asyncio
import sys
from datetime import date, timedelta
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from loguru import logger
from src.scrapers.auction_scraper import AuctionScraper
from src.utils.time import today_local

# Configure logger
logger.remove()
logger.add(sys.stderr, level="INFO")

async def probe_date(scraper: AuctionScraper, probe_date: date) -> int:
    """Returns 1 if auctions found, 0 otherwise (using max_properties=1 for speed)."""
    try:
        # Use max_properties=1 to just check for existence without parsing everything
        properties = await scraper.scrape_date(probe_date, fast_fail=True, max_properties=1)
        count = len(properties)
        logger.info(f"Date: {probe_date} | Found: {count > 0}")
        return count
    except Exception as e:
        logger.error(f"Failed to probe {probe_date}: {e}")
        return 0

def safe_replace_year(d: date, new_year: int) -> date:
    try:
        return d.replace(year=new_year)
    except ValueError:
        # Handle Feb 29
        return d + timedelta(days=1)

async def find_event_horizon():
    """Binary search-ish approach to find earliest data."""
    scraper = AuctionScraper()
    # AuctionScraper handles auth/navigation internally in scrape_date

    # Start probing backwards from 30 days ago, in major steps
    current_probe = today_local() - timedelta(days=30)
    
    logger.info("Starting probe backwards from 2025...")
    
    earliest_found = current_probe
    
    # Check 1 year jumps backwards
    for i in range(1, 20):
        probe_year = current_probe.year - i
        # Try a generally safe date: Oct 15th of that year (often active)
        probe = date(probe_year, 10, 15)
        
        # We need a weekday
        while probe.weekday() > 4:
            probe += timedelta(days=1)

        count = await probe_date(scraper, probe)
        
        if count > 0:
            earliest_found = probe
            logger.info(f"FOUND DATA in {probe_year}")
        else:
            # Try one more date in that year (March 15th)
            probe_alt = date(probe_year, 3, 15)
            while probe_alt.weekday() > 4:
                probe_alt += timedelta(days=1)
                
            count_alt = await probe_date(scraper, probe_alt)
            if count_alt > 0:
                 earliest_found = probe_alt
                 logger.info(f"FOUND DATA in {probe_year} (on second attempt)")
            else:
                logger.warning(f"Likely hit limit around {probe_year}")
                break
        
    logger.info(f"Earliest confirmed year: {earliest_found.year}")
    await scraper.cleanup()

if __name__ == "__main__":
    asyncio.run(find_event_horizon())
