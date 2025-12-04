
import asyncio
from typing import List, Optional
from loguru import logger
from playwright.async_api import async_playwright
from src.models.property import Lien
from src.services.scraper_storage import ScraperStorage

class TaxScraper:
    BASE_URL = "https://hillsborough.county-taxes.com/public"
    
    def __init__(self, storage: Optional[ScraperStorage] = None):
        self.storage = storage or ScraperStorage()
    
    async def get_tax_liens(self, parcel_id: str) -> List[Lien]:
        """
        Searches for unpaid property taxes.
        Returns a list of Liens (type='TAX').
        """
        logger.info(f"Searching Tax Collector for Parcel ID: {parcel_id}")
        liens = []
        
        async with async_playwright() as p:
            # User requested to surface browser
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context(viewport={'width': 1280, 'height': 720})
            page = await context.new_page()
            
            try:
                logger.info(f"Navigating to {self.BASE_URL}...")
                await page.goto(self.BASE_URL, timeout=60000)
                
                # Wait for load - networkidle can be flaky on these sites
                await page.wait_for_load_state("domcontentloaded")
                await asyncio.sleep(5) # Allow JS to init
                
                # Check for challenge
                if "challenge" in page.url:
                    logger.warning("Cloudflare challenge detected. Waiting...")
                    await asyncio.sleep(10)
                
                # Try to find search input
                # Common selectors for Grant Street Group sites
                search_input = page.locator("input[name='search_text'], input[placeholder*='Search'], input[type='search']").first
                
                if await search_input.count() > 0:
                    logger.info(f"Found search input. Searching for {parcel_id}...")
                    await search_input.fill(parcel_id)
                    await search_input.press("Enter")
                    
                    await page.wait_for_load_state("domcontentloaded")
                    await asyncio.sleep(5)
                    await asyncio.sleep(2)
                    
                    # Check for "No results"
                    if await page.locator("text=No results").count() > 0:
                        logger.info("No tax records found.")
                        return []
                        
                    # Check for "Amount Due" or similar
                    # This is highly dependent on the site structure
                    # We'll look for text containing "$" and "Due"
                    
                    # Dump text to debug if needed
                    # text = await page.inner_text("body")
                    
                    # Placeholder logic:
                    # If we see "Amount Due: $X.XX", we assume it's a lien
                    # This needs refinement based on actual HTML
                    
                    # For now, if we can't verify, we return empty list but log warning
                    logger.info("Tax search completed (structure verification needed).")
                    
                    # Save screenshot for debugging
                    screenshot_bytes = await page.screenshot()
                    screenshot_path = self.storage.save_screenshot(
                        property_id=parcel_id,
                        scraper="tax_collector",
                        image_data=screenshot_bytes,
                        context="search_results"
                    )
                    logger.info(f"Screenshot saved to {screenshot_path}")
                    
                else:
                    logger.error("Could not find search input on Tax site.")
                    
            except Exception as e:
                logger.error(f"Error scraping Tax site: {e}")
            # finally:
            #     await browser.close()
            logger.info("Browser left open for user inspection.")
                
        return liens

if __name__ == "__main__":
    scraper = TaxScraper()
    asyncio.run(scraper.get_tax_liens("1828243EN000007000350A"))
