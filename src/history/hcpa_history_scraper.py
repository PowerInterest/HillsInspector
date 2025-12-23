import asyncio
import random
from contextlib import suppress
from loguru import logger
from playwright.async_api import async_playwright
from pathlib import Path

from src.models.property import Property
from src.utils.time import now_utc

# Independent User Agent
USER_AGENT = "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Mobile Safari/537.36,gzip(gfe)"

class HistoricalHCPAScraper:
    """
    Independent scraper for Historical Analysis.
    Does NOT depend on property_master.db or ScraperStorage.
    """
    BASE_URL = "https://gis.hcpafl.org/propertysearch/"
    
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.temp_dir = Path("data/history_scrapes")
        self.temp_dir.mkdir(parents=True, exist_ok=True)
    
    async def enrich_property(self, prop: Property) -> Property:
        if not prop.parcel_id:
            logger.info("Skipping HCPA (History) for {case_number}: missing Parcel ID", case_number=prop.case_number)
            return prop
            
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(user_agent=USER_AGENT)
            page = await context.new_page()
            
            try:
                # logger.info("Visiting {url} for {parcel}", url=self.BASE_URL, parcel=prop.parcel_id)
                await page.goto(self.BASE_URL, timeout=60000)
                await page.wait_for_load_state("networkidle")
                
                # Parcel ID Search
                await page.click("#basic input[name='basicPinGroup'][value='pin']")
                search_box = page.locator("#basic input[data-bind*='value: parcelNumber']")
                await search_box.wait_for(state="visible")
                await asyncio.sleep(random.uniform(0.5, 1.5))  # noqa: S311
                await search_box.fill(prop.parcel_id)
                await page.click("#basic button[data-bind*='click: search']")
                await page.wait_for_timeout(3000)
                
                results_table = page.locator("#table-basic-results")
                
                # Fallback to address if needed (Copied logical flow)
                if not await results_table.is_visible():
                    # Reset and try address
                    await page.reload()
                    await page.wait_for_load_state("networkidle")
                    basic_search_tab = page.locator("li.tab:has-text('Basic Search')")
                    await basic_search_tab.click()
                    await asyncio.sleep(0.5)
                    
                    address_box = page.locator("#basic input[data-bind*='value: address']")
                    await address_box.fill(prop.address)
                    await page.click("#basic button[data-bind*='click: search']")
                    await page.wait_for_timeout(3000)
                
                if await results_table.is_visible():
                    # Click first result
                    await results_table.locator("tbody tr:first-child td").first.click()
                    
                    details_container = page.locator("#details")
                    await details_container.wait_for(state="visible", timeout=15000)
                    
                    with suppress(Exception):
                        await details_container.locator(
                            "h4",
                            has_text="PROPERTY RECORD CARD",
                        ).wait_for(state="visible", timeout=10000)
                    
                    await asyncio.sleep(2.0)

                    # Screenshot for Vision
                    screenshot_bytes = None
                    with suppress(Exception):
                        screenshot_bytes = await details_container.screenshot()
                    if screenshot_bytes is None:
                        screenshot_bytes = await page.screenshot(full_page=True)
                    
                    # Save local temp
                    timestamp = now_utc().strftime("%Y%m%d%H%M%S")
                    filename = f"hcpa_{prop.parcel_id}_{timestamp}.png"
                    full_screenshot_path = self.temp_dir / filename
                    full_screenshot_path.write_bytes(screenshot_bytes)
                    
                    # Vision Service
                    from src.services.vision_service import VisionService
                    vision = VisionService()
                    
                    data = vision.extract_hcpa_details(str(full_screenshot_path))
                    
                    if data:
                        # Extract Sales History (Primary Goal)
                        if "sales_history" in data and isinstance(data["sales_history"], list):
                            prop.sales_history = data["sales_history"]
                            logger.info("Extracted {count} sales for {parcel}", count=len(prop.sales_history), parcel=prop.parcel_id)
                        
                        # Extra metadata if needed (not saving to DB, just return prop)
                        if "owner_info" in data:
                            prop.owner_name = data["owner_info"].get("owner_name")
                            
                else:
                    logger.warning("No results for {parcel}", parcel=prop.parcel_id)
                    
            except Exception as e:
                logger.error("Error scraping history for {parcel}: {e}", parcel=prop.parcel_id, e=e)
            finally:
                await browser.close()
                
        return prop
