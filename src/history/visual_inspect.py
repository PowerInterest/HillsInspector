import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from loguru import logger
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

async def inspect_date(target_date: str):
    url = f"https://hillsborough.realforeclose.com/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE={target_date}"
    
    async with async_playwright() as p:
        # Launch with specific args to help in WSL/Container envs
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        
        logger.info(f"Navigating to {url}")
        await page.goto(url)
        
        # Explicitly wait for the auction items to load
        try:
            await page.wait_for_selector(".AUCTION_ITEM", timeout=15000)
            logger.info("Auction Items loaded.")
        except Exception as e:
            logger.error(f"Timed out waiting for .AUCTION_ITEM ({e}). Taking debug screenshot...")
            await page.screenshot(path="debug_timeout.png", full_page=True)
            content = await page.content()
            logger.info(f"Page Content Snippet: {content[:1000]}")
            await browser.close()
            return

        # Take screenshot of list view
        await page.screenshot(path="list_view.png", full_page=True)
        logger.info("Saved list_view.png")
        
        items = page.locator(".AUCTION_ITEM")
        count = await items.count()
        logger.info(f"Found {count} items.")

        found_sold_example = False
        
        for i in range(count):
            item = items.nth(i)
            text = await item.inner_text()
            
            # Check for Sold status
            if "Sold" in text or "Third Party" in text:
                found_sold_example = True
                logger.info(f"--- Found Sold Auction (Index {i}) ---")
                logger.info(f"Card Text:\n{text}")
                
                # Check for "Sold To" in list view
                if "Sold To" in text:
                    logger.info("CONFIRMED: 'Sold To' IS visible in List View")
                else:
                    logger.info("OBSERVATION: 'Sold To' NOT found in List View text")

                # Try to get details
                # The site triggers a detail view often by a specific click or it's already there in the table.
                # Let's inspect the table structure.
                details_table = item.locator("table.ad_tab")
                if await details_table.count() > 0:
                     rows = await details_table.locator("tr").all_inner_texts()
                     logger.info(f"Details Table Rows: {rows}")
                
                break

        if not found_sold_example:
            logger.warning("No 'Sold' auctions found on this page to analyze.")
            
        await browser.close()

if __name__ == "__main__":
    # Use a date known to have sales - March 15 2024 was confirmed by discovery script
    asyncio.run(inspect_date("03/15/2024"))
