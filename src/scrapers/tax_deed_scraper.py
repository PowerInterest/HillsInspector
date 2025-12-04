import asyncio
import random
from datetime import date
from typing import List, Optional
from loguru import logger
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeoutError

from src.models.property import Property

USER_AGENT = "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Mobile Safari/537.36,gzip(gfe)"


class TaxDeedScraper:
    BASE_URL = "https://hillsborough.realtaxdeed.com"
    
    async def scrape_date(self, target_date: date) -> List[Property]:
        """
        Scrapes tax deed auction data for a specific date, handling pagination.
        """
        date_str = target_date.strftime("%m/%d/%Y")
        url = f"{self.BASE_URL}/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE={date_str}"
        
        properties = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context(
                user_agent=USER_AGENT
            )
            page = await context.new_page()
            
            try:
                logger.info("Visiting {url} to collect tax deed data for {date}", url=url, date=date_str)
                await page.goto(url, timeout=60000)
                await page.wait_for_load_state("networkidle")
                
                # Check if we are on the right page or if there are no auctions
                content = await page.content()
                if "No auctions found" in content or "no auctions" in content.lower():
                    logger.info("No tax deed auctions found for {date}", date=date_str)
                    return []

                page_num = 1
                while True:
                    logger.info("Scraping tax deed results page {page_num} for {date}", page_num=page_num, date=date_str)
                    
                    # Wait for the table to be visible (with try/except for empty pages)
                    try:
                        await page.wait_for_selector(".Head_W", timeout=10000)
                    except Exception:
                        logger.info("No auction data found on tax deed page {page_num} for {date}", page_num=page_num, date=date_str)
                        break
                        
            except Exception as e:
                logger.error("Error during tax deed scraping for {date}: {error}", date=date_str, error=e)
                await page.screenshot(path=f"error_taxdeed_{date_str.replace('/', '-')}.png")
                raise e
            finally:
                await browser.close()
                
        return properties

    async def _scrape_current_page(self, page: Page, target_date: date) -> List[Property]:
        properties = []
        
        # Find all auction items. 
        # Based on structure, we look for "Case #:" labels and work from there.
        case_labels = await page.locator("text=Case #:").all()
        
        for label in case_labels:
            try:
                # Navigate up to the container (Table Row -> Table Body -> Table)
                item_container = label.locator("xpath=./ancestor::table[1]")
                
                # Helper to extract text by label
                async def get_text_by_label(lbl: str) -> str:
                    row = item_container.locator(f"tr:has-text('{lbl}')")
                    if await row.count() > 0:
                        # Assuming value is in the second cell
                        return await row.locator("td").nth(1).inner_text()
                    return ""

                # Case Number
                case_row = label.locator("xpath=./ancestor::tr[1]")
                case_link = case_row.locator("a")
                case_number = await case_link.inner_text()

                # Certificate Number
                cert_text = await get_text_by_label("Certificate #:")
                
                # Parcel ID
                parcel_id_text = await get_text_by_label("Parcel ID:")
                if "Link" in parcel_id_text or not parcel_id_text.strip():
                     row = item_container.locator("tr:has-text('Parcel ID:')")
                     if await row.count() > 0:
                         parcel_id_text = await row.locator("a").inner_text()

                address = await get_text_by_label("Property Address:")
                value_text = await get_text_by_label("Assessed Value:")
                opening_bid_text = await get_text_by_label("Opening Bid:")
                auction_type = await get_text_by_label("Auction Type:")
                
                prop = Property(
                    case_number=case_number.strip(),
                    certificate_number=cert_text.strip(),
                    parcel_id=parcel_id_text.strip(),
                    address=address.strip(),
                    assessed_value=self._parse_amount(value_text),
                    opening_bid=self._parse_amount(opening_bid_text),
                    auction_date=target_date,
                    auction_type=auction_type.strip()
                )
                properties.append(prop)
                
            except Exception as e:
                logger.error("Error parsing tax deed auction item: {error}", error=e)
                continue
                
        return properties

    def _parse_amount(self, text: str) -> Optional[float]:
        if not text:
            return None
        clean_text = text.replace("$", "").replace(",", "").replace("Hidden", "").strip()
        if not clean_text:
            return None
        try:
            return float(clean_text)
        except ValueError:
            return None

if __name__ == "__main__":
    # Test run
    scraper = TaxDeedScraper()
    # Run for a known date
    asyncio.run(scraper.scrape_date(date(2025, 11, 20)))
