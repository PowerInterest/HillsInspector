
import asyncio
import re
from typing import List, Optional
from loguru import logger
from playwright.async_api import async_playwright
from src.services.scraper_storage import ScraperStorage

class TaxScraper:
    BASE_URL = "https://hillsborough.county-taxes.com/public"
    
    def __init__(self, storage: Optional[ScraperStorage] = None):
        self.storage = storage or ScraperStorage()
    
    async def get_tax_liens(self, parcel_id: str) -> List[dict]:
        """
        Searches for unpaid property taxes.
        Returns a list of lien-like dicts (document_type='TAX').
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
                        
                    # Scrape visible text and look for balances
                    text = await page.inner_text("body")
                    amount_due = self._parse_amount_due(text)

                    if amount_due is not None and amount_due > 0:
                        liens.append({
                            "document_type": "TAX",
                            "recording_date": None,
                            "amount": amount_due,
                            "grantor": parcel_id,
                            "grantee": "Hillsborough County Tax Collector",
                            "description": f"Amount due detected on tax site: ${amount_due:,.2f}",
                        })
                        logger.success(f"Detected potential tax lien for {parcel_id}: ${amount_due:,.2f}")
                    elif amount_due == 0:
                        logger.info("Taxes appear paid (amount due parsed as $0.00).")
                    else:
                        lower_text = text.lower()
                        if "paid in full" in lower_text or "no taxes due" in lower_text:
                            logger.info("Taxes appear paid per page text.")
                        else:
                            logger.info("No tax balance detected; manual review may be needed.")
                    
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
            finally:
                await browser.close()
            
        return liens

    @staticmethod
    def _parse_amount_due(text: str) -> Optional[float]:
        """
        Try to extract an "Amount Due" dollar value from page text.
        Returns None if nothing obvious is found.
        """
        if not text:
            return None

        patterns = [
            r"Amount\s+Due[^$]*\$([\d,]+\.\d{2})",
            r"Total\s+Due[^$]*\$([\d,]+\.\d{2})",
            r"Balance\s+Due[^$]*\$([\d,]+\.\d{2})",
        ]

        for pat in patterns:
            match = re.search(pat, text, flags=re.IGNORECASE)
            if match:
                try:
                    return float(match.group(1).replace(",", ""))
                except ValueError:
                    continue

        # Fallback: grab first currency amount that appears after the word "due"
        lower = text.lower()
        idx = lower.find("due")
        if idx != -1:
            snippet = text[idx:]
            match = re.search(r"\$([\d,]+\.\d{2})", snippet)
            if match:
                try:
                    return float(match.group(1).replace(",", ""))
                except ValueError:
                    return None

        return None

if __name__ == "__main__":
    scraper = TaxScraper()
    asyncio.run(scraper.get_tax_liens("1828243EN000007000350A"))
