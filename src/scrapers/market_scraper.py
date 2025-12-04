"""
Market data scraper for Zillow/Realtor.com using VisionService.

NOTE: This scraper uses screenshot + Vision API approach due to aggressive
bot detection on real estate sites. Uses playwright-stealth for better success rates.
"""
import asyncio
import json
import random
from pathlib import Path
from typing import Optional
from datetime import datetime
from loguru import logger

from src.services.scraper_storage import ScraperStorage

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

async def stealth_async(page):
    """Apply stealth settings to a page."""
    await Stealth().apply_stealth_async(page)

from src.models.property import ListingDetails
from src.services.vision_service import VisionService


class MarketScraper:
    """Scraper for Zillow and Realtor.com using VisionService (Qwen3-VL)."""

    def __init__(self, headless: bool = False, storage: Optional[ScraperStorage] = None):
        """
        Initialize the market scraper.

        Args:
            headless: Run browser in headless mode (not recommended for these sites)
            storage: Optional ScraperStorage instance
        """
        self.headless = headless
        self.vision = VisionService()
        self.storage = storage or ScraperStorage()

    async def _human_like_delay(self, min_sec: float = 0.5, max_sec: float = 4.0):
        """Add random human-like delay."""
        await asyncio.sleep(random.uniform(min_sec, max_sec))

    async def _setup_stealth_context(self, playwright):
        """Create a stealth browser context with anti-detection measures."""
        browser = await playwright.chromium.launch(headless=self.headless)

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080},
            screen={'width': 1920, 'height': 1080},
            locale='en-US',
            timezone_id='America/New_York',
            extra_http_headers={
                'Accept-Language': 'en-US,en;q=0.9',
            },
        )

        # Create page and apply stealth
        page = await context.new_page()
        await stealth_async(page)

        return browser, context, page

    async def get_listing_details(
        self,
        address: str,
        city: str,
        state: str,
        zip_code: str,
        property_id: Optional[str] = None
    ) -> Optional[ListingDetails]:
        """
        Scrape listing details from Zillow using Screenshot + VisionService.

        Args:
            address: Street address
            city: City name
            state: State abbreviation
            zip_code: ZIP code
            output_dir: Directory to save screenshots (optional)

        Returns:
            ListingDetails object or None if failed
        """
        # Construct URLs
        zillow_url = f"https://www.zillow.com/homes/{address.replace(' ', '-')}-{city}-{state}-{zip_code}_rb/"
        realtor_url = f"https://www.realtor.com/realestateandhomes-search/{city}_{state}/{address.replace(' ', '-')}"

        logger.info(f"Searching Market Data for: {address}")
        logger.debug(f"  Zillow Link: {zillow_url}")

        details = ListingDetails(
            price=None,
            status="Unknown",
            description=f"Links:\nZillow: {zillow_url}\nRealtor: {realtor_url}"
        )

        # Use property_id or sanitize address as ID
        prop_id = property_id or self.storage._sanitize_filename(f"{address}_{city}")

        async with async_playwright() as p:
            browser, context, page = await self._setup_stealth_context(p)

            try:
                logger.info("Navigating to Zillow...")
                await self._human_like_delay(1.0, 2.0)

                # Navigate with extended timeout
                await page.goto(zillow_url, timeout=90000)

                # Wait for page and simulate human behavior
                logger.info("Waiting for page load and simulating human behavior...")
                await self._human_like_delay(2.0, 4.0)

                # Scroll to simulate reading
                await page.evaluate('window.scrollBy(0, 300)')
                await self._human_like_delay(0.5, 1.5)
                await page.evaluate('window.scrollBy(0, 200)')
                await self._human_like_delay(0.5, 1.0)

                # Random mouse movements
                for _ in range(random.randint(2, 4)):
                    await page.mouse.move(
                        random.randint(100, 800),
                        random.randint(100, 600)
                    )
                    await self._human_like_delay(0.2, 0.5)

                # Wait for content
                await page.wait_for_load_state("domcontentloaded")
                await self._human_like_delay(1.0, 2.0)

                # Take screenshot
                screenshot_bytes = await page.screenshot()
                
                # Save using ScraperStorage
                screenshot_path = self.storage.save_screenshot(
                    property_id=prop_id,
                    scraper="market_zillow",
                    image_data=screenshot_bytes,
                    context="listing"
                )
                logger.info(f"Screenshot saved to {screenshot_path}")
                details.screenshot_path = screenshot_path

                # Use VisionService to analyze
                logger.info("Analyzing screenshot with VisionService (Qwen3-VL)...")
                
                # We need absolute path for VisionService currently? 
                # VisionService takes path string. ScraperStorage returns relative path.
                abs_path = self.storage.get_full_path(prop_id, screenshot_path)
                
                data = self.vision.extract_market_listing(str(abs_path))

                if data:
                    logger.debug(f"VisionService Results: {json.dumps(data, indent=2)}")
                    details.text_content = json.dumps(data, indent=2)
                    
                    # Save vision output
                    self.storage.save_vision_output(
                        property_id=prop_id,
                        scraper="market_zillow",
                        vision_data=data,
                        screenshot_path=screenshot_path
                    )

                    # Update details from Vision data
                    if data.get('price'):
                        try:
                            price_str = str(data['price']).replace(',', '').replace('$', '')
                            details.price = float(price_str)
                        except (ValueError, TypeError):
                            pass

                    if data.get('zestimate'):
                        try:
                            val_str = str(data['zestimate']).replace(',', '').replace('$', '')
                            details.estimates['Zillow'] = float(val_str)
                        except (ValueError, TypeError):
                            pass

                    if data.get('rent_zestimate'):
                        try:
                            val_str = str(data['rent_zestimate']).replace(',', '').replace('$', '')
                            details.estimates['Rent Zestimate'] = float(val_str)
                        except (ValueError, TypeError):
                            pass

                    if data.get('description'):
                        details.description = data['description']

                    if data.get('listing_status'):
                        details.status = data['listing_status']

                else:
                    logger.warning("VisionService failed to extract data.")

            except Exception as e:
                logger.error(f"Zillow scraping failed: {e}")

            finally:
                # Don't close browser immediately to allow inspection
                logger.info("Browser left open for inspection. Will close in 5 seconds...")
                await asyncio.sleep(5)
                await browser.close()

        return details

    async def get_listing_with_captcha_handling(
        self,
        address: str,
        city: str,
        state: str,
        zip_code: str,
        property_id: Optional[str] = None,
        captcha_confidence_threshold: int = 80
    ) -> Optional[ListingDetails]:
        """
        Scrape listing with automatic CAPTCHA handling using VisionService.

        Args:
            address: Street address
            city: City name
            state: State abbreviation
            zip_code: ZIP code
            output_dir: Directory to save screenshots
            captcha_confidence_threshold: Min confidence to auto-solve CAPTCHA

        Returns:
            ListingDetails object or None if failed
        """
        zillow_url = f"https://www.zillow.com/homes/{address.replace(' ', '-')}-{city}-{state}-{zip_code}_rb/"
        
        # Use property_id or sanitize address as ID
        prop_id = property_id or self.storage._sanitize_filename(f"{address}_{city}")

        async with async_playwright() as p:
            browser, context, page = await self._setup_stealth_context(p)

            try:
                await page.goto(zillow_url, timeout=90000)
                await self._human_like_delay(2.0, 4.0)

                # Check for CAPTCHA
                captcha_selectors = [
                    "[class*='captcha']",
                    "[id*='captcha']",
                    "iframe[src*='recaptcha']",
                    "[class*='challenge']"
                ]

                for selector in captcha_selectors:
                    try:
                        captcha_elem = page.locator(selector)
                        if await captcha_elem.is_visible(timeout=2000):
                            logger.warning(f"CAPTCHA detected: {selector}")

                            # Take screenshot of CAPTCHA
                            captcha_bytes = await captcha_elem.screenshot()
                            
                            captcha_path = self.storage.save_screenshot(
                                property_id=prop_id,
                                scraper="market_zillow",
                                image_data=captcha_bytes,
                                context="captcha"
                            )
                            
                            abs_captcha_path = self.storage.get_full_path(prop_id, captcha_path)

                            # Try to solve with VisionService
                            result = self.vision.solve_captcha(
                                str(abs_captcha_path),
                                confidence_threshold=captcha_confidence_threshold
                            )

                            if result and result.get('confidence', 0) >= captcha_confidence_threshold:
                                logger.info(f"Attempting CAPTCHA solution: {result['solution']} (confidence: {result['confidence']})")

                                # Find input field and submit
                                input_field = page.locator("input[type='text']").first
                                if await input_field.is_visible(timeout=2000):
                                    await input_field.fill(result['solution'])
                                    await self._human_like_delay(0.5, 1.0)

                                    # Look for submit button
                                    submit_btn = page.locator("button[type='submit'], input[type='submit']").first
                                    if await submit_btn.is_visible(timeout=2000):
                                        await submit_btn.click()
                                        await self._human_like_delay(2.0, 4.0)
                            else:
                                logger.warning(f"CAPTCHA confidence too low or unknown type. Manual intervention needed.")
                                logger.info(f"CAPTCHA screenshot saved to: {captcha_path}")
                                # Wait longer for manual solving
                                logger.info("Waiting 30 seconds for manual CAPTCHA solving...")
                                await asyncio.sleep(30)

                            break
                    except Exception:
                        continue

                # Continue with normal scraping
                return await self.get_listing_details(address, city, state, zip_code, property_id)

            except Exception as e:
                logger.error(f"Error during CAPTCHA handling: {e}")
                return None

            finally:
                await browser.close()


if __name__ == "__main__":
    scraper = MarketScraper(headless=False)
    # Test
    result = asyncio.run(scraper.get_listing_details("3006 W Julia St", "Tampa", "FL", "33629"))
    if result:
        print(f"\nResult: {result}")
