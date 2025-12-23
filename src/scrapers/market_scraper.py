"""
Market data scraper for Zillow/Realtor.com using VisionService.

NOTE: This scraper uses screenshot + Vision API approach due to aggressive
bot detection on real estate sites. Uses playwright-stealth for better success rates.
"""
import asyncio
import json
import random
import re
from typing import Optional, Dict
from loguru import logger

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from src.models.property import ListingDetails
from src.services.scraper_storage import ScraperStorage
from src.services.vision_service import VisionService


async def stealth_async(page):
    """Apply stealth settings to a page."""
    await Stealth().apply_stealth_async(page)


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

    def _slugify(self, value: str) -> str:
        slug = re.sub(r"[^A-Za-z0-9]+", "-", (value or "").strip())
        return slug.strip("-")

    async def _human_like_delay(self, min_sec: float = 0.5, max_sec: float = 4.0):
        """Add random human-like delay."""
        await asyncio.sleep(random.uniform(min_sec, max_sec))  # noqa: S311

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
        Scrape listing details from Zillow and Realtor.com using Screenshot + VisionService.

        Args:
            address: Street address
            city: City name
            state: State abbreviation
            zip_code: ZIP code
            property_id: Optional property identifier

        Returns:
            ListingDetails object or None if failed
        """
        # Construct URLs
        address_slug = self._slugify(address) or address.replace(" ", "-")
        city_slug = self._slugify(city) or city.replace(" ", "-")
        state_slug = (state or "").strip().upper()
        zip_slug = re.sub(r"[^0-9]", "", zip_code or "")
        zillow_parts = [part for part in [address_slug, city_slug, state_slug, zip_slug] if part]
        zillow_url = f"https://www.zillow.com/homes/{'-'.join(zillow_parts)}_rb/"
        realtor_url = f"https://www.realtor.com/realestateandhomes-search/{city_slug}_{state_slug}/{address_slug}"

        logger.info(f"Searching Market Data for: {address}")
        
        details = ListingDetails(
            price=None,
            status="Unknown",
            description=f"Links:\nZillow: {zillow_url}\nRealtor: {realtor_url}"
        )

        # Use property_id or sanitize address as ID
        prop_id = property_id or self.storage._sanitize_filename(f"{address}_{city}")  # noqa: SLF001

        zillow_error = None
        realtor_error = None

        async with async_playwright() as p:
            browser, _context, page = await self._setup_stealth_context(p)

            try:
                # 1. Try Zillow
                logger.info(f"Attempting Zillow: {zillow_url}")
                try:
                    zillow_data, zillow_shot, _ = await self._scrape_source(
                        page, zillow_url, "zillow", prop_id, self.vision.extract_market_listing
                    )
                    if zillow_data:
                        self._update_details_from_data(details, zillow_data, "Zillow")
                        if zillow_shot and not details.screenshot_path:
                            details.screenshot_path = zillow_shot
                        logger.success(f"Successfully scraped Zillow for {address}")
                except Exception as e:
                    logger.warning(f"Zillow scrape failed: {e}")
                    zillow_error = e

                # 2. Try Realtor.com if Zillow failed or if we want secondary confirmation
                # For now, let's try Realtor if Zillow price is missing
                if not details.price or details.status == "Unknown":
                    logger.info(f"Zillow data incomplete, attempting Realtor.com: {realtor_url}")
                    try:
                        realtor_data, realtor_shot, _ = await self._scrape_source(
                            page, realtor_url, "realtor", prop_id, self.vision.extract_realtor_listing
                        )
                        if realtor_data:
                            self._update_details_from_data(details, realtor_data, "Realtor")
                            if realtor_shot and (not details.screenshot_path or not details.price):
                                details.screenshot_path = realtor_shot
                            logger.success(f"Successfully scraped Realtor.com for {address}")
                    except Exception as e:
                        logger.warning(f"Realtor scrape failed: {e}")
                        realtor_error = e

            finally:
                await browser.close()

        # If both sources failed with errors, raise to signal retry needed
        if zillow_error and realtor_error:
            raise RuntimeError(f"Both market sources failed - Zillow: {zillow_error}, Realtor: {realtor_error}")

        # If we tried both and got no useful data (but no hard errors), that's also a failure
        if not details.price and details.status == "Unknown" and (zillow_error or realtor_error):
            raise RuntimeError(f"Market scrape failed with partial errors - Zillow: {zillow_error}, Realtor: {realtor_error}")

        return details

    async def _scrape_source(self, page, url, source_node, prop_id, vision_func) -> tuple[Optional[Dict], Optional[str], Optional[str]]:
        """Generic source scraper helper. Raises on bot detection."""
        await self._human_like_delay(1.0, 2.0)
        await page.goto(url, timeout=60000)

        # Simulate human behavior
        await self._human_like_delay(2.0, 4.0)
        await page.evaluate('window.scrollBy(0, 300)')
        await self._human_like_delay(0.5, 1.5)

        # Check for generic block/captcha text - raise exception to signal retry needed
        content = await page.content()
        if "captcha" in content.lower() or "blocked" in content.lower() or "security check" in content.lower():
            raise RuntimeError(f"Bot detection triggered on {source_node}: CAPTCHA/block detected")

        # Take screenshot
        screenshot_bytes = await page.screenshot()

        # Save using ScraperStorage
        screenshot_path = self.storage.save_screenshot(
            property_id=prop_id,
            scraper=f"market_{source_node}",
            image_data=screenshot_bytes,
            context="listing"
        )

        abs_path = self.storage.get_full_path(prop_id, screenshot_path)
        data = await self.vision.process_async(vision_func, str(abs_path))

        if data:
            # Save vision output
            vision_path = self.storage.save_vision_output(
                property_id=prop_id,
                scraper=f"market_{source_node}",
                vision_data=data,
                screenshot_path=screenshot_path
            )
            self.storage.record_scrape(
                property_id=prop_id,
                scraper=f"market_{source_node}",
                screenshot_path=screenshot_path,
                vision_output_path=vision_path,
                vision_data=data,
                prompt_version="v1",
                success=True,
                source_url=url,
            )
            return data, screenshot_path, vision_path

        self.storage.record_scrape(
            property_id=prop_id,
            scraper=f"market_{source_node}",
            screenshot_path=screenshot_path,
            error="No data extracted",
            success=False,
            source_url=url,
        )
        return None, screenshot_path, None

    def _update_details_from_data(self, details: ListingDetails, data: Dict, source: str):
        """Update ListingDetails object from vision data."""
        # Update price (prefer highest confidence/most recent)
        if data.get('price'):
            try:
                price_str = str(data['price']).replace(',', '').replace('$', '')
                details.price = float(price_str)
            except (ValueError, TypeError):
                pass
        elif source == "Realtor" and data.get('list_price'): # Realtor specific key
            try:
                price_str = str(data['list_price']).replace(',', '').replace('$', '')
                details.price = float(price_str)
            except (ValueError, TypeError):
                pass

        # Update estimates
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
        elif data.get('rent_estimate'):
            try:
                val_str = str(data['rent_estimate']).replace(',', '').replace('$', '')
                details.estimates['Rent Estimate'] = float(val_str)
            except (ValueError, TypeError):
                pass

        hoa_val = data.get('hoa_fee') or data.get('hoa_monthly')
        if hoa_val:
            try:
                hoa_str = str(hoa_val).replace(',', '').replace('$', '')
                details.hoa_monthly = float(hoa_str)
            except (ValueError, TypeError):
                pass

        dom = data.get('days_on_market')
        if dom is not None:
            try:
                details.days_on_market = int(str(dom).replace(',', '').strip())
            except (ValueError, TypeError):
                pass

        price_history = data.get("price_history")
        if isinstance(price_history, list) and len(price_history) > len(details.price_history):
            details.price_history = price_history

        # Update other fields
        if data.get('description') and len(data['description']) > len(details.description or ""):
            details.description = data['description']

        if data.get('listing_status'):
            details.status = data['listing_status']
        elif source == "Realtor" and data.get('status'):
            details.status = data['status']
            
        # Store raw text if needed
        details.text_content = (details.text_content or "") + f"\n--- {source} Results ---\n" + json.dumps(data, indent=2)

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
        prop_id = property_id or self.storage._sanitize_filename(f"{address}_{city}")  # noqa: SLF001

        async with async_playwright() as p:
            browser, _context, page = await self._setup_stealth_context(p)

            try:
                logger.info(f"Zillow GET: {zillow_url}")
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
                            result = await self.vision.process_async(
                                self.vision.solve_captcha,
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
                    except Exception as exc:
                        logger.debug("CAPTCHA handling loop error for %s: %s", selector, exc)
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
