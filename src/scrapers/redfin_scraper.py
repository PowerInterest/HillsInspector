"""
Redfin scraper using Playwright + stealth for property data and photos.

Uses Redfin's Stingray API endpoints discovered through network inspection.
Requires browser context to bypass bot detection.
"""
import asyncio
import json
import random
import re
from typing import Any
from urllib.parse import quote

from loguru import logger
from playwright.async_api import async_playwright, Page
from playwright_stealth import Stealth

from src.services.scraper_storage import ScraperStorage


async def apply_stealth(page: Page) -> None:
    """Apply stealth settings to a page to avoid bot detection."""
    await Stealth().apply_stealth_async(page)


class RedfinScraper:
    """Scraper for Redfin property data using the hidden Stingray API."""

    BASE_URL = "https://www.redfin.com"
    STINGRAY_BASE = "https://www.redfin.com/stingray"

    # Rate limiting settings
    MIN_DELAY = 3.0
    MAX_DELAY = 8.0

    def __init__(self, headless: bool = True, storage: ScraperStorage | None = None):
        self.headless = headless
        self._storage = storage  # Lazy init to avoid DB lock on import

    async def _human_delay(self, min_sec: float | None = None, max_sec: float | None = None) -> None:
        """Add random human-like delay."""
        min_s = min_sec or self.MIN_DELAY
        max_s = max_sec or self.MAX_DELAY
        await asyncio.sleep(random.uniform(min_s, max_s))  # noqa: S311

    async def _setup_browser(self, playwright):
        """Create stealth browser context."""
        browser = await playwright.chromium.launch(headless=self.headless)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            screen={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="America/New_York",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        page = await context.new_page()
        await apply_stealth(page)
        return browser, context, page

    async def search_property(self, address: str) -> dict[str, Any] | None:
        """
        Search for a property by address and return property data.

        Uses the search box on Redfin to find the property, then extracts
        data from the property page.

        Args:
            address: Full address string (e.g., "123 Main St, Tampa, FL 33617")

        Returns:
            Dictionary with property data including photos, or None if not found.
        """
        async with async_playwright() as p:
            browser, _context, page = await self._setup_browser(p)
            try:
                # Navigate to Redfin
                logger.info(f"Searching Redfin for: {address}")
                await page.goto(self.BASE_URL, timeout=30000)
                await self._human_delay(2, 4)

                # Look for the search input - use multiple selectors in order of specificity
                search_input = await page.query_selector('#search-box-input')
                if not search_input:
                    search_input = await page.query_selector('input[placeholder*="Address"]')
                if not search_input:
                    search_input = await page.query_selector('input[placeholder*="City"]')
                if not search_input:
                    search_input = await page.query_selector('input[type="search"]')

                if not search_input:
                    logger.warning("Could not find search input on Redfin")
                    # Try direct URL approach
                    return await self._search_via_url(page, address)

                # Type the address with human-like typing
                await search_input.click()
                await self._human_delay(0.5, 1)
                await search_input.fill(address)
                await self._human_delay(2, 3)  # Wait longer for autocomplete

                # Wait for autocomplete dropdown - Redfin uses various selectors
                autocomplete_selectors = [
                    '.SearchBoxAutocomplete',
                    '[class*="autocomplete"]',
                    '.react-autosuggest__suggestions-container--open',
                    '.search-dropdown',
                    'div[role="listbox"]',
                ]

                autocomplete_found = False
                for sel in autocomplete_selectors:
                    try:
                        await page.wait_for_selector(sel, timeout=5000)
                        autocomplete_found = True
                        logger.debug(f"Found autocomplete with selector: {sel}")
                        break
                    except Exception as exc:
                        logger.debug(f"Autocomplete selector failed: {sel} ({exc})")
                        continue

                if autocomplete_found:
                    await self._human_delay(0.5, 1)
                    # Try to click first suggestion
                    suggestion_selectors = [
                        '.SearchBoxAutocomplete a',
                        '[class*="autocomplete"] a',
                        '.react-autosuggest__suggestion',
                        'div[role="option"]',
                        '.search-dropdown a',
                    ]

                    for sel in suggestion_selectors:
                        first_result = await page.query_selector(sel)
                        if first_result:
                            await first_result.click()
                            logger.debug(f"Clicked suggestion with selector: {sel}")
                            await self._human_delay(3, 5)
                            break
                    else:
                        # No suggestions found, press Enter
                        await search_input.press("Enter")
                        await self._human_delay(3, 5)
                else:
                    # No autocomplete appeared, press Enter to search
                    logger.debug("No autocomplete dropdown found, pressing Enter")
                    await search_input.press("Enter")
                    await self._human_delay(3, 5)

                # Check if we're on a property page
                current_url = page.url
                if "/home/" not in current_url and "/homedetails/" not in current_url:
                    logger.warning(f"Did not land on property page: {current_url}")
                    return None

                # Extract property data from the page
                return await self._extract_property_data(page, {"name": address, "url": current_url})

            except Exception as e:
                logger.error(f"Redfin search error: {e}")
                return None
            finally:
                await browser.close()

    async def _search_via_url(self, page: Page, address: str) -> dict[str, Any] | None:
        """Fallback: Search via URL encoding the address."""
        # Construct a search URL
        encoded = quote(address)
        search_url = f"{self.BASE_URL}/{encoded}"
        logger.info(f"Trying direct URL: {search_url}")

        try:
            await page.goto(search_url, timeout=30000)
            await self._human_delay(3, 5)

            current_url = page.url
            if "/home/" in current_url or "/homedetails/" in current_url:
                return await self._extract_property_data(page, {"name": address, "url": current_url})
        except Exception as e:
            logger.debug(f"URL search failed: {e}")

        return None

    async def _extract_property_data(self, page: Page, search_result: dict) -> dict[str, Any]:
        """Extract property data from the loaded page."""
        result = {
            "source": "redfin",
            "address": search_result.get("name"),
            "url": f"{self.BASE_URL}{search_result.get('url', '')}",
            "property_id": search_result.get("id"),
            "photos": [],
            "price": None,
            "beds": None,
            "baths": None,
            "sqft": None,
            "year_built": None,
            "lot_size": None,
            "property_type": None,
            "status": None,
            "estimate": None,
        }

        try:
            # Wait for the page to fully load
            await page.wait_for_load_state("networkidle", timeout=15000)

            # Try to get photos from the page
            # Redfin typically has a photo carousel
            photo_elements = await page.query_selector_all('img[src*="ssl.cdn-redfin"]')
            for elem in photo_elements[:20]:  # Limit to 20 photos
                src = await elem.get_attribute("src")
                if src and "photos" in src.lower():
                    # Get higher resolution version
                    high_res = re.sub(r"_\d+x\d+", "_1024x683", src)
                    if high_res not in result["photos"]:
                        result["photos"].append(high_res)

            # Extract price from page
            price_elem = await page.query_selector('[data-rf-test-id="abp-price"]')
            if price_elem:
                price_text = await price_elem.inner_text()
                price_match = re.search(r"\$[\d,]+", price_text)
                if price_match:
                    result["price"] = float(price_match.group().replace("$", "").replace(",", ""))

            # Extract beds/baths/sqft
            stats = await page.query_selector_all('[data-rf-test-id="abp-beds"], [data-rf-test-id="abp-baths"], [data-rf-test-id="abp-sqFt"]')
            for stat in stats:
                text = await stat.inner_text()
                if "bed" in text.lower():
                    match = re.search(r"(\d+)", text)
                    if match:
                        result["beds"] = int(match.group(1))
                elif "bath" in text.lower():
                    match = re.search(r"([\d.]+)", text)
                    if match:
                        result["baths"] = float(match.group(1))
                elif "sq" in text.lower():
                    match = re.search(r"([\d,]+)", text)
                    if match:
                        result["sqft"] = int(match.group(1).replace(",", ""))

            # Try to get Redfin estimate
            estimate_elem = await page.query_selector('[data-rf-test-id="avmLdpPrice"]')
            if estimate_elem:
                est_text = await estimate_elem.inner_text()
                est_match = re.search(r"\$[\d,]+", est_text)
                if est_match:
                    result["estimate"] = float(est_match.group().replace("$", "").replace(",", ""))

            # Get status (for sale, sold, etc)
            status_elem = await page.query_selector('[data-rf-test-id="abp-status"]')
            if status_elem:
                result["status"] = await status_elem.inner_text()

            logger.success(f"Extracted {len(result['photos'])} photos from Redfin")

        except Exception as e:
            logger.warning(f"Error extracting property data: {e}")

        return result

    @property
    def storage(self) -> ScraperStorage:
        """Lazy-load storage to avoid DB lock on import."""
        if self._storage is None:
            self._storage = ScraperStorage()
        return self._storage

    async def get_property_photos(self, address: str, folio: str) -> list[str]:
        """
        Get property photos for an address.

        Args:
            address: Property address
            folio: Property folio for storage

        Returns:
            List of photo URLs
        """
        data = await self.search_property(address)
        if data and data.get("photos"):
            # Save the result
            self.storage.save_json(
                property_id=folio,
                scraper="redfin",
                data=data,
                context="property_data",
            )
            return data["photos"]
        return []

    def get_property_photos_sync(self, address: str, folio: str) -> list[str]:
        """Synchronous wrapper for get_property_photos."""
        return asyncio.run(self.get_property_photos(address, folio))


if __name__ == "__main__":
    import sys

    async def main():
        scraper = RedfinScraper(headless=False)

        # Test with a Tampa address
        test_address = "6710 Yardley Way, Tampa, FL 33617"
        if len(sys.argv) > 1:
            test_address = " ".join(sys.argv[1:])

        print(f"Testing Redfin scraper with: {test_address}")
        result = await scraper.search_property(test_address)

        if result:
            print("\nProperty Data:")
            print(json.dumps(result, indent=2, default=str))
        else:
            print("No data found")

    asyncio.run(main())
