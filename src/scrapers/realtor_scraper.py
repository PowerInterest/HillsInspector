"""
Realtor.com scraper for market data enrichment.

Uses Screenshot + VisionService approach due to aggressive bot detection.
Supplements Zillow data with:
- HOA fees (often more accurate on Realtor.com)
- Price history
- Agent remarks/property description
- Interior photos

Note: Realtor.com has heavy anti-bot measures (PerimeterX/Datadome).
This scraper uses playwright-stealth and human-like behavior.

Usage:
    scraper = RealtorScraper()
    details = await scraper.get_listing_details("123 Main St", "Tampa", "FL", "33602")
"""

import asyncio
import json
import random
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from loguru import logger

from playwright.async_api import async_playwright, Page
from playwright_stealth import Stealth

# Import VisionService for screenshot analysis
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.services.vision_service import VisionService
from src.services.scraper_storage import ScraperStorage


@dataclass
class PriceHistoryEntry:
    """Single entry in price history."""
    date: Optional[str] = None
    event: Optional[str] = None  # Listed, Sold, Price Change, etc.
    price: Optional[float] = None
    price_change: Optional[float] = None
    source: Optional[str] = None


@dataclass
class RealtorListing:
    """Listing data from Realtor.com."""
    address: str
    city: str
    state: str
    zip_code: str

    # Pricing
    list_price: Optional[float] = None
    price_per_sqft: Optional[float] = None
    estimated_monthly_payment: Optional[float] = None

    # Status
    listing_status: Optional[str] = None  # For Sale, Sold, Pending, Off Market
    days_on_market: Optional[int] = None
    mls_number: Optional[str] = None

    # Property Details
    beds: Optional[float] = None
    baths: Optional[float] = None
    sqft: Optional[float] = None
    lot_size: Optional[str] = None
    year_built: Optional[int] = None
    property_type: Optional[str] = None  # Single Family, Condo, etc.

    # HOA Info (Realtor.com often has better HOA data)
    hoa_fee: Optional[float] = None
    hoa_frequency: Optional[str] = None  # Monthly, Annually

    # Description
    description: Optional[str] = None
    agent_remarks: Optional[str] = None

    # Price History
    price_history: List[PriceHistoryEntry] = field(default_factory=list)

    # Photos
    photo_count: Optional[int] = None
    photo_urls: List[str] = field(default_factory=list)

    # Raw data
    screenshot_path: Optional[str] = None
    raw_text: Optional[str] = None
    realtor_url: Optional[str] = None


class RealtorScraper:
    """
    Scraper for Realtor.com using Screenshot + VisionService approach.

    Due to aggressive anti-bot measures, we:
    1. Use playwright-stealth for fingerprint evasion
    2. Simulate human-like behavior (scrolling, delays, mouse movement)
    3. Capture screenshots instead of parsing HTML
    4. Use VisionService (Qwen3-VL) to extract structured data from screenshots
    """

    BASE_URL = "https://www.realtor.com"

    def __init__(self, headless: bool = False, vision_service: Optional[VisionService] = None, storage: Optional[ScraperStorage] = None):
        """
        Initialize the Realtor.com scraper.

        Args:
            headless: Run browser in headless mode (not recommended for this site)
            vision_service: Optional VisionService instance (creates one if not provided)
            storage: ScraperStorage instance for caching
        """
        self.headless = headless
        self.vision = vision_service or VisionService()
        self.storage = storage or ScraperStorage()
        self.output_dir = Path("data/realtor_screenshots")
        self.output_dir.mkdir(parents=True, exist_ok=True)

    async def _human_delay(self, min_sec: float = 0.5, max_sec: float = 2.0):
        """Add random human-like delay."""
        await asyncio.sleep(random.uniform(min_sec, max_sec))

    async def _setup_stealth_browser(self, playwright):
        """Create a stealth browser context with anti-detection measures."""
        # Create stealth instance
        stealth = Stealth()

        browser = await playwright.chromium.launch(
            headless=self.headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
            ]
        )

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080},
            screen={'width': 1920, 'height': 1080},
            locale='en-US',
            timezone_id='America/New_York',
            extra_http_headers={
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
            },
        )

        page = await context.new_page()
        await stealth.apply_stealth_async(page)

        return browser, context, page

    async def _simulate_human_behavior(self, page: Page):
        """Simulate human-like browsing behavior."""
        # Random scrolling
        for _ in range(random.randint(2, 4)):
            scroll_amount = random.randint(200, 500)
            await page.evaluate(f'window.scrollBy(0, {scroll_amount})')
            await self._human_delay(0.3, 0.8)

        # Random mouse movements
        for _ in range(random.randint(3, 6)):
            x = random.randint(100, 1800)
            y = random.randint(100, 900)
            await page.mouse.move(x, y)
            await self._human_delay(0.1, 0.3)

        # Scroll back up a bit
        await page.evaluate('window.scrollBy(0, -200)')
        await self._human_delay(0.5, 1.0)

    def _build_realtor_url(self, address: str, city: str, state: str, zip_code: str) -> str:
        """Build Realtor.com search URL for an address."""
        # Clean and format address
        addr_slug = re.sub(r'[^\w\s-]', '', address).replace(' ', '-')
        city_slug = city.replace(' ', '-')

        # Realtor.com URL pattern: /realestateandhomes-detail/{address}_{city}_{state}_{zip}
        url = f"{self.BASE_URL}/realestateandhomes-detail/{addr_slug}_{city_slug}_{state}_{zip_code}"
        return url

    async def get_listing_details(
        self,
        address: str,
        city: str,
        state: str,
        zip_code: str
    ) -> Optional[RealtorListing]:
        """
        Get listing details from Realtor.com.

        Args:
            address: Street address
            city: City name
            state: State abbreviation
            zip_code: ZIP code

        Returns:
            RealtorListing object or None if failed
        """
        listing = RealtorListing(
            address=address,
            city=city,
            state=state,
            zip_code=zip_code
        )

        realtor_url = self._build_realtor_url(address, city, state, zip_code)
        listing.realtor_url = realtor_url
        logger.info(f"Fetching Realtor.com listing: {realtor_url}")

        async with async_playwright() as p:
            browser, context, page = await self._setup_stealth_browser(p)

            try:
                # Navigate with extended timeout
                await self._human_delay(1.0, 2.0)
                await page.goto(realtor_url, timeout=60000, wait_until="domcontentloaded")

                # Wait for page and simulate human behavior
                await self._human_delay(2.0, 4.0)
                await self._simulate_human_behavior(page)

                # Check for blocking/captcha
                content = await page.content()
                if "captcha" in content.lower() or "blocked" in content.lower():
                    logger.warning("Realtor.com may have blocked the request")
                    # Still try to capture screenshot
                    await self._human_delay(5.0, 10.0)

                # Check if we landed on a valid listing page
                if "realestateandhomes-detail" not in page.url and "property" not in page.url:
                    logger.warning(f"May not have reached listing page. URL: {page.url}")

                # Wait for content to load
                await page.wait_for_load_state("networkidle", timeout=30000)
                await self._human_delay(1.0, 2.0)

                # Take full-page screenshot
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                safe_addr = re.sub(r'[^\w\s-]', '', address).replace(' ', '_')[:30]
                screenshot_path = self.output_dir / f"realtor_{safe_addr}_{timestamp}.png"

                await page.screenshot(path=str(screenshot_path), full_page=True)
                listing.screenshot_path = str(screenshot_path)
                logger.info(f"Screenshot saved: {screenshot_path}")

                # Scroll down to capture price history section
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight / 2)')
                await self._human_delay(1.0, 1.5)

                # Take second screenshot of lower portion
                screenshot_path_2 = self.output_dir / f"realtor_{safe_addr}_{timestamp}_page2.png"
                await page.screenshot(path=str(screenshot_path_2), full_page=False)

                # Use VisionService to extract data from screenshots
                logger.info("Analyzing screenshot with VisionService...")
                extracted = self.vision.extract_realtor_listing(str(screenshot_path))

                if extracted:
                    listing = self._parse_vision_response(listing, extracted)
                    listing.raw_text = json.dumps(extracted, indent=2)

                # Try to extract HOA specifically if not found
                if listing.hoa_fee is None:
                    hoa_result = self.vision.extract_json(
                        str(screenshot_path),
                        "Look for HOA fee information. Return JSON: {\"hoa_fee\": <number or null>, \"hoa_frequency\": \"<Monthly/Annually/null>\"}"
                    )
                    if hoa_result and hoa_result.get("hoa_fee"):
                        listing.hoa_fee = hoa_result.get("hoa_fee")
                        listing.hoa_frequency = hoa_result.get("hoa_frequency")

            except Exception as e:
                logger.error(f"Error scraping Realtor.com: {e}")

            finally:
                await self._human_delay(2.0, 3.0)
                await browser.close()

        return listing

    def _parse_vision_response(self, listing: RealtorListing, data: Dict[str, Any]) -> RealtorListing:
        """Parse VisionService response into RealtorListing fields."""
        try:
            if data.get("list_price"):
                listing.list_price = float(str(data["list_price"]).replace(",", "").replace("$", ""))
        except (ValueError, TypeError):
            pass

        listing.listing_status = data.get("listing_status")

        try:
            if data.get("beds"):
                listing.beds = float(data["beds"])
        except (ValueError, TypeError):
            pass

        try:
            if data.get("baths"):
                listing.baths = float(data["baths"])
        except (ValueError, TypeError):
            pass

        try:
            if data.get("sqft"):
                listing.sqft = float(str(data["sqft"]).replace(",", ""))
        except (ValueError, TypeError):
            pass

        listing.lot_size = data.get("lot_size")

        try:
            if data.get("year_built"):
                listing.year_built = int(data["year_built"])
        except (ValueError, TypeError):
            pass

        listing.property_type = data.get("property_type")

        try:
            if data.get("hoa_fee"):
                listing.hoa_fee = float(str(data["hoa_fee"]).replace(",", "").replace("$", ""))
        except (ValueError, TypeError):
            pass

        listing.hoa_frequency = data.get("hoa_frequency")

        try:
            if data.get("days_on_market"):
                listing.days_on_market = int(data["days_on_market"])
        except (ValueError, TypeError):
            pass

        try:
            if data.get("price_per_sqft"):
                listing.price_per_sqft = float(str(data["price_per_sqft"]).replace(",", "").replace("$", ""))
        except (ValueError, TypeError):
            pass

        try:
            if data.get("estimated_payment"):
                listing.estimated_monthly_payment = float(str(data["estimated_payment"]).replace(",", "").replace("$", ""))
        except (ValueError, TypeError):
            pass

        listing.description = data.get("description")
        listing.mls_number = data.get("mls_number")

        # Parse price history
        if data.get("price_history"):
            for entry in data["price_history"]:
                try:
                    price_entry = PriceHistoryEntry(
                        date=entry.get("date"),
                        event=entry.get("event"),
                        price=float(str(entry.get("price", 0)).replace(",", "").replace("$", "")) if entry.get("price") else None
                    )
                    listing.price_history.append(price_entry)
                except (ValueError, TypeError):
                    continue

        return listing

    async def get_listing_for_property(
        self,
        property_id: str,
        address: str,
        city: str,
        state: str,
        zip_code: str,
        force_refresh: bool = False
    ) -> Optional[RealtorListing]:
        """
        Get listing details with caching.

        Args:
            property_id: Property folio/ID for storage
            address: Street address
            city: City name
            state: State abbreviation
            zip_code: ZIP code
            force_refresh: Force re-scrape even if cached

        Returns:
            RealtorListing or None
        """
        # Check cache
        if not force_refresh and not self.storage.needs_refresh(property_id, "realtor", max_age_days=7):
            cached = self.storage.get_latest(property_id, "realtor")
            if cached and cached.extraction_success:
                logger.debug(f"Using cached Realtor data for {property_id}")
                return None  # Data is in cache

        # Scrape listing
        listing = await self.get_listing_details(address, city, state, zip_code)

        if listing:
            # Convert to dict for storage
            listing_data = {
                "address": listing.address,
                "city": listing.city,
                "state": listing.state,
                "zip_code": listing.zip_code,
                "list_price": listing.list_price,
                "price_per_sqft": listing.price_per_sqft,
                "estimated_monthly_payment": listing.estimated_monthly_payment,
                "listing_status": listing.listing_status,
                "days_on_market": listing.days_on_market,
                "mls_number": listing.mls_number,
                "beds": listing.beds,
                "baths": listing.baths,
                "sqft": listing.sqft,
                "lot_size": listing.lot_size,
                "year_built": listing.year_built,
                "property_type": listing.property_type,
                "hoa_fee": listing.hoa_fee,
                "hoa_frequency": listing.hoa_frequency,
                "description": listing.description,
                "agent_remarks": listing.agent_remarks,
                "price_history": [
                    {"date": e.date, "event": e.event, "price": e.price, "source": e.source}
                    for e in listing.price_history
                ],
                "photo_count": listing.photo_count,
                "realtor_url": listing.realtor_url
            }

            # Copy screenshot to property storage
            screenshot_path = None
            if listing.screenshot_path and Path(listing.screenshot_path).exists():
                screenshot_path = self.storage.save_screenshot_from_file(
                    property_id=property_id,
                    scraper="realtor",
                    source_path=listing.screenshot_path
                )

            # Save vision output
            vision_path = self.storage.save_vision_output(
                property_id=property_id,
                scraper="realtor",
                vision_data=listing_data,
                screenshot_path=screenshot_path,
                prompt_version="v1"
            )

            # Record in database
            self.storage.record_scrape(
                property_id=property_id,
                scraper="realtor",
                screenshot_path=screenshot_path,
                vision_output_path=vision_path,
                vision_data=listing_data,
                prompt_version="v1",
                success=True
            )

            logger.info(f"Saved Realtor data for {property_id}: ${listing.list_price or 'N/A'}")

        return listing

    async def search_by_address(
        self,
        address: str,
        city: str,
        state: str,
        zip_code: str
    ) -> List[RealtorListing]:
        """
        Search Realtor.com by address and return matching listings.

        This is useful when the direct URL doesn't work.
        """
        listings = []
        search_query = f"{address}, {city}, {state} {zip_code}"
        search_url = f"{self.BASE_URL}/realestateandhomes-search/{city}_{state}_{zip_code}/type-single-family-home"

        logger.info(f"Searching Realtor.com: {search_query}")

        async with async_playwright() as p:
            browser, context, page = await self._setup_stealth_browser(p)

            try:
                await page.goto(search_url, timeout=60000)
                await self._human_delay(2.0, 4.0)
                await self._simulate_human_behavior(page)

                # Look for search input and enter address
                search_input = page.locator("input[data-testid='search-input'], input[placeholder*='Address']").first
                if await search_input.is_visible(timeout=5000):
                    await search_input.fill(search_query)
                    await self._human_delay(0.5, 1.0)
                    await search_input.press("Enter")
                    await page.wait_for_load_state("networkidle")
                    await self._human_delay(2.0, 3.0)

                # If we landed on a property page directly, extract it
                if "realestateandhomes-detail" in page.url:
                    listing = await self.get_listing_details(address, city, state, zip_code)
                    if listing:
                        listings.append(listing)

            except Exception as e:
                logger.error(f"Error searching Realtor.com: {e}")

            finally:
                await browser.close()

        return listings


if __name__ == "__main__":
    async def main():
        scraper = RealtorScraper(headless=False)

        # Test with Tampa address
        print("\n=== Realtor.com Scraper Test ===\n")
        listing = await scraper.get_listing_details(
            "3006 W Julia St",
            "Tampa",
            "FL",
            "33629"
        )

        if listing:
            print(f"Address: {listing.address}, {listing.city}, {listing.state}")
            print(f"Status: {listing.listing_status}")
            print(f"Price: ${listing.list_price:,.0f}" if listing.list_price else "Price: N/A")
            print(f"Beds/Baths: {listing.beds}/{listing.baths}")
            print(f"Sqft: {listing.sqft}")
            print(f"Year Built: {listing.year_built}")
            print(f"HOA Fee: ${listing.hoa_fee}/{listing.hoa_frequency}" if listing.hoa_fee else "HOA: None")
            print(f"Days on Market: {listing.days_on_market}")
            print(f"\nPrice History:")
            for entry in listing.price_history:
                print(f"  {entry.date}: {entry.event} - ${entry.price:,.0f}" if entry.price else f"  {entry.date}: {entry.event}")
            print(f"\nScreenshot: {listing.screenshot_path}")
        else:
            print("Failed to get listing details")

    asyncio.run(main())
