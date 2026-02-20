"""
Redfin scraper using real Chrome with persistent profile.

Requires: channel="chrome", devtools=True, user_chrome profile.
Bare Playwright Chromium and the Stingray API are blocked by Redfin.
Only real page navigation with a real Chrome profile works.
"""
import asyncio
import contextlib
import random
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from loguru import logger
from playwright.async_api import async_playwright, BrowserContext, Page
from playwright_stealth import Stealth

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PROFILE_DIR = PROJECT_ROOT / "data" / "browser_profiles" / "user_chrome"
FORECLOSURES_URL = "https://www.redfin.com/county/464/FL/Hillsborough-County/foreclosures"


@dataclass
class RedfinListing:
    address: str = ""
    list_price: float | None = None
    redfin_estimate: float | None = None
    beds: int | None = None
    baths: float | None = None
    sqft: int | None = None
    year_built: int | None = None
    lot_size: str | None = None
    price_per_sqft: float | None = None
    hoa_monthly: float | None = None
    days_on_market: int | None = None
    listing_status: str | None = None
    property_type: str | None = None
    photos: list[str] = field(default_factory=list)
    detail_url: str = ""


class RedfinScraper:
    """Scraper for Redfin using real Chrome with persistent profile.

    Can use an externally-provided browser context (for shared sessions)
    or launch its own (for standalone use).
    """

    # Rate limiting
    LISTING_PAGE_DELAY = (3.0, 6.0)
    DETAIL_PAGE_DELAY = (5.0, 10.0)
    PAGE_SETTLE_SECS = 3000  # ms, after domcontentloaded

    def __init__(self, context: BrowserContext | None = None, page: Page | None = None):
        self._external_context = context
        self._external_page = page
        self._context: BrowserContext | None = None
        self._pw = None

    async def __aenter__(self):
        await self._launch()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._close()

    async def _launch(self):
        if self._external_context:
            self._context = self._external_context
            self._page = self._external_page or (
                self._context.pages[0]
                if self._context.pages
                else await self._context.new_page()
            )
            self._pw = None  # don't own the playwright instance
            logger.info("Redfin scraper: using external browser context")
            return
        PROFILE_DIR.mkdir(parents=True, exist_ok=True)
        self._pw = await async_playwright().__aenter__()
        self._context = await self._pw.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            channel="chrome",
            headless=False,
            devtools=True,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            timezone_id="America/New_York",
            args=["--disable-blink-features=AutomationControlled"],
        )
        await self._context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        self._page = (
            self._context.pages[0]
            if self._context.pages
            else await self._context.new_page()
        )
        await Stealth().apply_stealth_async(self._page)
        logger.info("Redfin scraper: Chrome launched with user profile")

    async def _close(self):
        if self._external_context:
            return  # don't close what we don't own
        if self._context:
            with contextlib.suppress(Exception):
                await self._context.close()
        if self._pw:
            with contextlib.suppress(Exception):
                await self._pw.__aexit__(None, None, None)
        self._context = None

    @property
    def page(self) -> Page:
        return self._page

    async def _navigate(self, url: str) -> bool:
        """Navigate to URL, wait for settle. Returns True on HTTP 200."""
        try:
            response = await self.page.goto(
                url, wait_until="domcontentloaded", timeout=60000
            )
            await self.page.wait_for_timeout(self.PAGE_SETTLE_SECS)
            status = response.status if response else None
            if status != 200:
                logger.warning(f"Redfin navigate {url}: HTTP {status}")
                return False
            return True
        except Exception as exc:
            logger.warning(f"Redfin navigate failed {url}: {exc}")
            return False

    async def delay(self, bounds: tuple[float, float] | None = None):
        lo, hi = bounds or self.DETAIL_PAGE_DELAY
        await asyncio.sleep(random.uniform(lo, hi))  # noqa: S311

    # ---- Tier 1: Foreclosure listings page ----

    async def scrape_foreclosure_listings(self) -> list[dict[str, Any]]:
        """Scrape listing cards from Hillsborough County foreclosures page.

        Returns list of dicts: {url, address, price}.
        Handles pagination if present.
        """
        all_listings: list[dict[str, Any]] = []

        ok = await self._navigate(FORECLOSURES_URL)
        if not ok:
            logger.error("Redfin: foreclosures page blocked or failed")
            return all_listings

        page_num = 1
        while True:
            listings = await self.page.evaluate("""
                () => {
                    const cards = document.querySelectorAll('.HomeCardContainer');
                    return Array.from(cards).map(card => {
                        const link = card.querySelector('a[href*="/home/"]');
                        const priceEl = card.querySelector('[class*="price"], [class*="Price"]');
                        return {
                            url: link ? link.href : null,
                            address: link ? link.innerText.trim().split('\\n')[0] : null,
                            price: priceEl ? priceEl.innerText.trim() : null,
                        };
                    }).filter(c => c.url && c.address);
                }
            """)
            logger.info(f"Redfin listings page {page_num}: {len(listings)} cards")
            all_listings.extend(listings)

            # Check for next page button
            next_btn = await self.page.query_selector(
                'button[data-rf-test-id="react-data-paginate-next"]'
            )
            if not next_btn:
                break
            is_disabled = await next_btn.get_attribute("disabled")
            if is_disabled is not None:
                break

            await next_btn.click()
            await self.page.wait_for_timeout(self.PAGE_SETTLE_SECS)
            await self.delay(self.LISTING_PAGE_DELAY)
            page_num += 1

        logger.info(f"Redfin: scraped {len(all_listings)} total foreclosure listings")
        return all_listings

    # ---- Tier 2: Detail page extraction ----

    async def scrape_detail_page(self, url: str) -> RedfinListing | None:
        """Navigate to a Redfin detail page and extract property data."""
        ok = await self._navigate(url)
        if not ok:
            return None

        listing = RedfinListing(detail_url=url)

        try:
            # Address
            addr_el = await self.page.query_selector(
                '[data-rf-test-id="abp-homeinfo-homeAddress"], .street-address'
            )
            if addr_el:
                listing.address = (await addr_el.inner_text()).strip()

            # Price
            price_el = await self.page.query_selector('[data-rf-test-id="abp-price"]')
            if price_el:
                listing.list_price = _parse_dollar(await price_el.inner_text())

            # Redfin Estimate
            est_el = await self.page.query_selector('[data-rf-test-id="avmLdpPrice"]')
            if est_el:
                listing.redfin_estimate = _parse_dollar(await est_el.inner_text())

            # Beds / Baths / SqFt
            stats = await self.page.query_selector_all(
                '[data-rf-test-id="abp-beds"], '
                '[data-rf-test-id="abp-baths"], '
                '[data-rf-test-id="abp-sqFt"]'
            )
            for stat in stats:
                text = await stat.inner_text()
                test_id = await stat.get_attribute("data-rf-test-id") or ""
                if "beds" in test_id:
                    m = re.search(r"(\d+)", text)
                    if m:
                        listing.beds = int(m.group(1))
                elif "baths" in test_id:
                    m = re.search(r"([\d.]+)", text)
                    if m:
                        listing.baths = float(m.group(1))
                elif "sqFt" in test_id:
                    m = re.search(r"([\d,]+)", text)
                    if m:
                        listing.sqft = int(m.group(1).replace(",", ""))

            # Status
            status_el = await self.page.query_selector(
                '[data-rf-test-id="abp-status"]'
            )
            if status_el:
                listing.listing_status = (await status_el.inner_text()).strip()

            # Key details (year_built, lot_size, price/sqft, HOA)
            key_details = await self.page.evaluate("""
                () => {
                    const items = document.querySelectorAll('.keyDetail, [class*="keyDetail"]');
                    return Array.from(items).map(el => el.innerText.trim()).slice(0, 20);
                }
            """)
            for detail in key_details:
                detail_lower = detail.lower()
                if "year built" in detail_lower:
                    m = re.search(r"(\d{4})", detail)
                    if m:
                        listing.year_built = int(m.group(1))
                elif "lot size" in detail_lower:
                    listing.lot_size = detail.split("\n")[-1].strip() if "\n" in detail else detail
                elif "price/sq.ft" in detail_lower or "price/sqft" in detail_lower:
                    listing.price_per_sqft = _parse_dollar(detail)
                elif "hoa" in detail_lower:
                    listing.hoa_monthly = _parse_dollar(detail)
                elif "on redfin" in detail_lower or "days on" in detail_lower:
                    m = re.search(r"(\d+)", detail)
                    if m:
                        listing.days_on_market = int(m.group(1))
                elif "style" in detail_lower or "type" in detail_lower:
                    listing.property_type = detail.split("\n")[-1].strip() if "\n" in detail else detail

            # Photos
            photos = await self.page.evaluate("""
                () => {
                    const imgs = document.querySelectorAll('img[src*="ssl.cdn-redfin"], img[src*="photos"]');
                    const urls = new Set();
                    for (const img of imgs) {
                        const src = img.src;
                        if (src && (src.includes('photos') || src.includes('genMid'))) {
                            urls.add(src);
                        }
                    }
                    return Array.from(urls).slice(0, 20);
                }
            """)
            listing.photos = photos

        except Exception as exc:
            logger.warning(f"Redfin detail extraction error for {url}: {exc}")

        return listing

    # ---- URL construction for Tier 2 ----

    @staticmethod
    def build_detail_url(address: str, city: str, state: str, zip_code: str) -> str:
        """Construct a Redfin detail URL from address components.

        Example: 1414 Maluhia Dr, Tampa, FL 33612
        → https://www.redfin.com/FL/Tampa/1414-Maluhia-Dr-33612/home/
        """
        street_slug = re.sub(r"[^\w\s]", "", address).strip()
        street_slug = re.sub(r"\s+", "-", street_slug)
        zip_clean = (zip_code or "").strip()[:5]
        slug = f"{street_slug}-{zip_clean}" if zip_clean else street_slug
        city_clean = city.strip().replace(" ", "-")
        state_clean = state.strip().upper()
        return f"https://www.redfin.com/{state_clean}/{city_clean}/{slug}/home/"

    # ---- Convert listing to DB payload ----

    @staticmethod
    def listing_to_market_payload(listing: RedfinListing) -> dict[str, Any]:
        """Convert RedfinListing → dict compatible with PropertyDB.save_market_data()."""
        return {
            "listing_status": listing.listing_status,
            "list_price": listing.list_price,
            "zestimate": listing.redfin_estimate,
            "rent_estimate": None,
            "hoa_monthly": listing.hoa_monthly,
            "days_on_market": listing.days_on_market,
            "price_history": [],
            "description": None,
            "beds": listing.beds,
            "baths": listing.baths,
            "sqft": listing.sqft,
            "year_built": listing.year_built,
            "lot_size": listing.lot_size,
            "price_per_sqft": listing.price_per_sqft,
            "property_type": listing.property_type,
            "photos": listing.photos,
            "detail_url": listing.detail_url,
        }


# ---- Helpers ----

def _parse_dollar(text: str) -> float | None:
    """Extract dollar amount from text like '$245,000' or 'Price: $1,200/mo'."""
    m = re.search(r"\$[\d,]+", text)
    if m:
        try:
            return float(m.group().replace("$", "").replace(",", ""))
        except ValueError:
            return None
    return None


def normalize_address_for_match(addr: str) -> str:
    """Normalize address for fuzzy matching: lowercase, strip unit/apt, punctuation."""
    if not addr:
        return ""
    addr = addr.lower().strip()
    # Take only the street portion (before first comma)
    addr = addr.split(",")[0].strip()
    # Remove unit/apt/suite suffixes
    addr = re.sub(r"\b(apt|unit|suite|ste|#)\s*\S*", "", addr)
    # Remove punctuation
    addr = re.sub(r"[^\w\s]", "", addr)
    # Collapse whitespace
    return re.sub(r"\s+", " ", addr).strip()
