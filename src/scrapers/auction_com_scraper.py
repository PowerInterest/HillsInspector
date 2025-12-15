"""
Auction.com scraper using Playwright with stealth.

This is a best-effort workflow: auction.com sits behind Incapsula, so we try
to load the site in a full browser context, apply stealth, and capture a
search attempt for a given address (city/state/ZIP or full street).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from loguru import logger
from playwright.async_api import async_playwright
from playwright_stealth import Stealth


@dataclass
class AuctionComResult:
    address: str
    success: bool
    listing_count: int
    screenshot_path: Optional[Path]
    error: Optional[str]
    note: Optional[str] = None


class AuctionComScraper:
    BASE_URL = "https://www.auction.com"

    def __init__(self, headless: bool = True, screenshot_dir: Path | str = "logs/auction_com"):
        self.headless = headless
        self.screenshot_dir = Path(screenshot_dir)
        self.screenshot_dir.mkdir(parents=True, exist_ok=True)

    async def _setup_page(self):
        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(headless=self.headless)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            viewport={"width": 1400, "height": 900},
            screen={"width": 1400, "height": 900},
            locale="en-US",
            timezone_id="America/New_York",
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        return playwright, browser, context, page

    async def search_address(self, address: str) -> AuctionComResult:
        playwright = None
        browser = None
        context = None
        page = None
        screenshot_path: Optional[Path] = None
        note: Optional[str] = None

        try:
            playwright, browser, context, page = await self._setup_page()
            logger.info("Navigating to auction.com")
            await page.goto(self.BASE_URL, timeout=90000, wait_until="domcontentloaded")

            # Handle Incapsula iframe if present
            if await page.frame(name="main-iframe"):
                note = "Incapsula iframe encountered; attempting to wait for challenge"
                logger.warning(note)
                await asyncio.sleep(10)
                await page.wait_for_timeout(5000)

            # Locate search box by placeholder text (best effort)
            search_box = page.get_by_placeholder("Search", exact=False).first
            if not await search_box.count():
                # Fallback: generic input
                search_box = page.locator("input[type='search'], input[type='text']").first

            if not await search_box.count():
                error = "Search input not found (possible bot challenge)"
                logger.error(error)
                screenshot_path = await self._save_screenshot(page, "no_search_box")
                return AuctionComResult(address=address, success=False, listing_count=0, screenshot_path=screenshot_path, error=error, note=note)

            await search_box.click()
            await search_box.fill(address)
            await search_box.press("Enter")

            # Wait for results
            await page.wait_for_timeout(5000)
            cards = page.locator("div[data-qa='search-card'], a[data-qa*='property-card']")
            count = await cards.count()

            screenshot_path = await self._save_screenshot(page, "results")

            if count == 0:
                error = "No results found or blocked by bot protection"
                logger.warning(error)
                return AuctionComResult(address=address, success=False, listing_count=0, screenshot_path=screenshot_path, error=error, note=note)

            logger.success(f"Found {count} listings for '{address}'")
            return AuctionComResult(address=address, success=True, listing_count=count, screenshot_path=screenshot_path, error=None, note=note)

        except Exception as exc:
            logger.error(f"Auction.com scrape failed: {exc}")
            if page:
                screenshot_path = await self._save_screenshot(page, "error")
            return AuctionComResult(address=address, success=False, listing_count=0, screenshot_path=screenshot_path, error=str(exc), note=note)
        finally:
            if context:
                await context.close()
            if browser:
                await browser.close()
            if playwright:
                await playwright.stop()

    async def _save_screenshot(self, page, label: str) -> Path:
        filename = f"{label}_{int(asyncio.get_event_loop().time() * 1000)}.png"
        path = self.screenshot_dir / filename
        await page.screenshot(path=str(path), full_page=True)
        logger.info(f"Saved screenshot: {path}")
        return path
