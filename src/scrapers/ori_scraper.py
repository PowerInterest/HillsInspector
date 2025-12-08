"""
ORI (Official Records Index) Scraper - PAVDirectSearch scraper for Hillsborough County Clerk.

Supports multiple search types:
- CQID=319: Book/Page search
- CQID=320: Instrument number search
- CQID=321: Legal description search
- CQID=326: Name search

Used by BatchTitleSearch service to pull document metadata.
"""
import asyncio
from typing import Dict, List, Optional
from urllib.parse import quote

from playwright.async_api import async_playwright
from loguru import logger


class ORIScraper:
    BASE_DIRECT = "https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html"

    async def _scrape_table(self, url: str, timeout: int = 30000) -> List[Dict[str, str]]:
        """
        Common method to scrape ORI result table from a PAVDirectSearch URL.
        Returns list of dicts with column headers as keys.
        """
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            try:
                await page.goto(url, timeout=timeout)
                await page.wait_for_load_state("networkidle")
                await asyncio.sleep(1)  # Extra wait for JS rendering

                # Get column headers
                headers = await page.query_selector_all("table thead th")
                header_names = [await h.inner_text() for h in headers]

                # Get all data rows
                rows = await page.query_selector_all("table tbody tr")
                results = []

                for row in rows:
                    cells = await row.query_selector_all("td")
                    if len(cells) >= 4:
                        data = {}
                        for i, cell in enumerate(cells):
                            if i < len(header_names):
                                data[header_names[i]] = (await cell.inner_text()).strip()
                        results.append(data)

                return results
            except Exception as e:
                logger.error(f"Error scraping ORI table: {e}")
                return []
            finally:
                await browser.close()

    async def fetch_instrument(self, instrument_number: str) -> Optional[Dict[str, str]]:
        """Fetch document metadata via Instrument search (CQID=320)."""
        url = f"{self.BASE_DIRECT}?CQID=320&OBKey__1006_1={quote(instrument_number)}"
        results = await self._scrape_table(url)
        if results:
            # Return first result with instrument number added
            result = results[0]
            result["Instrument #"] = instrument_number
            return result
        return None

    def fetch_instrument_sync(self, instrument_number: str) -> Optional[Dict[str, str]]:
        """Synchronous wrapper for fetch_instrument."""
        return asyncio.run(self.fetch_instrument(instrument_number))

    async def search_by_legal(self, legal_desc: str) -> List[Dict[str, str]]:
        """Search by legal description (CQID=321)."""
        url = f"{self.BASE_DIRECT}?CQID=321&OBKey__1011_1={quote(legal_desc)}"
        logger.debug(f"Legal search URL: {url}")
        return await self._scrape_table(url, timeout=60000)

    def search_by_legal_sync(self, legal_desc: str) -> List[Dict[str, str]]:
        """Synchronous wrapper for search_by_legal."""
        return asyncio.run(self.search_by_legal(legal_desc))

    async def search_by_name(self, name: str) -> List[Dict[str, str]]:
        """Search by party name (CQID=326)."""
        url = f"{self.BASE_DIRECT}?CQID=326&OBKey__486_1={quote(name)}"
        logger.debug(f"Name search URL: {url}")
        return await self._scrape_table(url, timeout=60000)

    def search_by_name_sync(self, name: str) -> List[Dict[str, str]]:
        """Synchronous wrapper for search_by_name."""
        return asyncio.run(self.search_by_name(name))

    async def search_by_book_page(self, book: str, page: str, book_type: str = "O") -> List[Dict[str, str]]:
        """Search by Book/Page (CQID=319)."""
        url = f"{self.BASE_DIRECT}?CQID=319&OBKey__1530_1={book_type}&OBKey__573_1={book}&OBKey__1049_1={page}"
        logger.debug(f"Book/Page search URL: {url}")
        return await self._scrape_table(url)

    def search_by_book_page_sync(self, book: str, page: str, book_type: str = "O") -> List[Dict[str, str]]:
        """Synchronous wrapper for search_by_book_page."""
        return asyncio.run(self.search_by_book_page(book, page, book_type))
