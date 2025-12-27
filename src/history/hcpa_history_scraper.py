import asyncio
import random
from contextlib import suppress
from loguru import logger
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from pathlib import Path
from bs4 import BeautifulSoup

from src.models.property import Property
from src.utils.time import now_utc

# Independent User Agent
USER_AGENT = "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Mobile Safari/537.36,gzip(gfe)"

class HistoricalHCPAScraper:
    """
    Independent scraper for Historical Analysis.
    Does NOT depend on property_master.db or ScraperStorage.
    """
    BASE_URL = "https://gis.hcpafl.org/propertysearch/"
    INVALID_PARCELS = {"property appraiser", "n/a", "none", "unknown"}
    
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.temp_dir = Path("data/history_scrapes")
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def _parse_sales_history_html(self, html: str) -> list[dict]:
        if not html:
            return []
        soup = BeautifulSoup(html, "lxml")

        def normalize(text: str) -> str:
            return " ".join(text.lower().strip().split())

        sales_header = soup.find(
            lambda tag: tag.name
            and tag.get_text(strip=True)
            and "sales history" in tag.get_text(strip=True).lower()
        )
        table = sales_header.find_next("table") if sales_header else None
        if not table:
            # fallback: look for tables with likely header columns
            for candidate in soup.find_all("table"):
                header_text = " ".join(th.get_text(" ", strip=True) for th in candidate.find_all(["th", "td"]))
                if "grantor" in header_text.lower() or "grantee" in header_text.lower():
                    table = candidate
                    break
                if "sale date" in header_text.lower() and "price" in header_text.lower():
                    table = candidate
                    break

        if not table:
            return []

        rows = table.find_all("tr")
        if not rows:
            return []

        headers = [normalize(cell.get_text(" ", strip=True)) for cell in rows[0].find_all(["th", "td"])]
        records: list[dict] = []

        for row in rows[1:]:
            cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["th", "td"])]
            if not cells:
                continue
            record: dict = {}
            for i, header in enumerate(headers):
                if i >= len(cells):
                    continue
                value = cells[i].strip()
                if not value:
                    continue
                if "date" in header:
                    record["date"] = value
                elif "price" in header or "amount" in header or "sale" in header and "$" in value:
                    record["price"] = value
                elif "instrument" in header:
                    record["instrument"] = value
                elif "book" in header and "page" in header:
                    record["book_page"] = value
                elif "deed" in header or "doc" in header or "type" in header:
                    record["deed_type"] = value
                elif "grantor" in header or "seller" in header:
                    record["grantor"] = value
                elif "grantee" in header or "buyer" in header:
                    record["grantee"] = value
            if record:
                records.append(record)

        return records
    
    async def enrich_property(self, prop: Property) -> Property:
        parcel_id = (prop.parcel_id or "").strip()
        if not parcel_id:
            logger.info(
                "Skipping HCPA (History) for {case_number}: missing Parcel ID",
                case_number=prop.case_number,
            )
            return prop
        if parcel_id.lower() in self.INVALID_PARCELS:
            logger.info(
                "Skipping HCPA (History) for {case_number}: invalid Parcel ID '{parcel}'",
                case_number=prop.case_number,
                parcel=parcel_id,
            )
            return prop
        prop.parcel_id = parcel_id
            
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(user_agent=USER_AGENT)
            page = await context.new_page()
            try:
                for attempt in range(2):
                    try:
                        logger.info("HCPA GET {url} for parcel {parcel}", url=self.BASE_URL, parcel=prop.parcel_id)
                        await page.goto(self.BASE_URL, timeout=60000)
                        await page.wait_for_load_state("networkidle", timeout=30000)
                        logger.info("HCPA landing URL: {url}", url=page.url)
                        
                        # Parcel ID Search
                        await page.click("#basic input[name='basicPinGroup'][value='pin']")
                        search_box = page.locator("#basic input[data-bind*='value: parcelNumber']")
                        await search_box.wait_for(state="visible")
                        await asyncio.sleep(random.uniform(0.5, 1.5))  # noqa: S311
                        await search_box.fill(prop.parcel_id)
                        await page.click("#basic button[data-bind*='click: search']")
                        await page.wait_for_timeout(3000)
                        
                        results_table = page.locator("#table-basic-results")
                        
                        # Fallback to address if needed (Copied logical flow)
                        if not await results_table.is_visible():
                            if not prop.address:
                                logger.warning(
                                    "No results for {parcel} and no address to retry.",
                                    parcel=prop.parcel_id,
                                )
                                return prop
                            # Reset and try address
                            await page.reload()
                            await page.wait_for_load_state("networkidle", timeout=30000)
                            basic_search_tab = page.locator("li.tab:has-text('Basic Search')")
                            await basic_search_tab.click()
                            await asyncio.sleep(0.5)
                            
                            address_box = page.locator("#basic input[data-bind*='value: address']")
                            await address_box.fill(prop.address)
                            await page.click("#basic button[data-bind*='click: search']")
                            await page.wait_for_timeout(3000)
                            logger.info(
                                "HCPA search by address for {parcel} -> {url}",
                                parcel=prop.parcel_id,
                                url=page.url,
                            )
                        
                        if await results_table.is_visible():
                            # Click first result
                            await results_table.locator("tbody tr:first-child td").first.click()
                            
                            details_container = page.locator("#details")
                            await details_container.wait_for(state="visible", timeout=15000)
                            logger.info("HCPA details URL for {parcel}: {url}", parcel=prop.parcel_id, url=page.url)
                            
                            with suppress(Exception):
                                await details_container.locator(
                                    "h4",
                                    has_text="PROPERTY RECORD CARD",
                                ).wait_for(state="visible", timeout=10000)
                            
                            await asyncio.sleep(2.0)

                            # Try HTML parsing for sales history before Vision.
                            with suppress(Exception):
                                details_html = await details_container.inner_html()
                                sales_history = self._parse_sales_history_html(details_html)
                                if sales_history:
                                    prop.sales_history = sales_history
                                    logger.info(
                                        "Extracted {count} sales history records via HTML for {parcel}",
                                        count=len(prop.sales_history),
                                        parcel=prop.parcel_id,
                                    )

                            if prop.sales_history:
                                return prop

                            # Screenshot for Vision
                            screenshot_bytes = None
                            with suppress(Exception):
                                screenshot_bytes = await details_container.screenshot()
                            if screenshot_bytes is None:
                                screenshot_bytes = await page.screenshot(full_page=True)
                            
                            # Save local temp
                            timestamp = now_utc().strftime("%Y%m%d%H%M%S")
                            filename = f"hcpa_{prop.parcel_id}_{timestamp}.png"
                            full_screenshot_path = self.temp_dir / filename
                            full_screenshot_path.write_bytes(screenshot_bytes)
                            
                            # Vision Service
                            from src.services.vision_service import VisionService
                            vision = VisionService()
                            
                            data = vision.extract_hcpa_details(str(full_screenshot_path))
                            
                            if data:
                                # Extract Sales History (Primary Goal)
                                if "sales_history" in data and isinstance(data["sales_history"], list):
                                    prop.sales_history = data["sales_history"]
                                    logger.info(
                                        "Extracted {count} sales for {parcel}",
                                        count=len(prop.sales_history),
                                        parcel=prop.parcel_id,
                                    )
                                
                                # Extra metadata if needed (not saving to DB, just return prop)
                                if "owner_info" in data:
                                    prop.owner_name = data["owner_info"].get("owner_name")
                            
                        else:
                            logger.warning("No results for {parcel}", parcel=prop.parcel_id)
                        return prop
                    except PlaywrightTimeoutError as exc:
                        if attempt == 0:
                            logger.warning(
                                "Timeout scraping {parcel}; retrying once. Error: {e}",
                                parcel=prop.parcel_id,
                                e=exc,
                            )
                            continue
                        raise
            except Exception as e:
                logger.error(
                    "Error scraping HCPA history for {parcel} at {url}: {e}",
                    parcel=prop.parcel_id,
                    url=page.url,
                    e=e,
                )
            finally:
                await browser.close()
                
        return prop
