import asyncio
import json
import random
import re
from contextlib import suppress
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any
import urllib.parse
from loguru import logger
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import Stealth

from src.models.property import Property
from src.db.operations import PropertyDB


from src.services.final_judgment_processor import FinalJudgmentProcessor
from src.scrapers.hcpa_gis_scraper import scrape_hcpa_property


async def apply_stealth(page):
    """Apply stealth settings to a page to avoid bot detection."""
    await Stealth().apply_stealth_async(page)

USER_AGENT_MOBILE = "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Mobile Safari/537.36,gzip(gfe)"
USER_AGENT_DESKTOP = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"


class AuctionScraper:
    BASE_URL = "https://hillsborough.realforeclose.com"

    def __init__(self):
        self.db = PropertyDB()
        self.judgment_processor = FinalJudgmentProcessor()
        from src.services.scraper_storage import ScraperStorage
        self.storage = ScraperStorage()

    async def scrape_next_available(self, start_date: date, max_days_ahead: int = 14) -> List[Property]:
        """Try current date, then walk forward until auctions are found or limit reached."""
        for delta in range(max_days_ahead + 1):
            target = start_date + timedelta(days=delta)
            if target.weekday() >= 5:
                continue
            
            props = await self.scrape_date(target, fast_fail=True)
            if props:
                return props
        return []

    async def scrape_all(self, start_date: date, end_date: date, max_properties: Optional[int] = None) -> List[Property]:
        """Scrape all auctions within a date range."""
        all_properties = []
        current = start_date
        while current <= end_date:
            # Skip weekends (5=Saturday, 6=Sunday)
            if current.weekday() >= 5:
                logger.debug(f"Skipping weekend: {current}")
                current += timedelta(days=1)
                continue

            try:
                remaining = None
                if max_properties is not None:
                    remaining = max(max_properties - len(all_properties), 0)
                    if remaining <= 0:
                        break
                props = await self.scrape_date(current, fast_fail=True, max_properties=remaining)
                all_properties.extend(props)
            except Exception as e:
                logger.error(f"Failed to scrape {current}: {e}")
            current += timedelta(days=1)
        return all_properties
    
    async def scrape_date(self, target_date: date, fast_fail: bool = False, max_properties: Optional[int] = None) -> List[Property]:
        """
        Scrapes auction data for a specific date, handling pagination.
        """
        date_str = target_date.strftime("%m/%d/%Y")
        url = f"{self.BASE_URL}/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE={date_str}"
        
        properties = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=USER_AGENT_DESKTOP,
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
                timezone_id='America/New_York',
            )
            page = await context.new_page()
            await apply_stealth(page)

            try:
                logger.info("Visiting {url} to collect auction data for {date}", url=url, date=date_str)
                await page.goto(url, timeout=60000)
                await page.wait_for_load_state("networkidle")
                
                # Check if we are on the right page or if there are no auctions
                content = await page.content()
                if "No auctions found" in content:
                    logger.info("No auctions found for {date}", date=date_str)
                    return []

                page_num = 1
                while True:
                    logger.info("Scraping auction results page {page_num} for {date}", page_num=page_num, date=date_str)
                    
                    # Wait for auction items to be visible (new layout uses AUCTION_ITEM cards)
                    try:
                        await page.wait_for_selector(".AUCTION_ITEM", timeout=10000 if fast_fail else 30000)
                    except PlaywrightTimeoutError:
                        try:
                            await page.wait_for_selector("text=Auction Starts", timeout=5000 if fast_fail else 15000)
                        except PlaywrightTimeoutError as te:
                            html_snapshot = await page.content()
                            if "No auctions found" in html_snapshot or "Preview Items For Sale" in html_snapshot:
                                logger.info("No auctions found for {date} after load", date=date_str)
                                break
                            logger.error("Auction page did not render expected selectors for {date}: {err}", date=date_str, err=te)
                            # Dump a truncated snapshot to aid debugging
                            logger.debug("Page snapshot (truncated): {snap}", snap=html_snapshot[:2000])
                            # Save full snapshot and screenshot for inspection
                            snapshot_path = f"logs/auction_page_{date_str.replace('/', '-')}.html"
                            Path(snapshot_path).write_text(html_snapshot, encoding="utf-8")
                            with suppress(Exception):
                                await page.screenshot(path=f"logs/auction_page_{date_str.replace('/', '-')}.png", full_page=True)
                            # If we cannot find the table, exit gracefully for this date
                            break
                    
                    # Scrape current page
                    remaining = None
                    if max_properties is not None:
                        remaining = max_properties - len(properties)
                        if remaining <= 0:
                            break
                    page_props = await self._scrape_current_page(page, target_date, max_properties=remaining)
                    properties.extend(page_props)
                    if max_properties is not None and len(properties) >= max_properties:
                        logger.info("Reached property limit for {date}", date=date_str)
                        break
                    logger.info("Found {count} auction entries on page {page_num}", count=len(page_props), page_num=page_num)
                    # Process Final Judgment PDFs if already downloaded
                    for prop in page_props:
                        await self._process_final_judgment(prop)
                    
                    if page_num >= 10:
                        logger.info("Reached max auction page limit (10) for {date}", date=date_str)
                        break
                    
                    # Check for "Next" button
                    # .PageRight_W = Active
                    # .PageRight_D = Disabled
                    next_btn = page.locator(".PageRight_W").first
                    
                    if max_properties is not None and len(properties) >= max_properties:
                        break

                    if await next_btn.count() > 0 and await next_btn.is_visible():
                        try:
                            logger.info("Advancing to next auction page for {date}", date=date_str)
                            await asyncio.sleep(random.uniform(0.5, 2))  # noqa: S311
                            await next_btn.click(timeout=5000)
                            await page.wait_for_load_state("networkidle")
                            await page.wait_for_timeout(2000) # Small pause for dynamic content
                            page_num += 1
                        except Exception as e:
                            logger.error("Pagination error while scraping auctions for {date}: {error}", date=date_str, error=e)
                            break
                    else:
                        logger.info("No more auction pages available for {date}", date=date_str)
                        break
                        
            except Exception as e:
                logger.error("Error during auction scraping for {date}: {error}", date=date_str, error=e)
                # Take screenshot on error
                await page.screenshot(path=f"error_{date_str.replace('/', '-')}.png")
                raise
            finally:
                await browser.close()
                
        return properties

    async def _scrape_current_page(self, page: Page, target_date: date, max_properties: Optional[int] = None) -> List[Property]:
        properties = []
        items = page.locator("div.AUCTION_ITEM")
        count = await items.count()

        for i in range(count):
            if max_properties is not None and len(properties) >= max_properties:
                break
            item = items.nth(i)
            try:
                start_text = await item.locator(".ASTAT_MSGB").inner_text()
                # Only keep items with a valid Auction Starts date
                if not start_text or "/" not in start_text:
                    continue

                # Extract details table
                details = item.locator("table.ad_tab")

                async def cell_after(label: str, *, _details=details) -> str:
                    row = _details.locator(f"tr:has-text('{label}')")
                    if await row.count() == 0:
                        return ""
                    return (await row.locator("td").nth(1).inner_text()).strip()

                # Case number/link
                case_row = details.locator("tr:has-text('Case #:')")
                case_link = case_row.locator("a")
                case_number = (await case_link.inner_text()).strip()
                case_href = await case_link.get_attribute("href")
                instrument_number = None
                if case_href and "OBKey__1006_1=" in case_href:
                    instrument_number = case_href.split("OBKey__1006_1=")[-1]

                # Parcel ID and HCPA link
                parcel_row = details.locator("tr:has-text('Parcel ID:')")
                parcel_id_text = ""
                hcpa_url = None
                if await parcel_row.count():
                    parcel_link = parcel_row.locator("a")
                    if await parcel_link.count():
                        raw_parcel = (await parcel_link.inner_text()).strip()
                        # Filter out non-parcel values like "Property Appraiser" links
                        if raw_parcel and raw_parcel.lower() not in ("property appraiser", "n/a", "none"):
                            parcel_id_text = raw_parcel
                            # Capture the HCPA link href for immediate enrichment
                            hcpa_url = await parcel_link.get_attribute("href")

                # Address (two rows)
                addr_row = details.locator("tr:has-text('Property Address:')")
                address = (await addr_row.locator("td").nth(1).inner_text()).strip() if await addr_row.count() else ""
                city_row = addr_row.locator("xpath=./following-sibling::tr[1]")
                if await city_row.count():
                    address = f"{address}, {(await city_row.locator('td').nth(1).inner_text()).strip()}"

                value_text = await cell_after("Assessed Value:")
                judgment_text = await cell_after("Final Judgment Amount:")
                auction_type = await cell_after("Auction Type:")

                pdf_path = None
                plaintiff = None
                defendant = None
                if case_href and "CQID=320" in case_href:
                    # Pass parcel_id (folio) to download method
                    judgment_result = await self._download_final_judgment(page, case_href, case_number, parcel_id_text, instrument_number)
                    pdf_path = judgment_result.get("pdf_path")
                    plaintiff = judgment_result.get("plaintiff")
                    defendant = judgment_result.get("defendant")

                prop = Property(
                    case_number=case_number,
                    parcel_id=parcel_id_text,
                    address=address,
                    assessed_value=self._parse_amount(value_text),
                    final_judgment_amount=self._parse_amount(judgment_text),
                    auction_date=target_date,
                    auction_type=auction_type,
                    final_judgment_pdf_path=pdf_path,
                    instrument_number=instrument_number,
                    plaintiff=plaintiff,
                    defendant=defendant,
                    hcpa_url=hcpa_url,
                    has_valid_parcel_id=bool(parcel_id_text),  # FALSE for mobile homes/unresolved
                )

                # Immediately enrich with HCPA data if we have the URL
                hcpa_failed = False
                hcpa_error = None
                if hcpa_url:
                    hcpa_result = await self._enrich_with_hcpa(prop)
                    if hcpa_result.get("success"):
                        # Log comparison between parcel_id from auction and folio from HCPA
                        hcpa_folio = hcpa_result.get("hcpa_folio")
                        if hcpa_folio and hcpa_folio != parcel_id_text:
                            logger.warning(
                                f"Folio mismatch for {case_number}: "
                                f"auction={parcel_id_text}, HCPA={hcpa_folio}"
                            )
                    else:
                        # HCPA scrape failed - mark for manual review
                        hcpa_failed = True
                        hcpa_error = hcpa_result.get("error", "Unknown HCPA scrape error")
                        logger.warning(f"HCPA scrape failed for {case_number}: {hcpa_error}")
                else:
                    # No HCPA URL available - also mark as failed
                    hcpa_failed = True
                    hcpa_error = "No HCPA URL available on auction page"

                # Store HCPA failure status in database for manual review
                if hcpa_failed:
                    try:
                        conn = self.db.connect()
                        conn.execute("ALTER TABLE auctions ADD COLUMN IF NOT EXISTS hcpa_scrape_failed BOOLEAN DEFAULT FALSE")
                        conn.execute("ALTER TABLE auctions ADD COLUMN IF NOT EXISTS hcpa_scrape_error VARCHAR")
                    except Exception as exc:
                        logger.debug("HCPA flags exist; skipping add: {}", exc)

                properties.append(prop)

            except Exception as e:
                logger.error("Error parsing auction item: {error}", error=e)
                continue

        return properties

    async def _download_final_judgment(
        self,
        page: Page,
        onbase_url: str,
        case_number: str,
        parcel_id: str,
        instrument_number: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Downloads the Final Judgment PDF from OnBase via the provided Instrument Search URL.
        Uses ScraperStorage to save to property folder.
        Also extracts Party 1 (plaintiff) and Party 2 (defendant) from the PAV page.

        Returns:
            Dict with keys: pdf_path, plaintiff, defendant (any may be None)
        """
        result = {"pdf_path": None, "plaintiff": None, "defendant": None}

        # Use parcel_id if available, otherwise fall back to case_number for storage
        # This ensures we always download the PDF, even when parcel_id is missing
        storage_id = parcel_id if parcel_id else f"unknown_case_{case_number}"
        if not parcel_id:
            logger.info(f"No Parcel ID for {case_number}, using fallback storage: {storage_id}")

        # Check if already exists in storage
        # We use instrument number as doc_id if available, else case number
        doc_id = instrument_number if instrument_number else case_number

        existing_path = self.storage.document_exists(
            property_id=storage_id,
            doc_type="final_judgment",
            doc_id=doc_id,
            extension="pdf"
        )
        if existing_path:
            logger.debug(f"PDF already exists for {case_number}: {existing_path}")
            result["pdf_path"] = str(existing_path)
            # Note: We don't have party info cached, but Step 2 will extract from PDF
            return result

        new_context = None
        new_page = None
        try:
            # Create a new context with a desktop User-Agent to ensure PDF downloads work correctly
            new_context = await page.context.browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                accept_downloads=True
            )
            new_page = await new_context.new_page()
            await apply_stealth(new_page)

            # Future to capture the Document ID and Party info from the API response
            doc_id_future = asyncio.Future()
            party_info = {"plaintiff": None, "defendant": None}

            async def handle_response(response):
                if "KeywordSearch" in response.url and not doc_id_future.done():
                    try:
                        json_data = await response.json()
                        if "Data" in json_data and len(json_data["Data"]) > 0:
                            first_record = json_data["Data"][0]
                            doc_id = first_record.get("ID")
                            if doc_id:
                                doc_id_future.set_result(doc_id)
                            # Extract Party 1 (plaintiff) and Party 2 (defendant)
                            # API field names may be "Party1", "Party 1", or similar
                            party_info["plaintiff"] = (
                                first_record.get("Party1") or
                                first_record.get("Party 1") or
                                first_record.get("party1") or
                                first_record.get("PARTY1")
                            )
                            party_info["defendant"] = (
                                first_record.get("Party2") or
                                first_record.get("Party 2") or
                                first_record.get("party2") or
                                first_record.get("PARTY2")
                            )
                    except Exception as exc:
                        logger.debug("Failed to parse OnBase response for {}: {}", case_number, exc)

            new_page.on("response", handle_response)

            logger.info(f"OnBase GET: {onbase_url}")
            await new_page.goto(onbase_url, timeout=30000)

            # Wait for the Document ID
            try:
                onbase_doc_id = await asyncio.wait_for(doc_id_future, timeout=15.0)
            except TimeoutError:
                logger.warning(f"Could not find Document ID for {case_number}")
                # Still try to return party info if we got it
                result["plaintiff"] = party_info.get("plaintiff")
                result["defendant"] = party_info.get("defendant")
                return result

            # Store party info in result
            result["plaintiff"] = party_info.get("plaintiff")
            result["defendant"] = party_info.get("defendant")
            if result["plaintiff"] or result["defendant"]:
                logger.info(f"Extracted parties for {case_number}: P1={result['plaintiff']}, P2={result['defendant']}")
                
            # Construct the download URL
            encoded_id = urllib.parse.quote(onbase_doc_id)
            download_url = f"https://publicaccess.hillsclerk.com/PAVDirectSearch/api/Document/{encoded_id}/?OverlayMode=View"
            
            logger.info(f"Downloading PDF for {case_number}...")
            
            # Trigger download
            async with new_page.expect_download(timeout=60000) as download_info:
                 await new_page.evaluate(f"window.location.href = '{download_url}'")
                 
            download = await download_info.value
            
            # Read bytes directly
            pdf_path = await download.path()
            with open(pdf_path, "rb") as f:
                pdf_bytes = f.read()
                
            # Save using ScraperStorage
            saved_path = self.storage.save_document(
                property_id=storage_id,
                file_data=pdf_bytes,
                doc_type="final_judgment",
                doc_id=doc_id, # Instrument number or Case number
                extension="pdf"
            )
            
            # Get full path for return
            full_path = self.storage.get_full_path(storage_id, saved_path)
            logger.info(f"Saved PDF to {full_path}")

            result["pdf_path"] = str(full_path)
            return result

        except Exception as e:
            logger.error(f"Error downloading PDF for {case_number}: {e}")
            return result
        finally:
            if new_page:
                await new_page.close()
            if new_context:
                await new_context.close()

    async def _process_final_judgment(self, prop: Property) -> None:
        """
        If a Final Judgment PDF exists for this case, extract structured data
        and store it in the auctions table.
        """
        # We need to find the PDF. Since we don't have the path explicitly stored in a simple way
        # (it's in the property object if we just scraped it, but if we're re-processing...)
        # We'll check the property object first.
        
        pdf_path = None
        if prop.final_judgment_pdf_path:
            pdf_path = Path(prop.final_judgment_pdf_path)
        elif prop.parcel_id:
            # Try to construct path from storage convention
            # This is a bit hacky, we should probably store the path in the DB when we save it.
            # But for now, let's try to find it.
            doc_id = prop.instrument_number if prop.instrument_number else prop.case_number
            # Sanitize folio
            folio = prop.parcel_id.replace("-", "").replace(" ", "")
            # Try both instrument and case number filenames
            potential_paths = [
                self.storage.get_full_path(folio, f"documents/final_judgment_{doc_id}.pdf"),
                self.storage.get_full_path(folio, f"documents/final_judgment_{prop.case_number}.pdf")
            ]
            for p in potential_paths:
                if p.exists():
                    pdf_path = p
                    break
        else:
            # No parcel_id - check fallback unknown_case folder
            doc_id = prop.instrument_number if prop.instrument_number else prop.case_number
            fallback_folder = f"unknown_case_{prop.case_number}"
            potential_paths = [
                self.storage.get_full_path(fallback_folder, f"documents/final_judgment_{doc_id}.pdf"),
                self.storage.get_full_path(fallback_folder, f"documents/final_judgment_{prop.case_number}.pdf")
            ]
            for p in potential_paths:
                if p.exists():
                    pdf_path = p
                    logger.info(f"Found PDF in fallback location for {prop.case_number}: {p}")
                    break
        
        if not pdf_path or not pdf_path.exists():
            logger.debug("No Final Judgment PDF found for case {case}", case=prop.case_number)
            return

        try:
            result = self.judgment_processor.process_pdf(str(pdf_path), prop.case_number)
            if not result:
                logger.warning("Failed to extract data from Final Judgment for case {case}", case=prop.case_number)
                return

            amounts = self.judgment_processor.extract_key_amounts(result)
            payload = {
                **result,
                **amounts,
                "extracted_judgment_data": json.dumps(result),
                "raw_judgment_text": result.get("raw_text", ""),
            }
            # Save vision output (extracted data)
            # We use the folio if available, otherwise we might need to rely on case number or skip
            # But ScraperStorage requires a property_id. 
            # If we don't have a folio, we can use case_number as property_id for storage purposes, 
            # but that might fragment data. 
            # Ideally we have prop.parcel_id.
            storage_id = prop.parcel_id if prop.parcel_id else prop.case_number
            
            vision_path = self.storage.save_vision_output(
                property_id=storage_id,
                scraper="final_judgment",
                vision_data=result,
                context=prop.case_number
            )
            
            # Record scrape
            self.storage.record_scrape(
                property_id=storage_id,
                scraper="final_judgment",
                vision_output_path=vision_path,
                vision_data=result,
                success=True
            )

            updated = self.db.update_judgment_data(prop.case_number, payload)
            if updated:
                logger.success("Stored Final Judgment data for case {case}", case=prop.case_number)
            else:
                logger.warning("No fields updated for case {case} (empty payload)", case=prop.case_number)

        except Exception as exc:
            logger.error("Error processing Final Judgment for case {case}: {err}", case=prop.case_number, err=exc)

    def _parse_amount(self, text: str) -> Optional[float]:
        if not text:
            return None
        # Remove currency symbols, commas, and "Hidden" text
        clean_text = text.replace("$", "").replace(",", "").replace("Hidden", "").strip()
        if not clean_text:
            return None
        try:
            return float(clean_text)
        except ValueError:
            return None

    async def _enrich_with_hcpa(self, prop: Property) -> Dict[str, Any]:
        """
        Scrape HCPA GIS immediately after auction scrape to get enrichment data.

        Returns dict with HCPA data that can be compared to bulk data.
        """
        result = {"success": False, "hcpa_data": None, "error": None}

        if not prop.hcpa_url:
            result["error"] = "No HCPA URL available"
            return result

        # Extract parcel ID from HCPA URL
        # Format 1: https://gis.hcpafl.org/propertysearch/#/parcel/basic/{parcel_id}
        # Format 2: http://www.hcpafl.org/CamaDisplay.aspx?...ParcelID={parcel_id}
        hcpa_parcel_id = None

        match = re.search(r'/parcel/basic/([A-Za-z0-9]+)', prop.hcpa_url)
        if match:
            hcpa_parcel_id = match.group(1)
        else:
            match = re.search(r'ParcelID=([A-Za-z0-9]+)', prop.hcpa_url)
            if match:
                hcpa_parcel_id = match.group(1)

        if not hcpa_parcel_id:
            result["error"] = f"Could not extract parcel ID from URL: {prop.hcpa_url}"
            return result
        logger.info(f"Enriching {prop.case_number} with HCPA data (parcel: {hcpa_parcel_id})")

        try:
            hcpa_data = await scrape_hcpa_property(parcel_id=hcpa_parcel_id, storage=self.storage)

            if hcpa_data:
                result["success"] = True
                result["hcpa_data"] = hcpa_data

                # Enrich property with HCPA data
                if hcpa_data.get("folio"):
                    # Store the folio from HCPA for comparison
                    result["hcpa_folio"] = hcpa_data["folio"]

                if hcpa_data.get("legal_description"):
                    prop.legal_description = hcpa_data["legal_description"]

                if hcpa_data.get("property_info", {}).get("site_address"):
                    # Only update if we don't have an address or HCPA is more complete
                    hcpa_addr = hcpa_data["property_info"]["site_address"]
                    if not prop.address or len(hcpa_addr) > len(prop.address):
                        prop.address = hcpa_addr

                if hcpa_data.get("building_info", {}).get("year_built"):
                    with suppress(ValueError, TypeError):
                        prop.year_built = int(hcpa_data["building_info"]["year_built"])

                if hcpa_data.get("image_url"):
                    prop.image_url = hcpa_data["image_url"]

                if hcpa_data.get("sales_history"):
                    prop.sales_history = hcpa_data["sales_history"]

                # Save the enriched parcel data to database immediately
                # This ensures legal_description is available for ORI search in Step 5
                try:
                    self.db.upsert_parcel(prop)
                    logger.debug(f"Saved parcel data for {prop.parcel_id}")
                except Exception as db_err:
                    logger.warning(f"Failed to save parcel data for {prop.parcel_id}: {db_err}")

                logger.success(f"HCPA enrichment successful for {prop.case_number}: legal='{prop.legal_description[:50] if prop.legal_description else 'N/A'}...'")

        except Exception as e:
            result["error"] = str(e)
            logger.error(f"HCPA enrichment failed for {prop.case_number}: {e}")

        return result

if __name__ == "__main__":
    # Test run
    scraper = AuctionScraper()
    # Run for a known date
    asyncio.run(scraper.scrape_date(date(2025, 11, 26)))
