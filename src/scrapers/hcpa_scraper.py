import asyncio
import random
import json
from typing import Optional
from loguru import logger
from playwright.async_api import async_playwright

from src.models.property import Property
from src.services.scraper_storage import ScraperStorage

USER_AGENT = "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Mobile Safari/537.36,gzip(gfe)"


class HCPAScraper:
    BASE_URL = "https://gis.hcpafl.org/propertysearch/"
    
    def __init__(self, headless: bool = True, storage: Optional[ScraperStorage] = None):
        from src.services.scraper_storage import ScraperStorage
        self.headless = headless
        self.storage = storage or ScraperStorage()
    
    async def enrich_property(self, prop: Property) -> Property:
        """
        Enriches a property object with details from the Property Appraiser website.
        Uses Parcel ID for search.
        """
        if not prop.parcel_id:
            logger.info("Skipping HCPA enrichment for {case_number}: missing Parcel ID", case_number=prop.case_number)
            return prop
            
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(
                user_agent=USER_AGENT
            )
            page = await context.new_page()
            
            try:
                logger.info("Visiting {url} to enrich parcel {parcel} (case {case})", url=self.BASE_URL, parcel=prop.parcel_id, case=prop.case_number)
                await page.goto(self.BASE_URL, timeout=60000)
                await page.wait_for_load_state("networkidle")
                
                # Try Parcel ID Search first
                # Select "Parcel Number" radio button in Basic Search
                await page.click("#basic input[name='basicPinGroup'][value='pin']")
                
                # Wait for the input to appear
                search_box = page.locator("#basic input[data-bind*='value: parcelNumber']")
                await search_box.wait_for(state="visible")
                
                await asyncio.sleep(random.uniform(0.5, 1.5))  # noqa: S311
                await search_box.fill(prop.parcel_id)
                await page.click("#basic button[data-bind*='click: search']")
                
                # Wait for results
                await page.wait_for_timeout(3000)
                
                # Check for results table
                results_table = page.locator("#table-basic-results")
                
                if not await results_table.is_visible():
                    logger.info("Parcel ID search for {parcel} yielded no results; falling back to address search for {address}", parcel=prop.parcel_id, address=prop.address)
                    # Reload page to reset state
                    await page.reload()
                    await page.wait_for_load_state("networkidle")
                    
                    # Click on Basic Search tab to ensure it's active
                    basic_search_tab = page.locator("li.tab:has-text('Basic Search')")
                    await basic_search_tab.click()
                    await asyncio.sleep(0.5)
                    
                    logger.info("Searching HCPA by address: {address}", address=prop.address)
                    address_box = page.locator("#basic input[data-bind*='value: address']")
                    await address_box.fill(prop.address)
                    await page.click("#basic button[data-bind*='click: search']")
                    await page.wait_for_timeout(3000)
                
                # Check for results table again
                if await results_table.is_visible():
                    logger.info("HCPA search returned results; clicking first row for {parcel}", parcel=prop.parcel_id)
                    # Click the first row's first cell
                    await results_table.locator("tbody tr:first-child td").first.click()
                    
                    # Wait for details tab to be active and visible
                    details_container = page.locator("#details")
                    await details_container.wait_for(state="visible", timeout=15000)
                    
                    # Wait for specific content to ensure full load
                    # "PROPERTY RECORD CARD" is in a h4.section-header
                    try:
                        await details_container.locator("h4", has_text="PROPERTY RECORD CARD").wait_for(state="visible", timeout=10000)
                        logger.info("HCPA details page loaded (found 'PROPERTY RECORD CARD') for {parcel}", parcel=prop.parcel_id)
                    except Exception:
                        logger.warning("Timed out waiting for 'PROPERTY RECORD CARD' header for {parcel}", parcel=prop.parcel_id)

                    # Give a brief pause for any dynamic content/images to render
                    await asyncio.sleep(2.0)

                    # Dump HTML for debugging
                    logger.info("Dumping HCPA details page HTML for {parcel}", parcel=prop.parcel_id)
                    content = await page.content()
                    with open("debug_hcpa_details.html", "w", encoding="utf-8") as f:
                        f.write(content)

                    # Take screenshot of the details container exclusively to avoid layout issues
                    # If full page is needed, we can do that too, but element screenshot is safer for Vision
                    try:
                        screenshot_bytes = await details_container.screenshot()
                    except Exception as e:
                        logger.warning(f"Element screenshot failed: {e}. Fallback to full page.")
                        screenshot_bytes = await page.screenshot(full_page=True)

                    # Save using ScraperStorage
                    screenshot_path = self.storage.save_screenshot(
                        property_id=prop.parcel_id,
                        scraper="hcpa",
                        image_data=screenshot_bytes,
                        context="details"
                    )
                    logger.info("Captured HCPA details screenshot: {path}", path=screenshot_path)

                    # Get full path for vision service
                    full_screenshot_path = self.storage.get_full_path(prop.parcel_id, screenshot_path)

                    # Use VisionService to extract data
                    from src.services.vision_service import VisionService
                    vision = VisionService()

                    logger.info("Analyzing HCPA screenshot with VisionService...")
                    data = vision.extract_hcpa_details(str(full_screenshot_path))

                    if data:
                        logger.info("Successfully extracted HCPA data: {keys}", keys=data.keys())

                        # Save vision output
                        vision_path = self.storage.save_vision_output(
                            property_id=prop.parcel_id,
                            scraper="hcpa",
                            vision_data=data,
                            screenshot_path=screenshot_path,
                            prompt_version="v1"
                        )

                        # Record scrape
                        self.storage.record_scrape(
                            property_id=prop.parcel_id,
                            scraper="hcpa",
                            screenshot_path=screenshot_path,
                            vision_output_path=vision_path,
                            vision_data=data,
                            success=True
                        )

                        # Map to Property object
                        if "owner_info" in data:
                            prop.owner_name = data["owner_info"].get("owner_name")

                        if "building_info" in data:
                            b_info = data["building_info"]
                            prop.year_built = self._parse_int(b_info.get("year_built"))
                            prop.beds = self._parse_float(b_info.get("beds"))
                            prop.baths = self._parse_float(b_info.get("baths"))
                            prop.heated_area = self._parse_float(b_info.get("heated_area"))

                        # Extract legal description from property details
                        if "property_details" in data:
                            p_details = data["property_details"]
                            if p_details.get("legal_description"):
                                prop.legal_description = p_details.get("legal_description")
                                logger.info("Extracted legal description for {parcel}: {legal}",
                                           parcel=prop.parcel_id,
                                           legal=prop.legal_description[:50] if prop.legal_description else "N/A")

                        # Extract value summary (assessed value, market value, etc.)
                        if "value_summary" in data:
                            v_summary = data["value_summary"]
                            if v_summary.get("assessed_value"):
                                prop.assessed_value = self._parse_float(v_summary.get("assessed_value"))
                            # Could add market_value to Property model if needed

                        # Extract sales history - this is important for chain of title
                        if "sales_history" in data and isinstance(data["sales_history"], list):
                            prop.sales_history = data["sales_history"]
                            logger.info("Extracted {count} sales history records for {parcel}",
                                       count=len(prop.sales_history), parcel=prop.parcel_id)

                        # Store the full raw analysis for later use
                        prop.market_analysis_content = json.dumps(data)

                    else:
                        logger.warning("VisionService returned no data for HCPA screenshot")
                        # Record failed scrape (or partial success since we got screenshot)
                        self.storage.record_scrape(
                            property_id=prop.parcel_id,
                            scraper="hcpa",
                            screenshot_path=screenshot_path,
                            success=False,
                            error="No data extracted from vision service"
                        )

                    # Image Extraction
                    try:
                        # Try to find the main property image
                        # Strategy 1: Look for img with specific ID or class if known (not known yet)
                        # Strategy 2: Look for img with src containing 'photo' or 'cam'
                        images = await page.locator("img").all()
                        for img in images:
                            src = await img.get_attribute("src")
                            if src and ("photo" in src.lower() or "pictometry" in src.lower() or "getimage" in src.lower()):
                                if src.startswith("http"):
                                    prop.image_url = src
                                elif src.startswith("/"):
                                    prop.image_url = f"https://gis.hcpafl.org{src}"
                                else:
                                    prop.image_url = f"https://gis.hcpafl.org/PropertySearch/{src}"
                                logger.info("Found property image URL for {parcel}: {url}", parcel=prop.parcel_id, url=prop.image_url)
                                break
                    except Exception as e:
                        logger.error("Error extracting image for parcel {parcel}: {error}", parcel=prop.parcel_id, error=e)

                    logger.info("Enriched parcel {parcel}: owner={owner} year_built={year}", parcel=prop.parcel_id, owner=prop.owner_name, year=prop.year_built)

                    if not prop.owner_name:
                        logger.warning("Owner not found for parcel {parcel}. Dumping HTML to debug_hcpa_result.html", parcel=prop.parcel_id)
                        content = await page.content()
                        with open("debug_hcpa_result.html", "w", encoding="utf-8") as f:
                            f.write(content)
                else:
                    logger.warning("HCPA search returned no results for parcel {parcel}", parcel=prop.parcel_id)
                
            except Exception as e:
                logger.error("Error enriching property {parcel}: {error}", parcel=prop.parcel_id, error=e)
                await page.screenshot(path=f"error_hcpa_{prop.parcel_id}.png")
                # We don't raise here to allow partial success of the pipeline
                
            finally:
                await browser.close()
                
        return prop

    def _parse_int(self, val):
        if not val: return None
        try:
            return int(str(val).replace(',', '').split('.')[0])
        except (ValueError, TypeError):
            return None

    def _parse_float(self, val):
        if not val: return None
        try:
            return float(str(val).replace(',', '').replace('$', ''))
        except (ValueError, TypeError):
            return None

if __name__ == "__main__":
    # Test run
    scraper = HCPAScraper()
    test_prop = Property(
        case_number="Test",
        parcel_id="127219-1000", # Example from user
        address="3006 W Julia St"
    )
    asyncio.run(scraper.enrich_property(test_prop))
