"""
Building permit scraper for City of Tampa and Hillsborough County.

City of Tampa (Accela): https://aca-prod.accela.com/TAMPA/Default.aspx
Hillsborough County: https://aca-prod.accela.com/HCFL/

Both use the Accela Citizen Access platform.

The search interface requires parsing addresses into components:
- Street Number: 3006
- Street Direction: W (optional)
- Street Name: Julia
- Street Type: St
- Unit: A (optional)

Usage:
    scraper = PermitScraper()
    permits = await scraper.get_permits_city("3006 W Julia St", "Tampa")
    permits = await scraper.get_permits_county("123 Main St", "Brandon")
"""

import asyncio
import re
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
from loguru import logger

from playwright.async_api import async_playwright, Page

# Import VisionService for complex pages
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.services.vision_service import VisionService
from src.services.scraper_storage import ScraperStorage


from src.models.property import Permit

# Removed PermitDetail dataclass in favor of shared Pydantic model


class AddressParser:
    """Parse street addresses into Accela search components."""

    # Common street types and their abbreviations
    STREET_TYPES = {
        'STREET': 'ST', 'ST': 'ST',
        'AVENUE': 'AVE', 'AVE': 'AVE',
        'BOULEVARD': 'BLVD', 'BLVD': 'BLVD',
        'DRIVE': 'DR', 'DR': 'DR',
        'ROAD': 'RD', 'RD': 'RD',
        'LANE': 'LN', 'LN': 'LN',
        'COURT': 'CT', 'CT': 'CT',
        'CIRCLE': 'CIR', 'CIR': 'CIR',
        'PLACE': 'PL', 'PL': 'PL',
        'WAY': 'WAY',
        'TERRACE': 'TER', 'TER': 'TER',
        'HIGHWAY': 'HWY', 'HWY': 'HWY',
        'PARKWAY': 'PKWY', 'PKWY': 'PKWY',
        'TRAIL': 'TRL', 'TRL': 'TRL',
    }

    DIRECTIONS = {'N', 'S', 'E', 'W', 'NE', 'NW', 'SE', 'SW', 'NORTH', 'SOUTH', 'EAST', 'WEST'}

    @classmethod
    def parse(cls, address: str) -> Dict[str, str]:
        """
        Parse address into components.

        Returns dict with keys:
        - street_number
        - street_direction (optional)
        - street_name
        - street_type
        - unit (optional)
        """
        result = {
            'street_number': '',
            'street_direction': '',
            'street_name': '',
            'street_type': '',
            'unit': ''
        }

        # Clean and uppercase
        addr = address.upper().strip()

        # Remove common unit designators and extract unit
        unit_match = re.search(r'\b(UNIT|APT|STE|SUITE)\s*([A-Z0-9-]+)$', addr)
        if unit_match:
            result['unit'] = unit_match.group(2)
            addr = addr[:unit_match.start()].strip()
        else:
            # Handle #100 style units
            hash_match = re.search(r'\s*#\s*([A-Z0-9-]+)$', addr)
            if hash_match:
                result['unit'] = hash_match.group(1)
                addr = addr[:hash_match.start()].strip()

        # Split into parts
        parts = addr.split()

        if not parts:
            return result

        # First part should be street number
        if parts[0].isdigit() or re.match(r'^\d+[A-Z]?$', parts[0]):
            result['street_number'] = parts[0]
            parts = parts[1:]

        if not parts:
            return result

        # Check for direction prefix
        if parts[0] in cls.DIRECTIONS or parts[0] in {'N', 'S', 'E', 'W'}:
            result['street_direction'] = parts[0][0] if len(parts[0]) > 1 else parts[0]
            parts = parts[1:]

        if not parts:
            return result

        # Last part might be street type
        last_part = parts[-1].rstrip('.,')
        if last_part in cls.STREET_TYPES:
            result['street_type'] = cls.STREET_TYPES[last_part]
            parts = parts[:-1]

        # Remaining parts are street name
        if parts:
            result['street_name'] = ' '.join(parts)

        return result


class PermitScraper:
    """
    Scraper for City of Tampa and Hillsborough County building permits.

    Both jurisdictions use Accela Citizen Access platform.
    """

    # Accela portal URLs
    CITY_URL = "https://aca-prod.accela.com/TAMPA/Default.aspx"
    COUNTY_URL = "https://aca-prod.accela.com/HCFL/Default.aspx"

    def __init__(self, headless: bool = False, use_vision: bool = True, storage: Optional[ScraperStorage] = None):
        """
        Initialize permit scraper.

        Args:
            headless: Run browser in headless mode
            use_vision: Use VisionService for complex pages (recommended)
            storage: ScraperStorage instance for caching
        """
        self.headless = headless
        self.use_vision = use_vision
        self.vision = VisionService() if use_vision else None
        self.storage = storage or ScraperStorage()
        self.output_dir = Path("data/permit_screenshots")
        self.output_dir.mkdir(parents=True, exist_ok=True)

    async def get_permits_city(self, address: str, city: str = "Tampa") -> List[Permit]:
        """
        Get building permits and other records from City of Tampa portal.

        Args:
            address: Street address
            city: City name (default Tampa)

        Returns:
            List of PermitDetail objects
        """
        return await self.get_tampa_records(address)

    async def get_permits_county(self, address: str) -> List[Permit]:
        """
        Get building permits from Hillsborough County portal.

        Args:
            address: Street address

        Returns:
            List of PermitDetail objects
        """
        return await self._scrape_accela(self.COUNTY_URL, address, "Hillsborough County")

    async def get_tampa_records(self, address: str) -> List[Permit]:
        """
        Get all records (Permits, Code Enforcement) from Tampa Global Search.

        Args:
            address: Street address

        Returns:
            List of PermitDetail objects
        """
        # Clean address for URL
        clean_addr = address.replace("#", "").strip()
        encoded_addr = clean_addr.replace(" ", "%20")
        url = f"https://aca-prod.accela.com/TAMPA/Cap/GlobalSearchResults.aspx?isNewQuery=yes&QueryText={encoded_addr}#CAPList"
        
        return await self._scrape_accela(url, address, "City of Tampa Global")

    async def get_permits(self, address: str, city: str = "Tampa") -> List[Permit]:
        """
        Get permits from both City and County portals.

        Properties within Tampa city limits should have City permits.
        Properties in unincorporated areas (Brandon, Lutz, etc.) have County permits.

        Args:
            address: Street address
            city: City name

        Returns:
            Combined list of permits from both sources
        """
        permits = []

        # Try City of Tampa first
        if city.upper() in ["TAMPA", "TEMPLE TERRACE", "PLANT CITY"]:
            try:
                city_permits = await self.get_permits_city(address, city)
                permits.extend(city_permits)
            except Exception as e:
                logger.warning(f"City permit search failed: {e}")

        # Also try County (some properties may have both)
        try:
            county_permits = await self.get_permits_county(address)
            permits.extend(county_permits)
        except Exception as e:
            logger.warning(f"County permit search failed: {e}")

        return permits

    async def _scrape_accela(self, base_url: str, address: str, source: str) -> List[Permit]:
        """
        Scrape Accela Citizen Access portal for permits.

        Args:
            base_url: Accela portal URL
            address: Street address to search
            source: Source name for logging

        Returns:
            List of Permit objects
        """
        logger.info(f"Searching {source} permits for: {address}")
        permits = []

        # Parse address
        addr_parts = AddressParser.parse(address)
        logger.debug(f"Parsed address: {addr_parts}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                viewport={'width': 1280, 'height': 900}
            )
            page = await context.new_page()

            try:
                # Navigate to portal
                logger.info(f"Permit Portal GET: {base_url}")
                await page.goto(base_url, timeout=60000)
                await page.wait_for_load_state("networkidle")
                await asyncio.sleep(2)

                # Look for global search box first (faster if available)
                global_search = page.locator("#txtSearchCondition, input[placeholder*='Search']").first

                if await global_search.is_visible(timeout=5000):
                    # Use global search
                    logger.info("Using global address search")
                    await global_search.fill(address)
                    await asyncio.sleep(0.5)

                    # Submit search
                    await global_search.press("Enter")
                    await page.wait_for_load_state("networkidle")
                    await asyncio.sleep(3)

                else:
                    # Navigate to Building module search
                    logger.info("Navigating to Building module search")

                    # Click on Building tab/link
                    building_link = page.locator("a:has-text('Building'), div:has-text('Building')").first
                    if await building_link.is_visible(timeout=5000):
                        await building_link.click()
                        await page.wait_for_load_state("networkidle")
                        await asyncio.sleep(2)

                    # Look for "Search Applications" or similar
                    search_link = page.locator("a:has-text('Search'), span:has-text('Search Applications')").first
                    if await search_link.is_visible(timeout=5000):
                        await search_link.click()
                        await page.wait_for_load_state("networkidle")
                        await asyncio.sleep(2)

                    # Fill address fields
                    await self._fill_address_fields(page, addr_parts)

                    # Submit search
                    submit_btn = page.locator("button:has-text('Search'), input[value='Search'], a:has-text('Search')").first
                    if await submit_btn.is_visible(timeout=5000):
                        await submit_btn.click()
                        await page.wait_for_load_state("networkidle")
                        await asyncio.sleep(3)

                # Take screenshot of results
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                safe_addr = re.sub(r'[^\w\s-]', '', address).replace(' ', '_')[:30]
                screenshot_path = self.output_dir / f"permit_{source.replace(' ', '_')}_{safe_addr}_{timestamp}.png"
                await page.screenshot(path=str(screenshot_path), full_page=True)
                logger.info(f"Screenshot saved: {screenshot_path}")

                # Parse results
                if self.use_vision and self.vision:
                    permits = await self._extract_with_vision(str(screenshot_path), page)
                else:
                    permits = await self._extract_from_page(page)

                # Set source for all permits
                for permit in permits:
                    permit.address = address

            except Exception as e:
                logger.error(f"Error scraping {source}: {e}")
                # Take error screenshot
                with suppress(Exception):
                    await page.screenshot(path=str(self.output_dir / f"error_{source}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"))

            finally:
                await browser.close()

        logger.info(f"Found {len(permits)} permits from {source}")
        return permits

    async def _fill_address_fields(self, page: Page, addr_parts: Dict[str, str]):
        """Fill Accela address search fields."""
        # Common field IDs in Accela
        field_mappings = [
            ('street_number', ['ctl00_PlaceHolderMain_refAddressSearch_txtStreetNo',
                              'txtStreetNo', 'StreetNo', 'txtHouseNumber']),
            ('street_direction', ['ctl00_PlaceHolderMain_refAddressSearch_ddlStreetDirection',
                                 'ddlStreetDirection', 'StreetDir']),
            ('street_name', ['ctl00_PlaceHolderMain_refAddressSearch_txtStreetName',
                            'txtStreetName', 'StreetName']),
            ('street_type', ['ctl00_PlaceHolderMain_refAddressSearch_ddlStreetSuffix',
                            'ddlStreetSuffix', 'StreetType']),
            ('unit', ['ctl00_PlaceHolderMain_refAddressSearch_txtUnit',
                     'txtUnit', 'UnitNo'])
        ]

        for field_name, selectors in field_mappings:
            value = addr_parts.get(field_name, '')
            if not value:
                continue

            for selector in selectors:
                try:
                    # Try as ID
                    field = page.locator(f"#{selector}").first
                    if await field.is_visible(timeout=1000):
                        tag = await field.evaluate("el => el.tagName")
                        if tag.lower() == 'select':
                            await field.select_option(value=value)
                        else:
                            await field.fill(value)
                        logger.debug(f"Filled {field_name}: {value}")
                        break
                except Exception as exc:
                    logger.debug(f"Could not fill {field_name} for selector {selector}: {exc}")
                    continue

    async def _extract_with_vision(self, screenshot_path: str, page: Page) -> List[Permit]:
        """Extract permit data from screenshot using VisionService."""
        permits = []

        try:
            data = await self.vision.process_async(self.vision.extract_permit_results, screenshot_path)

            if data and data.get("permits"):
                for p in data["permits"]:
                    permit = Permit(
                        permit_number=p.get("permit_number", "Unknown"),
                        type=p.get("permit_type", "Unknown"),
                        status=p.get("status", "Unknown"),
                        description=p.get("description"),
                    )

                    # Parse date
                    if p.get("issue_date"):
                        with suppress(ValueError, TypeError):
                            permit.issue_date = datetime.strptime(p["issue_date"], "%m/%d/%Y").date()

                    permits.append(permit)

        except Exception as e:
            logger.error(f"Vision extraction failed: {e}")
            # Fall back to page extraction
            permits = await self._extract_from_page(page)

        return permits

    async def _extract_from_page(self, page: Page) -> List[Permit]:
        """Extract permit data directly from page HTML."""
        permits = []

        try:
            # Look for results table
            # Global search table often has id 'ctl00_PlaceHolderMain_gdvPermitList'
            table = page.locator("table[id*='gdvPermitList']").first
            if await table.count() == 0:
                # Fallback to class
                table = page.locator("table.ACA_GridView").first
            
            rows = table.locator("tr")
            count = await rows.count()

            if count <= 1:  # Only header row
                logger.info("No permits found in results table")
                return permits

            # Find header row
            header_idx = 0
            found_headers = False
            
            # Scan first few rows for headers
            for i in range(min(5, count)):
                row = rows.nth(i)
                texts = await row.locator("th, td").all_inner_texts()
                texts = [t.strip().upper() for t in texts]
                
                # Check for characteristic column names
                if any("RECORD NUMBER" in t or "PERMIT NUMBER" in t or "DATE" in t for t in texts):
                    header_idx = i
                    headers = texts
                    found_headers = True
                    logger.info(f"Found headers at row {i}: {headers}")
                    break
            
            # Map column indices
            col_map = {}
            if found_headers:
                for idx, h in enumerate(headers):
                    if "RECORD NUMBER" in h or "PERMIT NUMBER" in h:
                        col_map['number'] = idx
                    elif "RECORD TYPE" in h or "PERMIT TYPE" in h:
                        col_map['type'] = idx
                    elif "STATUS" in h:
                        col_map['status'] = idx
                    elif "DATE" in h:
                        col_map['date'] = idx
                    elif "MODULE" in h:
                        col_map['module'] = idx
                    elif "DESCRIPTION" in h or "NOTES" in h:
                        col_map['desc'] = idx
                    elif "PROJECT NAME" in h:
                        col_map['project'] = idx
                logger.info(f"Column map: {col_map}")

            # Fallback if headers not found or incomplete
            if 'number' not in col_map:
                logger.info("Using default global search column mapping")
                col_map = {
                    'date': 0,
                    'number': 1,
                    'type': 2,
                    'module': 3,
                    'desc': 4,
                    'project': 5,
                    'address': 6,
                    'status': 7
                }
                # If we didn't find headers, assume row 0 is special (pagination) if it has colspan
                # But safer to just look for data pattern or assume header is row 1 if row 0 didn't match
                if not found_headers and count > 1:
                     # Heuristic: verify if row 1 looks like header? No, we already scanned.
                     # Just assume data starts after the row we checked? 
                     # If we failed to find headers, maybe the table doesn't have them or they are weird.
                     # Let's assume start index is 1 (legacy behavior) but check if row 0 was pagination.
                     pass

            # Iterate data rows
            start_row = header_idx + 1 if found_headers else 1
            for i in range(start_row, count):
                try:
                    row = rows.nth(i)
                    
                    # Check if it's a pagination row or empty
                    row_text = await row.inner_text()
                    if not row_text.strip() or "Prev" in row_text or "Next" in row_text or "Showing" in row_text:
                        continue

                    cells = row.locator("td")
                    cell_count = await cells.count()

                    if cell_count < len(col_map) and cell_count < 3:
                        continue
                        
                    # Extract data using map
                    permit_num = await cells.nth(col_map.get('number', 0)).inner_text() if 'number' in col_map else await cells.nth(0).inner_text()
                    permit_type = await cells.nth(col_map.get('type', 1)).inner_text() if 'type' in col_map else (await cells.nth(1).inner_text() if cell_count > 1 else "Unknown")
                    status = await cells.nth(col_map.get('status', 2)).inner_text() if 'status' in col_map else (await cells.nth(2).inner_text() if cell_count > 2 else "Unknown")
                    
                    module = "Unknown"
                    if 'module' in col_map:
                        module = await cells.nth(col_map['module']).inner_text()
                    
                    date_str = None
                    if 'date' in col_map:
                        date_str = await cells.nth(col_map['date']).inner_text()
                        
                    desc = None
                    if 'desc' in col_map:
                        desc = await cells.nth(col_map['desc']).inner_text()
                    elif 'project' in col_map:
                        desc = await cells.nth(col_map['project']).inner_text()

                    # Clean up strings
                    permit_num = permit_num.strip().split('\n')[0]
                    permit_type = permit_type.strip()
                    status = status.strip()
                    module = module.strip()
                    if desc:
                        desc = desc.strip()

                    permit = Permit(
                        permit_number=permit_num,
                        type=permit_type,
                        status=status,
                        module=module,
                        description=desc,
                        url=f"https://aca-prod.accela.com/TAMPA/Cap/GlobalSearchResults.aspx?QueryText={permit_num}"
                    )
                    
                    if date_str:
                        with suppress(Exception):
                            permit.issue_date = datetime.strptime(date_str.strip(), "%m/%d/%Y").date()
                            
                    permits.append(permit)

                except Exception as e:
                    logger.debug(f"Error parsing row {i}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Page extraction failed: {e}")

        return permits


    async def get_permits_for_property(
        self,
        property_id: str,
        address: str,
        city: str = "Tampa",
        force_refresh: bool = False
    ) -> List[Permit]:
        """
        Get permits for a property with caching.

        Args:
            property_id: Property folio/ID for storage
            address: Street address
            city: City name
            force_refresh: Force re-scrape even if cached

        Returns:
            List of Permit objects
        """
        # Check cache
        if not force_refresh and not self.storage.needs_refresh(property_id, "permits", max_age_days=7):
            cached = self.storage.get_latest(property_id, "permits")
            if cached and cached.extraction_success:
                logger.debug(f"Using cached permit data for {property_id}")
                return []  # Data is in cache

        # Scrape permits
        permits = await self.get_permits(address, city)

        # Convert to dicts for storage
        permits_data = []
        for p in permits:
            permit_dict = {
                "permit_number": p.permit_number,
                "permit_type": p.permit_type,
                "status": p.status,
                "issue_date": p.issue_date.isoformat() if p.issue_date else None,
                "expiration_date": p.expiration_date.isoformat() if p.expiration_date else None,
                "finaled_date": p.finaled_date.isoformat() if p.finaled_date else None,
                "description": p.description,
                "work_description": p.work_description,
                "contractor": p.contractor,
                "contractor_license": p.contractor_license,
                "estimated_cost": p.estimated_cost,
                "fees_paid": p.fees_paid,
                "address": p.address
            }
            permits_data.append(permit_dict)

        # Find most recent screenshot
        screenshot_files = list(self.output_dir.glob(f"permit_*_{address.replace(' ', '_')[:30]}*.png"))
        screenshot_path = None
        if screenshot_files:
            latest_screenshot = max(screenshot_files, key=lambda x: x.stat().st_mtime)
            # Copy to property storage
            screenshot_path = self.storage.save_screenshot_from_file(
                property_id=property_id,
                scraper="permits",
                source_path=str(latest_screenshot),
                context=city.lower().replace(" ", "_")
            )
            
            # Clean up temporary file
            try:
                latest_screenshot.unlink()
            except Exception as e:
                logger.debug(f"Failed to delete temp screenshot {latest_screenshot}: {e}")

        # Save vision output
        vision_path = None
        if permits_data:
            vision_path = self.storage.save_vision_output(
                property_id=property_id,
                scraper="permits",
                vision_data={"permits": permits_data, "address": address, "city": city},
                screenshot_path=screenshot_path,
                prompt_version="v1"
            )

        # Determine source URL
        source_url = None
        if city.upper() in ["TAMPA", "TEMPLE TERRACE", "PLANT CITY"]:
            clean_addr = address.replace("#", "").strip()
            encoded_addr = clean_addr.replace(" ", "%20")
            source_url = f"https://aca-prod.accela.com/TAMPA/Cap/GlobalSearchResults.aspx?isNewQuery=yes&QueryText={encoded_addr}#CAPList"
        else:
            source_url = self.COUNTY_URL

        # Record in database
        self.storage.record_scrape(
            property_id=property_id,
            scraper="permits",
            screenshot_path=screenshot_path,
            vision_output_path=vision_path,
            vision_data={"permits": permits_data},
            prompt_version="v1",
            success=True,
            source_url=source_url
        )

        logger.info(f"Saved permit data for {property_id}: {len(permits)} permits found")
        return permits


# Convenience function
async def check_property_permits(address: str, city: str = "Tampa") -> Dict[str, Any]:
    """
    Check for building permits on a property.

    Args:
        address: Street address
        city: City name

    Returns:
        Dictionary with permit summary
    """
    scraper = PermitScraper(headless=True)
    permits = await scraper.get_permits(address, city)

    result = {
        "address": address,
        "city": city,
        "total_permits": len(permits),
        "open_permits": 0,
        "finaled_permits": 0,
        "has_violations": False,
        "permits": []
    }

    for p in permits:
        permit_info = {
            "number": p.permit_number,
            "type": p.permit_type,
            "status": p.status,
            "issue_date": p.issue_date.isoformat() if p.issue_date else None,
            "description": p.description
        }
        result["permits"].append(permit_info)

        status_upper = p.status.upper() if p.status else ""
        if status_upper in ["FINALED", "FINAL", "CLOSED"]:
            result["finaled_permits"] += 1
        elif status_upper not in ["CANCELLED", "VOID", "WITHDRAWN"]:
            result["open_permits"] += 1

    return result


if __name__ == "__main__":
    async def main():
        scraper = PermitScraper(headless=False)

        print("\n=== City of Tampa Permit Search ===\n")
        permits = await scraper.get_permits_city("3006 W Julia St", "Tampa")

        for p in permits:
            print(f"  {p.permit_number} - {p.permit_type}")
            print(f"    Status: {p.status}")
            print(f"    Issued: {p.issue_date}")
            print(f"    Description: {p.description}")
            print()

        if not permits:
            print("  No permits found")

    asyncio.run(main())
