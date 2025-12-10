"""
Sunbiz.org scraper for Florida corporation and LLC status lookup.

Florida Division of Corporations - search.sunbiz.org
No public API available, so we scrape the web interface.

Usage:
    scraper = SunbizScraper()
    result = await scraper.search_entity("ACME HOLDINGS LLC")
    result = await scraper.search_by_officer("JOHN DOE")
"""

import asyncio
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import List, Optional, Dict, Any
from loguru import logger
from playwright.async_api import async_playwright, Page

from src.services.scraper_storage import ScraperStorage


@dataclass
class Officer:
    """Officer or registered agent of a business entity."""
    title: str  # e.g., "President", "Registered Agent", "Manager"
    name: str
    address: Optional[str] = None


@dataclass
class BusinessEntity:
    """Florida business entity information from Sunbiz."""
    document_number: str  # State file number (e.g., L12000012345)
    name: str
    status: str  # Active, Inactive, Dissolved, etc.
    entity_type: str  # LLC, Corporation, LP, etc.

    filing_date: Optional[date] = None
    state: Optional[str] = None  # State of formation (FL, DE, etc.)
    principal_address: Optional[str] = None
    mailing_address: Optional[str] = None
    registered_agent: Optional[str] = None
    registered_agent_address: Optional[str] = None

    officers: List[Officer] = field(default_factory=list)
    annual_reports: List[Dict[str, Any]] = field(default_factory=list)

    last_event: Optional[str] = None  # Most recent filing event
    fei_ein: Optional[str] = None  # Federal Employer ID

    sunbiz_url: Optional[str] = None  # Direct link to detail page


class SunbizScraper:
    """
    Scraper for Florida Sunbiz (Division of Corporations).

    Search methods:
    - By entity name: search_entity()
    - By officer/registered agent name: search_by_officer()
    - By document number: get_entity_details()
    """

    BASE_URL = "https://search.sunbiz.org"

    def __init__(self, headless: bool = True, storage: Optional[ScraperStorage] = None):
        self.headless = headless
        self.storage = storage or ScraperStorage()

    async def search_entity(self, name: str, max_results: int = 5) -> List[BusinessEntity]:
        """
        Search for business entities by name.

        Args:
            name: Entity name to search (partial matches allowed)
            max_results: Maximum number of results to return

        Returns:
            List of BusinessEntity objects
        """
        logger.info(f"Searching Sunbiz for entity: {name}")
        entities = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = await context.new_page()

            try:
                # Navigate to search page
                search_url = f"{self.BASE_URL}/Inquiry/CorporationSearch/ByName"
                logger.info(f"Sunbiz GET: {search_url}")
                await page.goto(search_url, timeout=30000)
                await page.wait_for_load_state("networkidle")

                # Fill search form
                search_input = page.locator("#SearchTerm")
                await search_input.fill(name)

                # Submit search
                await page.click("input[type='submit'][value='Search Now']")
                await page.wait_for_load_state("networkidle")

                # Parse results
                # Results are in a table with links to detail pages
                result_rows = page.locator("table tbody tr")
                count = await result_rows.count()

                if count == 0:
                    logger.info(f"No results found for '{name}'")
                    return []

                logger.info(f"Found {count} results for '{name}'")

                # Get details for each result (up to max_results)
                # Sunbiz table structure: Entity Name | Document Number | Status/Filing Date
                for i in range(min(count, max_results)):
                    try:
                        row = result_rows.nth(i)

                        # Extract basic info from row
                        cells = row.locator("td")
                        cell_count = await cells.count()
                        if cell_count < 2:
                            continue

                        # First cell has link with entity name
                        link = row.locator("a").first
                        entity_name = await link.inner_text()
                        href = await link.get_attribute("href")

                        # Document number is typically in second cell or URL
                        doc_number = ""
                        if cell_count >= 2:
                            doc_number = await cells.nth(1).inner_text()
                            doc_number = doc_number.strip()

                        # Fallback: try to extract from URL
                        if not doc_number and href:
                            doc_number_match = re.search(r'/([A-Z]\d+)', href)
                            if doc_number_match:
                                doc_number = doc_number_match.group(1)

                        # Status is typically in third cell (if present)
                        status = "Unknown"
                        if cell_count >= 3:
                            status = await cells.nth(2).inner_text()
                            status = status.strip()

                        entity = BusinessEntity(
                            document_number=doc_number,
                            name=entity_name.strip(),
                            status=status,
                            entity_type="Unknown",
                            sunbiz_url=f"{self.BASE_URL}{href}" if href else None
                        )

                        entities.append(entity)

                    except Exception as e:
                        logger.warning(f"Error parsing result row {i}: {e}")
                        continue

                # Get detailed info for first result if we have one
                if entities and entities[0].sunbiz_url:
                    entities[0] = await self._get_entity_details(page, entities[0])

            except Exception as e:
                logger.error(f"Error searching Sunbiz: {e}")

            finally:
                await browser.close()

        return entities

    async def search_by_officer(self, officer_name: str, max_results: int = 10) -> List[BusinessEntity]:
        """
        Search for business entities by officer or registered agent name.

        Args:
            officer_name: Name of officer/agent to search
            max_results: Maximum number of results to return

        Returns:
            List of BusinessEntity objects
        """
        logger.info(f"Searching Sunbiz by officer: {officer_name}")
        entities = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = await context.new_page()

            try:
                # Navigate to officer search
                search_url = f"{self.BASE_URL}/Inquiry/CorporationSearch/ByOfficerOrRegisteredAgent"
                logger.info(f"Sunbiz GET: {search_url}")
                await page.goto(search_url, timeout=30000)
                await page.wait_for_load_state("networkidle")

                # Fill search form - name fields
                # Split name into last, first, middle
                name_parts = officer_name.strip().split()

                if len(name_parts) >= 2:
                    # Assume "First Last" or "First Middle Last"
                    last_name = name_parts[-1]
                    first_name = name_parts[0]
                    middle_name = " ".join(name_parts[1:-1]) if len(name_parts) > 2 else ""
                else:
                    last_name = name_parts[0] if name_parts else ""
                    first_name = ""
                    middle_name = ""

                await page.locator("#SearchTerm").fill(last_name)

                first_input = page.locator("#SearchTerm2")
                if await first_input.count() > 0:
                    await first_input.fill(first_name)

                # Submit
                await page.click("input[type='submit'][value='Search Now']")
                await page.wait_for_load_state("networkidle")

                # Parse results (similar to entity search)
                result_rows = page.locator("table tbody tr")
                count = await result_rows.count()

                logger.info(f"Found {count} results for officer '{officer_name}'")

                for i in range(min(count, max_results)):
                    try:
                        row = result_rows.nth(i)
                        link = row.locator("a").first

                        if await link.count() == 0:
                            continue

                        entity_name = await link.inner_text()
                        href = await link.get_attribute("href")

                        doc_number_match = re.search(r'/([A-Z]\d+)', href) if href else None
                        doc_number = doc_number_match.group(1) if doc_number_match else ""

                        entity = BusinessEntity(
                            document_number=doc_number.strip(),
                            name=entity_name.strip(),
                            status="Unknown",
                            entity_type="Unknown",
                            sunbiz_url=f"{self.BASE_URL}{href}" if href else None
                        )
                        entities.append(entity)

                    except Exception as e:
                        logger.warning(f"Error parsing officer search row {i}: {e}")

            except Exception as e:
                logger.error(f"Error searching by officer: {e}")

            finally:
                await browser.close()

        return entities

    async def get_entity_by_doc_number(self, doc_number: str) -> Optional[BusinessEntity]:
        """
        Get detailed entity information by document number.

        Args:
            doc_number: Florida document number (e.g., L12000012345)

        Returns:
            BusinessEntity with full details or None
        """
        logger.info(f"Fetching Sunbiz entity: {doc_number}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = await context.new_page()

            try:
                # Direct URL to detail page
                detail_url = f"{self.BASE_URL}/Inquiry/CorporationSearch/SearchResultDetail?inquirytype=EntityName&directionType=Initial&searchNameOrder={doc_number}"

                entity = BusinessEntity(
                    document_number=doc_number,
                    name="",
                    status="Unknown",
                    entity_type="Unknown",
                    sunbiz_url=detail_url
                )

                return await self._get_entity_details(page, entity)

            except Exception as e:
                logger.error(f"Error fetching entity {doc_number}: {e}")
                return None

            finally:
                await browser.close()

    async def _get_entity_details(self, page: Page, entity: BusinessEntity) -> BusinessEntity:
        """
        Navigate to entity detail page and extract full information.
        """
        if not entity.sunbiz_url:
            return entity

        try:
            logger.info(f"Sunbiz GET: {entity.sunbiz_url}")
            await page.goto(entity.sunbiz_url, timeout=30000)
            await page.wait_for_load_state("networkidle")

            # Get page content
            content = await page.content()
            text = await page.locator("body").inner_text()

            # Parse entity type from title or content
            if "Limited Liability Company" in text or "L.L.C." in text:
                entity.entity_type = "LLC"
            elif "Corporation" in text:
                entity.entity_type = "Corporation"
            elif "Limited Partnership" in text:
                entity.entity_type = "Limited Partnership"

            # Parse structured data
            # Look for labeled sections
            lines = text.split('\n')

            for i, line in enumerate(lines):
                line = line.strip()

                # Document Number
                if "Document Number" in line:
                    next_line = lines[i+1].strip() if i+1 < len(lines) else ""
                    if next_line and ":" not in next_line:
                        entity.document_number = next_line

                # FEI/EIN Number
                if "FEI/EIN Number" in line:
                    next_line = lines[i+1].strip() if i+1 < len(lines) else ""
                    if next_line and ":" not in next_line:
                        entity.fei_ein = next_line

                # Date Filed
                if "Date Filed" in line:
                    next_line = lines[i+1].strip() if i+1 < len(lines) else ""
                    try:
                        entity.filing_date = datetime.strptime(next_line, "%m/%d/%Y").date()
                    except (ValueError, TypeError):
                        pass

                # Status
                if "Status" in line and line.strip() == "Status":
                    next_line = lines[i+1].strip() if i+1 < len(lines) else ""
                    if next_line:
                        entity.status = next_line

                # State
                if "State" in line and line.strip() == "State":
                    next_line = lines[i+1].strip() if i+1 < len(lines) else ""
                    if next_line and len(next_line) == 2:
                        entity.state = next_line

                # Principal Address
                if "Principal Address" in line:
                    addr_lines = []
                    for j in range(i+1, min(i+5, len(lines))):
                        next_line = lines[j].strip()
                        if not next_line or "Address" in next_line or "Agent" in next_line:
                            break
                        addr_lines.append(next_line)
                    if addr_lines:
                        entity.principal_address = ", ".join(addr_lines)

                # Mailing Address
                if "Mailing Address" in line:
                    addr_lines = []
                    for j in range(i+1, min(i+5, len(lines))):
                        next_line = lines[j].strip()
                        if not next_line or "Address" in next_line or "Agent" in next_line:
                            break
                        addr_lines.append(next_line)
                    if addr_lines:
                        entity.mailing_address = ", ".join(addr_lines)

                # Registered Agent
                if "Registered Agent Name" in line:
                    next_line = lines[i+1].strip() if i+1 < len(lines) else ""
                    if next_line:
                        entity.registered_agent = next_line

                # Last Event
                if "Last Event" in line:
                    next_line = lines[i+1].strip() if i+1 < len(lines) else ""
                    if next_line:
                        entity.last_event = next_line

            # Parse Officers section
            # Look for Officer/Director Detail section
            officer_section = re.search(r'Officer/Director Detail(.*?)(?:Annual Reports|Document Images|$)', text, re.DOTALL)
            if officer_section:
                officer_text = officer_section.group(1)
                # Parse individual officers - format varies
                officer_blocks = re.findall(r'(Title\s+\w+.*?)(?=Title|$)', officer_text, re.DOTALL)
                for block in officer_blocks[:10]:  # Limit to 10 officers
                    title_match = re.search(r'Title\s+(\w+)', block)
                    name_match = re.search(r'(?:Name|^)\s*([A-Z][A-Z\s,]+)', block)
                    if title_match:
                        officer = Officer(
                            title=title_match.group(1).strip(),
                            name=name_match.group(1).strip() if name_match else "Unknown"
                        )
                        entity.officers.append(officer)

            logger.info(f"Parsed entity: {entity.name} ({entity.status})")

        except Exception as e:
            logger.error(f"Error parsing entity details: {e}")

        return entity


    async def search_for_property(
        self,
        property_id: str,
        owner_name: str,
        force_refresh: bool = False
    ) -> List[BusinessEntity]:
        """
        Search for business entities by owner name and store results.

        Args:
            property_id: Property folio/ID for storage
            owner_name: Name of property owner
            force_refresh: Force re-search even if cached

        Returns:
            List of BusinessEntity objects
        """
        # Check cache
        if not force_refresh and not self.storage.needs_refresh(property_id, "sunbiz", max_age_days=30):
            cached = self.storage.get_latest(property_id, "sunbiz")
            if cached and cached.raw_data_path:
                logger.debug(f"Using cached Sunbiz data for {property_id}")
                return []  # Return empty, data is in cache

        # Search by officer name
        entities = await self.search_by_officer(owner_name, max_results=10)

        # Convert to dicts for storage
        entities_data = []
        for entity in entities:
            entity_dict = {
                "document_number": entity.document_number,
                "name": entity.name,
                "status": entity.status,
                "entity_type": entity.entity_type,
                "filing_date": entity.filing_date.isoformat() if entity.filing_date else None,
                "state": entity.state,
                "principal_address": entity.principal_address,
                "mailing_address": entity.mailing_address,
                "registered_agent": entity.registered_agent,
                "registered_agent_address": entity.registered_agent_address,
                "officers": [{"title": o.title, "name": o.name, "address": o.address} for o in entity.officers],
                "last_event": entity.last_event,
                "fei_ein": entity.fei_ein,
                "sunbiz_url": entity.sunbiz_url
            }
            entities_data.append(entity_dict)

        # Save raw data
        raw_path = self.storage.save_raw_data(
            property_id=property_id,
            scraper="sunbiz",
            data={
                "owner_name": owner_name,
                "search_date": datetime.now().isoformat(),
                "entities": entities_data
            },
            context="officer_search"
        )

        # Record in database
        self.storage.record_scrape(
            property_id=property_id,
            scraper="sunbiz",
            raw_data_path=raw_path,
            vision_data={"entities": entities_data},
            success=True
        )

        logger.info(f"Saved Sunbiz data for {property_id}: {len(entities)} entities found")
        return entities


async def check_owner_business_status(owner_name: str) -> Dict[str, Any]:
    """
    Convenience function to check if a property owner has associated business entities.

    Args:
        owner_name: Name of property owner

    Returns:
        Dictionary with business entity information
    """
    scraper = SunbizScraper(headless=True)

    result = {
        "owner_name": owner_name,
        "entities_found": [],
        "is_business_owner": False,
        "active_entities": 0,
        "inactive_entities": 0
    }

    # Search by officer name
    entities = await scraper.search_by_officer(owner_name, max_results=10)

    for entity in entities:
        result["entities_found"].append({
            "name": entity.name,
            "doc_number": entity.document_number,
            "status": entity.status,
            "type": entity.entity_type,
            "url": entity.sunbiz_url
        })

        if entity.status.upper() == "ACTIVE":
            result["active_entities"] += 1
        else:
            result["inactive_entities"] += 1

    result["is_business_owner"] = len(entities) > 0

    return result


if __name__ == "__main__":
    async def main():
        scraper = SunbizScraper(headless=False)

        # Test entity search
        print("\n=== Entity Search ===")
        results = await scraper.search_entity("TAMPA HOLDINGS LLC")
        for entity in results:
            print(f"  {entity.name} ({entity.document_number}) - {entity.status}")
            if entity.officers:
                print(f"    Officers: {[o.name for o in entity.officers]}")

        # Test officer search
        print("\n=== Officer Search ===")
        results = await scraper.search_by_officer("JOHN SMITH")
        for entity in results[:5]:
            print(f"  {entity.name} ({entity.document_number})")

    asyncio.run(main())
