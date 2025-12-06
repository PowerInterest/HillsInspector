"""
HCPA GIS Property Search Scraper

Scrapes the Hillsborough County Property Appraiser GIS portal to get:
- Sales History with Book/Page/Instrument numbers
- Property details

URL format: https://gis.hcpafl.org/propertysearch/#/parcel/basic/{PARCEL_ID}

The parcel ID format in the URL is different from the bulk data format:
- Bulk data: A-13-29-18-4XZ-000012-00009.0
- URL format: 1829134XZ000012000090A

Usage:
    uv run python -m src.scrapers.hcpa_gis_scraper --parcel 1829134XZ000012000090A
    uv run python -m src.scrapers.hcpa_gis_scraper --folio 1918870000
"""

import json
import re
import argparse
import asyncio
from pathlib import Path
from urllib.parse import quote
import requests
from playwright.async_api import async_playwright
from loguru import logger

from src.services.scraper_storage import ScraperStorage


def convert_bulk_parcel_to_url_format(bulk_parcel: str) -> str:
    """
    Convert bulk data parcel format to URL format.

    Bulk: A-13-29-18-4XZ-000012-00009.0
    URL:  1829134XZ000012000090A

    Pattern: Section-Township-Range-Subdivision-Block-Lot
    """
    # Remove dashes and dots, rearrange
    parts = bulk_parcel.replace('.', '').split('-')
    if len(parts) >= 6:
        # Reformat: SSTTRRSUBBLOCKLOTSUFFIX
        section = parts[0].replace('A', '')  # Remove A prefix
        township = parts[1]
        range_val = parts[2]
        subdivision = parts[3]
        block = parts[4]
        lot = parts[5]
        # URL format seems to be: TTRRSSSUBBLOCKLOTSUFFIX
        return f"{township}{range_val}{section}{subdivision}{block}{lot}A"
    return bulk_parcel


async def scrape_hcpa_property(parcel_id: str = None, folio: str = None, storage: ScraperStorage = None) -> dict:
    """
    Scrape property data from HCPA GIS portal (async version).

    Args:
        parcel_id: The parcel ID in URL format (e.g., 1829134XZ000012000090A)
        folio: The folio number (e.g., 1918870000) - will search for parcel
        storage: Optional ScraperStorage instance

    Returns:
        Dictionary with property data including sales history
    """
    if not storage:
        storage = ScraperStorage()

    result = {
        "parcel_id": parcel_id,
        "folio": folio,
        "sales_history": [],
        "property_info": {},
        "legal_description": None,
        "building_info": {},
        "tax_collector_link": None,
        "tax_collector_id": None,
        "image_url": None,
        "permits": [],
    }

    playwright = await async_playwright().start()
    try:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()

        if parcel_id:
            url = f"https://gis.hcpafl.org/propertysearch/#/parcel/basic/{parcel_id}"
        elif folio:
            # Go to search page and search by folio
            url = "https://gis.hcpafl.org/propertysearch/#/search/basic"
            await page.goto(url)
            await page.wait_for_load_state("domcontentloaded")
            await asyncio.sleep(2)

            # Find and fill folio search
            folio_input = page.locator('input[placeholder*="Folio"]').first
            if folio_input:
                await folio_input.fill(folio)
                await page.keyboard.press("Enter")
                await page.wait_for_load_state("domcontentloaded")
                await asyncio.sleep(2)

            # Get current URL which should have parcel ID
            url = page.url
            result["navigated_url"] = url
        else:
            await browser.close()
            await playwright.stop()
            raise ValueError("Must provide either parcel_id or folio")

        logger.info(f"Navigating to: {url}")
        await page.goto(url)
        await page.wait_for_load_state("domcontentloaded")

        # Wait for page to fully load - needs more time for all sections
        await asyncio.sleep(5)

        # Take screenshot
        screenshot_bytes = await page.screenshot(full_page=True)

        # Use folio if available, else parcel_id as property_id
        prop_id = folio if folio else parcel_id

        screenshot_path = storage.save_screenshot(
            property_id=prop_id,
            scraper="hcpa_gis",
            image_data=screenshot_bytes
        )
        logger.info(f"Screenshot saved to: {screenshot_path}")

        # Extract property info from the Property Record Card section
        try:
            # Get owner name (TBL 3 LLC in example)
            owner_elem = page.locator("text=Mailing Address").first
            if owner_elem:
                # The owner is above Mailing Address
                card_text = await page.locator(".parcel-result, #parcel-result").first.inner_text()
                result["property_info"]["card_text"] = card_text[:2000]

            # Get specific fields using the page structure
            # Folio
            folio_elem = page.locator("text=Folio:").first
            if folio_elem:
                parent = folio_elem.locator("xpath=..").first
                folio_text = await parent.inner_text()
                match = re.search(r'Folio:\s*([\d-]+)', folio_text)
                if match:
                    result["folio"] = match.group(1)

            # Site Address
            addr_elem = page.locator("text=Site Address").first
            if addr_elem:
                parent = addr_elem.locator("xpath=..").first
                if parent:
                    addr_text = await parent.inner_text()
                    lines = addr_text.strip().split('\n')
                    if len(lines) > 1:
                        result["property_info"]["site_address"] = lines[1].strip()

        except Exception as e:
            print(f"Error extracting property info: {e}")

        # Extract Sales History table
        try:
            # Find the Sales History section
            sales_header = page.locator("text=Sales History").first
            if sales_header:
                print("Found Sales History section")

                # Get the table that follows Sales History
                # The structure is: header row with columns, then data rows
                sales_table = page.locator("table").filter(has_text="Official Record").first

                if sales_table:
                    rows = await sales_table.locator("tr").all()
                    print(f"Found {len(rows)} rows in sales table")

                    for row in rows[1:]:  # Skip header row
                        cells = await row.locator("td").all()
                        if len(cells) >= 7:
                            book_page = (await cells[0].inner_text()).strip()
                            instrument = (await cells[1].inner_text()).strip()
                            month = (await cells[2].inner_text()).strip()
                            year = (await cells[3].inner_text()).strip()
                            doc_type = (await cells[4].inner_text()).strip()
                            qualified = (await cells[5].inner_text()).strip()
                            vacant_improved = (await cells[6].inner_text()).strip()
                            sale_price = (await cells[7].inner_text()).strip() if len(cells) > 7 else ""

                            # Get the href from the Book/Page link
                            book_page_link = cells[0].locator("a").first
                            link_href = None
                            if book_page_link:
                                try:
                                    link_href = await book_page_link.get_attribute("href")
                                except Exception:
                                    pass

                            # Also get the instrument link
                            instrument_link = cells[1].locator("a").first
                            instrument_href = None
                            if instrument_link:
                                try:
                                    instrument_href = await instrument_link.get_attribute("href")
                                except Exception:
                                    pass

                            # Parse book/page
                            book_page_match = re.search(r'(\d+)\s*/\s*(\d+)', book_page)
                            if book_page_match:
                                sale_record = {
                                    "book": book_page_match.group(1),
                                    "page": book_page_match.group(2),
                                    "instrument": instrument,
                                    "date": f"{month}/{year}",
                                    "doc_type": doc_type,
                                    "qualified": qualified,
                                    "vacant_improved": vacant_improved,
                                    "sale_price": sale_price,
                                    "book_page_link": link_href,
                                    "instrument_link": instrument_href,
                                }
                                result["sales_history"].append(sale_record)
                                print(f"  Sale: Book {sale_record['book']}/{sale_record['page']} - {sale_record['date']} - {sale_record['sale_price']}")
                                if link_href:
                                    print(f"    Link: {link_href}")
        except Exception as e:
            print(f"Error extracting sales history: {e}")

        # Extract Legal Description from Legal Lines section
        try:
            legal_header = page.locator("text=Legal Lines").first
            if legal_header:
                # Find the table after Legal Lines
                legal_table = page.locator("table").filter(has_text="Legal Description").first
                if legal_table:
                    legal_cells = await legal_table.locator("td").all()
                    if len(legal_cells) >= 2:
                        result["legal_description"] = (await legal_cells[1].inner_text()).strip()
                        print(f"Legal Description: {result['legal_description']}")
        except Exception as e:
            print(f"Error extracting legal description: {e}")

        # Extract Building Characteristics
        try:
            building_header = page.locator("text=Building Characteristics").first
            if building_header:
                # Get year built, type, etc.
                year_built = page.locator("text=Year Built").first
                if year_built:
                    parent = year_built.locator("xpath=..").first
                    text = await parent.inner_text()
                    match = re.search(r'Year Built\s+(\d+)', text)
                    if match:
                        result["building_info"]["year_built"] = match.group(1)

                # Get building type
                type_elem = page.locator("text=Type:").first
                if type_elem:
                    parent = type_elem.locator("xpath=..").first
                    text = await parent.inner_text()
                    match = re.search(r'Type:\s+(.+)', text)
                    if match:
                        result["building_info"]["type"] = match.group(1).strip()
        except Exception as e:
            print(f"Error extracting building info: {e}")

        # Extract Tax Collector Link
        try:
            tax_link = page.locator("a[href*='county-taxes.com']").first
            if tax_link:
                href = await tax_link.get_attribute("href")
                result["tax_collector_link"] = href
                # Extract the tax collector ID from URL
                # Format: http://hillsborough.county-taxes.com/public/real_estate/parcels/A1992944418
                if href:
                    tax_id_match = re.search(r'/parcels/([A-Za-z0-9]+)', href)
                    if tax_id_match:
                        result["tax_collector_id"] = tax_id_match.group(1)
                    print(f"Tax Collector Link: {href}")
        except Exception as e:
            print(f"Error extracting tax collector link: {e}")

        # Extract Property Image URL
        try:
            # Look for property image in various possible locations
            img_elem = page.locator("img[src*='hcpafl.org'], img[alt*='property'], img[class*='property-image']").first
            if img_elem:
                img_src = await img_elem.get_attribute("src")
                result["image_url"] = img_src
                print(f"Property Image URL: {img_src}")
            else:
                # Try finding image container
                img_container = page.locator(".property-image, .parcel-image, [class*='photo']").first
                if img_container:
                    nested_img = img_container.locator("img").first
                    if nested_img:
                        result["image_url"] = await nested_img.get_attribute("src")
        except Exception as e:
            print(f"Error extracting image URL: {e}")

        # Extract Permits
        try:
            # Look for permit section or permit links
            permit_links = await page.locator("a[href*='permit'], a[href*='accela']").all()
            for permit_link in permit_links[:10]:  # Limit to 10 permits
                try:
                    permit_href = await permit_link.get_attribute("href")
                    permit_text = (await permit_link.inner_text()).strip()
                    result["permits"].append({
                        "permit_number": permit_text,
                        "link": permit_href,
                    })
                except Exception:
                    pass

            # Also look for permits in a table
            permit_table = page.locator("table").filter(has_text="Permit").first
            if permit_table and len(result["permits"]) == 0:
                permit_rows = await permit_table.locator("tr").all()
                for row in permit_rows[1:]:  # Skip header
                    cells = await row.locator("td").all()
                    if len(cells) >= 2:
                        permit_number = (await cells[0].inner_text()).strip()
                        permit_link_elem = cells[0].locator("a").first
                        permit_link = await permit_link_elem.get_attribute("href") if permit_link_elem else None
                        permit_type = (await cells[1].inner_text()).strip() if len(cells) > 1 else ""

                        result["permits"].append({
                            "permit_number": permit_number,
                            "permit_type": permit_type,
                            "link": permit_link,
                        })

            if result["permits"]:
                logger.info(f"Found {len(result['permits'])} permits")
        except Exception as e:
            logger.error(f"Error extracting permits: {e}")

        await browser.close()
    finally:
        await playwright.stop()

    # Save raw data
    prop_id = folio if folio else parcel_id
    raw_path = storage.save_raw_data(
        property_id=prop_id,
        scraper="hcpa_gis",
        data=result,
        context="property_details"
    )
    
    # Record scrape
    storage.record_scrape(
        property_id=prop_id,
        scraper="hcpa_gis",
        screenshot_path=screenshot_path,
        raw_data_path=raw_path,
        vision_data=result, # It's not vision data but structured data
        success=True
    )

    return result


def build_pav_url(book: str, page: str) -> str:
    """
    Build the PAV Direct Search URL for a given Book/Page.

    URL Pattern:
    https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=319&OBKey__1530_1=O&OBKey__573_1={BOOK}&OBKey__1049_1={PAGE}

    Args:
        book: Book number (e.g., "23264")
        page: Page number (e.g., "1344" or "0238")

    Returns:
        The complete PAV Direct Search URL
    """
    # Remove leading zeros from book and page
    book_clean = str(int(book))
    page_clean = str(int(page))

    return (
        f"https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html"
        f"?CQID=319&OBKey__1530_1=O&OBKey__573_1={book_clean}&OBKey__1049_1={page_clean}"
    )


def search_ori_by_book_page(book: str, page: str) -> list:
    """
    Search ORI by Book/Page number to get document details.

    Args:
        book: Book number (e.g., "23264")
        page: Page number (e.g., "1344")

    Returns:
        List of matching documents from ORI
    """
    ORI_SEARCH_URL = "https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search"
    HEADERS = {
        "Content-Type": "application/json; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Origin": "https://publicaccess.hillsclerk.com",
        "Referer": "https://publicaccess.hillsclerk.com/oripublicaccess/",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }

    session = requests.Session()
    # Initialize session
    session.get("https://publicaccess.hillsclerk.com/oripublicaccess/")

    payload = {
        "BookPageBegin": f"{book}/{page}",
        "BookPageEnd": f"{book}/{page}",
    }

    response = session.post(ORI_SEARCH_URL, headers=HEADERS, json=payload, timeout=60)
    data = response.json()
    return data.get("ResultList", [])


def download_ori_document(doc_id: str, output_path: Path) -> bool:
    """
    Download a document from ORI by its ID.

    Args:
        doc_id: The document ID (CFN/Instrument number)
        output_path: Where to save the PDF

    Returns:
        True if successful, False otherwise
    """
    WATERMARK_URL = "https://publicaccess.hillsclerk.com/Public/ORIUtilities/OverlayWatermark/api/Watermark"
    HEADERS = {
        "Accept": "application/pdf",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }

    session = requests.Session()
    session.get("https://publicaccess.hillsclerk.com/oripublicaccess/")

    encoded_id = quote(doc_id, safe="")
    url = f"{WATERMARK_URL}/{encoded_id}"

    response = session.get(url, headers=HEADERS, timeout=60)
    if response.status_code == 200 and len(response.content) > 1000:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(response.content)
        return True
    return False


async def fetch_sales_documents(hcpa_result: dict, storage: ScraperStorage) -> dict:
    """
    Fetch all documents from HCPA sales history by following the links (async version).
    """
    results = []
    prop_id = hcpa_result.get("folio") or hcpa_result.get("parcel_id")

    playwright = await async_playwright().start()
    try:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            accept_downloads=True,
        )
        page = await context.new_page()

        # Initialize session
        session = requests.Session()
        session.get("https://publicaccess.hillsclerk.com/PAVDirectSearch/")

        for sale in hcpa_result.get("sales_history", []):
            book = sale.get("book")
            page_num = sale.get("page")
            instrument = sale.get("instrument")
            link_href = sale.get("book_page_link")

            # Build URL if not provided
            if not link_href and book and page_num:
                link_href = build_pav_url(book, page_num)

            sale_result = {
                "sale": sale,
                "downloaded_file": None,
                "download_success": False,
                "legal_from_doc": None,
            }

            if link_href:
                print(f"\nFetching Book {book}/Page {page_num}...")

                try:
                    # Navigate to the PAV search page
                    await page.goto(link_href)
                    await page.wait_for_load_state("domcontentloaded")
                    await asyncio.sleep(3)

                    # Double-click the first data row to open the document viewer
                    first_row = page.locator("table tr").nth(1)
                    if first_row:
                        await first_row.dblclick()
                        await asyncio.sleep(3)

                        # Find the iframe with the document
                        iframe = page.locator("iframe").first
                        if iframe:
                            iframe_src = await iframe.get_attribute("src")
                            if iframe_src and "api/Document" in iframe_src:
                                print(f"  Found document URL")

                                # Download the PDF directly
                                response = session.get(
                                    iframe_src.replace("?OverlayMode=View", ""),
                                    headers={"User-Agent": "Mozilla/5.0"},
                                    timeout=60,
                                )

                                if response.status_code == 200 and len(response.content) > 1000:
                                    # Save using ScraperStorage
                                    doc_id = instrument or f"{book}_{page_num}"
                                    saved_path = storage.save_document(
                                        property_id=prop_id,
                                        file_data=response.content,
                                        doc_type="deed",
                                        doc_id=doc_id,
                                        extension="pdf"
                                    )

                                    sale_result["downloaded_file"] = saved_path
                                    sale_result["download_success"] = True
                                    logger.info(f"  Downloaded {len(response.content)} bytes to {saved_path}")

                                    # Also capture the legal description from the page
                                    legal_cell = page.locator("td:has-text('L ')").first
                                    if legal_cell:
                                        legal_text = (await legal_cell.inner_text()).strip()
                                        sale_result["legal_from_doc"] = legal_text
                                        print(f"  Legal: {legal_text}")

                except Exception as e:
                    print(f"  Error: {e}")
            else:
                print(f"\nNo link for Book {book}/Page {page_num}, skipping...")

            results.append(sale_result)

        await browser.close()
    finally:
        await playwright.stop()

    return {
        "parcel_id": hcpa_result.get("parcel_id"),
        "folio": hcpa_result.get("folio"),
        "legal_description": hcpa_result.get("legal_description"),
        "sales_documents": results,
    }


async def async_main():
    parser = argparse.ArgumentParser(description="Scrape HCPA GIS property data")
    parser.add_argument("--parcel", help="Parcel ID in URL format")
    parser.add_argument("--folio", help="Folio number")
    parser.add_argument("--output", help="Output JSON file", default="data/hcpa_property.json")
    parser.add_argument("--download-docs", action="store_true", help="Also download ORI documents")
    parser.add_argument("--docs-dir", help="Directory for downloaded docs", default="data/property_docs")

    args = parser.parse_args()

    if not args.parcel and not args.folio:
        parser.error("Must provide either --parcel or --folio")

    storage = ScraperStorage()

    # Run the scraper
    result = await scrape_hcpa_property(
        parcel_id=args.parcel,
        folio=args.folio,
        storage=storage
    )

    # Save result
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(result, f, indent=2)
    print(f"Saved result to {output_path}")

    # Display summary
    print(f"\nFolio: {result.get('folio')}")
    print(f"Tax Collector ID: {result.get('tax_collector_id')}")
    print(f"Image URL: {result.get('image_url')}")
    print(f"Permits found: {len(result.get('permits', []))}")

    if result.get("sales_history"):
        print("\nSales History:")
        for sale in result["sales_history"]:
            print(f"  Book {sale.get('book')}, Page {sale.get('page')} - {sale.get('date')} - {sale.get('sale_price')}")

    if result.get("permits"):
        print("\nPermits:")
        for permit in result["permits"][:5]:  # Show first 5
            print(f"  {permit.get('permit_number')} - {permit.get('permit_type', '')}")

    # Optionally download documents
    if args.download_docs:
        print("\n--- Downloading ORI Documents ---")
        docs_result = await fetch_sales_documents(result, storage)
        print(f"\nDocuments downloaded: {len(docs_result.get('sales_documents', []))}")


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
