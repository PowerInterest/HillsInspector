import asyncio
import duckdb
from datetime import date
from playwright.async_api import async_playwright

# Case to test
CASE_NUMBER = "292025CC022863A001HC"
AUCTION_DATE = "12/30/2025"
URL = f"https://hillsborough.realforeclose.com/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE={AUCTION_DATE}"

async def run_test():
    print(f"Testing recovery for {CASE_NUMBER} on {AUCTION_DATE}")
    
    # 1. Scrape the address
    print("Step 1: Scraping address from realforeclose.com...")
    target_address = None
    from playwright_stealth import Stealth
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # Use desktop user agent to avoid mobile layout issues
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            locale='en-US'
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        
        await page.goto(URL)
        
        # Wait for content
        try:
            await page.wait_for_selector("div.AUCTION_ITEM", timeout=20000)
        except Exception:
            print("Timeout waiting for AUCTION_ITEM")
            # debug screenshot
            await page.screenshot(path="debug_scrape.png")
            
        # Simple extraction logic for test
        items = page.locator("div.AUCTION_ITEM")
        count = await items.count()
        print(f"Found {count} items")
        
        for i in range(count):
            item = items.nth(i)
            text = await item.inner_text()
            if CASE_NUMBER in text:
                print("Found target case!")
                
                # Extract address
                details = item.locator("table.ad_tab")
                addr_row = details.locator("tr:has-text('Property Address:')")
                if await addr_row.count():
                    addr_cell = addr_row.locator("td").nth(1)
                    target_row_text = (await addr_cell.inner_text()).strip()
                    
                    # Sometimes address is split across two rows
                    # Check next row for City/State/Zip
                    city_row = addr_row.locator("xpath=./following-sibling::tr[1]")
                    if await city_row.count():
                         target_row_text += ", " + (await city_row.locator("td").nth(1).inner_text()).strip()
                         
                    target_address = target_row_text
                    print(f"Scraped Address: '{target_address}'")
                break
        await browser.close()
    
    if not target_address:
        print("Failed to find case/address.")
        return

    # 2. Try Address Lookup in Bulk Data
    print("\nStep 2: Looking up address in bulk_parcels parquet...")
    
    # Connect directly to parquet to avoid DB lock
    con = duckdb.connect()
    con.execute("INSTALL parquet; LOAD parquet;")
    
    # Normalize address for matching (simple version)
    # Remove ZIP, remove "FL", remove generic city if present
    # Real logic: split by comma, take first part
    search_term = target_address.split(",")[0].strip()
    print(f"Search Term: '{search_term}'")
    
    query = """
        SELECT folio, owner_name, property_address, city, zip_code
        FROM 'data/parquet/bulk_parcels_latest.parquet'
        WHERE property_address = ?
        LIMIT 1
    """
    
    result = con.execute(query, [search_term]).fetchone()
    
    if result:
        print("\n✅ SUCCESS! Found match in Bulk Data:")
        print(f"Folio: {result[0]}")
        print(f"Owner: {result[1]}")
        print(f"Address: {result[2]}, {result[3]} {result[4]}")
        
        # 3. Verify with HCPA
        print("\nStep 3: Simulating HCPA Verification...")
        # (We skip actual HCPA scrape in this test to be fast, but we have the folio now)
        print(f"We can now successfully scrape HCPA using folio: {result[0]}")
        print(f"And we can download the Final Judgment because we have a valid folio folder: data/properties/{result[0]}/")
    else:
        print("\n❌ FAILED. No exact match found in bulk data.")
        # Debug: fuzzy search?
        print("Trying fuzzy search...")
        fuzzy_query = """
            SELECT folio, property_address, jaro_winkler_similarity(property_address, ?) as score
            FROM 'data/parquet/bulk_parcels_latest.parquet'
            WHERE score > 0.95
            ORDER BY score DESC
            LIMIT 3
        """
        fuzzy = con.execute(fuzzy_query, [search_term]).fetchall()
        for f in fuzzy:
             print(f"Potential Match: {f[0]} - {f[1]} (Score: {f[2]})")

if __name__ == "__main__":
    asyncio.run(run_test())
