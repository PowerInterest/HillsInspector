import asyncio
from datetime import date
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

async def inspect_page():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        
        # Use a date likely to have auctions
        target_date = date(2026, 1, 8) 
        date_str = target_date.strftime("%m/%d/%Y")
        url = f"https://hillsborough.realtaxdeed.com/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE={date_str}"
        
        print(f"Navigating to {url}")
        await page.goto(url)
        await page.wait_for_load_state("networkidle")
        
        # Check if we have content
        content = await page.content()
        if "No auctions found" in content:
            print("No auctions found for this date.")
        else:
            print("Auctions found. Inspecting structure...")
            
            # Find all "Case #:" labels
            case_labels = await page.locator("text=Case #:").all()
            print(f"Found {len(case_labels)} 'Case #:' labels")
            
            for i, label in enumerate(case_labels):
                print(f"--- Item {i} ---")
                # Robust Case Number Extraction: Try link first, then plain text
                case_row = label.locator("xpath=./ancestor::tr[1]")
                case_link = case_row.locator("a")
                
                if await case_link.count() > 0:
                    case_number = await case_link.nth(0).inner_text(timeout=5000)
                    print(f"Found Case # via Link: {case_number}")
                else:
                    # Fallback to second column text (usually class="AD_DTA")
                    case_number = await case_row.locator("td").nth(1).inner_text(timeout=5000)
                    print(f"Found Case # via Text Fallback: {case_number}")
                    
                # Verify Parcel ID fallback too
                try:
                    # Helper sim
                    async def get_text_by_label(lbl, _container=label.locator("xpath=./ancestor::table[1]")):
                        row = _container.locator(f"tr:has-text('{lbl}')")
                        if await row.count() > 0:
                            return await row.locator("td").nth(1).inner_text()
                        return ""
                        
                    parcel_id = await get_text_by_label("Parcel ID:")
                    print(f"Parcel ID raw: {parcel_id}")
                except Exception as e:
                    print(f"Parcel ID check failed: {e}")
                    
        await browser.close()

if __name__ == "__main__":
    asyncio.run(inspect_page())
