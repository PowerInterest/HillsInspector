import asyncio
from playwright.async_api import async_playwright

async def deep_scan_court_cqid():
    # We are striking out on finding the "Court Case" search.
    # It is possible it is NOT in the 300-350 range, or it requires login.
    
    # However, we know HOVER uses OnBase.
    # Let's try to look at the HOVER main page source code to see if we can find any clues.
    
    url = "https://hover.hillsclerk.com/html/case/caseSearch.html"
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        print(f"Navigating to {url}...")
        try:
            await page.goto(url, timeout=15000)
            await page.wait_for_load_state("networkidle")
            
            # This page is likely blocked by PerimeterX, but let's see what we get.
            title = await page.title()
            print(f"Title: {title}")
            
            content = await page.content()
            if "PerimeterX" in content or "Access Denied" in title:
                print("Blocked by PerimeterX.")
            else:
                print("Access successful (unexpected).")
                
            # Even if blocked, maybe we can see some JS or config in the source?
            # We'll look for "CQID" or "obpa" in the content.
            if "CQID" in content:
                print("Found 'CQID' in content!")
                # Extract context
                idx = content.find("CQID")
                print(content[idx:idx+100])
                
        except Exception as e:
            print(f"Error: {e}")
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(deep_scan_court_cqid())
