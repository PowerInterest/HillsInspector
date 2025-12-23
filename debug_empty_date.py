import asyncio
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

async def check():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        
        print("Going to home page...")
        await page.goto("https://hillsborough.realforeclose.com/", wait_until="networkidle")
        await asyncio.sleep(2)
        
        date_str = "04/18/2025"
        url = f"https://hillsborough.realforeclose.com/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE={date_str}"
        print(f"Checking {url}")
        
        await page.goto(url, wait_until="networkidle")
        await asyncio.sleep(2)
        
        content = await page.content()
        print(f"Content length: {len(content)}")
        
        # Look for typical "No auctions" text
        if "no auctions found" in content.lower():
            print("Found 'No auctions found' (case insensitive)")
        elif "there are no auctions" in content.lower():
            print("Found 'There are no auctions'")
        else:
            print("Could not find common empty text.")
            # Search for anything that looks like status
            if "Forbidden" in content:
                print("STILL Forbidden?")
            
        with open("debug_history_new.html", "w") as f:
            f.write(content)
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(check())
