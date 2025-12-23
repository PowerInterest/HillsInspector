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
        
        await page.goto("https://hillsborough.realforeclose.com/", wait_until="networkidle")
        
        date_str = "06/01/2023"
        url = f"https://hillsborough.realforeclose.com/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE={date_str}"
        print(f"Checking {url}")
        
        await page.goto(url, wait_until="networkidle")
        try:
            await page.wait_for_selector(".AUCTION_ITEM", timeout=15000)
            print("Found .AUCTION_ITEM")
        except:
            print("Timeout waiting for .AUCTION_ITEM")
            
        content = await page.content()
        with open("debug_history_success.html", "w") as f:
            f.write(content)
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(check())
