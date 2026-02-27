import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        # Monitor requests
        async def log_request(route, request):
            if (request.method == "POST" and "api/Search" in request.url) or "KeywordSearch" in request.url:
                print("FOUND API REQUEST!")
                print("URL:", request.url)
                print("Headers:", request.headers)
                print("Post Data:", request.post_data)
            await route.continue_()

        await page.route("**/*", log_request)

        # Go to ORI Utilities search page
        await page.goto("https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch")
        await page.wait_for_load_state("networkidle")

        # Fill out instrument form (we need to find the inputs)
        # Let's dump the HTML body to understand the form inputs first if needed
        # But wait, looking at standard ORI, there's usually a tab for Instrument
        
        # We will take a screenshot to see the page
        await page.screenshot(path="ori_search.png")
        
        await browser.close()
        
asyncio.run(main())
