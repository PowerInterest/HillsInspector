import asyncio
import json
from contextlib import suppress
from typing import Dict, Any
from playwright.async_api import async_playwright, Page

class APIDiscoveryCrawler:
    def __init__(self):
        self.captured_endpoints: Dict[str, Any] = {}

    async def _on_request(self, request):
        if request.resource_type in ("xhr", "fetch"):
            key = f"{request.method} {request.url}"
            if key not in self.captured_endpoints:
                self.captured_endpoints[key] = {
                    "method": request.method,
                    "url": request.url,
                    "payload": request.post_data,
                    "response_sample": None,
                }

    async def _on_response(self, response):
        key = f"{response.request.method} {response.request.url}"
        if key in self.captured_endpoints:
            try:
                text = await response.text()
                self.captured_endpoints[key]["response_sample"] = text[:500]
            except Exception as err:
                print(f"Response capture failed: {err}")

    async def crawl_hcpa(self, page: Page):
        print("  -> Crawling HCPA...")
        await page.goto("https://gis.hcpafl.org/propertysearch", wait_until="networkidle")
        # Basic search input - try common selectors
        try:
            await page.fill("input[placeholder='Search...']", "123")
        except Exception:
            # fallback to first text input
            await page.fill("input[type='text']", "123")
        await asyncio.sleep(2)
        await page.keyboard.press("Enter")
        await page.wait_for_load_state("networkidle")
        await asyncio.sleep(2)
        # Attempt to click first result if list appears
        try:
            # Many sites use a table row or list item; we try a generic selector
            await page.click("css=tr[data-index='0']", timeout=3000)
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(2)
        except Exception as err:
            print(f"HCPA first-result click failed: {err}")
        # Visit a few tabs if present
        for tab_selector in ["text=Details", "text=Sales", "text=Values", "text=Map"]:
            try:
                await page.click(tab_selector, timeout=2000)
                await page.wait_for_load_state("networkidle")
                await asyncio.sleep(1)
            except Exception as err:
                print(f"HCPA tab click failed: {err}")

    async def crawl_clerk(self, page: Page):
        print("  -> Crawling Clerk...")
        await page.goto("https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch", wait_until="networkidle")
        # Attempt to fill a generic search field
        with suppress(Exception):
            await page.fill("input[type='text']", "Smith")
        # Click search button - common text
        with suppress(Exception):
            await page.click("button:has-text('Search')", timeout=3000)
        if not self.captured_endpoints:
            with suppress(Exception):
                await page.click("button", timeout=3000)
        await page.wait_for_load_state("networkidle")
        await asyncio.sleep(2)
        # Click first result if list appears
        try:
            await page.click("css=tr[data-index='0']", timeout=3000)
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(2)
        except Exception as err:
            print(f"Clerk first-result click failed: {err}")

    async def run(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            page = await context.new_page()
            page.on("request", self._on_request)
            page.on("response", self._on_response)
            try:
                await self.crawl_hcpa(page)
            except Exception as e:
                print(f"Error crawling HCPA: {e}")
            try:
                await self.crawl_clerk(page)
            except Exception as e:
                print(f"Error crawling Clerk: {e}")
            await browser.close()
        # Write results to project root
        output_path = "api_endpoints.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(self.captured_endpoints, f, indent=2)
        print(f"Discovery complete. Captured {len(self.captured_endpoints)} endpoints. Saved to {output_path}")

if __name__ == "__main__":
    crawler = APIDiscoveryCrawler()
    asyncio.run(crawler.run())
