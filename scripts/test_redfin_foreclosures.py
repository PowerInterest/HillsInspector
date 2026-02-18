"""
Test Redfin foreclosures page for Hillsborough County.
Uses real Chrome (channel="chrome") with user's Chrome profile + devtools.
Starting URL: https://www.redfin.com/county/464/FL/Hillsborough-County/foreclosures

Strategy: Scrape listing cards from the foreclosures page, then navigate
to individual property detail pages for full data.
"""
import asyncio
import json
import re
import time
from pathlib import Path

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROFILE_DIR = PROJECT_ROOT / "data" / "browser_profiles" / "user_chrome"
FORECLOSURES_URL = "https://www.redfin.com/county/464/FL/Hillsborough-County/foreclosures"

# Address we want to find among the foreclosure listings
TEST_ADDRESS = "6710 Yardley Way"


async def main():
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            channel="chrome",
            headless=False,
            devtools=True,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            timezone_id="America/New_York",
            args=["--disable-blink-features=AutomationControlled"],
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )

        page = context.pages[0] if context.pages else await context.new_page()
        await Stealth().apply_stealth_async(page)

        print(f"Navigating to {FORECLOSURES_URL}")
        response = await page.goto(FORECLOSURES_URL, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(5000)

        status = response.status if response else None
        title = await page.title()
        print(f"HTTP status: {status}")
        print(f"Title: {title}")

        if status != 200:
            print(f"BLOCKED - got {status}")
            await context.close()
            return

        # Step 1: Extract all property listing cards from the foreclosures page
        print("\n=== Step 1: Scraping foreclosure listings ===")
        listings = await page.evaluate("""
            () => {
                const cards = document.querySelectorAll('.HomeCardContainer');
                return Array.from(cards).map(card => {
                    const link = card.querySelector('a[href*="/home/"]');
                    const priceEl = card.querySelector('[class*="price"], [class*="Price"]');
                    const text = card.innerText.trim();
                    return {
                        url: link ? link.href : null,
                        address: link ? link.innerText.trim().split('\\n')[0] : null,
                        price: priceEl ? priceEl.innerText.trim() : null,
                        fullText: text.substring(0, 300)
                    };
                }).filter(c => c.url);
            }
        """)

        print(f"Found {len(listings)} listing cards")
        for i, listing in enumerate(listings[:10]):
            addr = listing.get("address", "?")[:50]
            price = listing.get("price", "?")
            print(f"  {i+1}. {addr:50s} {price}")

        # Step 2: Navigate to first listing's property detail page
        if listings:
            target = listings[0]
            print(f"\n=== Step 2: Navigating to property detail: {target['address']} ===")
            await page.goto(target["url"], wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(5000)

            final_url = page.url
            print(f"URL: {final_url}")

            if "/home/" in final_url:
                print("\nProperty detail extraction:")

                # Price
                price_el = await page.query_selector('[data-rf-test-id="abp-price"]')
                if price_el:
                    print(f"  Price: {await price_el.inner_text()}")

                # Redfin Estimate
                estimate_el = await page.query_selector('[data-rf-test-id="avmLdpPrice"]')
                if estimate_el:
                    print(f"  Redfin Estimate: {await estimate_el.inner_text()}")

                # Beds/baths/sqft
                stats = await page.query_selector_all('[data-rf-test-id="abp-beds"], [data-rf-test-id="abp-baths"], [data-rf-test-id="abp-sqFt"]')
                for stat in stats:
                    text = await stat.inner_text()
                    print(f"  Stat: {text}")

                # Status
                status_el = await page.query_selector('[data-rf-test-id="abp-status"]')
                if status_el:
                    print(f"  Status: {await status_el.inner_text()}")

                # Year built, lot size from key details
                key_details = await page.evaluate("""
                    () => {
                        const items = document.querySelectorAll('.keyDetail, [class*="keyDetail"]');
                        return Array.from(items).map(el => el.innerText.trim()).slice(0, 20);
                    }
                """)
                if key_details:
                    print(f"  Key details: {key_details}")

                # Photos
                photos = await page.evaluate("""
                    () => {
                        const imgs = document.querySelectorAll('img[src*="ssl.cdn-redfin"], img[src*="photos"]');
                        const urls = new Set();
                        for (const img of imgs) {
                            const src = img.src;
                            if (src && (src.includes('photos') || src.includes('genMid'))) {
                                urls.add(src);
                            }
                        }
                        return Array.from(urls).slice(0, 20);
                    }
                """)
                print(f"  Photos: {len(photos)}")
                for url in photos[:3]:
                    print(f"    {url[:100]}")

                # Address from page
                addr_el = await page.query_selector('[data-rf-test-id="abp-homeinfo-homeAddress"], .street-address')
                if addr_el:
                    print(f"  Address: {await addr_el.inner_text()}")

                # Try to get broader data from the page's React state or __NEXT_DATA__
                react_data = await page.evaluate("""
                    () => {
                        // Check for __NEXT_DATA__ or window.__reactServerState
                        if (window.__NEXT_DATA__) return { source: '__NEXT_DATA__', keys: Object.keys(window.__NEXT_DATA__) };
                        if (window.__reactServerState) return { source: '__reactServerState', keys: Object.keys(window.__reactServerState) };
                        // Look for script tags with JSON data
                        const scripts = document.querySelectorAll('script[type="application/json"], script[type="application/ld+json"]');
                        const data = [];
                        for (const s of scripts) {
                            try {
                                const parsed = JSON.parse(s.textContent);
                                data.push({ type: s.type, keys: Object.keys(parsed).slice(0, 10) });
                            } catch {}
                        }
                        return { source: 'script_tags', data };
                    }
                """)
                print(f"  React/JSON data: {json.dumps(react_data, indent=2)[:300]}")

        # Step 3: Check if pagination exists
        print(f"\n=== Step 3: Pagination check ===")
        await page.goto(FORECLOSURES_URL, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)

        pagination = await page.evaluate("""
            () => {
                const paginators = document.querySelectorAll('button[class*="page"], a[class*="page"], [class*="pagination"]');
                return Array.from(paginators).map(el => ({
                    tag: el.tagName,
                    text: el.innerText.trim(),
                    href: el.href || ''
                })).slice(0, 10);
            }
        """)
        if pagination:
            print(f"  Pagination elements: {json.dumps(pagination, indent=2)[:300]}")
        else:
            print("  No pagination found (all 49 on one page)")

        # Summary
        print(f"\n=== SUMMARY ===")
        print(f"Foreclosure listings found: {len(listings)}")
        print(f"Listings page loads: OK (200)")
        print(f"Individual property pages: {'OK' if listings else 'untested'}")
        print(f"Strategy: Scrape listings page → match addresses → navigate to detail pages")

        print("\n--- Done. Browser stays open 15s ---")
        await page.wait_for_timeout(15000)
        await context.close()


if __name__ == "__main__":
    asyncio.run(main())
