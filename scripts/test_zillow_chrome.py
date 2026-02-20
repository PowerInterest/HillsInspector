"""
Test Zillow with real Chrome + user profile.
Acts like a real user: go to zillow.com, type address in search box, click result.
Uses CDP for typing (React inputs don't respond to Playwright fill()).
No screenshots.
"""
import asyncio
import json
import random
from pathlib import Path

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROFILE_DIR = PROJECT_ROOT / "data" / "browser_profiles" / "user_chrome"

TEST_ADDRESSES = [
    "1414 Maluhia Dr, Tampa, FL 33612",
    "11642 Addison Chase Dr, Riverview, FL 33579",
    "7913 Longwood Run Ln, Tampa, FL 33615",
]


async def _cdp_key(cdp, key: str, text: str = ""):
    """Send a single keyDown + keyUp via CDP."""
    await cdp.send("Input.dispatchKeyEvent", {
        "type": "keyDown", "key": key, "text": text,
    })
    await cdp.send("Input.dispatchKeyEvent", {
        "type": "keyUp", "key": key,
    })


async def human_type(cdp, text: str):
    """Type text via CDP with human-like timing, pauses, and occasional typos."""
    TYPO_RATE = 0.04  # ~4% chance of typo per char
    PAUSE_RATE = 0.08  # ~8% chance of a thinking pause
    ADJACENT = {
        "a": "sq", "s": "ad", "d": "sf", "f": "dg", "g": "fh",
        "h": "gj", "j": "hk", "k": "jl", "l": "k;",
        "q": "wa", "w": "qe", "e": "wr", "r": "et", "t": "ry",
        "y": "tu", "u": "yi", "i": "uo", "o": "ip", "p": "o",
        "1": "2", "2": "13", "3": "24", "4": "35", "5": "46",
        "6": "57", "7": "68", "8": "79", "9": "80", "0": "9",
    }

    for char in text:
        # Occasional thinking pause (longer after commas/spaces)
        if random.random() < PAUSE_RATE:  # noqa: S311
            await asyncio.sleep(random.uniform(0.3, 0.9))  # noqa: S311

        # Occasional typo → backspace → correct char
        if random.random() < TYPO_RATE and char.lower() in ADJACENT:  # noqa: S311
            wrong = random.choice(ADJACENT[char.lower()])  # noqa: S311
            await _cdp_key(cdp, wrong, wrong)
            await asyncio.sleep(random.uniform(0.08, 0.2))  # noqa: S311
            await _cdp_key(cdp, "Backspace")
            await asyncio.sleep(random.uniform(0.1, 0.3))  # noqa: S311

        await _cdp_key(cdp, char, char)

        # Variable inter-key delay: faster mid-word, slower after spaces/punctuation
        if char in (" ", ",", "."):
            await asyncio.sleep(random.uniform(0.15, 0.4))  # noqa: S311
        else:
            await asyncio.sleep(random.uniform(0.04, 0.18))  # noqa: S311


async def extract_property_data(cdp):
    """Extract Zillow property data via CDP after landing on a detail page."""
    # Try __NEXT_DATA__ first (Zillow SSR data)
    result = await cdp.send("Runtime.evaluate", {
        "expression": """
            (() => {
                const nd = window.__NEXT_DATA__;
                if (!nd) return null;
                const pp = nd.props?.pageProps;
                if (!pp) return null;

                const cp = pp.componentProps;
                if (!cp) return null;

                const gdp = cp.gdpClientCache;
                if (!gdp) return null;

                try {
                    const parsed = typeof gdp === 'string' ? JSON.parse(gdp) : gdp;
                    const keys = Object.keys(parsed);
                    for (const key of keys) {
                        const val = typeof parsed[key] === 'string' ? JSON.parse(parsed[key]) : parsed[key];
                        if (val?.property) {
                            const p = val.property;
                            return JSON.stringify({
                                source: 'gdpClientCache',
                                zpid: p.zpid,
                                address: p.address,
                                price: p.price,
                                zestimate: p.zestimate,
                                rentZestimate: p.rentZestimate,
                                bedrooms: p.bedrooms,
                                bathrooms: p.bathrooms,
                                livingArea: p.livingArea,
                                yearBuilt: p.yearBuilt,
                                lotSize: p.lotSize || p.lotAreaValue,
                                homeStatus: p.homeStatus,
                                homeType: p.homeType,
                                taxAssessedValue: p.taxAssessedValue,
                                photos_count: p.photos?.length || p.responsivePhotos?.length || 0,
                            });
                        }
                    }
                } catch(e) {
                    return JSON.stringify({error: e.message});
                }
                return null;
            })()
        """,
        "returnByValue": True,
    })
    val = result.get("result", {}).get("value")
    if val:
        return json.loads(val)

    # Fallback: scrape from visible DOM text
    dom_result = await cdp.send("Runtime.evaluate", {
        "expression": """
            (() => {
                const body = document.body.innerText;
                const data = {};

                const zestMatch = body.match(/Zestimate[^$]*\\$(\\d[\\d,]+)/i);
                if (zestMatch) data.zestimate_text = '$' + zestMatch[1];

                const rentMatch = body.match(/Rent Zestimate[^$]*\\$(\\d[\\d,]+)/i);
                if (rentMatch) data.rent_zestimate_text = '$' + rentMatch[1];

                const h1 = document.querySelector('h1');
                if (h1) data.h1 = h1.innerText.trim();

                const specMatch = body.match(/(\\d+)\\s*bd.*?(\\d+(?:\\.\\d+)?)\\s*ba.*?([\\d,]+)\\s*sqft/i);
                if (specMatch) {
                    data.beds = specMatch[1];
                    data.baths = specMatch[2];
                    data.sqft = specMatch[3];
                }

                return JSON.stringify(data);
            })()
        """,
        "returnByValue": True,
    })
    dom_val = dom_result.get("result", {}).get("value")
    return json.loads(dom_val) if dom_val else {}


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
        cdp = await context.new_cdp_session(page)

        # Step 1: Go to Zillow homepage like a normal user
        print("Navigating to zillow.com...")
        resp = await page.goto("https://www.zillow.com", wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(3000)

        status = resp.status if resp else None
        title = await page.title()
        print(f"HTTP: {status}, Title: {title}")

        # Check if homepage loaded or blocked
        is_blocked = await page.evaluate("""
            () => {
                const text = (document.body.innerText || '').toLowerCase();
                return text.includes('press and hold') || text.includes('captcha')
                    || text.includes('access denied') || text.includes('verify you are human');
            }
        """)
        if is_blocked or (status and status >= 400):
            body = await page.evaluate("() => document.body.innerText.substring(0, 300)")
            print(f"BLOCKED on homepage: {body}")
            await page.wait_for_timeout(10000)
            await context.close()
            return

        # Step 2: Search each address like a human
        for i, address in enumerate(TEST_ADDRESSES):
            print(f"\n{'='*60}")
            print(f"Property {i+1}: {address}")
            print(f"{'='*60}")

            # Click the search box
            search_box = await page.query_selector(
                'input#search-box-input, '
                'input[placeholder*="Enter an address"], '
                'input[type="search"], '
                'input[aria-label*="Search"]'
            )
            if not search_box:
                print("Could not find search box!")
                # Try clicking any visible input
                search_box = await page.query_selector('input[placeholder*="address" i]')
            if not search_box:
                print("No search input found at all. Page state:")
                h1 = await page.evaluate("() => document.querySelector('h1')?.innerText || 'no h1'")
                print(f"  h1: {h1}")
                continue

            await search_box.click()
            await page.wait_for_timeout(500)

            # Clear existing text
            await search_box.evaluate("el => el.value = ''")
            await page.wait_for_timeout(200)

            # Type address via CDP (human-like, triggers React onChange)
            print(f"Typing: {address}")
            await human_type(cdp, address)

            # Wait for autocomplete dropdown
            print("Waiting for autocomplete...")
            await page.wait_for_timeout(3000)

            # Click first autocomplete suggestion
            suggestion = await page.query_selector(
                '[id*="option-0"], '
                '[data-testid*="suggestion"], '
                'li[role="option"]:first-child, '
                'ul[role="listbox"] li:first-child, '
                '[class*="AutocompleteResult"]:first-child a'
            )
            if suggestion:
                print("Clicking first suggestion...")
                await suggestion.click()
            else:
                print("No autocomplete suggestion found, pressing Enter...")
                await page.keyboard.press("Enter")

            # Wait for navigation to property page
            await page.wait_for_timeout(8000)

            current_url = page.url
            current_title = await page.title()
            print(f"URL: {current_url}")
            print(f"Title: {current_title}")

            # Check if we're blocked
            blocked = await page.evaluate("""
                () => {
                    const text = (document.body.innerText || '').toLowerCase();
                    return text.includes('press and hold') || text.includes('captcha')
                        || text.includes('access denied');
                }
            """)
            if blocked:
                body = await page.evaluate("() => document.body.innerText.substring(0, 300)")
                print(f"BLOCKED: {body}")
                # Wait and try to recover for next address
                await page.wait_for_timeout(5000)
                await page.goto("https://www.zillow.com", wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(3000)
                continue

            # Check if we landed on a property detail page
            on_detail = "/homedetails/" in current_url or "_zpid" in current_url
            if not on_detail:
                print(f"Not on a detail page. URL: {current_url}")
                # Might be on search results — try clicking first result
                first_result = await page.query_selector(
                    'a[href*="/homedetails/"], '
                    '[data-test="property-card"] a, '
                    'article a[href*="_zpid"]'
                )
                if first_result:
                    print("Clicking first search result...")
                    await first_result.click()
                    await page.wait_for_timeout(6000)
                    current_url = page.url
                    on_detail = "/homedetails/" in current_url or "_zpid" in current_url
                    print(f"After click URL: {current_url}")

            if on_detail:
                # Extract property data
                data = await extract_property_data(cdp)
                if data:
                    print("\nExtracted data:")
                    for k, v in data.items():
                        print(f"  {k}: {v}")
                else:
                    print("No data extracted from detail page")
            else:
                print("Could not reach property detail page")

            # Delay between searches like a human
            delay = random.uniform(5.0, 10.0)  # noqa: S311
            print(f"\nWaiting {delay:.1f}s before next search...")
            await page.wait_for_timeout(int(delay * 1000))

            # Navigate back to homepage for next search
            await page.goto("https://www.zillow.com", wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

        print("\n--- Done. Browser stays open 15s ---")
        await page.wait_for_timeout(15000)
        await context.close()


if __name__ == "__main__":
    asyncio.run(main())
