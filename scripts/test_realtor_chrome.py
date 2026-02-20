"""
Test Realtor.com with real Chrome + user profile.
Acts like a real user: go to realtor.com, type address in search box, click result.
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
    """Extract Realtor.com property data via CDP after landing on a detail page."""
    # Try __NEXT_DATA__ first (Realtor.com is Next.js)
    result = await cdp.send("Runtime.evaluate", {
        "expression": """
            (() => {
                const nd = window.__NEXT_DATA__;
                if (!nd) return JSON.stringify({_debug: 'no __NEXT_DATA__'});
                const pp = nd.props?.pageProps;
                if (!pp) return JSON.stringify({_debug: 'no pageProps', keys: Object.keys(nd.props || {})});

                // Realtor.com stores property in various locations
                const property = pp.property || pp.initialReduxState?.propertyDetails?.propertyDetails;
                if (!property) {
                    // Try to find property data in any nested structure
                    const ppKeys = Object.keys(pp);
                    return JSON.stringify({_debug: 'no property key', pagePropsKeys: ppKeys});
                }

                try {
                    const loc = property.location || property.address || {};
                    const desc = property.description || {};
                    const p = property;
                    return JSON.stringify({
                        source: '__NEXT_DATA__',
                        property_id: p.property_id,
                        address: loc.address || p.address,
                        city: loc.city,
                        state: loc.state_code || loc.state,
                        zip: loc.postal_code || loc.zip,
                        price: p.list_price || p.price || desc.list_price,
                        estimate: p.estimate?.estimate || p.estimatedValue,
                        beds: desc.beds || p.beds,
                        baths: desc.baths || p.baths,
                        baths_full: desc.baths_full,
                        baths_half: desc.baths_half,
                        sqft: desc.sqft || p.sqft || desc.lot_sqft,
                        lot_sqft: desc.lot_sqft || p.lot_sqft,
                        year_built: desc.year_built || p.year_built,
                        garage: desc.garage,
                        stories: desc.stories,
                        type: desc.type || p.prop_type,
                        status: p.prop_status || p.status,
                        last_sold_price: p.last_sold_price,
                        last_sold_date: p.last_sold_date,
                        tax_amount: p.tax_history?.[0]?.tax || p.mortgage?.estimate?.monthly_payment,
                        hoa_fee: p.hoa?.fee,
                        photos_count: p.photos?.length || 0,
                    });
                } catch(e) {
                    return JSON.stringify({error: e.message, stack: e.stack?.substring(0, 200)});
                }
            })()
        """,
        "returnByValue": True,
    })
    val = result.get("result", {}).get("value")
    if val:
        parsed = json.loads(val)
        # If we got actual data (not debug info), return it
        if not parsed.get("_debug"):
            return parsed

    # Fallback: try window.__propertyDetails or other globals
    result2 = await cdp.send("Runtime.evaluate", {
        "expression": """
            (() => {
                // Some Realtor.com pages expose data on window
                if (window.__propertyDetails) {
                    return JSON.stringify({source: '__propertyDetails', data: window.__propertyDetails});
                }
                // Check for Apollo cache (GraphQL)
                if (window.__APOLLO_STATE__) {
                    const keys = Object.keys(window.__APOLLO_STATE__);
                    const propKeys = keys.filter(k => k.startsWith('Property:') || k.includes('property'));
                    if (propKeys.length > 0) {
                        const first = window.__APOLLO_STATE__[propKeys[0]];
                        return JSON.stringify({source: 'apollo_cache', key: propKeys[0], data: first});
                    }
                }
                return null;
            })()
        """,
        "returnByValue": True,
    })
    val2 = result2.get("result", {}).get("value")
    if val2:
        return json.loads(val2)

    # Final fallback: scrape from visible DOM
    dom_result = await cdp.send("Runtime.evaluate", {
        "expression": """
            (() => {
                const data = {source: 'dom_scrape'};

                // Price
                const priceEl = document.querySelector('[data-testid="list-price"], .list-price, .price-section .price');
                if (priceEl) data.price_text = priceEl.innerText.trim();

                // Estimate
                const estEl = document.querySelector('[data-testid="home-value-estimate"], .estimate-value');
                if (estEl) data.estimate_text = estEl.innerText.trim();

                // Address
                const addrEl = document.querySelector('[data-testid="address-line"], .detail-address h1, .address-value');
                if (addrEl) data.address = addrEl.innerText.trim();

                // Beds / Baths / SqFt from the property meta bar
                const metaItems = document.querySelectorAll('[data-testid="property-meta-beds"], [data-testid="property-meta-baths"], [data-testid="property-meta-sqft"]');
                metaItems.forEach(el => {
                    const text = el.innerText.trim().toLowerCase();
                    const num = text.match(/[\\d,.]+/)?.[0];
                    if (text.includes('bed')) data.beds = num;
                    else if (text.includes('bath')) data.baths = num;
                    else if (text.includes('sq')) data.sqft = num;
                });

                // If meta items not found, try the body text
                if (!data.beds) {
                    const body = document.body.innerText;
                    const specMatch = body.match(/(\\d+)\\s*bed.*?(\\d+(?:\\.\\d+)?)\\s*bath.*?([\\d,]+)\\s*(?:sq|SF)/i);
                    if (specMatch) {
                        data.beds = specMatch[1];
                        data.baths = specMatch[2];
                        data.sqft = specMatch[3];
                    }
                }

                // Year built, lot size from details section
                const detailItems = document.querySelectorAll('.key-fact, [data-testid*="key-fact"], .property-detail-item');
                detailItems.forEach(el => {
                    const text = el.innerText.trim().toLowerCase();
                    if (text.includes('year built')) {
                        const yr = text.match(/(\\d{4})/);
                        if (yr) data.year_built = yr[1];
                    }
                    if (text.includes('lot size') || text.includes('lot area')) {
                        const lot = text.match(/([\\d,.]+)\\s*(sq|acre)/i);
                        if (lot) data.lot_size = lot[1] + ' ' + lot[2];
                    }
                });

                // Status
                const statusEl = document.querySelector('[data-testid="listing-status"], .property-status');
                if (statusEl) data.status = statusEl.innerText.trim();

                // Photo count
                const photos = document.querySelectorAll('.gallery-image img, [data-testid="hero-image"] img, .photo-tile img');
                data.photos_count = photos.length;

                // H1 as fallback address
                if (!data.address) {
                    const h1 = document.querySelector('h1');
                    if (h1) data.h1 = h1.innerText.trim();
                }

                return JSON.stringify(data);
            })()
        """,
        "returnByValue": True,
    })
    dom_val = dom_result.get("result", {}).get("value")
    parsed_dom = json.loads(dom_val) if dom_val else {}

    # Merge debug info from first attempt if DOM also got data
    if val:
        first_parsed = json.loads(val)
        if first_parsed.get("_debug"):
            parsed_dom["_next_data_debug"] = first_parsed.get("_debug")
            if first_parsed.get("pagePropsKeys"):
                parsed_dom["_pagePropsKeys"] = first_parsed["pagePropsKeys"]

    return parsed_dom


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

        # Step 1: Go to Realtor.com homepage like a normal user
        print("Navigating to realtor.com...")
        resp = await page.goto("https://www.realtor.com", wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(3000)

        status = resp.status if resp else None
        title = await page.title()
        print(f"HTTP: {status}, Title: {title}")

        # Check if homepage loaded or blocked
        is_blocked = await page.evaluate("""
            () => {
                const text = (document.body.innerText || '').toLowerCase();
                return text.includes('press and hold') || text.includes('captcha')
                    || text.includes('access denied') || text.includes('verify you are human')
                    || text.includes('unusual traffic');
            }
        """)
        if is_blocked or (status and status >= 400):
            body = await page.evaluate("() => document.body.innerText.substring(0, 500)")
            print(f"BLOCKED on homepage: {body}")
            await page.wait_for_timeout(10000)
            await context.close()
            return

        # Step 2: Search each address like a human
        for i, address in enumerate(TEST_ADDRESSES):
            print(f"\n{'='*60}")
            print(f"Property {i+1}: {address}")
            print(f"{'='*60}")

            # Find the search box — Realtor.com uses various selectors
            search_box = await page.query_selector(
                'input#rdc-search-form-input, '
                'input[data-testid="search-bar-input"], '
                'input[placeholder*="Address"], '
                'input[placeholder*="address"], '
                'input[aria-label*="search" i], '
                'input[type="search"]'
            )
            if not search_box:
                # Broader fallback
                search_box = await page.query_selector(
                    'input[placeholder*="City" i], '
                    'input[placeholder*="ZIP" i], '
                    'input[class*="search" i]'
                )
            if not search_box:
                # Dump page state for debugging
                inputs = await page.evaluate("""
                    () => {
                        const inputs = document.querySelectorAll('input');
                        return Array.from(inputs).map(el => ({
                            id: el.id, type: el.type,
                            placeholder: el.placeholder,
                            ariaLabel: el.getAttribute('aria-label'),
                            className: el.className?.substring(0, 60),
                        }));
                    }
                """)
                print(f"No search input found. All inputs on page:")
                for inp in inputs:
                    print(f"  {inp}")
                continue

            await search_box.click()
            await page.wait_for_timeout(500)

            # Select all existing text and delete it (more natural than clearing programmatically)
            await page.keyboard.press("Control+a")
            await page.wait_for_timeout(100)
            await page.keyboard.press("Backspace")
            await page.wait_for_timeout(200)

            # Type address via CDP (human-like, triggers React onChange)
            print(f"Typing: {address}")
            await human_type(cdp, address)

            # Wait for autocomplete dropdown
            print("Waiting for autocomplete...")
            await page.wait_for_timeout(3000)

            # Click first autocomplete suggestion — Realtor uses various patterns
            suggestion = await page.query_selector(
                '[data-testid="suggestion-0"], '
                '[data-testid*="autocomplete"] li:first-child, '
                'li[data-testid*="result"]:first-child, '
                'ul[role="listbox"] li:first-child, '
                '[class*="AutoSuggest"] li:first-child, '
                '[class*="autocomplete"] li:first-child, '
                '[id*="react-autowhatever"] li:first-child, '
                'div[class*="suggestion"]:first-child a'
            )
            if suggestion:
                print("Clicking first suggestion...")
                await suggestion.click()
            else:
                # Try to find any clickable suggestion
                any_suggestion = await page.query_selector(
                    '[role="option"]:first-child, '
                    '[class*="Suggestion"]:first-child, '
                    'li[role="option"]'
                )
                if any_suggestion:
                    print("Clicking suggestion (alt selector)...")
                    await any_suggestion.click()
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
                        || text.includes('access denied') || text.includes('unusual traffic');
                }
            """)
            if blocked:
                body = await page.evaluate("() => document.body.innerText.substring(0, 500)")
                print(f"BLOCKED: {body}")
                await page.wait_for_timeout(5000)
                await page.goto("https://www.realtor.com", wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(3000)
                continue

            # Check if we landed on a property detail page
            # Realtor URLs: /realestateandhomes-detail/ADDRESS or /FL/Tampa/ADDRESS
            on_detail = (
                "/realestateandhomes-detail/" in current_url
                or ("/FL/" in current_url and "_" in current_url.split("/")[-1])
            )
            if not on_detail:
                print(f"Not on a detail page. URL: {current_url}")
                # Might be on search results — try clicking first result card
                first_result = await page.query_selector(
                    'a[href*="/realestateandhomes-detail/"], '
                    '[data-testid="property-card"] a, '
                    'div[data-testid="result-card"] a, '
                    '[class*="PropertyCard"] a[href*="/FL/"]'
                )
                if first_result:
                    print("Clicking first search result...")
                    await first_result.click()
                    await page.wait_for_timeout(6000)
                    current_url = page.url
                    on_detail = "/realestateandhomes-detail/" in current_url
                    print(f"After click URL: {current_url}")

            if on_detail:
                # Extract property data
                data = await extract_property_data(cdp)
                if data:
                    print("\nExtracted data:")
                    for k, v in data.items():
                        if v is not None and v not in ("", 0):
                            print(f"  {k}: {v}")
                else:
                    print("No data extracted from detail page")
            else:
                print("Could not reach property detail page")
                # Show what we can see
                h1 = await page.evaluate("() => document.querySelector('h1')?.innerText || 'no h1'")
                print(f"  Page h1: {h1}")

            # Delay between searches like a human
            delay = random.uniform(5.0, 10.0)  # noqa: S311
            print(f"\nWaiting {delay:.1f}s before next search...")
            await page.wait_for_timeout(int(delay * 1000))

            # Navigate back to homepage for next search
            await page.goto("https://www.realtor.com", wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

        print("\n--- Done. Browser stays open 15s ---")
        await page.wait_for_timeout(15000)
        await context.close()


if __name__ == "__main__":
    asyncio.run(main())
