"""
Realtor.com scraper using CDP on a real Chrome page.

Uses CDP for typing (React inputs don't respond to Playwright fill()).
Extracts data from __NEXT_DATA__ SSR payload with DOM fallback.
"""

import asyncio
import json
import random
from dataclasses import dataclass, field

from loguru import logger


@dataclass
class RealtorListing:
    property_id: str = ""
    address: str = ""
    price: float | None = None
    estimate: float | None = None
    rent_estimate: float | None = None
    beds: int | None = None
    baths: float | None = None
    sqft: int | None = None
    year_built: int | None = None
    lot_size: str | None = None
    property_type: str | None = None
    status: str | None = None
    photos: list[str] = field(default_factory=list)
    detail_url: str = ""


# ---------------------------------------------------------------------------
# CDP key helpers
# ---------------------------------------------------------------------------


async def _cdp_key(cdp, key: str, text: str = ""):
    await cdp.send(
        "Input.dispatchKeyEvent",
        {
            "type": "keyDown",
            "key": key,
            "text": text,
        },
    )
    await cdp.send(
        "Input.dispatchKeyEvent",
        {
            "type": "keyUp",
            "key": key,
        },
    )


async def human_type(cdp, text: str):
    TYPO_RATE = 0.04
    PAUSE_RATE = 0.08
    ADJACENT = {
        "a": "sq",
        "s": "ad",
        "d": "sf",
        "f": "dg",
        "g": "fh",
        "h": "gj",
        "j": "hk",
        "k": "jl",
        "l": "k;",
        "q": "wa",
        "w": "qe",
        "e": "wr",
        "r": "et",
        "t": "ry",
        "y": "tu",
        "u": "yi",
        "i": "uo",
        "o": "ip",
        "p": "o",
        "1": "2",
        "2": "13",
        "3": "24",
        "4": "35",
        "5": "46",
        "6": "57",
        "7": "68",
        "8": "79",
        "9": "80",
        "0": "9",
    }

    for char in text:
        if random.random() < PAUSE_RATE:  # noqa: S311
            await asyncio.sleep(random.uniform(0.3, 0.9))  # noqa: S311

        if random.random() < TYPO_RATE and char.lower() in ADJACENT:  # noqa: S311
            wrong = random.choice(ADJACENT[char.lower()])  # noqa: S311
            await _cdp_key(cdp, wrong, wrong)
            await asyncio.sleep(random.uniform(0.08, 0.2))  # noqa: S311
            await _cdp_key(cdp, "Backspace")
            await asyncio.sleep(random.uniform(0.1, 0.3))  # noqa: S311

        await _cdp_key(cdp, char, char)

        if char in (" ", ",", "."):
            await asyncio.sleep(random.uniform(0.15, 0.4))  # noqa: S311
        else:
            await asyncio.sleep(random.uniform(0.04, 0.18))  # noqa: S311


# ---------------------------------------------------------------------------
# Block detection
# ---------------------------------------------------------------------------


async def _is_blocked(page) -> bool:
    """Check if Realtor is showing a CAPTCHA or access denied page."""
    return await page.evaluate("""
        () => {
            const text = (document.body.innerText || '').toLowerCase();
            return text.includes('automated access') || text.includes('verify you are human') || text.includes('px-captcha');
        }
    """)


# ---------------------------------------------------------------------------
# Data extraction
# ---------------------------------------------------------------------------


async def _extract_from_next_data(cdp) -> RealtorListing | None:
    """Extract property data from Realtor's __NEXT_DATA__ SSR payload."""
    result = await cdp.send(
        "Runtime.evaluate",
        {
            "expression": """
            (() => {
                const nd = window.__NEXT_DATA__;
                if (!nd) return null;
                const props = nd.props?.pageProps?.initialReduxState?.propertyDetails;
                if (!props) return null;

                try {
                    const p = props.data || props; // May vary based on state shape
                    if (!p || !p.location) return null;

                    const addr = p.location.address || {};
                    const fullAddr = [
                        addr.line,
                        addr.city,
                        addr.state_code,
                        addr.postal_code
                    ].filter(Boolean).join(', ');

                    const photos = (p.photos || []).map(photo => photo.href).filter(Boolean).slice(0, 30);

                    return JSON.stringify({
                        property_id: String(p.property_id || ''),
                        address: fullAddr,
                        price: p.list_price,
                        estimate: p.estimates?.current_values?.[0]?.estimate,
                        beds: p.description?.beds,
                        baths: p.description?.baths,
                        livingArea: p.description?.sqft,
                        yearBuilt: p.description?.year_built,
                        lotSize: p.description?.lot_sqft,
                        homeStatus: p.status,
                        homeType: p.description?.type,
                        photos: photos,
                        detailUrl: window.location.href,
                    });
                } catch(e) {
                    return JSON.stringify({error: e.message});
                }
                return null;
            })()
        """,
            "returnByValue": True,
        },
    )

    val = result.get("result", {}).get("value")
    if not val:
        return None

    data = json.loads(val)
    if "error" in data:
        logger.warning(f"Realtor __NEXT_DATA__ parse error: {data['error']}")
        return None

    return RealtorListing(
        property_id=data.get("property_id", ""),
        address=data.get("address", ""),
        price=_safe_num(data.get("price")),
        estimate=_safe_num(data.get("estimate")),
        beds=_safe_int(data.get("beds")),
        baths=_safe_num(data.get("baths")),
        sqft=_safe_int(data.get("livingArea")),
        year_built=_safe_int(data.get("yearBuilt")),
        lot_size=str(data["lotSize"]) if data.get("lotSize") else None,
        status=data.get("homeStatus"),
        property_type=data.get("homeType"),
        photos=data.get("photos", []),
        detail_url=data.get("detailUrl", ""),
    )


async def _extract_from_dom(cdp) -> RealtorListing | None:
    """Fallback: extract basic data from visible DOM text."""
    dom_result = await cdp.send(
        "Runtime.evaluate",
        {
            "expression": """
            (() => {
                const body = document.body.innerText;
                const data = {};

                const zestMatch = body.match(/RealEstimate.*?\\$(\\d[\\d,]+)/i);
                if (zestMatch) data.estimate = zestMatch[1].replace(/,/g, '');

                const priceMatch = body.match(/\\$(\\d[\\d,]+)/i);
                if (priceMatch) data.price = priceMatch[1].replace(/,/g, '');

                const h1 = document.querySelector('h1');
                if (h1) data.address = h1.innerText.trim();

                const bedsMatch = body.match(/(\\d+)\\s*bed/i);
                if (bedsMatch) data.beds = bedsMatch[1];

                const bathsMatch = body.match(/(\\d+(?:\\.\\d+)?)\\s*bath/i);
                if (bathsMatch) data.baths = bathsMatch[1];

                const sqftMatch = body.match(/([\\d,]+)\\s*sqft/i);
                if (sqftMatch) data.sqft = sqftMatch[1].replace(/,/g, '');

                data.detailUrl = window.location.href;

                return JSON.stringify(data);
            })()
        """,
            "returnByValue": True,
        },
    )

    val = dom_result.get("result", {}).get("value")
    if not val:
        return None

    data = json.loads(val)
    if not data:
        return None

    return RealtorListing(
        address=data.get("address", ""),
        price=_safe_num(data.get("price")),
        estimate=_safe_num(data.get("estimate")),
        beds=_safe_int(data.get("beds")),
        baths=_safe_num(data.get("baths")),
        sqft=_safe_int(data.get("sqft")),
        detail_url=data.get("detailUrl", ""),
    )


# ---------------------------------------------------------------------------
# Main search function
# ---------------------------------------------------------------------------


async def search_property(page, cdp, address: str) -> RealtorListing | None:
    """Search Realtor by address.
    Expects to be on realtor.com homepage.
    """
    search_box = await page.query_selector(
        'input[id*="searchbox"], input[id*="search-input"], '
        'input[placeholder*="Address"], input[placeholder*="address"], '
        'input[placeholder*="Search"], input[type="search"], '
        'input[data-testid*="search"], input[class*="search-input"]'
    )
    if not search_box:
        # Try clicking the search area first to reveal the input
        search_area = await page.query_selector(
            '[data-testid*="search"], [class*="search-bar"], '
            '[class*="SearchBar"], [role="search"]'
        )
        if search_area:
            await search_area.click()
            await page.wait_for_timeout(1000)
            search_box = await page.query_selector(
                'input[id*="searchbox"], input[id*="search-input"], '
                'input[placeholder*="Address"], input[placeholder*="address"], '
                'input[placeholder*="Search"], input[type="search"], '
                'input[data-testid*="search"], input[class*="search-input"]'
            )
    if not search_box:
        current_url = page.url
        title = await page.title()
        logger.warning(f"Realtor: no search box found — url={current_url}, title={title}")
        return None

    await search_box.click()
    await page.wait_for_timeout(500)

    # Clear existing text
    await search_box.evaluate("el => el.value = ''")
    await page.wait_for_timeout(200)

    # Type address via CDP
    logger.debug(f"Realtor: typing '{address}'")
    await human_type(cdp, address)

    await page.wait_for_timeout(2000)

    # Click first autocomplete suggestion
    suggestion = await page.query_selector('[data-testid="search-result-item"], li[role="option"]:first-child')
    if suggestion:
        await suggestion.click()
    else:
        logger.debug("Realtor: no autocomplete, pressing Enter")
        await page.keyboard.press("Enter")

    await page.wait_for_timeout(8000)

    current_url = page.url

    if await _is_blocked(page):
        logger.warning("Realtor: CAPTCHA/block detected")
        return None

    on_detail = "/realestateandhomes-detail/" in current_url
    if not on_detail:
        first_result = await page.query_selector('a[href*="/realestateandhomes-detail/"]')
        if first_result:
            logger.debug("Realtor: clicking first search result")
            await first_result.click()
            await page.wait_for_timeout(6000)
            current_url = page.url
            on_detail = "/realestateandhomes-detail/" in current_url

    if not on_detail:
        logger.debug(f"Realtor: not on detail page. URL: {current_url}")
        return RealtorListing()  # Not found

    listing = await _extract_from_next_data(cdp)
    if not listing:
        listing = await _extract_from_dom(cdp)
    if listing:
        listing.detail_url = listing.detail_url or current_url
        return listing

    # We reached a detail URL but parsing failed (site shape drift).
    # Return an empty listing so caller records "attempted/not found" instead
    # of treating this as a hard block/captcha.
    logger.debug(f"Realtor: detail page parse produced no fields. URL: {current_url}")
    return RealtorListing(detail_url=current_url)


# ---------------------------------------------------------------------------
# Payload conversion
# ---------------------------------------------------------------------------


def listing_to_market_payload(listing: RealtorListing) -> dict:
    return {
        "listing_status": listing.status,
        "list_price": listing.price,
        "zestimate": listing.estimate,
        "rent_estimate": listing.rent_estimate,
        "beds": listing.beds,
        "baths": listing.baths,
        "sqft": listing.sqft,
        "year_built": listing.year_built,
        "lot_size": listing.lot_size,
        "property_type": listing.property_type,
        "photos": listing.photos,
        "detail_url": listing.detail_url,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_num(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int | None:
    if val is None:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None
