"""
Zillow scraper using CDP on a real Chrome page.

Stateless module — receives page + CDP session from the caller.
Does NOT manage its own browser.

Uses CDP for typing (React inputs don't respond to Playwright fill()).
Extracts data from __NEXT_DATA__ SSR payload with DOM fallback.
Never takes screenshots.
"""
import asyncio
import json
import random
from dataclasses import dataclass, field

from loguru import logger


@dataclass
class ZillowListing:
    zpid: str = ""
    address: str = ""
    price: float | None = None
    zestimate: float | None = None
    rent_zestimate: float | None = None
    beds: int | None = None
    baths: float | None = None
    sqft: int | None = None
    year_built: int | None = None
    lot_size: str | None = None
    home_status: str | None = None
    home_type: str | None = None
    tax_assessed_value: float | None = None
    photos: list[str] = field(default_factory=list)
    detail_url: str = ""


# ---------------------------------------------------------------------------
# CDP key helpers
# ---------------------------------------------------------------------------

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
    TYPO_RATE = 0.04
    PAUSE_RATE = 0.08
    ADJACENT = {
        "a": "sq", "s": "ad", "d": "sf", "f": "dg", "g": "fh",
        "h": "gj", "j": "hk", "k": "jl", "l": "k;",
        "q": "wa", "w": "qe", "e": "wr", "r": "et", "t": "ry",
        "y": "tu", "u": "yi", "i": "uo", "o": "ip", "p": "o",
        "1": "2", "2": "13", "3": "24", "4": "35", "5": "46",
        "6": "57", "7": "68", "8": "79", "9": "80", "0": "9",
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
    """Check if Zillow is showing a CAPTCHA or access denied page."""
    return await page.evaluate("""
        () => {
            const text = (document.body.innerText || '').toLowerCase();
            return text.includes('press and hold') || text.includes('captcha')
                || text.includes('access denied') || text.includes('verify you are human');
        }
    """)


# ---------------------------------------------------------------------------
# Data extraction
# ---------------------------------------------------------------------------

async def _extract_from_next_data(cdp) -> ZillowListing | None:
    """Extract property data from Zillow's __NEXT_DATA__ SSR payload via CDP."""
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
                        const val = typeof parsed[key] === 'string'
                            ? JSON.parse(parsed[key]) : parsed[key];
                        if (val?.property) {
                            const p = val.property;
                            const photos = (p.photos || p.responsivePhotos || []).map(photo => {
                                if (photo.mixedSources?.jpeg) {
                                    const jpegs = photo.mixedSources.jpeg;
                                    return jpegs[jpegs.length - 1]?.url;
                                }
                                return photo.url || photo.fullUrl || '';
                            }).filter(Boolean).slice(0, 30);

                            const addr = p.address || {};
                            const fullAddr = [
                                addr.streetAddress,
                                addr.city,
                                addr.state,
                                addr.zipcode
                            ].filter(Boolean).join(', ');

                            return JSON.stringify({
                                zpid: String(p.zpid || ''),
                                address: fullAddr,
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
                                photos: photos,
                                detailUrl: p.url || '',
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
    if not val:
        return None

    data = json.loads(val)
    if "error" in data:
        logger.warning(f"Zillow __NEXT_DATA__ parse error: {data['error']}")
        return None

    return ZillowListing(
        zpid=data.get("zpid", ""),
        address=data.get("address", ""),
        price=_safe_num(data.get("price")),
        zestimate=_safe_num(data.get("zestimate")),
        rent_zestimate=_safe_num(data.get("rentZestimate")),
        beds=_safe_int(data.get("bedrooms")),
        baths=_safe_num(data.get("bathrooms")),
        sqft=_safe_int(data.get("livingArea")),
        year_built=_safe_int(data.get("yearBuilt")),
        lot_size=str(data["lotSize"]) if data.get("lotSize") else None,
        home_status=data.get("homeStatus"),
        home_type=data.get("homeType"),
        tax_assessed_value=_safe_num(data.get("taxAssessedValue")),
        photos=data.get("photos", []),
        detail_url=data.get("detailUrl", ""),
    )


async def _extract_from_dom(cdp) -> ZillowListing | None:
    """Fallback: extract basic data from visible DOM text via CDP."""
    dom_result = await cdp.send("Runtime.evaluate", {
        "expression": """
            (() => {
                const body = document.body.innerText;
                const data = {};

                const zestMatch = body.match(/Zestimate[^$]*\\$(\\d[\\d,]+)/i);
                if (zestMatch) data.zestimate = zestMatch[1].replace(/,/g, '');

                const rentMatch = body.match(/Rent Zestimate[^$]*\\$(\\d[\\d,]+)/i);
                if (rentMatch) data.rentZestimate = rentMatch[1].replace(/,/g, '');

                const h1 = document.querySelector('h1');
                if (h1) data.address = h1.innerText.trim();

                const specMatch = body.match(/(\\d+)\\s*bd.*?(\\d+(?:\\.\\d+)?)\\s*ba.*?([\\d,]+)\\s*sqft/i);
                if (specMatch) {
                    data.beds = specMatch[1];
                    data.baths = specMatch[2];
                    data.sqft = specMatch[3].replace(/,/g, '');
                }

                return JSON.stringify(data);
            })()
        """,
        "returnByValue": True,
    })

    val = dom_result.get("result", {}).get("value")
    if not val:
        return None

    data = json.loads(val)
    if not data:
        return None

    return ZillowListing(
        address=data.get("address", ""),
        zestimate=_safe_num(data.get("zestimate")),
        rent_zestimate=_safe_num(data.get("rentZestimate")),
        beds=_safe_int(data.get("beds")),
        baths=_safe_num(data.get("baths")),
        sqft=_safe_int(data.get("sqft")),
    )


# ---------------------------------------------------------------------------
# Main search function
# ---------------------------------------------------------------------------

async def search_property(page, cdp, address: str) -> ZillowListing | None:
    """Search Zillow for a property by address.

    Expects the page to already be on zillow.com homepage.
    Returns ZillowListing on success, None if blocked/failed.
    """
    # Find and click the search box
    search_box = await page.query_selector(
        'input#search-box-input, '
        'input[placeholder*="Enter an address"], '
        'input[type="search"], '
        'input[aria-label*="Search"]'
    )
    if not search_box:
        search_box = await page.query_selector('input[placeholder*="address" i]')
    if not search_box:
        logger.warning("Zillow: no search box found on page")
        return None

    await search_box.click()
    await page.wait_for_timeout(500)

    # Clear existing text (select all + delete via CDP)
    await search_box.evaluate("el => el.value = ''")
    await page.wait_for_timeout(200)

    # Type address via CDP (human-like, triggers React onChange)
    logger.debug(f"Zillow: typing '{address}'")
    await human_type(cdp, address)

    # Wait for autocomplete dropdown
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
        await suggestion.click()
    else:
        logger.debug("Zillow: no autocomplete suggestion, pressing Enter")
        await page.keyboard.press("Enter")

    # Wait for navigation to property page
    await page.wait_for_timeout(8000)

    current_url = page.url

    # Check if blocked
    if await _is_blocked(page):
        logger.warning("Zillow: CAPTCHA/block detected")
        return None

    # Check if we landed on a detail page
    on_detail = "/homedetails/" in current_url or "_zpid" in current_url
    if not on_detail:
        # Might be on search results — try clicking first result
        first_result = await page.query_selector(
            'a[href*="/homedetails/"], '
            '[data-test="property-card"] a, '
            'article a[href*="_zpid"]'
        )
        if first_result:
            logger.debug("Zillow: clicking first search result")
            await first_result.click()
            await page.wait_for_timeout(6000)
            current_url = page.url
            on_detail = "/homedetails/" in current_url or "_zpid" in current_url

    if not on_detail:
        logger.debug(f"Zillow: not on detail page after search. URL: {current_url}")
        return ZillowListing()  # empty = not found (not blocked)

    # Extract data: __NEXT_DATA__ first, DOM fallback
    listing = await _extract_from_next_data(cdp)
    if not listing:
        listing = await _extract_from_dom(cdp)
    if listing:
        listing.detail_url = listing.detail_url or current_url

    return listing


# ---------------------------------------------------------------------------
# Payload conversion
# ---------------------------------------------------------------------------

def listing_to_market_payload(listing: ZillowListing) -> dict:
    """Convert ZillowListing → dict compatible with PropertyDB.save_market_data()."""
    return {
        "listing_status": listing.home_status,
        "list_price": listing.price,
        "zestimate": listing.zestimate,
        "rent_zestimate": listing.rent_zestimate,
        "rent_estimate": listing.rent_zestimate,
        "hoa_monthly": None,
        "days_on_market": None,
        "price_history": [],
        "beds": listing.beds,
        "baths": listing.baths,
        "sqft": listing.sqft,
        "year_built": listing.year_built,
        "lot_size": listing.lot_size,
        "price_per_sqft": None,
        "property_type": listing.home_type,
        "tax_assessed_value": listing.tax_assessed_value,
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
