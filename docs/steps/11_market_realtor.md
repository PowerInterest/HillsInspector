# Step 11: Realtor.com Market Data

## Overview
The `RealtorScraper` gathers detailed property data from Realtor.com, supplementing Zillow data with better HOA info, price history, and descriptions. It uses `VisionService` and `playwright-stealth`.

## Source
- **URL**: `https://www.realtor.com`
- **Type**: Web Scraping (Playwright Stealth) + Vision API
- **Scraper**: `src/scrapers/realtor_scraper.py`

## Why Both Zillow AND Realtor?

| Data Point | Zillow | Realtor.com |
|------------|--------|-------------|
| Zestimate | Best | N/A |
| HOA Fees | Often missing | More accurate |
| Price History | Basic | Detailed |
| Agent Remarks | Limited | Full description |
| Listing Status | Good | Good (redundant verification) |

## Inputs
- **Address**: Street address, city, state, zip code.
- **Property ID**: For storage and caching.

## Outputs
- **RealtorListing**: Object containing:
    - Price / Status
    - HOA Fees & Frequency
    - Price History
    - Property Details (Beds, Baths, Sqft, Year Built)
- **Files Stored via ScraperStorage**:
    - **Screenshots**: Full page screenshots saved to `data/properties/{property_id}/screenshots/realtor/realtor_{address}_{timestamp}.png`
    - **Vision Output**: Extracted data saved to `data/properties/{property_id}/vision/realtor/{context}.json`

## Orchestrator Integration

### Phase 1: Parallel Scraping
Realtor.com scraping runs in Phase 1 alongside other scrapers:

```python
# In orchestrator.py
async with asyncio.TaskGroup() as tg:
    tg.create_task(self._run_tax_scraper(parcel_id, address))
    tg.create_task(self._run_market_scraper(parcel_id, address))      # Zillow
    tg.create_task(self._run_realtor_scraper(parcel_id, address))     # Realtor.com
    tg.create_task(self._run_fema_checker(parcel_id, address))
    tg.create_task(self._run_sunbiz_scraper(parcel_id, prop.owner_name))
    tg.create_task(self._run_hcpa_gis(parcel_id))
```

### Concurrency Control
```python
# Low concurrency due to aggressive rate limiting
self.realtor_semaphore = asyncio.Semaphore(2)
```

### Skip Logic
```python
async def _run_realtor_scraper(self, parcel_id: str, address: str):
    # Skip if already have Realtor data
    if self.db.folio_has_realtor_data(parcel_id):
        return
```

### Data Saved to market_data Table
```python
await self.db_writer.enqueue("save_market_data", {
    "folio": parcel_id,
    "source": "Realtor",  # Distinguishes from "Zillow"
    "data": {
        "list_price": listing.list_price,
        "listing_status": listing.listing_status,
        "hoa_fee": listing.hoa_fee,
        "hoa_frequency": listing.hoa_frequency,
        "days_on_market": listing.days_on_market,
        "price_per_sqft": listing.price_per_sqft,
        "beds": listing.beds,
        "baths": listing.baths,
        "sqft": listing.sqft,
        "year_built": listing.year_built,
        "description": listing.description,
        "realtor_url": listing.realtor_url,
    },
    "screenshot_path": listing.screenshot_path
})
```

## Key Methods
- `get_listing_details(...)`: Navigates to the listing page, simulates human behavior, takes screenshots, and extracts data.
- `get_listing_for_property(...)`: Wrapper with caching support via `ScraperStorage`.

## Anti-Bot Measures

Realtor.com uses aggressive bot detection (PerimeterX/Datadome). The scraper implements:

1. **Playwright Stealth**: Hides automation fingerprints
2. **Human-like Behavior**:
   - Random scrolling (2-4 scrolls, 200-500px each)
   - Random mouse movements (3-6 movements)
   - Random delays (0.5-2.0 seconds)
3. **Realistic Browser Context**:
   - Chrome user agent
   - 1920x1080 viewport
   - US timezone and locale

### Blocking Detection
```python
is_blocked = (
    "captcha" in content.lower() or
    "blocked" in content.lower() or
    "request could not be processed" in page_text.lower() or
    "reference id is" in page_text.lower() or
    "unblockrequest@realtor.com" in page_text.lower()
)

if is_blocked:
    logger.warning("Realtor.com blocked the request")
    # Screenshot saved for debugging
    return listing  # Return empty listing
```

## Database Query
Check for existing Realtor data:
```sql
SELECT COUNT(*) FROM market_data
WHERE folio = ? AND source = 'Realtor'
```

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| Blocked/403 | Bot detection | Wait, reduce concurrency |
| No data | Property not on Realtor.com | Normal - not all properties listed |
| Missing HOA | Not disclosed | Check HOA separately |

## Code Location
- **Scraper**: `src/scrapers/realtor_scraper.py`
- **Orchestrator Call**: `src/orchestrator.py` → `_run_realtor_scraper()`
