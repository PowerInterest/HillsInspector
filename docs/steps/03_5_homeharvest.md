# Step 3.5: HomeHarvest Enrichment

## Overview
This step enriches properties with MLS data from Realtor.com via the HomeHarvest library. It provides property photos, listing history, and detailed property information that supplements HCPA bulk data.

## Source
- **Library**: [HomeHarvest](https://pypi.org/project/homeharvest/) (Python package)
- **Data Origin**: Realtor.com MLS data
- **Method**: Python API calls with rate limiting

## Process Flow

1. **Property Selection**:
   - Query auctions where `needs_homeharvest_enrichment = TRUE`
   - Require valid `property_address` from parcels table
   - Skip properties with `hcpa_scrape_failed = TRUE`
   - Skip properties with recent HomeHarvest data (< 7 days old)

2. **Address Formatting**:
   - Clean HCPA format addresses (fix "FL- " to "FL ")
   - Construct full address: `{street}, {city}, FL {zip}`
   - Handle incomplete addresses by appending missing components

3. **Data Retrieval**:
   - Call `scrape_property(location=address, listing_type="sold", past_days=3650)`
   - Rate limited: 15-30 second delay between requests
   - Auto-upgrade detection if library is outdated

4. **Storage**:
   - Insert record into `home_harvest` table
   - Mark `needs_homeharvest_enrichment = FALSE` on success

## Data Points

The following fields are captured in the `home_harvest` table:

### Property Photos (Critical for Web UI)
| Field | Type | Description |
|-------|------|-------------|
| `primary_photo` | VARCHAR | Main listing photo URL |
| `photos` | JSON | Array of all photo URLs |
| `alt_photos` | JSON | Alternative photo URLs |

### Listing Information
| Field | Type | Description |
|-------|------|-------------|
| `list_price` | DOUBLE | Current or last list price |
| `sold_price` | DOUBLE | Last sale price |
| `last_sold_date` | TIMESTAMP | Date of last sale |
| `days_on_mls` | INTEGER | Days on market |
| `mls_status` | VARCHAR | Active, Sold, Pending, etc. |
| `hoa_fee` | DOUBLE | Monthly HOA fee |

### Property Details
| Field | Type | Description |
|-------|------|-------------|
| `beds` | DOUBLE | Number of bedrooms |
| `full_baths` | DOUBLE | Full bathrooms |
| `half_baths` | DOUBLE | Half bathrooms |
| `sqft` | DOUBLE | Living area square footage |
| `lot_sqft` | DOUBLE | Lot size in square feet |
| `year_built` | INTEGER | Year constructed |
| `stories` | DOUBLE | Number of stories |
| `garage` | DOUBLE | Garage spaces |

### Location
| Field | Type | Description |
|-------|------|-------------|
| `latitude` | DOUBLE | GPS latitude |
| `longitude` | DOUBLE | GPS longitude |
| `neighborhoods` | VARCHAR | Neighborhood names |
| `nearby_schools` | JSON | School information |

## Pipeline Integration

### Location in Pipeline
```
Step 1   → Foreclosure Auction Scrape
Step 1.5 → Tax Deed Auction Scrape
Step 2   → Final Judgment Extraction
Step 3   → Bulk Data Enrichment
Step 3.5 → HomeHarvest Enrichment  ← THIS STEP
Step 4+  → Parallel Property Enrichment (Orchestrator)
```

### Code Location
- **Service**: `src/services/homeharvest_service.py`
- **Pipeline Call**: `main.py` → `handle_update()` (after Step 3)

### Execution
```python
# In main.py handle_update()
from src.services.homeharvest_service import HomeHarvestService

hh_service = HomeHarvestService()
hh_props = hh_service.get_pending_properties(limit=100, auction_date=start_date)

for prop_data in hh_props:
    success = hh_service._process_single_property(folio, location)
    if success:
        db.mark_step_complete(case_number, "needs_homeharvest_enrichment")
    time.sleep(random.uniform(15.0, 30.0))  # Rate limiting
```

## Rate Limiting

Realtor.com has aggressive bot detection. The service implements:

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `MIN_DELAY` | 15 seconds | Minimum wait between requests |
| `MAX_DELAY` | 30 seconds | Maximum wait (randomized) |
| `parallel` | False | Sequential requests only |

### Auto-Upgrade Feature
If blocked (403, RetryError), the service:
1. Checks for newer HomeHarvest version on PyPI
2. Upgrades via `uv pip install --upgrade homeharvest`
3. Spawns a fresh subprocess with the new version

## Importance

1. **Property Photos**: Critical for web UI - users need to see properties visually
2. **MLS Data**: More accurate/current than HCPA bulk data
3. **HOA Fees**: Important for investment analysis (monthly carrying costs)
4. **Sold History**: Provides comparable sales data

## Web UI Integration

Photos are displayed in the property detail page:
```python
# app/web/database.py
photos = []
primary = homeharvest.get("primary_photo")
if primary:
    photos.append(primary)
for field in ("photos", "alt_photos"):
    extra = _safe_json(homeharvest.get(field))
    if isinstance(extra, list):
        photos.extend(extra)
```

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| No data returned | Address not found in MLS | Property may be off-market or unlisted |
| Rate limit (403) | Too many requests | Increase delay, check for library update |
| Blocking error | Bot detection | Auto-upgrade triggers, or wait and retry |

### Manual Run
```bash
# Run HomeHarvest for specific properties
uv run python -c "
from src.services.homeharvest_service import HomeHarvestService
hh = HomeHarvestService()
props = hh.get_pending_properties(limit=10)
hh.fetch_and_save(props)
"
```

## Maintenance

- HomeHarvest library updates frequently to combat blocking
- Run `uv add homeharvest --upgrade` periodically
- Check PyPI for new versions if experiencing blocks
