# Property Market

## Overview

`property_market` is the consolidated market snapshot table in PostgreSQL — one row per
property (keyed by HCPA strap). It merges the best available data from three sources:
Zillow, Redfin, and HomeHarvest.

**Primary benefit**: fast single-row reads for the web UI, with locally-downloaded photos
served from disk instead of CDN URLs that rot.

---

## Table: `property_market` (PostgreSQL)

```sql
-- strap = HCPA strap format = pipeline parcel_id = hcpa_bulk_parcels.strap
SELECT m.*, p.owner_name, p.property_address, p.just_value
FROM property_market m
JOIN hcpa_bulk_parcels p ON p.strap = m.strap;

-- With sales history
SELECT m.strap, m.zestimate, s.sale_date, s.sale_amount, s.grantor, s.grantee
FROM property_market m
JOIN hcpa_allsales s ON s.folio = m.folio
ORDER BY s.sale_date DESC;
```

| Column | Type | Notes |
|--------|------|-------|
| `strap` | TEXT PK | HCPA strap = pipeline `parcel_id` = `hcpa_bulk_parcels.strap` |
| `folio` | TEXT | 10-digit folio = `hcpa_allsales.folio` |
| `case_number` | TEXT | Foreclosure case number |
| `zestimate` | NUMERIC | Best estimate: Zillow > Redfin > HH |
| `rent_zestimate` | NUMERIC | Best rent estimate: Zillow > HH |
| `list_price` | NUMERIC | Best list price: Redfin > Zillow > HH |
| `tax_assessed_value` | NUMERIC | Zillow > Redfin > HH |
| `beds`, `baths`, `sqft`, `year_built` | — | First non-null across sources |
| `lot_size` | TEXT | Raw string (sqft) |
| `property_type` | TEXT | e.g. `SINGLE_FAMILY` |
| `listing_status` | TEXT | e.g. `FOR_SALE`, `SOLD` |
| `detail_url` | TEXT | Link to the listing |
| `photo_local_paths` | JSONB `[]` | Relative paths: `Foreclosure/{case}/photos/001_abc.jpg` |
| `photo_cdn_urls` | JSONB `[]` | Original CDN URLs (fallback when local file missing) |
| `zillow_json` | JSONB | Full Zillow raw payload |
| `redfin_json` | JSONB | Full Redfin raw payload |
| `homeharvest_json` | JSONB | Full HomeHarvest record |
| `primary_source` | TEXT | Which source contributed the most data |
| `created_at`, `updated_at` | TIMESTAMPTZ | Row lifecycle |

---

## Service: `PropertyMarketService`

**File**: `src/services/property_market_service.py`

Invoked automatically at the end of `MarketDataService.run_batch()`. Can also be run
standalone:

```bash
uv run python -m src.services.property_market_service --limit 10
```

### Flow

```
PropertyMarketService.download_and_consolidate(properties)
    ├── _resolve_folios()         PG batch lookup: strap → 10-digit folio
    ├── _gather_photo_urls()      SQLite home_harvest + market_data CDN URLs
    ├── _download_photos()        CDN URLs → data/Foreclosure/{case}/photos/
    ├── _gather_market_fields()   SQLite → merged dict with priority rules
    └── _upsert_pg()              pg_insert ON CONFLICT DO UPDATE
```

### Photo download details

- **Max 15 photos** per property (hero + 14 thumbnails)
- **Naming**: `{idx:03d}_{sha1_12char}{ext}` e.g. `000_3bcc5e828e9b.webp`
- **Idempotent**: checks for existing file by hash before downloading
- **15s timeout** per image; skips on failure
- **0.5s delay** between downloads (CDN throttle avoidance)
- **Accepted types**: `image/jpeg`, `image/png`, `image/webp`, `image/gif`

### Consolidation priority

| Field | 1st | 2nd | 3rd |
|-------|-----|-----|-----|
| zestimate | Zillow | Redfin (zestimate field) | HomeHarvest estimated_value |
| rent_zestimate | Zillow rent_estimate | HomeHarvest estimated_monthly_rental | — |
| list_price | Redfin | Zillow | HomeHarvest |
| beds/baths/sqft/year_built | HomeHarvest | Redfin raw_json | — |
| tax_assessed_value | Zillow raw_json | Redfin raw_json | HomeHarvest |
| photos | HomeHarvest (primary, photos, alt_photos) | Redfin raw_json | Zillow raw_json |

---

## Web Integration

### Photo serving

Photos are served via FastAPI at:
```
GET /properties/{folio}/photos/{filename}
```
Looks up `case_number` from `auctions` by folio, then serves
`data/Foreclosure/{case}/photos/{filename}`.

### Template rendering

`property.html` uses `market.photos_with_fallback` (list of `{url, cdn_fallback}` dicts)
with an `onerror` CDN fallback:

```html
<img src="{{ p.url }}" loading="lazy"
     data-fallback="{{ p.cdn_fallback }}"
     onerror="if(this.dataset.fallback && !this.dataset.tried){this.dataset.tried='1';this.src=this.dataset.fallback}">
```

### Database read path

`get_market_snapshot(folio)` in `app/web/database.py`:
1. Tries `PgDashboardQueries.get_pg_market_snapshot(strap)` — returns consolidated row with local photo paths
2. Falls back to original SQLite multi-source query if PG unavailable

`get_bulk_homeharvest_photos(folios)` for card grid:
1. Tries `PgDashboardQueries.get_pg_bulk_thumbnails(straps)` — uses `photo_local_paths->>0`
2. Falls back to SQLite HomeHarvest/Redfin/Zillow CDN URLs

---

## Verification

```bash
# PG rows with owner join
PGPASSWORD=hills_dev psql -U hills -h localhost -d hills_sunbiz -c \
  "SELECT m.strap, m.folio, m.zestimate, \
          jsonb_array_length(COALESCE(m.photo_local_paths,'[]'::jsonb)) AS photos, \
          p.owner_name \
   FROM property_market m \
   LEFT JOIN hcpa_bulk_parcels p ON p.strap = m.strap \
   LIMIT 5"

# Local photo files
ls data/Foreclosure/*/photos/

# Run consolidation on 10 properties
uv run python -m src.services.property_market_service --limit 10
```
