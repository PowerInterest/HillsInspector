# Pipeline Steps

## Execution Flow

The pipeline runs via `uv run main.py --update`. Steps 1-3 run sequentially. Steps 4-12 run **in parallel per property** inside the orchestrator.

```
Sequential:  Step 1 → Step 1.5 → Step 2 → Step 3
                                                 ↓
Per-property parallel:  ┌─ Phase 1 (parallel) ────────────────┐
                        │  Step 4   HCPA GIS Enrichment       │
                        │  Step 3.5 HomeHarvest Market Data   │
                        │  Step 7   Building Permits           │
                        │  Step 9   Market Data (Zillow)       │
                        │  Step 12  Tax Payment Status         │
                        └──────────────────────────────────────┘
                                         ↓
                        ┌─ Phase 2 (sequential) ──────────────┐
                        │  Step 5   ORI Ingestion & Chain      │
                        └──────────────────────────────────────┘
                                         ↓
                        ┌─ Phase 3 (sequential) ──────────────┐
                        │  Step 6   Lien Survival Analysis     │
                        │  Step 8   FEMA Flood Zone            │
                        └──────────────────────────────────────┘
                                         ↓
Sequential:  Step 15  Geocode Missing Parcels
```

## Step Details

### Step 1: Scrape Foreclosure Auctions
- **Source**: Hillsborough County Clerk auction calendar
- **Output**: `auctions` table rows with case_number, parcel_id, auction_date, plaintiff, defendant
- **Skip if**: Date already has auctions in DB matching calendar count
- **Key file**: `src/scrapers/auction_scraper.py`

### Step 1.5: Scrape Tax Deed Auctions
- **Source**: Same calendar, different auction type
- **Output**: `auctions` table rows with `auction_type = 'tax_deed'`
- **Skip if**: `skip_tax_deeds=True` (default) or date already scraped
- **Key file**: `src/scrapers/tax_deed_scraper.py`

### Step 2: Download & Extract Final Judgment PDFs
- **Source**: ORI (Official Records Index) via case number search
- **Process**: Download PDF → render pages to images → vision/OCR extraction → structured JSON
- **Output**: `extracted_judgment_data` JSON on auctions row (defendant, plaintiff, legal description, amounts)
- **Skip if**: `extracted_judgment_data IS NOT NULL`
- **Key files**: `src/scrapers/auction_scraper.py` (download), `src/services/final_judgment_processor.py` (extraction), `src/services/vision_service.py` (OCR)

### Step 3: Bulk Data Enrichment
- **Source**: Pre-loaded bulk parcel data (`bulk_parcels` table from county data dumps)
- **Process**: Matches auctions to bulk data by parcel_id, fills in address/owner/specs
- **Output**: Updates `auctions` table with address, owner, property details
- **Skip if**: Already bulk-enriched (status flag)
- **Key file**: `src/ingest/bulk_parcel_ingest.py`

### Step 3.5: HomeHarvest Market Data
- **Source**: HomeHarvest API (MLS listing data)
- **Output**: `home_harvest` table with listing price, beds, baths, sqft, days on market
- **Skip if**: Has recent data (within 7 days)
- **Key file**: `src/services/homeharvest_service.py`

### Step 4: HCPA GIS Enrichment
- **Source**: Hillsborough County Property Appraiser (HCPA) website
- **Process**: Scrapes property details, sales history, images, tax collector links
- **Output**: `parcels` table (owner, specs, coords), `sales_history` table
- **Skip if**: `parcels` already has `owner_name` for this folio
- **Key file**: `src/scrapers/hcpa_gis_scraper.py`

### Step 5: ORI Ingestion & Chain of Title (Iterative Discovery)
- **Source**: Hillsborough County Official Records Index (ORI)
- **Process**: Searches by legal description, name, book/page, instrument → discovers all recorded documents → builds ownership chain → identifies encumbrances
- **Output**: `documents`, `chain_of_title`, `encumbrances` tables
- **Skip if**: Folio has chain AND `last_analyzed_case_number` matches current case
- **Key files**: `src/services/step4v2/discovery.py`, `src/services/step4v2/chain_builder.py`, `src/services/step4v2/search_queue.py`

### Step 6: Lien Survival Analysis
- **Source**: Encumbrances from Step 5 + Florida lien priority rules
- **Process**: Determines which liens survive the foreclosure sale (MRTA, tax liens, HOA, etc.)
- **Output**: `encumbrances.survival_status`, `encumbrances.survival_reason`
- **Skip if**: Already has `survival_status` AND same case number
- **Key file**: `src/services/lien_survival_analyzer.py`

### Step 7: Building Permits
- **Source**: Hillsborough County + City of Tampa permit portals (Accela)
- **Process**: Screenshot-based scraping → vision OCR extraction of permit table
- **Output**: `permits` table with permit type, status, dates, contractor
- **Skip if**: Has permit data for this folio
- **Key file**: `src/scrapers/permit_scraper.py`

### Step 8: FEMA Flood Zone
- **Source**: FEMA National Flood Hazard Layer API
- **Output**: Flood zone designation on parcels (Zone X, AE, VE, etc.)
- **Skip if**: Already has flood data
- **Key file**: `src/scrapers/fema_flood_scraper.py`

### Step 9: Market Data (Zillow)
- **Source**: Zillow property page scraping
- **Output**: `market_data` table with Zestimate, tax assessment, listing status
- **Skip if**: Always runs (refresh data)
- **Key file**: `src/scrapers/market_scraper.py`

### Step 12: Tax Payment Status
- **Source**: Hillsborough County Tax Collector
- **Output**: Tax delinquency status, amount owed, years delinquent
- **Skip if**: Has tax data for this folio
- **Key file**: `src/scrapers/tax_scraper.py`

### Step 15: Geocode Missing Parcels
- **Source**: Nominatim geocoding API
- **Process**: Geocodes property addresses to lat/lng for map display
- **Output**: `parcels.latitude`, `parcels.longitude`
- **Runs**: After all property enrichment, for parcels missing coordinates

## Database

Single SQLite database at `data/property_master_sqlite.db` (WAL mode).

## Key Identifiers

| ID | Description | Example |
|----|-------------|---------|
| `case_number` | Unique per foreclosure filing | `292024CA001637A001HC` |
| `folio` | Numeric property identifier | `0720960000` |
| `parcel_id` | HCPA strap format | `1929084NUB00000000040A` |
| `instrument_number` | ORI document identifier | `2026048604` |

## CLI Options

```
--update              Run full pipeline
--auction-limit N     Cap auctions scraped per date in Step 1 (testing only)
--start-date DATE     Override start date (default: today)
--end-date DATE       Override end date (default: +42 days)
--retry-failed        Re-process previously failed cases
--new                 Archive old DB and create fresh
--web                 Start web dashboard instead of pipeline
```
