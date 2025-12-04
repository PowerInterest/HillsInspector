# Data Ingestion Guide



This creates `data/property_master.db` with all necessary tables and indices.

### 2. Ingest Bulk Parcel Data (Recommended First)

Before scraping auctions, load the bulk parcel data from HCPA for instant enrichment:

```powershell
# Download parcel data from https://www.hcpafl.org -> DOWNLOADS -> GIS DATA
# Extract parcel.dbf to data/bulk_data/

# Ingest parcels (~528K records, takes ~50 seconds)
uv run python -m src.ingest.bulk_parcel_ingest "data/bulk_data/parcel_4_public.dbf"

# Ingest lookup tables (DOR codes, subdivisions)
uv run python -m src.ingest.bulk_parcel_ingest --lookup-tables

# Validate the data
uv run python -m src.ingest.bulk_parcel_ingest --validate
```

This creates:
- `bulk_parcels` table (~528K Hillsborough parcels)
- `dor_codes` table (305 land use categories)
- `subdivisions` table (~11,500 subdivision names)
- Parquet files in `data/parquet/` for efficient storage

**IMPORTANT: Weekly Refresh Policy**
- Bulk parcel data should be refreshed **weekly**
- The `bulk_parcels` table is **dropped and recreated** on each refresh
- Enriched data (liens, auction analysis, OCR results) is stored in **separate tables** that persist across refreshes
- This separation ensures we don't lose scraped/analyzed data when bulk data is updated

| Table | Refresh Frequency | Persistence |
|-------|------------------|-------------|
| `bulk_parcels` | Weekly (DROP & RECREATE) | Transient |
| `dor_codes` | Weekly | Transient |
| `subdivisions` | Weekly | Transient |
| `auctions` | As scraped | **Persistent** |
| `liens` | As scraped | **Persistent** |
| `documents` | As downloaded | **Persistent** |
| `analysis_results` | As computed | **Persistent** |

### 3. Run Auction Ingestion

#### Ingest Next 2 Months of Auctions
```powershell
uv run python -m src.ingest.run_ingestion
```

This will:
- Scrape foreclosure auctions for the next 60 days
- Scrape tax deed auctions for the next 60 days
- Enrich all properties with HCPA data
- Store everything in the database
- Create folder structures for each property

#### Ingest Specific Date
```powershell
uv run python -m src.ingest.run_ingestion 2025-11-26
```

### 3. Query Data

```python
from src.db.operations import PropertyDB

with PropertyDB() as db:
    # Get all auctions for a date
    auctions = db.get_auctions_by_date(date(2025, 11, 26))
    
    # Get properties pending analysis
    pending = db.get_pending_analysis(limit=10)
```

## Database Schema

### Tables

#### Bulk Data Tables (HCPA Weekly Dump)

1. **bulk_parcels** - All Hillsborough County parcels (~528K records)
   - `folio` (PK), `pin`, `strap` - Property identifiers
   - `owner_name`, `property_address`, `city`, `zip_code`
   - `land_use`, `land_use_desc` - DOR codes
   - `year_built`, `beds`, `baths`, `stories`, `heated_area`, `lot_size`
   - `assessed_value`, `market_value`, `just_value`, `taxable_value`
   - `last_sale_date`, `last_sale_price`
   - `raw_sub` - Links to subdivisions table

2. **dor_codes** - DOR land use code lookups (305 codes)
   - `dor_code` (PK) - e.g., "0100"
   - `description` - e.g., "SINGLE FAMILY R"

3. **subdivisions** - Subdivision name lookups (~11,500)
   - `sub_code` (PK)
   - `sub_name`, `plat_book`, `plat_page`

#### Working Tables (Scraped/Enriched)

4. **parcels** - Property Appraiser data (enriched from bulk)
   - `folio` (PK) - Unique property identifier
   - `owner_name`, `property_address`, `city`, `zip_code`
   - `year_built`, `beds`, `baths`, `heated_area`
   - `assessed_value`, `market_value`

5. **auctions** - Auction listings
   - `case_number` (PK) - Court case number
   - `folio` (FK) - Links to parcels
   - `auction_type` - 'FORECLOSURE' or 'TAX_DEED'
   - `auction_date`, `assessed_value`, `final_judgment_amount`
   - `lien_position` - '1st', '2nd', 'HOA', 'UNKNOWN'
   - `est_surviving_debt` - Critical for equity calculation
   - `is_toxic_title` - Flag for bad deals

6. **liens** - Recorded mortgages and liens
   - `folio` (FK), `case_number` (FK)
   - `recording_date`, `document_type`, `amount`
   - `grantor`, `grantee`
   - `survives_foreclosure` - Does this lien survive?

7. **permits** - Building permits and violations
   - `folio` (FK)
   - `permit_number`, `issue_date`, `status`
   - `estimated_cost`

8. **documents** - PDFs and evidence
   - `folio` (FK), `case_number` (FK)
   - `document_type` - 'FINAL_JUDGMENT', 'LIS_PENDENS', etc.
   - `file_path` - Path to PDF
   - `ocr_text` - Extracted text
   - `extracted_data` - JSON of parsed data

9. **analysis_results** - Final equity analysis
   - `folio` (FK), `case_number` (FK)
   - `market_value`, `rehab_cost`, `surviving_liens_total`
   - `net_equity`, `roi_percentage`, `risk_score`
   - Flags: `has_hoa_lien`, `has_surviving_mortgage`, etc.

## Folder Structure

Each property gets its own folder:

```
data/properties/{folio}/
├── documents/          # PDFs (Final Judgment, Lis Pendens, etc.)
├── ocr/                # OCR text output
├── analysis/           # Analysis reports
└── metadata.json       # Property metadata
```

## Scraping Limits

To avoid getting blocked:
- **Max 10 pages** per scraper run
- **Random delays** (2-5 seconds) between page clicks
- **Retry logic** with exponential backoff
- **Weekly intervals** for date range scraping

## Next Steps

1. **Phase 4: Lien Analysis**
   - Implement Official Records scraper
   - Complete HOVER PDF download
   - Build lien survival logic

2. **Phase 5: Web Interface**
   - FastAPI backend
   - HTMX dashboard

## Troubleshooting

### Database Locked
If you get "database is locked" errors, make sure to close all connections:
```python
with PropertyDB() as db:
    # Your code here
    pass  # Connection auto-closes
```

### Scraper Blocked
If scrapers are getting blocked:
1. Increase delays in scraper code
2. Run in headed mode (set `headless=False`)
3. Check your IP isn't blacklisted

### Missing Data
If properties aren't being enriched:
1. Check that `parcel_id` is present
2. Verify HCPA site is accessible
3. Check error logs for specific failures
