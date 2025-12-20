# Data Ingestion Guide

## Overview

The ingestion pipeline collects property data from multiple sources, downloads and analyzes documents using vLLM vision, and builds chain of title analysis for foreclosure auction properties.

## Document Analysis Pipeline

During property ingestion, PDFs are automatically downloaded and analyzed using the Qwen-VL vision model. This extracts structured data from recorded documents.

### Analyzed Document Types

| Type Code | Description | Key Extracted Data |
|-----------|-------------|-------------------|
| D, WD, QC, SWD | Deeds | Grantor, grantee, consideration, legal description, red flags |
| MTG, MTGNT, DOT | Mortgages | Principal amount, lender, interest rate, MERS info, terms |
| LN, LIEN, JUD, HOA | Liens | Amount, creditor, lien type, survival notes |
| SAT, REL | Satisfactions | Releasing party, original instrument reference |
| ASG, ASGN | Assignments | Assignor, assignee, original mortgage reference |
| LP | Lis Pendens | Case number, plaintiff, defendants, mortgage reference |
| NOC | Notice of Commencement | Property owner, contractor, dates, bond info |
| AFF | Affidavits | Affiant, subject matter, heirs if applicable |
| FJ | Final Judgment | Judgment amount, plaintiff, defendants, sale date |

### How It Works

1. **Document Discovery** - ORI search finds all documents for a property
2. **PDF Download** - Important document types are downloaded to `data/properties/{folio}/documents/`
3. **Vision Analysis** - PDF pages converted to images, analyzed with type-specific prompts
4. **Data Storage** - Extracted JSON saved to `documents.extracted_data` column

### Controlling PDF Analysis

```python
from src.services.ingestion_service import IngestionService

# Enable PDF analysis (default)
svc = IngestionService(analyze_pdfs=True)

# Disable for faster testing
svc = IngestionService(analyze_pdfs=False)
```

### Vision Service Usage

```python
from src.services.vision_service import VisionService

vs = VisionService()

# Analyze a single image
result = vs.extract_deed("path/to/deed_page1.png")

# Analyze multi-page document
result = vs.extract_mortgage_multi(["page1.png", "page2.png", "page3.png"])

# Auto-route by document type
result = vs.extract_document_by_type_multi(image_paths, "MTG")
```

### Document Analyzer (High-Level API)

```python
from src.services.document_analyzer import DocumentAnalyzer

analyzer = DocumentAnalyzer()

# Download and analyze a document by instrument number
result = analyzer.download_and_analyze(
    instrument="2021173566",
    folio="172817060000007000090U",
    doc_type="D"
)

# Analyze an existing PDF
result = analyzer.analyze_document(
    pdf_path="data/properties/.../deed.pdf",
    doc_type="WD",
    instrument="2021173566"
)
```

## Database Setup

This creates `data/property_master.db` with all necessary tables and indices.

### 2. Ingest Bulk Parcel Data (Recommended First)

Before scraping auctions, load the bulk parcel data from HCPA for instant enrichment:

```powershell
# Download parcel data from https://www.hcpafl.org -> DOWNLOADS -> GIS DATA
# Extract parcel.dbf to data/bulk_data/

# Ingest parcels (~528K records, takes ~50 seconds)
# This automtically downloads parcel and LatLon zip files, merges them, and saves to Parquet
uv run python -m src.ingest.bulk_parcel_ingest --download
 
# Ingest lookup tables (DOR codes, subdivisions)
uv run python -m src.ingest.bulk_parcel_ingest --lookup-tables

# Validate the data
uv run python -m src.ingest.bulk_parcel_ingest --validate
```

This creates:
- `bulk_parcels` table (~528K Hillsborough parcels, including Lat/Lon)
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

#### Run Full Update (Next 60 Days)
```powershell
uv run python main.py --update
```

#### Run Test Batch (Small Set)
```powershell
uv run python main.py --test
```

#### Debug Single Property
```powershell
uv run python main.py --debug
```

### 4. Pipeline Steps
The pipeline (`src/pipeline.py`) executes the following 14 steps.

#### 1. Scrape Foreclosure Auctions
- **Source:** [hillsborough.realforeclose.com](https://hillsborough.realforeclose.com)
- **Method:** Playwright (Headless)
- **Downloads:** Final Judgment PDF
- **DB Updates:** `auctions` table
  - Columns: `case_number`, `folio`, `parcel_id`, `auction_date`, `final_judgment_amount`, `assessed_value`, `property_address`, `plaintiff`, `defendant`

#### 1.5. Scrape Tax Deed Auctions
- **Source:** [hillsborough.realtaxdeed.com](https://hillsborough.realtaxdeed.com)
- **Method:** Playwright (Headless)
- **Downloads:** None
- **DB Updates:** `auctions` table
  - Columns: `case_number`, `certificate_number`, `opening_bid`, `parcel_id`, `auction_type`

#### 2. Extract Final Judgment Data
- **Source:** [publicaccess.hillsclerk.com](https://publicaccess.hillsclerk.com) (OnBase)
- **Method:** Playwright (Download) + Vision API (Extraction)
- **Downloads:** Final Judgment PDF (if missing)
- **DB Updates:** `auctions` table
  - Columns: `extracted_judgment_data`, `raw_judgment_text`, `principal_amount`, `interest_amount`, `foreclosure_type`, `lis_pendens_date`

#### 3. Bulk Data Enrichment
- **Source:** Local `parcel.dbf` (from [hcpafl.org](https://www.hcpafl.org))
- **Method:** Polars / DuckDB
- **Downloads:** None
- **DB Updates:** `parcels` table
  - Columns: `owner_name`, `legal_description`, `year_built`, `beds`, `baths`, `heated_area`, `lot_size`, `market_value`

#### 4. HCPA GIS - Sales History
- **Source:** [gis.hcpafl.org](https://gis.hcpafl.org/propertysearch/)
- **Method:** Playwright
- **Downloads:** Sales History Documents (optional)
- **DB Updates:**
  - `sales_history` table: `book`, `page`, `instrument`, `sale_date`, `sale_price`, `doc_type`
  - `parcels` table: `legal_description` (high quality from GIS)

#### 5. ORI Ingestion & Chain of Title
- **Source:** [publicaccess.hillsclerk.com](https://publicaccess.hillsclerk.com/PAVDirectSearch/)
- **Method:** API (Primary) + Playwright (Fallback)
- **Downloads:** PDFs (Deeds, Mortgages, Liens, Lis Pendens)
- **DB Updates:**
  - `documents` table: `document_type`, `instrument_number`, `book`, `page`, `recording_date`, `ocr_text`, `extracted_data`
  - `chain_of_title` table: `owner_name`, `acquisition_date`, `acquisition_instrument`
  - `encumbrances` table: `encumbrance_type`, `amount`, `creditor`, `recording_date`

#### 6. Lien Survival Analysis
- **Source:** Internal Logic
- **Method:** Python (Statute rules + Date comparison)
- **Downloads:** None
- **DB Updates:** `encumbrances` table
  - Columns: `survival_status` ('SURVIVED', 'WIPED_OUT', 'EXPIRED')

#### 7. Sunbiz Entity Lookup
- **Source:** [search.sunbiz.org](https://search.sunbiz.org)
- **Method:** Playwright
- **Downloads:** None
- **DB Updates:** `scraper_results` (via Storage)
  - Content: JSON blob with officer names, entity status, and filing dates

#### 8. Building Permits
- **Source:** [aca-prod.accela.com](https://aca-prod.accela.com/TAMPA/)
- **Method:** Playwright + Vision API
- **Downloads:** Screenshots
- **DB Updates:** `permits` table
  - Columns: `permit_number`, `status`, `issue_date`, `description`, `contractor`, `estimated_cost`

#### 9. FEMA Flood Zone
- **Source:** [hazards.fema.gov](https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer)
- **Method:** REST API
- **Downloads:** None
- **DB Updates:** `parcels` table
  - Columns: `flood_zone`, `flood_risk`, `flood_insurance_required`

#### 10. Market Data - Zillow
- **Source:** [zillow.com](https://www.zillow.com)
- **Method:** Playwright (Stealth) + Vision API
- **Downloads:** Screenshots
- **DB Updates:** `market_data` table
  - Columns: `zestimate`, `rent_estimate`, `listing_status`, `price`

#### 11. Market Data - Realtor.com
- **Source:** [realtor.com](https://www.realtor.com)
- **Method:** Playwright (Stealth) + Vision API
- **Downloads:** Screenshots
- **DB Updates:** `market_data` table
  - Columns: `hoa_monthly`, `list_price`, `days_on_market`, `price_history`

#### 12. Property Enrichment (Fallback)
- **Source:** [gis.hcpafl.org](https://gis.hcpafl.org/propertysearch/)
- **Method:** Playwright + Vision API
- **Downloads:** Screenshots
- **DB Updates:** `parcels` table
  - Columns: `owner_name`, `year_built`, `beds`, `baths` (only if missing)

#### 13. Tax Payment Status
- **Source:** [hillsborough.county-taxes.com](https://hillsborough.county-taxes.com/public)
- **Method:** Playwright (Visible Mode)
- **Downloads:** Screenshots
- **DB Updates:** `liens` table
  - Columns: `document_type`='TAX', `amount`, `description`

### 5. Query Data

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
   - `latitude`, `longitude` - From LatLon table
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
   - `latitude`, `longitude`

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
   - `extracted_data` - JSON of vision-extracted structured data (see below)

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

## Extracted Data Structures

The `documents.extracted_data` column contains JSON with type-specific fields:

### Deed Extraction
```json
{
  "document_type": "WARRANTY_DEED",
  "grantor": "SMITH, JOHN AND JANE",
  "grantee": "DOE, RICHARD",
  "consideration": 250000.00,
  "legal_description": "LOT 5, BLOCK 2, SUNSET HILLS...",
  "subdivision": "SUNSET HILLS",
  "lot": "5",
  "block": "2",
  "plat_book": "45",
  "plat_page": "12",
  "execution_date": "2024-01-15",
  "recording_date": "2024-01-20",
  "instrument_number": "2024012345",
  "red_flags": [
    {"flag": "Low consideration suggests non-arm's length", "severity": "high"}
  ],
  "confidence": "high"
}
```

### Mortgage Extraction
```json
{
  "document_type": "MORTGAGE",
  "principal_amount": 350000.00,
  "borrower": "DOE, RICHARD",
  "lender": "WELLS FARGO BANK NA",
  "interest_rate": 6.5,
  "loan_term_years": 30,
  "is_mers_registered": true,
  "mers_min": "1000123456789012345",
  "maturity_date": "2054-01-01",
  "recording_date": "2024-01-20",
  "instrument_number": "2024012346",
  "confidence": "high"
}
```

### Lien Extraction
```json
{
  "document_type": "HOA_LIEN",
  "amount": 5200.00,
  "creditor": "SUNSET HILLS HOA INC",
  "debtor": "DOE, RICHARD",
  "lien_type": "hoa",
  "recording_date": "2024-06-15",
  "instrument_number": "2024067890",
  "survives_foreclosure": true,
  "survival_notes": "HOA lien within safe harbor period",
  "confidence": "high"
}
```

### Final Judgment Extraction
```json
{
  "case_number": "24-CA-007587",
  "plaintiff": "CITIBANK NA AS TRUSTEE",
  "defendants": [
    {"name": "DOE, RICHARD", "party_type": "borrower"},
    {"name": "SUNSET HILLS HOA", "party_type": "hoa"}
  ],
  "property_address": "123 Sunset Dr, Tampa FL 33626",
  "principal_amount": 594900.00,
  "total_judgment_amount": 736329.86,
  "foreclosure_sale_date": "2025-12-09",
  "foreclosure_type": "FIRST MORTGAGE",
  "confidence_score": 0.95
}
```

### Testing Document Extraction

```powershell
# Test extraction on a specific PDF
uv run python -m src.services.document_analyzer path/to/document.pdf D

# Batch process encumbrances missing amounts
uv run python -m src.services.document_analyzer --batch 10

# Run document extraction test suite
uv run python test_document_extraction.py
```

## Architecture

### Key Files

| File | Purpose |
|------|---------|
| `src/services/vision_service.py` | vLLM API client, document-specific prompts |
| `src/services/document_analyzer.py` | PDF download, image conversion, analysis orchestration |
| `src/services/ingestion_service.py` | Full property ingestion pipeline |
| `src/scrapers/ori_api_scraper.py` | Official Records Index search and PDF download |

### Flow Diagram

```
Auction Scraper → Property Discovery
                        ↓
              ORI Document Search
                        ↓
              For each document:
                ├── Save metadata to DB
                ├── Download PDF (if analyzable type)
                ├── Convert PDF → Images
                ├── Vision analysis → JSON
                └── Update extracted_data column
                        ↓
              Build Chain of Title
                        ↓
              Lien Survival Analysis
```
