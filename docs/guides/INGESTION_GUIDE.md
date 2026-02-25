# Data Ingestion Guide

## Overview

This guide covers practical usage of the ingestion pipeline. For pipeline architecture and step details, see `docs/steps/`.

## Bulk Parcel Data Setup

Before scraping auctions, load the bulk parcel data from HCPA for instant enrichment:

```powershell
# Download and ingest parcels (~528K records, takes ~50 seconds)
# Automatically downloads parcel and LatLon zip files, merges them, and saves to Parquet
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

**Weekly Refresh Policy:**
- Bulk parcel data should be refreshed **weekly**
- The `bulk_parcels` table is **dropped and recreated** on each refresh
- Enriched data (encumbrances, chain of title, documents) persists across refreshes

## Vision Service Usage

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

## Document Analyzer API

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

### Testing Document Extraction

```powershell
# Test extraction on a specific PDF
uv run python -m src.services.document_analyzer path/to/document.pdf D

# Batch process encumbrances missing amounts
uv run python -m src.services.document_analyzer --batch 10
```

## Ingestion Service

```python
from src.services.ingestion_service import IngestionService

# Enable PDF analysis (default)
svc = IngestionService(analyze_pdfs=True)

# Disable for faster testing
svc = IngestionService(analyze_pdfs=False)
```

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
  "loan_term_months": 360,
  "is_mers": true,
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

## Troubleshooting

### Database Locked
If you get "database is locked" errors, ensure connections are closed:
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
1. Check that `parcel_id` is present and valid
2. Verify HCPA site is accessible
3. Check error logs for specific failures

## Related Documentation

- **Pipeline Architecture**: `docs/steps/00_pipeline_overview.md`
- **Step Details**: `docs/steps/01-13*.md`
- **Database Schema**: `docs/schema.md`
