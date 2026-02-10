# HCPA GIS Scraper

## Overview
The `hcpa_gis_scraper` scrapes detailed property information from the Hillsborough County Property Appraiser's GIS portal. It is the primary source for sales history, building characteristics, and legal descriptions.

## Source
- **URL**: `https://gis.hcpafl.org/propertysearch/`
- **Type**: Web Scraping (Playwright)

## Inputs
- **Parcel ID**: 19-digit URL-formatted parcel ID (e.g., `1829134XZ000012000090A`).
- **Folio**: Property Folio number (e.g., `1918870000`).

## Outputs
- **Dictionary**: Contains:
    - Sales History (Book/Page, Date, Price, Instrument)
    - Building Info (Year Built, Type)
    - Legal Description
    - Tax Collector Link
    - Permits (Basic info)
- **Files Stored via ScraperStorage**:
    - **Screenshots**: Full page screenshot saved to `data/Foreclosure/{case_number}/screenshots/hcpa_gis/property_details.png`
    - **Raw Data**: Scraped data dictionary saved to `data/Foreclosure/{case_number}/raw/hcpa_gis/property_details.json`
    - **Documents**: Downloaded deeds/instruments from sales history saved to `data/Foreclosure/{case_number}/documents/deed_{doc_id}.pdf`

## Database Writes

### `save_hcpa_to_parcel` (Added 2026-02-09)

After scraping, the orchestrator writes key HCPA data to the `parcels` table via `PropertyDB.save_hcpa_to_parcel()`. This is critical because Phase 2 (ORI ingestion) reads legal descriptions from the `parcels` table to construct search queries.

**Fields written:**
- `property_address` (from `property_info.site_address`)
- `year_built` (from `building_info.year_built`)
- `image_url` (from `image_url`)
- `legal_description` (from `legal_description`)

Uses UPSERT pattern: `INSERT OR IGNORE` + `UPDATE ... SET col = COALESCE(?, col)` to preserve existing non-null values.

### `update_legal_description`

Also uses UPSERT pattern (fixed 2026-02-09 from plain UPDATE that silently affected 0 rows on non-existent parcels).

### Parcel ID Format

The `parcels.folio` column stores the **HCPA-format parcel ID** (e.g., `1929084NUB00000000040A`), matching `auctions.parcel_id`. This is NOT the short folio format (e.g., `000411-0000`). The orchestrator queries `parcels WHERE folio = ?` using the auction's `parcel_id`.

## Key Methods
- `scrape_hcpa_property(parcel_id, folio)`: Main scraping function.
- `fetch_sales_documents(hcpa_result)`: Follows links in sales history to download documents from the Clerk's PAV system.

## Bug History

### Parcels Table Empty (Fixed 2026-02-09)

**Root cause:** `_run_hcpa_gis` in `orchestrator.py` never called `upsert_parcel()` or any method to write HCPA data to the `parcels` table. It only called `update_legal_description()`, which was a plain UPDATE on a row that didn't exist yet (affecting 0 rows silently).

**Impact:** The `parcels` table remained empty (0 rows) across all pipeline runs. This caused Phase 2 (ORI ingestion) to find no legal descriptions, skipping chain-of-title analysis for every property. This was the root cause of zero chain-of-title data.

**Fix:**
1. Added `save_hcpa_to_parcel()` method to `PropertyDB`
2. Added call to it in `_run_hcpa_gis` (orchestrator)
3. Changed `update_legal_description()` from plain UPDATE to UPSERT
4. Backfilled 120 parcels from 339 existing HCPA JSON files on disk
