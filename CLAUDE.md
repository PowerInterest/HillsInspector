# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

HillsInspector is a data ingestion and analysis pipeline for Hillsborough County real estate foreclosure and tax deed auctions. It aggregates data from multiple county sources (auction listings, property appraiser, official records, permits) to assess property equity and lien survival.

## Commands

```powershell
# Run quick test (5 auctions)
uv run main.py --test

# Full update (scrape, extract, analyze, enrich)
uv run main.py --update

# Start web dashboard (NiceGUI on port 8089)
uv run main.py --web

# Reset database (archives old, creates new)
uv run main.py --new

# Alternative web server (FastAPI on port 8080)
uv run python -m app.web.main

# Linting and type checking (run after modifying files)
uv run ruff check <path> --fix
uv run ty check <path>

# Install dependencies
uv sync

# Install Playwright browsers
uv run playwright install chromium
```

## Tech Stack Constraints

**Package Manager**: Only `uv` - never pip or poetry

**DataFrames**: Only `polars` - never pandas

**Database**: Only `duckdb` (OLAP columnar) - never SQLite

**Web**: FastAPI + Jinja2 SSR + HTMX (no React/SPA/client-side JS)

**OCR/Vision**: Qwen-VL via VisionService (`src/services/vision_service.py`) at `http://10.10.1.5:6969` - never EasyOCR

**Shell**: PowerShell 7 on Windows - never bash/cmd

## DuckDB Critical Pattern

DuckDB is columnar OLAP. Row-by-row operations are catastrophically slow.

```python
# NEVER do this:
for row in data:
    conn.execute("INSERT INTO table VALUES (?)", [row])

# ALWAYS do this:
conn.register("df_temp", polars_df)
conn.execute("INSERT INTO table SELECT * FROM df_temp")
```

## Architecture

### Pipeline Flow (`src/pipeline.py`)

| Step | Description | Skip If |
|------|-------------|---------|
| 1 | Scrape Foreclosure Auctions | Past/today dates; future dates where DB count >= calendar count |
| 1.5 | Scrape Tax Deed Auctions | Same as Step 1 |
| 2 | Extract Final Judgment PDFs | `case_number` has `extracted_judgment_data` |
| 3 | HCPA GIS - Sales History | `folio` has records in `sales_history` |
| 4 | ORI Ingestion & Chain of Title | `folio` has chain AND `last_analyzed_case_number` = current case |
| 5 | Lien Survival Analysis | `folio` has `survival_status` AND same case |
| 6 | Sunbiz Entity Lookup | Only runs if party is LLC/Corp/Trust |
| 7 | Building Permits | `folio` has permit data |
| 8 | FEMA Flood Zone | `folio` has flood data |
| 9 | Market Data - Zillow | Always runs (refresh) |
| 10 | Market Data - Realtor.com | `folio` has realtor data |
| 11 | Property Enrichment (HCPA) | `folio` has `owner_name` |
| 12 | Tax Payment Status | `folio` has tax data |

**Key identifiers:**
- `folio` (parcel_id) - Unique property identifier. Same property in multiple auctions = same folio.
- `case_number` - Unique per foreclosure filing. New case for same property = re-analyze.
- `last_analyzed_case_number` - Tracks which case triggered the last chain/survival analysis.

### Key Directories
- `src/scrapers/` - Data acquisition from county websites (auction, tax deed, HCPA, ORI, permits)
- `src/services/` - Business logic (ingestion, chain building, lien analysis, vision/OCR)
- `src/analyzers/` - Lien survival rules, encumbrance calculations
- `src/db/` - DuckDB operations and schema
- `app/web/` - FastAPI web interface with Jinja2 templates
- `data/properties/{folio}/` - Raw data per property (PDFs, images, parquet)

### Core Classes
- `PropertyDB` (`src/db/operations.py`) - All database operations
- `IngestionService` (`src/services/ingestion_service.py`) - Full property ingestion pipeline
- `TitleChainService` (`src/services/title_chain_service.py`) - Chain of title analysis
- `LienSurvivalAnalyzer` (`src/services/lien_survival_analyzer.py`) - Determines which liens survive foreclosure
- `AuctionScraper` / `TaxDeedScraper` - Scrape auction listings
- `ORIScraper` / `ORIApiScraper` - Official Records Index document search
- `HCPAScraper` - Hillsborough County Property Appraiser data

### Data Flow
Scrapers -> IngestionService -> PropertyDB (DuckDB) -> Analyzers -> Web UI

### Database
Single DuckDB file at `data/property_master.db`. Key tables:
- `auctions` - Foreclosure/tax deed auction listings
- `parcels` - Property details (owner, specs, coords)
- `encumbrances` - Liens, mortgages with survival status
- `chain_of_title` - Ownership history
- `documents` - ORI document metadata
- `market_data` - Zillow/listing data

## Logging

Uses `loguru`. Single log at `logs/`. Configure via `src/utils/logging_config.py`.
