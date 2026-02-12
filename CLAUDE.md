# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Pipeline Success Criteria (READ THIS FIRST)

A successful `--update` run is measured by **data completeness**, not by steps completing without errors. The pipeline's purpose is to produce actionable foreclosure analysis. If the output data is missing, the run has failed regardless of whether the code ran without exceptions.

**After any `--update` run, you MUST validate these thresholds:**

| Metric | Target | Validation Query |
|--------|--------|-----------------|
| Final Judgment PDFs | 90%+ of foreclosures | Count `data/Foreclosure/*/documents/*.pdf` vs total auctions |
| Extracted judgment data | 90%+ of PDFs | `SELECT COUNT(*) FROM auctions WHERE extracted_judgment_data IS NOT NULL` |
| Chain of title | **80%+ of foreclosures with judgments** | `SELECT COUNT(DISTINCT folio) FROM chain_of_title` |
| Encumbrances identified | **80%+ of foreclosures with judgments** | `SELECT COUNT(DISTINCT folio) FROM encumbrances` |
| Lien survival analysis | **80%+ of foreclosures with judgments** | `SELECT COUNT(DISTINCT folio) FROM encumbrances WHERE survival_status IS NOT NULL` |

**If any threshold is not met, the run is a FAILURE.** Do not report success. Instead:
1. Diagnose why the data is missing (query the `status` table, check logs, read the relevant step code)
2. Fix the root cause
3. Re-run the affected steps
4. Keep iterating until thresholds are met

The chain of title and encumbrance data are the core deliverable. Without them, the pipeline produces no investment-grade analysis. Judgment PDFs and enrichment data are intermediate steps toward that goal.

## Final Judgment PDF — The Critical Document

The Final Judgment PDF is **THE** critical piece of information for this entire project. Every downstream analysis (chain of title, encumbrances, lien survival) depends on it. The auction website sometimes has incorrect or missing links to the actual judgment document. **The pipeline must make all attempts to locate the Final Judgment**, including:

1. **Primary**: Download from the clerk link on the auction page
2. **Fallback**: Search ORI by case number (`_search_judgment_by_case_number`) when the instrument number is empty or the primary link fails
3. **Recovery (CC cases / thin extractions)**: When extracted judgment data is missing critical fields (legal description, mortgage details) — especially for County Court (CC) cases — the downloaded PDF may be a fee order, not the real Final Judgment. Recovery strategy:
   - Extract party names from whatever PDF was downloaded (even if thin)
   - Search ORI by party name to find related recorded documents
   - Look for **(LP) LIS PENDENS** — the LP is filed at the start of a foreclosure and is recorded under the **real CA (Circuit Court) case number**
   - Any other documents found (liens, assignments, etc.) provide property identifiers (address, legal description, parcel number) as a bonus
   - Search ORI by the LP's case number for **(JUD) JUDGMENT** documents
   - Download and extract the **real** Final Judgment with full mortgage/lien data
4. If all strategies fail, log clearly and flag the case for manual review — do not silently skip it

**Case number format**: `29YYYYCCNNNNNN` = County Court (HOA liens, code enforcement, small claims). `29YYYYCANNNNNN` = Circuit Court (mortgage foreclosure). CC cases often reference or are related to a CA case for the same property. Do NOT dismiss CC cases as low-value — follow the chain to the real foreclosure judgment.

Never treat a missing judgment as acceptable. If a case has no PDF, investigate why and add new retrieval strategies as needed. Never suggest skipping CC cases or lowering success thresholds — find the real judgment instead.

## Foreclosing Lien — Every Foreclosure Has One

Every foreclosure auction has a foreclosing lien — that is how the law works. There must be a lis pendens recorded to initiate a foreclosure. If the survival analysis reports "Could not identify foreclosing lien", **the code is at fault**, not the data. Possible causes:

1. **Encumbrance type mismatch**: The ORI stores doc types like `(MTG) MORTGAGE` but survival code expects normalized `mortgage`. Always match broadly (check for "MORTGAGE" or "MTG" substring, not exact equality).
2. **ORI didn't find the mortgage**: The iterative discovery search terms may not have matched the property's legal description. Check the search queue for exhaustion vs. success.
3. **Plaintiff name doesn't match creditor**: The mortgage may have been assigned/transferred. The current servicer (plaintiff) name differs from the original lender (creditor in ORI). Use fuzzy matching.
4. **Chain builder didn't classify it as an encumbrance**: Check `_classify_encumbrance` and the document type mapping.

Never log "foreclosing lien not found" and move on. Investigate the root cause and fix the matching logic.

## Project Overview

HillsInspector is a data ingestion and analysis pipeline for Hillsborough County real estate foreclosure and tax deed auctions. It aggregates data from multiple county sources (auction listings, property appraiser, official records, permits) to assess property equity and lien survival.

## Commands

```powershell
# Run quick test (5 auctions)
uv run main.py --update --start-date YYYY-MM-DD --end-date YYYY-MM-DD --auction-limit 5

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

**Database**: SQLite (WAL mode) at `data/property_master_sqlite.db` — single unified database. Always access through `PropertyDB` class (`src/db/operations.py`), never open the file directly with `sqlite3.connect()` (PropertyDB sets row_factory, WAL mode, and runs migrations).

**Web**: FastAPI + Jinja2 SSR + HTMX (no React/SPA/client-side JS)

**OCR/Vision**: Qwen-VL via VisionService (`src/services/vision_service.py`) at `http://10.10.1.5:6969` - never EasyOCR

**Shell**: PowerShell 7 on Windows - never bash/cmd

**Browser Automation**: All Playwright scrapers must use `playwright-stealth` to avoid bot detection. Apply stealth to every page immediately after creation:
```python
from playwright_stealth import Stealth

async def apply_stealth(page):
    """Apply stealth settings to a page to avoid bot detection."""
    await Stealth().apply_stealth_async(page)

# After creating a page:
page = await context.new_page()
await apply_stealth(page)
```

## SQLite Critical Patterns

- **Row factory**: `PropertyDB` sets `sqlite3.Row` — use `dict(row)` to convert rows, never `dict(zip(columns, row))`
- **WAL mode**: Requires explicit `conn.commit()` for writes to be visible to other connections
- **UNIQUE constraints**: SQLite treats NULL as distinct — use `COALESCE` in expression indexes for nullable columns
- **No RETURNING**: Use `cursor.lastrowid` instead of `INSERT ... RETURNING id`
- **Date arithmetic**: Use `date('now', '-7 days')` not `CURRENT_DATE - INTERVAL 7 DAY`

## Architecture

### Pipeline Flow (`src/orchestrator.py` — legacy reference in `src/pipeline_OLD.py`)

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
- `src/db/` - SQLite operations and schema
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
Scrapers -> IngestionService -> PropertyDB (SQLite) -> Analyzers -> Web UI

### Database Architecture

**Single SQLite Database**: `data/property_master_sqlite.db` (WAL mode)

Always access via `PropertyDB` class — never `sqlite3.connect()` directly.
The DB is accessible through `PropertyDB` (`src/db/operations.py`).

**Tables** (auctions & enrichment):
- `auctions` - Foreclosure/tax deed auction listings
- `status` - Pipeline step completion tracking
- `parcels` - Property details (owner, specs, coords)
- `bulk_parcels` - HCPA bulk parcel data (strap, folio, address, legal desc)
- `permits` - Building permit data
- `market_data` - Zillow/listing data
- `sales_history` - HCPA sales history

**Tables** (ORI & title chain):
- `documents` - ORI document metadata
- `chain_of_title` - Ownership history periods
- `encumbrances` - Liens, mortgages with survival status
- `ori_search_queue` - Search queue for iterative discovery
- `linked_identities` - Name change/trust transfer mappings

**Creating New Databases:**
```bash
uv run main.py --new  # Archives old, creates fresh database
```

## Logging

Uses `loguru`. Single log at `logs/`. Configure via `src/utils/logging_config.py`.
