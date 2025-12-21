# Pipeline Overview

## Architecture

The HillsInspector pipeline uses a hybrid architecture:
- **Sequential Pre-processing**: Steps 1-3.5 run sequentially in `main.py`
- **Parallel Enrichment**: Steps 4+ run in parallel via `PipelineOrchestrator`

## Pipeline Stages

### Pre-Processing (Sequential in main.py)

| Step | Name | Description | Code Location |
|------|------|-------------|---------------|
| 1 | Foreclosure Auctions | Scrape auction calendar | `src/scrapers/auction_scraper.py` |
| 1.5 | Tax Deed Auctions | Scrape tax deed sales | `src/scrapers/tax_deed_scraper.py` |
| 2 | Final Judgment | Download & extract PDFs | `src/services/final_judgment_processor.py` |
| 3 | Bulk Enrichment | Match to HCPA parcel data | `src/ingest/bulk_parcel_ingest.py` |
| 3.5 | HomeHarvest | Fetch MLS photos & data | `src/services/homeharvest_service.py` |

### Parallel Enrichment (Orchestrator)

The orchestrator processes each auction property through 3 phases:

#### Phase 1: Independent Scrapers (Parallel)
| Task | Description | Semaphore |
|------|-------------|-----------|
| Tax Scraper | Get tax payment status | 5 |
| Market (Zillow) | Get Zestimate, status | 3 |
| Market (Realtor) | Get HOA, price history | 2 |
| FEMA Flood | Check flood zone | 10 |
| Sunbiz | Corporate entity lookup | 5 |
| HCPA GIS | Get sales history | 5 |

#### Phase 2: ORI Ingestion (Sequential)
- Search Official Records Index
- Build chain of title
- Download document PDFs
- Extract encumbrances

#### Phase 3: Survival Analysis (Sequential)
- Analyze lien priority
- Calculate surviving debt
- Mark analysis complete

## Execution Flow

```
main.py --update
    |
    v
+-------------------+
| Step 1: Auctions  |  Scrape foreclosure calendar
+-------------------+
    |
    v
+-------------------+
| Step 1.5: Tax     |  Scrape tax deed calendar
+-------------------+
    |
    v
+-------------------+
| Step 2: Judgments |  Download/extract final judgment PDFs
+-------------------+
    |
    v
+-------------------+
| Step 3: Bulk      |  Enrich from HCPA parcel dump
+-------------------+
    |
    v
+-------------------+
| Step 3.5: HH      |  Fetch HomeHarvest photos/MLS
+-------------------+
    |
    v
+--------------------------------------+
| PipelineOrchestrator.process_auctions|
|                                      |
|  For each auction (parallel):        |
|    +-> Phase 1: Gather data          |
|    |     Tax, Zillow, Realtor,       |
|    |     FEMA, Sunbiz, HCPA GIS      |
|    +-> Phase 2: ORI Ingestion        |
|    |     Chain of title, documents   |
|    +-> Phase 3: Survival Analysis    |
|          Lien priority, calc debt    |
+--------------------------------------+
    |
    v
+-------------------+
| Complete          |
+-------------------+
```

## Key Components

### DatabaseWriter (Serialized Writes)
All database writes are serialized through a queue to prevent DuckDB concurrency issues:
```python
writer = DatabaseWriter(Path(db.db_path))
await writer.start()
# ... pipeline runs ...
await writer.stop()
```

### Semaphores (Concurrency Control)
```python
property_semaphore = asyncio.Semaphore(15)   # Max properties in parallel
market_semaphore = asyncio.Semaphore(3)      # Zillow rate limit
realtor_semaphore = asyncio.Semaphore(2)     # Realtor.com aggressive blocking
tax_semaphore = asyncio.Semaphore(5)         # Tax collector
permit_semaphore = asyncio.Semaphore(5)      # Building permits
hcpa_semaphore = asyncio.Semaphore(5)        # HCPA GIS
sunbiz_semaphore = asyncio.Semaphore(5)      # Sunbiz
fema_semaphore = asyncio.Semaphore(10)       # FEMA API (fast)
```

### Skip Logic
Each step checks if work was already done:
```python
# Skip if already analyzed for this case
last_case = db.get_last_analyzed_case(parcel_id)
if db.folio_has_chain_of_title(parcel_id) and last_case == case_number:
    db.mark_step_complete(case_number, "needs_ori_ingestion")
    return  # Skip
```

## Command Line Usage

```bash
# Full pipeline (60 days from today)
uv run main.py --update

# Custom date range
uv run main.py --update --start-date 2025-01-15 --end-date 2025-02-15

# Start from specific step (resume after failure)
uv run main.py --update --start-step 3

# Small test run (5 auctions)
uv run main.py --test

# Single property debug
uv run main.py --debug
```

## Files

| File | Purpose |
|------|---------|
| `main.py` | Entry point, runs pre-processing steps |
| `src/orchestrator.py` | Parallel property enrichment |
| `src/db/writer.py` | Serialized database writes |
| `src/services/ingestion_service.py` | ORI search & chain building |
| `src/services/lien_survival_analyzer.py` | Lien priority analysis |

## Data Flow

```
Auctions (scraped)
    -> parcels (bulk enriched)
    -> home_harvest (MLS photos)
    -> documents (ORI PDFs)
    -> chain_of_title (ownership)
    -> encumbrances (liens/mortgages)
    -> market_data (Zillow/Realtor)
    -> analysis_results (survival calc)
```
