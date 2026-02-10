# Pipeline Overview

## Architecture

The HillsInspector pipeline uses a hybrid architecture:
- **Sequential Pre-processing**: Steps 1-3.5 run sequentially in `main.py`
- **Parallel Enrichment**: Steps 4+ run in parallel via `PipelineOrchestrator`
- **Single Database**: All data stored in SQLite (`data/property_master_sqlite.db`) with WAL mode

## Database

**SQLite only** (`data/property_master_sqlite.db`) with WAL mode enabled. All pipeline data — auctions, parcels, documents, chain of title, encumbrances, and analysis results — lives in this single database.

Key tables:
- `auctions` - Foreclosure/tax deed auction listings
- `status` - Pipeline step completion tracking
- `parcels` - Property details (owner, address, legal description, coords)
- `permits` - Building permit data
- `market_data` - Zillow/listing data
- `sales_history` - HCPA sales history
- `bulk_parcels` - HCPA bulk parcel import (529K+ rows)

## Pipeline Stages

### Pre-Processing (Sequential in main.py)

| Step | Name | Description | Code Location |
|------|------|-------------|---------------|
| 1 | Foreclosure Auctions | Scrape auction calendar | `src/scrapers/auction_scraper.py` |
| 1.5 | Tax Deed Auctions | Scrape tax deed sales | `src/scrapers/tax_deed_scraper.py` |
| 2 | Final Judgment | Download & extract PDFs | `src/services/final_judgment_processor.py` |
| 2.5 | Resolve Parcel IDs | Resolve missing parcel_id from judgment/bulk data | `docs/steps/02_5_resolve_parcel_ids.md` |
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
| HCPA GIS | Get sales history, write to parcels table | 5 |

#### Phase 2: ORI Ingestion (Sequential)
- Read legal description from `parcels` table (populated by HCPA GIS in Phase 1)
- Fallback chain: `parcels.legal_description` -> `parcels.judgment_legal_description` -> `bulk_parcels.raw_legal1-4` -> party name search
- Search Official Records Index via PAV Direct Search browser (CQID 321) and API fallback
- **Fast zero-result detection**: All browser searches intercept the PAV `KeywordSearch` API response (`page.expect_response`). Empty `Data` array → bail instantly instead of waiting 30s for table rows that never appear. Saves ~3.6h per run (76% of searches are zero-result).
- Build chain of title
- Download document PDFs
- Vision extraction gated by `VISION_EXTRACT_DOC_TYPES` — skips NOC, ASG, RELLP, PR, AGR, AFF (ORI metadata sufficient for those types)
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
+--------------------+
| Step 2.5: Resolve  |  Resolve missing parcel_id values
+--------------------+
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
|    |     FEMA, Sunbiz, HCPA GIS     |
|    |     (writes to parcels table)   |
|    +-> Phase 2: ORI Ingestion        |
|    |     Read legal desc from parcels|
|    |     Search ORI, build chain     |
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
All database writes are serialized through an async queue to prevent SQLite locking issues:
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
homeharvest_semaphore = asyncio.Semaphore(1) # HomeHarvest (serial)
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
uv run main.py --update --start-date YYYY-MM-DD --end-date YYYY-MM-DD --auction-limit 5

```

## Files

| File | Purpose |
|------|---------|
| `main.py` | Entry point, runs pre-processing steps |
| `src/orchestrator.py` | Parallel property enrichment |
| `src/db/writer.py` | Serialized database writes |
| `src/db/operations.py` | All SQLite database operations |
| `src/services/ingestion_service.py` | ORI search & chain building |
| `src/scrapers/ori_api_scraper.py` | ORI browser & API searches (PAV Direct Search) |
| `src/services/lien_survival_analyzer.py` | Lien priority analysis |

## Data Flow

```
Auctions (scraped)
    -> parcels (HCPA GIS + bulk enriched)
    -> home_harvest (MLS photos)
    -> documents (ORI PDFs)
    -> chain_of_title (ownership)
    -> encumbrances (liens/mortgages)
    -> market_data (Zillow/Realtor)
    -> analysis_results (survival calc)
```
