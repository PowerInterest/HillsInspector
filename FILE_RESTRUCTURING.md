# Codebase Restructuring Plan

The `src/services` directory currently holds ~35 files that mix business logic, database queries, web scraping, API loading, and background workers. To improve maintainability, discoverability, and testability, we propose restructuring the `src/` directory to separate concerns based on their functional role.

## Current Pain Points
- **Mixing I/O with Logic**: Files like `pg_mortgage_extraction_service.py` contain Playwright automation alongside database queries and OpenAI API calls.
- **Ambiguous Definitions**: `services` is too generic and acts as a catch-all for anything that isn't a base model or simple utility.
- **Tight Coupling**: Controllers (like `pg_pipeline_controller.py`) import directly from scripts or isolated tools.

## Proposed Directory Structure

We will restructure `src/` into the following primary domains:

### 1. `src/scrapers/` (External Data Acquisition)
**Focus**: Fetching data from external APIs, web pages, or FTPs. No complex database writes or business logic. Returns clean data structures (e.g., pandas DataFrames, Pydantic models, or raw JSON).
- `tampa_accela_scraper.py` (formerly `TampaPermit.py`, isolated to just Playwright/requests)
- `county_permit_scraper.py` (formerly `CountyPermit.py`, API interaction)
- `clerk_scraper.py` (PAV Direct Search interaction)
- `sunbiz_scraper.py` (existing)
- `property_market_scrapers/` (zillow, redfin, realtor)

### 2. `src/ingest/` (Bulk Data Loading)
**Focus**: Reading large, static files (CSV, Parquet, SQLite) or processing massive API dumps into the primary PostgreSQL database.
- `hcpa_loader.py` (HCPA suite ingestion)
- `dor_nal_loader.py` (formerly `pg_nal_service.py`)
- `sunbiz_flr_loader.py` (formerly `pg_flr_service.py`)
- `clerk_bulk_loader.py` (formerly `pg_clerk_bulk_service.py`)

### 3. `src/domain/` (Core Business Logic)
**Focus**: Pure business logic, algorithms, and orchestration. Contains the specific rules for how data interacts.
- `title_chain/`
  - `builder.py` (The logic of ranking and linking events)
  - `breaks.py` (formerly `pg_title_break_service.py`)
  - `survival.py` (formerly `pg_survival_service.py`)
- `foreclosure/`
  - `identifier_recovery.py`
  - `auction_logic.py`
- `tax/`
  - `millage_calculator.py`
  - `tax_estimator.py`

### 4. `src/db/` (Database Operations)
**Focus**: All complex SQL queries, migrations, and ORM setups. If it talks to the database, it lives here.
- `queries/` (e.g., `foreclosure_queries.py`, `sales_queries.py`)
- `repositories/` (Repository pattern for entities)
- `migrations/` (existing)
- `models/` (existing SQLAlchemy models)

### 5. `src/orchestration/` (Pipelines & Workers)
**Focus**: Wiring together components. These scripts import from `scrapers`, `domain`, and `db` to execute workflows.
- `pipeline_controller.py` (the main pipeline orchestrator)
- `market_data_dispatcher.py` / `worker.py` (Celery/background task queues)
- `bulk_step_worker.py`

### 6. `src/integrations/` (Third-Party APIs)
**Focus**: Wrappers around non-scraping 3rd party APIs.
- `vision_service.py` (OpenAI / GLM Vision)
- AWS/S3 storage adapters (`scraper_storage.py`)

## Phase 0: Deletion of Stale and Unused Code
Before executing the full restructure, the following files have been identified as completely unreferenced, obsolete, or one-time data backfill scripts that can be safely deleted right now to immediately declutter the codebase:

### Obsolete Services / Logic
- `src/services/institutional_names.py` (Superseded by `config/generic_names.txt` logic)
- `src/services/pg_competition.py` (Unused competition logic)
- `src/services/pg_sunbiz_service.py` (Refactored into `sunbiz/pg_loader.py`)
- `src/services/pg_tax_service.py` (Refactored into `pg_nal_service.py`)
- `src/utils/amount_validator.py` (Unused utility)
- `src/utils/relevance_checker.py` (Unused utility)

### Obsolete / One-Off Tools
- `src/tools/check_success_rates.py` (Old SQLite-based CLI tool)
- `src/tools/pg_check_success_rates.py` (One-off manual dashboard)
- `src/tools/pg_db_audit.py` (One-off DB audit script)
- `src/tools/purge_fuzzy_encumbrances.py` (One-time DB cleanup run)
- `src/tools/run_ori_remaining.py` (One-time batch execution script)
- `src/analysis/db_audit.py` (Old SQLite-era analysis script)

### Project Root / Throwaway Scripts
- `Controller.py` (Old SQLite pipeline controller, replaced by `pg_pipeline_controller.py`)
- `debug_ori_search.py` (One-time PAV search debugging script)
- `debug_pav.py` (One-time PAV PDF download debugging script)
- `debug_requests.py` (One-time manual HTTP test script)
- `run_db_cleanup.py` (One-time manual script to prune test records)
- `test_api.py` (One-time API testing script for Vision)
- `test_fetch_id.py` (One-time ID testing script)
- `test_pav_direct.py` (One-time testing script for PAV download links)
- `track_network.py` (One-time Playwright network interceptor script)

## Benefits of this Structure
1. **Testing**: You can test the `domain` logic by mocking the `db` layer, without needing to run a Playwright browser.
2. **Readability**: A massive file like `pg_mortgage_extraction_service.py` gets split into `clerk_scraper.py` (downloads PDF), `vision_service.py` (extracts text), and a pipeline script that glues them together.
3. **Reusability**: `clerk_scraper.py` can be used to download *any* document, not just mortgages, freeing it up for judgments or deeds.

## Action Plan
Since this is a massive rewrite that touches imports across the entire repository (including the web dashboard and CLI scripts), we will **NOT implement this immediately**. When we are ready, we will do it incrementally, starting by isolating the pure scrapers and pure DB loaders.
