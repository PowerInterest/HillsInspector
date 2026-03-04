# Codebase Restructuring Plan

the `src/` directory to separate concerns based on their functional role.

## Current Pain Points
- **Mixing I/O with Logic**: Files like `pg_mortgage_extraction_service.py` contain Playwright automation alongside database queries and OpenAI API calls.
- **Ambiguous Definitions**: `services` is too generic and acts as a catch-all for anything that isn't a base model or simple utility.
- **Tight Coupling**: Controllers (like `pg_pipeline_controller.py`) import directly from scripts, services, or isolated tools.

## Proposed Directory Structure

We will restructure `src/` into the following primary domains:

### 1. `src/services/` (Bulk Data Loading, External Data Acquistion)
**Focus**: Large data, API Endpoints, vision, playwright.
- `vision_service.py` (OpenAI / GLM Vision)
- `playwright_service.py'

### 2. `src/domain/` (Core Business Logic)
**Focus**: Pure business logic, algorithms, and orchestration. Contains the specific rules for how data interacts.
- `title_chain/`
- `foreclosure/`
- `tax/`
- "

### 3. `src/db/` (Database Operations)
**Focus**: All complex SQL queries, migrations, and ORM setups. If it talks to the database, it lives here.
- `queries/` (e.g., `foreclosure_queries.py`, `sales_queries.py`)
- `migrations/` (existing)
- `models/` (existing SQLAlchemy models)
- `normalizers`

### 4. `src/orchestration/` (Pipelines & Workers)
**Focus**: Wiring together components. These scripts import from `scrapers`, `domain`, and `db` to execute workflows.
- `pipeline_controller.py` (the main pipeline orchestrator)
- `market_data_dispatcher.py` / `worker.py` (Celery/background task queues)
- `bulk_step_worker.py`

### 5. `src/tools`  Standalone Tools
- `src/tools/pg_check_success_rates.py` (One-off manual dashboard)
- `src/tools/db_audit.py` (PostgreSQL DB audit script)
- `src/tools/purge_fuzzy_encumbrances.py` (One-time DB cleanup run)


DOCUMENTATION STRUCTURE TO MATCH SRC/ structure
## Benefits of this Structure
1. **Testing**: You can test the `domain` logic by mocking the `db` layer, without needing to run a Playwright browser.
2. **Readability**: A massive file like `pg_mortgage_extraction_service.py` gets split into `clerk_scraper.py` (downloads PDF), `vision_service.py` (extracts text), and a pipeline script that glues them together.
3. **Reusability**: `clerk_scraper.py` can be used to download *any* document, not just mortgages, freeing it up for judgments or deeds.

