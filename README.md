# Hillsborough County Property Inspector

A data ingestion and analysis pipeline for Hillsborough County real estate, focusing on foreclosure and tax deed auctions. This tool aggregates data from multiple sources to help assess property equity and risk. https://publicrec.hillsclerk.com/Civil/

## Project Structure

    src/scrapers/* get the data, and then call a service to store what they found, sometimes they get all the data, others can give us a link to the data
    src/services/* Analyze the data, transforming it and writing it into the database
    src/db/*  has everything to start a new database, and db scripts
    src/ingest/* has to do with bringing in data before we start scraping websites
    docs/*  is where we keep documentation for the project, each scraper has its own documentation in docs/scrapers/*
    data/properties/*  holds all the raw data like, pictures, parquet files, json, pdfs, etc. , each directory is a property using the folio number 
    app/*  holds the web application code, any heavy data manipulation should be done under /src/services
    logs/  one log file for the whole project using loguru
    utils/  has utility functions for the project
    Controller.py  the PG-first pipeline entry point for the project


## Usage

**Pipeline startup is handled via `Controller.py` (PG-first).**

### 1. Run Full Pipeline
Runs Phase A (bulk refresh) + Phase B (per-auction enrichment).
```powershell
uv run Controller.py
```

### 2. Quick Sanity Run
Runs the controller with narrow limits for auction/judgment/ORI/survival steps.
```powershell
uv run Controller.py --auction-limit 5 --judgment-limit 5 --ori-limit 5 --survival-limit 5 --limit 5
```

### 3. Run Phase A Only (bulk refresh)
```powershell
uv run Controller.py --skip-auction-scrape --skip-judgment-extract --skip-identifier-recovery --skip-ori-search --skip-mortgage-extract --skip-survival --skip-encumbrance-audit --skip-encumbrance-recovery --skip-final-refresh --skip-market-data
```

### 4. Run Phase B Only (enrichment)
```powershell
uv run Controller.py --skip-hcpa --skip-clerk-bulk --skip-clerk-criminal --skip-clerk-civil-alpha --skip-nal --skip-flr --skip-sunbiz-entity --skip-county-permits --skip-tampa-permits --skip-single-pin-permits --skip-foreclosure-refresh --skip-trust-accounts --skip-title-chain --skip-title-breaks --skip-market-data
```

### 5. Start Web Server
Launches the local web dashboard to view results.
```powershell
uv run python -m app.web.main
```
Start the same web app with a public `ngrok` tunnel:
```powershell
uv run python -m app.web.main --ngrok
```
`ngrok` auth can come from either:
- `ngrok config add-authtoken <token>`
- `NGROK_AUTHTOKEN=<token>`

Database tab (CloudBeaver):
- `CLOUDBEAVER_PG_URL` (or fallback `CLOUDBEAVER_URL`, default `http://localhost:8978`) controls PostgreSQL embed.
- `CLOUDBEAVER_SQLITE_URL` (optional) can point to a CloudBeaver SQLite connection.
- `CLOUDBEAVER_EMBED=0` disables iframe embed and leaves only the "Open In New Tab" link.
The `/database` page includes both backends:
- `PostgreSQL` tab (default, primary workflow)
- `SQLite` tab (read-only local query/table preview)

### 6. Reset/Initialize PG Schema
```powershell
uv run python -m src.db.migrations.create_foreclosures --dsn <postgres-dsn>
```

## Technical Stack & Rules

### Package Management
- **ONLY use `uv`** for package management. Never use `pip` or `poetry`.
- Run scripts with `uv run python <script.py>`

### Data Processing
- **ONLY use `Polars`** for DataFrames. Never use `pandas`.
- **Store bulk data as Parquet files** for efficient columnar storage.

### Database: SQLite & PostgreSQL

**SQLite** is used for local operational data, pipeline state, and the active auction window.
**PostgreSQL** is used for bulk data, historical analysis, and cross-source linking (Clerk, Sales, Sunbiz).

**STRICT RULE:** All local pipeline state must be stored in **SQLite** (`data/property_master_sqlite.db`).
**STRICT RULE:** All bulk datasets and complex historical analytics must be stored in **PostgreSQL**.

#### PostgreSQL Extensions (Installed)
- `pg_search`: Full-text/hybrid search extension for fast property and party search workflows.
- `pg_trgm`: Trigram similarity + GIN support for fuzzy name/address matching.
- `fuzzystrmatch`: Phonetic/string-distance helpers for entity resolution.
- `citext`: Case-insensitive text type for equality/grouping on names.
  Columns now using `citext`: `foreclosures.sold_to`, `foreclosures_history.sold_to`, `sunbiz_flr_parties.name`, `sunbiz_entity_parties.party_name`.
- `unaccent`: Accent/diacritic normalization support (useful with `lower(...)` + `pg_trgm`).
- `pgcrypto`: Hashing and crypto-safe random/UUID helper functions for IDs/dedup workflows.

#### SQLite Best Practices (WAL Mode)
The pipeline uses SQLite in Write-Ahead Logging (WAL) mode to handle concurrent reads and writes.

**ALWAYS DO THIS:**
```python
# GOOD - Use the provided PropertyDB or DatabaseWriter
with PropertyDB() as db:
    db.upsert_auction(data)
```

### Tooling Requirements

**Only `uv`, `ruff`, and `ty` are approved developer tools.** No pip, flake8, or mypy.

## Setup
### Quick Start (Linux/WSL/MacOS)
We provide a `Makefile` to streamline the setup process.

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/PowerInterest/HillsInspector
    cd HillsInspector
    ```

2.  **Run Setup**:
    This will install system dependencies, sync Python packages, and install Playwright browsers.
    ```bash
    make setup
    ```

### Manual Setup / Windows Powershell

1.  **Install `uv`**:
    Follow instructions at [https://github.com/astral-sh/uv](https://github.com/astral-sh/uv)

2.  **Install Dependencies**:
    ```powershell
    uv sync
    ```

3.  **Install Playwright Browsers**:
    ```powershell
    uv run playwright install chromium
    ```

4.  **Verify Installation**:
    ```powershell
    uv run Controller.py --help
    ```

## Documentation Index

The `docs/` folder contains comprehensive guides and technical design artifacts, systematically broken down into domains:

### 📐 Architecture & Infrastructure
- [Ingestion Guide](docs/guides/INGESTION_GUIDE.md) - End-to-end data pipeline logic and ingestion states.

### ⚖️ Real Estate Domain Logic
- [Encumbrance Audit Buckets](docs/domain/ENCUMBRANCE_AUDIT_BUCKETS.md) - Taxonomy for separating ORI discovery gaps, survival-risk gaps, and identity/title-break gaps on active foreclosures.
- [Lien Survival Analysis](docs/domain/LIEN_SURVIVAL.md) - Exact logic for modeling Florida statues covering extinguished/surviving liens and foreclosures.
- [Auction Expiration Rules](docs/domain/AUCTION_EXPIRE.md) - Modeling property forfeiture and expiration mechanics.
- [Chain of Title Recovery](docs/domain/TITLE_CHAIN_RECOVERY.md) - The mechanism of scraping and verifying a complete chain of title.
- [NOC & Permit Linking](docs/domain/NOC_PERMIT_LINKING.md) - How we link notices of commencement to active building permits.
- [Party Matching Strategy](docs/domain/PARTY_MATCHING_STRATEGY.md) - Entity resolution logic for fuzzy matching property owners across completely different data silos.
- [Legal Issues Overview](docs/domain/LEGAL_ISSUES.md) - Real estate law nuances codified into algorithms.
- [Auction Buyer Resolution](docs/domain/AUCTION_BUYER_RESOLUTION.md) - Utilizing post-auction Property Appraiser Deeds to backwards resolve unknown auction winners.

### 🌐 External Systems & Scraping
- [Deep Search Implementation](docs/DEEP_SEARCH_IMPLEMENTATION.md) - Bypassing ORI rate limits and complex search logic.
- [Hyland PAV NOC Discovery](docs/external/HYLAND_PAV_NOC_DISCOVERY.md) - Live query IDs, keyword fields, and the search order that actually finds NOCs in the Clerk's Hyland public-access stack.
- [Sunbiz Data Dictionary](docs/external/SUNBIZ_DATA_DICTIONARY.md) - Layout definition and tables for the Florida Division of Corporations bulk open datasets.
- [Tax Data Research](docs/external/TAX_DATA_RESEARCH.md) - Scraping instructions for the DOR (Department of Revenue) property millage layers.
- [Case Fallback Scraping](docs/domain/CASE_FALLBACK.md) - Fail-safe mechanisms when primary URLs vanish.

### 📖 Guides
- [Operations Runbook](docs/guides/RUNBOOK.md) - Standard operational procedures and recurring scripts.
- [ORI Lis Pendens Recovery](docs/guides/ORI_LIS_PENDENS_RECOVERY.md) - How active foreclosure LP gaps are retried without adding new PostgreSQL schema.
- [Foreclosure Identifier Repair](docs/guides/FORECLOSURE_IDENTIFIER_REPAIR.md) - How the pipeline repairs non-null but invalid HCPA straps from folio-backed parcel data.
- [Tampa Permit Value And Enforcement](docs/guides/TAMPA_PERMIT_VALUE_AND_ENFORCEMENT.md) - Why enforcement rows stay stored, but are excluded from permit-gap scoring, and how Tampa valuation parsing works.
- [Scheduled Jobs](docs/guides/SCHEDULED_JOBS.md) - PG-controlled job config/run tracking with cron-triggered Python workers.
- [Scheduled Jobs Walkthrough](docs/guides/WALKTHROUGH_SCHEDULED_JOBS.md) - Notes captured from the initial bulk-job scheduler integration pass.
- [Clerk Civil Alpha Merge](docs/CLERK_CIVIL_ALPHA_MERGE.md) - Merging 1958-present civil alpha index into normalised clerk tables with automated download.

## Roadmap / TODO
*   **System Identification**: The backend is **Hyland OnBase**.
    *   *Action*: Leverage [OnBase Documentation](https://support.hyland.com/r/OnBase/Public-Sector-Constituency-Web-Access/English/Foundation-22.1/Public-Sector-Constituency-Web-Access/Configuration/Front-End-Client-Configuration/Search-Panel-Settings/Configuring-Custom-Queries/Predefine-Keyword-Values-to-Search/Dynamic-Keyword-Values) to discover more advanced search capabilities and potential API endpoints.
*   **Permit Analysis**: Integrate `HillsGovHubScraper` to verify NOCs against actual permits.
*   **Court Case Search**: Implement scraping for CQIDs 324-348 to find foreclosure and probate cases.
*   **Avoid rework**: If data (judgments, liens, geocodes) already exists, skip reprocessing; only fill gaps (e.g., geocode missing lat/lon, skip PDFs already extracted).

## 1. Core Package Management & Runtime
**STRICT RULE:** Do **NOT** use `pip`. Do **NOT** use `poetry`.
We exclusively use **[uv](https://github.com/astral-sh/uv)** for all Python package management.

* **Why:** Instant dependency resolution, Rust-based speed, and seamless lockfile management.
* **Commands:**
    * `uv sync` - Install dependencies from `uv.lock`.
    * `uv add <package>` - Add a new library.
    * `uv run <script.py>` - Run a script within the virtual environment.

## 2. Web Server & UI (Zero-JS Philosophy)
**STRICT RULE:** The web interface must function without client-side JavaScript.
* **Framework:** `FastAPI`
* **Templating:** `Jinja2` (Server-Side Rendering).
* **Interaction Model:**
    * Use standard HTML `<form>` elements (POST/GET) for all data actions.
    * Use standard `<a>` links for navigation.
    * **No React, Vue, or SPAs.**
    * *exception:* `HTMX` is permitted **only** if strictly necessary to avoid full page reloads for minor updates, but standard HTML is the priority.

## 3. Data Storage & Processing
**STRICT RULE:** Use **SQLite** for local operational state and **PostgreSQL** for bulk/analytical data.
**STRICT RULE:** Do **NOT** use `pandas`. Use `polars`.

### **Dataframes: Polars**
* **Tool:** `polars`
* **Why:** Multithreaded, lazy evaluation, and handles larger-than-memory datasets efficiently.

### **Databases**
* **Operational:** SQLite (Version 3.40+) with WAL mode enabled.
* **Analytical:** PostgreSQL (Version 15+) for high-volume historical and clerk data.
* **Storage Pattern:**
    * Raw scrapes -> Saved as `Parquet` or `JSON` (structured).
    * Active Pipeline -> `property_master_sqlite.db`.
    * Historical/Bulk -> PostgreSQL.

## 4. Quality Assurance (Linting & Typing)
We enforce strict code quality using the [Astral](https://astral.sh) suite.

### **Linter & Formatter: Ruff**
* **Tool:** `ruff`
* **Config:** `pyproject.toml`
* **Usage:**
    * `uv run ruff check .` (Lint)
    * `uv run ruff format .` (Format)

### **Type Checking: Ty**
* **Tool:** `ty` (Astral's new type checker)
* **Why:** Significantly faster than MyPy.
* **Usage:**
    * `uv run ty check`

## 5. Logging
* **Tool:** `loguru`
* **Why:** Thread-safe, colorized output for Windows, and removes standard logging boilerplate.
* **Config:**
    ```python
    from loguru import logger
    logger.add("logs/inspector_{time}.log", rotation="10 MB")
    ```

## 6. Scraping & Browser Automation
* **Tool:** `playwright` (Python Sync/Async API)
* **Visual Extraction & OCR:** Qwen-VL via `src/services/vision_service.py` hitting `http://10.10.1.5:6969`
  - Model: `Qwen/Qwen3-VL-8B-Instruct`
  - Used for: Screenshot parsing (Accela/Realtor.com), PDF text extraction (ORI documents)
  - **Note:** Do NOT use EasyOCR. All vision/OCR tasks go through the Qwen service.



## 3. Technical Stack (Strict)
* **Language:** Python 3.12
* **Package Manager:** `uv` (No pip/poetry).
* **Data Storage:** `SQLite` (Operational) + `PostgreSQL` (Analytical) + `Parquet` (Raw).
* **Data Processing:** `Polars` (Lazyframes). **No Pandas.**
* **Web Framework:** `FastAPI` + `Jinja2` (SSR) + `HTMX` (Interactivity). **No React/SPA.**
* **Scraping:** `Playwright` (Browser Automation) + `playwright-stealth`.
* **AI/Vision:** `Qwen-VL` via VisionService (Visual extraction & OCR). No EasyOCR.
