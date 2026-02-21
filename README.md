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
uv run Controller.py --skip-auction-scrape --skip-judgment-extract --skip-ori-search --skip-survival --skip-final-refresh
```

### 4. Run Phase B Only (enrichment)
```powershell
uv run Controller.py --skip-hcpa --skip-clerk-bulk --skip-nal --skip-flr --skip-sunbiz-entity --skip-county-permits --skip-tampa-permits --skip-foreclosure-refresh --skip-trust-accounts --skip-title-chain --skip-market-data
```

### 5. Start Web Server
Launches the local web dashboard to view results.
```powershell
uv run python -m app.web.main
```
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

## Final Judgment Retrieval (current approach)
- Auction pages provide a “Case #” link that redirects to OnBase Instrument Search (`CQID=320` with `OBKey__1006_1=<instrument>`).
- The scraper captures the resulting Document ID from `PAVDirectSearch` and downloads the Final Judgment PDF directly—no HOVER scraping required.
- See `docs/ONBASE_FINDINGS.md` and `docs/FINAL_JUDGMENT_EXTRACTION.md` for details.

## Map & Geocoding
- The web dashboard map expects `parcels` to have `latitude` and `longitude` columns populated.
- Add coords by running `uv run python scripts/geocode_missing_parcels.py` (uses Nominatim with local caching) or integrate your own geocoder.
- The map API is exposed at `/api/map-auctions` and will skip properties without coordinates.

## Deep Search Strategy
We have implemented a comprehensive "Deep Search" strategy for the Official Records Index (ORI) to bypass rate limits and ensure complete chain of title analysis.
See [DEEP_SEARCH_IMPLEMENTATION.md](DEEP_SEARCH_IMPLEMENTATION.md) for details on:
*   Direct Search Endpoints (CQIDs)
*   Chain of Title Analysis Flow
*   Name Change Detection (Marriage/Divorce)
*   NOC & Permit Matching

## Bot Detection
Some sources (HOVER, Realtor.com) have aggressive bot detection. The scrapers use a stealth User-Agent, but if you encounter blocking:
1.  Try running in **headed mode** (set `headless=False` in the scraper code).
2.  Ensure your IP is not blacklisted.

## Lien Survival Analysis

The `LienSurvivalAnalyzer` (`src/services/lien_survival_analyzer.py`) determines which liens will survive the **upcoming** foreclosure sale based on Florida law and the foreclosure type.

### Survival Status Values

| Status | Meaning |
|--------|---------|
| **SURVIVED** | Will survive the upcoming foreclosure sale (senior liens, superpriority, first mortgage in HOA foreclosure) |
| **EXTINGUISHED** | Will be wiped out by the upcoming sale (junior liens) |
| **EXPIRED** | Already expired by statute of limitations (e.g., mechanic's lien >1 year, judgment >10 years) |
| **SATISFIED** | Already paid off/released (satisfaction recorded) |
| **HISTORICAL** | From a prior ownership period - already wiped by a previous foreclosure |
| **FORECLOSING** | This is the lien being foreclosed (the plaintiff's lien) |

### Key Logic

1. **Historical Detection**: Liens recorded before the current owner's acquisition date are marked `HISTORICAL`. These were already wiped by a prior foreclosure that transferred title.

2. **Foreclosing Party Detection**: Liens where the creditor matches the plaintiff (foreclosing party) are marked `FORECLOSING`.

3. **Superpriority Liens**: Tax liens, IRS liens, municipal liens, utility liens, and code enforcement liens **always survive** any foreclosure.

4. **Foreclosure Type Rules**:
   - **HOA/COA Foreclosure**: First mortgage **SURVIVES** per Florida Safe Harbor (Fla. Stat. 720.3085 / 718.116). Junior liens are `EXTINGUISHED`.
   - **First Mortgage Foreclosure**: Everything junior (second mortgages, HOA liens, judgments) is `EXTINGUISHED`.
   - **Tax Deed Sale**: Everything is `EXTINGUISHED` except federal tax liens.

5. **Expiration Rules** (Florida Statutes):
   - Mechanic's/Construction Liens: 1 year to file suit (Fla. Stat. 713.22)
   - HOA Claim of Lien: 1 year to file suit (Fla. Stat. 720.3085)
   - Judgment Liens: 10 years, renewable to 20 (Fla. Stat. 55.10)
   - Code Enforcement: 20 years (Fla. Stat. 162.09)
   - Mortgages: 5 years after maturity (~35 years total)

### Example Analysis

For an **HOA foreclosure** on a property acquired in 2023:
- 1996-2002 mortgages from prior owner: **HISTORICAL** (wiped by 2003 foreclosure)
- 2023 first mortgage ($211k): **SURVIVED** (Florida Safe Harbor)
- 2023-2025 HOA liens: **FORECLOSING** (plaintiff's liens)

## Auction Buyer Resolution (hcpa_allsales)

The auction website only shows "3rd Party Bidder" — never the real buyer's name. We resolve the real buyer from `hcpa_allsales` (2.4M property transfer records in PostgreSQL) by looking at the **first deed recorded after the auction date** for the same folio.

The key insight is that **different deed types put the auction winner on different sides of the transfer**:

| Deed Type | Code | Winner is | Why |
|-----------|------|-----------|-----|
| Certificate of Title | **CT** | **grantee** | Clerk issues certificate directly **to** the auction winner; grantor is the old foreclosed homeowner |
| Certificate of Deed | **CD** | **grantee** | Same as CT — Clerk-issued certificate **to** the winner |
| Warranty Deed | **WD** | **grantor** | Auction winner already owns the property, now **selling** it |
| Quit Claim Deed | **QC** | **grantor** | Same — winner is **selling/transferring** out |
| Transfer | **TR** | **grantor** | Same — winner is **transferring** |
| Fee / Final Deed | **FD** | **grantor** | Same — winner is **selling** |
| Deed (generic) | **DD** | **grantor** | Same — winner is **selling** |

**How it works in practice:**
- After a foreclosure sale, the Clerk issues a CT or CD to the auction winner (avg 74 days after auction). The `grantee` on that deed IS the buyer.
- If no CT/CD appears in `hcpa_allsales` (HCPA doesn't always record these), we fall back to the first WD/QC/etc., where the `grantor` is the person who bought at auction and is now reselling.
- This logic runs automatically via a PostgreSQL trigger (`trg_resolve_buyer`) on every INSERT/UPDATE to `historical_auctions`. It's also available as `HistoryService.backfill_buyers_from_hcpa()` for manual runs.

**Coverage:** ~80% of auctions get a real buyer name. The remaining ~20% have no post-auction deed in `hcpa_allsales` (property not yet resold, or folio data gap).

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



claude --dangerously-skip-permissions
