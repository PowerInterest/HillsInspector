# Hillsborough County Property Inspector

A data ingestion and analysis pipeline for Hillsborough County real estate, focusing on foreclosure and tax deed auctions. This tool aggregates data from multiple sources to help assess property equity and risk.

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
    main.py  the main entry point for the project


## Usage

**All operations are handled via the `main.py` entry point.**

### 1. Run a Quick Test
Runs the pipeline for the next 5 auctions to verify functionality.
```powershell
uv run main.py --test
```

### 2. Run Full Update
Runs the complete pipeline: scraping auctions (next 60 days), downloading judgments, analyzing liens, and enriching property data.
```powershell
uv run main.py --update
```

### 2.5. Process Limited Date Range
To process only auctions within a specific date range, use `--start-date` and `--end-date`:
```powershell
uv run python main.py --update --start-date 2025-12-18 --end-date 2025-12-18
```
This is useful for testing a single day's auctions or reprocessing a specific period.

### 3. Start Web Server
Launches the local web dashboard to view results.
```powershell
uv run main.py --web
```

### 4. Reset Database
Archives the existing database and creates a fresh one.
```powershell
uv run main.py --new
```

## Technical Stack & Rules

### Package Management
- **ONLY use `uv`** for package management. Never use `pip` or `poetry`.
- Run scripts with `uv run python <script.py>`

### Data Processing
- **ONLY use `Polars`** for DataFrames. Never use `pandas`.
- **Store bulk data as Parquet files** for efficient columnar storage.

### Database: DuckDB

**CRITICAL**: DuckDB is a **columnar OLAP database**, NOT a row-by-row OLTP database like PostgreSQL or SQLite.

Operations that seem fast on small datasets become **catastrophically slow** at scale.

**NEVER DO THIS:**
```python
# BAD - Row-by-row inserts are extremely slow
for row in data:
    conn.execute("INSERT INTO table VALUES (?, ?)", [row.a, row.b])

# BAD - executemany is still row-by-row under the hood
conn.executemany("INSERT INTO table VALUES (?, ?)", rows)
```

**ALWAYS DO THIS:**
```python
# GOOD - Register DataFrame and bulk insert
conn.register("df_temp", polars_df)
conn.execute("INSERT INTO table SELECT * FROM df_temp")

# GOOD - Direct Parquet read
conn.execute("INSERT INTO table SELECT * FROM 'data.parquet'")

# GOOD - COPY for CSVs
conn.execute("COPY table FROM 'data.csv'")
```

**For Bulk Updates - Use Polars register + UPDATE FROM:**
```python
# GOOD - Bulk update from DataFrame
conn.register("updates_df", polars_df)
conn.execute("""
    UPDATE target_table SET
        column1 = u.column1,
        column2 = u.column2
    FROM updates_df u
    WHERE target_table.id = u.id
""")
```

### Tooling Requirements

**Only `uv`, `ruff`, and `ty` are approved developer tools.** No pip, flake8, or mypy.

After modifying any file:
1. Run `uv run ruff check <path>` (plus `--fix` when safe)
2. Run `uv run ty check <path>`
3. Resolve all issues before committing
4. Document exact commands in PR description

## Setup

1.  **Install Dependencies**:
    ```bash
    uv sync
    ```

2.  **Environment**:
    - Python 3.12+
    - Windows WSL Ubuntu 24.04.5

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
**STRICT RULE:** Do **NOT** use or suggest `sqlite`. All local storage must be **DuckDB**.
**STRICT RULE:** Do **NOT** use `pandas`. Use `polars`.

### **Dataframes: Polars**
* **Tool:** `polars`
* **Why:** Multithreaded, lazy evaluation, and handles larger-than-memory datasets efficiently.

### **Database: DuckDB**
* **Tool:** `duckdb` (Version 1.1+)
* **Constraint:** All SQL queries must be compatible with **DuckDB 1.1+**.
* **Why:** Serverless SQL analytics engine that allows querying `.csv`, `.parquet`, or `.json` files directly.
* **Storage Pattern:**
    * Raw scrapes -> Saved as `Parquet` or `JSON` (structured).
    * Analytical tables -> `property_master.db` (DuckDB persistent file).

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


## 7. Setup Commands (Windows)

```powershell
# 1. Clone Repo
git clone [https://github.com/PowerInterest/HillsInspector](https://github.com/PowerInterest/HillsInspector)
cd HillsInspector

# 2. Initialize uv (if starting fresh)
uv init

# 3. Add Core Dependencies
uv add polars duckdb loguru playwright tenacity python-dotenv fastapi uvicorn jinja2 python-multipart

# 4. Add Dev Dependencies
uv add --dev ruff ty playwright-stealth tqdm

# 5. Install Playwright Browsers
uv run playwright install chromium

# 6. Run Checks
uv run ruff check .
uv run ty check

## 3. Technical Stack (Strict)
* **Language:** Python 3.12
* **Package Manager:** `uv` (No pip/poetry).
* **Data Storage:** `DuckDB` 1.4+ (Analytical DB) + `Parquet` (Raw storage). **No SQLite.**
* **Data Processing:** `Polars` (Lazyframes). **No Pandas.**
* **Web Framework:** `FastAPI` + `Jinja2` (SSR) + `HTMX` (Interactivity). **No React/SPA.**
* **Scraping:** `Playwright` (Browser Automation) + `playwright-stealth`.
* **AI/Vision:** `Qwen-VL` via VisionService (Visual extraction & OCR). No EasyOCR.



claude --dangerously-skip-permissions
