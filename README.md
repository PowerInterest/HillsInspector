# Hillsborough County Property Inspector

A data ingestion and analysis pipeline for Hillsborough County real estate, focusing on foreclosure and tax deed auctions. This tool aggregates data from multiple sources to help assess property equity and risk. https://publicrec.hillsclerk.com/Civil/

## 1. Project Overview & Structure
* `src/scrapers/*`: get the data, and then call a service to store what they found
* `src/services/*`: Analyze the data, transforming it and writing it into the database
* `src/db/*`: has everything to start a new database, and db scripts
* `src/ingest/*`: has to do with bringing in data before we start scraping websites
* `docs/*`: is where we keep documentation for the project
* `data/properties/*`: holds all the raw data (pictures, parquet files, json, pdfs, etc.)
* `app/*`: holds the web application code
* `logs/`: one log file for the whole project using loguru
* `utils/`: utility functions for the project
* `Controller.py`: the PG-first pipeline entry point for the project

## 2. Setup

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

1.  **Install `uv`**: Follow instructions at [https://github.com/astral-sh/uv](https://github.com/astral-sh/uv)
2.  **Install Dependencies**: `uv sync`
3.  **Install Playwright Browsers**: `uv run playwright install chromium`
4.  **Verify Installation**: `uv run Controller.py --help`

## 3. Usage

**Pipeline startup is handled via `Controller.py` (PG-first).**

*   **Run Full Pipeline**: Runs Phase A (bulk refresh) + Phase B (per-auction enrichment).
    `uv run Controller.py` (enforces schema sync at startup by running `alembic upgrade head`)
*   **Quick Sanity Run**: Runs the controller with narrow limits.
    `uv run Controller.py --auction-limit 5 --judgment-limit 5 --ori-limit 5 --survival-limit 5 --limit 5`
*   **Run Phase A Only (bulk refresh)**:
    `uv run Controller.py --skip-auction-scrape --skip-judgment-extract --skip-identifier-recovery --skip-ori-search --skip-encumbrance-extraction --skip-survival --skip-encumbrance-audit --skip-encumbrance-recovery --skip-final-refresh --skip-market-data`
*   **Run Phase B Only (enrichment)**:
    `uv run Controller.py --skip-hcpa --skip-clerk-bulk --skip-clerk-criminal --skip-clerk-civil-alpha --skip-nal --skip-flr --skip-sunbiz-entity --skip-county-permits --skip-tampa-permits --skip-single-pin-permits --skip-foreclosure-refresh --skip-trust-accounts --skip-title-chain --skip-title-breaks --skip-market-data`
*   **Start Web Server**:
    `uv run python -m app.web.main`
    (Start with a public `ngrok` tunnel: `uv run python -m app.web.main --ngrok`)
*   **Reset/Initialize PG Schema**:
    `uv run python -m src.db.migrations.create_foreclosures --dsn <postgres-dsn>`
    `uv run alembic upgrade head`

## 4. Technical Stack & Rules

*   **Language**: Python 3.12
*   **Package Manager**: `uv` **ONLY**. Never use `pip` or `poetry`.
*   **Data Processing**: `Polars` **ONLY** for DataFrames. Never use `pandas`. Store bulk data as Parquet.
*   **Web Framework**: `FastAPI` + `Jinja2` (SSR). **No React/SPA.** HTMX is permitted only if strictly necessary. Standard HTML forms and links are the priority (Zero-JS Philosophy).
*   **Database**: PostgreSQL (Version 15+) single runtime database for pipeline state and analytics. Migrations are forward-only.
    *   *Extensions*: `pg_search`, `pg_trgm`, `fuzzystrmatch`, `citext`, `unaccent`, `pgcrypto`
*   **Scraping**: `Playwright` (Browser Automation) + `playwright-stealth`
*   **AI/Vision & OCR**: Qwen-VL (`Qwen/Qwen3-VL-8B-Instruct`) via VisionService. **No EasyOCR.**
*   **QA & Tooling Requirements**:
    *   **Linter/Formatter**: `ruff` (`uv run ruff check .` / `uv run ruff format .`)
    *   **Type Checker**: `ty` (`uv run ty check`)
    *   *No flake8 or mypy.*
*   **Logging**: `loguru` (`logs/inspector_{time}.log`)

## 5. Documentation Index

The `docs/` folder contains comprehensive guides and technical design artifacts, systematically broken down into domains:

### 📐 Architecture & Infrastructure
- [Ingestion Guide](docs/guides/INGESTION_GUIDE.md) - End-to-end data pipeline logic and ingestion states.
- [LLM Extraction Schema Contract](docs/domain/LLM_EXTRACTION_SCHEMA_CONTRACT.md) - Hard JSON-schema and validation rules for OCR-to-LLM document extraction.
- [Final Judgment Text-First Extraction](docs/domain/FINAL_JUDGMENT_TEXT_EXTRACTION.md) - Why final judgments use Tesseract OCR text as the primary extraction source.

### ⚖️ Real Estate Domain Logic
- [Encumbrance Audit Buckets](docs/domain/ENCUMBRANCE_AUDIT_BUCKETS.md) - Taxonomy for separating ORI discovery gaps, survival-risk gaps, and identity gaps.
- [Lien Survival Analysis](docs/domain/LIEN_SURVIVAL.md) - Exact logic for modeling Florida statues covering extinguished/surviving liens and foreclosures.
- [Per-Foreclosure Survival Persistence](docs/domain/PER_FORECLOSURE_SURVIVAL.md) - Shared-strap survival results keyed by `(foreclosure_id, encumbrance_id)`.
- [Pipeline Quality Thresholds](docs/domain/PIPELINE_QUALITY_THRESHOLDS.md) - Hard gates and diagnostics for judging title-chain and encumbrance quality.
- [Auction Expiration Rules](docs/domain/AUCTION_EXPIRE.md) - Modeling property forfeiture and expiration mechanics.
- [Chain of Title Recovery](docs/domain/TITLE_CHAIN_RECOVERY.md) - Scraping and verifying a complete chain of title.
- [NOC & Permit Linking](docs/domain/NOC_PERMIT_LINKING.md) - Notice of commencement linking to active building permits.
- [ORI Property Matching](docs/domain/ORI_PROPERTY_MATCHING.md) - ORI document filter to prevent cross-property contamination.
- [ORI SQL Parameter Typing](docs/domain/ORI_SQL_PARAMETER_TYPING.md) - Preventing psycopg `AmbiguousParameter` skips.
- [Workflow Retry Contracts](docs/domain/WORKFLOW_RETRY_CONTRACTS.md) - Persistence and retry semantics for identifier recovery and search.
- [Upsert Source Tracking](docs/domain/UPSERT_SOURCE_TRACKING.md) - Change-log persistence and priority-aware market upserts.
- [Party Matching Strategy](docs/domain/PARTY_MATCHING_STRATEGY.md) - Entity resolution logic for fuzzy matching property owners.
- [Legal Issues Overview](docs/domain/LEGAL_ISSUES.md) - Real estate law nuances codified into algorithms.
- [Auction Buyer Resolution](docs/domain/AUCTION_BUYER_RESOLUTION.md) - Utilizing post-auction Property Appraiser Deeds to backwards resolve unknown auction winners.
- [Encumbrance Linking](docs/domain/ENCUMBRANCE_LINKING.md) - Satisfaction, modification, and lifecycle document linking algorithms.

### 🌐 External Systems & Scraping
- [Deep Search Implementation](docs/DEEP_SEARCH_IMPLEMENTATION.md) - Bypassing ORI rate limits and complex search logic.
- [Hyland PAV NOC Discovery](docs/external/HYLAND_PAV_NOC_DISCOVERY.md) - Search order and keywords for finding NOCs.
- [Sunbiz Data Dictionary](docs/external/SUNBIZ_DATA_DICTIONARY.md) - Layout definition and tables for Florida Division of Corporations bulk open datasets.
- [Tax Data Research](docs/external/TAX_DATA_RESEARCH.md) - Scraping instructions for the DOR property millage layers.
- [Case Fallback Scraping](docs/domain/CASE_FALLBACK.md) - Fail-safe mechanisms when primary URLs vanish.

### 📖 Guides
- [Operations Runbook](docs/guides/RUNBOOK.md) - Standard operational procedures and recurring scripts.
- [Encumbrance Audit Web UI](docs/guides/ENCUMBRANCE_AUDIT_WEB_UI.md) - Read-only implementation guide for surfacing encumbrance audit issues.
- [Municipal Utility Lien Plan](docs/guides/MUNICIPAL_UTILITY_LIEN_PLAN.md) - Provider-aware closure plan for municipal-lien risk handling.
- [Plant City + Temple Terrace Permit Expansion](docs/guides/PERMIT_EXPANSION_PLANT_CITY_TEMPLE_TERRACE.md) - Jurisdiction-aware municipal permit routing.
- [ORI Lis Pendens Recovery](docs/guides/ORI_LIS_PENDENS_RECOVERY.md) - How active foreclosure LP gaps are retried.
- [Foreclosure Identifier Repair](docs/guides/FORECLOSURE_IDENTIFIER_REPAIR.md) - How the pipeline repairs invalid HCPA straps.
- [Tampa Permit Value And Enforcement](docs/guides/TAMPA_PERMIT_VALUE_AND_ENFORCEMENT.md) - Why enforcement rows stay stored but excluded from permit-gap scoring.
- [Scheduled Jobs](docs/guides/SCHEDULED_JOBS.md) - PG-controlled job config/run tracking.
- [Scheduled Jobs Walkthrough](docs/guides/WALKTHROUGH_SCHEDULED_JOBS.md) - Initial bulk-job scheduler integration pass.
- [Clerk Civil Alpha Merge](docs/CLERK_CIVIL_ALPHA_MERGE.md) - Merging 1958-present civil alpha index.

## 6. Code Fixes & Improvements

This section tracks detailed documentation, post-mortem analyses, and system audit resolutions for code fixes and improvements that have been applied.

### System Audits
- [Massive Audit Resolution (2026-03-10)](docs/AUDIT_FIXES_2026_03_10.md) - Resolution status for High/Medium findings from `docs/MASSIVE_AUDIT.md`, plus triage guidance for Low findings.
- [System Audit Final Pass (2026-03-08)](docs/AUDIT_FIXES_2026_03_08_FINAL.md) - Final 3 deferred issues: title chain gap metric, judgment loader unification, zestimate UI labels.
- [System Audit Fixes (2026-03-08)](docs/AUDIT_FIXES_2026_03_08.md) - 11 confirmed bugs fixed from multi-reviewer system audit (JSONB key typos, COALESCE direction bugs, encumbrance misclassification, etc.).

### Post-Mortems & Session Notes
- [Controller Run Post-Mortem (2026-03-10)](docs/STILL.md) - Persistence-focused analysis of the 2026-03-10 controller run, including why `identifier_recovery`, `ori_search`, and `title_breaks` failed to improve downstream data quality.

## 7. Roadmap / TODO
*   **System Identification**: The backend is **Hyland OnBase**. Leverage [OnBase Documentation](https://support.hyland.com/r/OnBase/Public-Sector-Constituency-Web-Access/English/Foundation-22.1/Public-Sector-Constituency-Web-Access/Configuration/Front-End-Client-Configuration/Search-Panel-Settings/Configuring-Custom-Queries/Predefine-Keyword-Values-to-Search/Dynamic-Keyword-Values) to discover more advanced search capabilities and potential API endpoints.
*   **Permit Analysis**: Integrate `HillsGovHubScraper` to verify NOCs against actual permits.
*   **Court Case Search**: Implement scraping for CQIDs 324-348 to find foreclosure and probate cases.
*   **Avoid rework**: If data (judgments, liens, geocodes) already exists, skip reprocessing; only fill gaps.
