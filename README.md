# Hillsborough County Property Inspector

A data ingestion and analysis pipeline for Hillsborough County real estate, focusing on foreclosure and tax deed auctions. This tool aggregates data from multiple sources to help assess property equity and risk. https://publicrec.hillsclerk.com/Civil/

## Mission
Build and operate a single PostgreSQL-first foreclosure intelligence pipeline that answers:
- Is this property investable at auction?
- What encumbrances survive the sale?
- What is realistic net equity after judgment + surviving debt?

## Canonical Pipeline Architecture

The architecture utilizes a PostgreSQL-backed system using scheduled `cron` jobs for bulk data ingestion and a focused pipeline (`Controller.py`) for specific per-auction enrichment. 

### Scheduled Bulk Refresh (Background Jobs)
Background jobs routinely sync massive county feeds into PostgreSQL. These are tracked via `pipeline_job_config` and `pipeline_job_runs`.
| Job / Step Name | Primary Outputs |
|---|---|
| `hcpa_bulk` | `hcpa_bulk_parcels`, `hcpa_allsales`, related HCPA tables |
| `clerk_bulk` | `clerk_civil_cases`, `clerk_civil_parties`, events/index tables |
| `dor_nal_annual` | `dor_nal_parcels` |
| `sunbiz_daily` | `sunbiz_raw_records` |
| `sunbiz_flr_quarterly` | `sunbiz_flr_*` |
| `sunbiz_entity_quarterly` | `sunbiz_entity_*` |
| `auction_results` | `auction_status`, `winning_bid`, `sold_to`, `buyer_type`, `archived_at` |

### Per-Auction Enrichment Pipeline (Controller.py)
The `Controller.py` workflow orchestrates complex, targeted data extraction, joining data across silos for specific auctions.
| Job / Step Name | Primary Outputs | Execution Mode |
|---|---|---|
| `county_permits` | `county_permits` | controller step |
| `tampa_permits` | `tampa_accela_records` | controller step |
| `foreclosure_refresh` | `foreclosures` hub refresh | controller step |
| `trust_accounts` | `TrustAccount`, `TrustAccountSummary` | controller step |
| `title_chain` | `foreclosure_title_chain`, `foreclosure_title_summary` | controller step |
| `auction_scrape` | Refreshed auction rows in `foreclosures` | controller step |
| `judgment_extract` | `foreclosures.judgment_data`, `step_judgment_extracted` | controller step |
| `ori_search` | `ori_encumbrances`, `step_ori_searched` | controller step |
| `survival_analysis` | `ori_encumbrances.survival_status` | controller step |
| `encumbrance_audit` | Read-only audit buckets + signal coverage over foreclosure/ORI data | controller step |
| `encumbrance_recovery` | Targeted ORI retries, mortgage extraction reruns, survival reruns | controller step |
| `final_refresh` | Recomputed foreclosure hub metrics | controller step |
| `market_data` | `property_market` | background worker |

The encumbrance audit/recovery loop is documented in [docs/guides/ENCUMBRANCE_AUDIT_RECOVERY_LOOP.md](docs/guides/ENCUMBRANCE_AUDIT_RECOVERY_LOOP.md).

## Definition of Done: Completeness Gates
A pipeline run is successful **only** if all data completeness gates are met. Step execution without exceptions is not enough.
The explicit, official source of truth for what is considered completely "done" is measured by the reporting in **`src/tools/db_audit.py`**. You must consult `CLAUDE.md`, `AGENTS.md`, and the `db_audit.py` script output to verify if the pipeline is healthy.

**Target gates:**
- Final Judgment PDFs: >= 90% of active foreclosures
- Extracted judgment data: >= 90% of active foreclosures with PDFs
- Chain coverage: >= 80% of active foreclosures with judgment data
- Complete chain: >= 90% of chained foreclosures (terminal link, no gaps)
- Lis pendens coverage: >= 90% of judged foreclosures have LP (ori_encumbrances or title events)
- Encumbrance coverage: >= 80% of active foreclosures with judgment data and strap
- Survival coverage: >= 80% of active foreclosures with judgment data and strap

## Usage & Runbook

**Pipeline startup is handled via `Controller.py`.**

### 1. Run Full Integration
Runs the full enrichment pipeline for the active auction scope.
```powershell
uv run Controller.py
```

### 2. Quick Sanity Run
Runs the controller with narrow limits for auction/judgment/ORI/survival steps.
```powershell
uv run Controller.py --auction-limit 5 --judgment-limit 5 --ori-limit 5 --survival-limit 5 --limit 5
```

### 3. Run Scheduled Jobs Manually
```powershell
uv run python -m src.tools.run_scheduled_job --job clerk_bulk
uv run python -m src.tools.run_scheduled_job --job auction_results
```

### 4. Start Web Server
Launches the local web dashboard to view results.
```powershell
uv run python -m app.web.main
```

### 5. Reset/Initialize PG Schema
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
**PostgreSQL** is the canonical engine for bulk data, historical analysis, and cross-source linking. **SQLite** is used sparingly for reading snapshot configurations or extremely light local locking/state that does not belong in the central PG orchestrator.
**STRICT RULE:** All bulk datasets and complex historical analytics must be stored in **PostgreSQL**.

**Query Layer:** All complex SQL queries are centralized in `src/db/` as reusable singleton classes. The web app and pipeline both import from these modules instead of embedding raw SQL. See [Query Modules](docs/guides/QUERY_MODULES.md) for a full inventory.

### AI & Vision
- **Visual Extraction & OCR:** Qwen-VL via `src/services/vision_service.py` hitting `http://10.10.1.5:6969`
- **Note:** Do NOT use EasyOCR. All vision/OCR tasks go through the Qwen service.

## Setup
We provide a `Makefile` to streamline the setup process (Linux/WSL/MacOS).
```bash
git clone https://github.com/PowerInterest/HillsInspector
cd HillsInspector
make setup
```

## Documentation Index

The `docs/` folder contains comprehensive guides and technical design artifacts, systematically broken down into domains:

### 📐 Architecture & Infrastructure
- [Query Modules](docs/guides/QUERY_MODULES.md) - Centralized SQL query layer in `src/db/`: encumbrances, foreclosures, tax, sales, Sunbiz UCC.
- [Scheduled Jobs](docs/guides/SCHEDULED_JOBS.md) - PG-controlled job config/run tracking with cron-triggered Python workers.
- [Scheduled Jobs Walkthrough](docs/guides/WALKTHROUGH_SCHEDULED_JOBS.md) - Notes captured from the initial bulk-job scheduler integration pass.
- [Database Audit Definition](src/tools/db_audit.py) - The system-of-record metrics script.

### ⚖️ Real Estate Domain Logic
- [Encumbrance Audit Buckets](docs/domain/ENCUMBRANCE_AUDIT_BUCKETS.md) - Taxonomy for separating ORI discovery gaps, survival-risk gaps, and identity/title-break gaps on active foreclosures.
- [Lien Survival Analysis](docs/domain/LIEN_SURVIVAL.md) - Exact logic for modeling Florida statues covering extinguished/surviving liens and foreclosures. *(Implemented via `src/services/lien_survival/survival_service.py`)*
- [Auction Expiration Rules](docs/domain/AUCTION_EXPIRE.md) - Modeling property forfeiture and expiration mechanics.
- [Chain of Title Recovery](docs/domain/TITLE_CHAIN_RECOVERY.md) - The mechanism of scraping and verifying a complete chain of title.
- [NOC & Permit Linking](docs/domain/NOC_PERMIT_LINKING.md) - How we link notices of commencement to active building permits.
- [Party Matching Strategy](docs/domain/PARTY_MATCHING_STRATEGY.md) - Entity resolution logic for fuzzy matching property owners across completely different data silos.
- [Legal Issues Overview](docs/domain/LEGAL_ISSUES.md) - Real estate law nuances codified into algorithms.
- [Auction Buyer Resolution](docs/domain/AUCTION_BUYER_RESOLUTION.md) - Utilizing post-auction Property Appraiser Deeds to backwards resolve unknown auction winners. *(Implemented via `_classify_buyer()` in `pg_auction_results_service.py`)*

### 🌐 External Systems & Scraping
- [HCPA Bulk Data Dictionary](docs/external/HCPA_BULK_DATA_DICTIONARY.md) - Documentation on the Hillsborough County Property Appraiser weekly shapefile data exports, `.dbf` columns, and PIN mappings.
- [Hyland PAV NOC Discovery](docs/external/HYLAND_PAV_NOC_DISCOVERY.md) - Live query IDs, keyword fields, and the search order that actually finds NOCs in the Clerk's Hyland public-access stack.
- [Sunbiz Data Dictionary](docs/external/SUNBIZ_DATA_DICTIONARY.md) - Layout definition and tables for the Florida Division of Corporations bulk open datasets.
- [Tax Data Research](docs/external/TAX_DATA_RESEARCH.md) - Scraping instructions for the DOR (Department of Revenue) property millage layers.
- [Case Fallback Scraping](docs/domain/CASE_FALLBACK.md) - Fail-safe mechanisms when primary URLs vanish.

### 📖 Guides
- [Operations Runbook](docs/guides/RUNBOOK.md) - Standard operational procedures and recurring scripts.
- [ORI Lis Pendens Recovery](docs/guides/ORI_LIS_PENDENS_RECOVERY.md) - How active foreclosure LP gaps are retried without adding new PostgreSQL schema.
- [Foreclosure Identifier Repair](docs/guides/FORECLOSURE_IDENTIFIER_REPAIR.md) - How the pipeline repairs non-null but invalid HCPA straps from folio-backed parcel data.
- [Tampa Permit Value And Enforcement](docs/guides/TAMPA_PERMIT_VALUE_AND_ENFORCEMENT.md) - Why enforcement rows stay stored, but are excluded from permit-gap scoring, and how Tampa valuation parsing works.
- [Clerk Civil Alpha Merge](docs/CLERK_CIVIL_ALPHA_MERGE.md) - Merging 1958-present civil alpha index into normalised clerk tables with automated download.

## Roadmap / Planned Features
The following documents detail subsystems that are either in actively-developed feature branches or unwritten parts of the pipeline:
- [Property Market Pricing](docs/domain/PROPERTY_MARKET.md) - Intended to fetch Zillow/Redfin data. Highly unstable due to ongoing Market Photo Storage CDN/caching barriers.
- [Document OCR Types](docs/domain/DOC_TYPES.md) - Detailed OCR schemas specifically designed for the `VisionService`. Development is gated by upstream PDF downloading blockers.
- [Permit Expansion Plan](docs/plans/2026-03-02-permit-expansion-plan.md) - Expansion logic for scraping Plant City and Temple Terrace.
