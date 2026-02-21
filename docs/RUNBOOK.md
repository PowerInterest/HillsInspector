# HillsInspector Operational Runbook

Consolidated operational reference for the HillsInspector foreclosure analysis pipeline.

---

## 1. Quick Start

```bash
# Install dependencies
uv sync
uv run playwright install chromium

# Initialize PostgreSQL schema
uv run python -m src.db.migrations.create_foreclosures

# Seed the foreclosures hub table
uv run python scripts/refresh_foreclosures.py --migrate

# Run full pipeline (expect 4+ hours)
uv run Controller.py

# Start web UI
uv run python -m app.web.main
# → http://localhost:8080
```

---

## 2. CLI Reference — Controller.py

```bash
uv run Controller.py [OPTIONS]
```

### General

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--dsn` | str | env | PostgreSQL DSN override |
| `--force-all` | flag | off | Force all loaders (ignore staleness) |
| `--fail-fast` | flag | off | Stop after first step failure |

### Phase A Skip Flags (Bulk Data Refresh)

| Flag | Skips |
|------|-------|
| `--skip-hcpa` | HCPA parcels, sales, subdivisions |
| `--skip-clerk-bulk` | Clerk civil cases & events |
| `--skip-nal` | DOR NAL tax data |
| `--skip-flr` | Sunbiz FLR (UCC filings) |
| `--skip-sunbiz-entity` | Sunbiz entity lookup |
| `--skip-county-permits` | County permit API sync |
| `--skip-tampa-permits` | Tampa permit Accela scrape |
| `--skip-foreclosure-refresh` | Foreclosures hub table refresh |
| `--skip-trust-accounts` | Trust account ledger sync |
| `--skip-title-chain` | PG chain of title builder |
| `--skip-market-data` | Market data (Zillow/Realtor) |
| `--skip-final-refresh` | Phase B data pickup refresh |

### Phase B Skip Flags (Per-Auction Enrichment)

| Flag | Skips |
|------|-------|
| `--skip-auction-scrape` | Scraping upcoming auctions |
| `--skip-judgment-extract` | Judgment PDF extraction (Vision OCR) |
| `--skip-ori-search` | ORI document search |
| `--skip-survival` | Lien survival analysis |

### Per-Auction Limits

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--auction-limit` | int | unlimited | Max auctions to scrape per date |
| `--judgment-limit` | int | unlimited | Max PDFs to extract |
| `--ori-limit` | int | unlimited | Max foreclosures for ORI search |
| `--survival-limit` | int | unlimited | Max foreclosures for survival |
| `--limit` | int | unlimited | Total row limit for chain builder |

### Staleness Windows

| Flag | Default | Controls |
|------|---------|----------|
| `--hcpa-stale-days` | 7 | HCPA data refresh interval |
| `--clerk-stale-days` | 7 | Clerk data refresh interval |
| `--nal-stale-days` | 60 | NAL tax data refresh interval |
| `--flr-stale-days` | 7 | FLR UCC filings refresh |
| `--sunbiz-entity-stale-days` | 90 | Sunbiz entity refresh |
| `--county-permit-stale-days` | 7 | County permits refresh |
| `--tampa-stale-days` | 3 | Tampa permits refresh |

### HCPA Options

| Flag | Default | Description |
|------|---------|-------------|
| `--hcpa-download-dir` | `data/bulk_data/hcpa` | HCPA bulk file directory |
| `--include-hcpa-latlon` | off | Include lat/lon file in load |

### Sunbiz Options

| Flag | Default | Description |
|------|---------|-------------|
| `--sunbiz-data-dir` | `data/sunbiz` | Sunbiz SFTP mirror root |
| `--sunbiz-manifest` | `data/sunbiz/manifest.json` | SFTP mirror state file |

### County Permit Options

| Flag | Default | Description |
|------|---------|-------------|
| `--county-where` | `1=1` | SQL WHERE clause filter |
| `--county-page-size` | 2000 | API pagination size |

### Tampa Permit Options

| Flag | Default | Description |
|------|---------|-------------|
| `--tampa-lookback-days` | 30 | Lookback if no date range given |
| `--tampa-start-date` | none | Start date (YYYY-MM-DD) |
| `--tampa-end-date` | none | End date (YYYY-MM-DD) |
| `--tampa-keep-csv` | off | Keep CSV temp files after import |
| `--tampa-enrich-limit` | 250 | Max records to enrich (0=disabled, negative=all) |

### Title Chain Scope

| Flag | Default | Description |
|------|---------|-------------|
| `--foreclosure-id` | none | Limit to single foreclosure ID |
| `--case-number` | none | Limit to single case number |
| `--active-only` | off | Only analyze active foreclosures |
| `--similarity-threshold` | 0.68 | Fuzzy match threshold for party names |

### Common Recipes

```bash
# Quick sanity check (5 per step)
uv run Controller.py --auction-limit 5 --judgment-limit 5 --ori-limit 5 --survival-limit 5 --limit 5

# Phase A only (bulk refresh)
uv run Controller.py \
  --skip-auction-scrape --skip-judgment-extract --skip-ori-search \
  --skip-survival --skip-final-refresh

# Phase B only (per-auction enrichment)
uv run Controller.py \
  --skip-hcpa --skip-clerk-bulk --skip-nal --skip-flr \
  --skip-sunbiz-entity --skip-county-permits --skip-tampa-permits \
  --skip-foreclosure-refresh --skip-trust-accounts --skip-title-chain \
  --skip-market-data

# Single case analysis
uv run Controller.py --case-number 292024CA012345 \
  --skip-hcpa --skip-clerk-bulk --skip-nal --skip-flr \
  --skip-sunbiz-entity --skip-county-permits --skip-tampa-permits

# Force all bulk loaders (ignore freshness)
uv run Controller.py --force-all --skip-auction-scrape --skip-judgment-extract \
  --skip-ori-search --skip-survival
```

---

## 3. Pipeline Steps

### Phase A — Bulk Data Refresh

Idempotent. Skips if data is fresh (within stale-days). Override with `--force-all`.

| # | Step | Skip Flag | Stale Days | Runs As |
|---|------|-----------|------------|---------|
| 1 | HCPA suite (parcels, sales, subdivisions) | `--skip-hcpa` | 7 | Background |
| 2 | Clerk civil cases & events | `--skip-clerk-bulk` | 7 | Background |
| 3 | DOR NAL tax data | `--skip-nal` | 60 | Inline |
| 4 | Sunbiz FLR (UCC filings) | `--skip-flr` | 7 | Background |
| 5 | Sunbiz entity lookup | `--skip-sunbiz-entity` | 90 | Background |
| 6 | County permits (Accela API) | `--skip-county-permits` | 7 | Background |
| 7 | Tampa permits (Accela scrape) | `--skip-tampa-permits` | 3 | Background |
| 8 | Foreclosures hub table refresh | `--skip-foreclosure-refresh` | — | Inline |
| 9 | Trust account ledger sync | `--skip-trust-accounts` | — | Inline |
| 10 | PG chain of title builder | `--skip-title-chain` | — | Inline |

Background steps are dispatched via `controller_step_dispatcher.py` and don't block the main thread.

### Phase B — Per-Auction Enrichment

Sequential. Processes **all incomplete auctions** (no date filter, no staleness check).

| # | Step | Skip Flag | Limit Flag | Description |
|---|------|-----------|------------|-------------|
| 11 | Auction scrape | `--skip-auction-scrape` | `--auction-limit` | Scrape upcoming auctions from clerk site |
| 12 | Judgment extract | `--skip-judgment-extract` | `--judgment-limit` | Download PDFs, extract via Vision OCR |
| 13 | ORI search | `--skip-ori-search` | `--ori-limit` | Search Official Records, ingest documents |
| 14 | Survival analysis | `--skip-survival` | `--survival-limit` | Lien survival determination |
| 15 | Final refresh | `--skip-final-refresh` | — | Re-run foreclosure refresh (pick up Phase B data) |
| 16 | Market data | `--skip-market-data` | — | Zillow/Realtor (background, non-blocking) |

---

## 4. Database Setup

### PostgreSQL

Default connection: `postgresql://hills:hills_dev@localhost:5432/hills_sunbiz`

```bash
# Create/migrate foreclosures schema (idempotent — IF NOT EXISTS everywhere)
uv run python -m src.db.migrations.create_foreclosures

# With explicit DSN
uv run python -m src.db.migrations.create_foreclosures --dsn "postgresql://user:pass@host:5432/dbname"

# Refresh foreclosures hub table from reference data
uv run python scripts/refresh_foreclosures.py

# Create tables + refresh in one command
uv run python scripts/refresh_foreclosures.py --migrate
```

**Key tables created by migration:**

| Table | Purpose |
|-------|---------|
| `foreclosures` | Hub table — one row per case + auction date (~180 columns) |
| `foreclosures_history` | Aged past-auction rows (archived automatically) |
| `foreclosure_events` | Docket timeline (FK to foreclosures, CASCADE DELETE) |

**Functions created:**
- `normalize_case_number_fn(text)` — converts `29YYYYTTNNNNNN` to `YY-TT-NNNNNN`
- `normalize_foreclosure()` — trigger that normalizes + cross-fills folio/strap on INSERT/UPDATE

**View created:**
- `property_timeline` — UNION ALL of sales, events, auctions, encumbrances

### Reference Data (read-only from pipeline)

| Table | Records | Source |
|-------|---------|--------|
| `hcpa_allsales` | ~2.4M | HCPA sales history |
| `hcpa_bulk_parcels` | ~530K | HCPA parcel data |
| `hcpa_parcel_sub_names` | ~11.5K | Subdivision code → name + plat |
| `sunbiz_flr_filings` | ~21K | UCC financing statements |

```bash
# Load/refresh PostgreSQL reference data (idempotent)
uv run python -m sunbiz.pg_loader
```

### SQLite (Legacy)

Located at `data/property_master_sqlite.db`. WAL mode. Access only through `PropertyDB` class.

```bash
# Create SQLite schema (only for legacy pipeline)
uv run python -m src.db.migrations.create_sqlite_database
```

---

## 5. Web Interface

```bash
uv run python -m app.web.main
```

- **URL**: `http://localhost:8080`
- **Host**: `0.0.0.0` (all interfaces)
- **Auto-reload**: enabled (dev mode)

### Routes

| Path | Description |
|------|-------------|
| `/` | Dashboard — auction list, stats |
| `/property/{id}` | Property detail page |
| `/api/...` | JSON API (map data, search, health) |
| `/review/...` | Review/annotation interface |
| `/static/...` | Static assets |

### Error Handling

- Database lock errors → HTTP 503 with HTMX-aware fragments
- Each error gets a unique 8-char UUID for log correlation

---

## 6. Utility Scripts

```bash
# Refresh foreclosures hub table
uv run python scripts/refresh_foreclosures.py [--migrate] [--dsn DSN]

# Benchmark encumbrance algorithms
uv run python scripts/benchmark_encumbrance_algorithms.py

# Lint and fix
uv run ruff check src/ --fix
uv run ruff check tests/ --fix

# Type check
uv run ty check src/

# Run tests
uv run pytest tests/
uv run pytest tests/ -v --tb=short

# Install browser for Playwright
uv run playwright install chromium
```

---

## 7. Environment & Dependencies

### Required Services

| Service | Default Address | Purpose |
|---------|----------------|---------|
| PostgreSQL | `localhost:5432` | Primary database (`hills_sunbiz`) |
| Vision API | `http://10.10.1.5:8002` | GLM-4.6V-Flash (judgment OCR) |
| Vision fallback | `http://192.168.86.26:6969` | Secondary local Vision endpoint |

### Environment Variables

Set in `.env` (loaded by `dotenv`):

| Variable | Default | Purpose |
|----------|---------|---------|
| `SUNBIZ_PG_DSN` | `postgresql://hills:hills_dev@localhost:5432/hills_sunbiz` | PostgreSQL connection |
| `GEMINI_API_KEY` | none | Cloud Vision fallback (Gemini 2.5 Flash Lite) |
| `SUNBIZ_SFTP_HOST` | `sftp.floridados.gov` | Sunbiz SFTP host |
| `SUNBIZ_SFTP_PORT` | `22` | Sunbiz SFTP port |
| `SUNBIZ_SFTP_USER` | `Public` | Sunbiz SFTP user |
| `SUNBIZ_SFTP_PASSWORD` | `PubAccess1845!` | Sunbiz SFTP password |
| `SUNBIZ_SFTP_DAILY_DIR` | `/public/doc` | SFTP daily directory |
| `SUNBIZ_SFTP_QUARTERLY_DIR` | `/public/doc/quarterly` | SFTP quarterly directory |
| `SUNBIZ_DATA_DIR` | `data/sunbiz` | Local Sunbiz mirror |
| `SUNBIZ_MANIFEST` | `data/sunbiz/manifest.json` | SFTP mirror state |
| `LOG_LEVEL` | `INFO` | Log verbosity |
| `LOG_DEBUG_FILE` | none | Path for a DEBUG-level log sink |
| `LOG_JSON` | `0` | Set `1` for structured JSON logs to `logs/` |

### Python Requirements

- **Python**: `>=3.12, <3.13`
- **Package manager**: `uv` only (never pip or poetry)

### Key Dependencies

| Package | Purpose |
|---------|---------|
| `playwright`, `playwright-stealth` | Browser automation + bot evasion |
| `fastapi`, `uvicorn`, `jinja2` | Web framework (SSR + HTMX) |
| `psycopg[binary]`, `sqlalchemy` | PostgreSQL driver/ORM |
| `polars` | DataFrames (never pandas) |
| `pymupdf` | PDF text extraction |
| `loguru` | Logging |
| `rapidfuzz` | Fuzzy string matching |
| `paramiko` | SFTP for Sunbiz mirror |

---

## 8. Success Criteria & Validation

A pipeline run is measured by **data completeness**, not by steps completing without errors.

| Metric | Target | Validation |
|--------|--------|------------|
| Final Judgment PDFs | 90%+ of foreclosures | `ls data/Foreclosure/*/documents/*.pdf \| wc -l` vs total auctions |
| Extracted judgment data | 90%+ of PDFs | `SELECT COUNT(*) FROM foreclosures WHERE judgment_data IS NOT NULL` |
| Chain of title | 80%+ of judged | `SELECT COUNT(DISTINCT folio) FROM ori_encumbrances WHERE is_inferred=0` |
| Encumbrances identified | 80%+ of judged | `SELECT COUNT(DISTINCT folio) FROM ori_encumbrances` |
| Lien survival analysis | 80%+ of judged | `SELECT COUNT(DISTINCT folio) FROM ori_encumbrances WHERE survival_status IS NOT NULL` |

**If any threshold is not met:**

1. Query the pipeline tracking columns in `foreclosures` to find incomplete rows
2. Check `logs/` for errors on the failing step
3. Fix the root cause
4. Re-run the affected steps (use skip flags to target)
5. Run `scripts/refresh_foreclosures.py` to pick up new data
6. Re-check thresholds

---

## 9. Troubleshooting

### Pipeline takes less than 4 hours

Something is skipping. Check that skip flags aren't set and that staleness windows haven't made steps no-op. Use `--force-all` to bypass freshness checks.

### Zero chain/encumbrances after ORI step

ORI search found documents but chain builder returned nothing. Common causes:
- SearchQueue writes weren't committed (check for `conn.commit()` after inserts)
- Legal description search terms didn't match (check `ori_search_queue`)
- Document type normalization mismatch (raw `(MTG) MORTGAGE` vs normalized `mortgage`)

### "Could not identify foreclosing lien"

Every foreclosure has one by law. The code is at fault. Check:
- Encumbrance type matching (must handle both raw ORI and normalized forms)
- Plaintiff name vs. original lender (assignments change the name — use fuzzy match)
- ORI didn't find the mortgage (check search queue exhaustion)

### Judgment PDF missing

1. Primary: clerk link on auction page may have empty instrument number
2. Fallback: case number search on ORI (`_search_judgment_by_case_number`)
3. Recovery: for CC cases, follow party names → LP (Lis Pendens) → real CA case number → JUD

### Vision OCR timeouts

- Primary endpoint (`10.10.1.5:8002`) should respond in 1-2s/page
- Fallback (`192.168.86.26:6969`) is slow (~240s timeout)
- Timed-out endpoints are suspended for 10 minutes automatically
- Cloud fallback uses Gemini (`GEMINI_API_KEY` required)

### Database lock errors (SQLite legacy)

- Check for stale `.lock` file at the SQLite path
- Ensure only one pipeline instance is running (`ps aux | grep Controller`)
- Never write to SQLite from `run_in_executor` threads — return data, write on main thread

### Bot detection on county sites

- Always use real Chrome with `channel="chrome"` and persistent browser profiles
- Apply `playwright-stealth` to every page immediately after creation
- Use profiles from `data/browser_profiles/` (never empty profiles)
- Never take screenshots of property websites

### Stale judgment checkpoint after DB rebuild

The file `data/judgments/judgment_extracts_checkpoint.parquet` persists across DB rebuilds. Delete it when starting fresh or it will cause Step 2 to skip already-extracted cases.

---

## 10. Key Data Identifiers

### Property Identifiers

| Name | Format | Example | Usage |
|------|--------|---------|-------|
| `strap` | HCPA parcel format | `1929084NUB00000000040A` | PG `hcpa_bulk_parcels`, pipeline `parcel_id` |
| `folio` | 10-digit PG format | `000411-0000` | PG `foreclosures.folio`, sales joins |
| `parcel_id` | = `strap` | same as strap | Pipeline convention |

**Join rule**: Pipeline `parcel_id` = PG `strap` column (NOT `folio`).

### Case Number Formats

| Pattern | Meaning | Example |
|---------|---------|---------|
| `29YYYYCANNNNNN` | Circuit Court (mortgage foreclosure) | `292024CA012345` |
| `29YYYYCCNNNNNN` | County Court (HOA liens, code enforcement) | `292024CC001234` |
| `YYYY-NNN` | Tax deed | `2024-123` |

CC cases often reference a related CA case for the same property. Follow the chain via Lis Pendens.

### Survival Status Values (UPPERCASE)

| Status | Meaning |
|--------|---------|
| `FORECLOSING` | The lien being foreclosed (plaintiff's mortgage) |
| `EXTINGUISHED` | Wiped out by the foreclosure sale |
| `SURVIVED` | Senior to the foreclosing lien — survives sale |
| `EXPIRED` | Past MRTA or statute of limitations |
| `SATISFIED` | Already satisfied/released of record |
| `HISTORICAL` | Pre-acquisition, no longer relevant |
| `UNCERTAIN` | Insufficient data to determine |

### ORI Document Types

Common types in the `ori_encumbrances` table (raw ORI format):

| Code | Full Name |
|------|-----------|
| `(MTG) MORTGAGE` | Mortgage |
| `(JUD) JUDGMENT` | Judgment |
| `(LP) LIS PENDENS` | Lis Pendens |
| `(SAT) SATISFACTION` | Satisfaction |
| `(ASG) ASSIGNMENT` | Assignment |
| `(REL) RELEASE` | Release |
| `(NOC) NOTICE OF COMMENCEMENT` | Notice of Commencement |
| `(AFF) AFFIDAVIT` | Affidavit |

### Data Directories

| Path | Contents |
|------|----------|
| `data/Foreclosure/{case_number}/documents/` | Judgment PDFs + `*_extracted.json` |
| `data/bulk_data/hcpa/` | HCPA bulk downloads |
| `data/sunbiz/` | Sunbiz SFTP mirror |
| `data/browser_profiles/` | Playwright browser profiles |
| `data/cache/{category}/` | DiskCache for API responses |
| `logs/` | Pipeline logs (loguru) |
