# Pipeline Documentation & Logic Audit
**Date:** 2025-12-31 | **Updated:** 2026-02-10
**Status:** RESOLVED (Items 1-2, 8-15), ACTION REQUIRED (Items 3-7)

This document summarizes conflicts, logic flaws, and redundancies identified during a review of the `docs/steps/*` documentation and `src/orchestrator.py`.

---

## Resolved Issues

### 1. Dual Scraping of Realtor.com ~~(CRITICAL)~~
**Status:** FIXED (2026-01-01)

**Problem:** Pipeline ran two scrapers for Realtor.com in parallel (HomeHarvest + MarketScraper).

**Fix Applied:** Modified `src/scrapers/market_scraper.py` to remove Realtor fallback. MarketScraper now hits **Zillow only**; HomeHarvest handles Realtor exclusively.

### 2. ORI Ingestion Versioning ~~(HIGH)~~
**Status:** FIXED (2026-01-01)

**Problem:** Conflicting V1/V2 documentation for ORI ingestion.

**Fix Applied:**
- Moved `05_ori_ingestion.md` -> `docs/archive/05_ori_ingestion_v1_DEPRECATED.md`
- Renamed `04_ori_ingestion_v2.md` -> `docs/steps/05_ori_ingestion_v2.md`

### 8. Parcels Table Empty - Zero Chain of Title Data (CRITICAL)
**Status:** FIXED (2026-02-09)

**Problem:** The `parcels` table was completely empty (0 rows) across all pipeline runs. This caused Phase 2 (ORI ingestion) to find no legal descriptions for any property, which meant chain-of-title analysis was skipped for every foreclosure. The pipeline reported 0/186 properties with chain data.

**Root Cause:** `_run_hcpa_gis` in `orchestrator.py` never called `upsert_parcel()` or any method that creates rows in the `parcels` table. It only called `update_legal_description()`, which was a plain `UPDATE` on rows that didn't exist yet, silently affecting 0 rows.

**Fix Applied:**
1. Added `PropertyDB.save_hcpa_to_parcel()` method using UPSERT pattern (`INSERT OR IGNORE` + `UPDATE`)
2. Added call to `save_hcpa_to_parcel()` in orchestrator's `_run_hcpa_gis` method
3. Changed `update_legal_description()` from plain UPDATE to UPSERT pattern
4. Added `judgment_legal_description` column to parcels schema migrations
5. Backfilled 120 parcels from 339 existing HCPA JSON files on disk

**Files Modified:** `src/db/operations.py`, `src/orchestrator.py`

### 9. Parcel ID Format Mismatch
**Status:** FIXED (2026-02-09)

**Problem:** Initial backfill of parcels used the short folio format (e.g., `000411-0000`) from HCPA JSON files as the `parcels.folio` key. But the orchestrator queries `parcels WHERE folio = ?` using `auctions.parcel_id`, which stores the HCPA format (e.g., `1929084NUB00000000040A`).

**Fix Applied:** Rebuilt parcels table with correct keys by mapping `case_number` -> `auction.parcel_id` -> HCPA data. Result: 120 parcels with legal descriptions correctly keyed to match auction lookups.

### 10. DuckDB Removed — SQLite Only
**Status:** FIXED (2026-02-10)

**Problem:** The project used two database engines: SQLite for the main pipeline and DuckDB for V2 ORI ingestion and chain-of-title analysis. The V2 path had systemic issues:
- Queried SQLite tables (`parcels`, `bulk_parcels`, `sales_history`) from a DuckDB connection
- DuckDB schema was out of sync with code (column name mismatches)
- Maintaining two databases added unnecessary complexity and caused lock contention

**Fix Applied:**
- DuckDB completely removed from the codebase
- All code consolidated to use SQLite (`data/property_master_sqlite.db`)
- Deleted: `src/db/new.py`, `src/db/v2_database.py`, `config/step4v2.py`, `pipelinev2/`, V2 tests
- `duckdb` removed from `pyproject.toml` dependencies

**Files Modified:** Multiple (see `docs/Production_Fixes.md` for full list)

### 11. Step 2 Judgment Extraction Gap
**Status:** IDENTIFIED (2026-02-09)

**Problem:** 180/186 final judgment PDFs exist on disk, but only 18 have been processed through VisionService to extract structured data (plaintiff, defendant, amounts, legal description). The `extracted_judgment_data` column in `auctions` is NULL for 162 cases.

**Root Cause:** `step_pdf_downloaded` was never marked after Step 1 PDF download (fixed 2026-02-08), so Step 2 found 0 auctions to process on subsequent runs. The 18 extracted cases were from the initial run before the bug was introduced.

**Status:** The `step_pdf_downloaded` marking bug is fixed. A re-run of Step 2 (`--start-step 2`) will process the remaining 162 cases.

### 12. update_legal_description Silent No-Op
**Status:** FIXED (2026-02-09)

**Problem:** `PropertyDB.update_legal_description()` used a plain `UPDATE parcels SET legal_description = ? WHERE folio = ?`. When called for a folio that didn't yet exist in the `parcels` table, it affected 0 rows silently - no error, no warning, no data written.

**Fix Applied:** Changed to UPSERT pattern: `INSERT OR IGNORE INTO parcels (folio) VALUES (?)` followed by the UPDATE. This ensures the row exists before updating.

**File Modified:** `src/db/operations.py`

### 13. ORI Browser Search 30s Timeout on Zero Results (PERFORMANCE)
**Status:** FIXED (2026-02-10)

**Problem:** 76% of ORI legal description browser searches returned zero results but waited the full 30 seconds (`wait_for_selector("table tbody tr", timeout=30000)`) before timing out. This wasted approximately 3.6 hours per full pipeline run (460 zero-result searches × 30s each).

**Root Cause:** The PAV Direct Search site is an Angular SPA. When zero documents match, the table `tbody tr` elements never appear — the page shows "No documents found" as its initial state. Our code waited 30s for table rows that would never come.

**Discovery:** The Angular app fires a `POST /PAVDirectSearch/api/CustomQuery/KeywordSearch` API call automatically when the page loads with search parameters. This API returns `{"Data":[],"Truncated":false,"DisplayColumns":null}` **instantly** for zero results. The response arrives in <1s but our code ignored it and waited for DOM elements.

**Fix Applied:** All four browser search methods now use Playwright's `page.expect_response()` to intercept the `KeywordSearch` API response:
1. `search_by_legal_browser` (CQID 321) — Legal description search
2. `search_by_book_page_browser` (CQID 319) — Book/page search
3. `search_by_party_browser` (CQID 326) — Party name search
4. `search_by_party_and_instrument_browser` (CQID 326) — Party + instrument search

Pattern:
```python
async with page.expect_response(
    lambda r: "CustomQuery/KeywordSearch" in r.url,
    timeout=30000,
) as response_info:
    await page.goto(url, timeout=60000)

api_response = await response_info.value
api_data = await api_response.json()

if not api_data.get("Data"):
    return []  # Zero results – bail immediately

# Results exist – wait for table to render (shorter 15s timeout)
await page.wait_for_selector("table tbody tr", timeout=15000)
```

**Performance Impact:**
- Zero-result searches: **1.5s** (was 30s) — 20x faster
- Results searches: Unchanged (table renders in <15s)
- Estimated pipeline savings: **~3.6 hours per full run**

**PAV API Reference:**
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/PAVDirectSearch/api/CustomQuery` | GET | List available search types |
| `/PAVDirectSearch/api/Keywords` | POST `{QueryID}` | Get keyword definitions for a search type |
| `/PAVDirectSearch/api/CustomQuery/KeywordSearch` | POST | Execute keyword-based search |

Legal Description search (CQID 321) keyword IDs:
- `1011` — Legal Description text
- `1285` — ORI Doc Type filter
- `1634` — Recording Date Time filter

**File Modified:** `src/scrapers/ori_api_scraper.py`

### 14. Vision Extraction on Low-Value Document Types (PERFORMANCE)
**Status:** FIXED (2026-02-10)

**Problem:** Vision OCR was running on every downloaded ORI document, including types where the extracted data adds no value over ORI metadata. NOC (Notice of Commencement), ASG (Assignment), RELLP (Release Lis Pendens), PR (Partial Release), AGR (Agreement), and AFF (Affidavit) documents yielded no useful structured financial data from vision — ORI metadata already provides parties, dates, and instrument numbers.

**Fix Applied:** Added `VISION_EXTRACT_DOC_TYPES` gate in `src/services/ingestion_service.py`. Only doc types that yield financial data (amounts, rates, sale prices) are sent to vision. Others are downloaded for record-keeping but skip the expensive OCR step.

**Performance Impact:** ~22% of vision calls eliminated (~70 out of 314 documents per run), saving ~1-2 hours per run.

**File Modified:** `src/services/ingestion_service.py`

### 15. Step 2 Silent Skip on Empty parcel_id (DATA LOSS)
**Status:** FIXED (2026-02-10)

**Problem:** 53 auctions had final judgment PDFs on disk and were ready for vision extraction, but Step 2 silently skipped them. No log line, no error, no database status update — they simply vanished from processing.

**Root Cause:** The Step 2 judgment extraction loop had an `invalid_parcel_ids` check:
```python
invalid_parcel_ids = {"property appraiser", "n/a", "none", ""}
# ...
if parcel_id.lower() in invalid_parcel_ids:
    continue  # silent skip
```

65 auctions had empty-string `""` parcel_ids (not NULL — the auction scraper stores `""` when the clerk page has no parcel link). The SQL query's `WHERE a.parcel_id IS NOT NULL` passed these through, but the Python check caught them and silently skipped. Judgment extraction doesn't need a parcel_id — it just reads the PDF and runs vision OCR. The lookup is by `case_number`, not `parcel_id`.

**Fix Applied:**
1. Removed `invalid_parcel_ids` set and the silent `continue` check
2. Removed `WHERE a.parcel_id IS NOT NULL` from the Step 2 SQL query
3. Added `logger.debug()` for "no PDF on disk" cases (was also silent)

**Impact:** 53 additional judgments will be extracted on next Step 2 run (68 → ~121 out of 186).

**File Modified:** `src/orchestrator.py`

---

## Action Required

### 3. HCPA Vision Scraper (Dead Code)
**Severity:** LOW | **Decision:** MOVE TO SCRIPTS

**Current State:** `docs/steps/12_hcpa_fallback.md` documents a Vision-based HCPA scraper that is not wired into the pipeline as a fallback.

**Action Plan:**
1. Move `src/scrapers/hcpa_scraper.py` -> `scripts/hcpa_vision_scraper.py`
2. Update `12_hcpa_fallback.md` to indicate it's a standalone utility, not a pipeline step
3. Remove Step 12 from pipeline documentation

### 4. Geocoding: Fail Fast if Missing
**Severity:** MEDIUM | **Decision:** FAIL PIPELINE

**Current State:** Flood Zone check may fail if Bulk doesn't provide coordinates, even though HomeHarvest might later.

**New Requirement:** Bulk should **always** produce geocodes. If a property doesn't have coordinates after Bulk enrichment, the pipeline should **halt** (fail fast) rather than silently proceeding.

**Action Plan:**
1. Add validation in `orchestrator.py` after Bulk enrichment: if `lat/lon` is null, skip enrichment for that property with a clear error
2. Remove the race condition concern - coordinates are expected from Bulk, not optional

### 5. Tax Deed Scraper (Not Implemented)
**Severity:** LOW | **Decision:** MOVE TO SCRIPTS

**Current State:** Tax Deed scraping (`docs/steps/01_5_tax_deed_auctions.md`) was never fully implemented.

**Action Plan:**
1. Move `src/scrapers/tax_deed_scraper.py` -> `scripts/tax_deed_scraper.py`
2. Archive documentation: `01_5_tax_deed_auctions.md` -> `docs/archive/`
3. Remove Step 1.5 references from pipeline overview

### 6. Step Numbering Alignment
**Severity:** MEDIUM | **Decision:** RENUMBER TO WHOLE NUMBERS

**Current State:** Documentation uses fractional steps (1.5, 3.5, 4v2) that don't match code phases.

**Action Plan:**
1. Audit `orchestrator.py` to extract actual execution order
2. Renumber documentation to use **whole integers** (1, 2, 3, ...)
3. Align step numbers with Orchestrator phases:
   - **Steps 1-2:** Auction Scraping & PDF Extraction
   - **Steps 3-8:** Phase 1 Parallel Enrichment
   - **Step 9:** ORI Ingestion (Phase 2)
   - **Steps 10-11:** Analysis (Phase 3)

### 7. Semaphore Documentation Mismatch
**Severity:** LOW | **Verified:** REAL BUT MINOR

**Findings:**

| Semaphore | Code | Docs | Issue |
|-----------|------|------|-------|
| `realtor_semaphore` | Does not exist | `= 2` | Docs reference non-existent semaphore |
| `homeharvest_semaphore` | `= 1` | Missing | Not documented |
| `v2_db_semaphore` | `= 1` | Missing | Not documented |

**Action Plan:**
1. Remove `realtor_semaphore` from `00_pipeline_overview.md`
2. Add `homeharvest_semaphore = 1` and `v2_db_semaphore = 1` to docs

---

## Summary of Work

| # | Issue | Status | Action |
|---|-------|--------|--------|
| 1 | Dual Realtor Scraping | FIXED | N/A |
| 2 | ORI Versioning | FIXED | N/A |
| 3 | HCPA Vision Fallback | Pending | Move to `scripts/` |
| 4 | Geocoding Race | Pending | Fail fast if no coords |
| 5 | Tax Deed Scraper | Pending | Move to `scripts/` |
| 6 | Step Numbering | Pending | Renumber to whole ints |
| 7 | Semaphore Docs | Pending | Update `00_pipeline_overview.md` |
| 8 | **Parcels Table Empty** | **FIXED** | `save_hcpa_to_parcel` + UPSERT |
| 9 | **Parcel ID Mismatch** | **FIXED** | Use HCPA-format key |
| 10 | **DuckDB Removed** | **FIXED** | SQLite only, DuckDB fully removed |
| 11 | **Judgment Extraction Gap** | **Identified** | Re-run Step 2 |
| 15 | **Step 2 Silent Skip on Empty parcel_id** | **FIXED** | Remove invalid_parcel_ids check |
| 12 | **update_legal_description No-Op** | **FIXED** | UPSERT pattern |
| 13 | **ORI 30s Timeout on Zero Results** | **FIXED** | `expect_response` API intercept |
| 14 | **Vision on Low-Value Doc Types** | **FIXED** | `VISION_EXTRACT_DOC_TYPES` gate |
