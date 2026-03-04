# Fixing Foreclosure Data Flow Architecture (Revised)

This plan addresses the data duplication loop caused by `refresh_foreclosures.py` moving records between `foreclosures` and `foreclosures_history`. It also incorporates strict feedback from the architecture review to ensure zero downtime, preservation of enrichment behavior, safe schema migration, and strict pipeline metric validation.

## Architectural Decision
We will embrace the "Soft Delete" pattern. The `foreclosures` table will be the sole master state table for all auctions. Past auctions will be marked with `archived_at` and remain in place so their foreign keys (chain of title, encumbrances) remain fully intact.

To prevent breaking existing live code paths (FastAPI history routes, DB functions, and title-chain joins) that rely on the `foreclosures_history` table name, **we will convert `foreclosures_history` into a PostgreSQL `VIEW`**.

## Schema Changes
This architecture requires the following specific changes to the database schema:

### [DROP] Table: `foreclosures_history`
- The physical table will be completely dropped to eliminate data duplication.

### [NEW] View: `foreclosures_history`
- **Definition:** `SELECT *, archived_at AS moved_to_history_at FROM foreclosures WHERE archived_at IS NOT NULL;`
- **Columns:** This view inherits exactly all columns from the `foreclosures` table.
- **Computed Column:** It aliases `archived_at` as `moved_to_history_at` to preserve backward compatibility for any routes expecting that specific timestamp column.

*(Note: There are no new physical tables or physical columns being added to the database. The only schema modification is converting one physical table into a logical view.)*

## Proposed Changes

### 1. Safe Schema Migration via Alembic
We will create a proper Alembic revision to safely convert the table to a view on the live database without data loss.
#### [NEW] `alembic/versions/XXXX_viewify_foreclosures_history.py`
This Alembic upgrade script will:
1. Handle dependency order explicitly if any DB functions/views reference `foreclosures_history` (drop them, replace table with view, recreate them).
2. Sync any "orphaned" history rows back into `foreclosures` using `INSERT ... ON CONFLICT DO UPDATE` with explicit `COALESCE` merge rules for enrichment fields.
3. Run `DROP TABLE foreclosures_history;`
4. Run `CREATE VIEW foreclosures_history AS SELECT *, archived_at AS moved_to_history_at FROM foreclosures WHERE archived_at IS NOT NULL;`

*(Note: We will not trigger this migration from `pg_pipeline_controller.py`. Schema migration will remain a separate manual/deployment step.)*

### 2. Update DB Setup Scripts
For fresh database boots, the setup script must create a view instead of a table.
#### [MODIFY] `src/db/migrations/create_foreclosures.py`
- Remove the `CREATE TABLE ... foreclosures_history` block.
- **[NEW]** Remove the `CREATE INDEX` statements targeting `foreclosures_history`, as indexes cannot be created on a plain view and will cause bootstrap to fail.
- Add the `CREATE VIEW foreclosures_history ...` definition at the end.

### 3. Update Refresh Orchestration
We must surgically fix the refresh script to rely purely on `foreclosures` while preserving its ability to enrich historical records, and add logic to carry over enrichment data for rescheduled auctions.
#### [MODIFY] `scripts/refresh_foreclosures.py`
- **Step 1: Enrichment Base:** Current Step 1 acts as a historical seed using `INSERT ... SELECT` from `foreclosures_history`. We will rewrite this as a targeted `UPDATE foreclosures ... FROM (SELECT ... JOIN hcpa_bulk ... JOIN clerk ...) sub WHERE foreclosures.foreclosure_id = sub.foreclosure_id` pass to enrich active and historical rows directly in place, preventing a self-upsert anti-pattern.
- **Step 5: Archive Post-Auction:** Preserve this logic exactly as is.
- **Step 5.5: Move to History:** Completely delete this step.
- **[NEW] Step 7: Rescheduled Auction Data Reuse:** Add an `UPDATE` block that finds new active rows (`f.foreclosure_id > old_f.foreclosure_id` and same `case_number_raw`) and copies data from the *most recently archived* row using `DISTINCT ON (case_number_raw) ... ORDER BY case_number_raw, archived_at DESC NULLS LAST`. It will copy `strap`, `folio`, `property_address`, `judgment_data`, `step_judgment_extracted`, and conditionally copy `step_ori_searched` and `step_survival_analyzed` to avoid completely re-running the pipeline.

### 4. Controller & Service Wiring (Audit Fixes)
Based on `docs/PIPELINE_SCRAPING_AUDIT.md`, these fixes will be applied to prevent external API waste specifically during the overlap window between an old and new auction row.
#### [MODIFY] `src/services/pg_pipeline_controller.py`
- In `_run_title_chain()`, preserve the behavior to naturally run active-only, but ensure it gracefully supports CLI overrides for historical investigations.
- **[NEW]** In `_select_single_pin_permit_candidates()`, add `AND f.archived_at IS NULL` to candidate selection, and use `SELECT DISTINCT ON (f.strap)` to prevent scraping the same PIN twice if multiple active rows share it.

#### [MODIFY] `src/services/pg_auction_service.py`
- In `_dates_with_auctions()`, add `AND archived_at IS NULL` to prevent skipping valid active dates if an archived row exists for that date.

#### [MODIFY] `src/services/pg_judgment_service.py`
- In `_load_judgment_data_to_pg()`, update the query that builds the `case_map` to explicitly use `DISTINCT ON (case_number_raw) ... ORDER BY case_number_raw, auction_date DESC`. This ensures the JSON cache correctly attaches to the newest active `foreclosure_id` during overlap windows.

#### [MODIFY] `src/services/trust_accounts.py`
- In `_load_upcoming_auction_context()`, ensure `AND archived_at IS NULL` is explicitly enforced when joining against `foreclosures`.

## Verification Plan

### Automated Completeness Validation
After applying the migration and running a full pipeline execution (`uv run Controller.py`), we MUST validate the strict completeness thresholds defined in `CLAUDE.md`. We will run the exact validation SQL from the “Pipeline Success Criteria — Validation SQL” section of `CLAUDE.md`.

All queries target PostgreSQL tables: `foreclosures`, `foreclosure_title_chain`, `ori_encumbrances`.
Denominator for all ratios: `SELECT COUNT(*) FROM foreclosures WHERE archived_at IS NULL.`

**Pass criteria (with explicit SQL):**

1. **Final Judgment PDFs >= 90%**
```sql
SELECT
  COUNT(*) FILTER (WHERE pdf_path IS NOT NULL) AS with_pdf,
  COUNT(*) AS total_active,
  ROUND(100.0 * COUNT(*) FILTER (WHERE pdf_path IS NOT NULL) / NULLIF(COUNT(*), 0), 2) AS pct
FROM foreclosures WHERE archived_at IS NULL;
```

2. **Extracted Judgment Data >= 90%** — see `CLAUDE.md` "Judgment extraction coverage" query
3. **Chain of Title >= 80%** — see `CLAUDE.md` "Chain coverage" query
4. **Encumbrance Coverage >= 80%** — see `CLAUDE.md` "Encumbrance coverage" query
5. **Survival Coverage >= 80%** — see `CLAUDE.md` "Survival coverage" query

### Manual Verification
1. Open the Web Dashboard. Navigate to the History routes and the Active properties routes to verify no 500 errors occur and properties load correctly.
2. Verify `refresh_foreclosures.log` no longer attempts to push rows to a history table and confirms all active/historical rows are being enriched successfully.

---

## Implementation Record (2026-02-27)

All changes from the plan above have been implemented and applied. This section documents what was done, file by file, for review by other agents.

### 1. Alembic Setup + Migration — DONE

**New files created:**
- `alembic.ini` — Configured with DSN resolved programmatically (not hardcoded)
- `alembic/env.py` — Reads `SUNBIZ_PG_DSN` env var with default `postgresql+psycopg://hills:hills_dev@localhost:5433/hills_sunbiz` (same source as `sunbiz.db.resolve_pg_dsn()`)
- `alembic/versions/001_viewify_foreclosures_history.py` — The migration revision

**Migration `upgrade()` logic:**
1. Idempotent guard: checks `information_schema.tables` — if `foreclosures_history` is already a VIEW, returns immediately
2. Syncs orphaned history rows into `foreclosures` via `INSERT ... ON CONFLICT DO UPDATE SET` with `COALESCE` merge on all enrichment columns (judgment_data, strap, folio, property_address, step flags, etc.)
3. `DROP TABLE foreclosures_history CASCADE` — removes table + all 5 indexes (`idx_fch_auction_date`, `idx_fch_case_raw`, `idx_fch_strap`, `idx_fch_folio`, `idx_fch_case_date_unique`)
4. `CREATE OR REPLACE VIEW foreclosures_history AS SELECT *, archived_at AS moved_to_history_at FROM foreclosures WHERE archived_at IS NOT NULL`

**Migration `downgrade()` logic:**
1. Drops the view
2. Recreates physical table via `LIKE foreclosures INCLUDING DEFAULTS INCLUDING CONSTRAINTS`
3. Adds `moved_to_history_at` column
4. Recreates all 5 original indexes
5. Populates from `SELECT ... FROM foreclosures WHERE archived_at IS NOT NULL`

**Dependency analysis:** No PG functions had hard dependencies requiring drop/recreate. `fn_title_chain_gaps` references `foreclosures_history` but is SQL-language (names resolved at execution time, not definition time). All 5 indexes were auto-dropped by `CASCADE`.

**Migration executed successfully:** `uv run alembic upgrade head` — `foreclosures_history` is now a VIEW returning 1,287 archived rows. `foreclosures` table intact at 1,434 total rows.

### 2. Bootstrap Script — DONE

**File modified:** `src/db/migrations/create_foreclosures.py`

**Changes:**
- Replaced `CREATE TABLE IF NOT EXISTS foreclosures_history (LIKE foreclosures ...)` + `ALTER TABLE ... ADD COLUMN moved_to_history_at` with `CREATE OR REPLACE VIEW foreclosures_history AS SELECT *, archived_at AS moved_to_history_at FROM foreclosures WHERE archived_at IS NOT NULL`
- Removed 5 `CREATE INDEX` statements targeting `foreclosures_history` (indexes cannot exist on views; base table already has equivalent indexes)
- Updated section comment from "History table" to "History view"
- Updated module docstring to list `foreclosures_history` under Views instead of Tables
- PG functions referencing `foreclosures_history` left untouched (VIEW preserves backward compat)

### 3. Refresh Orchestration — DONE

**File modified:** `scripts/refresh_foreclosures.py`

**Step 1 rewrite (INSERT→UPDATE):**
- Old: `UPSERT_SQL` — `INSERT ... SELECT ... FROM foreclosures_history fh LEFT JOIN hcpa_bulk_parcels ... ON CONFLICT DO UPDATE`
- New: `ENRICH_BASE_SQL` — `UPDATE foreclosures f SET ... FROM foreclosures f2 LEFT JOIN LATERAL hcpa_bulk_parcels ... LEFT JOIN clerk_civil_cases ... LEFT JOIN dor_nal_parcels ... LEFT JOIN property_market ... WHERE f.foreclosure_id = f2.foreclosure_id`
- Same enrichment joins preserved (hcpa_bulk, clerk, dor_nal, property_market), same COALESCE logic
- Uses `LEFT JOIN LATERAL` with ordering (e.g., `source_file_id DESC` for latest parcel, `tax_year DESC` for latest NAL) — same as original
- Renamed constant and log messages from "upserted" to "enriched"

**Step 5.5 deleted:**
- Removed 4 SQL constants: `ENSURE_HISTORY_TABLE_SQL`, `ENSURE_HISTORY_COLUMN_SQL`, `ENSURE_HISTORY_INDEX_SQL`, `HISTORY_SYNC_SQL`
- Removed execution block and logging for "Step 5.5: synced N rows into foreclosures_history"

**Step 7 added (Rescheduled Auction Data Reuse):**
- New `RESCHEDULED_REUSE_SQL` constant
- Donor subquery: `SELECT DISTINCT ON (case_number_raw) ... FROM foreclosures WHERE archived_at IS NOT NULL ORDER BY case_number_raw, archived_at DESC NULLS LAST`
- Target: `WHERE new_f.case_number_raw = donor.case_number_raw AND new_f.archived_at IS NULL AND new_f.judgment_data IS NULL`
- **Unconditionally copied** (via COALESCE): `strap`, `folio`, `property_address`, `judgment_data`, `step_judgment_extracted`
- **Conditionally copied** `step_ori_searched`: only when `new_f.strap = donor.strap AND EXISTS (SELECT 1 FROM ori_encumbrances WHERE strap = donor.strap)`
- **Conditionally copied** `step_survival_analyzed`: only when strap matches AND `NOT EXISTS (... WHERE survival_status IS NULL)` (all encumbrances analyzed)
- Logging: `Step 7: copied enrichment data to N rescheduled auction rows`

### 4. Service Wiring (Audit Fixes) — DONE

#### `src/services/pg_auction_service.py`
- **Method:** `_dates_with_auctions()`
- **Change:** Added `AND archived_at IS NULL` to WHERE clause
- **Reason:** Prevents archived rows from blocking scraping of valid active dates

#### `src/services/pg_judgment_service.py`
- **Method:** `_load_judgment_data_to_pg()` (case_map query)
- **Change:** `SELECT DISTINCT ON (case_number_raw) case_number_raw, foreclosure_id FROM foreclosures WHERE archived_at IS NULL ORDER BY case_number_raw, auction_date DESC`
- **Reason:** During overlap windows, ensures JSON cache attaches to newest foreclosure_id

#### `src/services/pg_pipeline_controller.py` — Single-Pin Permits
- **Method:** `_select_single_pin_permit_candidates()` (line 807)
- **Change 1:** `archived_at IS NULL` filter made unconditional (was previously gated behind `self.settings.active_only`)
- **Change 2:** Added `DISTINCT ON (pin)` with `ORDER BY pin, foreclosure_id` to prevent scraping the same property twice
- **Reason:** Archived rows should never trigger permit scraping; duplicate strap = duplicate API calls

#### `src/services/pg_pipeline_controller.py` — Title Chain active_only
- **Change:** `ControllerSettings.active_only` default changed from `False` to `True`
- **CLI:** Upgraded from `store_true` to `BooleanOptionalAction` — users can pass `--no-active-only` for backfills
- **Reason:** Title chain should default to active foreclosures; historical investigations are the exception

#### `src/services/trust_accounts.py`
- **No change needed.** `_load_upcoming_auction_context()` already dynamically builds `AND archived_at IS NULL` when the column exists (line 1162: `archived_clause = "AND archived_at IS NULL" if "archived_at" in columns else ""`)

### Additional Changes From Earlier in Session (Pre-Plan)

These were implemented before the plan was written, based on the pipeline scraping audit:

#### `src/services/pg_pipeline_controller.py` — Tampa Permits Incremental Window
- **Method:** `_resolve_tampa_window()`
- **Change:** Queries `MAX(record_date)` from `tampa_accela_records`; starts from `latest - 1 day` instead of fixed 30-day lookback
- **Logging:** `Tampa permits: last record_date in DB is {date}, fetching {N} days ({start} -> {end})`
- **Fallback:** Full 30-day lookback on empty DB or `--force-all`

#### `src/services/pg_pipeline_controller.py` — County Permits Incremental OBJECTID
- **Method:** `_run_county_permits()` + new `_get_county_max_object_id()`
- **Change:** Queries `MAX(source_object_id)` from `county_permits`; adds `WHERE OBJECTID > {max}` to ArcGIS query
- **Logging:** `County permits: last OBJECTID in DB is {oid}, fetching only new records (existing {N} rows)`
- **Fallback:** Full ArcGIS pull on empty DB or `--force-all`

---

## Additional Audit Findings (2026-02-27, Round 2)

1. `scripts/refresh_foreclosures.py` still has overlap risk in Step 6 judgment ingest:
   - `_load_judgment_data()` currently builds `case_map` from `SELECT foreclosure_id, case_number_raw, strap FROM foreclosures` without `archived_at` filtering or deterministic `DISTINCT ON` ordering.
   - This can still attach extracted JSON/PDF data to the wrong row when active+archived rows share the same case number.

2. Step 7 conditional copy logic has a sequencing bug:
   - `RESCHEDULED_REUSE_SQL` checks `new_f.strap IS NOT NULL AND new_f.strap = donor.strap` for copying `step_ori_searched` / `step_survival_analyzed`.
   - In the same statement, `strap` is also being backfilled via `COALESCE(new_f.strap, donor.strap)`, but the `CASE` checks use pre-update values, so rows with null target strap can miss expected step reuse.

3. Step 7 targeting is narrower than the plan intent:
   - The update gate includes `AND new_f.judgment_data IS NULL`, so rows that are missing only `strap`/`folio`/`property_address` (but already have judgment JSON) will not receive backfill.
   - The originally described safeguard (`new_f.foreclosure_id > donor.foreclosure_id`) is also not enforced.

4. `create_foreclosures.py` is not fully backward-compatible in mixed-schema environments:
   - It now issues `CREATE OR REPLACE VIEW foreclosures_history ...` directly.
   - If an older DB still has `foreclosures_history` as a TABLE and Alembic migration has not yet run, this bootstrap path will fail because PostgreSQL cannot replace a TABLE with a VIEW via `OR REPLACE`.

5. Verification wording is internally inconsistent:
   - Section says to run the “exact validation SQL” from `CLAUDE.md`, but the listed pass criteria also include “Final Judgment PDFs >= 90%”, which is not produced by the single SQL health check in `CLAUDE.md`.
   - Add an explicit PDF ratio query (numerator/denominator) so all five criteria are directly measurable.

---

## Round 2 Fixes (2026-02-27)

All 5 findings above have been addressed:

### Finding 1 — Step 6 `_load_judgment_data()` case_map overlap — FIXED
**File:** `scripts/refresh_foreclosures.py`, `_load_judgment_data()` function

**Change:** Replaced `SELECT foreclosure_id, case_number_raw, strap FROM foreclosures` with `SELECT DISTINCT ON (case_number_raw) foreclosure_id, case_number_raw, strap FROM foreclosures ORDER BY case_number_raw, archived_at NULLS FIRST, auction_date DESC`. This deterministically selects the active row (archived_at IS NULL sorts first), and within active rows, the newest auction_date. During overlap windows, judgment JSON always attaches to the correct active foreclosure_id.

### Finding 2 — Step 7 sequencing bug (strap COALESCE vs CASE) — FIXED
**File:** `scripts/refresh_foreclosures.py`, `RESCHEDULED_REUSE_SQL`

**Change:** Replaced `new_f.strap IS NOT NULL AND new_f.strap = donor.strap` in both CASE expressions with `COALESCE(new_f.strap, donor.strap) = donor.strap`. Since PostgreSQL evaluates SET expressions using pre-update values, the COALESCE mirrors the same logic that the strap assignment uses, so rows where new_f.strap is NULL (but will be filled from donor.strap) correctly match the condition.

### Finding 3 — Step 7 targeting too narrow — FIXED
**File:** `scripts/refresh_foreclosures.py`, `RESCHEDULED_REUSE_SQL`

**Changes:**
1. Widened WHERE gate from `AND new_f.judgment_data IS NULL` to `AND (new_f.judgment_data IS NULL OR new_f.strap IS NULL OR new_f.folio IS NULL OR new_f.property_address IS NULL)`. Rows missing any enrichable field now qualify.
2. Added `AND new_f.foreclosure_id > donor.foreclosure_id` guard to prevent copying from newer to older rows.
3. Added `foreclosure_id` to donor subquery SELECT (needed for the guard).

### Finding 4 — `create_foreclosures.py` mixed-schema safety — FIXED (v2)
**File:** `src/db/migrations/create_foreclosures.py`

**Change (v1):** Initially added a bare `DROP TABLE CASCADE` in the `DO $$` block — this fixed the bootstrap failure but had a data-safety risk: legacy history rows would be destroyed without merging.

**Change (v2):** Replaced the bare DROP with a full `INSERT ... ON CONFLICT DO UPDATE` merge (mirroring the Alembic migration logic) that syncs orphan history rows back into `foreclosures` via COALESCE before dropping the table. This ensures zero data loss on the bootstrap path even when Alembic hasn't been run.

### Finding 5 — Verification wording / missing PDF SQL — FIXED
**File:** `docs/plans/data_flow_fix_plan.md` (this file)

**Change:** Added explicit SQL query for the “Final Judgment PDFs >= 90%” criterion using `pdf_path IS NOT NULL` as the numerator. The other 4 criteria reference the corresponding queries already in `CLAUDE.md`.
