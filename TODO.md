# TODO

## 2026-03-08 Audit Follow-Up

**Status:** OPEN

These items came out of the March 8 audit / regression review and were not
already tracked elsewhere in this file.

### Market Data Source Priority Model

**Status:** SUBSUMED by "Upsert Overwrite Logging" Phase 3 below.

See the overwrite logging section for the full design including
`specs_source` column and source-priority-aware upsert logic.

---

### ~~Photo Placeholder Healing And UI Fallback~~

**RESOLVED** (2026-03-09 audit)

All 4 sub-items confirmed fixed:
1. `photo_cdn_urls` self-healing on re-scrape — `market_data_service.py` filters
   placeholders before upsert with `_filter_placeholder_photos()`.
2. `photo_local_paths` cleared when no valid CDN photos — handled in download path.
3. Web photo selection scans for first valid image — `_best_photo()` in web layer.
4. Tests exist for placeholder filtering in `test_market_data_worker.py`.

---

### Permit Sync Observability

**Status:** DEFERRED

The Plant City and Temple Terrace permit fixes corrected the overwrite
direction, but the logs still only report coarse `written` counts. They do not
show inserts vs updates or status/open-flag churn, which makes it hard to prove
that stale permit rows were actually repaired.

**What needs to happen**

1. Add insert/update counters to permit sync stats.
2. Surface status-transition counts such as `open -> closed`.
3. Log suspicious mass-update patterns so a bad upstream response is visible.

---

### Photo Download Failure Logging

**Status:** DEFERRED

Per-image photo download failures are still too quiet for production triage.
Current logs do not consistently include the property identifier or failing URL,
and failures are easy to miss.

**What needs to happen**

1. Include `strap` and failing photo URL in download-failure logs.
2. Promote repeated or terminal download failures above `debug`.
3. Add a summary counter for properties with zero successfully-downloaded
   photos after refresh.

---

### ~~Trust Accounts Service-Unavailable Detail~~

**RESOLVED** (2026-03-09 audit)

`PgPipelineController._run_trust_accounts()` now passes through the full
service result dict including `unavailable_reason` detail. The controller no
longer strips diagnostic detail from skipped steps.

---

### Audit Regression Test Gaps

**Status:** PARTIALLY COVERED

Some regression tests added; gaps remain.

**Covered:**
- Judgment loader count-gating: `test_pg_foreclosure_service.py::test_load_judgment_data_to_pg_only_counts_actual_updates`
- Placeholder-photo filtering: `test_market_data_worker.py` (placeholder filter tests exist)

**Still missing:**
1. Multi-PDF judgment selection path in `PgJudgmentService`.
2. Controller lock-contention path (`EX_TEMPFAIL` + `pipeline_job_runs` row).

---

## Upsert Overwrite Logging & Source-of-Truth Metadata

**Status:** Phase 1 COMPLETE, Phases 2-3 OPEN
**Discovered:** 2026-03-08 system audit

### The Problem

At least half the audit findings trace back to one root cause: data from
different sources silently overwrites each other and there is no record of what
changed. When beds goes from 4 to 3, or a permit status flips back to Open,
nothing in the logs or database tells you it happened, which source did it, or
what the old value was. You only find out weeks later when a dashboard number
looks wrong.

This affects every upsert-heavy table: `property_market`, `foreclosures`,
`ori_encumbrances`, `tampa_accela_records`, and `hcpa_bulk_parcels`.

### Phase 1: Overwrite Detection in Python (no schema change) — COMPLETE

`UpsertResult` and `OverwriteTracker` implemented in `src/utils/upsert.py`.
Actively used by `market_data_service.py`. Logs overwrite events when a
non-null value is replaced by a different non-null value from a different source.

### Phase 2: `data_change_log` Table (Alembic migration) — COMPLETE

For queryable audit trail beyond ephemeral logs:

```sql
CREATE TABLE data_change_log (
    id          BIGSERIAL PRIMARY KEY,
    table_name  TEXT NOT NULL,
    row_key     TEXT NOT NULL,     -- strap, folio, or foreclosure_id
    column_name TEXT NOT NULL,
    old_value   TEXT,
    new_value   TEXT,
    source      TEXT NOT NULL,     -- 'zillow', 'realtor', 'homeharvest', etc.
    changed_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_dcl_table_key ON data_change_log(table_name, row_key);
CREATE INDEX idx_dcl_changed_at ON data_change_log(changed_at);
```

Not a trigger — the Python upsert helpers emit the rows. This keeps it opt-in
per table and avoids trigger overhead on bulk loads.

**Queryable insights this enables:**
- "Show every time Realtor overwrote a Zillow value in the last 30 days"
- "Which properties had beds/baths change more than once this month?"
- "What source is most frequently overwriting others?"

### Phase 3: `last_updated_by` + `updated_at` on Key Columns — COMPLETE

Add source-tracking columns to high-value tables so the upsert itself can make
priority decisions:

```sql
ALTER TABLE property_market ADD COLUMN specs_source TEXT;
ALTER TABLE property_market ADD COLUMN specs_updated_at TIMESTAMPTZ;
```

Upsert logic becomes source-priority-aware instead of chronological:

```sql
CASE WHEN source_priority(:new_source) > source_priority(property_market.specs_source)
     THEN EXCLUDED.beds
     ELSE property_market.beds
END
```

This subsumes the "Market Data Source Priority Model" item above.

### Rollout Order

1. **Phase 1 first** — zero schema changes, immediate visibility. Every upsert
   service starts returning `UpsertResult` and logging overwrites.
2. **Phase 2 when Phase 1 shows patterns** — add the `data_change_log` table
   once you know which tables and columns have the most overwrite churn.
3. **Phase 3 for market data** — source-priority upserts for `property_market`
   first, then extend to permits and encumbrances if needed.

### Candidate Alembic Migration (Phases 2 + 3 + Confidence)

Combines the schema changes for `data_change_log`, `property_market` source
tracking, and `ori_encumbrances` confidence into a single migration. This is
the schema half — the Python `UpsertResult` helper (Phase 1) should land first.

```python
"""Add data quality tracking columns and overwrite log.

Creates:
- `data_change_log` table for tracking source overwrites
- `specs_source` and `specs_updated_at` on `property_market`
- `confidence` on `ori_encumbrances`

Retention note: data_change_log will grow proportionally to upsert volume.
Plan a periodic cleanup job (e.g. DELETE WHERE changed_at < now() - interval
'90 days') once table size is monitored in production.

Revision ID: 011_data_quality   -- NOTE: 009 and 010 are now taken by survival fixes
Revises: 010_survival_pg_functions
Create Date: 2026-03-08
"""

from alembic import op
import sqlalchemy as sa


revision = "011_data_quality"
down_revision = "010_survival_pg_functions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # 1. Create data_change_log table
    conn.execute(
        sa.text(
            """
            CREATE TABLE IF NOT EXISTS data_change_log (
                id          BIGSERIAL PRIMARY KEY,
                table_name  TEXT NOT NULL,
                row_key     TEXT NOT NULL,
                column_name TEXT NOT NULL,
                old_value   TEXT,
                new_value   TEXT,
                source      TEXT NOT NULL,
                changed_at  TIMESTAMPTZ DEFAULT now()
            )
            """
        )
    )
    conn.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS idx_dcl_table_key "
            "ON data_change_log(table_name, row_key)"
        )
    )
    conn.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS idx_dcl_changed_at "
            "ON data_change_log(changed_at)"
        )
    )

    # 2. Add specs_source and specs_updated_at to property_market
    conn.execute(
        sa.text(
            """
            ALTER TABLE property_market
            ADD COLUMN IF NOT EXISTS specs_source TEXT,
            ADD COLUMN IF NOT EXISTS specs_updated_at TIMESTAMPTZ
            """
        )
    )

    # 3. Add confidence to ori_encumbrances
    conn.execute(
        sa.text(
            """
            ALTER TABLE ori_encumbrances
            ADD COLUMN IF NOT EXISTS confidence FLOAT NOT NULL DEFAULT 1.0
            """
        )
    )


def downgrade() -> None:
    raise NotImplementedError("Forward-only migration")
```

---

## Encumbrance Confidence Scoring

**Status:** OPEN — needs design
**Discovered:** 2026-03-08 system audit

### The Problem

Inferred encumbrances, fuzzy-linked satisfactions, party-date heuristics, and
exact instrument matches all feed the same `ori_encumbrances` table with the
same weight. The equity model and dashboard treat a party-name-heuristic
satisfaction link identically to an exact instrument reference match. When the
heuristic is wrong, survived-debt totals silently inflate, and a clean property
looks toxic.

### Proposed Design

Add a `confidence` float column to `ori_encumbrances`:

| Discovery Method | Confidence | Examples |
|---|---|---|
| Exact instrument match | 1.0 | Direct ORI lookup by instrument number |
| Book/page reference match | 0.9 | Satisfaction references encumbrance by book/page |
| Case number match | 0.85 | Lifecycle doc shares case number with parent |
| Party-date heuristic | 0.7 | `_link_satisfactions()` Strategy 4 |
| Judgment-inferred | 0.5 | `infer_encumbrances_from_judgment()` |
| Legal description only | 0.4 | Found via legal desc search, no party/instrument match |

**How this changes downstream behavior:**

1. **Equity model**: Weight uncertain liens by confidence. A 0.5-confidence
   mortgage contributes 50% of its face value to survived debt instead of 100%.
2. **Dashboard**: Show confidence badges (green/yellow/red) next to
   encumbrances. Users can see which liens are solid vs speculative.
3. **Survival analysis**: Below a confidence threshold (e.g., 0.6), flag
   survival status as `UNCERTAIN` regardless of type classification.
4. **Audit buckets**: Add a "low confidence encumbrances" bucket so the
   encumbrance audit surfaces properties where manual verification is most
   needed.

### Implementation

1. Alembic migration: `ALTER TABLE ori_encumbrances ADD COLUMN confidence FLOAT DEFAULT 1.0`
2. Set confidence at write time in `pg_ori_service._save_documents()` based on
   how the document was discovered.
3. Set confidence at link time in `_link_satisfactions()` based on which
   strategy succeeded.
4. Update `compute_net_equity()` PG function to weight by confidence.
5. Update property detail encumbrance rendering to show confidence indicator.

---

## Structured Step Results (Idempotent Step Replay)

**Status:** OPEN — high priority
**Discovered:** 2026-03-08 system audit

### The Problem

Several audit bugs (#10, #15, #18) went unnoticed for extended periods because
re-running a pipeline step doesn't distinguish "nothing to do" from "silently
failed." The controller logs coarse success/failure, but there's no structured
record of what each step actually accomplished. A step that processes 0 rows
looks identical to a step that processes 500.

This also makes the `_payload_failed` duplication (issue #19) worse — five files
have slightly different notions of "did this step succeed."

### Proposed Design

Every pipeline step returns a standardized `StepResult`:

```python
@dataclass
class StepResult:
    step_name: str
    status: Literal["success", "skipped", "failed", "noop"]
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    errors: int = 0
    duration_ms: int = 0
    details: dict = field(default_factory=dict)  # step-specific extras
    overwrites: list[OverwriteEvent] = field(default_factory=list)
```

**Status semantics:**
- `success` — step ran and changed data (`inserted + updated > 0`)
- `noop` — step ran but found nothing to do (all rows already current)
- `skipped` — step was not attempted (service unavailable, flag disabled)
- `failed` — step attempted but hit errors

**Key property:** `noop` is explicitly not `success`. A step returning `noop`
10 runs in a row is suspicious and should be flagged. This catches bugs like
#10 (clerk_civil_alpha dispatched but never handled) and #15 (PDFs skipped
because directory-level JSON check blocked them).

### What This Replaces

- The 5 duplicated `_payload_failed` helpers → one `StepResult.is_failure` check
- Ad-hoc `{"success": True/False}` dicts → structured objects
- Missing logging → every step emits a one-line summary:
  ```
  STEP auction_scrape: success (inserted=12, updated=3, skipped=112, errors=0) 4.2s
  STEP clerk_civil_alpha: noop (0 rows, 0 errors) 0.3s  ← suspicious after 10 runs
  ```

### Implementation

1. Define `StepResult` in `src/utils/step_result.py`.
2. Migrate `_run_*` methods in `pg_pipeline_controller.py` to return
   `StepResult` instead of raw dicts. Start with the simplest steps.
3. Controller summary logs aggregate `StepResult` counts instead of
   `succeeded_steps` / `failed_steps` integers.
4. Add a `noop` alert: if a step returns `noop` for N consecutive runs
   (configurable, default 5), log a warning.
5. Store `StepResult` JSON in `pipeline_job_runs.result` for historical query.

---

## PDF Download: Retry Mechanism for Failed Downloads

**Discovered:** 2026-02-28
**Audited:** 2026-03-05 — PARTIALLY FIXED

### Current State

PDFs **are** being downloaded during `auction_scrape`. The original claim that
`process_final_judgments=False` blocks downloads was incorrect — a duplicate
`_scrape_current_page()` method definition in `auction_scraper.py` shadows the
first, and the second calls `_download_final_judgment()` unconditionally. The
flag only gates Vision OCR, not download.

**As of 2026-03-05:** 126/127 active foreclosures have PDFs and `step_pdf_downloaded` set.
`PgJudgmentService._load_judgment_data_to_pg()` now writes `step_pdf_downloaded`.

### Remaining Issues

1. **No retry for failed downloads.** Case `292025CC015052A001HC` (CC type, no
   strap) has no PDF and no judgment data. There is no mechanism to retry
   download for cases where the initial attempt failed.
2. **`_save_to_pg()` never writes `pdf_path`** from scraper results. The DB only
   gets `pdf_path` later when `judgment_extract` scans disk.
3. ~~**Dead code**: Two `_scrape_current_page` definitions in `auction_scraper.py`~~
   **RESOLVED** (2026-03-08 audit fix #5) — first dead definition deleted.

### What Needs to Happen

1. Add a retry/gap-fill query: `foreclosures WHERE archived_at IS NULL AND
   pdf_path IS NULL` run at the end of `auction_scrape` or as a mini-step.
2. Fix `pg_auction_service._save_to_pg()` to write `pdf_path` on successful
   download.
3. ~~Remove the dead first `_scrape_current_page` method definition.~~ **RESOLVED.**

---

## Notice Of Commencement To Permit Matching

**Audited:** 2026-03-05 — PARTIALLY FIXED

### Already Implemented

- NOC persistence in `ori_encumbrances` is complete (19 records in DB).
- Date-proximity matching exists in web layer (`_match_nocs_to_permits()` in
  `app/web/routers/properties.py`, lines 605-651).
- Both Tampa (840 events) and County (172 events) permit sources are loaded and
  merged into the matching pool via `foreclosure_title_events`.
- Full documentation in `docs/domain/NOC_PERMIT_LINKING.md`.

### Still Missing

1. **Jurisdiction-aware routing** — `_match_nocs_to_permits()` receives a flat
   mixed pool and picks by date alone. No address-to-jurisdiction classification.
2. **Richer matching signals** — no address token overlap, permit-number hints,
   contractor/builder name, or permit type matching.
3. **PG persistence of links** — matching is ephemeral (render-time only). No FK
   columns exist to store confirmed NOC-to-permit links.
4. **Unmatched NOC bucketing** — no distinction between "permit not yet pulled"
   vs "matching logic missed it".
5. **Discovery provenance** — no field tracks whether a NOC came from official
   records seed, legal search, party search, or full-text fallback.

### Next Step

Jurisdiction-aware routing: classify each property's address as Tampa vs County
(using the `city` string already in permit data or a folio-prefix rule), then
restrict the permit candidate pool before running date-proximity matching.

---

## Lis Pendens Coverage

**Audited:** 2026-03-05 — MOSTLY RESOLVED

### Current State

LP coverage is at **92.1%** (116/126 judged foreclosures) via the canonical
health-check formula (combining `ori_encumbrances` LP rows + `foreclosure_title_events`
LP/LPR subtypes). This **passes** the CLAUDE.md >=90% target.

LP recovery logic (`_find_lis_pendens_gap_targets()` with `lp_recovery_mode=True`)
is implemented and working.

### 10 Remaining Gaps

| Category | Count | Notes |
|----------|-------|-------|
| CC/HOA cases | 5 | HOA foreclosures often have no traditional recorded LP — likely legitimate absence |
| CA cases (old filings) | 4 | 2014-2023 filings, ORI searched but LP not found — may need targeted retry |
| No-strap case (fc_id 21007 / 24-CA-003727) | 1 | `strap=NULL`, `folio=NULL`, judgment address resolves to Pennsylvania — investigate if out-of-county |

### What Needs to Happen

1. Investigate fc_id 21007 — if truly out-of-Hillsborough, archive it.
2. Try targeted LP recovery retry on the 4 CA cases.
3. Accept 5 CC/HOA cases as legitimate LP absence.

---

## ~~Estate/Inherited Properties Have No Enrichment Data~~

**RESOLVED** (2026-03-05 audit)

The evidence case (`292024CA009849A001HC`) was archived on 2026-03-04. Phase 1A
(case-number search) found 2 real ORI encumbrances (LP + JUD) on 2026-03-01 —
the original claim that "Phase 1A finds no encumbrance-type docs" was wrong.

Currently 0/127 active foreclosures have zero sales history. All pipeline health
metrics pass. The risk remains theoretical for future estate cases but no action
is needed now.

---

## Housekeeping

### ~~`FILE_RESTRUCTURING.md` Has Incorrect Claim About `Controller.py`~~

**RESOLVED** (2026-03-09 audit)

`FILE_RESTRUCTURING.md` no longer exists in the repository. The file was
deleted during cleanup. No action needed.

### `sunbiz_entity_cordata` Table Missing

**Status:** OPEN — trivial fix

The table name `sunbiz_entity_cordata` is a `db_audit.py` bug.
The string "cordata" in `sunbiz/pg_loader.py` is a filename classifier inside
`_classify_entity_member()`, not a table name. The entity job loads into
`sunbiz_entity_filings`, `sunbiz_entity_parties`, and `sunbiz_entity_events`.

**Fix:** Change `db_audit.py` line ~269 from `"sunbiz_entity_cordata"` to
`"sunbiz_entity_filings"`. Still not applied.

### ~~`clerk_name_index` Removal~~

**RESOLVED.** Table dropped via Alembic 005, no code references remain, civil
alpha load path is intact. No follow-up needed.

---

## ~~Permit Expansion: Plant City & Temple Terrace~~

**COMPLETE** (2026-03-09 audit)

All 4 sub-items implemented:
1. Plant City API reverse-engineered and scraper built (`src/services/PlantCityPermit.py`).
2. Temple Terrace API reverse-engineered and scraper built (`src/services/TempleTerracePermit.py`).
3. Dynamic jurisdiction routing in pipeline (`pg_pipeline_controller.py`).
4. Both sources integrated into title chain events and web dashboard.

Docs: `docs/plans/2026-03-02-permit-expansion-plan.md`,
`docs/guides/PERMIT_EXPANSION_PLANT_CITY_TEMPLE_TERRACE.md`.

---

## New Pipeline Ingestion Targets

**Status:** NOT STARTED

### 1. Weekly Undisposed Case Snapshots (Pre-Foreclosure)
- **URL**: `https://publicrec.hillsclerk.com/Civil/undisposed/`
- **Value**: 8 weekly CSVs — direct feed of open foreclosure cases before
  judgment/auction. Pre-foreclosure lead generation.

### 2. Tax Deed Sales Excess Proceeds
- **URL**: `https://hillsborough.realtaxdeed.com`
- **Value**: Surplus funds spreadsheet after tax deed sales. Tax deeds extinguish
  subordinate liens — critical for title chain accuracy.

### 3. Cross-Agency Intelligence Scrapers
- NOC-triggered permit search, code enforcement lien-triggered Special Magistrate
  scrape, utility lien-triggered vacancy flagging.

### 4. Daily New Civil Case Filings
- **URL**: `https://publicrec.hillsclerk.com/Civil/dailyfilings/`
- **Value**: 30 daily CSVs for ultra-low-latency new foreclosure/HOA lien alerts.

### 5. Delinquent Utility Bills (Water & Power)
- **Value**: Unpaid water/sewer/electric bills are unrecorded municipal liens
  that survive foreclosure under FL Ch. 159 and transfer to the auction buyer.
  These do not appear in official records and cannot be discovered via ORI.
- **Sources to investigate**:
  - Tampa Water / City of Tampa Utilities — account balance lookup by address
  - TECO (Tampa Electric) — delinquency or shutoff records
  - Hillsborough County Water Resources — unincorporated area utility accounts
- **Risk**: A property with $5K+ in unpaid water/sewer is common on distressed
  foreclosures. The buyer inherits the balance. This is one of the most
  frequently missed liabilities at auction.
- **Implementation**: Scraper per utility provider, keyed by property address.
  Store as unrecorded liens in a new table or flag on the property record.

---

## ~~"Auction Today" Dashboard Tab~~

**BLOCKED** — now the Auction Intelligence tab. Trust account data foundation
exists in `pg_web.py`. The remaining intelligence flags (Toxic Bid Alert,
Anomalous Valuation, etc.) depend on correct net equity calculations, which are
still being refined (per diem accrual, survived lien totals). No further UI work
needed until equity numbers are validated.

---

## Encumbrance Coverage Gaps (Remaining Work)

**Context:** A 2026-03-03 encumbrance gap analysis identified systemic issues in
ORI document discovery. Seven fixes were implemented (category-aware Phase 3,
Phase 1B+ lifecycle chain, type normalizer fixes, date backfill, offset widening,
PG-only satisfaction linking, SA/CEL/SPECASMT mapping).

**Audited:** 2026-03-05 — Gaps 1, 2, 3, 4 all RESOLVED. See
[Encumbrance Linking](docs/domain/ENCUMBRANCE_LINKING.md) for full algorithm
documentation.

### ~~1. Phase 0 PG Seed Expansion~~

**RESOLVED** (2026-03-05)

Added `doc_type IN (...)` filter to `_seed_from_official_records()` SQL covering
all 41 encumbrance-relevant types from [DOC_TYPES.md](docs/domain/DOC_TYPES.md)
(mortgages, judgments, liens, LP, satisfactions, releases, assignments, court
papers, NOC, MOD, SUB, NCL, EAS). Noise types (deeds, GOV, POA, NOT, BND, AFF,
CP, etc.) excluded. Reduces candidate pool from ~103K to ~56K rows before the
400-row LIMIT is applied.

### ~~2. Satisfaction Linking: Party/Date/Amount Heuristic~~

**RESOLVED** (2026-03-05)

Implemented Strategy 4 (`party_date_heuristic`) in `_link_satisfactions()`:
- `rapidfuzz.fuzz.token_set_ratio >= 85` for party name matching
- Date guard: SAT must be recorded after encumbrance
- Ambiguity guard: only links when exactly 1 candidate matches
- PG enum `satisfaction_link_method` already included `'party_date_heuristic'`

### ~~3. Survival & Title Breaks Pipeline Ordering and Force Flag~~

**RESOLVED** (2026-03-05, earlier session)

All 3 bugs fixed in `pg_pipeline_controller.py`:
1. `force_reanalysis=self.settings.force_all` now passed to `PgSurvivalService.run()`.
2. Survival step reordered after encumbrance recovery.
3. `step_survival_analyzed` cleared for foreclosures augmented by recovery.

### ~~4. Lifecycle Doc Reference Linking~~

**RESOLVED** (2026-03-05)

1. Alembic migration `007_add_mod_link` adds `modifies_encumbrance_id BIGINT
   REFERENCES ori_encumbrances(id)`.
2. `_link_modifications(strap)` implemented using instrument reference,
   book/page reference, and case number match strategies. Wired after
   `_link_satisfactions()` in per-property ORI flow.
3. Property page nesting of lifecycle docs under parent is future work (low
   priority — data linkage is in place).

---

## Market Data Source-Priority Model (Architectural)

**Discovered:** 2026-03-08 system audit (exposed by fix #6)
**Status:** OPEN — needs design

### The Problem

All three market-data upserts (`_upsert_homeharvest`, `_upsert_zillow`,
`_upsert_realtor`) use COALESCE-based "first non-null wins" for property specs
(beds, baths, sqft, year_built). There is no source-priority awareness.

The intended priority hierarchy is: **HomeHarvest > Zillow/Redfin > Realtor**.

- HomeHarvest: `COALESCE(EXCLUDED, existing)` — incoming wins (correct, highest priority)
- Zillow/Redfin: `COALESCE(existing, EXCLUDED)` — existing wins (preserves HomeHarvest)
- Realtor: `COALESCE(existing, EXCLUDED)` — existing wins (preserves Zillow/Redfin)

This works **only if scrapes execute in priority order**. If Realtor or
HomeHarvest inserts inferior specs first (because Zillow timed out yesterday),
Zillow's COALESCE sees non-null existing values and refuses to overwrite them
with superior data. The specs are permanently locked to whichever source
succeeded first.

This flaw predates all audit fixes. Fix #6 (Realtor COALESCE correction) is
correct — it stopped Realtor from actively overwriting Zillow. But it made the
broader chronological-order dependency visible.

### Proposed Fix

Add a `specs_source` column (or similar) to `property_market` that tracks which
source last wrote the spec fields. Upsert logic checks source priority before
deciding whether to overwrite:

```sql
-- Only overwrite if incoming source has higher priority
CASE WHEN source_priority(:new_source) > source_priority(property_market.specs_source)
     THEN EXCLUDED.beds
     ELSE property_market.beds
END
```

This decouples data quality from scrape execution order.

---

## Future Hardening Notes (from 2026-03-08 Audit)

Low-priority defensive improvements identified during the system audit.
Not active bugs — tracked here so they don't get lost.

### Permit Upsert: Null-Value Overwrite Guard

Both Plant City and Temple Terrace scrapers currently normalize empty strings to
None (`PlantCityPermit.py:41`, `TempleTerracePermit.py:104`) before building
upsert rows. The COALESCE-first pattern (`COALESCE(EXCLUDED, existing)`) means a
NULL incoming value harmlessly falls through to the existing value. However, if a
future parser change skips normalization and passes empty strings, the COALESCE
would treat `''` as non-null and overwrite real data with blanks.

**Guard:** If permit parsers are ever refactored, ensure empty-string → None
normalization is preserved at the boundary, or add explicit `NULLIF(EXCLUDED.field, '')`
in the upsert SQL.

### Generic Name Word-Boundary Edge Case

The `_is_generic_name()` fix (audit #21) uses `\b` word boundaries in regex. The
`\b` anchor sits between a `\w` and `\W` character. If a future generic term
ends with punctuation (e.g., `"INC."` with trailing dot), the `\b` after the dot
wouldn't fire because dot-to-space is `\W`-to-`\W`. Current generic terms list
does not contain such forms.

**Guard:** If adding new generic terms, use only alphanumeric entries (no trailing
punctuation). Or switch to token-split matching if the list grows complex.

### Permit Observability Gap

Plant City and Temple Terrace permit services log coarse `written` counts but do
not surface insert vs update counts or status/open-flag churn. Future regressions
in the COALESCE direction would be hard to prove from logs alone.

**Guard:** Add `xmax = 0` counting after upsert to distinguish inserts from
updates, or log status-change counts when `is_open` flips.

https://hillsborough.realforeclose.com/index.cfm?ZACTION=HOME&ZMETHOD=RESETPW&urlrc=7AC2F22AFF6F4AA491AC604D9B07B62A
Dear Bidder:

Welcome to the Hillsborough County Clerk of Court’s online sale of foreclosed properties. We appreciate your participation in this revolutionary auction process, which allows us to ensure fairness and increase the number of potential buyers for each property. We believe this will be a benefit to bidders and property owners alike.

Please review the information on how the proceedings work before getting started. It’s a good idea to read through the Frequently Asked Questions section. You are required to register before placing your bid, so be sure to complete the administrative portion first.

The online sale offers you the opportunity to review relevant property information from the comfort of your home or office or wherever you have access to the Internet. Updated property information will be added to the website as it becomes available, and you may place your bid at a time that is convenient to you.

Again, thank you for joining us as we become one of the counties in Florida to offer online bidding for foreclosed property sales. If you have any questions about the online sales, please contact customer service at 877-361-7325 or by email to customerservice@realauction.com.  If you have questions about the Clerk of Courts or court cases, please contact our office at 813-276-8100 Extension 4789 or email to Foreclos@hillsclerk.com.

Sincerely,

Victor Crist
Clerk of the Circuit and County Courts
Hillsborough County, Florida


 
This User Agreement governs the relationships between the User (hereinafter “User”, “You”, “Your", or “Applicant”), Realauction.com, L.L.C. (hereinafter “Realauction”) and the Hillsborough County Clerk of Court, (hereinafter “Clerk”), concerning the online purchase of foreclosure property offered by the Clerk of Courts of Hillsborough County.  The term, "User", includes any individual or entity using this website, including but not limited to all bidders, property owners and their employees, agents, and legal representatives.   Before using this site to access or change information about and/or purchase or attempt to purchase real property, you must agree that you have read, understand and agree to be legally bound by the terms and conditions of this User Agreement by clicking on the “I Agree” button above.

License for Use of Information. By completing the registration process and agreeing to the terms and conditions of this User Agreement, the User will be granted a non-exclusive license to access and use the information that appears on this website for the sole purpose of purchasing or attempting to purchase real property foreclosed upon and offered for sale by the Clerk. This license is granted to the User personally and to the User’s employees and agents, and may not be transferred to any third party other than the Clerk and Realauction. The software used to create and maintain information on Realauction is the sole and exclusive property of Realauction, and neither the software nor the information presented by Realauction may be used, altered, sold or distributed by the User in any way, without the express written consent of Realauction.
User Representations and Warranties
User shall be solely responsible for the protection and use of the User IDs and passwords obtained from Realauction, and will notify Realauction immediately in the event of theft or unauthorized use of IDs and passwords.

User will follow all instructions for use posted on the website and agrees to all instructions for registraton and participation in auctions and all bidding rules posted on the website as well as all state and federal laws and county ordinances pertainng to the foreclosure process.  User agrees that said instructions, bidding rules, and state and federal laws and county ordinances pertaiing to the foreclosure sale process are terms and conditions of this agreement and are made a part of this agreement.

User acknowledges that all properties are sold without warranties and that User is solely responsible for performing any and all research necessary regarding the condition, marketability, existing or potential users, title, or encumbrances of the existence of any condition, zoning regulations or law that may affect current or future use of the property, regarding any property and structures or fixtures thereon offered for sale by the Clerk and Realauction.

User acknowledges and understands that there is no guarantee that any given property offered for sale by the Clerk will actually be sold by the Clerk and that there is no guarantee that the User will be the successful purchaser of such property.

User warrants that they are of legal age to enter into binding contracts, and/or that they are legaly authorized to act as an agent on behalf of any employer or principal to whom a license for use of the informaton presented by Realauction has been granted.

User represents and warrants that all information provided by User to the Clerk and to Realauction for the purpose of attempting to purchase real property offered for sale by the Clerk is truthful and accurate to the best of their knowledge.  User agrees to request changes to any registraton information by submitting and request to Realaucton as needed.  Clerk and Realauction are entitled to rely on User's registration information as provided for all transactions and notices unless and until changes are made.

User will not violate any laws or regulations in participating as a registrant or bidder in any auction or in the transfer of any funds required to register as a user or complete any sale.  User will not engage in any process, practice or conduct that manipulates, influences or controls any auction including but not limited to collusion or agreements with other bidders or the use of any automated means, technology or devices such as robot , spiders, or scrapers to place bids or prevent others from placing bids.

User acknowledges that all User registration records and records of sales are public records pursuant to Chapter 119, Florida statutes unless exempt as provided in Florida or Federal law.  User acknowledges that they must notify the Clerk or Realauction if their information on realauction is to be maintained as exempt from disclosure as a public record pursuant to Florida or Federal law.

User warrants that they have read all information provided by the Clerk and Realauction pertaining to the process of attempting to purchase real property, and that they understand and accept the obligatons described herein, before User participates in the application process.

USER AGREES TO ABIDE BY ALL TERMS AND CONDITIONS OF SALE CONTAINED HEREIN AND ON ANY OTHER PAGE OR SITE CONNECTED WITH THE SALE OF REAL PROPERTY OFFERED FOR BY THE CLERK.  SUCH TERMS AND CONDITIONS INCLUDE, BUT ARE NOT LIMITED TO:

 Foreclosure Sales:  Prior to the start of the sale, each participant (except Plaintiff) wishing to place a successful bid on a property must post with the Clerk a deposit of 5 percent of the anticipated high bid for each item on which they would like to bid.  Advance deposits may be made on the website via domestic wire or electronic check (ACH).  If you choose to place your funds on deposit by ACH deposit, PLEASE NOTE:  Deposit payments made via ACH require 5 full business days to arrive (settle) in the Clerk's account.  ACH deposits and wires will not be available for bidding until such funds have cleared.  All wire payments must include an additional $ 4.00 wire fee and all wire transfers must contain the bidder number or the wire transfer will be refused.  Advance deposits may also be made in person by cash or cashier's check at the Hillsborough County Clerk of Circuit Court Customer Service Center, located at 800 E. Twiggs Street, Room 101, Tampa, FL 33602, or at one of our satellite office which may be more convenient to you.  This includes our Plant City office at 301 North Michigan Ave, Plant City, FL 33363 or our Brandon office at 311 Pauls Drive, Brandon, FL 33511.  Deposits made in prerson must be made by 4:00 PM the day prior to the sale.  CASHIER'S CHECKS SHALL NOT BE MORE THAN SIX (6) MONTHS OLD, and shall be payable to VICTOR CRIST, CLERK OF THE COURT.  Other restrictions may apply, please check with the Hillsborough County Clerk's office for all rules regarding deposits.  The high bidder's deposit is nonrefundable in the event full payment is not made.

Settlement of Sums Due: If User is determined to be the successful high bidder, User agrees to pay the balance of the sale price by 12:00 PM EST, the next business day.  In addition, the successful bidder must also pay fees to the Clerk equal to three percent (3%) of the first five hundred dollars ($ 500.00) of the sale price, plus one and one-half percent (1.5%) for each subsequent one hundred dollars ($100.00) of the remainder of the sale price, pursuant to Section 28.24, Florida Statutes, or as otherwise provide for therein, plus any other applciable fees.

A User who is determined to be the successful high bidder on any given property may not cancel the purchase, and any successful high bidder who attempts to do so shall forfeit any deposits paid to or debited from the bidder's bank acount by the Clerk.

Sale Changes/Cancellations:  User acknowledges that the Clerk reserves the right to correct, revise, add to, delete and/or modify any information regaridng any property being offered for sale, and may cancel the sale of any property at any time, including after the bidding has been completed.  User further acknowledges that all sales are subject to cancellation by the Clerk or court order or by any action the occurrence of which requires cancellaiton prior to auction due to operation of law, regulation or court rule, such as owner redemption or the filing of bankruptcy proceedings.  User acknowledges that properties sold pursuant to applicaiotn for tax deed sale may be set aside by owner redemption, stays due to the filing bankruptcy proceedings or court order.
Refunds:  Upon request of the User, through methods set forth on the website or in writing, good faith deposits held by the Clerk will be refunded to User after receipt of the request for the same.
Deposit Accounts with the Clerk
Florida Statutes authorize the Clerk to set up and maintain deposit accounts for the purpose of facilitating qualified bidders’ payment of the nonrefundable deposit of 5 percent of the successful bid amount on the date of the sale.

The Clerk will establish a deposit account for each Bidder otherwise qualified by Realauction to participate in the auction bid process described in this User Agreement.

Bidder deposits will be maintained in the Clerk’s deposit account with Wells Fargo Bank. Funds on deposit will not earn interest during the period of retention.

Only a Bidder’s cleared funds will be authorized for payment of the required deposit amount or withdrawal. Bidder funds that have not cleared will not be available for payment of the deposit or withdrawal.

All deposits of funds, withdrawal of funds and payments of funds from a Bidder’s deposit are governed by the terms established by Wells Fargo Bank for the Clerk’s deposit account and all state and federal banking regulations and state and federal laws.

Any charge or assessment required or authorized by Wells Fargo Bank against funds deposited by a Bidder will be deducted from the Bidder’s funds on deposit with the Clerk.

All service charges authorized by law that result from any action on the part of the Bidder or by another person acting on behalf of the Bidder will be deducted by the Clerk from a Bidder’s funds deposited in the Clerk’s deposit account, without any prior authorization of the Bidder.

The Clerk will provide any required notice to a Bidder at the last known address provided by the Bidder. It is the responsibility of the Bidder to maintain current address information with the Clerk through Realauction.

If a Bidder maintains funds on deposit with the Clerk under the terms of this User Agreement and the Bidder has not terminated the User Agreement and there has been no deposit, payment or withdrawal activity for a period of one year, the Clerk will notify the Bidder that the funds on deposit may be considered “unclaimed funds” under Florida law and the Clerk will treat the deposit funds as “unclaimed funds” under Florida law.

Payment of the required deposit funds of a successful Bidder will be made on behalf of the Bidder only and for no other person or entity, irrespective of any contrary instructions from the Bidder.

A Bidder deposit with the Clerk may not be assigned or transferred in any manner to another person or entity. A new Bidder deposit must be established with the Clerk by every person or entity that wants to bid as part of the electronic sales conducted by Realauction.

At all times while the Clerk is holding Bidder deposit funds: the Clerk may exercise their rights under common law or statutory set off; the Bidder deposit funds may be subject to garnishment under state garnishment laws; the Bidder funds may be subject to common law or statutory liens including IRS liens.

The Clerk’s and Realauction’s Disclaimer and Limitation of Liability and Disclaimer of Warranties
ALL INFORMATION PROVIDED BY THE CLERK AND BY REALAUCTION IS BELIEVED TO BE CORRECT AND ACCURATE WHEN POSTED. PLAINTIFFS, THEIR EMPLOYEES, AGENTS AND LEGAL REPRESENTATIVES ARE SOLELY RESPONSIBLE FOR REVIEWING ALL INFORMATION POSTED, TO MAKE CHANGES WHERE AUTHORIZED, AND TO NOTIFY THE CLERK AND REALAUCTION OF ANY REQUESTED CHANGES TO INFORMATION WHICH MAY BE MADE ONLY BY THE CLERK AND/OR REALAUCTION. "Neither the Clerk nor Realauction shall be liable for any claim of loss alleged to have resulted from any errors, omissions or inaccuracies concerning any of the information posted on this site."

THE CLERK AND REALAUCTION MAKE NO WARRANTIES OR REPRESENTATION ABOUT THE CONDITION, MARKETABILITY, EXISTING OR POTENTIAL USES, TITLE, OUTSTANDING LIENS, MORTGAGES OR OTHER ENCUMBRANCES, ZONING REGULATIONS OR LAWS THAT MAY AFFECT CURRENT OR FUTURE USES OF THE PROPERTY, OR EXISTENCE OF ANY CONDITIONS REGARDING ANY PROPERTY AND STRUCTURES OR FIXTURES THEREON OFFERED FOR SALE BY THE CLERK. The Clerk and Realauction make no express or implied warranty regarding any property offered for sale by the Clerk. The Clerk and Realauction disclaim any warranty of merchantability or fitness for a particular purpose. It is the sole responsibility of the bidder to perform any and all research necessary regarding the condition, marketability, use of, and current state of the title to any property offered for sale by the Clerk.

User agrees that (1) the maximum liability of the Clerk and/or Realauction shall be limited to the amount of deposit or down payment, sales price and/or additional fees actually paid to the Clerk by User at any time during the process of attempting to purchase real property offered for sale by the Clerk; and (2) the Clerk and Realauction shall not be liable for consequential damages of any kind, including, but not limited to, any anticipated return of or on investment in any form.

The Clerk and Realauction disclaim any warranty that the website and information presented on it are free of viruses or other forms of malicious code. Neither Realauction nor the Clerk shall be liable for any loss or damage resulting from voluntary shutdown of the website by Realauction to address computer viruses, denial- of-service attacks, or other similar problems. Neither Realauction nor the Clerk shall be liable for any damage to User's property alleged to have resulted from User's use of the website.

Neither the Clerk nor Realauction shall be liable for any loss resulting from a cause over which Clerk and/or Realauction do not have direct and exclusive control, including but not limited to problems concerning the Internet; unauthorized interception of email; computer and communications equipment and software; malicious and/or criminal acts of unauthorized users and other third parties; acts of war, terrorism, insurrection or revolution; acts of God; or any similar event.

Neither the Clerk nor Realauction shall be liable for interruptions to access of information or for suspensions in the foreclosure sale process caused by website maintenance or the need to alter and update information provided by the Clerk. Such interruptions and suspensions will occur at the sole discretion of the Clerk and Realauction.

Termination/Exclusion of Use
The Clerk and Realauction, maintain the right to terminate this User Agreement and to revoke the license granted to User in the event User breaches this User Agreement or fails to meet their obligations under this User Agreement.

The Clerk may exclude User from the foreclosure sale process and from access to the website if User fails to furnish any information or pay any fees required by the Clerk, including, but not limited to drivers license number, state ID, tax identification information or email addresses.

Except with the prior written approval of Realauction, User agrees not to use any software program, application or any other device to (a) access or log on to the website; (b) automate any process or functions available on the website; (c) transfer or transmit any information or functionality; (d) submit or modify bids; or (e) automate functions that normally require a mouse-click.  User agrees not to use, without limitation, any "robot", "spider", or other device or utility to monitor or copy any pages of the website or any content or informaton accessible through this Site.  User agrees not to use any device, software or routine to interfere with proper operations of the website.  Users should be forewarned that it may be hazardous to attempt bid submission using custom-built software devices that are designed to work with certain web pages of the website, particularly when based on a version of the HTML or GUI in use at any point prior to the sale.  Users may employ applications commonly known as web browser software; provided however that the browser software is not capable of performing the functions or processes describe and prohibited herein.
Indemnity. User agrees to defend, indemnify and hold both the Clerk and Realauction harmless against all claims by third parties for damages arising from User’s deposit and maintenance of funds in the Clerk’s deposit account, and the use of the license and information in the process of attempting to purchase real property offered by the Clerk, including all claims alleging that the Clerk or Realauction was solely negligent for those damages.
Governing Law/Venue/Jurisdiction. This Agreement shall be governed by and interpreted in accordance with the laws of the State of Florida, without regard to its choice of law provisions. Exclusive venue for any litigation arising under this User Agreement shall be in the state and/or federal courts of Hillsborough County, Florida. User agrees to submit to the personal jurisdiction of the state and/or federal courts of Hillsborough County, Florida.
Assignment. This Agreement may not be assigned by User without the prior written consent of both the Clerk and Realauction. This Agreement shall be binding upon and inure to the benefit of the parties hereto and their respective successors and assigns.
Entire Agreement/Severability. This Agreement contains the entire agreement between the parties. In the event any one or more of the provisions of this Agreement shall be deemed to be invalid, illegal or unenforceable, the remaining provisions shall remain valid and enforceable.
Amendments. This User Agreement may be amended at any time by the Clerk or Realauction, through a written or electronic notice to users. User will be required to agree to any such amendment(s) to this User Agreement as a condition of future access and use when such amendments are posted on the website, and failure to agree to such amendments will terminate this User Agreement and may exclude User from using the website and/or participating in the auction of real property offered for sale by the Clerk.

By using this web site, you have read, understand and agree to be legally bound by the terms and conditions of this User Agreement.

I have read, understand and agree to be legally bound by the terms and conditions of this User Agreement.


COUNTER PAYMENT INSTRUCTIONS

 

In order for a counter payment to be processed, ALL bidders must create a payment batch on this website indicating "Counter Payment" as the transaction type.  When your payment is received by the Clerk's office you must provide both your bidder account number and the payment batch number as well otherwise your payment WILL NOT be applied.  It is recommended that you print the "Payment Instructions" form that is provided on this website which will include the requested information.

If you need further assistance, please contact Realauction Customer Service at 1-877-361-7325.


 
Important Message Regarding ACH Transaction

Effective immediately the bank account information associated with ACH transactions will be subject to a verification process to determine its validity. If there is an issue with the account information entered, there will be an immediate response displayed on the screen prompting you to contact the Customer Service department of Forte, the vendor providing this service. Once an account is deemed valid, and Forte advises that the account has been cleared, you will need to contact the Realauction Customer Service Center at 1-877-361-7325 with your bank account information, so it can be cleared on our end as well. Upon completion of this process for the account, it will not have to be repeated each time.

Update to Refund Process

Please be advised that if your bidder account includes a Company Name, ALL refund checks will be issued in that name.  If there is no Company Name indicated then refund checks will be issued in the first and last name of the registered bidder.  To ensure that the check is issued accurately make certain that the Company Name or Bidder Name (whichever applies) are valid and accurate. 


 
Wire Transfers for Deposits

Effective September 25, 2023 the Clerk's office is adding wire transfers as another method for submittig your funds to participate in any auction of interest.  In order to initiate a wire transfer you MUST indicate your intent by logging onto the auction site, clicking on the Make Deposit link, and selecting "wire transfer".  That will allow you to obtain the wire instructions needed in order to initiate the transaction at your bank.  This action will also notify the Clerk's office to expect the transfer which will expedite the posting of the funds to your bidder account.

Alll wire transfers are subject to a $ 4.00 wire fee and must be added to the amount of your deposit.  Also, you MUST include your bidder number in the reference line of the wire to avoid refusal of the transfer and to ensure timely posting of your funds.

If you should have any questions regarding the implementation of this new option please contact the Realauction Customer Service Center at 1-877-361-7325 or via email at customerservice@realauction.com.

	
 
New Bank Information

The Clerk's office has advised that effective immediately the bank information needed to initiate a wire deposit or payment has changed.  If your intent is to fund your bidder account or submit your final payment for a winning bid via wire transfer, make certain you have the new bank information for your funds to be transmitted to the Clerk's bank account in a timely manner.

To ensure you have the updated information you MUST go through the Make Deposit link when initiating a deposit and the Make Payment link when submitting final payment for a winning bid.  That will allow you to retrieve the new wire transfer information to provide to your bank.

	
My Payments
My Payments
Total Amount Owed		$0.00
Payments Made		$0.00
Total Due		$0.00
Total Items		0
Items Due		0
Items Paid		0
Items Canceled		0
Items Defaulted On		0
My Deposits
My Deposits
Funds Available		$0.00
Funds Pending		$0.00
Funds Held		$0.00
Funds Used		$0.00
Funds Refunded		$5,000.00
Funds Rejected		$0.00
My Funds History
My Fund's History
Refund Requested	$5,000.00	09/17/2014
Cash Deposit	$5,000.00	03/15/2013
My Auction Summary
My Auction Summary
Active Bids/Proxys		0
Auctions Won		0
Auctions Lost		4
Next Auction with Active Bid		No Active Bids
Messages
Messages
Welcome to Hillsborough Foreclosure Sale	03/07/2013


Subject: Welcome to Hillsborough Foreclosure Sale

Dated: 03/07/2013

Message: Welcome to Hillsborough Foreclosure Sale . Your bidder ID for this county is 7384

https://hillsborough.realforeclose.com/index.cfm?ZACTION=HOME&ZMETHOD=TRAINING