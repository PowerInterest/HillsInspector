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

### Photo Placeholder Healing And UI Fallback

**Status:** DEFERRED

Incoming market photos are now filtered, but existing dirty `photo_cdn_urls`
rows do not automatically heal, stale `photo_local_paths` can remain after a
property loses all valid photos, and the web read path still only looks at the
first candidate photo.

**What needs to happen**

1. Make `property_market.photo_cdn_urls` self-healing on re-scrape instead of
   always preserving the longer existing array.
2. Clear or rewrite `photo_local_paths` when no valid CDN photos remain.
3. Update the web/photo selection path to scan for the first valid non-logo,
   non-placeholder image instead of checking only element `0`.
4. Add focused tests for PG upsert + web rendering of mixed photo arrays like
   `[logo, real_photo]`.

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

### Trust Accounts Service-Unavailable Detail

**Status:** OPEN

`PgTrustAccountsService.run()` still returns `details` for
`service_unavailable`, but `PgPipelineController._run_trust_accounts()` now
drops that detail and only returns `{"skipped": true, "reason":
"service_unavailable"}`.

**What needs to happen**

1. Preserve the underlying `unavailable_reason` in controller step output.
2. Keep the step marked as `skipped`, but do not discard the diagnostic detail.

---

### Audit Regression Test Gaps

**Status:** OPEN

Several important fixes now exist in code but still lack focused regression
tests.

**Missing targeted tests**

1. Multi-PDF judgment selection path in `PgJudgmentService`.
2. Placeholder-photo filtering through PG upsert plus web rendering.
3. Controller lock-contention path (`EX_TEMPFAIL` + `pipeline_job_runs` row).

---

## Upsert Overwrite Logging & Source-of-Truth Metadata

**Status:** OPEN — high priority
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

### Phase 1: Overwrite Detection in Python (no schema change)

Use PostgreSQL `RETURNING` + `xmax` to detect insert vs update at the upsert
call site. When `xmax != 0` (update), compare returned values against what was
sent. Log when a **non-null value is replaced by a different non-null value**
from a different source:

```
OVERWRITE property_market strap=1929084NUB00000000040A beds: 4→3 source: realtor (was: zillow)
```

Ignore harmless cases:
- `NULL → value` = gap fill (good)
- `value → same value` = idempotent (fine)
- `value → NULL` = shouldn't happen with COALESCE (bug if it does)

Implementation: a shared `UpsertResult` dataclass in `src/utils/upsert.py` that
every service returns:

```python
@dataclass
class UpsertResult:
    table: str
    inserted: int
    updated: int
    unchanged: int  # updated but no values actually changed
    overwrites: list[OverwriteEvent]  # non-null → different non-null
```

### Phase 2: `data_change_log` Table (Alembic migration)

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

### Phase 3: `last_updated_by` + `updated_at` on Key Columns

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

Revision ID: 009_data_quality
Revises: 008_add_municipal_lien_findings
Create Date: 2026-03-08
"""

from alembic import op
import sqlalchemy as sa


revision = "009_data_quality"
down_revision = "008_add_municipal_lien_findings"
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
3. Remove the dead first `_scrape_current_page` method definition.

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

### `FILE_RESTRUCTURING.md` Has Incorrect Claim About `Controller.py`

**Still valid.** Line 80 says Controller.py is "Old SQLite pipeline controller,
replaced by `pg_pipeline_controller.py`". This is wrong — Controller.py is the
canonical active entrypoint. Either delete `FILE_RESTRUCTURING.md` (the
restructuring was never executed) or correct line 80.

### ~~`sunbiz_entity_cordata` Table Missing~~

**Clarified.** The table name `sunbiz_entity_cordata` was a `db_audit.py` bug.
The string "cordata" in `sunbiz/pg_loader.py` is a filename classifier inside
`_classify_entity_member()`, not a table name. The entity job loads into
`sunbiz_entity_filings`, `sunbiz_entity_parties`, and `sunbiz_entity_events`.

**Fix:** Change `db_audit.py` line 265 from `"sunbiz_entity_cordata"` to
`"sunbiz_entity_filings"`.

### ~~`clerk_name_index` Removal~~

**RESOLVED.** Table dropped via Alembic 005, no code references remain, civil
alpha load path is intact. No follow-up needed.

---

## Permit Expansion: Plant City & Temple Terrace

**Status:** IN PROGRESS (core services + routing implemented on 2026-03-06)




**Goal:** Expand building permit coverage beyond Tampa and Unincorporated
Hillsborough County.

Plan documented in `docs/plans/2026-03-02-permit-expansion-plan.md`.
Implementation docs: `docs/guides/PERMIT_EXPANSION_PLANT_CITY_TEMPLE_TERRACE.md`.

1. Identify and reverse-engineer **Plant City** permit portal API.
2. Identify and reverse-engineer **Temple Terrace** permit portal API.
3. Build `src/services/PlantCityPermit.py` and `src/services/TempleTerracePermit.py`.
4. Implement dynamic jurisdiction routing in the pipeline.

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
