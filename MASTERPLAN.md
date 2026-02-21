# HillsInspector MASTERPLAN

**Updated: 2026-02-21 (full reality sync for agent startup)**

## 0) Agent Startup Context

This document is the startup source of truth for active work.

- Canonical pipeline entry point: `Controller.py`
- Canonical operational database: PostgreSQL
- Success is measured by **data completeness thresholds**, not by "no exceptions"
- Legacy commands (`main.py --update`, `--run-step`) are not valid workflow

Critical runtime behavior:

- Steps `hcpa_suite`, `clerk_bulk`, `dor_nal`, `sunbiz_flr`, `sunbiz_entity`, `county_permits`, `tampa_permits` are dispatched as background workers.
- Step `market_data` is also background-dispatched.
- A `Controller.py` summary can return before those workers finish, so completion must be verified via logs + table-level validation.

## 1) Mission

Build and operate a single PG-first foreclosure intelligence pipeline that answers:

- Is this property investable at auction?
- What encumbrances survive the sale?
- What is realistic net equity after judgment + surviving debt?

## 2) Canonical Pipeline Architecture

## Phase A: Bulk Refresh

| # | Step Name | Implementation | Primary Outputs | Execution Mode |
|---|-----------|----------------|-----------------|----------------|
| 1 | `hcpa_suite` | `sunbiz.pg_loader.load_hcpa_suite` | `hcpa_bulk_parcels`, `hcpa_allsales`, related HCPA tables | background worker |
| 2 | `clerk_bulk` | `PgClerkBulkService.update()` | `clerk_civil_cases`, `clerk_civil_parties`, events/index tables | background worker |
| 3 | `dor_nal` | `PgNalService.update()` | `dor_nal_parcels` | background worker |
| 4 | `sunbiz_flr` | `PgFlrService.update()` | `sunbiz_flr_*` | background worker |
| 5 | `sunbiz_entity` | `load_sunbiz_entity` | `sunbiz_entity_*` | background worker |
| 6 | `county_permits` | `CountyPermitService.sync_postgres()` | `county_permits` | background worker |
| 7 | `tampa_permits` | `TampaPermitService.sync_date_range()` | `tampa_accela_records` | background worker |
| 8 | `foreclosure_refresh` | `PgForeclosureService.refresh()` | `foreclosures` hub refresh | inline |
| 9 | `trust_accounts` | `PgTrustAccountsService.run()` | `TrustAccount`, `TrustAccountSummary` | inline |
| 10 | `title_chain` | `TitleChainController.run()` | `foreclosure_title_events`, `foreclosure_title_chain`, `foreclosure_title_summary` | inline |

## Phase B: Per-Auction Enrichment

| # | Step Name | Implementation | Primary Outputs | Execution Mode |
|---|-----------|----------------|-----------------|----------------|
| 11 | `auction_scrape` | `PgAuctionService.run()` | refreshed auction rows in `foreclosures` | inline |
| 12 | `judgment_extract` | `PgJudgmentService.run()` | `foreclosures.judgment_data`, `step_judgment_extracted` | inline |
| 13 | `ori_search` | `PgOriService.run()` | `ori_encumbrances`, `step_ori_searched` | inline |
| 14 | `survival_analysis` | `PgSurvivalService.run()` | `ori_encumbrances.survival_status`, `step_survival_analyzed` | inline |
| 15 | `final_refresh` | `scripts.refresh_foreclosures.refresh()` | recomputed foreclosure hub metrics | inline |
| 16 | `market_data` | `dispatch_market_data_worker()` | `property_market` + post-market refresh | background worker |

## 3) Current Implementation Reality (Code-Verified)

Implemented and wired:

- PG-first controller orchestration is active in `Controller.py` + `src/services/pg_pipeline_controller.py`.
- ORI search and survival analysis write to `ori_encumbrances` via `PgOriService` + `PgSurvivalService`.
- Property detail (`app/web/routers/properties.py`) now loads encumbrances from `ori_encumbrances` and computes:
  - `liens_total`
  - `liens_surviving` (SURVIVED + UNCERTAIN)
  - `est_surviving_debt`
  - `net_equity`

Still incomplete (high-priority web/product gaps):

- Market tab/detail payload is still largely placeholder-shaped in `_pg_market_snapshot` and not a direct `property_market` representation.
- `property.sources` provenance is returned as `[]`.
- NOCs are returned as `[]` in permits view.
- Tax tab lien rows are returned as `[]`.
- Chain tab sets `document_id = None` for every chain item, so no chain document links render.
- Several auction/judgment fields are hardcoded `None` (`plaintiff`, `plaintiff_max_bid`, `foreclosure_type`, `lis_pendens_date`).
- Encumbrance UI metadata flags `is_joined` / `is_inferred` are always `False` today.

Operational gap:

- Controller run summaries are not persisted to `pipeline_runs` / `pipeline_run_steps` tables yet.
- No scheduler-owned, auditable daily run loop is implemented.

## 4) Definition of Done: Completeness Gates

A run is successful only if all gates are met.

Target gates:

- Final Judgment PDFs: >= 90% of active foreclosures
- Extracted judgment data: >= 90% of active foreclosures with PDFs
- Chain coverage: >= 80% of active foreclosures with judgment data
- Encumbrance coverage: >= 80% of active foreclosures with judgment data and strap
- Survival coverage: >= 80% of active foreclosures with judgment data and strap

Validation commands/queries:

```bash
# Count foreclosure case folders with at least one PDF
uv run python - <<'PY'
from pathlib import Path
root = Path("data/Foreclosure")
with_pdf = sum(1 for d in root.glob("*/documents") if any(d.glob("*.pdf")))
print(with_pdf)
PY
```

```sql
-- Denominator: active foreclosure rows
SELECT COUNT(*) AS active_foreclosures
FROM foreclosures
WHERE archived_at IS NULL;

-- Judgment extraction coverage
SELECT
  COUNT(*) FILTER (WHERE judgment_data IS NOT NULL) AS with_judgment_data,
  COUNT(*) AS total_active,
  ROUND(
    100.0 * COUNT(*) FILTER (WHERE judgment_data IS NOT NULL) / NULLIF(COUNT(*), 0),
    2
  ) AS pct_with_judgment_data
FROM foreclosures
WHERE archived_at IS NULL;

-- Chain coverage for active foreclosures with judgment data
WITH scope AS (
  SELECT foreclosure_id
  FROM foreclosures
  WHERE archived_at IS NULL
    AND judgment_data IS NOT NULL
)
SELECT
  COUNT(DISTINCT c.foreclosure_id) AS covered,
  (SELECT COUNT(*) FROM scope) AS total,
  ROUND(
    100.0 * COUNT(DISTINCT c.foreclosure_id) / NULLIF((SELECT COUNT(*) FROM scope), 0),
    2
  ) AS pct
FROM foreclosure_title_chain c
JOIN scope s ON s.foreclosure_id = c.foreclosure_id;

-- Encumbrance coverage for active foreclosures with judgment data + strap
WITH scope AS (
  SELECT DISTINCT foreclosure_id, strap
  FROM foreclosures
  WHERE archived_at IS NULL
    AND judgment_data IS NOT NULL
    AND strap IS NOT NULL
)
SELECT
  COUNT(DISTINCT s.foreclosure_id) FILTER (WHERE oe.id IS NOT NULL) AS covered,
  COUNT(DISTINCT s.foreclosure_id) AS total,
  ROUND(
    100.0 * COUNT(DISTINCT s.foreclosure_id) FILTER (WHERE oe.id IS NOT NULL)
    / NULLIF(COUNT(DISTINCT s.foreclosure_id), 0),
    2
  ) AS pct
FROM scope s
LEFT JOIN ori_encumbrances oe ON oe.strap = s.strap;

-- Survival coverage for active foreclosures with judgment data + strap
WITH scope AS (
  SELECT DISTINCT foreclosure_id, strap
  FROM foreclosures
  WHERE archived_at IS NULL
    AND judgment_data IS NOT NULL
    AND strap IS NOT NULL
)
SELECT
  COUNT(DISTINCT s.foreclosure_id) FILTER (WHERE oe.survival_status IS NOT NULL) AS covered,
  COUNT(DISTINCT s.foreclosure_id) AS total,
  ROUND(
    100.0 * COUNT(DISTINCT s.foreclosure_id) FILTER (WHERE oe.survival_status IS NOT NULL)
    / NULLIF(COUNT(DISTINCT s.foreclosure_id), 0),
    2
  ) AS pct
FROM scope s
LEFT JOIN ori_encumbrances oe ON oe.strap = s.strap;
```

Required failure loop if any gate misses target:

1. Diagnose root cause (`step_*` columns, worker logs, target selectors in services)
2. Re-run only affected stages using valid skip-flag combinations
3. Re-validate gates
4. Repeat until thresholds pass

## 5) Runbook (Valid Commands Only)

```bash
# Full pipeline (Phase A + Phase B)
uv run Controller.py

# Force all sources
uv run Controller.py --force-all

# Quick bounded sanity run
uv run Controller.py --auction-limit 5 --judgment-limit 5 --ori-limit 5 --survival-limit 5 --limit 5

# Phase A only (skip per-auction enrichment + final refresh)
uv run Controller.py --skip-auction-scrape --skip-judgment-extract --skip-ori-search --skip-survival --skip-final-refresh

# Phase B core only (skip bulk refresh and market background dispatch)
uv run Controller.py --skip-hcpa --skip-clerk-bulk --skip-nal --skip-flr --skip-sunbiz-entity --skip-county-permits --skip-tampa-permits --skip-foreclosure-refresh --skip-trust-accounts --skip-title-chain --skip-market-data

# Market-only worker
uv run python -m src.services.market_data_worker

# Final refresh only
uv run python scripts/refresh_foreclosures.py

# Web app
uv run python -m app.web.main

# (Re)create PG schema functions/triggers
uv run python -m src.db.migrations.create_foreclosures --dsn <postgres-dsn>
```

Background-worker verification after controller runs:

```bash
# Inspect latest bulk-step worker logs
ls -1t logs/step_workers/*.log | head

# Inspect latest market worker log
ls -1t logs/market_data_worker_*.log | head
```

## 6) Explicitly Removed / Invalid Workflow

Do not use:

- `uv run main.py --update`
- `Controller.py --run-step ...` (flag does not exist)
- SQLite fallback execution paths for pipeline decisions
- Archived legacy services under `docs/archive/legacy_sqlite/` as runtime dependencies

## 7) Priority Execution Plan (What Is Next)

## Priority 1: Close web bridge gaps to make UI fully investment-grade

1. Wire property market payload directly from `property_market` into property detail + market tab.
2. Populate `property.sources` provenance from actual source JSON timestamps/URLs.
3. Populate NOCs (ORI doc-type based) in permits tab.
4. Populate tax-tab lien rows from PG encumbrance/tax data.
5. Attach chain document IDs/links for chain timeline rows.

Acceptance criteria:

- Property pages show non-placeholder market estimates/photos when `property_market` exists.
- Data Sources table renders at least one row when source payload exists.
- Permits tab includes NOCs when recorded docs exist.
- Tax tab lists tax-related encumbrance rows.
- Chain tab renders working document links for rows with discoverable files.

## Priority 1: Make runs operationally auditable

1. Add `pipeline_runs` + `pipeline_run_steps` tables.
2. Persist controller summary payloads for every run.
3. Record background worker dispatch + completion states.
4. Add scheduler for daily controller execution with logs.

Acceptance criteria:

- Every run has one persisted run record + per-step records.
- Background steps have explicit final outcome, not only dispatch state.
- Daily run history is queryable in PG.

## Priority 1: Raise and hold completeness thresholds

1. Clear `step_judgment_extracted IS NULL` backlog.
2. Clear `step_ori_searched IS NULL` backlog.
3. Clear `step_survival_analyzed IS NULL` backlog.
4. Re-run `final_refresh` after enrichment updates.
5. Re-validate all gates in Section 4.

Useful backlog queries:

```sql
SELECT COUNT(*) AS pending_judgment
FROM foreclosures
WHERE archived_at IS NULL AND step_judgment_extracted IS NULL;

SELECT COUNT(*) AS pending_ori
FROM foreclosures
WHERE archived_at IS NULL AND step_ori_searched IS NULL;

SELECT COUNT(*) AS pending_survival
FROM foreclosures
WHERE archived_at IS NULL AND step_ori_searched IS NOT NULL AND step_survival_analyzed IS NULL;
```

## Priority 2: Data quality hardening

1. Improve buyer-name resolution coverage (`sold_to` unknown reduction).
2. Add FEMA flood-zone enrichment feed into web-facing enrichments.
3. Increase `property_market` coverage and freshness for active auction set.

## 8) Fast Handoff Checklist For Any Agent

1. Read this file before coding.
2. Confirm you are using only valid commands in Section 5.
3. Choose one priority track from Section 7 and state acceptance criteria before edits.
4. After changes, run the relevant validation gates in Section 4.
5. Do not claim success unless gate thresholds are met.
