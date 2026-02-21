# CLAUDE.md

Guidance for coding agents working in this repository.

## Core Rule

Treat `MASTERPLAN.md` as the startup source of truth.

If this file and `MASTERPLAN.md` ever diverge, follow `MASTERPLAN.md` and fix this file.

## Pipeline Success Criteria (Required)

A `Controller.py` run is successful only when data completeness targets are met.

Do not report success based only on step execution without validation.

| Metric | Target |
|--------|--------|
| Final Judgment PDFs | >= 90% of active foreclosures |
| Extracted judgment data | >= 90% of active foreclosures with PDFs |
| Chain coverage | >= 80% of active foreclosures with judgment data |
| Encumbrance coverage | >= 80% of active foreclosures with judgment data + strap |
| Survival coverage | >= 80% of active foreclosures with judgment data + strap |

Validation SQL:

```sql
-- Active denominator
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
  ) AS pct
FROM foreclosures
WHERE archived_at IS NULL;

-- Chain coverage
WITH scope AS (
  SELECT foreclosure_id
  FROM foreclosures
  WHERE archived_at IS NULL
    AND judgment_data IS NOT NULL
)
SELECT
  COUNT(DISTINCT c.foreclosure_id) AS covered,
  (SELECT COUNT(*) FROM scope) AS total
FROM foreclosure_title_chain c
JOIN scope s ON s.foreclosure_id = c.foreclosure_id;

-- Encumbrance coverage
WITH scope AS (
  SELECT DISTINCT foreclosure_id, strap
  FROM foreclosures
  WHERE archived_at IS NULL
    AND judgment_data IS NOT NULL
    AND strap IS NOT NULL
)
SELECT
  COUNT(DISTINCT s.foreclosure_id) FILTER (WHERE oe.id IS NOT NULL) AS covered,
  COUNT(DISTINCT s.foreclosure_id) AS total
FROM scope s
LEFT JOIN ori_encumbrances oe ON oe.strap = s.strap;

-- Survival coverage
WITH scope AS (
  SELECT DISTINCT foreclosure_id, strap
  FROM foreclosures
  WHERE archived_at IS NULL
    AND judgment_data IS NOT NULL
    AND strap IS NOT NULL
)
SELECT
  COUNT(DISTINCT s.foreclosure_id) FILTER (WHERE oe.survival_status IS NOT NULL) AS covered,
  COUNT(DISTINCT s.foreclosure_id) AS total
FROM scope s
LEFT JOIN ori_encumbrances oe ON oe.strap = s.strap;
```

If any target is missed:

1. Diagnose root cause (`step_*` columns, service target queries, logs).
2. Re-run only affected stages.
3. Re-validate.
4. Repeat until thresholds pass.

## Canonical Commands

```bash
# Full pipeline
uv run Controller.py

# Force all loaders
uv run Controller.py --force-all

# Quick sanity run
uv run Controller.py --auction-limit 5 --judgment-limit 5 --ori-limit 5 --survival-limit 5 --limit 5

# Phase A only
uv run Controller.py --skip-auction-scrape --skip-judgment-extract --skip-ori-search --skip-survival --skip-final-refresh

# Phase B core only
uv run Controller.py --skip-hcpa --skip-clerk-bulk --skip-nal --skip-flr --skip-sunbiz-entity --skip-county-permits --skip-tampa-permits --skip-foreclosure-refresh --skip-trust-accounts --skip-title-chain --skip-market-data

# Market worker only
uv run python -m src.services.market_data_worker

# Final refresh only
uv run python scripts/refresh_foreclosures.py

# Web app
uv run python -m app.web.main

# PG schema/functions
uv run python -m src.db.migrations.create_foreclosures --dsn <postgres-dsn>
```

## Controller Behavior You Must Account For

- Bulk steps are background-dispatched:
  - `hcpa_suite`, `clerk_bulk`, `dor_nal`, `sunbiz_flr`, `sunbiz_entity`, `county_permits`, `tampa_permits`
- Market step is background-dispatched:
  - `market_data`
- A controller run can finish before those workers complete.

Always verify worker completion:

```bash
ls -1t logs/step_workers/*.log | head
ls -1t logs/market_data_worker_*.log | head
```

## Tech Constraints

- Package manager: `uv` only.
- DataFrames: `polars` only.
- Runtime pipeline DB: PostgreSQL only.
- Do not reintroduce SQLite fallback logic into active pipeline/web paths.

## Pipeline Architecture (Current)

### Phase A (bulk)

1. `hcpa_suite`
2. `clerk_bulk`
3. `dor_nal`
4. `sunbiz_flr`
5. `sunbiz_entity`
6. `county_permits`
7. `tampa_permits`
8. `foreclosure_refresh`
9. `trust_accounts`
10. `title_chain`

### Phase B (enrichment)

11. `auction_scrape`
12. `judgment_extract`
13. `ori_search`
14. `survival_analysis`
15. `final_refresh`
16. `market_data`

## Web-Readiness Checklist

Before claiming web completion, verify these are true in code and UI:

1. Property detail uses real `property_market` payload (not placeholder-only snapshot shaping).
2. Encumbrances render from `ori_encumbrances` with survival statuses.
3. `property.sources` provenance is populated when source data exists.
4. NOCs are populated in permits tab.
5. Tax tab lien rows are populated.
6. Chain rows provide document links when files are available.

## Invalid / Stale Workflow (Do Not Use)

- Any legacy entrypoint besides `Controller.py`.
- Any nonexistent controller flags.
- Any instruction that assumes SQLite is the active pipeline backing store.
