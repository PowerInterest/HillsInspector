# CLAUDE.md

Guidance for coding agents working in this repository.

## Ground Rules

- **Source of Truth**: Treat `MASTERPLAN.md` as the startup source of truth. If this file diverges, follow `MASTERPLAN.md` and update this file.
- **Tech Stack**: Use **uv** (package manager), **polars** (dataframes), **PostgreSQL** (runtime pipeline db), and **Alembic** (schema migrations).
- **Hard Constraints**: Never apply raw `ALTER TABLE` manually. Do not reintroduce SQLite logic.

## Pipeline Success Criteria (Required)

A `Controller.py` run is successful only when data completeness targets are met.

Do not report success based only on step execution without validation.

| Metric | Target |
|--------|--------|
| Final Judgment PDFs | >= 90% of active foreclosures |
| Extracted judgment data | >= 90% of active foreclosures with PDFs |
| Chain coverage | >= 80% of active foreclosures with judgment data |
| Complete chain | >= 90% of chained foreclosures (terminal link, no gaps) |
| Lis pendens coverage | >= 90% of judged foreclosures have LP (ori_encumbrances or title events) |
| Encumbrance coverage | >= 80% of active foreclosures with judgment data + strap |
| Survival coverage | >= 80% of active foreclosures with judgment data + strap |

Validation SQL (Single Health Check):

```sql
WITH active_scope AS (
    SELECT foreclosure_id, strap, judgment_data FROM foreclosures WHERE archived_at IS NULL
),
metrics AS (
    SELECT
        (SELECT COUNT(*) FROM active_scope) as active_count,
        (SELECT COUNT(*) FROM active_scope WHERE judgment_data IS NOT NULL) as judg_count,
        (SELECT COUNT(DISTINCT c.foreclosure_id) FROM foreclosure_title_chain c JOIN active_scope s ON c.foreclosure_id = s.foreclosure_id WHERE s.judgment_data IS NOT NULL) as chain_count,
        (SELECT COUNT(DISTINCT c.foreclosure_id) FROM foreclosure_title_chain c JOIN active_scope s ON c.foreclosure_id = s.foreclosure_id WHERE s.judgment_data IS NOT NULL AND c.is_terminal = true AND c.foreclosure_id NOT IN (SELECT DISTINCT foreclosure_id FROM foreclosure_title_chain WHERE is_gap = true)) as complete_chain_count,
        (SELECT COUNT(DISTINCT s.foreclosure_id) FROM active_scope s WHERE s.judgment_data IS NOT NULL AND (EXISTS (SELECT 1 FROM ori_encumbrances oe WHERE oe.strap = s.strap AND oe.encumbrance_type = 'lis_pendens') OR EXISTS (SELECT 1 FROM foreclosure_title_events fte WHERE fte.foreclosure_id = s.foreclosure_id AND fte.event_subtype IN ('LP', 'LPR')))) as lp_count,
        (SELECT COUNT(DISTINCT s.foreclosure_id) FROM active_scope s JOIN ori_encumbrances oe ON oe.strap = s.strap WHERE s.judgment_data IS NOT NULL AND oe.id IS NOT NULL) as enc_count,
        (SELECT COUNT(DISTINCT s.foreclosure_id) FROM active_scope s JOIN ori_encumbrances oe ON oe.strap = s.strap WHERE s.judgment_data IS NOT NULL AND oe.survival_status IS NOT NULL) as surv_count
)
SELECT
    active_count AS "Active Auctions",
    ROUND(100.0 * judg_count / NULLIF(active_count, 0), 2) || '%' AS "Judgment Data (Target: >=90%)",
    ROUND(100.0 * chain_count / NULLIF(judg_count, 0), 2) || '%' AS "Title Chain (Target: >=80%)",
    ROUND(100.0 * complete_chain_count / NULLIF(chain_count, 0), 2) || '%' AS "Complete Chain (Target: >=90%)",
    ROUND(100.0 * lp_count / NULLIF(judg_count, 0), 2) || '%' AS "Lis Pendens (Target: >=90%)",
    ROUND(100.0 * enc_count / NULLIF(judg_count, 0), 2) || '%' AS "Encumbrances Scope (Target: >=80%)",
    ROUND(100.0 * surv_count / NULLIF(judg_count, 0), 2) || '%' AS "Survival Analyzed (Target: >=80%)"
FROM metrics;
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


