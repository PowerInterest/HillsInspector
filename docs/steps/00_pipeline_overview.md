# Pipeline Overview

## Architecture

HillsInspector runs a single PG-first pipeline:

- Entry point: `Controller.py`
- Orchestrator: `src/services/pg_pipeline_controller.py`
- Database: PostgreSQL (pipeline + analytics)

The controller executes 16 ordered steps across two phases.

Important runtime behavior:

- Bulk ingestion steps are background-dispatched (`logs/step_workers/`).
- Market data is background-dispatched (`logs/market_data_worker_*.log`).
- A controller summary can return before background workers finish.

## Entry Point

```bash
# Full run (Phase A + Phase B)
uv run Controller.py

# Force stale checks off and run all loaders
uv run Controller.py --force-all

# Quick bounded sanity run
uv run Controller.py --auction-limit 5 --judgment-limit 5 --ori-limit 5 --survival-limit 5 --limit 5

# Phase A only
uv run Controller.py --skip-auction-scrape --skip-judgment-extract --skip-ori-search --skip-survival --skip-final-refresh

# Phase B core only
uv run Controller.py --skip-hcpa --skip-clerk-bulk --skip-nal --skip-flr --skip-sunbiz-entity --skip-county-permits --skip-tampa-permits --skip-foreclosure-refresh --skip-trust-accounts --skip-title-chain --skip-market-data
```

## Pipeline Stages

### Phase A: Bulk Refresh

| Step | Name | Service | Primary PG Outputs | Mode |
|------|------|---------|--------------------|------|
| 1 | `hcpa_suite` | `load_hcpa_suite` | `hcpa_bulk_parcels`, `hcpa_allsales` | background |
| 2 | `clerk_bulk` | `PgClerkBulkService` | `clerk_civil_cases`, `clerk_civil_parties`, related clerk tables | background |
| 3 | `dor_nal` | `PgNalService` | `dor_nal_parcels` | background |
| 4 | `sunbiz_flr` | `PgFlrService` | `sunbiz_flr_*` | background |
| 5 | `sunbiz_entity` | `load_sunbiz_entity` | `sunbiz_entity_*` | background |
| 6 | `county_permits` | `CountyPermitService` | `county_permits` | background |
| 7 | `tampa_permits` | `TampaPermitService` | `tampa_accela_records` | background |
| 8 | `foreclosure_refresh` | `PgForeclosureService` | `foreclosures` (hub refresh) | inline |
| 9 | `trust_accounts` | `PgTrustAccountsService` | `TrustAccount`, `TrustAccountSummary` | inline |
| 10 | `title_chain` | `TitleChainController` | `foreclosure_title_events`, `foreclosure_title_chain`, `foreclosure_title_summary` | inline |

### Phase B: Per-Auction Enrichment

| Step | Name | Service | Primary PG Outputs | Mode |
|------|------|---------|--------------------|------|
| 11 | `auction_scrape` | `PgAuctionService` | refreshed `foreclosures` auction rows | inline |
| 12 | `judgment_extract` | `PgJudgmentService` | `foreclosures.judgment_data`, `step_judgment_extracted` | inline |
| 13 | `ori_search` | `PgOriService` | `ori_encumbrances`, `step_ori_searched` | inline |
| 14 | `survival_analysis` | `PgSurvivalService` | `ori_encumbrances.survival_status`, `step_survival_analyzed` | inline |
| 15 | `final_refresh` | `scripts.refresh_foreclosures.refresh` | recomputed foreclosure metrics | inline |
| 16 | `market_data` | `dispatch_market_data_worker` | `property_market` (+ post-market refresh) | background |

## Key Data Domains

| Domain | Key Tables |
|--------|------------|
| Foreclosure hub | `foreclosures`, `foreclosures_history`, `foreclosure_events` |
| Title chain | `foreclosure_title_chain`, `foreclosure_title_events`, `foreclosure_title_summary` |
| Encumbrances | `ori_encumbrances` |
| Parcels & sales | `hcpa_bulk_parcels`, `hcpa_allsales` |
| Clerk | `clerk_civil_cases`, `clerk_civil_parties`, `clerk_civil_events` |
| Tax | `dor_nal_parcels` |
| Permits | `county_permits`, `tampa_accela_records` |
| Market | `property_market` |

## Verification

```sql
-- Active foreclosures
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

-- Encumbrance coverage (by active foreclosure with strap)
WITH scope AS (
  SELECT DISTINCT foreclosure_id, strap
  FROM foreclosures
  WHERE archived_at IS NULL
    AND strap IS NOT NULL
    AND judgment_data IS NOT NULL
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
    AND strap IS NOT NULL
    AND judgment_data IS NOT NULL
)
SELECT
  COUNT(DISTINCT s.foreclosure_id) FILTER (WHERE oe.survival_status IS NOT NULL) AS covered,
  COUNT(DISTINCT s.foreclosure_id) AS total
FROM scope s
LEFT JOIN ori_encumbrances oe ON oe.strap = s.strap;
```

```bash
# Check background worker outputs after controller run
ls -1t logs/step_workers/*.log | head
ls -1t logs/market_data_worker_*.log | head
```
