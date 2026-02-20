# Pipeline Overview

## Architecture

HillsInspector uses a **single PG-first pipeline** (`Controller.py` → `PgPipelineController`).

- **Phase A**: Bulk data loading into PostgreSQL (530K parcels, 2.4M sales, 73K cases, etc.)
- **Phase B**: Per-auction enrichment (scraping, PDF extraction, ORI search, survival analysis)
- **Database**: PostgreSQL (`hills_sunbiz`) is the sole production database

## Entry Point

```bash
uv run Controller.py                           # Full pipeline (Phase A + B)
uv run Controller.py --skip-hcpa --skip-nal    # Skip bulk refresh
uv run Controller.py --skip-auction-scrape     # Skip scraping
uv run Controller.py --ori-limit 10            # Limit ORI search
```

## Pipeline Stages

### Phase A: Bulk Data Refresh (idempotent, no per-property scraping)

| Step | Name | Source | PG Table | Rows |
|------|------|--------|----------|------|
| 1 | HCPA Suite | Bulk files | `hcpa_bulk_parcels`, `hcpa_allsales` | 530K / 2.4M |
| 2 | Clerk Bulk | Bulk files | `clerk_civil_cases`, `clerk_civil_parties` | 73K / 271K |
| 3 | DOR NAL | DOR tax file | `dor_nal_parcels` | 524K |
| 4 | Sunbiz UCC | SFTP | `sunbiz_flr_filings`, `sunbiz_flr_parties` | 21K / 44K |
| 5 | Sunbiz Entity | SFTP | `sunbiz_entity_filings` | 81 |
| 6 | County Permits | REST API | `county_permits` | 89K |
| 7 | Tampa Permits | Accela scrape | `tampa_accela_records` | 1K |
| 8 | Foreclosure Refresh | Join all bulk | `foreclosures` (hub) | 1.3K |
| 9 | Trust Accounts | Clerk registry | `TrustAccount` | 3.4K |
| 10 | Title Chain | PG analysis | `foreclosure_title_chain` | 5.8K |
| 11 | Market Data | Zillow scrape | `property_market` | 126 |

### Phase B: Per-Auction Enrichment (scraping + analysis)

| Step | Name | Method | PG Table Updated |
|------|------|--------|------------------|
| 12 | Auction Scrape | Playwright → clerk website | `foreclosures` |
| 13 | Judgment Extract | VisionService → PDF OCR | `foreclosures.judgment_data` |
| 14 | ORI Search | ORIApiScraper → ORI website | `ori_encumbrances` |
| 15 | Survival Analysis | SurvivalService computation | `ori_encumbrances.survival_status` |
| 16 | Final Refresh | Re-join with new data | `foreclosures` (enrichment) |

## Key Tables

### Hub Table: `foreclosures`
One row per (case_number, auction_date). Enriched by PG trigger with strap↔folio cross-fill, case-number normalization, and joins to bulk data (property specs, tax, market).

### Encumbrances: `ori_encumbrances`
Mortgages, liens, judgments, lis pendens found in ORI. Each has `survival_status` (FORECLOSING, SURVIVED, EXTINGUISHED, EXPIRED, SATISFIED, HISTORICAL, UNCERTAIN).

### Title Chain: `foreclosure_title_chain` + `foreclosure_title_events`
Ownership history built from sales records and clerk events.

## Key Services

| Service | File | Purpose |
|---------|------|---------|
| `PgPipelineController` | `src/services/pg_pipeline_controller.py` | Phase A+B orchestrator |
| `PgAuctionService` | `src/services/pg_auction_service.py` | Scrape auctions → PG |
| `PgJudgmentService` | `src/services/pg_judgment_service.py` | Extract judgment PDFs → PG |
| `PgOriService` | `src/services/pg_ori_service.py` | ORI search → PG |
| `PgSurvivalService` | `src/services/pg_survival_service.py` | Survival analysis → PG |
| `AuctionScraper` | `src/scrapers/auction_scraper.py` | Playwright scraper (stateless) |
| `ORIApiScraper` | `src/scrapers/ori_api_scraper.py` | ORI API + browser (stateless) |
| `SurvivalService` | `src/services/lien_survival/survival_service.py` | Lien analysis (stateless) |
| `FinalJudgmentProcessor` | `src/services/final_judgment_processor.py` | PDF extraction (stateless) |
| `VisionService` | `src/services/vision_service.py` | OCR engine |

## Verification

```sql
-- Active upcoming auctions
SELECT COUNT(*) FROM foreclosures WHERE archived_at IS NULL;

-- Judgment coverage
SELECT COUNT(*) FILTER (WHERE judgment_data IS NOT NULL) * 100.0 / COUNT(*)
FROM foreclosures WHERE archived_at IS NULL;

-- Encumbrance coverage
SELECT COUNT(DISTINCT strap) FROM ori_encumbrances;

-- Survival coverage
SELECT COUNT(*) FILTER (WHERE survival_status IS NOT NULL) * 100.0 / COUNT(*)
FROM ori_encumbrances;

-- Dashboard test
SELECT * FROM get_dashboard_stats(60);
```

## Legacy (deprecated)

The SQLite pipeline (`main.py --update` → `orchestrator.py`) is deprecated.
Use `Controller.py` for all pipeline operations.
