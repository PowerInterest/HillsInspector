# HillsInspector MASTERPLAN

**Updated: 2026-02-20**

## 1) Mission

Single PG-first pipeline that answers: *"Is this foreclosure property worth buying at auction, or is the title toxic?"*

One entry point (`Controller.py`), one database (PostgreSQL), two phases:
- **Phase A**: Bulk data refresh (idempotent, no per-property scraping)
- **Phase B**: Per-auction enrichment (scraping, PDF extraction, ORI search, survival analysis)

## 2) Current Architecture

```
Controller.py
  → PgPipelineController (src/services/pg_pipeline_controller.py)

    Phase A: Bulk Data Refresh
      1.  HCPA suite         → hcpa_bulk_parcels (530K), hcpa_allsales (2.4M)
      2.  Clerk bulk         → clerk_civil_cases (73K), clerk_civil_parties (271K)
      3.  DOR NAL            → dor_nal_parcels (524K)
      4.  Sunbiz UCC         → sunbiz_flr_filings (21K)
      5.  Sunbiz entities    → sunbiz_entity_filings (81)
      6.  County permits     → county_permits (89K)
      7.  Tampa permits      → tampa_accela_records (1K)
      8.  Foreclosure refresh → foreclosures hub (1.3K) + foreclosure_events
      9.  Trust accounts     → TrustAccount (3.4K)
      10. Title chain        → foreclosure_title_chain (5.8K)
      11. Market data        → property_market (126)

    Phase B: Per-Auction Enrichment                         ← NEW (2026-02-20)
      12. Auction scrape     → PgAuctionService  → foreclosures
      13. Judgment extract   → PgJudgmentService → foreclosures.judgment_data
      14. ORI search         → PgOriService      → ori_encumbrances
      15. Survival analysis  → PgSurvivalService → ori_encumbrances.survival_status
      16. Final refresh      → re-join all Phase B data into foreclosures hub
```

## 3) Implementation Status

### Phase A: Bulk Data Refresh — DONE
All 11 steps working. Staleness-aware skip/run logic with `--force-all` override.

| Step | Service | Status |
|------|---------|--------|
| 1. HCPA suite | `sunbiz.pg_loader.load_hcpa_suite` | Done |
| 2. Clerk bulk | `PgClerkBulkService.update` | Done |
| 3. DOR NAL | `PgNalService.update` | Done |
| 4. Sunbiz UCC | `PgFlrService.update` | Done |
| 5. Sunbiz entity | `SunbizMirror.sync` + `load_sunbiz_entity` | Done |
| 6. County permits | `CountyPermitService.sync_postgres` | Done |
| 7. Tampa permits | `TampaPermitService.sync_date_range` | Done |
| 8. Foreclosure refresh | `PgForeclosureService.refresh` | Done |
| 9. Trust accounts | `TrustAccountsService.run` | Done |
| 10. Title chain | `TitleChainController.run` | Done |
| 11. Market data | `MarketDataService.run_batch` | Done |

### Phase B: Per-Auction Enrichment — SERVICES CREATED, NEEDS VALIDATION

| Step | Service | File | Status |
|------|---------|------|--------|
| 12. Auction scrape | `PgAuctionService` | `src/services/pg_auction_service.py` | **Created** — needs end-to-end test |
| 13. Judgment extract | `PgJudgmentService` | `src/services/pg_judgment_service.py` | **Created** — needs end-to-end test |
| 14. ORI search | `PgOriService` | `src/services/pg_ori_service.py` | **Created** — needs end-to-end test |
| 15. Survival analysis | `PgSurvivalService` | `src/services/pg_survival_service.py` | **Created** — needs end-to-end test |
| 16. Final refresh | `refresh_foreclosures.refresh()` | `scripts/refresh_foreclosures.py` | **Wired** — re-runs after Phase B |

All 4 services are wired into `PgPipelineController.run()` with CLI flags:
- `--skip-auction-scrape`, `--skip-judgment-extract`, `--skip-ori-search`, `--skip-survival`
- `--auction-limit N`, `--judgment-limit N`, `--ori-limit N`, `--survival-limit N`

### Phase C: SQLite Deletion — NOT STARTED

| File | Lines | Replacement | Status |
|------|-------|-------------|--------|
| `src/orchestrator.py` | 2,729 | Phase B services | Not deleted yet |
| `src/db/operations.py` (PropertyDB) | ~1,500 | PG queries | Not deleted yet |
| `src/services/pg_hydrate_service.py` | ~200 | Eliminated (backwards direction) | Not deleted yet |
| `src/services/homeharvest_service.py` | ~150 | `hcpa_allsales` in PG | Not deleted yet |
| `src/ingest/bulk_parcel_ingest.py` | ~400 | `sunbiz/pg_loader.py` | Not deleted yet |
| `src/scrapers/hcpa_gis_scraper.py` | ~300 | `hcpa_bulk_parcels` | Not deleted yet |
| `src/scrapers/tax_scraper.py` | ~200 | `dor_nal_parcels` | Not deleted yet |
| `src/scrapers/permit_scraper.py` | ~300 | `county_permits` | Not deleted yet |
| `app/web/database.py` | ~800 | `pg_web.py` / `pg_database.py` | Not deleted yet |
| `main.py --update` path | ~200 | `Controller.py` | Not deleted yet |

### Phase D: Data Quality Hardening — NOT STARTED
- Buyer resolution (`sold_to` unknown rate reduction)
- Nightly reconciliation SQL assertions
- Case-number normalization edge cases

### Phase E: Full Web PG Migration — PARTIALLY DONE
- Dashboard, API, properties routers already use `pg_web.py` / `pg_database.py`
- History router still uses `database.py` (SQLite)
- Review router still uses `database.py` (SQLite)

### Phase F: Operational Reliability — NOT STARTED
- `pipeline_runs` + `pipeline_run_steps` PG tables
- Step-level run logs persisted to PG
- Alertable thresholds for stale datasets

## 4) What Each Phase B Service Does

### Step 12: PgAuctionService (`pg_auction_service.py`)
- Calculates scrape window (today → 60 days out)
- Skips dates already in PG `foreclosures`
- Calls `AuctionScraper.scrape_date()` per weekday (Playwright → clerk website)
- AuctionScraper downloads Final Judgment PDFs to `data/Foreclosure/{case}/documents/`
- UPSERTs each `Property` object into PG `foreclosures` table
- Stores plaintiff/defendant in `judgment_data` jsonb (partial, pre-extraction)
- PG trigger `normalize_foreclosure()` handles case-number normalization + strap↔folio cross-fill

### Step 13: PgJudgmentService (`pg_judgment_service.py`)
- Scans `data/Foreclosure/*/documents/` for PDFs without `_extracted.json` cache
- Calls `FinalJudgmentProcessor.process_pdf()` (VisionService OCR) for each
- Processor saves `{stem}_extracted.json` next to PDF (disk cache, survives DB rebuilds)
- Scans all `_extracted.json` files and UPDATEs `foreclosures.judgment_data` in PG
- Sets `step_judgment_extracted` timestamp on each foreclosure

### Step 14: PgOriService (`pg_ori_service.py`)
- Queries PG for foreclosures where `step_ori_searched IS NULL` and `strap IS NOT NULL`
- Joins to `hcpa_bulk_parcels` for legal description fields (`raw_legal1`..`raw_legal4`)
- Generates search terms from legal description + judgment data
- Calls `ORIApiScraper.search_by_legal()` for each term (API, not Playwright)
- Classifies results using `type_normalizer` (mortgage, judgment, lis_pendens, lien, etc.)
- INSERTs encumbrance-type documents into PG `ori_encumbrances` (ON CONFLICT DO UPDATE)
- Falls back to judgment-inferred encumbrances when ORI finds 0 docs
- Sets `step_ori_searched` timestamp on each foreclosure

### Step 15: PgSurvivalService (`pg_survival_service.py`)
- Queries PG for foreclosures where `step_survival_analyzed IS NULL` AND `step_ori_searched IS NOT NULL`
- Only processes foreclosures that have unanalyzed encumbrances in `ori_encumbrances`
- Loads encumbrances from PG `ori_encumbrances` (maps party1→creditor, party2→debtor)
- Loads judgment data from PG `foreclosures.judgment_data`
- Loads chain from PG `foreclosure_title_chain`
- Reads `homestead_exempt` flag from `foreclosures` table
- Calls `SurvivalService.analyze()` (stateless, same logic as SQLite version)
- UPDATEs `survival_status` + `survival_reason` back to PG `ori_encumbrances`
- Sets `step_survival_analyzed` timestamp on each foreclosure

### Step 16: Final Refresh
- Re-runs `refresh_foreclosures.refresh()` to:
  - Count encumbrances per strap → `foreclosures.encumbrance_count`
  - Count UCC exposure → `foreclosures.ucc_active_count`
  - Cross-fill coords, property specs, tax data for newly-scraped auctions
  - Archive past auctions

## 5) What Stays As-Is (no changes needed)

| File | Purpose |
|------|---------|
| `src/scrapers/auction_scraper.py` | Stateless Playwright scraper — returns Property objects |
| `src/scrapers/ori_api_scraper.py` | Stateless ORI API + browser search — returns document dicts |
| `src/services/final_judgment_processor.py` | Stateless PDF extraction — returns JSON + disk cache |
| `src/services/lien_survival/survival_service.py` | Stateless analysis — returns survival results dict |
| `src/services/vision_service.py` | OCR engine (GLM-4.6V-Flash + Gemini fallback) |
| `sunbiz/pg_loader.py` | Phase A bulk loaders |
| `src/services/pg_pipeline_controller.py` | Phase A+B orchestrator |
| `src/services/pg_title_chain_controller.py` | PG-only title chain builder |
| `scripts/refresh_foreclosures.py` | Hub table joins + enrichment |
| `src/db/migrations/create_foreclosures.py` | DDL + PG functions |
| `app/web/pg_web.py` + `pg_database.py` | PG web queries |

## 6) What's Left To Do (ordered by priority)

### Priority 1: Validate Phase B end-to-end
Run each Phase B step individually and verify data lands in PG correctly.
```bash
# Test auction scraping (5 per date)
uv run Controller.py --skip-hcpa --skip-clerk-bulk --skip-nal --skip-flr \
  --skip-sunbiz-entity --skip-county-permits --skip-tampa-permits \
  --skip-foreclosure-refresh --skip-trust-accounts --skip-title-chain \
  --skip-judgment-extract --skip-ori-search --skip-survival \
  --auction-limit 5

# Test judgment extraction (10 PDFs)
uv run Controller.py --skip-hcpa --skip-clerk-bulk --skip-nal --skip-flr \
  --skip-sunbiz-entity --skip-county-permits --skip-tampa-permits \
  --skip-foreclosure-refresh --skip-trust-accounts --skip-title-chain \
  --skip-auction-scrape --skip-ori-search --skip-survival \
  --judgment-limit 10

# Test ORI search (5 properties)
uv run Controller.py --skip-hcpa --skip-clerk-bulk --skip-nal --skip-flr \
  --skip-sunbiz-entity --skip-county-permits --skip-tampa-permits \
  --skip-foreclosure-refresh --skip-trust-accounts --skip-title-chain \
  --skip-auction-scrape --skip-judgment-extract --skip-survival \
  --ori-limit 5

# Test survival (5 properties)
uv run Controller.py --skip-hcpa --skip-clerk-bulk --skip-nal --skip-flr \
  --skip-sunbiz-entity --skip-county-permits --skip-tampa-permits \
  --skip-foreclosure-refresh --skip-trust-accounts --skip-title-chain \
  --skip-auction-scrape --skip-judgment-extract --skip-ori-search \
  --survival-limit 5
```

Verify with:
```sql
SELECT COUNT(*) FROM foreclosures WHERE archived_at IS NULL;
SELECT COUNT(*) FILTER (WHERE judgment_data IS NOT NULL) * 100.0 / COUNT(*)
  FROM foreclosures WHERE archived_at IS NULL;
SELECT COUNT(DISTINCT strap) FROM ori_encumbrances;
SELECT COUNT(*) FILTER (WHERE survival_status IS NOT NULL) * 100.0 / COUNT(*)
  FROM ori_encumbrances;
```

### Priority 2: Fix bugs found during validation
Likely issues:
- ORI search term generation may need tuning (legal description parsing)
- Satisfaction matching in PgOriService is basic (no 4-pass matching like ChainBuilder)
- PgSurvivalService maps `party1→creditor` but ORI uses party1=grantor for mortgages (party roles differ by doc type)
- Foreclosure dates may need type casting in survival service

### Priority 3: Enhance PgOriService with iterative discovery
Current `PgOriService` does single-pass legal description search only. The SQLite pipeline's `IterativeDiscovery` does:
- Multi-pass: legal → instrument → party name → book/page → gap-bounded searches
- Up to 15 iterations expanding search from found documents
- Cross-property contamination filtering (lot/block check)

Options:
1. Port `IterativeDiscovery` to work with PG search queue table (most thorough)
2. Add party name + instrument search passes to `PgOriService` (medium effort)
3. Keep current single-pass for now, rely on existing 16K migrated encumbrances (quickest)

### Priority 4: Delete SQLite pipeline (~7,000 lines)
Once Phase B is validated end-to-end:
1. Delete `src/orchestrator.py` (2,729 lines)
2. Delete `src/db/operations.py` PropertyDB class (~1,500 lines)
3. Delete `src/services/pg_hydrate_service.py` (backwards PG→SQLite, no longer needed)
4. Delete `src/services/homeharvest_service.py` (replaced by `hcpa_allsales`)
5. Delete `src/ingest/bulk_parcel_ingest.py` (replaced by `sunbiz/pg_loader.py`)
6. Delete `src/scrapers/hcpa_gis_scraper.py` (replaced by `hcpa_bulk_parcels`)
7. Delete `src/scrapers/tax_scraper.py` (replaced by `dor_nal_parcels`)
8. Delete `src/scrapers/permit_scraper.py` (replaced by `county_permits`)
9. Delete `app/web/database.py` (replaced by `pg_web.py` / `pg_database.py`)
10. Remove `main.py --update` path
11. Delete SQLite step4v2 services: `discovery.py`, `search_queue.py`, `chain_builder.py`, `name_matcher.py`
12. Update `CLAUDE.md` to remove SQLite references

### Priority 5: Web migration completion
- Migrate `app/web/routers/history.py` from `database.py` → `pg_database.py`
- Migrate `app/web/routers/review.py` from `database.py` → `pg_database.py`
- Delete `app/web/database.py`
- Verify all endpoints work with SQLite file absent

### Priority 6: Operational reliability
- Add `pipeline_runs` + `pipeline_run_steps` PG tables
- Persist controller JSON summaries to PG
- Add threshold alerts for stale datasets
- Daily scheduled controller runs

### Priority 7: Data quality hardening
- Buyer resolution (`sold_to` unknown rate reduction)
- FEMA flood zone batch job (quick API, deferred)
- Market data coverage improvement (126/1,408 = 9%)

## 7) Runbook

```bash
# Full pipeline (Phase A + B)
uv run Controller.py

# Force all sources to refresh
uv run Controller.py --force-all

# Phase A only (bulk refresh, skip scraping)
uv run Controller.py --skip-auction-scrape --skip-judgment-extract \
  --skip-ori-search --skip-survival

# Phase B only (skip bulk refresh)
uv run Controller.py --skip-hcpa --skip-clerk-bulk --skip-nal --skip-flr \
  --skip-sunbiz-entity --skip-county-permits --skip-tampa-permits \
  --skip-foreclosure-refresh --skip-trust-accounts --skip-title-chain

# ORI search with limit
uv run Controller.py --ori-limit 20 --skip-auction-scrape

# Legacy SQLite pipeline (deprecated — do not use for new work)
uv run main.py --update
```

## 8) PG Data Domains

| Domain | Tables | Owner Process |
|--------|--------|---------------|
| Foreclosure hub | `foreclosures`, `foreclosures_history`, `foreclosure_events` | `refresh_foreclosures.py` |
| Title chain | `foreclosure_title_chain`, `foreclosure_title_events`, `foreclosure_title_summary` | `TitleChainController` |
| Encumbrances | `ori_encumbrances`, `ori_encumbrance_assignments`, `ori_encumbrance_satisfactions` | `PgOriService` + `PgSurvivalService` |
| Parcels/Sales | `hcpa_bulk_parcels`, `hcpa_allsales`, `hcpa_parcel_sub_names` | `load_hcpa_suite` |
| Clerk | `clerk_civil_cases`, `clerk_civil_parties`, `clerk_civil_events`, `clerk_disposed_cases`, `clerk_garnishment_cases`, `clerk_name_index` | `PgClerkBulkService` |
| Tax | `dor_nal_parcels` | `PgNalService` |
| UCC/Sunbiz | `sunbiz_flr_filings`, `sunbiz_flr_parties`, `sunbiz_flr_events` | `PgFlrService` |
| Entities | `sunbiz_entity_filings`, `sunbiz_entity_parties`, `sunbiz_entity_events` | `load_sunbiz_entity` |
| Permits | `county_permits`, `tampa_accela_records` | `CountyPermitService`, `TampaPermitService` |
| Market | `property_market` | `MarketDataService` |
| Trust | `TrustAccount`, `TrustAccountSummary` | `TrustAccountsService` |
| Historical | `historical_auctions` | `HistoryService` (dormant) |
