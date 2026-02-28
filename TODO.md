# TODO

## Critical: PG Pipeline Has No PDF Download Step

**Discovered:** 2026-02-28
**Impact:** New foreclosure cases that weren't in the old SQLite pipeline will never get judgment PDFs, judgment extraction, ORI search, or survival analysis.

### The Problem

The PG pipeline's auction scrape step (`PgAuctionService`) creates the `AuctionScraper` with `process_final_judgments=False` (line 83 of `pg_auction_service.py`). This means:

1. **Step 11 — `auction_scrape`** scrapes case metadata (case number, date, strap, address, amounts, plaintiff/defendant) but **skips PDF download entirely**.
2. **Step 12 — `judgment_extract`** (`PgJudgmentService`) scans the `data/Foreclosure/` directory for PDFs **already on disk**. It never downloads anything — it only processes what's there.
3. The actual PDF download code exists in `AuctionScraper._download_final_judgment()` and `AuctionScraper.search_judgment_by_case_number()`, but neither is invoked by the PG pipeline.

The 137 PDFs currently on disk are **leftovers from old SQLite pipeline runs**. Any new case that appears after the SQLite pipeline was retired will have:
- `pdf_path = NULL`
- `step_pdf_downloaded = NULL`
- `step_judgment_extracted = NULL`
- `judgment_data = NULL`
- No ORI search, no survival analysis

### Evidence

```
Active foreclosures:              138
With pdf_path set:                137  (all from legacy runs)
With step_pdf_downloaded set:       0  (column was never written by anything)
With step_judgment_extracted set: 137  (set by PgJudgmentService from on-disk PDFs)
```

### What Needs to Happen

A dedicated PDF download step needs to be added to the PG pipeline between `auction_scrape` and `judgment_extract`. It should:

1. Query `foreclosures WHERE archived_at IS NULL AND pdf_path IS NULL` to find cases missing PDFs.
2. For each case, attempt PDF download via the clerk's PAV Direct Search API (the code already exists in `AuctionScraper._download_final_judgment()` and `search_judgment_by_case_number()`).
3. On success, update `foreclosures SET pdf_path = :path, step_pdf_downloaded = now()`.
4. Handle the CC-case recovery flow (party search to find LP, then real CA case number) that currently lives in `AuctionScraper._recover_judgment_via_party_search()`.

### Related Code

| File | Role |
|------|------|
| `src/services/pg_auction_service.py` | Step 11 — scrapes auction metadata, `process_final_judgments=False` |
| `src/services/pg_judgment_service.py` | Step 12 — extracts from on-disk PDFs, never downloads |
| `src/scrapers/auction_scraper.py` | Has `_download_final_judgment()` and `search_judgment_by_case_number()` |
| `src/services/pg_foreclosure_service.py` | Has `update_pipeline_step()` supporting `step_pdf_downloaded` but nobody calls it |
| `src/services/final_judgment_processor.py` | Vision-based PDF extraction, called by `PgJudgmentService` |

### Related Column

`foreclosures.step_pdf_downloaded` exists in the schema (`create_foreclosures.py` line 142) and is recognized by `pg_foreclosure_service.update_pipeline_step()`, but has **never been written** by any service. It should be set by the new download step.

---

## Housekeeping

### `FILE_RESTRUCTURING.md` Has Incorrect Claim About `Controller.py`

The file claims `Controller.py` is an "Old SQLite pipeline controller, replaced by `pg_pipeline_controller.py`". This is **wrong**. `Controller.py` is the canonical active pipeline entry point per `MASTERPLAN.md`. It imports and runs `PgPipelineController`. Do not delete it.

### `sunbiz_entity_cordata` Table Missing

The `db_audit` report shows this table doesn't exist. Either the Sunbiz entity quarterly job hasn't been run yet, or the table name is different. Verify against `sunbiz/pg_loader.py` and run the job if needed.
