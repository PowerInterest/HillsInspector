# TODO

## Completed

### ✅ Final Judgment PDF Download Fix (Dec 8, 2024)
**Problem**: 72 of 160 auctions had no Final Judgment PDFs downloaded, causing missing `extracted_judgment_data`.

**Root Causes**:
1. **64 auctions with `parcel_id = "Property Appraiser"`**: The auction scraper captured the link text "Property Appraiser" instead of recognizing there was no valid parcel ID. Since PDF downloads require a valid `parcel_id` for the storage path, these were skipped.
2. **8 auctions with valid parcel_ids but no PDFs**: These far-future auctions (Jan 2026) simply didn't trigger PDF downloads during the original scrape.

**Fixes Applied**:
- `src/scrapers/auction_scraper.py` (lines 205-214): Enhanced parcel_id parsing to filter out invalid values like "Property Appraiser", "N/A", and "none"
- `src/pipeline.py` Step 2: Now downloads missing PDFs before extraction instead of just skipping auctions without PDFs
  - Added `_download_missing_judgment_pdfs()` helper function
  - Added `_download_single_judgment_pdf()` helper function
  - Groups downloads by auction date to minimize page loads
  - Filters out invalid parcel_ids from processing
- `scripts/redownload_missing_judgments.py`: New standalone script to retroactively download missing PDFs

### ✅ Full Document Analysis Pipeline (Dec 2024)
- Added comprehensive vLLM prompts for ALL document types:
  - Deeds (WD, QC, SWD, TD, etc.) - extracts grantor, grantee, consideration, legal description, red flags
  - Mortgages - extracts principal, lender, interest rate, MERS info, terms
  - Liens - extracts amount, creditor, lien type, survival analysis
  - Satisfactions - extracts releasing party, original instrument reference
  - Assignments - extracts assignor, assignee, chain tracking
  - Lis Pendens - extracts case number, plaintiff, defendants
  - NOC - extracts owner, contractor, dates, bond info
  - Affidavits - extracts affiant, subject matter, heirs
- Wired document analyzer into ingestion pipeline (`IngestionService`)
- PDFs automatically downloaded and analyzed during property ingestion
- `robust_json_parse()` handles LLM JSON formatting issues (missing commas, etc.)
- Control flag `analyze_pdfs=True/False` for faster testing
- Updated `docs/INGESTION_GUIDE.md` with full documentation

### ✅ Async Party 2 Resolution (Dec 2024)
- Implemented async-compatible Party 2 resolution to avoid event loop conflicts during batch processing
- `IngestionService.ingest_property_async()` and `_resolve_missing_party2_async()` methods added
- Shared ORI scraper between services to avoid multiple browser sessions
- Batch processing script `run_all_properties.py` now handles 180+ document properties without timeouts

### ✅ Multi-Image PDF Processing (Dec 2024)
- Vision service now sends all PDF pages to vLLM in a single batch request instead of page-by-page
- `VisionService.analyze_images()` and `*_multi()` variants for deed, mortgage, lien, final judgment extraction
- Increased `max_tokens` from 1024 to 10000 for complex Final Judgments

## High Priority

### Get the pipeline working
- ~~Primary blocker: title analysis is not running—fix legal_description propagation, ORI ingestion crashes~~ MOSTLY FIXED
- Avoid dropping chain/encumbrance tables on pipeline start
- Continue batch processing remaining properties (29 remaining as of last run)

### Restriction/Tax Validation
- Pull restriction/easement docs via `PropertyDB.get_restriction_documents` and manually review the sample instruments found (folio `1827349TP000000000370U` instrument `2013149721`; folio `202935ZZZ000002717700U` instruments `2023303440` and `2023239553`).
- Run the updated tax scraper on a parcel with known outstanding taxes and confirm a `document_type='TAX'` lien is saved with a parsed balance.


### Court Case Search
Implement scraping for CQIDs 324-348 to find foreclosure and probate cases.

### ORI Browser Search Reliability
- ~~Browser-based ORI searches slow/unreliable in batches~~ IMPROVED with async refactor
- ~~Playwright `EPIPE`/context corruption after first search~~ FIXED by sharing browser session
- Still need to investigate narrowing search terms for properties with no ORI docs found
- Document stable batch size and retry strategy

### Permit Analysis
Integrate `HillsGovHubScraper` to verify NOCs (Notice of Commencement) against actual permits.

### Async Pipeline Refactor
- ✅ Basic async ingestion implemented (`ingest_property_async`)
- Still needed: bounded async orchestration and checkpointed stages (see `docs/async.md`) so reruns skip completed work and IO-heavy scrapes run concurrently with rate limits.

## Medium Priority

### Avoid Rework
If data (judgments, liens, geocodes) already exists, skip reprocessing. Only fill gaps:
- Geocode missing lat/lon
- Skip PDFs already extracted
- Don't re-download existing documents

### Architecture
- Consider using Pydantic and SQLAlchemy instead of DuckDB for better data modeling and ORM support.

## Very Important Priority

### Bot Detection Mitigation
Some sources (HOVER, Realtor.com) have aggressive bot detection. Current mitigations:
- Stealth User-Agent
- Headed mode option (`headless=False`)
- IP rotation (manual)
- Browser must have a logged in google account, if we can use chrome we should.

# LOW
### OnBase API Discovery
The backend is **Hyland OnBase**. Leverage [OnBase Documentation](https://support.hyland.com/r/OnBase/Public-Sector-Constituency-Web-Access/English/Foundation-22.1/Public-Sector-Constituency-Web-Access/Configuration/Front-End-Client-Configuration/Search-Panel-Settings/Configuring-Custom-Queries/Predefine-Keyword-Values-to-Search/Dynamic-Keyword-Values) to discover more advanced search capabilities and potential API endpoints.
