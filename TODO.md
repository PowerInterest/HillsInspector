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

### ✅ Multi-Image PDF Processing (Dec 2024)
- Vision service now sends all PDF pages to vLLM in a single batch request instead of page-by-page
- `VisionService.analyze_images()` and `*_multi()` variants for deed, mortgage, lien, final judgment extraction
- Increased `max_tokens` from 1024 to 10000 for complex Final Judgments

### ✅ Infrastructure & Schema (Dec 2024 - Antigravity)
- **Playwright Setup**: Created `Makefile` for automated setup, synced dependencies.
- **Database Schema Repair**: Consolidated `encumbrances` table schema between `src/db/new.py` and `src/db/operations.py`. Fixed missing `debtor` and resolution columns in data persistence.
- **Documentation**: Updated `README.md` with setup instructions and removed redundant sections.
- **HCPA Scraper**: Refactored `hcpa_gis_scraper.py` with robust locators and increased timeouts for slow connections.

## In Progress

### Anti-Blocking Strategy
- [x] Investigate/Configure Cloudflare WARP integration
- [ ] Implement browser switching fallback (e.g. Chrome -> Firefox) when Chromium is blocked

### Data Ingestion Enhancements
- [x] Implement auto-download of bulk data in `--new` code path via FTP

## High Priority

### Async VisionService Refactor (Performance)
The current `VisionService` uses synchronous `requests`, blocking the pipeline during AI processing. Refactoring to `httpx` will allow concurrent processing.

**Plan:**
1.  **Refactor `VisionService`**:
    - Switch `requests` to `httpx.AsyncClient`.
    - Make `analyze_image`, `analyze_images` and all extraction methods `async`.
    - Keep synchronous wrappers for backward compatibility.
2.  **Update Pipeline Step 2**:
    - Modify `run_full_pipeline` to use `asyncio.gather` for Final Judgment processing.
    - Implement a `Semaphore` (e.g., limit 5-10) to control load on the vLLM server.
3.  **Update Consumers**:
    - Update `FinalJudgmentProcessor`, `PermitScraper`, etc., to `await` vision calls.

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
- **Constraint:** Implement parallelizable phases (as detailed in `docs/Fast.md`) **ONLY AFTER** the whole pipeline and website works completely.

### Liens & Mortgage Survivability on Web
- Web dashboard is not yet showing which liens survive the sale; need to surface survival status and totals.
- Mortgage documents must be fully processed to compute total encumbrances vs the final judgment.
- Determine first vs second mortgages by recording date and document text; display the order and amounts.
- Each referenced document should be clickable to open the underlying file in a new tab (link to stored doc).
- Sales History is not rendering on the web page; this comes from HCPA data and should be wired in.
- Title report view is broken and needs to be fixed/rewired.
- Title chain should be visualized as a graph with linked documents.

### MULTIPLE PARCEL Cases
Properties with "MULTIPLE PARCEL" as their parcel_id cannot be looked up in HCPA, Tax Collector, or other county databases that require a valid folio number. These are typically:
- Foreclosures involving multiple properties bundled together
- Cases where the auction lists several parcels as one sale item

**Challenges:**
- No single folio to query against county systems
- Would need to parse the final judgment PDF to extract individual parcel IDs
- Each parcel would need separate lookups for: HCPA data, tax status, chain of title, permits, etc.
- Database schema may need adjustment to handle one auction -> multiple parcels relationship

**Potential Solutions:**
1. Extract individual parcel IDs from final judgment documents during Step 2
2. Create a junction table `auction_parcels` to link one auction to multiple parcels
3. Run enrichment steps for each parcel independently
4. Aggregate results back to the auction level for analysis

**Current Status:** These properties are skipped by most pipeline steps due to invalid parcel_id.

## Medium Priority

### Avoid Rework
If data (judgments, liens, geocodes) already exists, skip reprocessing. Only fill gaps:
- Geocode missing lat/lon
- Skip PDFs already extracted
- Don't re-download existing documents

### Architecture
- Consider using Pydantic and SQLAlchemy instead of DuckDB for better data modeling and ORM support.

## Very Important Priority

### Log Analysis & Root Cause Analysis (CRITICAL)
- **Goal**: Review logs to analyze all warnings and errors. Identify root causes.
- **Why**: Parsing the Final Judgment document is the basis of the whole pipeline. Any failure here is a "big deal".
- **Action**: Locate `final_judgment_parsing` errors in logs, extract sample failures, and fix the underlying issue (regex, OCR, or layout changes).

### Bot Detection Mitigation
Some sources (HOVER, Realtor.com) have aggressive bot detection. Current mitigations:
- Stealth User-Agent
- Headed mode option (`headless=False`)
- IP rotation (manual)
- Browser must have a logged in google account, if we can use chrome we should.

# LOW
### OnBase API Discovery
The backend is **Hyland OnBase**. Leverage [OnBase Documentation](https://support.hyland.com/r/OnBase/Public-Sector-Constituency-Web-Access/English/Foundation-22.1/Public-Sector-Constituency-Web-Access/Configuration/Front-End-Client-Configuration/Search-Panel-Settings/Configuring-Custom-Queries/Predefine-Keyword-Values-to-Search/Dynamic-Keyword-Values) to discover more advanced search capabilities and potential API endpoints.

### Past Auction Analysis Page
- **Goal**: Create a detailed page for past auctions.
- **Content**:
  - Successfully auctioned properties & price paid.
  - "Good Deal" analysis (Market Value vs. Winning Bid).
  - Unsold properties analysis (Why weren't they purchased? Reverted to plaintiff?).


### ✅ No Parcel ID Recovery Strategy (Dec 2024)

**Problem**: ~15% of auctions scraped have missing/invalid Parcel IDs (e.g., "Property Appraiser" link text). This blocks PDF download and parsing.

**Root Cause**: The scraper requires a valid Parcel ID to create the storage folder. Without it, the Final Judgment PDF is never downloaded.

**Fixed**:
1. **Always Download PDF** (even without Parcel ID)
   - Downloads to `data/properties/unknown_case_{case_number}/documents/`
2. **Parse the PDF**
   - Parses normally to extract legal description and parties
3. **Limited Analysis Flag**
   - Adds `has_valid_parcel_id` flag to DB
   - Website displays "Limited Analysis" banner for these properties

---

### ✅ Log Analysis - General Errors (Dec 2024)
- Fixed Tax Deed scraper timeouts (Case # text fallback).
- Diagnosed vision service connection errors.

## High Priority

### Pipeline Optimization (Parallel Architecture)
- **Goal**: Reduce pipeline runtime from >20h to <5h.
- **Strategy**: Single-Writer Queue + Parallel Enrichment + Async Orchestration.
- [x] **Step 1: Database Writer Queue**
  - Create `src/db/writer.py` and `DBQueue` class.
  - Refactor DB connection for queued writes. (Completed: Implemented writer and migrated methods to PropertyDB)
- [ ] **Step 2: Scraper Standardization**
  - Refactor `TaxScraper`, `PermitScraper`, `MarketScraper` to return models.
  - Remove direct DB writes from scrapers.
- [ ] **Step 3: Async Orchestrator**
  - Create `src/orchestrator.py` to manage TaskGroups.
  - Implement concurrent work pool logic.
- [ ] **Step 4: Vision Batching**
  - Implement bounded semaphore for Vision Service.