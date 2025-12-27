# TODO

## Vision JSON Recovery (Dec 25, 2025)
- [ ] Wire `json_repair` into `robust_json_parse` to salvage malformed Vision responses.

## Log Error Triage (Dec 21, 2025)
- [x] Permit scraper model mismatch (`Permit` missing address/permit_type fields)
- [x] Vision endpoints unavailable (timeouts/connection errors) causing tax/ocr failures
- [x] ORI browser timeouts (selector wait) causing ORI ingestion failures
- [x] FEMA API DNS failures (hazards.fema.gov)
- [x] Sunbiz/Playwright sandbox launch failures in restricted environment
- [x] history.db lock contention between web/history pipeline
- [x] Survival analysis "Low quality judgment data" / "Could not identify foreclosing lien" (see below)
- [x] Tax scraper "No View buttons or result links found" (see below)
- [ ] OnBase "Could not find Document ID" failures (see below)

### ✅ Step 6 (Survival Analysis) Foreclosing Lien Inference (Dec 25, 2025) — CONFIRMED

**Problem**: Survival analysis failing with two warnings for most properties:
1. "Low quality judgment data for {folio}"
2. "Could not identify foreclosing lien for {folio}"

**Root Cause 1 - Data Quality Check Too Strict**:
- `_check_data_quality()` required `plaintiff`, `foreclosure_type`, AND `lis_pendens_date`
- `lis_pendens_date` is often NULL because Final Judgment PDFs frequently don't contain this info
- Result: All properties flagged as "low quality" even with good data

**Root Cause 2 - No Foreclosing Lien Match**:
- System tried to match encumbrance creditor names to plaintiff name
- Securitized mortgages have different entities: originator (e.g., "KIAVI FUNDING INC") vs trustee (e.g., "U.S. BANK NATIONAL ASSOCIATION, AS TRUSTEE FOR...")
- Assignment from originator to trust often not recorded in public records
- Result: No encumbrance matched the plaintiff, so no foreclosing lien identified

**Fixes Applied**:

1. **Relaxed data quality check** (`survival_service.py:177-181`):
   - Now requires only `plaintiff` + either `foreclosure_type` OR `lis_pendens_date`
   - Most Final Judgments have `foreclosure_type` even if `lis_pendens_date` is missing

2. **Foreclosing lien inference fallback** (`survival_service.py:77-94`):
   - When no exact match found AND `foreclosure_type` contains "FIRST" or "MORTGAGE"
   - System infers the most recent unsatisfied mortgage as the foreclosing lien
   - Adds `FORECLOSING_LIEN_INFERRED` uncertainty flag
   - Logs: "Inferred foreclosing lien for {folio}: {creditor}"

**Files Modified**: `src/services/lien_survival/survival_service.py`

**Result**: Survival analysis now identifies foreclosing lien via inference when exact match fails.

### ✅ Step 12 (Tax Scraper) Detail Page Detection Fix (Dec 25, 2025) — CONFIRMED

**Problem**: Tax scraper failing with "No View buttons or result links found" for valid addresses.

**Root Cause**:
- The county-taxes.net site auto-redirects to detail page when search returns a unique match
- Scraper checked for "View" buttons immediately after navigation
- But JavaScript redirect hadn't completed yet
- Result: Scraper looked for buttons that don't exist on detail page

**Fixes Applied**:

1. **Address normalization** (`tax_scraper.py:427-449`):
   - Extracts just street address without city/state/zip
   - "441 LUCERNE AVE, TAMPA, FL- 33606" → "441 LUCERNE AVE"
   - Improves search matching on tax collector site

2. **Parcel ID search fallback** (`tax_scraper.py:65-75`):
   - If address search returns no results, tries searching by parcel ID/folio
   - More reliable for properties with non-standard addresses

3. **Detail page detection timing fix** (`tax_scraper.py:104-123`):
   - Added 3-second wait after navigation for JavaScript redirect to complete
   - Re-reads page URL and content after wait
   - Improved URL pattern detection: `/property-tax/{base64_id}` with length > 20
   - Added more content markers: "Account Summary", "Amount due", "Account history", "Your account is"

**Files Modified**: `src/scrapers/tax_scraper.py`

**Result**: Tax scraper now correctly detects auto-redirect to detail page and extracts data.

### ✅ Step 1 (Auction Scraper) Empty Instrument Number Fix (Dec 25, 2025) — CONFIRMED

**Problem**: Some auctions remain stuck in "processing" status despite all steps showing complete.

**Root Cause**:
- Some newly scheduled foreclosures have final judgment amount listed but no instrument number yet
- Auction page link format: `PAVDirectSearch/index.html?CQID=320&OBKey__1006_1=` (empty after =)
- Scraper checked for `CQID=320` in URL but not whether `OBKey__1006_1` had a value
- No PDF could be downloaded, leaving `step_pdf_downloaded` and `step_judgment_extracted` as NULL
- Completion logic requires all steps to be non-NULL, so auction stayed in "processing"

**Fixes Applied**:

1. **Instrument number validation** (`auction_scraper.py:260-268`):
   - Added check: `if case_href and "CQID=320" in case_href and instrument_number:`
   - Only attempts PDF download if instrument number is truthy (not empty)
   - Logs warning when instrument number missing: "No instrument number for {case} - judgment PDF not yet available"

2. **Manual fix for stuck cases**:
   - Cases without judgment PDFs can be manually marked by setting `step_pdf_downloaded` and `step_judgment_extracted` timestamps
   - This allows completion to proceed for auctions where judgment document isn't available yet

**Files Modified**: `src/scrapers/auction_scraper.py`

**Result**: Scraper no longer attempts to download non-existent PDFs, and logs clear warning when judgment unavailable.

### OnBase Document ID Not Found (Dec 22, 2025)

**Problem**: Some Final Judgment PDFs fail to download with warning `Could not find Document ID for {case_number}`.

**Root Cause**: The `_download_final_judgment()` method intercepts OnBase's `KeywordSearch` API response to extract the Document ID. The timeout occurs when:
1. The KeywordSearch API returns empty `Data` array (document not indexed)
2. The first record has no `ID` field
3. OnBase rate limiting or bot detection blocks the request
4. The instrument number search doesn't match (typo, different format)

**Current Behavior**:
- PDF download fails (`pdf_path=None`)
- Plaintiff/defendant may still be captured from OnBase response
- Step 2 (judgment extraction) fails for this case since no PDF exists
- Step 5 has separate logic for invalid folios that falls back to party-based ORI search

**Impact**: Cases without downloaded PDFs cannot be analyzed. The final judgment contains critical data: legal description, judgment amount, foreclosing mortgage details, lis pendens date.

**Potential Solutions** (not yet implemented):

1. **Try alternate OnBase search methods**:
   - CQID=320 (Instrument Search) - current method
   - CQID=326 (Party Name Search) - search by plaintiff/defendant
   - CQID=321 (Legal Description Search) - if we have legal desc from HCPA
   - CQID=319 (Book/Page Search) - if we have recording references

2. **Retry with case number fallback**: If instrument number search fails, try searching OnBase by case number pattern (e.g., `2023CA002070`)

3. **Scrape page directly**: Instead of intercepting API, look for embedded PDF viewer iframe or direct download links on the OnBase page

4. **Use ORI to find Lis Pendens**: Search ORI by party name to find the Lis Pendens document, which contains the legal description. Then use legal description to search for related documents.

5. **Queue for manual review**: Flag cases needing manual intervention and surface them in the web UI

**Files Involved**:
- `src/scrapers/auction_scraper.py:311-454` - `_download_final_judgment()` method
- `src/orchestrator.py:588-614` - Invalid folio fallback (separate issue)

**Partial Fix Implemented (Dec 22, 2025)**:
Step 5 now has a party-based ORI search fallback that covers this scenario indirectly:
- When no legal description is available from HCPA/judgment/bulk, Step 5 now tries party-based search
- This finds Lis Pendens documents which contain the legal description
- Cases are flagged with "Processed via party-based ORI (no legal desc); needs review"
- The browser-based search (`search_by_party_browser_sync`) is used to avoid the 25-record API limit

**Additional Step 5 Bug Fixes (Dec 22, 2025)**:
1. **Early returns now mark status**: Cases skipped for "no parcel_id" or "no address" are now marked as skipped
2. **Consistent step_ori_ingested marking**: All code paths now properly mark `step_ori_ingested` for accurate status tracking
3. **Browser-based party search**: Switched from API (25-record limit) to browser-based search (unlimited)

### ✅ Step 9 (Market Data) Bug Fixes (Dec 22, 2025) — NEEDS CONFIRMATION

**Bug 1: Step marked complete even with no useful data (Critical)**
- If `listing.price` was None AND `listing.status == "Unknown"`, no data was saved but step was marked complete
- Properties with failed scrapes were never retried

**Fix**: Only mark step complete when useful data is obtained. Mark as failed otherwise so it can be retried.

**Bug 2: No address validation (Critical)**
- No check for "Unknown", empty, or invalid addresses
- Malformed URLs were generated and scrapes failed silently

**Fix**: Added address validation (same pattern as Step 7) - skip with warning for invalid addresses.

**Bug 3: Silent bot detection failures (Medium)**
- CAPTCHA/block detection returned `None` instead of raising exception
- Orchestrator marked step complete instead of failed

**Fix**: `_scrape_source()` now raises `RuntimeError` on bot detection. `get_listing_details()` raises when both sources fail.

**Bug 4: Address parsing improvements (Medium)**
- Added handling for 2-part addresses ("123 Main St, Tampa")
- Fixed indentation issues in parsing logic

**Bug 5: Pydantic v2 compatibility (Low)**
- Changed `listing.dict()` to `listing.model_dump()`

**Files Modified**:
- `src/orchestrator.py:952-1029` - Address validation, conditional step completion, model_dump()
- `src/scrapers/market_scraper.py:102-149, 151-173` - Error tracking, raise on bot detection

**Status**: Awaiting confirmation after next pipeline run.

### ✅ Step 9/10 (Market Data) Additional Fixes (Dec 23, 2025) — NEEDS CONFIRMATION

**Bug 1: Consolidated rows missing most fields**
- `ListingDetails` did not map to `save_market_data()` schema (listing_status, zestimate, rent, HOA, DOM)
- Resulted in NULLs for most columns in `market_data`

**Fix**: Map consolidated payload explicitly to `save_market_data()` fields.

**Bug 2: Screenshot path always NULL**
- Market scraper saved screenshots but never set `ListingDetails.screenshot_path`

**Fix**: Capture and carry screenshot path from the successful source.

**Bug 3: Skip paths didn’t clear `needs_market_data`**
- When skipping due to existing data or invalid address, `needs_market_data` flag stayed TRUE

**Fix**: Mark `needs_market_data` false on skip paths.

**Bug 4: Address parsing + URL encoding**
- Two-part addresses like `"Street, Tampa FL 33602"` were misparsed
- URLs were built without proper slugging/encoding

**Fix**: Robust 2-part parse and slugified URLs; omit ZIP if missing.

**Files Modified**:
- `src/orchestrator.py:952-1029` - payload mapping, skip flags, address parsing
- `src/scrapers/market_scraper.py:70-230` - URL slugging, screenshot capture, record_scrape
- `src/models/property.py:61-69` - ListingDetails additions

**Status**: Awaiting confirmation after next pipeline run.

### ✅ Step 6 (Survival Analysis) Bug Fix (Dec 22, 2025) — NEEDS CONFIRMATION

**Bug: Encumbrance update key collisions**
- When multiple encumbrances share the same date+type (e.g., multiple HOA liens) with no instrument or book/page
- They got the same fallback key `DTYPE:{date}_{type}`
- Later encumbrance overwrote earlier one in lookup map
- Updates were applied to wrong record or skipped entirely

**Fix**: Include row ID in fallback key to ensure uniqueness:
- Map building: `DTYPE:{date}_{type}_{row_id}`
- Lookup: Use original `id` field if preserved through analysis, else fallback without ID for new encumbrances

**Files Modified**: `src/orchestrator.py:330-339, 497-508`

**Status**: Awaiting confirmation after next pipeline run.

### ✅ Step 7 (Permits) Bug Fixes (Dec 22, 2025) — NEEDS CONFIRMATION

**Bug 1: Silent scrape failures masked as success (Critical)**
- In `permit_scraper.py` `get_permits()`, exceptions from city/county scrapes were caught internally and swallowed
- Method returned empty `[]` on failure
- Orchestrator marked step complete even when scrape actually failed
- Properties with failed scrapes never got retried

**Fix**: Modified `get_permits()` to raise `RuntimeError` when both city AND county scrapes fail. This propagates to the orchestrator which marks the step as failed (with retry).

**Bug 2: No validation for "Unknown" address**
- If address was missing, `"Unknown"` was passed to permit scraper
- Permit search with "Unknown" would fail or return garbage
- Step still marked complete

**Fix**: Added address validation in `_run_permit_scraper()` - skips permit check for invalid addresses like "Unknown", "N/A", "None".

**Files Modified**:
- `src/orchestrator.py:1169-1176` - Added address validation
- `src/scrapers/permit_scraper.py:207-254` - Raise on both-fail scenario

**Status**: Awaiting confirmation after next pipeline run.

### ✅ Step 12/13 (Tax Check) Fixes (Dec 23, 2025) — NEEDS CONFIRMATION

**Bug 1: Tax updates missed parcels**
- `update_parcel_tax_status()` updated by `parcel_id` only
- Parcels with only `folio` set never got tax_status/tax_warrant

**Fix**: Update by `parcel_id OR folio` and insert missing parcel row first.

**Bug 2: Missing address silently marked complete**
- Tax scraper returns empty `TaxStatus` if address missing or search fails
- Orchestrator marked step complete with UNKNOWN status (no retries)

**Fix**: Validate address and treat empty scrape as failure so it can retry.

**Bug 3: Skip path didn’t clear needs_tax_check**
- When tax data already existed, status step completed but `needs_tax_check` stayed TRUE

**Fix**: Mark `needs_tax_check` false when skipping due to existing data.

**Bug 4: No tax scrape audit trail**
- Tax step didn’t write to `scraper_outputs`, so failures weren’t visible in web/status views

**Fix**: Record tax scraper results (success/failure) in `scraper_outputs` and include screenshot path when available.

**Files Modified**:
- `src/db/operations.py:518-526` - update by folio/parcel_id
- `src/orchestrator.py:914-948` - address validation + empty-result failure + skip flag

**Status**: Awaiting confirmation after next pipeline run.

### ✅ Step 15 (Geocoding) Fixes (Dec 23, 2025) — NEEDS CONFIRMATION

**Bug 1: Geocode query missed auctions with only `folio` set**
- Join used `a.parcel_id = p.folio` only

**Fix**: Use `COALESCE(a.parcel_id, a.folio)` and normalize auction_date parsing.

**Bug 2: Invalid/placeholder addresses were geocoded**
- Rows with `property_address` like "Unknown" were included

**Fix**: Filter out placeholder addresses in geocode query.

**Bug 3: Geocode attempts not logged**
- Geocode step didn’t write to `scraper_outputs`, making misses invisible

**Fix**: Record geocode success/failure in `scraper_outputs`.

**Files Modified**:
- `src/orchestrator.py:1804-1849` - normalized geocode query + address filter

**Status**: Awaiting confirmation after next pipeline run.

### ✅ Step 11 (HCPA GIS) Bug Fixes (Dec 22, 2025) — NEEDS CONFIRMATION

**Bug 1: Step marked complete even with no useful data (Critical)**
- Only mark complete if we got useful data (sales_history OR legal_description)
- If neither present, mark as failed so it can be retried

**Bug 2: No parcel_id validation (Medium)**
- Added validation for invalid parcel_id values ("unknown", "property appraiser", "multiple parcel", etc.)
- Skip with warning and mark step complete for invalid IDs

**Bug 3: Silent failure when folio search doesn't navigate (Medium)**
- Added URL check after folio search to verify navigation to property page
- Set error key if URL doesn't contain "/parcel/"
- Added page content check for "no results" indicators

**Files Modified**:
- `src/orchestrator.py:1304-1377` - parcel_id validation, conditional step completion
- `src/scrapers/hcpa_gis_scraper.py:166-172, 185-191` - navigation validation, no-results check

**Status**: Awaiting confirmation after next pipeline run.

### ✅ Step 12 (Tax Scraper) Bug Fixes (Dec 22, 2025) — NEEDS CONFIRMATION

**Bug 1: situs field always making has_data=True (Critical)**
- The `situs` field was set from input `property_address`, not scraped data
- `has_data` check always passed because `situs` was always set
- Removed `situs` from `has_data` check so only actual scraped data counts

**Bug 2: Exceptions silently swallowed in scraper (Medium)**
- `tax_scraper.py` caught all exceptions and just logged, then returned empty TaxStatus
- Now re-raises exceptions so orchestrator can properly mark as failed with actual error message

**Files Modified**:
- `src/orchestrator.py:981-990` - removed situs from has_data check
- `src/scrapers/tax_scraper.py:212-215` - re-raise exceptions instead of swallowing

**Status**: Awaiting confirmation after next pipeline run.

### ✅ Step 2 Infinite Retry Bug Fix (Dec 22, 2025) — CONFIRMED

**Problem**: Judgment extraction (Step 2) kept re-running the same auctions on every pipeline restart.

**Root Cause**: When `FinalJudgmentProcessor.process_pdf()` returned `None` (vision service returned no structured data), the orchestrator's `if result:` block was skipped silently. No status was marked, so the auction was selected again on the next run, causing an infinite retry loop.

**Symptoms**:
- Same auctions processed repeatedly across restarts
- No error messages for these cases (silent failure)
- Step 2 never completed for affected auctions

**Fix**:
1. Added `else` branch to mark status as failed when `result` is None:
   ```python
   else:
       db.mark_status_failed(case_number, "Vision service returned no structured data", error_step=2)
   ```
2. Added periodic checkpoint every 10 auctions to prevent data loss if process is killed mid-loop

**Files Modified**: `src/orchestrator.py:1488-1505`

**Status**: Fix confirmed and deployed.

## Completed

### ✅ Final Judgment PDF Download Fix (Dec 8, 2024)
**Problem**: 72 of 160 auctions had no Final Judgment PDFs downloaded, causing missing `extracted_judgment_data`.

**Root Causes**:
1. **64 auctions with `parcel_id = "Property Appraiser"`**: The auction scraper captured the link text "Property Appraiser" instead of recognizing there was no valid parcel ID. Since PDF downloads require a valid `parcel_id` for the storage path, these were skipped.
2. **8 auctions with valid parcel_ids but no PDFs**: These far-future auctions (Jan 2026) simply didn't trigger PDF downloads during the original scrape.

**Fixes Applied**:
- `src/scrapers/auction_scraper.py` (lines 205-214): Enhanced parcel_id parsing to filter out invalid values like "Property Appraiser", "N/A", and "none"
- `src/orchestrator.py` Step 2: Now downloads missing PDFs before extraction instead of just skipping auctions without PDFs
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
    - Modify `run_full_update` (orchestrator) to use `asyncio.gather` for Final Judgment processing.
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
    - [x] Create `src/db/writer.py` (DatabaseWriter class).
    - [x] Implement worker loop to process queue.
    - [x] Move write methods from `PipelineDB` to `PropertyDB`/`DatabaseWriter`.
- [x] **Step 2: Standardize Scrapers**
    - [x] Refactor `TaxScraper` to return Pydantic models (`TaxStatus`).
    - [x] Refactor `PermitScraper` to return Pydantic models (`Permit`).
    - [x] Audit `MarketScraper` (already returns `ListingDetails`).
    - [x] Ensure scrapers have no direct DB write logic.
- [x] **Step 3: The Orchestrator (`src/orchestrator.py`)** to manage TaskGroups.
  - [x] Implement concurrent work pool logic.
- [x] **Step 4: Vision Batching**
  - [x] Implement bounded semaphore for Vision Service.

If you want to pull all data, a url like this is the way to go: THis searches for addresses starting with "10111" (https://gis.hcpafl.org/arcgis/rest/services/Webmaps/HillsboroughFL_WebParcels/MapServer/0/query?f=json&returnIdsOnly=false&returnCountOnly=false&where=FullAddress%20LIKE%20%27%2510111%25%27&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=strap%20asc)

I don't think there is a restriction on an address like (all)
```
