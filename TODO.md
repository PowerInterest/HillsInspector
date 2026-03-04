# TODO
https://github.com/D4Vinci/Scrapling?tab=readme-ov-file
## Market Photo Storage Investigation

**Note:** This has been looked at 3 times, and we still have issues; seems difficult to solve.

**Discovered:** 2026-03-04
**Impact:** `property_market` can report that source scraping is complete while local property photos are still missing or incomplete, which makes "how many properties have pictures?" produce inconsistent answers depending on whether the query looks at remote photo URLs or local cached files.

### What I Believed Was Wrong

During the market-photo investigation, the evidence pointed to three separate issues:

1. **Source completion was being treated as photo completion**
   - A row could have `redfin_json`, `zillow_json`, and `homeharvest_json` populated, but still have missing or incomplete `photo_local_paths`.
   - Once the row was considered "done" for market data, the worker would stop revisiting it, so missing photos stayed missing.

2. **Zillow image downloads likely required a proper `Referer` header**
   - The downloader fetches CDN image URLs outside the browser.
   - Zillow photo URLs (`photos.zillowstatic.com`) appeared valid in-browser, but local caching was failing for some rows.
   - My working theory was that Zillow was rejecting or degrading those direct requests unless they looked like they came from Zillow pages.

3. **The count definitions were being mixed together**
   - `photo_cdn_urls` means remote image URLs were discovered.
   - `photo_local_paths` means files were actually saved under `data/Foreclosure/{case_number}/photos/`.
   - Those are not the same state, so one question can produce multiple answers unless the definition is explicit.

### Evidence

At the time of investigation, the PostgreSQL `property_market` snapshot looked like this:

```
Total property_market rows:         1280
Rows with remote photo URLs:        1147
Rows with any local photo files:    1103
Rows with remote URLs but no local:   44
Rows with local cache below target:   90
```

That snapshot suggested the main storage gap was not "no photos found anywhere". The gap was specifically "photos found remotely, but not fully saved locally".

I also verified one concrete example during the investigation:

- Strap: `182936509000044000130A`
- Before the check:
  - `photo_cdn_urls` count: `30`
  - `photo_local_paths` count: `0`
- After a one-off downloader test using source-aware request behavior:
  - local files saved: `15`

That result is why I believed the problem was at least partly in the photo download/storage path, not only in upstream scraping.

### Why Claude Could Report Different Counts

The evidence suggests these are different questions:

1. "How many properties have remote photos available?"
   - Count rows where `photo_cdn_urls` has at least one URL.
2. "How many properties have saved local pictures on disk?"
   - Count rows where `photo_local_paths` has at least one saved file.
3. "How many properties are missing all pictures?"
   - Count rows where both arrays are empty.
4. "How many properties still need local photo backfill?"
   - Count rows where remote URLs exist but local saved files are below the cache target.

If those are not kept separate, the same dataset will appear to have multiple contradictory answers.

### What Needs To Be Verified Next

1. Re-check whether market rows with complete source JSON but incomplete local photos are still being skipped by selection logic.
2. Re-check whether Zillow CDN image downloads fail without source-aware headers.
3. Define one canonical photo metric for operator questions:
   - remote-photo coverage
   - local-photo coverage
   - fully missing
   - incomplete local cache
4. Add a durable audit query/report so picture counts are computed the same way every time.

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

## Notice Of Commencement To Permit Matching

We need to review how we find Notices of Commencement (NOCs) and then define
how we search for the permit that matches each NOC.

### What Needs To Happen

1. Audit the current NOC discovery flow so we know exactly which sources,
   tables, and fields are producing NOC records today.
2. Define the matching workflow from NOC -> permit candidate, including date,
   owner/contractor, address, and any permit-number hints captured in the NOC.
3. Route the permit search by jurisdiction:
   - if the property address is inside the City of Tampa footprint, search
     `tampa_accela_records`
   - otherwise search Hillsborough County permit sources
4. Make the routing logic explicit in code and documentation so Tampa-vs-county
   permit lookup is consistent everywhere we link NOCs to permits.
5. Decide how unmatched NOCs should be surfaced so we can tell the difference
   between "no permit found yet" and "matching logic is incomplete".

### Follow-On NOC / Permit Work

1. Persist NOC -> permit links in PostgreSQL instead of only doing closest-date
   matching in the web layer.
2. Build jurisdiction-aware NOC -> permit matching that uses address, permit
   number hints, contractor/builder overlap, permit type, and date window.
3. Feed permit-derived evidence back into NOC discovery for unresolved cases,
   especially contractor names and permit identifiers.
4. Store discovery provenance and match confidence for each NOC so we know
   whether it came from official seed data, legal search, party search, or
   full-text fallback, and why it matched.
5. Split the backlog into explicit buckets:
   - no NOC, permit exists
   - NOC exists, no matched permit
   - no NOC and no permit
6. Add operational guardrails for broad backfills so suspiciously high
   per-property NOC hit counts are flagged for review instead of silently
   saved.

### Why This Matters

The real gap is often not just "missing permit rows". The real question is:
"we found a Notice of Commencement, so where is the permit that should go with
that work?" Address-driven jurisdiction detection is a key part of answering
that correctly.

---

## Lis Pendens Coverage Audit

Every active foreclosure should have a lis pendens. If a property is truly in
foreclosure, there should be LP evidence in the official records.

### What Needs To Happen

1. Audit why live PG currently shows only 90 active foreclosures with
   `encumbrance_type = 'lis_pendens'` even though all active foreclosures
   should have an LP.
2. Determine whether the gap is caused by ORI discovery, property matching,
   persistence, target-selection skip logic (`step_ori_searched` already set),
   or stale/bad foreclosure identifiers.
3. Identify which active foreclosures are missing LP coverage and group them by
   failure mode so we can fix the real cause instead of backfilling blindly.
4. Re-run or repair the affected ORI flow until active foreclosure LP coverage
   is effectively 100%, because foreclosure without lis pendens is not a valid
   steady-state outcome for this dataset.
5. Investigate foreclosure `21007` / `24-CA-003727` specifically: live
   case-based LP recovery finds LP instruments, but `ori_encumbrances` cannot
   persist case-only LP rows when `folio` is null. Decide how to persist that
   case within the existing schema or another already-existing PG store.

### Why This Matters

Lis pendens is foundational foreclosure evidence. If active foreclosures are
missing LPs in PG, the ORI/encumbrance pipeline is incomplete even if other
steps reported success.

---

## Estate/Inherited Properties Have No Enrichment Data

**Discovered:** 2026-03-01
**Impact:** Properties that have never been sold on the open market get an empty chain of title, which cascades into zero ORI encumbrance discovery and an incomplete property page.

### The Problem

The ORI document discovery pipeline relies heavily on the ownership chain (deed instrument numbers from `hcpa_allsales`) to seed searches. When a property has never been sold — typically estate/inherited properties — the entire enrichment cascade fails:

1. **Chain of title is empty** — `hcpa_allsales` has zero rows for the folio because the property was never sold, only inherited. The chain service reports `NO_FOLIO_MATCH`.
2. **ORI discovery finds nothing** — Phase 1B (deed chain + adjacent instruments) has no seed instruments to search. Phase 1A (case number) finds the foreclosure case but no encumbrance-type docs. Phase 3 (party name fallback) skips the plaintiff because mega-entity names like "U.S. BANK TRUST COMPANY" are in `generic_names.txt`.
3. **Only a judgment-inferred lien exists** — a placeholder with no recording date, no amount, no instrument number.
4. **Property page looks nearly empty** — no chain, one inferred lien, no encumbrances with real data.

### Evidence

Case `292024CA009849A001HC` (7006 TIDEWATER TRL):
- Owner: `ESTATE OF JUEL V AYERS` (deceased, property inherited)
- `hcpa_allsales`: 0 rows for folio `0455573506` — adjacent lots (20, 21, 22) all have sales, lot 19 has none
- `foreclosure_title_chain`: 0 rows
- `ori_encumbrances`: 1 row (judgment-inferred placeholder only)
- Judgment data is fully extracted (vision service worked fine)
- Strap/folio correctly resolved

### What Needs to Happen

The ORI discovery needs an alternative seed strategy for properties with no sales history:

1. **Detect the gap** — if `hcpa_allsales` returns 0 rows for a strap/folio, flag the property as "no-chain" before ORI search begins.
2. **Alternative ORI seeds** — use data already available from the judgment extraction:
   - Original mortgage recording reference (book/page/instrument from `foreclosed_mortgage`)
   - Lis pendens recording reference (from `lis_pendens`)
   - Legal description text search in PAV
   - Defendant names (the actual borrowers, not the plaintiff bank)
3. **Scope**: Currently 1/138 active foreclosures (0.7%) is affected. Low frequency but will recur whenever an estate/inherited property enters the auction pipeline.

### Related Code

| File | Role |
|------|------|
| `src/services/pg_ori_service.py` | `_discover_property()` — Phase 1B depends on ownership chain deeds |
| `src/services/pg_title_chain_service.py` | Builds chain from `hcpa_allsales`; reports `NO_FOLIO_MATCH` |
| `src/services/pg_ori_service.py` | `_get_ownership_chain()` — returns empty list when no sales exist |
| `config/generic_names.txt` | Blocks plaintiff name searches (correctly, but eliminates fallback) |

---


## Permit Expansion: Plant City & Temple Terrace

**Goal:** Expand building permit coverage beyond Tampa and Unincorporated Hillsborough County to achieve 100% municipal geographic coverage.

### What Needs to Happen
We must discover, reverse engineer, and integrate the permitting platforms for the remaining incorporated jurisdictions. The complete blueprint for how to execute this is documented in `docs/plans/2026-03-02-permit-expansion-plan.md`.

1. Identify the public portal software used by **Plant City** and trace its API.
2. Identify the public portal software used by **Temple Terrace** and trace its API.
3. Build `src/services/PlantCityPermit.py` and `src/services/TempleTerracePermit.py`.
4. Implement dynamic jurisdiction routing inside the pipeline so addresses automatically hit the correct city/county scraper.

---

## New Pipeline Ingestion Targets

**Goal:** Build data ingestion pipelines for the newly discovered bulk data endpoints to improve pre-foreclosure tracking and auction intelligence.

### 1. Weekly Undisposed Case Snapshots (Pre-Foreclosure)
- **URL**: `https://publicrec.hillsclerk.com/Civil/undisposed/`
- **Value**: 8 weekly CSVs that provide a direct feed of the "active foreclosure universe" (cases that are open but have not yet reached final judgment or auction).
- **Action**: Build a CSV ingestion service to populate a pre-foreclosure tracking table, generating leads before they hit the auction site.

### 3. Tax Deed Sales Excess Proceeds
- **URL**: `https://hillsborough.realtaxdeed.com`
- **Value**: Excel spreadsheet detailing surplus funds after tax deed sales. Highly valuable for title chain updates (tax deeds extinguish subordinate liens) and surplus recovery lead generation.
- **Action**: Build a spreadsheet parser to ingest excess proceeds into a tracking table.

### 4. Cross-Agency Intelligence Scrapers
- **Value**: Encumbrance interactions are highly predictive of property status. When the ORI parser finds a specific lien type, it should trigger a secondary scrape of a different county site.
- **Action**: 
  - Build a Hillsborough County Permit scraper triggered by `Notice of Commencement` encumbrances. Look for open permits and expired NOCs.
  - Build a Hillsborough County Code Enforcement / Special Magistrate scraper triggered by `Code Enforcement Lien` encumbrances. Super-priority liens flag extreme property distress.
  - Build a structural linkage indicating potential vacancy when `Utility Liens (Ch 159)` are discovered.

### 4. Daily New Civil Case Filings
- **URL**: `https://publicrec.hillsclerk.com/Civil/dailyfilings/`
- **Value**: 30 daily CSV files. Can provide ultra low-latency alerts for newly filed foreclosures (CA cases) and HOA liens (CC cases).
- **Action**: Evaluate schema overlap with the root `DailyNewCaseFilings/` directory and build a daily ingestion job.

---

## "Auction Today" Dashboard Tab (For Claude)

**Goal:** Build a highly focused, tactical dashboard tab that strictly shows properties scheduled for auction *today*. It must merge live auction data with the `TrustAccount` (RealAuction escrow) data to predict who is bidding and what their maximum bid cap is.

### How to Build the Prediction Logic

1. **The Join:** Join the `auctions` table (where `auction_date = CURRENT_DATE`) to the `TrustAccount` table on `case_number` (filtering for `source = 'real'` and the most recent `report_date`).
2. **Predicting the Max Bid:**
   - Third-party bidders are required to post a 5% deposit of their intended maximum bid in good funds.
   - If `TrustAccount.amount` = $10,000, then the predicted `Max Bid Capacity` = $200,000 ($10,000 * 20).
   - Display this predicted max bid directly on the dashboard card.
3. **Predicting the Bidder (Counterparty Identification):**
   - The `TrustAccount` table already has a `plaintiff_name` and a `counterparty_type` column (calculated by `trust_accounts.py`).
   - If the counterparty is the Plaintiff/Bank, it means the bank has wired money (likely for fees, not for a third-party bid).
   - If the counterparty is categorized as `third_party_bidder` or `unknown`, and the string does *not* match the auction's plaintiff, flag this row as **"ACTIVE 3RD PARTY INTEREST"**.
4. **Multi-Variable Bidder Intelligence:**
   Instead of just showing the escrow balance, combine the trust account data with the rest of the PG pipeline to build a true predictive model:
   - **The Bidding War Indicator (`multiple_recipients`):** The `TrustAccount` table tracks `multiple_recipients=1`. If multiple third parties have wired funds into the identical case number, the dashboard must flag this property with a **HIGH COMPETITION** banner. A bidding war is guaranteed.
   - **The Toxic Asset Alert (Escrow vs. Lien Survival):** Cross-reference the highly capitalized auction target against our `Lien Survival Analysis`. If a third-party bidder has deposited $20k (implying a $400k bid) on a property where our pipeline identified $150k in surviving IRS/Code Enforcement liens, flag it as **TOXIC BID**. This means the bidder is likely unaware of the hidden liens and is about to make a fatal mistake, OR they already own the subordinate liens.
   - **The Whale Tracker (Counterparty Win Rate):** Don't just show their average balance from `TrustAccountSummary`. Compute their **Conversion Rate**. Divide the number of times they parked capital (`case_count`) by the number of times they actually *won* the auction (`winning_bid_match_count`).
     - 80%+ Win Rate = **WHALE / RUTHLESS BIDDER**. They bid to win. Do not bid against them.
     - <10% Win Rate = **BOTTOM FEEDER**. They drop hundreds of 5% deposits but only throw out lowball max bids hoping to steal a property. They are easy to outbid.
   - **The Overpay Ratio (Max Bid vs. Assessed Value):** Calculate `(Predicted Max Bid) / (HCPA Assessed Value)`. If a bidder's 5% deposit implies they are willing to pay 140% of the HCPA assessed market value, they either know the property is massively undervalued by the county, or they are desperately trying to acquire it. Flag this as an **ANOMALOUS VALUATION**.

### UI/UX Requirements
- Sort the tab by **"Predicted Bidding Intensity"** (properties with `multiple_recipients=1` and highest escrow balances float to the top).
- Visually map the four intelligence flags above (High Competition, Toxic Bid, Whale, Anomalous Valuation) as colored pill tags on the property card.

---

## Encumbrance Coverage Gaps (Remaining Work)

**Context:** A 2026-03-03 encumbrance gap analysis identified systemic issues in ORI
document discovery. Seven fixes were implemented:
1. Category-aware Phase 3 — runs legal/party fallback whenever superpriority lien
   categories or mortgages are absent, regardless of total doc count.
2. Phase 1B+ lifecycle chain following from all encumbrance instruments.
3. Type normalizer preserves MOD/SUB/NCL/CTF as `other` instead of dropping them.
4. Inferred encumbrance date backfill (45/48 fixed).
5. Adjacent instrument offset widening (7 → 10 positions).
6. PG-only satisfaction/release cross-reference linking.
7. SA/CEL/SPECASMT correctly mapped to `lien`.

### 1. Phase 0 PG Seed Expansion

**Priority:** Medium
**Why it matters:** We have `official_records_daily_instruments` in PG with
thousands of rows that could seed ORI discovery without hitting the PAV API. The
current Phase 0 seeding is narrow — expanding it to pre-filter by doc_type for
LIEN, JUD, SAT, REL, MOD, SUB, NCL, CODE, ASSESS, TAX would let us find
encumbrances from local data before making expensive live searches.

**What to change:**
Extend `_seed_from_official_records()` to query a broader set of
`official_records_daily_instruments` doc types beyond the current scope.
Cross-reference with `clerk_civil_cases` to prioritize CC Enforce Lien and real
property foreclosure buckets.

### 2. Satisfaction Linking: Party/Date/Amount Heuristic

**Priority:** Low
**Why it matters:** The instrument-reference and book/page linking strategies
covered 30 of ~200 SAT/REL docs. The remaining unlinked satisfaction docs
mostly reference encumbrances outside our active foreclosure set, but some may be
linkable via party name + recording date proximity + amount matching.

**What to change:**
Add a `party_date_heuristic` strategy to `_link_satisfactions()`:
1. Match SAT party1 to MTG party1 (fuzzy, >85% token_set_ratio).
2. SAT recording_date must be after MTG recording_date.
3. If amounts match (within 5%), prefer that match.
4. Only link when exactly one candidate matches (ambiguous = skip).

Risk: False positives when the same servicer (Wells Fargo, JPMorgan) appears on
multiple mortgages for the same strap. The unambiguous-match-only guard mitigates
this. Mark with `satisfaction_method = 'party_date_heuristic'` for auditability.

### 3. Lifecycle Doc Reference Linking

**Priority:** Low
**Why it matters:** MOD/SUB/NCL docs are now persisted as `encumbrance_type='other'`
with their raw type in `raw_document_type`. But they're orphaned — not linked to the
parent mortgage or lien they modify. Survival analysis and the property page can't
reason about them without that link.

**What to change:**
A post-save pass (similar to satisfaction linking) that:
1. Parses MOD/SUB `legal_description` for instrument/book-page references.
2. Links to the parent encumbrance via a new `modifies_encumbrance_id` column or by
   reusing `satisfies_encumbrance_id` with appropriate semantics.
3. Updates the property page to show lifecycle docs nested under their parent
   encumbrance rather than as standalone rows.
