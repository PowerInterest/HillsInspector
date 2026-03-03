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

## Housekeeping

### `FILE_RESTRUCTURING.md` Has Incorrect Claim About `Controller.py`

The file claims `Controller.py` is an "Old SQLite pipeline controller, replaced by `pg_pipeline_controller.py`". This is **wrong**. `Controller.py` is the canonical active pipeline entry point per `MASTERPLAN.md`. It imports and runs `PgPipelineController`. Do not delete it.

### `sunbiz_entity_cordata` Table Missing

The `db_audit` report shows this table doesn't exist. Either the Sunbiz entity quarterly job hasn't been run yet, or the table name is different. Verify against `sunbiz/pg_loader.py` and run the job if needed.

### `clerk_name_index` Removal

Completed via Alembic migration `005_drop_clerk_name_index`. The redundant
denormalised table and ORM model were removed after alpha index coverage was
merged into `clerk_civil_cases` and `clerk_civil_parties`.

Remaining follow-up:
1. Keep validating that web/person search and dossier queries continue to use
   only the normalised clerk tables.
2. Keep the new civil alpha load path (`download-civil-alpha-index`,
   `load-civil-alpha-index`, controller step, and `load_all`) in sync with the
   active ingest workflow.

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
