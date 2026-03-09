# System Audit Fixes — 2026-03-08

Fixes applied from the multi-reviewer system audit (`issues_from_agent.md`).
Each issue was confirmed by at least two of three reviewers (original agent, Claude Code, Codex)
before being implemented.

## Summary

| # | Issue | Severity | Files Changed | Impact |
|---|-------|----------|--------------|--------|
| 2 | Wrong JSONB key in trust accounts | HIGH | `trust_accounts.py` | Plaintiff classification always NULL |
| 4 | COALESCE direction in permit upserts | HIGH | `PlantCityPermit.py`, `TempleTerracePermit.py` | Status updates silently dropped |
| 5 | Duplicate `_scrape_current_page` | LOW | `auction_scraper.py` | 91 lines dead code removed |
| 6 | Realtor COALESCE priority | HIGH | `market_data_service.py` | Realtor overwrote Zillow/Redfin specs |
| 9 | SAT/REL mortgage misclassification | HIGH | `type_normalizer.py` | Satisfactions classified as active mortgages |
| 10 | `clerk_civil_alpha` worker mapping | HIGH | `bulk_step_worker.py` | Background step silently failing |
| 10a | Controller concurrency lock | HIGH | `Controller.py` | Concurrent runs corrupt title_chain DDL |
| 15 | Per-directory PDF skip logic | HIGH | `pg_judgment_service.py` | Fee orders block real judgment extraction |
| 18 | Trust accounts skip vs failure | MEDIUM | `pg_pipeline_controller.py` | Pipeline health metrics inflated |
| 21 | Generic name substring matching | MEDIUM | `pg_ori_service.py` | Legitimate party searches skipped |
| 22 | Cross-folio encumbrance update | MEDIUM | `pg_ori_service.py` | Encumbrances reassigned between properties |
| — | **Review Round 2 Fixes** | | | |
| 15b | Judgment writeback per-PDF | HIGH | `pg_judgment_service.py` | Fee orders overwriting real judgments in PG |
| R1 | HOA inference regression | HIGH | `survival_service.py` | Mortgage lenders misclassified as HOA |
| R2 | ORI SAT row shape + migration guard | HIGH | `pg_ori_service.py`, tests | Test break + unmigrated DB hard-fail |
| R3 | Controller exit code | MEDIUM | `Controller.py` | Skipped runs looked successful |
| R5 | `foreclosing_refs` backfill | MEDIUM | `pg_survival_service.py` | Null/empty refs bypassed repair |

## Post-Merge Review & Second-Pass Fixes

A code review after the initial 11 fixes identified additional issues. A second
coding pass resolved the real ones. A third-pass reconciliation review (cross-checking
Gemini, Codex, and Claude findings against the current tree) corrected stale findings
and downgraded theoretical concerns.

### Review Round 2 — Resolution Status

| Area | Initial Review | Resolution |
|---|---|---|
| #10a Controller exit code | PARTIAL — `sys.exit(0)` hides skipped runs | FIXED — changed to `sys.exit(75)` (EX_TEMPFAIL) + `pipeline_job_runs` status row |
| #15 Judgment writeback | NOT FIXED — case-level writeback, wrong PDF | FIXED — `_select_best_judgment()` ranks final judgments over fee orders, derives PDF from JSON stem |
| Survival HOA inference | NOT FIXED — "HOMEOWNER" token misclassifies lenders | FIXED — guard requires blank `foreclosure_type` + match requires "ASSOCIATION"/"ASSN"/"HOA" phrases |
| ORI satisfaction linking | NOT FIXED — SAT row shape break, missing migration guard | FIXED — test updated to 7-column shape, migration guard added to `_chase_unlinked_sat_parents()` |
| PG `foreclosing_refs` repair | PARTIAL — only triggers on missing key | FIXED — `not jdata.get("foreclosing_refs")` covers None, `{}`, missing |

### Review Round 3 — Reconciliation Against Current Tree

| Finding | Verdict | Rationale |
|---|---|---|
| Market spec chronology flaw (first-writer-wins) | REAL — architectural, not a regression from #6 | `market_data_service.py:381,453,593` all implement first-writer-wins for specs. Inferior early data can block later Zillow/Redfin upgrades. Pre-dates the Realtor patch; the fix for #6 is correct but the broader priority model needs design work. |
| Judgment multi-PDF writeback stale | DROPPED — already fixed | `pg_judgment_service.py:115` has `_select_best_judgment()`, line 191 groups by case, line 216 derives PDF from chosen stem. Earlier review note was written before round 2. |
| Null-folio ORI duplicate risk | DROPPED — not possible in current schema | `ori_encumbrances.folio` is `NOT NULL` with a matching unique index. Live table has zero null-folio rows. The `IS NOT DISTINCT FROM` change is harmless defensive code but the concern was unfounded. |
| Permit null-overwrite footgun | DOWNGRADED to future-hardening note | Both scrapers normalize empty strings to None (`PlantCityPermit.py:41`, `TempleTerracePermit.py:104`) before building upsert rows. Not an active bug; becomes relevant only if parser changes skip normalization. |
| Generic-name `\b` edge case | DOWNGRADED to config-hygiene warning | Current `generic_names` list does not include punctuation-ended forms that would trigger the `\b` edge case. The word-boundary fix is correct; the theoretical concern is about future list contents. |

### Open Architectural Issue

**Market data spec priority model**: The first-writer-wins scheme across
`_upsert_homeharvest`, `_upsert_zillow`, and `_upsert_realtor` means scrape
execution order determines which source's beds/baths/sqft/year_built survives.
If HomeHarvest runs first with stale data, later Zillow/Redfin updates won't
overwrite it. This is a design issue predating all audit fixes and needs a
source-priority-aware upsert strategy. Tracked for future design work.

### Tests Added in Review Round 2

- `test_mortgage_lender_with_homeowner_in_name_not_misclassified_as_hoa` — confirms mortgage lenders aren't treated as HOA
- `test_homeowner_in_plaintiff_no_fc_type_not_hoa_without_association` — ensures "HOMEOWNER" alone doesn't trigger HOA inference
- ORI satisfaction-linking test fixtures updated to 7-column row shape

### Remaining Coverage Gaps

- No focused test covers the multi-PDF judgment writeback selection path
- No focused test covers placeholder-photo filtering through PG upsert + web rendering
- No focused test covers the controller lock-contention exit path
- No integration test exercises OverwriteTracker through the actual MarketDataService upserts end-to-end

### Follow-On Fix — ORI Case-Only Identity Recovery

After the original audit closed, one additional ORI persistence bug was fixed:

- **Problem:** case-only ORI/LP discovery could find documents for foreclosures with
  unknown `strap`/`folio`, but `_save_documents()` persisted into `ori_encumbrances`,
  where `folio` is `NOT NULL`. PostgreSQL rejected those inserts and the documents
  were effectively dropped.
- **Fix:** `PgOriService` now attempts to recover parcel identity from
  `judgment_data.parcel_id` and judgment legal description *before* discovery and
  persistence. When identity still cannot be resolved, ORI/LP documents are staged
  under `data/Foreclosure/{case_number}/ori/` instead of being discarded.
- **Verification:** foreclosure `21007 / 292024CA003727A001HC` now resolves to
  `strap=19283348Y000000000310A`, `folio=1534060000`, persists LP instrument
  `2024194401`, and clears the staged-case file on success.

## Detailed Fix Descriptions

### #2 — Wrong JSONB Key in Trust Accounts

**File:** `src/services/trust_accounts.py` (line 1160)

**Bug:** `_load_upcoming_auction_context` queried `judgment_data->>'plaintiff_name'` but
`FinalJudgmentProcessor` stores the key as `plaintiff` (no `_name` suffix). Every other module
in the codebase (`survival_service.py`, `pg_ori_service.py`, `pg_encumbrance_audit.py`) correctly
uses `judgment_data->>'plaintiff'`. This caused the plaintiff field to always be NULL, making
counterparty/bank classification fall back to "unknown".

**Fix:** Changed `plaintiff_name` to `plaintiff`. Updated corresponding test assertion.

---

### #4 — COALESCE Direction in Permit Upserts

**Files:** `src/services/PlantCityPermit.py` (lines 269-313), `src/services/TempleTerracePermit.py` (lines 474-518)

**Bug:** Both files used `COALESCE(tampa_accela_records.field, EXCLUDED.field)` in their
`ON CONFLICT DO UPDATE SET` clauses. Since `COALESCE` returns the first non-NULL argument,
existing values always won — meaning status changes (Open → Closed), address corrections, and
cost updates from re-scrapes were silently dropped.

**Fix:** Flipped all 17+ COALESCE expressions to `COALESCE(EXCLUDED.field, tampa_accela_records.field)`
so incoming data wins when present, falling back to existing data only when incoming is NULL.
Also fixed the `estimated_cost_source` CASE expression to update when incoming cost is present.
This matches the pattern already used by `TampaPermit.py`.

---

### #5 — Dead Code: Duplicate `_scrape_current_page`

**File:** `src/scrapers/auction_scraper.py` (removed lines 261-351)

**Bug:** `_scrape_current_page` was defined twice. Python silently overrides the first definition
with the second. The first definition (91 lines) lacked ORI case search fallback logic that the
second (active) definition has.

**Fix:** Deleted the first (dead) definition. The active definition with ORI fallback is now the
only one.

---

### #6 — Realtor COALESCE Priority

**File:** `src/services/market_data_service.py` (lines ~593-597)

**Bug:** The `_upsert_realtor` docstring says "realtor is backup if zillow/redfin present" but
the COALESCE order was `COALESCE(EXCLUDED.beds, property_market.beds)` — making Realtor data
overwrite existing Zillow/Redfin/HomeHarvest values for beds, baths, sqft, and year_built.

**Fix:** Flipped to `COALESCE(property_market.beds, EXCLUDED.beds)` for those four fields, so
existing higher-priority source values are preserved and Realtor only fills NULLs. Added a
clarifying inline comment. Verified that Zillow and HomeHarvest upserts already use the correct
order for their respective priority levels.

---

### #9 — SAT/REL Mortgage Misclassification

**File:** `src/db/type_normalizer.py`

**Bug:** In `normalize_encumbrance_type()`, the `"MTG" in t` substring check ran before `"SAT" in t`
and `"REL" in t`. Compound ORI codes like `SATMTG` (satisfaction of mortgage), `RELMTG` (release
of mortgage), and `ASGNMTG` (assignment of mortgage) all contain "MTG", so they were classified
as active mortgages instead of their correct lifecycle type. This directly inflated survived-debt
totals in survival analysis — satisfactions looked like active liens.

**Fix:** Moved satisfaction, release, and assignment checks **above** the mortgage check. New
precedence order: satisfaction → release → assignment → mortgage. Added 6 regression tests in
`tests/test_type_normalizer.py` confirming SATMTG → satisfaction, RELMTG → release,
ASGNMTG → assignment, and plain MTG/MORTGAGE/DOT/HELOC → mortgage.

---

### #10 — `clerk_civil_alpha` Worker Mapping Missing

**File:** `src/services/bulk_step_worker.py` (line 19)

**Bug:** `BACKGROUND_BULK_STEPS` in `pg_pipeline_controller.py` includes `clerk_civil_alpha`,
and the controller has a fully implemented `_run_clerk_civil_alpha` method with staleness checks.
However, `STEP_METHODS` in `bulk_step_worker.py` had no mapping for it, so every background
dispatch returned `{"success": False, "error": "unknown_bulk_step"}`.

**Fix:** Added `"clerk_civil_alpha": "_run_clerk_civil_alpha"` to `STEP_METHODS`.

---

### #10a — Controller Concurrency Lock

**File:** `Controller.py` (lines ~168-200, 239-246)

**Bug:** Scheduled jobs use `pg_try_advisory_lock` for singleton protection, but `Controller.py`
didn't. Two overlapping controller runs could enter `title_chain` simultaneously, and DDL
(`CREATE OR REPLACE FUNCTION`) inside transactions caused `psycopg.errors.InternalError_: tuple
concurrently updated`.

**Fix:** Added a PostgreSQL session-level advisory lock at the controller entry point:
- Acquires `pg_try_advisory_lock(hashtext('pg_pipeline_controller'))` before any pipeline work
- Non-blocking: if lock cannot be acquired, logs a warning and exits with `sys.exit(75)` (EX_TEMPFAIL)
- Lock ID uses `hashtext()` in the same namespace as scheduled job locks but with a unique string
- Released automatically via `finally` block closing the dedicated lock connection
- Process crashes also release the lock (PostgreSQL releases on connection drop)
- Records a `"skipped"` row in `pipeline_job_runs` for operational visibility

---

### #15 — Per-Directory PDF Skip Logic

**File:** `src/services/pg_judgment_service.py`, method `_find_unextracted_pdfs`

**Bug:** The code checked `has_json = any(f.endswith('.json') for f in files)` and then
`if has_json: continue` — skipping the entire case directory. In CC (County Court) cases, a fee
order PDF gets extracted first (producing JSON), then the real final judgment PDF is added to the
same directory. The real judgment was permanently blocked from extraction, directly undermining
the 90% extraction completeness gate.

**Fix (Round 1 — discovery):** Changed to per-PDF checking. For each PDF in the directory,
checks if its specific `{stem}_extracted.json` exists. Only that specific PDF is skipped.

**Fix (Round 2 — writeback):** Rewrote `_load_judgment_data_to_pg()` with a new
`_select_best_judgment()` static method that:
- Filters to only `final_judgment_*_extracted.json` files (ignores mortgage extractions, orders)
- Uses `FinalJudgmentProcessor.is_thin_extraction()` to prefer real judgments over fee orders
- Breaks ties by highest instrument number (most recently recorded)
- Derives the PDF path from the chosen JSON's stem (`{stem}_extracted.json` → `{stem}.pdf`)
- Logs the selection when multiple JSONs exist for a case
- Added `pdf_path` to extraction log messages for multi-PDF auditability

---

### #18 — Trust Accounts Skip vs Failure

**File:** `src/services/pg_pipeline_controller.py` (lines 743-744)

**Bug:** When the trust accounts service is unavailable, `_run_trust_accounts` returned
`{"success": False, "error": "service_unavailable"}` while every other `_run_*` method returns
`{"skipped": True, "reason": "service_unavailable"}`. The `success: False` return incremented
`failed_steps` in the pipeline summary, distorting health metrics.

**Fix:** Changed to `{"skipped": True, "reason": "service_unavailable"}` matching all other
service-unavailable patterns.

---

### #21 — Generic Name Substring Matching Too Broad

**File:** `src/services/pg_ori_service.py`, method `_is_generic_name`

**Bug:** `any(g in name_upper for g in generic)` used Python substring matching. Short generic
terms like `"INC"` matched inside `"SINCLAIR"`, `"THE"` matched inside `"THERON"`, etc. This
caused legitimate party searches to be incorrectly skipped as "generic."

**Fix:** Replaced with `re.search(r"\b" + re.escape(g) + r"\b", name_upper)` for word-boundary
matching. Now `"INC"` matches `"ACME INC"` but not `"SINCLAIR"`. The `re` module was already
imported.

---

### #22 — Cross-Folio Encumbrance Update

**File:** `src/services/pg_ori_service.py`, method `_save_documents` (line ~3380)

**Bug:** The UPDATE statement matched by `instrument_number` only, without scoping to the current
folio. If two properties share the same instrument number (blanket mortgages, HOA liens), running
ORI for property B could overwrite property A's encumbrance row.

**Fix (Round 1):** Added `AND folio = :folio` to the WHERE clause.

**Fix (Round 2):** Changed to `AND folio IS NOT DISTINCT FROM :folio` to correctly handle
null-folio rows from case-only/no-parcel recovery paths (`NULL = NULL` is false in SQL,
but `NULL IS NOT DISTINCT FROM NULL` is true).

---

## Issues NOT Fixed (Deferred)

These issues were reviewed but deferred for various reasons:

| # | Issue | Reason Deferred |
|---|-------|----------------|
| 1 | Chain gap metric broken | Needs status model redesign, not a simple fix |
| 3 | Three judgment loaders | Tech debt — needs shared persistence helper design |
| 7 | Zestimate column naming | Needs schema migration + UI rename |
| 8 | Equity PG vs Python discrepancy | By design (different fidelity for different contexts) |
| 11 | Federal lien detection duplication | Dead code — production path works correctly |
| 12 | Full-table audit queries | Performance — not urgent at current dataset size |
| 13 | Duplicate audit engine files | CLI/tool vs service drift — delete stale copy |
| 14 | Two market data workers | Fragmented orchestration — needs design |
| 16 | Strap overwrite in refresh | Needs data investigation before changing |
| 17 | Redundant Step 1.6 enrichment | Harmless idempotent redundancy |
| 19 | `_payload_failed` duplication | Premature to abstract — standardize contract first |
| 20 | Clerk 3x HTTP requests | Low impact — once per pipeline run |
| 23-32 | Various code quality | Tech debt — tracked for future cleanup sprints |
