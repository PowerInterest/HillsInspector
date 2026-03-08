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

## Post-Merge Review Notes

This section is the current authoritative review if it conflicts with the
"fixed" descriptions below. It reflects a second-pass code review plus
targeted test execution after the fixes landed.

| Area | Review Status | Notes |
|---|---|---|
| #4 Permit upserts | ACCEPT WITH OBSERVABILITY GAP | The COALESCE direction fix is correct, but the current services still only log coarse `written` counts. They do not surface insert/update counts or status/open-flag churn, so future regressions here will be hard to prove from logs alone. |
| #10a Controller concurrency | PARTIAL | The advisory lock prevents the original `title_chain` catalog race, but lock contention exits with `sys.exit(0)`. That makes a skipped controller invocation look like success to cron/CI wrappers unless they parse logs. |
| #15 Judgment extraction | NOT FIXED | Per-PDF discovery is fixed, but PG writeback is still case-level. `_load_judgment_data_to_pg()` scans every `*_extracted.json` under a case and updates the same foreclosure row repeatedly, while choosing the first `*.pdf` in the folder instead of the PDF matching the JSON stem. In multi-PDF folders, the last JSON processed can overwrite the real final judgment with a fee order or other unrelated extraction. |
| Survival HOA inference | NOT FIXED | The new plaintiff-name fallback runs whenever `is_hoa_fc` is false, not only when `foreclosure_type` is blank. Because it keys off `HOMEOWNER`, ordinary mortgage plaintiffs such as `HOMEOWNERS FINANCIAL GROUP USA LLC` get reclassified as HOA foreclosures, which skips exact `foreclosing_refs` matching and can mark a lis pendens as the foreclosing lien instead of the mortgage. |
| ORI satisfaction linking | NOT FIXED | `_link_satisfactions()` now assumes a 7-column SAT row shape and unconditionally reads `sat[6]`, but the existing unit-test contract still supplies the older tuple shape. The new SAT-parent chase also queries `satisfies_encumbrance_id` without the migration guard already used by `_link_satisfactions()`, so unmigrated DBs can still hard-fail the ORI step. |
| #21 Generic-name matching | ACCEPT | The word-boundary change is the right fix for the original false-positive problem. No blocker found in the reviewed call sites. |
| #22 Cross-folio encumbrance update | ACCEPT | Adding `AND folio = :folio` is the right containment fix for the original overwrite bug. No regression found in the surrounding upsert path. |
| Null-folio ORI recovery | PARTIAL | The new `AND folio = :folio` containment fix does not match null-folio rows. That leaves the case-only/no-parcel recovery path unable to repair existing encumbrances discovered without a folio. |
| PG survival `foreclosing_refs` repair | PARTIAL | The backfill from `foreclosed_mortgage` only runs when the `foreclosing_refs` key is absent. Rows with `foreclosing_refs: null` or `{}` still bypass the repair even when usable instrument/book/page data is present. |
| Market photo/logo filtering | PARTIAL | Incoming photo lists are now filtered, but the upsert logic still preserves whichever JSON array is longer. That means existing dirty `photo_cdn_urls` rows do not heal on re-scrape, and stale `photo_local_paths` are not cleared when a property now has zero valid photos. The web read path also only inspects the first array element and only filters a narrow subset of placeholder patterns. |
| Logging quality | NEEDS FOLLOW-UP | Judgment extraction logs still omit `pdf_path`, so multi-PDF cases are ambiguous. Photo download failures are logged only at `debug` and do not include the failing `strap` or URL. |

Targeted tests executed during this review:

- `uv run pytest tests/test_pg_trust_accounts.py tests/test_type_normalizer.py tests/test_lien_survival_service.py tests/test_web_equity_model.py`

Coverage gaps observed during this review:

- No focused test currently covers the multi-PDF judgment writeback path in `pg_judgment_service.py`.
- No focused test currently covers placeholder-photo filtering through PG upsert plus web rendering.
- No focused test currently covers the controller lock-contention exit path.
- The new `pg_survival_service` repair path is not covered for `foreclosing_refs: null` / `{}` payloads.
- The ORI null-folio update path is not covered by a focused regression test.

Additional concrete review findings:

- I reproduced the HOA-regression path directly with `plaintiff='HOMEOWNERS FINANCIAL GROUP USA LLC'` and `foreclosure_type='MORTGAGE FORECLOSURE'`. The current logic infers HOA foreclosure anyway and marks a lis pendens as the foreclosing lien.
- The existing satisfaction-linking fixture in `tests/test_pg_ori_service.py` still returns the older SAT tuple shape, so the current `_link_satisfactions()` indexing change is not review-clean even before considering production behavior.
- The new SAT-parent chase is still weak on diagnostics. When a chased parent lookup fails or saves an unexpected document, the logs only emit aggregate counts and do not identify the source SAT row or chased instrument reference.

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
- Non-blocking: if lock cannot be acquired, logs a warning and exits cleanly (`sys.exit(0)`)
- Lock ID uses `hashtext()` in the same namespace as scheduled job locks but with a unique string
- Released automatically via `finally` block closing the dedicated lock connection
- Process crashes also release the lock (PostgreSQL releases on connection drop)

**Post-merge review note:** This closes the original concurrency corruption bug,
but the exit path is still too quiet for operations. A lock-contention skip
returns process exit code `0`, so external schedulers and wrappers will report a
successful run unless they inspect logs. Treat this as a partial fix until
"skipped because another controller is active" is surfaced explicitly in the
run status observed by automation.

---

### #15 — Per-Directory PDF Skip Logic

**File:** `src/services/pg_judgment_service.py`, method `_find_unextracted_pdfs`

**Bug:** The code checked `has_json = any(f.endswith('.json') for f in files)` and then
`if has_json: continue` — skipping the entire case directory. In CC (County Court) cases, a fee
order PDF gets extracted first (producing JSON), then the real final judgment PDF is added to the
same directory. The real judgment was permanently blocked from extraction, directly undermining
the 90% extraction completeness gate.

**Fix:** Changed to per-PDF checking. For each PDF in the directory, checks if its specific
`{stem}_extracted.json` exists. Only that specific PDF is skipped. Other PDFs without their own
JSON are returned for processing.

**Post-merge review note:** This only fixes the discovery half of the bug. The
loadback half is still wrong. `PgJudgmentService._load_judgment_data_to_pg()`
scans every `*_extracted.json` under a case directory and updates the same
`foreclosures` row repeatedly, but it selects the first `*.pdf` in the folder
instead of the PDF whose stem matches the JSON file. In folders containing both
a fee order and a real final judgment, the final stored `judgment_data` now
depends on filesystem iteration order. The issue should remain open until PG
writeback is made per-PDF or the service positively identifies the correct
judgment document before updating the foreclosure row.

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

**Fix:** Added `AND folio = :folio` to the WHERE clause. The `:folio` parameter was already
present in the params dict. When the folio-scoped UPDATE finds no matching row (rowcount == 0),
the existing INSERT with ON CONFLICT handles the new row correctly.

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
