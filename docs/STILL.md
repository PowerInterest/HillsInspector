# Controller Run Post-Mortem: 2026-03-10T065735Z

**Log file**: `logs/controller_runs/controller-20260310T065735Z-pid971481.log`
**18,612 lines** | **452 WARNING/ERROR entries** | **Run time**: 02:57:45 — 03:34:22 EDT (36m 37s)

This document is a historical analysis of the March 10, 2026 run. It explains
why that run produced misleading `noop` results. The code paths for
`identifier_recovery`, `ori_search`, and `title_breaks` have since been
remediated; see [`docs/domain/WORKFLOW_RETRY_CONTRACTS.md`](domain/WORKFLOW_RETRY_CONTRACTS.md)
for the current persistence and retry behavior.

---

## Step Reporting Summary

All 25 registered pipeline steps reported a result. None were silently dropped.

| Step | Status | Inserted | Updated | Errors | Duration |
|------|--------|----------|---------|--------|----------|
| hcpa_suite | skipped | 0 | 0 | 0 | 0.7s |
| clerk_bulk | skipped | 0 | 0 | 0 | 2.0s |
| clerk_criminal | skipped | 0 | 0 | 0 | 0.8s |
| clerk_civil_alpha | noop | 0 | 0 | 0 | 18.9s |
| dor_nal | skipped | 0 | 0 | 0 | 1.1s |
| sunbiz_flr | skipped | 0 | 0 | 0 | 0.2s |
| sunbiz_entity | skipped | 0 | 0 | 0 | 0.0s |
| county_permits | skipped | 0 | 0 | 0 | 0.2s |
| tampa_permits | skipped | 0 | 0 | 0 | 0.1s |
| single_pin_permits | success | 5 | 0 | 0 | 50.4s |
| foreclosure_refresh | success | 0 | 5664 | 0 | 60.6s |
| trust_accounts | noop | 0 | 0 | 0 | 0.5s |
| title_chain | success | 3480 | 0 | 0 | 165.4s |
| title_breaks | noop | 0 | 0 | 0 | 418.7s |
| auction_scrape | success | 3 | 0 | 0 | 35.1s |
| judgment_extract | noop | 0 | 0 | 0 | 18.3s |
| identifier_recovery | noop | 0 | 0 | 0 | 21.9s |
| ori_search | noop | 0 | 0 | 0 | 188.5s |
| municipal_liens_phase0 | success | 360 | 0 | 0 | 0.1s |
| mortgage_extract | noop | 0 | 0 | 0 | 0.0s |
| encumbrance_audit | success | 0 | 0 | 0 | 0.6s |
| encumbrance_recovery | success | 0 | 93 | 0 | 262.4s |
| survival_analysis | success | 0 | 109 | 0 | 2.8s |
| final_refresh | success | 0 | 5664 | 0 | 61.3s |
| market_data | **degraded** | 0 | 3 | **1** | 894.4s |

**Why no steps are missing**: The pipeline controller at `pg_pipeline_controller.py:156-242` defines an ordered list of exactly 25 `(name, skip_flag, fn)` tuples. The `run()` loop at line 244 iterates all of them unconditionally, calling `_execute_step()` for each. Every step always produces a `StepResult` — even skipped steps get `status="skipped"`. There is no early-exit logic unless `fail_fast` is set (it was not). All 25 steps appear in the log.

### Why 8 steps self-skipped

**No `--skip-*` CLI flags were passed** — the startup log at line 2 confirms all `skip_*` settings are `False`. These steps skipped themselves at runtime via the `_should_run()` staleness guard at `pg_pipeline_controller.py:1759`. The logic: if `force_all` is False, data already exists (`count > 0`), and `latest_loaded_at` is within `stale_days` of now, the step returns `skipped` with `reason: "fresh"`.

Each of these steps has a separate cron job that refreshes it on its own schedule. The controller skips them because the data is already fresh:

| Skipped Step | `stale_days` | Cron Job | Why Fresh |
|---|---|---|---|
| hcpa_suite | 7 | `cron_hcpa_bulk` | HCPA bulk data loaded within last 7 days |
| clerk_bulk | 7 | `cron_clerk_bulk` | Ran at 02:00 this same morning (run 249, success) |
| clerk_criminal | 7 | `cron_clerk_criminal` | Criminal name index loaded within last 7 days |
| dor_nal | 60 | (manual) | NAL CSV data loaded within last 60 days |
| sunbiz_flr | 7 | `cron_sunbiz_daily` | FLR data loaded within last 7 days |
| sunbiz_entity | 90 | `cron_sunbiz_daily` | Entity data loaded within last 90 days |
| county_permits | 7 | `cron_county_permits` | County ArcGIS permits loaded within last 7 days |
| tampa_permits | 3 | `cron_tampa_permits` | Tampa Accela permits loaded within last 3 days |

This is normal and correct for incremental runs. The controller delegates bulk ingestion to crons and focuses its own runtime on the per-auction enrichment steps (Phase B).

---

## Why 7 Steps Returned `noop`

These steps were not skipped by freshness guards. They executed, but produced zero inserts/updates.

| Step | Duration | Why noop | Verdict |
|---|---|---|---|
| clerk_civil_alpha | 18.9s | 0 new downloads, 54 already-loaded files | Healthy idempotency |
| trust_accounts | 0.5s | Discovered reports but processed real=0, registry=0 | Healthy idempotency |
| title_breaks | 418.7s | 241 gaps found, 0 repairs, but 107 `SEARCH_NO_RESULT` sentinel rows were persisted | **Code bug** — wrote retry-blocking state without improving title quality |
| judgment_extract | 18.3s | No unextracted PDFs found | Healthy, but 18.3s spent on filesystem scan |
| identifier_recovery | 21.9s | 11 evaluated, but `_UPDATE_FORECLOSURE_SQL` was never reached because resolution stayed at 10 `no_match` + 1 `ambiguous` | **Coverage + matching bug** — several cases have plausible HCPA matches that current logic rejects |
| ori_search | 188.5s | 14 foreclosures processed: 9 no-identity cases staged JSON only, 5 parcel-backed cases hit `_save_documents` failures | **Bug** — mixed persistence failure hidden behind `noop` |
| mortgage_extract | 0.0s | "No unextracted mortgages found." | Healthy idempotency |

### Deep-dive: `judgment_extract` — 18.3s Silent Scan

The 18.3 seconds with zero log output is **not a crash or timeout** — it's filesystem I/O. `PgJudgmentService.run()` calls `_find_unextracted_pdfs()` which iterates every directory under `data/Foreclosure/*/documents/`, globbing for `final_judgment_*.pdf` files and checking if a sibling `*_extracted.json` exists. With hundreds of case directories on WSL2, this `iterdir()` + `glob()` + `exists()` chain takes ~18s. Finding nothing unextracted, it returns `{"skipped": True, "reason": "all_judgments_extracted"}` without ever logging — there is no log statement in `_find_unextracted_pdfs()` for the "scanning" phase or the "nothing found" result.

**Not a bug, but a telemetry gap.** Adding a log line at scan start/end would make this step's 18s non-mysterious.

### Deep-dive: `title_breaks` — 241 Gaps, 0 Repairs, 107 Persisted Sentinels

This step was **not** a harmless no-op. It wrote state to PostgreSQL, but that state was only `SEARCH_NO_RESULT` sentinels in `foreclosure_title_events`, not deed-repair rows.

What is proven from code + live DB:

1. `_process_one()` inserts a sentinel whenever gap search returns no deeds. See `pg_title_break_service.py:_process_one()` calling `_insert_search_sentinel()`.
2. `_find_targets()` excludes any foreclosure that already has **any** `ORI_DEED_SEARCH` event, regardless of subtype.
3. Live DB now shows:
   - `107` `ORI_DEED_SEARCH / SEARCH_NO_RESULT` rows dated `2026-03-10`
   - `0` deed rows from `ORI_DEED_SEARCH` dated `2026-03-10`
   - current `_find_targets()`-equivalent scope = `0`
   - `68` sentinelized foreclosures still have `chain_status='BROKEN'` or `gap_count > 0`

That means the step advanced "search completed" bookkeeping without advancing title quality, and then removed those still-broken foreclosures from future retry scope.

The PAV >1500 builder/entity warnings are still relevant. They likely explain why many searches came back empty, and the local ORI fallback gate still looks too narrow. But the **proven last-run defect** is broader than truncation accounting:

1. sentinel writes are treated as `noop`
2. sentinel writes do not trigger a title-chain rebuild
3. sentinel writes permanently suppress future retries for the same foreclosure

There is also an over-scoping problem. Of the `107` sentinelized foreclosures, `39` are currently `COMPLETE` with `gap_count = 0`, which means `_find_targets()` + `fn_title_chain_gaps()` are feeding some non-repairable or already-acceptable gaps into this search workflow.

### Deep-dive: `ori_search` — 188.5s, 0 Saved (AmbiguousParameter)

This was not a single failure mode.

1. **Five parcel-backed targets** reached `_save_documents()` and hit the psycopg3 `AmbiguousParameter` bug documented in **Issue 1** below. Those documents were found, savepoints rolled back, and the step still reported `noop`.
2. **Nine no-identity targets** never reached `_save_documents()` at all. `_process_target()` staged case-only / LP-only bundles to disk because `_has_persistable_identity()` was false. Current file evidence still shows 13 staged JSON artifacts under `data/Foreclosure/*/ori/case_only_unresolved*.json`.
3. Current DB state for those nine staged cases still shows:
   - `step_ori_searched IS NULL`
   - no `ori_encumbrances` rows tied to their case/strap

There is also a separate state-machine concern in `_process_target()`: whenever a target has persistable identity and was not staged, `_mark_searched()` overwrites `foreclosures.step_ori_searched = now()` even if `saved == 0` and `inferred == 0`. That means later retries can make a case look successfully searched even when a given run persisted nothing.

### Deep-dive: `identifier_recovery` — 11 Unresolved

The key point is that the persistence write path was never reached. `PgForeclosureIdentifierRecoveryService.run()` only executes `_UPDATE_FORECLOSURE_SQL` after `_resolve_one()` returns both a candidate and a method. For the last run, `_resolve_one()` stayed at `10 no_match + 1 ambiguous`, so `foreclosures.strap` / `folio` were never updated.

That is not purely an upstream-data story. Live HCPA spot checks show at least some plausible parcel matches exist but the current matching logic rejects them:

- `292024CA005958A001HC`
  - judgment address: `1202 DESERT HILLS DR SUN CITY CENTER FL 33573`
  - relaxed HCPA search finds `folio 0780650000`, address `1202 DESERT HILLS DR`
  - `legal_descriptions_match()` rejects it with `Unit mismatch: NO vs 02`
- `292022CA010632A001HC`
  - judgment address: `11112 BLAINE PLACE, TAMPA, FL 33626`
  - relaxed HCPA search finds `folio 0040375448`, address `11112 BLAINE TOP PL`
  - matcher rejects it with `Lot/block match but no subdivision to verify`
- `292025CA007403A001HC`
  - judgment address: `5105 CRESTHILL DRIVE, TAMPA, FL 33615`
  - relaxed HCPA search finds `folio 0118150000`, address `5105 CREST HILL DR`
  - matcher rejects it with `Unit mismatch: NO vs 06`

The code-level reasons are visible:

1. `_lookup_by_address()` uses exact equality on `property_address = :address`
2. `_lookup_by_legal_description()` ANDs subdivision, lot, block, unit, and plat filters together
3. `legal_descriptions_match()` intentionally refuses lot/block-only matches without subdivision confirmation, and its parser currently over-reads some `UNIT NO` text as condo-unit evidence

---

## Issue 1: ORI `_save_documents` — 224 Documents Silently Skipped (DATA LOSS)

**Severity**: High — real encumbrance data dropped without failing the step
**Log lines**: 236 through the run, repeating in both ORI phases
**Unique instruments affected**: 221 (3 instruments retried in a second ORI phase and failed again)

### What happened

The `_save_documents` method at `pg_ori_service.py:3897-4111` wraps each document upsert in a SAVEPOINT. When the UPDATE query fails, the except block at line 4107-4109 rolls back the savepoint and logs a warning:

```
Skip document 2026040674: (psycopg.errors.AmbiguousParameter) could not determine data type of parameter $3
```

All 224 skips are `psycopg.errors.AmbiguousParameter`. Zero other exception types.

### Root cause

The UPDATE dirty-check WHERE clause (lines 3932-3969) used bare parameter references in `IS NOT NULL` checks:

```sql
OR (:book IS NOT NULL AND book IS DISTINCT FROM :book)
```

With psycopg3 (the PostgreSQL driver under SQLAlchemy), parameters are sent via server-side binding (`$N` placeholders). When a parameter value is `None`, psycopg3 sends it with OID 0 (unknown type). PostgreSQL cannot resolve the type of `$3` from `$3 IS NOT NULL` alone — the `IS NOT NULL` predicate has no column context to infer a type from.

The error only triggers when the document's `book` field is `None` (which is common — many ORI documents like judgments and lis pendens have no book/page). The SET clause (`COALESCE(:book, book)`) doesn't have this problem because the column reference gives PG type context. But the dirty-check WHERE clause evaluates `$3 IS NOT NULL` before reaching `book IS DISTINCT FROM $3`, and PG fails at the ambiguous check.

The INSERT path (lines 3976-4101) is NOT affected because its ON CONFLICT clause uses `EXCLUDED.*` references, not raw `:params`.

### Why the step still reported "noop" not "failed"

The except block at line 4107-4109 catches ALL exceptions, rolls back just that one document's savepoint, and continues to the next document. The step-level error counter is never incremented by `_save_documents` — it only returns the count of successfully saved rows. The calling code sees `saved=0` and reports noop.

This is a design problem: the step reports success even when 100% of its documents failed. The ORI step should propagate skip counts upstream so the pipeline can detect degradation.

### Fix applied

`pg_ori_service.py:3932-3969` — Added explicit `CAST(:param AS TYPE)` on every `IS NOT NULL` check in the dirty-check clause:

```sql
OR (CAST(:book AS TEXT) IS NOT NULL AND book IS DISTINCT FROM :book)
OR (CAST(:amount AS NUMERIC) IS NOT NULL AND amount IS DISTINCT FROM :amount)
OR (CAST(:is_sat_update AS BOOLEAN) IS TRUE AND is_satisfied IS DISTINCT FROM TRUE)
```

**Verified against PG**: The fixed query runs successfully with the exact parameters that caused the original error. The original query still fails with the same `AmbiguousParameter` error. Repository documentation for the bug and verification lives in `docs/domain/ORI_SQL_PARAMETER_TYPING.md`, and regression coverage lives in `tests/test_pg_ori_service.py::test_save_documents_casts_change_detection_params_for_pg_type_inference`.

### Remaining work

1. **Re-run ORI to backfill the 221 dropped instruments**. These documents were discovered by the PAV API but never persisted. A targeted re-run of the ori_search step for the affected foreclosures will re-fetch and now successfully save them.
2. **Add skip counting to `_save_documents`**. Currently the method only returns `saved` count. It should also return a skip/error count so the calling code can flag the step as degraded when skips exceed a threshold.
3. **Audit other `text()` queries in `pg_ori_service.py`** for the same bare-parameter-in-IS-NOT-NULL pattern. The INSERT ON CONFLICT path is safe (uses EXCLUDED), but any future raw SQL text queries should use explicit casts for nullable parameters.

---

## Issue 2: `_run_realtor` Method Signature Mismatch (CODE BUG)

**Severity**: High — entire browser-phase Realtor pipeline broken
**Log line**: 18510-18515
**Error**: `TypeError: PgMarketDataScraplingService._run_realtor() takes 2 positional arguments but 5 were given`

### What happened

The browser-phase market data service calls `self._run_realtor(page, cdp, properties, already_matched)` at `market_data_service.py:390-394`. The parent class `MarketDataService._run_realtor()` expects 5 positional arguments (`self, page, cdp, properties, already_matched`).

But `PgMarketDataScraplingService` overrides `_run_realtor` at `pg_market_data_scrapling.py:1392` with a completely different signature:

```python
# Parent (market_data_service.py:1360):
async def _run_realtor(self, page, cdp, properties, already_matched) -> tuple[set, int]:

# Override (pg_market_data_scrapling.py:1392):
async def _run_realtor(self, properties: list[dict[str, Any]]) -> tuple[int, int]:
```

The override takes only `(self, properties)` — 2 positional args. The parent's `run_batch()` calls it with 4 args (page, cdp, properties, already_matched), triggering the TypeError.

### Impact

The browser-phase Realtor path crashes immediately, before attempting any properties. The error is caught by the `run_batch` except block and logged as `browser_phase_failed`. The separate scrapling async phase (which calls `_run_realtor` correctly with just `properties`) also failed this run due to HTTP 429s, so Realtor contributed zero market data.

### Fix needed

The override signature needs to match the parent, or the parent's call site needs to detect the subclass and adjust. The cleanest fix: update `PgMarketDataScraplingService._run_realtor()` to accept the parent's signature but ignore the `page`/`cdp`/`already_matched` args it doesn't need (the scrapling implementation uses its own HTTP client, not Playwright).

---

## Issue 3: Identifier Recovery — Write Path Never Reached

**Severity**: Medium — conservative no-write behavior plus real matching false negatives
**Log line**: 227

11 foreclosures were evaluated; all came back unresolved:

| Case Number | Resolution |
|---|---|
| 292025CA012216A001HC | no_match |
| 292025CC016619A001HC | no_match |
| 292022CA010632A001HC | no_match |
| 292023CA015629A001HC | no_match |
| 292025CA007403A001HC | no_match |
| 292025CA008140A001HC | no_match |
| 292024CA002668A001HC | no_match |
| 292024CA005958A001HC | no_match |
| 292023CC128635A001HC | no_match |
| 292025CA005858A001HC | no_match |
| 292025CC033739A001HC | resolved_ori_case_legal_lot_block_ambiguous |

What matters here is that `_UPDATE_FORECLOSURE_SQL` was never attempted. `run()` only reaches the `UPDATE foreclosures ... RETURNING` write when `_resolve_one()` yields both a parcel candidate and a winning method. In this run it never did, so `rows_updated=0` is not a transaction failure. It is a pre-write resolution failure.

Some of these are genuine upstream or ambiguity cases. But several are self-inflicted false negatives:

- `_lookup_by_address()` requires exact equality on `property_address = :address`, so human-equivalent addresses like `CRESTHILL` vs `CREST HILL` or `BLAINE PLACE` vs `BLAINE TOP PL` return zero candidates.
- `_lookup_by_legal_description()` over-constrains candidate generation by AND-ing subdivision, lot, block, unit, and plat filters.
- `legal_descriptions_match()` then rejects some otherwise-plausible candidates because `UNIT NO` phase text is treated like a condo-unit mismatch, or because lot/block matches are refused when subdivision extraction is incomplete.

Without a resolved strap/folio, these foreclosures cannot participate in parcel-backed ORI persistence, title-chain materialization, map geocoding, or downstream encumbrance analysis.

---

## Issue 4: ORI Discovery — 9 Staged-Only Cases + Zero-Save Accounting

**Severity**: Medium — work was produced, but much of it never became persisted encumbrance data
**Log lines**: 233, 632-663, 836-838, 8623, 9641, 17605-17615

### No ownership chain rows block parcel-backed persistence

These cases had zero rows in `foreclosure_title_chain` when ORI ran `_discover_property`:

292022CA010632A001HC, 292023CA013582A001HC, 292023CA015629A001HC, 292024CA001233A001HC, 292024CA002668A001HC, 292024CA005958A001HC, 292025CA005858A001HC, 292025CA007403A001HC, 292025CA008140A001HC, 292025CA012216A001HC, 292025CC033739A001HC

Without ownership chain data, ORI cannot determine which parties to search for in the Official Records Index. The service falls back to case-number-only and LP-only document searches, which produce broad, unfiltered result sets.

### What actually persisted

For nine of the 14 ORI targets from the run, this fallback produced staged artifacts instead of PostgreSQL writes. Current file evidence still shows 13 unresolved bundles:

- `case_only_unresolved_lis_pendens_docs.json` for:
  `292025CC033739A001HC`, `292022CA010632A001HC`, `292023CA015629A001HC`,
  `292024CA005958A001HC`, `292024CA002668A001HC`, `292025CA012216A001HC`,
  `292025CA005858A001HC`, `292025CA007403A001HC`, `292025CA008140A001HC`
- `case_only_unresolved_documents.json` for:
  `292025CA012216A001HC`, `292025CA005858A001HC`,
  `292025CA007403A001HC`, `292025CA008140A001HC`

Current DB state for those staged cases still shows `step_ori_searched IS NULL` and no persisted `ori_encumbrances` rows. That staging behavior is conservative and intentional.

The other five parcel-backed ORI targets are a different failure class: they did reach `_save_documents()`, but that write path was broken by **Issue 1** (`AmbiguousParameter`) in the last run.

### Why the step still looked like `noop`

The controller only counts `encumbrances_saved + inferred_saved + satisfactions_linked`. It does not count:

- staged case-only / LP-only artifacts written to disk
- per-document savepoint rollbacks inside `_save_documents()`
- `step_ori_searched` timestamps that may be overwritten on later zero-save retries

So `ori_search` flattened "found documents but staged them" and "found documents but failed to save them" into the same `noop` outcome.

---

## Issue 5: Missing Map Coordinates — 10 Properties, Emitted 3 Times

**Severity**: Low — informational, upstream data incompleteness
**Log lines**: 74-84, 18065-18075, 18588-18598

The same 10 properties were logged three times (once per `foreclosure_refresh` invocation: initial, post-ORI, and final):

| ID | Date | Strap | Folio | Address |
|---|---|---|---|---|
| 100059 | 2026-04-24 | None | None | None |
| 100058 | 2026-04-24 | None | None | None |
| 100057 | 2026-04-24 | None | None | None |
| 100047 | 2026-04-23 | None | None | None |
| 100046 | 2026-04-23 | None | None | None |
| 100040 | 2026-04-22 | None | None | None |
| 100039 | 2026-04-22 | None | None | None |
| 100038 | 2026-04-22 | None | None | None |
| 100037 | 2026-04-22 | None | None | None |
| 15319 | 2026-03-20 | None | None | None |

All fields are None — these are foreclosure rows ingested from the clerk but not yet matched to any HCPA parcel. They are the same set of cases from Issue 3 (identifier recovery failures). Not a geocoder regression; the geocoder has nothing to geocode.

---

## Issue 6: Market Data Step — Degraded (Multiple Sub-Failures)

**Severity**: Medium — market data enrichment mostly failed this run
**Log line**: 18609

The market_data step reported `degraded` with `errors=1, updated=3`. The step ran three sub-phases:

### Phase 1: Scrapling (async HTTP, no browser)

Realtor scrapling attempted 5 properties, matched 0:
- 4 HTTP 429 (rate limited): 13908 PEPPERRELL DR, 2007 E CLINTON ST, 4102 E HANNA AVE, 10731 BANFIELD DR
- 1 blocked/captcha response: 15906 STAGS LEAP DR

### Phase 2: Browser phase (Playwright)

**Realtor browser**: Crashed immediately due to the `_run_realtor` signature mismatch (Issue 2). Zero properties attempted. Logged as `browser_phase_failed`.

**Redfin browser**: Launched, captcha overlay detected, waited 120s for manual solve. After solving, attempted 5 properties — 2 stale-result rejections, 5 consecutive "no search box" failures (blocked). Stopped after 5 consecutive failures.

**Zillow browser**: Captcha detected, waited 5 minutes, still present — aborted. 2 properties attempted, both blocked.

**Realtor browser (fallback via MarketDataService)**: 3 blocks on 3 properties.

### Phase 3: Worker (HomeFeedWorker)

23 foreclosures queried, final tally: redfin=2, zillow=0, realtor=0, homeharvest=1. This is the only phase that contributed real data (3 updates).

### Net result

Only 3 properties got market data out of 23 that needed it. The dominant failure modes are external blocking (captchas, rate limits) plus the `_run_realtor` code bug.

---

## Issue 7: PAV >1500 Records — Intentional Guardrails (128 Warnings)

**Severity**: None — working as designed
**Log lines**: 108-835 (interspersed)

ORI party-name searches for 9 large corporate entities exceeded the 1,500-record threshold and were skipped:

| Entity | Skipped Searches |
|---|---|
| LENNAR HOMES LLC | 58 |
| SECRETARY OF HOUSING AND URBAN DEVELOPMENT | 35 |
| NAVY FEDERAL CREDIT UNION | 14 |
| CALATLANTIC GROUP INC | 6 |
| WCI COMMUNITIES INC | 5 |
| LENNAR HOMES INC | 5 |
| PULTE HOME CORP | 2 |
| LGI HOMES FLORIDA LLC | 2 |
| CENTEX HOMES | 1 |

These entities have thousands of ORI records across Hillsborough County. Ingesting them all would flood the encumbrance table with false positives unrelated to the target properties. The `_pav_search` method at `pg_ori_service.py:3283` correctly skips these. This is noise, not a problem.

---

## Issue 8: `title_breaks` Persisted Retry-Blocking State Without Repairs

**Severity**: Medium-High — title quality stayed broken while retry scope was consumed
**Log lines**: 106-205 (PAV >1500 warnings for builder entities)
**Gaps affected**: 241 across 107 foreclosures

### What happened

The service identified 241 gaps and then wrote `107` `SEARCH_NO_RESULT` rows into `foreclosure_title_events`. It wrote **zero** `ORI_DEED_SEARCH` deed rows. The controller still labeled the step `noop` because it only counts `deeds_inserted + backfilled`.

That is the core bug:

1. the step persisted state
2. that persisted state did not improve title quality
3. that persisted state now excludes those foreclosures from future `title_breaks` scope

### Why this is worse than a plain search miss

Current DB state proves the retry poison:

- `_find_targets()`-equivalent scope now returns `0`
- `107` `SEARCH_NO_RESULT` rows exist from `2026-03-10`
- `68` sentinelized foreclosures still have `chain_status='BROKEN'` or `gap_count > 0`

So the step consumed retry budget on broken title chains and then hid behind `noop`.

The PAV truncation / fallback logic is still suspicious and likely contributes to empty search results for builder-heavy gaps. But even if those searches legitimately miss, the current sentinel semantics are still wrong for a repair workflow that is supposed to keep retrying unresolved broken chains.

---

## Priority Summary

| # | Issue | Severity | Fix Status |
|---|---|---|---|
| 1 | ORI `_save_documents` AmbiguousParameter — 221 instruments dropped | **High** | CAST fix applied, needs re-run to backfill + skip-count propagation |
| 2 | `_run_realtor` signature mismatch — browser Realtor broken | **High** | Not yet fixed |
| 8 | `title_breaks` sentinel writes poison retry scope without repairs | **Medium-High** | Fixed in code: target scope excludes complete chains, sentinels use a 14-day TTL refresh, and sentinel-writing runs now report degraded |
| 3 | Identifier recovery — write path never reached for 11 cases | Medium | Fixed in code: retry cooldown added, `UNIT NO` parsing tightened, and normalized address fallback now reaches conservative legal-confirmed matches |
| 4 | ORI staged-only cases + zero-save accounting | Medium | Fixed in code: zero-persistence runs no longer mark `step_ori_searched`, and staged/save-skip outcomes now report degraded instead of `noop` |
| 6 | Market data degraded — external blocking + Issue 2 | Medium | Partially addressed by fixing Issue 2; external blocking is operational noise |
| 5 | Missing coordinates — 10 properties x3 logs | Low | Same root cause as Issue 3 |
| 7 | PAV >1500 corporate skips — 128 warnings | Low | By design for encumbrances, but see Issue 8 for title_breaks impact |
