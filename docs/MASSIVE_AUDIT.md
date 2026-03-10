# Persistence Audit Report — Full Services Audit

**Date**: 2026-03-10
**Scope**: All 48 files in `src/services/` including subdirectories `audit/` and `lien_survival/`
**Method**: Automated parallel agent audit — 9 agents reading every line of every service file, tracing write paths end-to-end from controller step through service method to SQL/file writes to downstream consumers.

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Systemic Issue: Controller Stats Key Mismatch](#systemic-issue-controller-stats-key-mismatch)
3. [HIGH Severity Findings](#high-severity-findings)
4. [MEDIUM Severity Findings](#medium-severity-findings)
5. [LOW Severity Findings](#low-severity-findings)
6. [Per-Service Audit Details](#per-service-audit-details)
7. [Recommended Fix Priority](#recommended-fix-priority)

---

## Executive Summary

The pipeline **persists data correctly** in almost all cases. The SQL writes, UPSERTs, and file writes across all 48 services work as intended. However, the pipeline is **operationally blind** — nearly every step reports `status="noop"` to the controller regardless of actual work done, because the controller reads nonexistent dict keys from service return values.

**By the numbers:**
- **48 files** audited across 9 domains
- **5 HIGH** severity findings (1 systemic + 4 specific)
- **18 MEDIUM** severity findings
- **25+ LOW** severity findings
- **12+ pipeline steps** affected by the controller key mismatch
- **0 data loss bugs** — all writes reach PostgreSQL correctly
- **0 uncommitted transactions** — all services use `engine.begin()` properly

The single highest-ROI fix is the controller key mismatch, which is one file (`pg_pipeline_controller.py`) touching 12+ steps.

> **🔴 PUSHBACK (Antigravity):** The claim that the pipeline is "operationally blind" is **fundamentally misleading**. The controller constructs `StepResult` objects directly in each `_run_*` method, and **every StepResult includes `details=result`** containing the full raw service return dict. Downstream consumers (pipeline summary, `pipeline_job_runs.summary_json`) receive the complete data. The key name mismatches only affect convenience `.inserted`/`.updated` fields on the StepResult — not the authoritative record. This is a cosmetic reporting issue, not pipeline blindness. Additionally, 2 HIGH findings (H1, H2) are wrong or overstated, and 4 MEDIUM findings (M9, M11, M14, M18) are mischaracterized. The true severity breakdown after verification is: **2 genuine HIGH** (H3, H4), **7 confirmed MEDIUM**, and the rest are LOW/cosmetic/intentional design choices.

---

## Systemic Issue: Controller Stats Key Mismatch

**File**: `src/services/pg_pipeline_controller.py`
**Impact**: 12+ pipeline steps always report `status="noop"` with `inserted=0`/`updated=0`
**Root Cause**: The controller reads nonexistent dict keys from service return values

This is the most impactful finding. Every service persists data correctly, but the controller cannot see the results because it reads the wrong keys. The pipeline summary, `StepResult` log lines, and `pipeline_job_runs.summary_json` records are all misleading.

> **🔴 PUSHBACK:** This framing is wrong. The controller does NOT "read keys and feed them to some reporting layer." Each `_run_*` method constructs a `StepResult` object directly. For example, `_run_auction_scrape` (line 1052) does: `scraped = int(result.get("scraped", 0)) + int(result.get("rows_scraped", 0))`. This is **defensive additive summing** — it sums two possible key names gracefully defaulting to 0. The key names DO mismatch (confirmed), so `inserted=0` on the StepResult. But the full service return dict is ALWAYS attached via `details=result`. The `_step_result_from_payload` converter (lines 312-369) also preserves the raw payload. **This is a cosmetic reporting issue on StepResult convenience fields, not "operational blindness."** Severity should be LOW-MEDIUM at most.

### Complete Mismatch Table

| Step | Controller Method | Line | Controller Reads | Service Returns | Service File |
|------|------------------|------|-----------------|----------------|--------------|
| `auction_scrape` | `_run_auction_scrape` | 1052 | `scraped`, `rows_scraped` | `auctions_saved`, `dates_scraped`, `auctions_found` | `pg_auction_service.py` |
| `foreclosure_refresh` | `_run_foreclosure_refresh` | 866 | `refreshed`, `inserted`, `updated` | `enriched`, `strap_resolved`, `coords_enriched`, `resale`, `events_inserted`, `encumbrances`, `archived`, `judgments`, `upcoming_auctions` | `refresh_foreclosures.py` |
| `identifier_recovery` | `_run_identifier_recovery` | 1088 | `recovered` | `rows_updated`, `rows_scanned`, `errors` | `pg_foreclosure_identifier_recovery_service.py` |
| `judgment_extract` | `_run_judgment_extract` | 1065 | `updated`, `extracted`, `errors` | `pdfs_extracted`, `judgments_loaded_to_pg`, `pdfs_found` | `pg_judgment_service.py` |
| `mortgage_extract` | `_run_mortgage_extract` | 1135 | `extracted`, `updated`, `errors` | `mortgages_extracted`, `mortgages_found`, `elapsed_seconds` | `pg_mortgage_extraction_service.py` |
| `clerk_bulk` | `_run_clerk_bulk` | 428 | `rows_written`, `cases_upserted` | Nested: `cases["rows_upserted"]`, `events["rows_inserted"]`, `parties["rows_upserted"]`, etc. | `pg_clerk_bulk_service.py` |
| `clerk_criminal` | `_run_clerk_criminal` | 475 | `rows_written` | Nested: `load["rows_inserted"]` | `pg_clerk_criminal_service.py` |
| `clerk_civil_alpha` | `_run_clerk_civil_alpha` | 525 | `rows_written` | Nested: `load["cases_upserted"]`, `load["parties_upserted"]` | `pg_clerk_civil_alpha_service.py` |
| `nal` | `_run_nal` | 562 | `rows_written`, `parcels_upserted` | Nested: `load_stats["parcels_upserted"]` | `pg_nal_service.py` |
| `flr` | `_run_flr` | 592 | `rows_written`, `filings_upserted` | Nested: `load_stats["filings_upserted"]` | `pg_flr_service.py` |
| `trust_accounts` | `_run_trust_accounts` | 888 | `updated`, `inserted` | `rows_upserted`, `rows_deleted`, `summary_rows_written` | `trust_accounts.py` |
| `municipal_liens` | `_run_municipal_liens_phase0` | 1122 | `found`, `processed` | `findings_written`, `targets`, `evidence_rows_scanned` | `pg_municipal_lien_service.py` |

### Steps That ARE Correctly Wired

| Step | Controller Method | Line | Notes |
|------|------------------|------|-------|
| `hcpa` | `_run_hcpa_suite` | 400 | Reads `parcels_inserted`, `parcels_updated` — correct |
| `sunbiz_entity` | `_run_sunbiz_entity` | 651 | Reads `rows_written`, `filings_inserted` — correct |
| `county_permits` | `_run_county_permits` | 697 | Reads `written`, `rows_written` — correct |
| `tampa_permits` | `_run_tampa_permits` | 740 | Reads `written_total` — correct |
| `single_pin_permits` | `_run_single_pin_permits` | 826 | Reads `permits_observed_total`, `total_writes` — correct |
| `title_chain` | `_run_title_chain` | 899 | Reads `chain_rows`, `summary_rows`, `events_inserted` — correct |
| `title_breaks` | `_run_title_breaks` | 930 | Reads `deeds_inserted`, `backfilled` — correct |
| `ori_search` | `_run_ori_search` | 1101 | Reads `searched` (wrong), `targets` (exists), `errors` (exists) — **partially correct** |
| `survival` | `_run_survival_analysis` | 1154 | Reads `analyzed`, `errors` — correct |
| `encumbrance_recovery` | `_run_encumbrance_recovery` | 1250 | Reads `recovered`, `errors`, `skipped`, `degraded` — correct |

**Note on `ori_search`**: The controller reads `searched` (doesn't exist) + `targets` (exists). So `rows` = `0 + targets`, which is non-zero. The step reports `success` but with a slightly understated row count. Not a full mismatch but still inaccurate.

---

## HIGH Severity Findings

### H1. Controller Stats Key Mismatch (12+ steps)

See [Systemic Issue](#systemic-issue-controller-stats-key-mismatch) above.

**Fix**: Single PR to `pg_pipeline_controller.py` updating 12 `stats.get()` / `result.get()` calls.

> **🔴 PUSHBACK — VERDICT: ❌ OVERSTATED → Downgrade to LOW-MEDIUM.** The mismatch table IS accurate (key names don't match), and the fix IS easy. But the characterization as "pipeline is blind" is wrong. The full details dict is always propagated. Align the key names as a cleanup, not an urgent fix.

---

### H2. Job Control Finalization Cascade

**File**: `src/services/pg_job_control_service.py`, lines 217-233
**Issue**: If the job handler raises an exception AND `_finalize_run()` also raises (e.g., PG connection dropped), the `pipeline_job_runs` row stays permanently stuck as `status='running'` with no `finished_at`.

**Mechanism**:
```
handler() raises ExceptionA
  -> except block: conn.rollback()
  -> _finalize_run(..., status="failed") raises ExceptionB
  -> ExceptionB propagates out of except block
  -> finally block: conn.rollback() may also fail
  -> advisory lock may not be released
  -> pipeline_job_runs row stuck as "running"
```

**Self-healing**: `_expire_stale_running_rows()` eventually cleans up, but only when the same job is triggered again AND `max_runtime_sec` has elapsed. A singleton job would be blocked until then.

**Fix**: Wrap `_finalize_run` in the `except` block with its own try/except:
```python
except Exception as exc:
    conn.rollback()
    try:
        _finalize_run(conn, run_id, status="failed", ...)
        conn.commit()
    except Exception as finalize_exc:
        logger.error(f"Failed to finalize run {run_id}: {finalize_exc}")
    raise
```

> **🟡 PUSHBACK — VERDICT: ⚠️ OVERSTATED → Downgrade to LOW.** The scenario requires a *double failure* — handler raises AND PG drops during finalize. Verified the actual code (lines 217-242):
> 1. The `finally` block (lines 235-242) ALWAYS releases the advisory lock via `pg_advisory_unlock()` and calls `conn.commit()`, so **future runs are NOT blocked**.
> 2. `_expire_stale_running_rows()` runs **at the start of every job invocation** (line 113 in `run_job`), not "only when the same job is triggered again." ANY job run will clean stale rows for that job name.
> 3. The audit's claim "a singleton job would be blocked until then" is wrong — the lock is released in `finally`, and stale row expiry runs pre-flight.
> Wrapping `_finalize_run` in try/except is fine defensive coding but the blast radius is minimal.

---

### H3. Criminal Name Index NULL Constraint Collision

**File**: `src/services/pg_loader_clerk.py`, line 2276
**Issue**: `_insert_criminal_name_index_batch()` uses `ON CONFLICT DO NOTHING` on constraint `(ucn, count_number, disposition_code)`. In PostgreSQL, `NULL != NULL` in unique constraints. Rows with NULL `count_number` and/or `disposition_code` will **never be deduplicated** at the DB level.

**Impact**: Each full re-load of changed criminal files inserts duplicate rows for records with NULL `count_number`/`disposition_code`. The in-memory dedup (`_dedup_criminal_name_index()` at line 2262) coalesces NULLs to empty strings for the key, so duplicates within a single batch are caught. But across batches (re-loads), the DB constraint fails to catch them.

**Fix**: Normalize NULL `count_number` and `disposition_code` to empty strings before insert, matching the in-memory dedup behavior. Or add a partial unique index with `COALESCE`.

> **🟢 PUSHBACK — VERDICT: ✅ CONFIRMED.** Verified `_dedup_criminal_name_index()` (line 2258) coalesces NULLs to `""` for the in-memory key, but `_insert_criminal_name_index_batch()` (line 2271) passes raw rows to `pg_insert().on_conflict_do_nothing()` without normalization. PostgreSQL's `NULL != NULL` means cross-batch duplicates WILL accumulate. Fix is correct: COALESCE before insert.

---

### H4. Partial Mortgage Extraction Blocks Retries Forever

**File**: `src/services/pg_mortgage_extraction_service.py`, lines 271-289
**Issue**: When `_is_cache_complete(result)` returns False (partial extraction), the JSON cache is intentionally NOT written to disk (so future runs can retry). But the partial data IS written to the DB via `_save_to_pg()`. Since `_find_unextracted_mortgages` filters by `mortgage_data IS NULL`, the row now has non-null `mortgage_data` and will **never be retried**.

**Design intent**: Skip cache to enable retry. Actual behavior: DB write defeats the retry mechanism.

**Fix**: Either (a) skip the DB write when cache is incomplete, or (b) change the retry filter to also check for a `mortgage_data_complete` flag or a specific marker in the JSONB.

> **🟢 PUSHBACK — VERDICT: ✅ CONFIRMED.** Verified the full flow: Lines 272-281 skip cache when `_is_cache_complete()` is False. But lines 284-286 call `_save_to_pg(enc_id, result)` **unconditionally** regardless of completeness. Line 130 filters by `mortgage_data IS NULL`. Once partial data is written, the row is permanently excluded from retries. The design intent (skip cache → retry) is fully defeated by the unconditional DB write. Fix (a) is simpler — wrap `_save_to_pg()` in an `if self._is_cache_complete(result)` guard.

---

### H5. `refresh_foreclosures` Failure Swallowed in Both Market Workers

**Files**:
- `src/services/market_data_worker.py`, lines 106-109
- `src/services/pg_market_data_scrapling.py`, lines 1616-1620

**Issue**: Both market data entry points catch `refresh_foreclosures()` failure as a warning without setting an error/degraded flag on the output dict. This means:
1. Market data is successfully written to `property_market`
2. `refresh_foreclosures()` fails (doesn't propagate zestimate/list_price to `foreclosures`)
3. Worker returns `success`
4. `compute_net_equity()` reads stale/NULL values from `foreclosures.zestimate`
5. Dashboard shows stale equity figures

**Fix**: Set `output["degraded"] = True` and `output["error"] = str(exc)` when `refresh_foreclosures` fails.

> **🟡 PUSHBACK — VERDICT: ⚠️ PARTIALLY CORRECT → Downgrade to MEDIUM.** Verified both code paths. The failure IS silently caught (only logged as warning). Setting a degraded flag is a good fix. However, the impact chain is overstated:
> 1. The market data IS successfully written to `property_market` — only the copy to `foreclosures.zestimate` is deferred.
> 2. The pipeline's `_run_foreclosure_refresh` step (line 858) AND `_run_final_refresh` step (line 1270) run independently and will re-sync everything on the next pipeline run.
> 3. The dashboard reads from both `property_market` and `foreclosures` depending on the endpoint.
> The data is NOT lost, just temporarily out-of-sync until the next pipeline run. Severity is MEDIUM, not HIGH.

---

## MEDIUM Severity Findings

### M1. ORI `saved` Counter Inflated by UPSERT No-ops

**File**: `src/services/pg_ori_service.py`, `_save_documents` (~line 3992)
**Issue**: `saved += 1` is incremented after both UPDATE and INSERT/ON CONFLICT DO UPDATE paths regardless of whether data actually changed. A re-discovery pass that finds already-persisted documents reports `saved > 0`.
**Cascade**: `encumbrance_recovery.py:_changed_target_rows()` uses `saved > 0` to trigger unnecessary downstream mortgage extraction and survival re-analysis.
**Fix**: Return `saved_new` vs `saved_upserted` counters, or check whether the UPSERT actually changed data.

> **🟡 PUSHBACK — VERDICT: ⚠️ OVERSTATED → Downgrade to LOW.** The counter IS inflated, but the audit overstates the cascade. The recovery service only triggers when the audit report has open issues — it doesn't blindly re-run on `saved > 0`. The wasted downstream work amounts to a few extra SQL queries per run, not a significant performance issue.

---

### M2. ORI UPDATE/INSERT Key Mismatch for Multi-Parcel Instruments

**File**: `src/services/pg_ori_service.py`, `_save_documents` (~lines 3907-3947)
**Issue**: UPDATE uses `WHERE instrument_number = :instrument AND (folio IS NOT DISTINCT FROM :folio OR strap = :strap)`, but INSERT uses `ON CONFLICT (folio, COALESCE(instrument_number, ''), ...)`. For multi-parcel instruments with different folios, the UPDATE misses, the INSERT creates a new row.
**Impact**: Duplicate encumbrance rows for the same instrument across parcels.

> **🟡 PUSHBACK — VERDICT: ⚠️ NEEDS VERIFICATION.** The scenario IS theoretically possible with the described WHERE clause differences. However, without tracing the full 3900+ line ORI service and checking production data for actual duplicates, it's hard to confirm the real-world impact. Would need to run a query like `SELECT instrument_number, COUNT(*) FROM ori_encumbrances GROUP BY instrument_number HAVING COUNT(*) > 1` to validate.

---

### M3. LP-Missing Audit Bucket vs Recovery Query Scope Mismatch

**Files**: `src/services/audit/pg_audit_encumbrance.py` `_bucket_lp_missing` (~line 297) vs `src/services/pg_ori_service.py` `_find_lis_pendens_gap_targets` (~line 1414)
**Issue**: Audit checks LP by `oe.strap = f.strap` only. Recovery query also checks `oe.case_number = f.case_number_raw OR oe.case_number = f.case_number_norm`.
**Impact**: Audit can report gaps that recovery won't try to fix, or vice versa.

> **🟢 PUSHBACK — VERDICT: ✅ CONFIRMED.** The audit and recovery queries DO use different join conditions. This is a legitimate scope mismatch that can cause audit-reported gaps to be ignored by recovery.

---

### M4. Recovery `_changed_target_rows` False Positives

**File**: `src/services/audit/encumbrance_recovery.py`, lines 83-89
**Issue**: Uses `saved > 0` from ORI service to determine if target changed. Due to M1 (inflated `saved` counter), recovery triggers unnecessary downstream work.
**Impact**: Wasted processing time for mortgage extraction and survival re-analysis.

> **🟡 PUSHBACK — VERDICT: ⚠️ DEPENDENT ON M1.** Since M1 is overstated (recovery only fires on audit-reported issues, not blindly), M4's impact is proportionally reduced. The wasted work is extra SQL queries, not expensive vision/OCR processing. Downgrade to LOW.

---

### M5. Failed Audit Bucket Queries Reported as count=0

**File**: `src/services/audit/pg_audit_encumbrance.py`, lines 995-1008
**Issue**: When a bucket query throws (missing table, timeout), the bucket is marked `deferred=True` with count=0. Operators checking only counts would see "0 issues" for a bucket that actually failed.
**Fix**: Add a separate `error_count` field to the report.

> **🟢 PUSHBACK — VERDICT: ✅ CONFIRMED.** Legitimate operational visibility concern. Failed queries silently appearing as count=0 is misleading for operators.

---

### M6. Full-Table SQL for Single-Property Audit Snapshot

**File**: `src/services/audit/web_audit_service.py`, lines 194-213
**Issue**: `get_property_audit_snapshot` runs ALL bucket handlers (9 full-table queries) to check one property, then filters client-side.
**Impact**: Significant performance penalty for web requests on the property detail page.

> **🟢 PUSHBACK — VERDICT: ✅ CONFIRMED.** Running 9 full-table queries for a single-property web request is genuinely wasteful. Real performance issue.

---

### M7. Stale `foreclosure_encumbrance_survival` Rows on Re-analysis

**File**: `src/services/pg_survival_service.py`, `_save_survival_results` (~line 330)
**Issue**: Re-analysis UPSERTs current encumbrances but never DELETEs rows for encumbrances that have been reclassified (e.g., to NOC) or replaced with a different ID since the last analysis.
**Impact**: Downstream reads see outdated survival statuses for removed/reclassified encumbrances.
**Fix**: `DELETE FROM foreclosure_encumbrance_survival WHERE foreclosure_id = :fid` before the UPSERT loop.

> **🟢 PUSHBACK — VERDICT: ✅ CONFIRMED.** Verified `_save_survival_results` (line 330) does UPSERT per encumbrance without a preceding DELETE. Old rows for reclassified/removed encumbrances will persist. Fix is correct. However, practical impact depends on how often encumbrances get reclassified — likely rare in production but can accumulate over time.

---

### M8. No Sentinel Row for Failed PAV Searches in Title Breaks

**File**: `src/services/pg_title_break_service.py`, `_process_one` (~line 214)
**Issue**: When PAV returns zero deeds for a gap, no `ORI_DEED_SEARCH` event is inserted. `_find_targets` filters by `NOT EXISTS (... event_source = 'ORI_DEED_SEARCH')`, so the same foreclosure is re-queried every pipeline run.
**Impact**: Repeated PAV API calls for unfillable gaps on every run.
**Fix**: Insert a sentinel `ORI_DEED_SEARCH` event when search completes with 0 results.

> **🟢 PUSHBACK — VERDICT: ✅ CONFIRMED.** Verified `_process_one` returns `(len(gaps), 0)` when no deeds found (line ~218), with no sentinel event inserted. The target filter `NOT EXISTS (... event_source = 'ORI_DEED_SEARCH')` will re-include this foreclosure every run. Real API waste.

---

### M9. Scrapling Exceptions Lost from Step Details

**File**: `src/services/pg_pipeline_controller.py`, `_run_market_data` (~line 966)
**Issue**: The `except Exception` around scrapling catches and logs the error but leaves `scrapling_result = {}`. The step details dict has no record of the failure. `is_failed_payload({})` returns False.
**Fix**: Set `scrapling_result = {"error": str(exc), "success": False}`.

> **🔴 PUSHBACK — VERDICT: ❌ WRONG — THIS IS INTENTIONAL.** Verified `_run_market_data` (lines 940-1038). The scrapling phase is a **best-effort enrichment layer** that runs BEFORE the browser-based worker. When scrapling fails (line 966-967), it logs via `logger.exception()` and continues to the browser worker fallback. The overall step status (lines 997-1027) reflects the worker's outcome. The scrapling failure IS logged and the step DOES proceed with fallback. This is **intentional graceful degradation**, not a bug. Capturing the error in `scrapling_result` would be nice but the step is NOT misreporting — the composite StepResult details contain both `scrapling` and `worker` payloads.

---

### M10. Background Bulk Steps Have Zero Audit Trail

**Files**: `src/services/controller_step_dispatcher.py`, `src/services/bulk_step_worker.py`
**Issue**: Background-dispatched steps have no `pipeline_job_runs` record. The worker calls `run_bulk_step()` directly without `PgJobControlService` wrapping. If the worker crashes, the only evidence is the log file.
**Impact**: No DB-level audit trail for background step outcomes.

> **🟢 PUSHBACK — VERDICT: ✅ CONFIRMED.** Legitimate gap in audit trail coverage.

---

### M11. `_payload_status` Doesn't Handle `StepResult` Objects

**File**: `src/services/pg_job_control_service.py`, lines 444-468
**Issue**: `_payload_status` calls `payload.get("status")`, `payload.get("skipped")`, etc. If a handler returns a `StepResult` (which uses attributes, not dict `.get()`), this raises `AttributeError`.
**Impact**: Latent — currently all handlers return raw dicts, but a refactor could trigger this.

> **🔴 PUSHBACK — VERDICT: ❌ NOT A FINDING.** The audit itself says "Latent — currently all handlers return raw dicts." By definition, if no code path triggers it, it's a hypothetical, not a bug. `_payload_status` is only called from `run_job` (line 201) which invokes `definition.handler()` — and all registered handlers return plain dicts. If someone refactors a handler to return StepResult, they'll get a test failure immediately. This is speculative future-proofing, not a finding.

---

### M12. Enrichment State Query Failure Re-scrapes Everything

**File**: `src/services/pg_market_data_scrapling.py`, `_get_enrichment_state` (~line 157)
**Issue**: Returns `None` on any DB query failure. Caller treats `None` as "needs all sources", attempting to re-scrape every property for every source.
**Impact**: Temporary PG issue → full re-scrape → rate-limit risk.

> **🟡 PUSHBACK — VERDICT: ⚠️ OVERSTATED → Downgrade to LOW.** This requires a PG connection failure DURING a scraping run — the connection was working moments before to load the property list. This is an edge case within an edge case. And the rate-limiting delay profiles (15-55s between requests) would prevent actual rate-limit bans even if extra scraping occurred.

---

### M13. Consecutive Failure Backoff Doesn't Trigger on Captcha 200s

**File**: `src/services/pg_market_data_scrapling.py`, `_run_site_loop` (~line 1306)
**Issue**: `consecutive_failures` resets to 0 on ANY successful HTTP fetch, even if the response is a captcha page. Only fetch-level exceptions increment the counter.
**Impact**: A site returning captcha pages indefinitely will never trigger backoff.

> **🟢 PUSHBACK — VERDICT: ✅ CONFIRMED.** Verified line 1306: `consecutive_failures = 0` fires immediately after successful fetch, before `html_parser()` or `is_useful_fn()` evaluate the content. A captcha page returning HTTP 200 resets the counter. Legitimate backoff gap.

---

### M14. Two Divergent "Need Market Data" Queries

**Files**: `src/services/market_data_worker.py` (~line 21) vs `src/services/pg_market_data_scrapling.py` (~line 71)
**Issue**: Worker uses `_sql_source_has_market_content()`. Scrapling uses JSONB `?` operator for enrichment markers. Different definitions of "incomplete".
**Impact**: Inconsistent re-scraping decisions depending on entry point.

> **🔴 PUSHBACK — VERDICT: ❌ INTENTIONAL DESIGN.** The browser worker (`market_data_worker.py`) and scrapling service (`pg_market_data_scrapling.py`) serve DIFFERENT purposes. The scrapling service adds rich enrichment markers (`facts_and_features`, `property_facts`, etc.) that the browser worker doesn't produce. Different "completeness" definitions are intentional — the scrapling service checks for scrapling-specific markers, while the browser worker checks for basic content presence. They're complementary systems, not competing ones.

---

### M15. Tampa Enrichment Error Rate Unchecked

**File**: `src/services/TampaPermit.py`, `enrich_missing_details()` (~line 1623)
**Issue**: 100% enrichment failure (all 500 records error out) still returns without raising. Pipeline controller doesn't check the error rate.
**Impact**: Step reports `success` even when enrichment completely failed.

> **🟢 PUSHBACK — VERDICT: ✅ CONFIRMED.** Legitimate issue. 100% enrichment failure silently passing as success is a real visibility gap.

---

### M16. Temple Terrace Drops Entire Record on Detail Page Failure

**File**: `src/services/TempleTerracePermit.py`, lines 568-577
**Issue**: When `_fetch_detail_fields()` fails for a permit, the entire record (including search-row data) is discarded.
**Fix**: Persist partial records from search results even when detail page fails.

> **🟡 PUSHBACK — VERDICT: ⚠️ PLAUSIBLE but not fully verified.** Would need to trace the actual TempleTerracePermit code path to confirm whether partial persistence is feasible without breaking downstream consumers that expect complete records.

---

### M17. Municipal Error Understates `total_writes`

**File**: `src/services/pg_permit_single_pin_service.py`, lines 797-833
**Issue**: Municipal service error raises `RuntimeError` after HCPA-sourced writes are already committed. The error path returns `total_writes: 0`, understating actual persisted writes.

> **🟡 PUSHBACK — VERDICT: ⚠️ OVERSTATED.** The `RuntimeError` is caught by the controller, which marks the step as failed. The already-committed writes ARE in the database. The `total_writes: 0` in the error dict is misleading but the step is marked as failed regardless, so operators know something went wrong. The data is not lost.

---

### M18. Trust Accounts Schema Bypasses Alembic

**File**: `src/services/trust_accounts.py`, lines 312-398
**Issue**: `_ensure_schema()` uses raw `CREATE TABLE IF NOT EXISTS` and `ALTER TABLE ADD COLUMN` instead of Alembic migrations. Violates project rule requiring Alembic for all schema changes.

> **🔴 PUSHBACK — VERDICT: ❌ NOT A PERSISTENCE BUG.** This is a project convention/style judgment, not a correctness finding. `CREATE TABLE IF NOT EXISTS` is a common bootstrap pattern, especially for services that manage their own schema lifecycle. The audit judges project practices, not data persistence correctness. Whether to migrate to Alembic is a project governance decision, not a severity-rated finding.

---

## LOW Severity Findings

### L1. `pav_cache_put` Swallows Write Errors at `debug` Level
**File**: `src/services/pav_cache.py`, lines 73-74
**Impact**: Cache write failures invisible in normal log output.
**Fix**: Change to `logger.warning`.

> **🟢 PUSHBACK: ✅ Confirmed.** Trivial fix, agreed.

### L2. `pav_cache_get` Leaves Corrupt Files in Place
**File**: `src/services/pav_cache.py`, lines 57-62
**Impact**: Repeated warnings until 7-day TTL expires.
**Fix**: Delete corrupt files on read failure.

> **🟢 PUSHBACK: ✅ Confirmed.** Agreed.

### L3. `scraper_storage.needs_refresh()` Always Returns `True`
**File**: `src/services/scraper_storage.py`, lines 269-315
**Impact**: All scrapers always re-scrape, no cache benefit.

> **🟡 PUSHBACK: ⚠️ INTENTIONAL DESIGN.** The docstring literally says `"Always True (no DB cache tracking)."` This was a **deliberate design decision**, not a forgotten implementation. The `ScraperStorage` class was designed as write-only storage, not a cache layer. Calling this a "bug" misrepresents an architectural choice.
>
> **🔵 AUDIT RESPONSE: Pushback accepted.** Verified: the docstring explicitly says `"Always True (no DB cache tracking)."` and the methods are grouped under a section labeled `"Cache Stubs (No-ops for Inbox Pattern)"`. This is documented intentional design. However, `sunbiz_scraper.py` (line 468) and `fema_flood_scraper.py` (line 198) both have unreachable cache-loading branches because `not needs_refresh()` is always `False`. Those dead branches should either be removed or the stub should be implemented — having unreachable code that *looks* like it does something is confusing. **Verdict: Not a bug, but the dead caller branches are cleanup candidates. Withdraw as a finding.**

### L4. ORI `force_satisfaction_relink` Flag Is Dead Code
**File**: `src/services/pg_ori_service.py` (~line 779)
**Impact**: Audit-driven satisfaction re-linking only fires when new documents found.

> **🟡 PUSHBACK: ⚠️ NOT DEAD CODE.** This is a feature flag / toggle point. Being unused currently doesn't make it dead — it's an explicit configuration point that allows forcing satisfaction re-linking when needed. Dead code would be unreachable logic; this is a parameter defaulting to False.
>
> **🔵 AUDIT RESPONSE: Pushback rejected.** Verified the full code path: `force_satisfaction_relink` IS defined as a parameter (line 744) and IS checked at line 778 (`if force_satisfaction_relink: target["force_satisfaction_relink"] = True`). But `_process_target()` **never reads** `target["force_satisfaction_relink"]`. The key is set on the dict and then never consumed. A feature flag must have code that *reads* it and *changes behavior*. This parameter has zero behavioral effect — the `if` block at line 778-779 could be deleted without changing any execution path. The test at `tests/test_encumbrance_recovery.py:182` only verifies the parameter is *passed*, not that it *does* anything. **Verdict: This IS dead code. The original finding stands.**

### L5. ORI `saved` Field Double-Counts Inferred Encumbrances
**File**: `src/services/pg_ori_service.py` (~line 1716)
**Impact**: Callers cannot derive "ORI-discovered saves only" without subtraction.

> **🟢 PUSHBACK: ✅ Confirmed.** Minor counter clarity issue.

### L6. PAV API Failures Indistinguishable from Zero Results
**File**: `src/services/pg_ori_service.py`, `_post_pav` (~line 3351)
**Impact**: No `failed_api_calls` counter — failed search looks like empty results.

> **🟢 PUSHBACK: ✅ Confirmed.** Agree this is a visibility gap.

### L7. Auction Scrape Failures Not Tracked in Return Dict
**File**: `src/services/pg_auction_service.py`, `_scrape_range` (~line 110)
**Impact**: No programmatic way to know which dates failed.

> **🟢 PUSHBACK: ✅ Confirmed.** Verified `_scrape_range` catches per-date exceptions and logs them, but the return dict only has `dates_scraped`/`dates_skipped`/`auctions_found`/`auctions_saved` — no `dates_failed` counter.

### L8. Auction Results Pagination Capped at 10 Pages
**File**: `src/services/pg_auction_results_service.py`, `_scrape_date` (~line 229)
**Impact**: Silent truncation on high-volume auction days.

> **🟢 PUSHBACK: ✅ Confirmed.** Low impact; Hillsborough County rarely has >10 pages of auction results per day.

### L9. Auction Results `not_found_in_pg` Has No Threshold Check
**File**: `src/services/pg_auction_results_service.py`, `_save_outcomes` (~line 370)
**Impact**: High mismatch rate not surfaced as a warning.

> **🟢 PUSHBACK: ✅ Confirmed.** Minor visibility improvement.

### L10. Clerk Row Count Inflation in `on_conflict_do_nothing` Loaders
**Files**: `pg_loader_clerk.py` (events ~line 998, garnishments ~line 1535, criminal ~line 2276)
**Impact**: `rows_inserted` counts CSV rows parsed, not actual DB inserts.

> **🟢 PUSHBACK: ✅ Confirmed.** Cosmetic counter issue.

### L11. Clerk Events `on_conflict_do_nothing` Discards Corrections
**File**: `pg_loader_clerk.py`, `_insert_events_batch` (~line 1031)
**Impact**: Corrections from new file snapshots silently dropped.

> **🟡 PUSHBACK: ⚠️ INTENTIONAL BEHAVIOR.** `ON CONFLICT DO NOTHING` is the INTENDED behavior for idempotent loads of clerk bulk data. Clerk bulk files are additive snapshots — corrections come as new rows, not updates to existing rows. Using `DO UPDATE` would risk overwriting authoritative data with re-exported duplicates.
>
> **🔵 AUDIT RESPONSE: Pushback rejected.** Verified the unique constraint `uq_clerk_events_case_code_date_party` is on `(case_number, event_code, event_date, party_last_name)` — it does NOT include `event_description`. If the clerk re-publishes a monthly bulk file with a corrected event description (e.g., fixing an OCR typo from "TRANSERED" to "TRANSFERRED"), the constraint matches the old row and `ON CONFLICT DO NOTHING` silently discards the correction. The claim that "corrections come as new rows" is incorrect — clerk monthly bulk files are *complete snapshots*, not append-only streams. A corrected event has the same case/code/date/party but different description text. The stale description persists forever. **Verdict: The original finding stands. `DO UPDATE SET event_description = EXCLUDED.event_description` would be safe and correct.**

### L12. Clerk Alpha Index Party Dedup NULL/Empty-String Inconsistency
**File**: `pg_loader_clerk.py`, `_dedup_alpha_parties` (~line 2043)
**Impact**: In-memory dedup stricter than DB constraint for NULL parties.

> **🟢 PUSHBACK: ✅ Confirmed.** Same class of issue as H3 but for party rows. Lower impact because parties have more columns in the unique constraint, making accidental duplicates rarer.

### L13. Empty Clerk File Downloaded Then Blocks Future Downloads
**File**: `pg_loader_clerk.py` (~line 573-576)
**Impact**: Permanently empty files prevent re-download.

> **🟡 PUSHBACK: ⚠️ NOT A BUG.** The file tracking uses SHA-256 checksums. An empty file with a valid checksum IS treated as "already loaded." This prevents repeatedly downloading a file the clerk intentionally left empty (e.g., no new garnishments for a period). If a file is truly invalid, manual deletion of the local file will trigger re-download.
>
> **🔵 AUDIT RESPONSE: Pushback rejected on facts.** Verified the code: the *download* phase (lines 556-559) uses **file existence only** (`if target.exists() and not force: continue`), NOT SHA-256. SHA-256 is only used in the *load* phase (lines 785-798) for skip-unchanged logic. The pushback conflates two different code paths. An empty file saved to disk during download (e.g., server returned 0 bytes) will be skipped on every subsequent download run because `target.exists()` returns True. The load phase will then try to parse the empty file and fail/skip. **Verdict: The original finding stands — empty files block re-download via naive path existence check, not SHA-256.**

### L14. Triple HTTP Fetch of Same Clerk Listing Page
**File**: `pg_loader_clerk.py`, `download_clerk_bulk` (~lines 517-519)
**Impact**: 3 identical HTTP requests for case/event/party categories.

> **🟡 PUSHBACK: ⚠️ MISLEADING.** The clerk website has SEPARATE listing pages for cases, events, and parties. These are different URLs, not the same page fetched 3 times. Each category has its own file listing endpoint. Would need to verify the exact URLs to confirm, but the download function iterates over different categories.
>
> **🔵 AUDIT RESPONSE: Pushback rejected on facts.** Verified lines 517-519: all three calls pass the exact same constant `CLERK_BULK_URL` (`https://publicrec.hillsclerk.com/Civil/bulkdata/`). `_fetch_listing_filenames` takes only a URL parameter — no category filter. The page is fetched 3 times, returning the same HTML each time. File filtering by category happens *after* the fetch, using regex patterns (`CASE_FILE_PATTERN`, `EVENT_FILE_PATTERN`, `PARTY_FILE_PATTERN`). The `disposed`, `garnishment`, and `official_records` categories correctly use separate URLs. But case/event/party ARE the same URL fetched 3 times. **Verdict: The original finding stands. The fix is trivial — fetch once and share.**

### L15. NAL Millage Backfill Failure Marks Entire Step as Failed
**File**: `src/services/pg_nal_service.py`, lines 405-412
**Impact**: Post-load enrichment failure invalidates already-committed parcel data.

> **🟢 PUSHBACK: ✅ Confirmed.** Agreed — enrichment failure should be degraded, not failed.

### L16. FLR "Downloaded" Count Includes Pre-existing Files
**File**: `src/services/pg_flr_service.py`, lines 307-321
**Impact**: Misleading sync stats.

> **🟢 PUSHBACK: ✅ Confirmed.** Cosmetic only.

### L17. Foreclosure Service Read Methods Mask DB Errors
**File**: `src/services/pg_foreclosure_service.py` (lines 91, 106, 154, 172, 201)
**Impact**: Dashboard shows empty results instead of error when PG fails.

> **🟢 PUSHBACK: ✅ Confirmed.** Legitimate — masking DB errors as empty results is poor UX.

### L18. Dead Code: `update_pipeline_step()`
**File**: `src/services/pg_foreclosure_service.py`, lines 208-243
**Impact**: No callers. Can be removed.

> **🟡 PUSHBACK: ⚠️ CLEANUP CANDIDATE, NOT A BUG.** Dead code removal is housekeeping, not a persistence finding.
>
> **🔵 AUDIT RESPONSE: Pushback accepted.** Fair point — dead code is housekeeping. Withdraw as a persistence finding, retain as cleanup candidate.

### L19. Identifier Recovery ORI Session Bootstrap Failure Ignored
**File**: `src/services/pg_foreclosure_identifier_recovery_service.py`, lines 305-317
**Impact**: Service attempts ORI searches that may fail due to missing session cookies.

> **🟡 PUSHBACK: ⚠️ NEEDS DEEPER TRACE.** The recovery service's `run()` method (line 332) shows a well-structured flow. The bootstrap failure claim needs to trace the actual `_ensure_ori_session()` call path to confirm whether failed sessions actually cause search failures or if the ORI API works without session cookies for public data.
>
> **🔵 AUDIT RESPONSE: Pushback accepted — original finding overstated.** Verified by comparing the identifier recovery service (lines 303-317, which bootstraps cookies) against `pg_ori_service.py` (lines 460-461, which does NOT bootstrap). Both hit the same PAV KeywordSearch API. The ORI service works successfully without any cookie seeding, proving the bootstrap is unnecessary overhead (~500ms per init). The PAV APIs are stateless public endpoints. The bootstrap failure is harmless because it doesn't actually affect API functionality. **Verdict: The bootstrap is dead code that should be removed as cleanup, but it's not a "failure ignored" bug. Downgrade to cleanup candidate.**

### L20. Mortgage `_save_to_pg` Doesn't Check Rowcount
**File**: `src/services/pg_mortgage_extraction_service.py`, line 341
**Impact**: Counter incremented even when UPDATE affects 0 rows (phantom success).

> **🟢 PUSHBACK: ✅ Confirmed.** Agreed — rowcount check would prevent phantom success.

### L21. No Error Count Returned from Judgment/Mortgage Services
**Files**: `pg_judgment_service.py`, `pg_mortgage_extraction_service.py`
**Impact**: Controller's failure detection (`errs > 0 and extracted == 0`) can never trigger.

> **🟡 PUSHBACK: ⚠️ MISLEADING.** The controller reads `result.get("errors", 0)` which safely returns 0 when the key doesn't exist. The judgment service returns `pdfs_found`, `pdfs_extracted`, `judgments_loaded_to_pg` — no `errors` key because errors are caught per-PDF and logged individually. `errors == 0` correctly reflects "no unrecoverable errors" — the service handles errors internally. The controller's `extracted == 0` check still fires correctly when no work was done.
>
> **🔵 AUDIT RESPONSE: Pushback rejected.** Verified the full control flow. The pushback claims "the controller's `extracted == 0` check still fires correctly when no work was done." This is **wrong**. Here's the exact path when ALL 10 PDFs fail extraction: (1) Each `process_pdf()` throws, caught at line 114, logged, loop continues. (2) `extracted = 0`, `loaded = 0`. (3) Return dict: `{"pdfs_found": 10, "pdfs_extracted": 0, "judgments_loaded_to_pg": 0}` — no `errors` key. (4) Controller: `updated = int(result.get("updated", 0)) + int(result.get("extracted", 0))` → both keys missing → `updated = 0`. (5) `errs = int(result.get("errors", 0))` → `0`. (6) Status: `"failed" if (0 > 0 and 0 == 0)` → False → `"success" if (0 > 0)` → False → **"noop"**. The step reports "noop" (nothing to do) when in reality 10 PDFs failed. The failure condition `errs > 0 and updated == 0` is inoperative because `errs` is always 0. **Verdict: This is a real finding. Should be upgraded to MEDIUM — 100% extraction failure is misclassified as "nothing to do".**

### L22. Final Judgment Merge Truthiness Bug
**File**: `src/services/final_judgment_processor.py`, `_merge_page_data` (~line 191)
**Impact**: `False`, `0`, `0.0` treated as "missing" and overwritten by later pages.

> **🟢 PUSHBACK: ✅ Confirmed.** Legitimate edge case for multi-page PDFs where page 1 returns `0` for a field and page 2 has a non-zero value.

### L23. Survival `_prior_survival_status` Set But Never Read
**File**: `src/services/lien_survival/survival_service.py`, line 149
**Impact**: Dead code adding confusion.

> **🟡 PUSHBACK: ⚠️ CLEANUP CANDIDATE, NOT A BUG.** Same as L18 — dead code cleanup, not a persistence finding.
>
> **🔵 AUDIT RESPONSE: Pushback accepted.** Same rationale as L18. Withdraw as a persistence finding, retain as cleanup candidate.

### L24. Legacy `ori_encumbrances.survival_status` Last-Writer-Wins
**File**: `src/services/pg_survival_service.py`, lines 397-412
**Impact**: Unreliable for shared-strap properties. Documented but still risky.

> **🟢 PUSHBACK: ✅ Confirmed.** Agreed—this is a known limitation documented in the code.

### L25. `pg_trust_accounts.py` Doesn't Call `super().__init__()`
**File**: `src/services/pg_trust_accounts.py`, lines 28-36
**Impact**: Fragile if parent adds new attributes.

> **🟡 PUSHBACK: ⚠️ SPECULATIVE.** The parent class's `__init__` may not define attributes that need initialization. This is a defensive coding concern, not a current bug. If the parent adds attributes, the child will break immediately in testing.
>
> **🔵 AUDIT RESPONSE: Pushback accepted.** Speculative future-proofing is not a finding. Withdraw.

### L26. Trust Accounts Single Transaction for All Reports
**File**: `src/services/trust_accounts.py`, line 162
**Impact**: Failure on 5th report loses writes from reports 1-4.

> **🟢 PUSHBACK: ✅ Confirmed.** Legitimate concern — wrapping each report in its own transaction would prevent data loss on partial failures.

### L27. Market Data `_f()` and `_i()` Discard Zero Values
**File**: `src/services/market_data_service.py` (~lines 1488, 1499)
**Impact**: Valid `baths=0` (studio) discarded. `COALESCE` falls through to stale value.

> **🟢 PUSHBACK: ✅ Confirmed.** Legitimate edge case for `baths=0`.

### L28. Background Dispatcher Returns `skipped=True` for Successful Dispatch
**File**: `src/services/controller_step_dispatcher.py`, lines 100-107
**Impact**: Successfully dispatched step reported as "skipped" in pipeline summary.

> **🟡 PUSHBACK: ⚠️ INTENTIONAL BEHAVIOR.** A dispatched step IS "skipped" from the controller's perspective — the controller didn't run it synchronously. The dispatch itself succeeds and is tracked separately. This is by design: the pipeline summary shows what the controller did, and dispatched steps run asynchronously.
>
> **🔵 AUDIT RESPONSE: Pushback partially accepted.** The "skipped from the controller's perspective" framing is reasonable for *market_data* — verified that `_run_market_data` (lines 1082-1151) explicitly checks `worker_result.get("dispatched") is True` and handles it separately. However, for **bulk steps**, `_step_result_from_payload` (lines 315-371) only checks `payload_dict.get("skipped")` and ignores `dispatched=True` entirely. Bulk steps cannot distinguish "worker already running" from "successfully dispatched" — both become `status="skipped"`. The intent is reasonable but the implementation is inconsistent between market_data and bulk steps. **Verdict: Partially intentional. Market_data: withdraw. Bulk steps: the `dispatched` key is set but unused — same class of issue as L4.**

### L29. Vision Service Global Semaphore Not Thread-Safe
**File**: `src/services/vision_service.py`, lines 1233-1234
**Impact**: Theoretical — pipeline is single-threaded.

> **🔴 PUSHBACK: ❌ NON-FINDING.** The audit itself says "pipeline is single-threaded." A theoretical thread-safety concern in a single-threaded system is not a finding. If the architecture ever becomes multi-threaded, this would be caught in testing.
>
> **🔵 AUDIT RESPONSE: Pushback accepted.** Theoretical concern in a single-threaded system. Withdraw.

### L30. Municipal Lien Utility Hint Token Overly Broad
**File**: `src/services/pg_municipal_lien_service.py`, line 69
**Impact**: `"LIEN"` token combined with `"HILLSBOROUGH COUNTY"` matches non-utility liens.

> **🟢 PUSHBACK: ✅ Confirmed.** Legitimate false-positive risk in lien matching.

### L31. `_upsert_tampa_from_single_pin` Missing `is_violation` Column
**File**: `src/services/pg_permit_single_pin_service.py`, lines 623-665
**Impact**: Reversed COALESCE + missing column can lock in incorrect initial value.

> **🟢 PUSHBACK: ✅ Confirmed.** Legitimate data quality issue.

### L32. Scrapling Error Count Understates Severity
**File**: `src/services/pg_market_data_scrapling.py`, `_safe_site_run` (~line 1573)
**Impact**: If 50 properties queued for a site that crashes, `scrapling_errors=1` not `50`.

> **🟢 PUSHBACK: ✅ Confirmed.** Legitimate counter issue — `_safe_site_run` returns `(0, 1)` on exception, but the `1` represents one site failure, not per-property failures.

---

## Per-Service Audit Details

### Domain 1: Auction & Foreclosure Core

#### `pg_auction_service.py`
- **Purpose**: Phase B Step 1. Scrapes upcoming auctions from realforeclose.com, UPSERTs into `foreclosures`.
- **Write targets**: `foreclosures` table (INSERT ON CONFLICT DO UPDATE by `case_number_raw, auction_date`)
- **Issues**: H1 (controller key mismatch), L7 (failures not tracked)
- **Downstream**: `PgAuctionResultsService`, `refresh_foreclosures`, `PgJudgmentService`, dashboard

#### `pg_auction_results_service.py`
- **Purpose**: Re-scrapes auction outcomes (sold/canceled/status) for active dates.
- **Write targets**: `foreclosures` table (UPDATE), `archived_at` for terminal outcomes
- **Issues**: L8 (pagination cap), L9 (no mismatch threshold)
- **Downstream**: `refresh_foreclosures`, dashboard, `trust_accounts`, competition analysis

#### `pg_foreclosure_service.py`
- **Purpose**: Read/write service layer over `foreclosures`. Delegates refresh to `refresh_foreclosures.py`.
- **Write targets**: `foreclosures` table (via refresh delegation)
- **Issues**: H1 (controller key mismatch), L17 (read methods mask errors), L18 (dead code)
- **Downstream**: Dashboard routes, pipeline controller

#### `pg_foreclosure_identifier_recovery_service.py`
- **Purpose**: Phase B Step 2.5. Fills missing `strap`/`folio` using judgment data, parcel lookups, ORI, legal description matching.
- **Write targets**: `foreclosures` table (UPDATE: strap, folio, property_address via COALESCE)
- **Issues**: H1 (controller key mismatch), L19 (ORI session bootstrap)
- **Downstream**: `PgOriService`, `PgSurvivalService`, title chain, dashboard

### Domain 2: Judgment, Vision & Mortgage

#### `pg_judgment_service.py`
- **Purpose**: Phase B Step 2. Finds PDFs, runs vision extraction, pushes JSON to `foreclosures.judgment_data`.
- **Write targets**: `foreclosures.judgment_data` (JSONB), `foreclosures.pdf_path`, `foreclosures.final_judgment_amount`, timestamps
- **Issues**: H1 (controller key mismatch), L21 (no error count)
- **Downstream**: ORI service, survival, municipal liens, refresh, dashboard

#### `final_judgment_processor.py`
- **Purpose**: Multi-pass VisionService extraction. Renders PDF pages, merges results, writes JSON cache.
- **Write targets**: JSON cache files, OCR debug text, temp images
- **Issues**: L22 (merge truthiness bug)
- **Downstream**: `PgJudgmentService`, `refresh_foreclosures`

#### `vision_service.py`
- **Purpose**: Stateless extraction layer. Wraps multiple vision endpoints with fallback.
- **Write targets**: None (pure extraction)
- **Issues**: None persistence-related. MEDIUM silent risk: truncated JSON can be "repaired" into semantically wrong data.
- **Downstream**: `FinalJudgmentProcessor`, `PgMortgageExtractionService`, `PgOriService`

#### `pg_mortgage_extraction_service.py`
- **Purpose**: Phase B Step 5. Downloads mortgage PDFs from PAV, runs vision extraction, saves to `ori_encumbrances.mortgage_data`.
- **Write targets**: `ori_encumbrances.mortgage_data` (JSONB), `ori_encumbrances.amount`, PDF files, JSON cache
- **Issues**: H1 (controller key mismatch), H4 (partial extraction blocks retries), L20 (no rowcount check), L21 (no error count)
- **Downstream**: `ori_encumbrances.amount` used by `compute_net_equity()`; `mortgage_data` currently has **no consumers**

### Domain 3: ORI & Encumbrances

#### `pg_ori_service.py`
- **Purpose**: Phase B Step 3. ORI document discovery and encumbrance persistence.
- **Write targets**: `ori_encumbrances` (INSERT/UPDATE/UPSERT), `foreclosures.step_ori_searched`, ORI JSON stage files, PAV cache
- **Issues**: M1 (saved counter inflation), M2 (UPDATE/INSERT key mismatch), M3 (LP scope mismatch), L4 (dead flag), L5 (double-count), L6 (API failures invisible)
- **Downstream**: Survival service, mortgage extraction, recovery service, dashboard, audit system, title chain

#### `audit/encumbrance_audit_signals.py`
- **Purpose**: Read-only signal extraction for LP-to-judgment delta analysis.
- **Write targets**: None
- **Issues**: None persistence-related
- **Downstream**: Audit report, web audit, recovery service

#### `audit/encumbrance_recovery.py`
- **Purpose**: Audit-driven recovery orchestrator. Delegates to ORI, mortgage, survival services.
- **Write targets**: Delegates all writes to sub-services
- **Issues**: M4 (false positives from inflated saved counter)
- **Downstream**: Pipeline controller, operators, post-audit comparison

#### `audit/pg_audit_encumbrance.py`
- **Purpose**: Read-only encumbrance audit report generator.
- **Write targets**: None (CSV in CLI mode only)
- **Issues**: M5 (failed queries masked as count=0)
- **Downstream**: Recovery service, web audit, CLI, operators

#### `audit/web_audit_service.py`
- **Purpose**: Read-only web adapter for audit system.
- **Write targets**: None
- **Issues**: M6 (full-table queries for single property)
- **Downstream**: Web property detail page, operator inbox

### Domain 4: Title Chain, Breaks & Survival

#### `pg_title_chain_controller.py`
- **Purpose**: Materializes timeline data into `foreclosure_title_events`, `foreclosure_title_chain`, `foreclosure_title_summary`.
- **Write targets**: Three tables (INSERT after DELETE), DDL for schema/functions
- **Issues**: None significant. Sound transactional design.
- **Downstream**: Title break service, survival service, dashboard, pipeline quality gates

#### `pg_title_break_service.py`
- **Purpose**: ORI deed search to fill title chain gaps. Writes overlay rows to `foreclosure_title_events`.
- **Write targets**: `foreclosure_title_events` (INSERT with NOT EXISTS dedup)
- **Issues**: M8 (no sentinel row for failed PAV searches)
- **Downstream**: Title chain controller (incorporates overlays during rebuild)

#### `pg_survival_service.py`
- **Purpose**: Orchestrates lien survival analysis. Persists to `foreclosure_encumbrance_survival` and `ori_encumbrances`.
- **Write targets**: `foreclosure_encumbrance_survival` (INSERT ON CONFLICT DO UPDATE), `ori_encumbrances` (UPDATE), `foreclosures.step_survival_analyzed`
- **Issues**: M7 (stale rows not cleaned), L24 (last-writer-wins for shared straps)
- **Downstream**: Dashboard, pipeline quality gates, `compute_net_equity`

#### `lien_survival/survival_service.py`
- **Purpose**: Pure computation. Takes encumbrances + judgment + chain, returns categorized survival statuses.
- **Write targets**: None (in-memory mutation only)
- **Issues**: L23 (dead `_prior_survival_status`)
- **Downstream**: `PgSurvivalService`

#### `lien_survival/priority_engine.py`
- **Purpose**: Pure computation. Foreclosing lien identification, seniority, historical detection.
- **Write targets**: None
- **Issues**: None
- **Downstream**: `SurvivalService`

#### `lien_survival/joinder_validator.py`
- **Purpose**: Pure computation. Validates creditor joinder via fuzzy name matching.
- **Write targets**: None
- **Issues**: None
- **Downstream**: `SurvivalService`

#### `lien_survival/statutory_rules.py`
- **Purpose**: Pure computation. Florida statute-based expiration checks.
- **Write targets**: None
- **Issues**: None persistence-related
- **Downstream**: `SurvivalService`

### Domain 5: Clerk Services

#### `pg_clerk_bulk_service.py`
- **Purpose**: Downloads and loads Hillsborough Clerk civil bulk CSV data.
- **Write targets**: `clerk_civil_cases`, `clerk_civil_events`, `clerk_civil_parties`, `clerk_disposed_cases`, `clerk_garnishment_cases`, `official_records_daily_instruments`, `ingest_files`
- **Issues**: H1 (controller key mismatch — nested stats)
- **Downstream**: Pipeline controller, dashboard

#### `pg_clerk_civil_alpha_service.py`
- **Purpose**: Downloads and loads Clerk civil alphabetical index.
- **Write targets**: `clerk_civil_cases`, `clerk_civil_parties`, `ingest_files`
- **Issues**: H1 (controller key mismatch — nested stats)
- **Downstream**: Pipeline controller, dashboard

#### `pg_clerk_criminal_service.py`
- **Purpose**: Downloads and loads Clerk criminal name index.
- **Write targets**: `clerk_criminal_name_index`, `ingest_files`
- **Issues**: H1 (controller key mismatch — nested stats), H3 (NULL constraint collision)
- **Downstream**: Pipeline controller, dashboard

#### `pg_loader_clerk.py`
- **Purpose**: Core loader with download, parse, SHA-256 dedup, batched upserts.
- **Write targets**: All clerk tables + `ingest_files`
- **Issues**: H3 (NULL constraint), L10 (row count inflation), L11 (corrections discarded), L12 (dedup inconsistency), L13 (empty file blocking), L14 (triple HTTP fetch)
- **Downstream**: All clerk services

#### `models_clerk.py`
- **Purpose**: SQLAlchemy ORM model definitions. No write logic.
- **Issues**: None
- **Downstream**: All loaders

### Domain 6: Permits

#### `CountyPermit.py`
- **Purpose**: Bulk pull Hillsborough County permits from ArcGIS.
- **Write targets**: `county_permits` table, optional Parquet
- **Issues**: None significant. COALESCE-never-clear pattern (LOW).
- **Downstream**: Title chain controller, `get_property_permits()`, dashboard

#### `TampaPermit.py`
- **Purpose**: Browser-automated CSV export from Tampa Accela.
- **Write targets**: `tampa_accela_records`, CSV files
- **Issues**: M15 (enrichment error rate unchecked)
- **Downstream**: Title chain controller, dashboard

#### `PlantCityPermit.py`
- **Purpose**: Plant City Maintstar portal permits.
- **Write targets**: `tampa_accela_records` (with `PLANTCITY:` prefix)
- **Issues**: None significant
- **Downstream**: Title chain controller, dashboard

#### `TempleTerracePermit.py`
- **Purpose**: Temple Terrace Click2Gov permits.
- **Write targets**: `tampa_accela_records` (with `TEMPLETERRACE:` prefix)
- **Issues**: M16 (drops entire record on detail failure)
- **Downstream**: Title chain controller, dashboard

#### `pg_permit_single_pin_service.py`
- **Purpose**: Single-PIN permit gap-fill across all jurisdictions.
- **Write targets**: `county_permits`, `tampa_accela_records`
- **Issues**: M17 (municipal error understates writes), L31 (missing `is_violation`)
- **Downstream**: Title chain controller, dashboard

### Domain 7: Market Data

#### `market_data_service.py`
- **Purpose**: Core market data service. Redfin, Zillow, Realtor, HomeHarvest scraping.
- **Write targets**: `property_market` table, `data_change_log`, photo files
- **Issues**: L27 (`_f()`/`_i()` discard zeroes)
- **Downstream**: `compute_net_equity`, `refresh_foreclosures`, dashboard

#### `market_data_dispatcher.py`
- **Purpose**: Process launcher for market data worker.
- **Write targets**: PID/lock files only
- **Issues**: L28 (returns `skipped=True` for successful dispatch)
- **Downstream**: Pipeline controller

#### `market_data_worker.py`
- **Purpose**: Standalone worker for browser-based market scraping.
- **Write targets**: `property_market` (via `MarketDataService.run_batch()`)
- **Issues**: H5 (refresh_foreclosures failure swallowed), M14 (divergent neededness query)
- **Downstream**: `refresh_foreclosures`, `compute_net_equity`

#### `pg_market_data_scrapling.py`
- **Purpose**: Scrapling-based (headless fetch) market scrapers.
- **Write targets**: `property_market` (via inherited upserts)
- **Issues**: H5 (refresh failure swallowed), M12 (enrichment state failure), M13 (backoff doesn't trigger on captchas), L32 (error count understates)
- **Downstream**: `refresh_foreclosures`, `compute_net_equity`

### Domain 8: Pipeline Control & Jobs

#### `pg_pipeline_controller.py`
- **Purpose**: Top-level orchestrator. ~24 steps, delegates to services.
- **Write targets**: `foreclosures.step_survival_analyzed` (nullified during recovery)
- **Issues**: H1 (systemic key mismatch), M9 (scrapling exceptions lost), M10 (background no audit trail)
- **Downstream**: `Controller.py`, `PgJobControlService`, `bulk_step_worker`

#### `controller_step_dispatcher.py`
- **Purpose**: Fire-and-forget subprocess launcher for bulk steps.
- **Write targets**: PID/lock/log files only
- **Issues**: L28 (skipped=True for dispatch)
- **Downstream**: Pipeline controller, bulk_step_worker

#### `bulk_step_worker.py`
- **Purpose**: Standalone subprocess for background bulk steps.
- **Write targets**: Delegates to controller methods
- **Issues**: M10 (no `pipeline_job_runs` tracking)
- **Downstream**: Launched by dispatcher, calls controller methods

#### `pg_job_control_service.py`
- **Purpose**: PG-backed job execution with policy enforcement.
- **Write targets**: `pipeline_job_config`, `pipeline_job_runs`
- **Issues**: H2 (finalization cascade), M11 (`_payload_status` type safety)
- **Downstream**: `run_scheduled_job.py`

### Domain 9: NAL, FLR, Trust, Municipal, Storage, Cache

#### `pg_nal_service.py`
- **Purpose**: DOR NAL tax assessment data loader.
- **Write targets**: `dor_nal_parcels`, `ingest_files`
- **Issues**: H1 (controller key mismatch — nested stats), L15 (millage failure marks step failed)
- **Downstream**: `compute_net_equity`, survival (homestead), dashboard tax tab

#### `pg_flr_service.py`
- **Purpose**: UCC/FLR filing data from FL SOS SFTP.
- **Write targets**: `sunbiz_flr_filings`, `sunbiz_flr_parties`, `sunbiz_flr_events`, `ingest_files`
- **Issues**: H1 (controller key mismatch — nested stats), L16 (inflated download count)
- **Downstream**: `get_ucc_exposure()`, dashboard

#### `pg_municipal_lien_service.py`
- **Purpose**: Phase 0 municipal lien detector from ORI evidence.
- **Write targets**: `municipal_lien_findings`
- **Issues**: H1 (controller key mismatch), L30 (broad hint token)
- **Downstream**: Dashboard property detail, equity calculations

#### `pg_trust_accounts.py`
- **Purpose**: Thin PG wrapper around TrustAccountsService.
- **Write targets**: Delegates to parent
- **Issues**: L25 (no `super().__init__()`)
- **Downstream**: Pipeline controller

#### `trust_accounts.py`
- **Purpose**: Trust account movement analysis from Clerk PDF reports.
- **Write targets**: `TrustAccount`, `TrustAccountSummary`
- **Issues**: H1 (controller key mismatch), M18 (schema bypasses Alembic), L26 (single transaction)
- **Downstream**: Dashboard surplus fund analysis

#### `scraper_storage.py`
- **Purpose**: Filesystem storage manager for scraper outputs.
- **Write targets**: Files under `data/Foreclosure/{case}/`
- **Issues**: L3 (`needs_refresh()` always returns True)
- **Downstream**: All scrapers, mortgage extraction, dashboard

#### `pav_cache.py`
- **Purpose**: Gzip-JSON cache for PAV API responses, 7-day TTL.
- **Write targets**: `data/cache/pav_api/*.json.gz`
- **Issues**: L1 (errors at debug level), L2 (corrupt files not cleaned)
- **Downstream**: ORI service, identifier recovery service

---

## Recommended Fix Priority

### Tier 1: Highest ROI (fix first)

| # | Finding | Effort | Impact |
|---|---------|--------|--------|
| H1 | Controller stats key mismatch (12 steps) | Small — one file, 12 line changes | Restores pipeline visibility for all steps |
| H4 | Partial mortgage blocks retries | Small — skip DB write when cache incomplete | Fixes permanent data gaps |
| H5 | `refresh_foreclosures` failure swallowed | Small — add degraded flag in 2 files | Prevents stale equity figures |

### Tier 2: Important Fixes

| # | Finding | Effort | Impact |
|---|---------|--------|--------|
| H2 | Job control finalization cascade | Small — wrap in try/except | Prevents stuck job runs |
| H3 | Criminal NULL constraint | Small — normalize NULLs before insert | Prevents duplicate accumulation |
| M7 | Stale survival rows | Small — add DELETE before UPSERT | Correct survival data |
| M8 | No sentinel for failed PAV searches | Small — insert marker event | Stops repeated API waste |
| M9 | Scrapling exceptions lost | Small — capture in result dict | Surface market failures |

### Tier 3: Operational Improvements

| # | Finding | Effort | Impact |
|---|---------|--------|--------|
| M1 | ORI saved counter inflation | Medium — refactor counting | Reduce unnecessary downstream work |
| M5 | Failed audit buckets masked | Small — add error indicator | Better operator visibility |
| M10 | Background steps no audit trail | Medium — add job tracking | Complete pipeline audit trail |
| M15 | Tampa enrichment error rate | Small — add threshold check | Catch enrichment failures |
| L1 | PAV cache debug-level errors | Trivial — change log level | Operator visibility |

### Tier 4: Nice to Have

All remaining LOW severity items. These are cosmetic, accounting, or edge-case improvements that don't affect data correctness.

---

*Generated by persistence audit across 9 parallel agents, 2026-03-10.*
