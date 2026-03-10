# Massive Audit Resolution - 2026-03-10

This document records the code changes made in response to `docs/MASSIVE_AUDIT.md`.

Scope rules for this pass:

- High and Medium findings were re-verified against the live tree before any code change.
- Confirmed correctness and observability gaps were fixed in code and covered by tests.
- Low findings were reviewed for follow-up value, but not implemented in this pass.
- No issue was "fixed" by suppressing errors. Where a step can partially succeed, the
  pipeline now records degraded/error state explicitly instead of silently returning success.

## Summary

### Fixed in code

- High: `H1`, `H2`, `H3`, `H4`, `H5`
- Medium: `M1`, `M3`, `M4`, `M5`, `M6`, `M7`, `M8`, `M9`, `M10`, `M11`, `M12`, `M13`, `M15`, `M16`, `M17`

### Reviewed and intentionally not changed

- `M2`: not a bug in current design. Parcel-scoped `ori_encumbrances` rows for the same instrument are intentional. See [Per-Foreclosure Survival Persistence](docs/domain/PER_FORECLOSURE_SURVIVAL.md).
- `M14`: different "needs market data" predicates are intentional because scrapling and browser scraping have different completeness contracts.
- `M18`: schema governance issue, not a runtime correctness bug.

## High Findings

| Issue | Status | Files | Resolution |
|---|---|---|---|
| H1 Controller stats key mismatch | Fixed | `src/services/pg_pipeline_controller.py` | Rewired controller counters to the actual nested keys returned by each service and refresh script. Added helper path readers and regression tests. |
| H2 Job control finalization cascade | Fixed | `src/services/pg_job_control_service.py` | Failure finalization now has its own guarded path, rollback/unlock failures are logged, and failed runs no longer depend on a clean finalize write to return a failed status. |
| H3 Criminal NULL conflict collision | Fixed | `src/services/pg_loader_clerk.py` | `count_number` and `disposition_code` are normalized before insert so the DB conflict target matches in-memory dedup behavior. |
| H4 Partial mortgage extraction blocks retries | Fixed | `src/services/pg_mortgage_extraction_service.py` | Partial mortgage extracts now skip DB persistence, preserving `mortgage_data IS NULL` so retry logic still sees the row. |
| H5 `refresh_foreclosures` failure swallowed | Fixed | `src/services/market_data_worker.py`, `src/services/pg_market_data_scrapling.py` | Both market workers now emit degraded output with explicit refresh error details instead of only logging a warning. |

## Medium Findings

| Issue | Status | Files | Resolution |
|---|---|---|---|
| M1 ORI `saved` counter inflated | Fixed | `src/services/pg_ori_service.py` | No-op UPDATE/UPSERT paths no longer count as saved rows. |
| M2 Multi-parcel instrument key mismatch | Rejected | none | Re-verified as parcel-scoped intentional behavior, not duplicate corruption. |
| M3 LP audit/recovery scope mismatch | Fixed | `src/services/audit/pg_audit_encumbrance.py`, `src/tools/pg_encumbrance_audit.py` | LP bucket now matches by strap and case number, consistent with recovery targeting. |
| M4 Recovery false positives from `saved > 0` | Fixed via M1 | `src/services/pg_ori_service.py` | Once no-op saves stopped incrementing, `_changed_target_rows()` stopped seeing false changes from rediscovery-only passes. |
| M5 Failed audit buckets reported as `count=0` | Fixed | `src/services/audit/pg_audit_encumbrance.py`, `src/tools/pg_encumbrance_audit.py`, `src/services/audit/web_audit_service.py` | Added `error_count` and propagated it into web summaries. |
| M6 Full-table SQL for single-property snapshot | Fixed | `src/services/audit/web_audit_service.py`, `src/services/audit/pg_audit_encumbrance.py` | Bucket handlers now accept `foreclosure_ids`, and the property snapshot calls scoped SQL instead of full-table scans. |
| M7 Stale survival rows on re-analysis | Fixed | `src/services/pg_survival_service.py` | Existing per-foreclosure survival rows are deleted before current results are persisted. |
| M8 No sentinel for empty title-break search | Fixed | `src/services/pg_title_break_service.py` | Empty ORI deed searches now insert an `ORI_DEED_SEARCH` / `SEARCH_NO_RESULT` sentinel row. |
| M9 Scrapling exceptions lost from step details | Fixed | `src/services/pg_pipeline_controller.py` | Scrapling pre-worker failures are now stored in step details instead of disappearing into logs only. |
| M10 Background bulk steps lack audit trail | Fixed | `src/services/bulk_step_worker.py` | Background bulk steps now run through `PgJobControlService` and write `pipeline_job_runs` rows. |
| M11 `_payload_status` does not handle `StepResult` | Fixed | `src/services/pg_job_control_service.py` | Job control now accepts and serializes `StepResult` payloads. |
| M12 Enrichment state query failure re-scrapes everything | Fixed | `src/services/pg_market_data_scrapling.py`, `src/services/market_data_service.py` | Query failures now assume sources are complete for the current run, record degraded failure counts, and avoid fan-out re-scraping against rate-limited sites. |
| M13 Captcha 200s never trigger backoff | Fixed | `src/services/pg_market_data_scrapling.py` | Blocked HTML now counts toward consecutive failures, so delay backoff can engage. |
| M14 Two divergent market-data completeness queries | Rejected | none | Browser worker and scrapling service intentionally measure different completion surfaces. |
| M15 Tampa enrichment error rate unchecked | Fixed | `src/services/pg_pipeline_controller.py` | Tampa permit step now reports degraded status when detail enrichment errors occur against rows that were otherwise fetched/synced. |
| M16 Temple Terrace drops record on detail failure | Fixed | `src/services/TempleTerracePermit.py` | Search-row data is now persisted even when detail-page fetch fails, with explicit detail error metadata. |
| M17 Municipal error understates writes | Fixed | `src/services/pg_permit_single_pin_service.py` | Partial per-pin write stats now survive municipal failures instead of collapsing to zero. |
| M18 Trust Accounts bypasses Alembic | Rejected | none | Valid policy concern, but not a runtime data-loss bug in this pass. |

## Tests and Verification

Commands run after the fix set:

```bash
uv run pytest tests/test_pg_job_control_service.py \
  tests/test_bulk_step_worker.py \
  tests/test_market_data_worker.py \
  tests/test_pg_market_data_scrapling.py \
  tests/test_pg_mortgage_extraction_service.py \
  tests/test_pg_loader_clerk.py \
  tests/test_pg_survival_service.py \
  tests/test_pg_title_break_service.py \
  tests/test_pg_permit_single_pin_service.py \
  tests/test_permit_pipeline_guardrails.py \
  tests/test_temple_terrace_permit_service.py \
  tests/test_pg_pipeline_controller_metrics.py \
  tests/test_pg_ori_service.py \
  tests/test_encumbrance_audit_service.py \
  tests/test_run_scheduled_job.py
```

```text
75 passed
145 passed
```

```bash
uv run ruff check .
uv run ty check
```

Both repo-wide static checks passed.

## Low Finding Feedback

These were reviewed for follow-up value. "Worth fixing" means the issue has a good signal-to-effort ratio and is appropriate to hand to another coding pass.

### Worth fixing

| Issue | Recommendation |
|---|---|
| L1 `pav_cache_put` debug-only write failures | Raise to warning; operationally useful and trivial. |
| L2 Corrupt PAV cache files persist | Delete corrupt cache entries on read failure so warnings do not repeat for days. |
| L6 PAV API failures look like zero results | Add explicit failure counters or error metadata; useful for ORI diagnostics. |
| L7 Auction scrape failures missing from return dict | Add `dates_failed` so scheduler/controller summaries can surface bad days. |
| L9 Auction results mismatch rate has no threshold warning | Add a warning threshold around `not_found_in_pg`. |
| L10 Clerk row-count inflation | Make inserted counts reflect real DB inserts where practical. |
| L12 Clerk alpha NULL/empty dedup mismatch | Same class as H3; worth aligning. |
| L15 NAL enrichment failure marks full step failed | Change to degraded when parcel load succeeded but millage backfill failed. |
| L17 Foreclosure read methods mask DB errors | Stop returning empty sets on DB failure in web/service reads. |
| L20 Mortgage `_save_to_pg` rowcount unchecked | Prevent phantom success counts. |
| L21 Judgment/mortgage services do not return error counts | Worth treating as Medium; full extraction failure should not collapse to `noop`. |
| L22 Final judgment merge truthiness bug | Real data-correctness edge case; worth fixing. |
| L26 Trust accounts single transaction for all reports | Chunk transaction scope per report. |
| L27 Market `_f()` and `_i()` discard zeros | Fix to preserve valid zero values. |
| L30 Municipal utility hint token too broad | Tighten false-positive matching. |
| L31 Single-pin Tampa upsert missing `is_violation` | Real data-quality issue. |
| L32 Scrapling error count understates severity | Count affected properties or site-level blast radius more accurately. |

### Defer or leave alone

| Issue | Recommendation |
|---|---|
| L3 `needs_refresh()` always returns `True` | Documented design stub; only clean up dead caller branches if they keep confusing people. |
| L4 ORI `force_satisfaction_relink` dead code | Cleanup candidate, but not urgent unless you are already touching recovery flow. |
| L5 ORI `saved` double-counts inferred encumbrances | Counter clarity improvement only. |
| L8 Auction result pagination cap | Real but low frequency in this county. |
| L11 Clerk events `DO NOTHING` drops corrected descriptions | Real, but needs a careful correctness decision before changing conflict behavior. |
| L13 Empty clerk file blocks future download | Worth fixing eventually, but less urgent than extraction/classification defects. |
| L14 Triple clerk listing fetch | Cheap optimization, not correctness-critical. |
| L18 Dead code `update_pipeline_step()` | Cleanup only. |
| L19 Identifier recovery ORI bootstrap failure | More cleanup than bug; bootstrap appears unnecessary. |
| L23 `_prior_survival_status` never read | Cleanup only. |
| L24 Legacy parcel-scoped `ori_encumbrances.survival_status` | Known limitation; bigger design topic than a quick fix. |
| L25 `pg_trust_accounts.py` missing `super().__init__()` | Defensive future-proofing, not a present bug. |
| L28 Background dispatcher returns `skipped=True` | Mostly semantics; worth revisiting only if summary UX becomes a problem. |
| L29 Vision semaphore thread safety | Non-finding in current single-threaded architecture. |

## Notes

- `M2` was specifically checked because it looked plausible at first glance. The current ORI persistence model intentionally stores parcel-scoped encumbrance rows even when the same instrument touches multiple parcels. Changing that would break survival persistence semantics.
- The most important theme in this pass was observability, not just exception avoidance: degraded states, bucket query errors, background audit trail coverage, and partial-write accounting now survive into pipeline summaries instead of vanishing into logs.
