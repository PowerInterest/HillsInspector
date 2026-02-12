# Fix ORI Ingestion & Survival Stalling — Detailed Execution Plan

## Goal
Restore pipeline throughput and data completeness to meet success thresholds in `AGENTS.md`:
- Chain of title: 80%+ of foreclosures with judgments
- Encumbrances: 80%+
- Survival analysis: 80%+

This plan addresses the 5 unresolved risks directly and in a safe rollout order.

## Scope
- `src/db/operations.py`
- `src/orchestrator.py`
- One-time DB repair runbook (SQL)

No broad schema redesign. No hidden auto-repair behavior without logging.

---

## Issue 1: Permanent non-critical blockers can loop forever

### Problem
If non-critical permanent conditions (no coordinates, no address, no folio, etc.) only “log + return,” cases stay `processing` with unset steps forever and are retried every run.

### Plan
1. Define explicit permanent outcomes for non-critical steps:
   - Mark the step complete for that case (`mark_status_step_complete(..., step_number)`), with a clear `last_error`/reason note.
   - Mark matching auction flag complete (`mark_step_complete(case, needs_...)`) so backfill logic stays consistent.
2. Reserve “do nothing and return” only for genuinely deferred dependencies that will be satisfied later in the same run.
3. Ensure this policy is consistent across:
   - `_run_fema_checker`
   - `_run_tax_scraper`
   - `_run_hcpa_gis`
   - `_run_permit_scraper` (where applicable)
   - `_run_ori_ingestion_v2` no-folio branch

### Acceptance criteria
- Cases with permanent non-critical blockers stop reappearing indefinitely in `processing`.
- Step timestamps reflect terminal outcomes instead of hanging unset forever.

---

## Issue 2: Survival guard can be fooled by incorrect ORI completion semantics

### Problem
Guarding survival only by `step_ori_ingested` is unsafe if step 5 can be backfilled from folio-global evidence or stale flags.

### Plan
1. Tighten Step 5 backfill semantics in `backfill_status_steps()`:
   - Do not mark ORI complete solely because folio exists in `chain_of_title`.
   - Require case-specific evidence (for example: case-linked ORI activity, or trustworthy case-scoped completion marker).
2. Remove/limit any “needs flag implies step timestamp” backfill for Step 5 and Step 6 if that can create false positives.
3. In `_run_ori_ingestion`, do not early-return on `step_ori_ingested` alone:
   - Require expected downstream artifacts to exist for this folio context before skipping.
4. In `_run_survival_analysis`, preflight:
   - If ORI is not truly ready for this case context, skip survival without marking it complete.

### Acceptance criteria
- No case reaches survival with empty ORI prerequisites due to false step-5 completion.
- ORI and survival step timestamps are causally correct.

---

## Issue 3: `mark_status_step_failed()` must not corrupt pipeline state

### Problem
A step-level failure method can accidentally overwrite critical failure state or cause state regressions.

### Plan
1. Add `mark_status_step_failed(case, error, step_number)` with strict behavior:
   - Updates only `last_error`, `error_step`, `updated_at`.
   - Does **not** increment `retry_count`.
   - Does **not** change `pipeline_status` if already `failed`, `completed`, or `skipped`.
2. Use this only for transient non-critical step errors.
3. Keep `mark_status_failed()` only for critical failures (ORI/survival/judgment extraction).

### Acceptance criteria
- Non-critical transient errors are visible but do not poison global retries.
- Critical failures remain authoritative and are not downgraded.

---

## Issue 4: Disabled-step config must be validated and explicit

### Problem
Hardcoded disabled steps are brittle; environment-driven configuration can silently fail on typos.

### Plan
1. Read `HILLS_DISABLED_STEPS` from env (default includes `step_market_fetched` while Zillow is intentionally disabled).
2. Validate names against known step columns.
3. Log both:
   - Active disabled steps
   - Unknown/ignored configured step names
4. Apply disabled-step filtering only in `_get_applicable_steps()` (completion calculation), not in raw step tracking.

### Acceptance criteria
- Auctions can become `completed` when intentionally disabled steps are the only missing items.
- Misconfigured env values are visible in logs and not silently applied.

---

## Issue 5: Existing poisoned retry state needs controlled recovery

### Problem
Even after logic fixes, previously poisoned rows (especially non-critical failures with high retry_count) may stay excluded from practical reruns.

### Plan
1. Add one-time, operator-run recovery SQL (not implicit auto-run on every update):
   - Target date range only.
   - Target non-critical error steps only.
   - Reset `retry_count` and move cases back to `pending`/`processing` as appropriate.
2. Keep critical-failure rows untouched unless explicitly selected by operator.
3. Capture counts before/after reset in logs for auditability.

### Recommended recovery query pattern
- Reset only non-critical failed/processing rows in-range where retry is saturated and `error_step` is non-critical.
- Do not blanket-reset all failed cases.

### Acceptance criteria
- Stuck non-critical rows re-enter processing.
- Critical true failures are preserved.

---

## Implementation Order (Strict)

1. Fix resume query logic for `processing` vs `failed` retry gating.
2. Implement `mark_status_step_failed()` with state safeguards.
3. Update non-critical step wrappers to use terminal/permanent vs transient handling consistently.
4. Implement disabled-step env parsing + validation.
5. Tighten Step 5/6 backfill semantics and ORI/survival preflight logic.
6. Run one-time targeted recovery SQL.
7. Run bounded rerun (`--start-step 5`) on affected date range.
8. Run full threshold verification.

---

## Verification Checklist

## Operational
1. Resume stats: `to_process` aligns with visible in-scope `processing` + retry-eligible `failed`.
2. Non-critical step errors do not increase global retry count.
3. Previously stuck cases flow through ORI and survival.
4. Cases can reach `completed` when disabled steps are the only missing steps.

## Thresholds (required)
1. Final Judgment PDFs: 90%+
2. Extracted judgment data: 90%+
3. Chain of title: 80%+
4. Encumbrances: 80%+
5. Survival status populated: 80%+

If any threshold fails: diagnose -> fix root cause -> rerun affected steps -> re-check.

---

## Rollback / Safety

1. Keep changes isolated to `operations.py` and orchestrator step wrappers.
2. Apply fixes behind clear logging so behavior shifts are observable immediately.
3. If regressions appear, revert step-level failure routing first, keep resume-query fix.
