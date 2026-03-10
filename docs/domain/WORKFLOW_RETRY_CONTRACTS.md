# Workflow Retry Contracts

This document records the persistence and retry rules for the three controller
steps that were producing work without reliably improving downstream data
quality in the March 10, 2026 audit pass.

## Identifier Recovery

`identifier_recovery` now uses `foreclosures.step_identifier_recovery` as a
cooldown marker.

- Scope includes only active judged foreclosures with missing `strap` or
  `folio` whose recovery marker is null or older than 14 days.
- The service stamps `step_identifier_recovery = now()` only after a row reaches
  a decision point:
  - identifiers persisted
  - unresolved/no-match
  - ambiguous
- Per-row exceptions are not stamped. Those rows stay in scope so controller
  reruns surface the failure instead of silently suppressing it for 14 days.
- Matching remains conservative:
  - `UNIT NO` text is ignored as condo-unit evidence when a platted lot was
    already parsed
  - address fallback normalizes punctuation/spacing and strips directionals plus
    street suffixes before applying a legal-description cross-check

## ORI Search

`ori_search` is only considered persisted when the run changes downstream state.

- `step_ori_searched` is written only when the target has parcel identity and
  the run persisted at least one encumbrance, inferred encumbrance, or
  satisfaction/modification linkage.
- Zero-persistence runs leave `step_ori_searched` null so the case can re-enter
  scope on the next pass.
- Case-only discoveries that cannot be tied to a parcel are staged to disk, but
  staged-only targets are reported as `degraded`, not `noop`.
- Savepoint rollbacks inside `_save_documents()` are counted as `save_skips` and
  also force a `degraded` controller status.
- Downstream encumbrance counts treat missing survival status as `'UNKNOWN'`
  instead of filtering those rows out via PostgreSQL `NULL NOT IN (...)`
  semantics.

## Title Breaks

`title_breaks` retries only broken or gap-bearing chains and uses expiring
sentinels for true no-result searches.

- `_find_targets()` excludes `chain_status = 'COMPLETE'` rows whose
  `gap_count = 0`.
- `SEARCH_NO_RESULT` sentinels block retries for 14 days, then allow a new deed
  search attempt.
- When a retry again finds no deeds, the service writes a fresh sentinel so the
  cooldown advances instead of re-querying PAV on every run after the first TTL
  expiry.
- Any run that writes sentinels is reported as `degraded` because those
  foreclosures remain broken even though the no-result state was persisted.

## Verification

The regression coverage for these contracts lives in:

- `tests/test_pg_foreclosure_identifier_recovery_service.py`
- `tests/test_legal_parser.py`
- `tests/test_pg_ori_service.py`
- `tests/test_pg_pipeline_controller_degraded.py`
- `tests/test_pg_title_break_service.py`
- `tests/test_pg_pipeline_controller_audit_recovery.py`
- `tests/test_foreclosure_identifier_repair_sql.py`
- `tests/test_refresh_foreclosures_reuse.py`
