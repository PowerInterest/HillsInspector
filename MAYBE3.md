Partially.

1. `mark_status_retriable_error()` is a good change, but the claim “prevents permanent blocking” is overstated.
`mark_status_retriable_error` increments `retry_count` (`src/db/operations.py:3269`), and case selection still requires `retry_count < max_retries` for all non-completed cases (`src/db/operations.py:2721`), so repeated transient errors can still quarantine a case.

2. Timeout burst detection is a good, safe improvement.
The cooldown/abort logic in backfill/recovery is pragmatic and reduces wasted time when ORI is unhealthy (`src/orchestrator.py:2309`, `src/orchestrator.py:2652`).

3. `reset_pipeline_status()` is useful, but limited.
It resets `pipeline_status` only (`src/db/operations.py:3299`) and does not reset `retry_count`, so it will not revive cases already over retry limits.

4. These changes support `#5` well, but only partially support `#3`.
Unconditional ORI-complete paths still exist (`src/orchestrator.py:1081`, `src/orchestrator.py:1042`, `src/orchestrator.py:1047`).
