# Encumbrance Algorithm Benchmark Results

Run date: 2026-02-20
Command:

```bash
uv run python scripts/benchmark_encumbrance_algorithms.py --sample-size 8
```

Artifacts:
- `logs/encumbrance_benchmark_20260220_195717.json`
- `logs/encumbrance_benchmark_20260220_195717.md`

## Ranking (from benchmark)
1. `chain_adjacent`
2. `chain_adjacent_clerk`
3. `chain_adjacent_clerk_legal_fallback`
4. `baseline_case_legal_party`

## Key Metrics

### `chain_adjacent`
- Avg instrument recall: `0.1488`
- LP found rate: `1.0`
- Judgment found rate: `1.0`
- Mortgage release rule rate: `0.1429`
- Avg API calls/case: `26.88`
- Avg runtime/case: `8.14s`
- Total truncations: `0`

### `baseline_case_legal_party`
- Avg instrument recall: `0.1256`
- LP found rate: `1.0`
- Judgment found rate: `1.0`
- Mortgage release rule rate: `0.25`
- Avg API calls/case: `20.12`
- Avg runtime/case: `31.78s`
- Total truncations: `33` (4 unresolved)

### `chain_adjacent_clerk` and fallback
- Same recall as `chain_adjacent` on this sample.
- Much higher cost and frequent truncation spikes in hard cases.

## Observations
- Case-seeded chain+adjacent search avoided the worst truncation blowups from
  broad party/legal searches.
- Clerk-party expansion was expensive and frequently added no document yield.
- No NOC documents were discovered in this 8-case sample, so NOC->permit
  linkage remained `N/A` for this run.

## Recommended Production Strategy
- Default: `chain_adjacent`.
- Conditional fallback: only run legal/party fallback when either:
  - LP/Judgment anchor is missing, or
  - document count is below a minimum threshold after chain-adjacent pass.
- Keep clerk-party expansion disabled by default; use only as targeted fallback.
