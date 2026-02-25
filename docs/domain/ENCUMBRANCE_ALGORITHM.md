# Consolidation Document

This document is a consolidated reference composed of the following historical files:
- `ENCUMBRANCE_ALGORITHM_RESULTS.md`
- `ENCUMBRANCE_ALGORITHM_TESTPLAN.md`

---



## Source: ENCUMBRANCE_ALGORITHM_RESULTS.md

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


## Source: ENCUMBRANCE_ALGORITHM_TESTPLAN.md

# Encumbrance Algorithm Test Plan

## Objective
Determine the most complete and efficient encumbrance discovery algorithm for
Hillsborough foreclosure properties using current PG data plus ORI API calls.

The algorithm must satisfy these business rules:
- If foreclosure exists, find lis pendens (`LP`) and final judgment (`JUD/FNLJ`).
- If mortgage exists, find downstream lifecycle docs (satisfaction/release/assignment).
- If notice of commencement (`NOC`) exists, link it to permit evidence in PG.

## Scope
- In scope:
  - ORI discovery strategy comparison.
  - PG-only enrichment joins (sales chain, clerk parties, permits).
  - Per-property rule coverage and API-cost measurement.
- Out of scope:
  - Schema migrations.
  - Web endpoint wiring.
  - PDF OCR extraction.

## Candidate Strategies
1. `baseline_case_legal_party`
- Seed by case number.
- Add legal-term search and party search (plaintiff/defendant/owner).
- Chase references for discovered lien/mortgage instruments.

2. `chain_adjacent`
- Seed by case number.
- Use `hcpa_allsales` deed chain (PG) and query deed instrument + offsets.
- Chase references for discovered lien/mortgage instruments.

3. `chain_adjacent_clerk`
- Strategy 2 plus defendant-name search from `clerk_civil_parties`
  (date-bounded).

4. `chain_adjacent_clerk_legal_fallback`
- Strategy 3 plus targeted legal fallback only when LP or mortgage coverage is
  insufficient.

## Test Corpus
- Source: `foreclosures` where `archived_at IS NULL` and strap is valid.
- Stratification buckets by sales-chain depth (`hcpa_allsales` rows):
  - Low complexity (0-2 transfers)
  - Medium complexity (3-6 transfers)
  - High complexity (7+ transfers)
- Default benchmark size: 12 properties (balanced by bucket).

## Gold Truth / Reference Set
For each property, build a per-case reference universe:
- Union of discovered instruments across all tested strategies.
- Union with existing `ori_encumbrances` rows for the same strap.

This creates a practical completeness baseline without requiring manual labeling
for every case.

## Metrics
- Completeness:
  - `instrument_recall = strategy_instruments / reference_instruments`
  - `mortgage_lifecycle_recall = matched_mortgage_release_links / reference_links`
- Rule coverage:
  - `lp_found_rate`
  - `judgment_found_rate`
  - `mortgage_release_rule_rate`
  - `noc_permit_link_rate`
- Efficiency:
  - `avg_api_calls_per_case`
  - `avg_runtime_seconds_per_case`
  - `truncated_response_rate`
  - `error_rate`

## Pass/Selection Criteria
- Hard minimums:
  - LP found in 95%+ of sampled foreclosure cases.
  - Mortgage lifecycle rule satisfied in 80%+ of sampled cases with mortgages.
  - NOC→permit linkage in 80%+ of sampled cases that contain NOC docs.
- Ranking:
  1. Highest instrument recall.
  2. Highest mortgage lifecycle rule rate.
  3. Lowest API calls per case.
  4. Lowest runtime per case.

## Execution Plan
1. Run benchmark script:
```bash
uv run python scripts/benchmark_encumbrance_algorithms.py --sample-size 12
```
2. Review generated JSON + markdown summary in `logs/`.
3. Promote winner to default ORI strategy design.
4. Keep fallback-only paths for edge cases with truncation/noisy party matches.

## Expected Decision Pattern
- Preferred default is expected to be `chain_adjacent` family.
- `baseline_case_legal_party` likely has lower precision and higher truncation.
- `chain_adjacent_clerk_legal_fallback` should win on completeness if added API
  cost remains within acceptable bounds.
