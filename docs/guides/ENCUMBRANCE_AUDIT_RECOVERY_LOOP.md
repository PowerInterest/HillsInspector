# Encumbrance Audit Recovery Loop

## Purpose

The encumbrance audit is not only an operator report. In the PG-first pipeline
it now acts as a targeted recovery planner:

1. Run the normal Phase B encumbrance path.
2. Audit the resulting coverage and LP-to-judgment deltas.
3. Feed selected audit buckets back into the existing enrichment writers.
4. Re-run survival on the foreclosures whose encumbrance set changed.
5. Publish the refreshed foreclosure hub view.

This avoids two bad patterns:

- writing heuristic audit rows back as if they were facts
- rerunning all ORI/mortgage/survival work for every active foreclosure

## Pipeline Placement

The controller order is:

1. `ori_search`
2. `mortgage_extract`
3. `survival_analysis`
4. `encumbrance_audit`
5. `encumbrance_recovery`
6. `final_refresh`

The audit must run after survival because several buckets depend on
`ori_encumbrances.survival_status`. The recovery loop must run before
`final_refresh` so the foreclosure hub reflects any newly discovered
encumbrance facts in the same controller pass.

## Bucket Routing

`src/services/audit/encumbrance_recovery.py` maps the audit report to the
current pipeline writers.

### Automatic recovery buckets

- `lp_missing`
  - routed to `PgOriService.run_lis_pendens_backfill()`
- `construction_lien_risk`
  - routed to both targeted ORI rediscovery and `run_recent_permit_noc_backfill()`
- `foreclosing_lien_missing`
- `plaintiff_chain_gap`
- `cc_lien_gap`
- `sat_parent_gap`
- `lifecycle_base_gap`
- `judgment_joined_party_gap`
- `judgment_instrument_gap`
- `lp_to_judgment_plaintiff_change`
- `lp_to_judgment_party_expansion`
  - routed to `PgOriService.run_targeted_recovery()`

After ORI writes succeed, the recovery loop narrows the follow-up work:

- `PgMortgageExtractionService.run(straps=[...])`
- `PgSurvivalService.run(foreclosure_ids=[...], force_reanalysis=True)`

## Review-only buckets

These buckets currently remain analyst-facing only:

- `superpriority_non_ori_risk`
- `historical_window_gap`
- `lp_to_judgment_property_change`
- `long_case_interim_risk`

They are useful signals, but they are not yet strong enough to map to a safe
automatic writer.

## Persistence Strategy

No new audit tables are required for this loop.

- Current issues remain derivable from live `foreclosures`, title-chain data,
  clerk data, and `ori_encumbrances`.
- The web audit UI can query the current state directly.
- The controller uses in-memory handoff between `encumbrance_audit` and
  `encumbrance_recovery` inside one run.

If the product later needs workflow state such as `open`, `ignored`,
`resolved`, analyst notes, or first/last seen timestamps, that is the point
where dedicated audit persistence becomes justified.
