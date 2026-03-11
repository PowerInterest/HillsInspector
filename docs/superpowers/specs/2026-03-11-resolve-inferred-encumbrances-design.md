# Design: Resolve Inferred Encumbrances

## Goal

Replace placeholder `INFERRED-{case_number}` encumbrance rows with real ORI documents, or delete them when they're redundant with existing real encumbrances on the same strap.

## Problem

When ORI search finds zero documents for a folio, `_infer_from_judgment()` creates a synthetic encumbrance row from judgment data. These rows have no PDF, no ori_id, and limited data (just plaintiff name, case number, sometimes amount). 118 such rows exist (68 mortgage, 50 lien). However, 113/116 of the affected straps already have real ORI documents — the inferred rows are largely redundant placeholders.

## Architecture

New pipeline step `resolve_inferred_encumbrances()` in `pg_ori_service.py`, wired before extraction. Two-pass algorithm:

### Pass 1: Local Party Match (no API calls)

Single SQL query using PG's `entity_match_score()` function. For each inferred row, check if any real (non-inferred) encumbrance on the same strap has a party matching the inferred `party1` (the foreclosing plaintiff). If match score >= 0.60 → delete the inferred row.

Match targets: `party1` and `party2` on real encumbrances (the ORI-populated party fields).

### Pass 2: ORI Case Search (API calls for remaining)

For inferred rows that survive Pass 1 (expected: ~3-5):
1. Search ORI by case number via existing `_search_case_pav()`
2. If results found that aren't already in `ori_encumbrances` for this strap → save them via normal ORI pipeline
3. Delete the inferred row (real docs now exist, or the case search confirms nothing is recorded)

If the case search returns zero results, keep the inferred row — it's still the best data we have for survival analysis.

## Files to Modify

- `src/services/pg_ori_service.py`: Add `resolve_inferred_encumbrances()`
- `src/services/pg_pipeline_controller.py`: Wire as pipeline step before extraction
- `tests/test_pg_ori_service.py`: Tests for both passes

## Success Criteria

- Most of the 118 inferred rows are deleted (matched to existing real docs)
- Zero real encumbrances lost (only inferred rows are deleted)
- Remaining inferred rows (if any) genuinely have no real ORI docs
- No regressions in survival analysis
