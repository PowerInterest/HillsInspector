# System Audit Fixes — 2026-03-08 Final Pass

Final three deferred issues from the multi-reviewer system audit (`issues_from_agent.md`).
All changes reviewed and approved by Claude Code, Codex, and the original auditing agent
before merge.

## Summary

| # | Issue | Severity | Files Changed | Impact |
|---|-------|----------|--------------|--------|
| 1 | Chain gap metric always zero | HIGH | `pg_title_chain_controller.py`, tests | `is_gap` and `gap_count` never triggered — chain completeness always reported as COMPLETE |
| 3 | Three divergent judgment loaders | HIGH | `pg_judgment_service.py`, `refresh_foreclosures.py`, `pg_foreclosure_service.py`, tests | Path 3 (refresh) overwrote Path 2's best-judgment selection; Path 1 was dead code |
| 7 | Zestimate column naming in UI | LOW | `property_search_results.html` | Vendor-specific label on blended estimated value column |

---

## Issue 1: Chain Gap Metric Broken

**Root Cause:** `_score_sales_links_sql` produces link statuses `ROOT`, `LINKED_EXACT`,
`LINKED_FUZZY`, `MISSING_PARTY`, and `CHAINED_BY_FOLIO`. But both `_build_chain_sql` and
`_build_summary_sql` checked for `link_status = 'GAP'` — a value the scoring query never
produces. This meant `is_gap` was always `false` and `gap_count` was always `0`, so
`chain_status` was always `'COMPLETE'` regardless of actual chain quality.

**Fix:**
- `_build_chain_sql` (line 876): Changed `(s.link_status = 'GAP') AS is_gap` to
  `(s.link_status IN ('MISSING_PARTY', 'CHAINED_BY_FOLIO')) AS is_gap`
- `_build_summary_sql` (line 907): Changed
  `COUNT(*) FILTER (WHERE e.link_status = 'GAP') AS gap_count` to
  `COUNT(*) FILTER (WHERE e.link_status IN ('MISSING_PARTY', 'CHAINED_BY_FOLIO')) AS gap_count`
- Summary `chain_status` derivation (`WHEN coalesce(ss.gap_count, 0) = 0 THEN 'COMPLETE'`
  / `ELSE 'BROKEN'`) now produces correct values.

**Tests:** `tests/test_title_chain_gap_metric.py` — 8 regression tests:
- gap-status classification: `MISSING_PARTY` / `CHAINED_BY_FOLIO` are gaps; `ROOT`, `LINKED_EXACT`, and `LINKED_FUZZY` are not
- chain summary classification: `MISSING_FOLIO`, `NO_SALES`, `COMPLETE`, and `BROKEN`
- SQL integration: generated chain/summary SQL uses the same shared gap predicate and no longer references phantom `'GAP'`

**Data Repair:** Code fix alone does not heal already-persisted rows in
`foreclosure_title_chain` and `foreclosure_title_summary`. A targeted title-chain rebuild
is required post-deploy:

```bash
uv run Controller.py \
  --skip-hcpa --skip-clerk-bulk --skip-clerk-criminal --skip-clerk-civil-alpha \
  --skip-nal --skip-flr --skip-sunbiz-entity --skip-county-permits \
  --skip-tampa-permits --skip-single-pin-permits --skip-foreclosure-refresh \
  --skip-trust-accounts --skip-title-breaks --skip-market-data \
  --skip-final-refresh \
  --skip-auction-scrape --skip-judgment-extract --skip-identifier-recovery \
  --skip-ori-search --skip-municipal-liens --skip-mortgage-extract \
  --skip-survival --skip-encumbrance-audit --skip-encumbrance-recovery
```

The title-chain step is self-healing: it drops and rebuilds its outputs per foreclosure
(`pg_title_chain_controller.py:282`), so no manual data deletion is needed beforehand.

**Repair run result (active scope):**
- 127 foreclosures rebuilt in 177 seconds
- 43 complete chains
- 75 broken chains
- 133 total gaps detected
- 9 missing-folio cases

---

## Issue 3: Three Divergent Judgment Loaders

**Root Cause:** Three separate code paths wrote judgment data to `foreclosures`:

| Path | Location | Status |
|------|----------|--------|
| Path 1 | `PgForeclosureService.update_judgment_data` | Dead code (test-only) |
| Path 2 | `PgJudgmentService._load_judgment_data_to_pg` | Primary production path with `select_best_judgment` |
| Path 3 | `refresh_foreclosures._load_judgment_data` | Ran AFTER Path 2, bypassed best-judgment selection, skipped `step_pdf_downloaded` |

Path 3 was dangerous because `final_refresh` (step 27) executes after `judgment_extract`
(step 16), so its inferior logic could silently overwrite Path 2's correct best-judgment
data.

**Fix:**
1. **Extracted `persist_judgment()`** as a public `@staticmethod` on `PgJudgmentService` —
   the single canonical persistence path for judgment data into PG. Handles `judgment_data`
   JSONB, `pdf_path`, `final_judgment_amount`, `step_pdf_downloaded`, and
   `step_judgment_extracted` with COALESCE preserve-first semantics.

2. **Promoted `_select_best_judgment` → `select_best_judgment`** (public) so both callers
   can use the same ranking logic.

3. **Rewrote `refresh_foreclosures._load_judgment_data`** to call
   `PgJudgmentService.select_best_judgment()` + `PgJudgmentService.persist_judgment()`.
   Groups JSONs by case directory, filters to `final_judgment_*` files, derives PDF path
   from the chosen JSON's stem.

4. **Deleted `PgForeclosureService.update_judgment_data`** (dead code).

5. **Gated `updated += 1`** on `persist_judgment()` return value in both
   `_load_judgment_data_to_pg` and `_load_judgment_data` (caught by Codex review).

**Design decision — active-only vs archived-inclusive matching:**
`PgJudgmentService` filters `WHERE archived_at IS NULL` (extraction targets live rows only).
`refresh_foreclosures` includes archived rows with `ORDER BY ... archived_at NULLS FIRST`
(donor-row behavior for historical reuse). This difference is intentional and was confirmed
by all three reviewers.

**Tests:**
- `tests/test_pg_foreclosure_service.py` — 5 tests: step flag setting, COALESCE semantics,
  amount extraction, no-match return value, loader-level regression (gates count on return)
- `tests/test_pg_judgment_service.py` — updated to use public `select_best_judgment`

---

## Issue 7: Zestimate Column Naming

**Root Cause:** The search results table displayed "Zestimate" and "Rental Zest." as column
headers for blended estimated values that may come from Zillow, Redfin, HomeHarvest, or
Realtor.com. The vendor-specific label was misleading.

**Fix:** Changed UI labels in `property_search_results.html`:
- `<th>Zestimate</th>` → `<th>Est. Value</th>`
- `<th>Rental Zest.</th>` → `<th>Est. Rent</th>`

No database changes. Views showing Zillow-specific data (e.g., `zillow_json["zestimate"]`)
retain the "Zestimate" label where appropriate.

---

## Template Formatting

All three Jinja templates were reformatted with `djlint --reformat` per the project's
new djlint configuration (`pyproject.toml`, profile `jinja`):
- `app/web/templates/base.html`
- `app/web/templates/property.html`
- `app/web/templates/partials/property_search_results.html`
