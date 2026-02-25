# Tampa Permit Accela Failure (2026-02-23)

This runbook documents the Tampa permit ingestion failure seen in the full pipeline run on **2026-02-22 to 2026-02-23**.

## Symptoms

- Controller step failure:
  - `Step failed: tampa_permits: Tampa Accela returned Error page after search submit ... Error.aspx?...`
- Earlier runs also showed:
  - `Tampa window capture missing export button ... page_url=.../Error.aspx?...`
- Effect:
  - Tampa permit ingest produced zero rows or hard-failed, reducing permit coverage.

## Root Cause

Two regressions were present in `src/services/TampaPermit.py`:

1. Date input corruption on submit
- `page.fill()` appended to prefilled date values in Accela's masked fields.
- Posted values became malformed, e.g.:
  - `txtGSStartDate=02/22/202501/24/2026`
  - `txtGSEndDate=02/22/202602/23/2026`
- Accela then redirected to `Error.aspx`.

2. Export button selector drift
- Code looked for the legacy selector path containing `CapView`.
- Current markup uses `dgvPermitList` path:
  - `ctl00_PlaceHolderMain_dgvPermitList_gdvPermitList_gdvPermitListtop4btnExport`
- Result: "missing export button" even when search succeeded.

3. Address normalization mismatch reduced downstream join coverage
- Many Tampa export addresses arrived as:
  - `911 S Fremont Ave T 33606`
  - `603 W Emma St Tampa FL 33603`
- Existing downstream joins compare the street segment before comma.
- Without canonical commas/city/state formatting, joins to foreclosure/HCPA address strings frequently missed.

4. Large-window download timeout occurred before split logic
- For very large windows (for example 120-day backfill), CSV download event could timeout before returning a file.
- Existing split logic was only reached **after** a successful download/csv parse, so the run failed instead of splitting.

## Fix Applied

File: `src/services/TampaPermit.py`

- Added `_set_accela_date_input(...)`:
  - Uses `Control+A` + typed replacement (not raw `fill`).
  - Verifies the final field value exactly matches expected date.
  - Falls back to JS value assignment + input/change events if needed.
- Added `_resolve_export_button(...)`:
  - Supports both legacy and current Accela export button IDs.
  - Includes generic selector fallbacks (`a[id*='btnExport']`).
- Expanded `normalize_address(...)`:
  - Canonicalizes common Accela variants like `... T 33606` and `... Tampa FL 33603`
    to `street, TAMPA, FL zip`.
  - Improves downstream street-level matching.
- Added timeout-aware split fallback in `sync_date_range(...)`:
  - If export download times out on a multi-day window, the window is halved and retried.
  - Prevents hard failures on large backfills.
- Updated iframe selector to support both `iframeExport` and `iframeexport`.
- Expanded module-level docstring with explicit architectural context.

## Verification Performed

1. Direct capture for failing window:
- `capture_window_export(2026-01-24, 2026-02-23)` now succeeds.
- Download completed with `row_count=4093` (no `Error.aspx`).

2. End-to-end sync smoke test:
- `sync_date_range(2026-02-20, 2026-02-23, keep_csv=False)` completed.
- Summary: `parsed_total=223`, `written_total=223`.

3. Tampa-only controller backfill:
- 30-day forced refresh completed with split windows and writes (`~4090` rows).
- 120-day forced refresh completed after timeout-split fallback:
  - `windows_processed=43`, `windows_split=21`,
  - `parsed_total=15066`, `written_total=15066`.

4. Linkage impact snapshot:
- Before normalization + broader backfill: near-zero/very low foreclosure matches.
- After fixes and 120-day backfill: foreclosure address matches increased (example run: `11` matched foreclosures).

5. Code checks:
- `uv run ruff check src/services/TampaPermit.py` passed.
- `python -m py_compile src/services/TampaPermit.py` passed.

## Operational Guidance

- If Tampa Accela changes markup again, first inspect:
  - Date field POST payload values.
  - Export button IDs in search results HTML.
- Do not treat a 7+ day Tampa window with zero rows as success (controller guardrail already enforces this).
