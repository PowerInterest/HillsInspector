# Claude Web Audit Instructions

## Goal

Implement a read-only web UI for the encumbrance audit so users can see what is
still wrong with a foreclosure, both:

1. on the individual property page, and
2. in a global audit inbox page for operators.

This is a web-app pass only. Do not implement persistence in PostgreSQL yet.


## Hard Constraints

- Do **not** create new PostgreSQL tables, columns, indexes, views, or materialized views.
- Do **not** create an Alembic migration.
- Do **not** wire any controller/pipeline writeback.
- Do **not** persist audit runs or audit hits anywhere.
- If you think schema changes are necessary, stop and leave a clear note instead of implementing them.

The user has explicitly said that PG schema changes require direct approval first.


## Current Backend State

The audit implementation already exists and is read-only:

- [src/services/audit/pg_audit_encumbrance.py](/mnt/c/code/HillsInspector/src/services/audit/pg_audit_encumbrance.py)
- [src/services/audit/encumbrance_audit_signals.py](/mnt/c/code/HillsInspector/src/services/audit/encumbrance_audit_signals.py)
- Compatibility CLI:
  - [src/tools/pg_encumbrance_audit.py](/mnt/c/code/HillsInspector/src/tools/pg_encumbrance_audit.py)

The audit now has materially better live signal quality after cleanup:

- `sat_parent_gap`: `41`
- `judgment_joined_party_gap`: `9`
- `lp_to_judgment_party_expansion`: `5`
- `lp_to_judgment_property_change`: `66`

Latest live snapshot:

- [logs/encumbrance_audit_20260304_125225.json](/mnt/c/code/HillsInspector/logs/encumbrance_audit_20260304_125225.json)


## Existing Web Seams

Use the existing property-page HTMX pattern and current router structure.

Relevant files:

- [app/web/routers/properties.py](/mnt/c/code/HillsInspector/app/web/routers/properties.py)
- [app/web/templates/property.html](/mnt/c/code/HillsInspector/app/web/templates/property.html)
- [app/web/templates/base.html](/mnt/c/code/HillsInspector/app/web/templates/base.html)
- [app/web/routers/review.py](/mnt/c/code/HillsInspector/app/web/routers/review.py)
- [app/web/main.py](/mnt/c/code/HillsInspector/app/web/main.py)

The property page already lazy-loads tab partials using `hx-get="/property/{folio}/..."`.
Follow that exact pattern for the audit tab.


## What To Build

### 1. Property-Level Audit Surface

Add a current-issues summary to the property page.

Requirements:

- Add an `Audit` tab to [property.html](/mnt/c/code/HillsInspector/app/web/templates/property.html).
- The tab should lazy-load via HTMX, same pattern as `judgment`, `chain`, `liens`, etc.
- Add a new route in [properties.py](/mnt/c/code/HillsInspector/app/web/routers/properties.py):
  - `GET /property/{folio}/audit`
- Render a new partial:
  - `app/web/templates/partials/audit.html`

Also add a small server-rendered audit summary card or banner on the default `Basic`
tab so the user does not have to click into `Audit` to know there are still issues.

That summary should show:

- total open audit issues for this foreclosure,
- number of affected families,
- top 2-3 bucket labels,
- a clear CTA to open the `Audit` tab.

If there are no current issues, show a quiet success state instead of an empty box.


### 2. Global Audit Inbox Page

Add a global operator page for open encumbrance-audit issues.

Preferred route:

- `GET /review/encumbrance-audit`

Put it in [app/web/routers/review.py](/mnt/c/code/HillsInspector/app/web/routers/review.py)
unless there is a clearly better fit. Do not create a whole new architecture for this.

Add a new template, for example:

- `app/web/templates/review/encumbrance_audit.html`

Add a nav link in [base.html](/mnt/c/code/HillsInspector/app/web/templates/base.html)
so the page is discoverable. Label recommendation: `Audit Queue`.

The inbox page should show:

- total open issues,
- distinct affected foreclosures,
- counts by bucket,
- a table of current hits,
- property-page links for each row.

Keep the first version server-rendered and simple. Query-param filters are enough.
You do not need to build a large client-side filtering UI.


## Read-Only Service Layer You Should Add

Do **not** call the full global `run_audit()` for every property-page request.

Instead, add a small read-only helper layer under `src/services/audit/` for web use.

Recommended new file:

- `src/services/audit/web_audit_service.py`

Required module-level docstring:

- explain this is a read-only adapter for web presentation,
- explain that it consumes the existing audit engine,
- explain that it does not persist results.

Recommended functions:

1. `get_property_audit_snapshot(...)`

Suggested inputs:

- `foreclosure_id`
- `folio`
- `strap`
- `case_number`
- optional `conn`

Suggested output:

- `total_open_issues`
- `family_counts`
- `bucket_counts`
- `issues`
- `has_issues`

This helper should resolve only the current property’s issues, not run the full
audit across every active foreclosure.

It may reuse:

- existing SQL bucket handlers from [pg_audit_encumbrance.py](/mnt/c/code/HillsInspector/src/services/audit/pg_audit_encumbrance.py)
- `AuditSignalExtractor.extract_signals_for(...)`

2. `get_encumbrance_audit_inbox(...)`

This helper may call the full global audit once per request because the current
active foreclosure population is small. Keep it read-only.

Suggested output:

- `summary_cards`
- `bucket_summaries`
- `rows`


## Bucket Metadata For Presentation

The raw audit report only gives bucket names and reasons. Add a presentation
mapping in Python so the UI can display stable labels and families.

Create metadata for each bucket with:

- `bucket`
- `label`
- `family`
- `why_it_matters`
- `tone` or `badge_class`

Suggested families:

- `Data Coverage`
  - `lp_missing`
  - `foreclosing_lien_missing`
  - `sat_parent_gap`
  - `lifecycle_base_gap`
  - `cc_lien_gap`
- `Identity / Parties`
  - `plaintiff_chain_gap`
  - `judgment_joined_party_gap`
  - `lp_to_judgment_plaintiff_change`
  - `lp_to_judgment_party_expansion`
- `Property Mismatch`
  - `lp_to_judgment_property_change`
  - `judgment_instrument_gap`
- `Risk Signals`
  - `construction_lien_risk`
  - `superpriority_non_ori_risk`
  - `long_case_interim_risk`

This mapping should live in Python, not duplicated across templates.


## Property Page UX Requirements

### Server-rendered summary on main property page

Use [property_detail()](/mnt/c/code/HillsInspector/app/web/routers/properties.py#L1283)
to inject a small `audit_summary` object into the initial template context.

Use the already available values in `prop`:

- `_foreclosure_id`
- `_strap`
- `_folio_raw`
- `_case_number_raw`

Do not infer from `folio` alone if the foreclosure id is already available.

### Audit tab partial

The `Audit` tab partial should:

- group issues by family,
- show human bucket label,
- show `reason`,
- show `why it matters`,
- show count chips if multiple issues share a bucket,
- link back to the property itself only when useful,
- degrade gracefully when no issues exist.

Do not expose raw internal JSON blobs in the first UI pass.


## Global Inbox UX Requirements

The inbox page should answer:

- what buckets are still open,
- how many foreclosures they affect,
- which properties should an operator inspect next.

Recommended columns:

- property address
- case number
- folio or strap
- bucket label
- family
- reason
- link to property page

Recommended summary cards:

- open issues
- affected foreclosures
- top bucket
- data-coverage issues count

Recommended simple filters:

- `bucket`
- `family`
- free-text search across address / case number / reason

Keep filters simple and server-rendered unless HTMX is trivial to add.


## Error Handling

The UI must never hard-fail the page if the audit service errors.

Requirements:

- property page summary should degrade to “Audit temporarily unavailable”
- audit tab partial should render an error fragment instead of 500ing
- inbox page should render a useful warning state instead of stack traces

Follow the existing patterns already used by the web app for graceful degradation.


## Tests To Add

Follow the style in:

- [tests/test_database_view.py](/mnt/c/code/HillsInspector/tests/test_database_view.py)
- [tests/test_web_encumbrance_summary.py](/mnt/c/code/HillsInspector/tests/test_web_encumbrance_summary.py)

Add tests for:

1. property-detail context includes `audit_summary` when helper is monkeypatched
2. property audit tab route returns the expected template and grouped context
3. review inbox route returns the expected template/context when helper is monkeypatched
4. bucket metadata helper correctly classifies buckets into families
5. property template contains the `Audit` tab and HTMX partial target

Do not require a live PG connection in tests.


## Out of Scope For This Pass

- PG persistence of audit runs or audit hits
- controller/pipeline integration
- status tracking like `open/resolved/ignored`
- operator write actions
- schema changes of any kind

If you believe any of the above is required, stop and note it clearly rather than implementing it.


## Acceptance Criteria

This work is done when:

1. the property page visibly tells the user whether audit issues remain,
2. the property page has a working lazy-loaded `Audit` tab,
3. there is a global audit inbox page in the web app,
4. all of it works in read-only mode with the current PG schema,
5. no new PG tables/columns/migrations were created,
6. focused tests pass,
7. `ruff` and `ty check` pass on changed files.


## Commands Claude Should Run

At minimum:

```bash
uv run pytest tests/test_database_view.py tests/test_web_encumbrance_summary.py
uv run pytest <new web audit tests>
uv run ruff check <changed files>
uv run ty check <changed files>
```

If a route/template test file is added, include it in the focused `pytest` run.
