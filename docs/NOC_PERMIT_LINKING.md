# NOC Persistence & Permit Linking

## Overview

Notices of Commencement (NOCs) are public records filed with the Clerk before
construction work begins.  They indicate that a contractor has been hired and
that mechanic's liens may follow if the contractor is not paid.  NOCs are
discovered incidentally by ORI (Official Records Index) searches during
Phase B Step 3, alongside mortgages, judgments, and other encumbrances.

**Before this change** NOCs were silently dropped by three cascading filters:

1. The PG `encumbrance_type_enum` did not include `'noc'` — an INSERT would crash.
2. `normalize_encumbrance_type("noc")` returned `"other"` — no recognition.
3. `_save_documents()` in `pg_ori_service.py` skipped anything not in
   `CANONICAL_ENCUMBRANCE_TYPES` / `CANONICAL_SATISFACTION_TYPES` / assignment.

The web UI already had a query (`_pg_nocs_for_property()`) and a template
section for NOCs, but they always returned empty.

## Architecture

### Data Flow

```
ORI API  ──>  pg_ori_service._save_documents()
                  │
                  ├── canonical = normalize_document_type("(NOC) NOTICE OF COMMENCEMENT")  →  "noc"
                  ├── enc_type  = normalize_encumbrance_type("noc")  →  "noc"
                  ├── is_noc    = True  →  passes filter
                  └── INSERT INTO ori_encumbrances (encumbrance_type = 'noc', ...)
```

### Where NOCs Are Excluded

NOCs are **not liens** — they are administrative notices.  They must never
pollute lien counts, survival analysis, or encumbrance summaries:

| Location | File | Exclusion |
|----------|------|-----------|
| PG function `get_property_encumbrances()` | `create_foreclosures.py` | `AND oe.encumbrance_type != 'noc'` |
| Survival target selection | `pg_survival_service.py` `_find_targets()` | `AND oe.encumbrance_type != 'noc'` in EXISTS |
| Survival encumbrance loading | `pg_survival_service.py` `_load_encumbrances()` | `AND encumbrance_type != 'noc'` |
| Web encumbrances tab | `properties.py` `_pg_encumbrances_for_property()` | `AND oe.encumbrance_type::text != 'noc'` |
| Web list-view lien summary | `pg_web.py` `_encumbrance_lateral_join()` | `AND oe.encumbrance_type != 'noc'` |
| Refresh foreclosures counts | `refresh_foreclosures.py` `ENCUMBRANCE_SQL` | `AND oe.encumbrance_type != 'noc'` |

### Where NOCs Are Included

| Location | File | Query |
|----------|------|-------|
| Permits tab (NOC section) | `properties.py` `_pg_nocs_for_property()` | Filters by `raw_document_type LIKE '%(NOC)%'` |
| Template rendering | `templates/partials/permits.html` | Renders NOC table with linked-permit column |

## Date-Proximity Matching (NOC → Permit)

Real-world flow: **NOC filed → permit pulled → work done**.

`_match_nocs_to_permits()` in `app/web/routers/properties.py` links each NOC
to the closest permit by date:

- **Window**: permit `issue_date` must fall between **30 days before** and
  **730 days after** the NOC `recording_date`.
- **Selection**: closest absolute gap wins.
- **Output**: mutates NOC dicts in-place with `matched_permit` (permit number)
  and `matched_permit_date`.

The template shows this as a "Linked Permit" column in the NOC table.

## Type Normalizer Changes

`src/db/type_normalizer.py`:

- `ALLOWED_ENCUMBRANCE_TYPES` now includes `"noc"`
- `CANONICAL_NOC_TYPES = frozenset({"noc"})` added
- `normalize_encumbrance_type()` recognizes `"NOC"` and
  `"NOTICE OF COMMENCEMENT"` patterns
- `_DOC_TYPE_MAP["NOC"]` already mapped to `"noc"` (pre-existing)

## Migration

`src/db/migrations/create_foreclosures.py`:

- `ENUM_EXTENSIONS` list runs `ALTER TYPE encumbrance_type_enum ADD VALUE IF NOT EXISTS 'noc'`
  in **autocommit mode** before the main DDL transaction (PG requirement for
  `ADD VALUE`).
- `migrate()` updated to execute `ENUM_EXTENSIONS` first.

Run: `uv run python -m src.db.migrations.create_foreclosures`

## Verification SQL

```sql
-- Count persisted NOCs
SELECT count(*) FROM ori_encumbrances WHERE encumbrance_type = 'noc';

-- Confirm NOCs excluded from encumbrance function
SELECT count(*) FROM get_property_encumbrances('<some_strap>')
WHERE encumbrance_type = 'noc';  -- should be 0

-- Confirm NOCs excluded from lien counts
SELECT encumbrance_type, count(*)
FROM ori_encumbrances
WHERE encumbrance_type != 'noc'
GROUP BY encumbrance_type
ORDER BY count(*) DESC;
```

## Files Modified

| File | Change |
|------|--------|
| `src/db/migrations/create_foreclosures.py` | ENUM extension + PG function exclusion |
| `src/db/type_normalizer.py` | Add `'noc'` to sets + normalize pattern |
| `src/services/pg_ori_service.py` | Add `'noc'` to `_PG_ENCUMBRANCE_TYPES` + expand save filter |
| `src/services/pg_survival_service.py` | Exclude from target selection + encumbrance loading |
| `app/web/routers/properties.py` | Exclude from encumbrances tab + add matching function |
| `app/web/pg_web.py` | Exclude from lateral join lien summary |
| `scripts/refresh_foreclosures.py` | Exclude from encumbrance count update |
| `app/web/templates/partials/permits.html` | Add linked-permit column |
