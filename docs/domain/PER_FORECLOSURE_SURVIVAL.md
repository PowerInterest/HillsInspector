# Per-Foreclosure Survival Persistence

## Problem

`ori_encumbrances` is parcel-scoped. That works for ORI discovery, but it is
not enough for lien-survival outcomes.

Two active foreclosure cases can share one strap:

- the same lis pendens can be `FORECLOSING` in one case and only
  `HISTORICAL`/procedural in the other
- a recorded judgment from case A can be `HISTORICAL` for case A but
  `SURVIVED` as an external junior lien in case B

Persisting `survival_status` directly on `ori_encumbrances` collapses both case
analyses onto one shared row. The last analysis wins and silently overwrites the
other case.

## Fix

Survival results are now persisted in:

- `foreclosure_encumbrance_survival`

Key:

- `(foreclosure_id, encumbrance_id)` unique

Stored fields:

- `survival_status`
- `survival_reason`
- `survival_case_number`
- `analyzed_at`

`PgSurvivalService` still updates the legacy `ori_encumbrances.survival_*`
columns for compatibility, but those columns are now best-effort cache only.
Correctness-sensitive reads must prefer the per-foreclosure table.

## Read Semantics

When the current foreclosure is known, use:

```sql
LEFT JOIN foreclosure_encumbrance_survival fes
  ON fes.foreclosure_id = f.foreclosure_id
 AND fes.encumbrance_id = oe.id
```

and read:

```sql
COALESCE(fes.survival_status, oe.survival_status)
COALESCE(fes.survival_reason, oe.survival_reason)
```

This keeps older rows readable while new per-foreclosure results are present.

## Updated Paths

The following paths now read the per-foreclosure table:

- property detail encumbrance queries
- dashboard/search encumbrance aggregates
- encumbrance audit summary counts and historical-window bucket
- refresh-time encumbrance counts
- PostgreSQL helper functions `get_dashboard_auctions`,
  `get_property_encumbrances`, and `compute_net_equity`

The helper functions now accept explicit foreclosure context where needed:

- `get_dashboard_auctions(...)` uses the row's `foreclosure_id`
- `get_property_encumbrances(p_strap, p_foreclosure_id DEFAULT NULL)`
- `compute_net_equity(p_strap, p_foreclosure_id DEFAULT NULL)`

When `p_foreclosure_id` is omitted, the strap-scoped helpers fall back to the
latest active foreclosure on that strap for backward compatibility. Shared-strap
callers should pass the foreclosure id explicitly.

## Rescheduled Auctions

`refresh_foreclosures` can copy enrichment from archived donor rows to a newer
active row with the same `case_number_raw`. Once survival moved into
`foreclosure_encumbrance_survival`, copying only `step_survival_analyzed` was no
longer enough. The refresh step now clones the donor's per-foreclosure survival
rows to the new foreclosure when the donor's survival set is complete.

## Operational Repair

After deploying the migration, rerun survival so the new table is populated:

```bash
uv run python - <<'PY'
from src.services.pg_survival_service import PgSurvivalService
print(PgSurvivalService().run(force_reanalysis=True))
PY
```

Then refresh per-foreclosure encumbrance counts:

```bash
uv run python - <<'PY'
from sqlalchemy import text
from sunbiz.db import get_engine, resolve_pg_dsn
from src.scripts.refresh_foreclosures import ENCUMBRANCE_SQL

engine = get_engine(resolve_pg_dsn())
with engine.begin() as conn:
    conn.execute(text(ENCUMBRANCE_SQL))
PY
```

## Why This Matters

Without this table, the pipeline can report the wrong foreclosing lien and the
wrong surviving liens for parcels that have multiple active foreclosure cases.
That is a correctness bug, not just a presentation issue.
