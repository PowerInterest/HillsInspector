# Upsert Source Tracking — Phases 2 & 3 Design

**Date:** 2026-03-09
**Status:** Implementation in progress
**Depends on:** Phase 1 (OverwriteTracker in `src/utils/upsert.py`) — COMPLETE

## Problem Summary

Phase 1 detects value overwrites at runtime and logs them. But the logs are
ephemeral — you cannot query "which properties had specs change in the last
month" or "what source wrote these beds/baths values." Phase 2 persists the
change events. Phase 3 makes upserts source-priority-aware so the COALESCE
order-dependency bug is eliminated.

The current COALESCE pattern in `market_data_service.py` only works when
scrapes execute in priority order (HomeHarvest → Zillow → Redfin → Realtor).
If a lower-priority source inserts specs first (because a higher-priority
source timed out), the specs are permanently locked to the inferior source.
`primary_source` is row-level and first-writer-wins — it does not track which
source wrote the spec columns.

## Phase 2: `data_change_log` Table

### Purpose

Queryable audit trail for value changes detected by `OverwriteTracker`. The
Python tracker already captures `OverwriteEvent` objects — Phase 2 flushes
them into PostgreSQL so they survive log rotation.

### Table Schema

```sql
CREATE TABLE data_change_log (
    id          BIGSERIAL PRIMARY KEY,
    table_name  TEXT NOT NULL,
    row_key     TEXT NOT NULL,     -- strap, folio, or foreclosure_id cast to text
    column_name TEXT NOT NULL,
    old_value   TEXT,              -- cast to text; NULL means was-null (insert)
    new_value   TEXT,              -- cast to text; NULL means set-to-null
    source      TEXT NOT NULL,     -- writer identity: 'zillow', 'redfin', etc.
    changed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_dcl_table_key ON data_change_log(table_name, row_key);
CREATE INDEX idx_dcl_changed_at ON data_change_log(changed_at);
```

### Column Semantics

| Column | Meaning |
|--------|---------|
| `table_name` | Target table being upserted (`property_market`, `foreclosures`, etc.) |
| `row_key` | Primary key value of the affected row, always cast to text |
| `column_name` | The specific column that changed |
| `old_value` | Previous non-null value, cast to text |
| `new_value` | New non-null value after upsert, cast to text |
| `source` | Identity of the writer that caused the change |
| `changed_at` | Wall-clock time of the change (server-side default) |

### Write Path

`OverwriteTracker.compare_after()` already returns an `UpsertResult` with a
list of `OverwriteEvent` objects. Phase 2 adds a `flush_to_log()` method on
`UpsertResult` that bulk-inserts events into `data_change_log`:

```python
# In UpsertResult (src/utils/upsert.py)
def flush_to_log(self, conn: Connection) -> int:
    """Persist overwrite events to data_change_log. Best-effort."""
    if not self.overwrites:
        return 0
    try:
        with conn.begin_nested():
            conn.execute(
                text("""
                    INSERT INTO data_change_log
                        (table_name, row_key, column_name, old_value, new_value, source)
                    VALUES
                        (:table_name, :row_key, :column_name, :old_value, :new_value, :source)
                """),
                [
                    {
                        "table_name": ow.table,
                        "row_key": ow.row_key,
                        "column_name": ow.column,
                        "old_value": str(ow.old_value) if ow.old_value is not None else None,
                        "new_value": str(ow.new_value) if ow.new_value is not None else None,
                        "source": ow.incoming_source,
                    }
                    for ow in self.overwrites
                ],
            )
        return len(self.overwrites)
    except Exception:
        logger.debug("flush_to_log failed for {}.{}", self.table, self.row_key)
        return 0
```

Call site in `market_data_service.py` (each of the 4 upsert methods):

```python
result = tracker.compare_after(conn, strap, MARKET_TRACKED_COLUMNS, ...)
result.log_overwrites()       # existing Phase 1 logging
result.flush_to_log(conn)     # NEW Phase 2 persistence
```

The flush runs inside the same `engine.begin()` transaction as the upsert,
but inside its own nested transaction/savepoint. When the insert succeeds, the
change log commits with the real data write. If the insert fails, the savepoint
rolls back just the log write and the real upsert still commits. This preserves
best-effort observability without making the tracker a data-loss footgun.

### Retention Policy

`data_change_log` grows proportionally to overwrite volume. For ~130
properties scraped from 4 sources, expect low hundreds of rows per pipeline
run. A cron job or pipeline cleanup step should periodically purge rows
older than 90 days:

```sql
DELETE FROM data_change_log WHERE changed_at < now() - interval '90 days';
```

This is not part of the Alembic migration — it will be added as a
maintenance task once table size is observed in production.

### Backfill Strategy

None. The table starts empty. It captures changes going forward from the
first pipeline run after migration. Historical overwrites are not
recoverable.

### Queryable Insights

```sql
-- Properties where specs changed in the last 30 days
SELECT DISTINCT row_key FROM data_change_log
WHERE table_name = 'property_market'
  AND column_name IN ('beds', 'baths', 'sqft', 'year_built')
  AND changed_at > now() - interval '30 days';

-- Which source overwrites the most?
SELECT source, COUNT(*) FROM data_change_log
GROUP BY source ORDER BY 2 DESC;

-- Full change history for one property
SELECT * FROM data_change_log
WHERE table_name = 'property_market' AND row_key = 'ABC123'
ORDER BY changed_at;
```

---

## Phase 3: `specs_source` Column and Priority-Aware Upserts

### Purpose

Decouple data quality from scrape execution order. Instead of relying on
COALESCE argument order, each upsert checks the source that last wrote the
spec columns and only overwrites if the incoming source has higher priority.

### New Columns on `property_market`

```sql
ALTER TABLE property_market
    ADD COLUMN specs_source     TEXT,         -- source that last wrote spec columns
    ADD COLUMN specs_updated_at TIMESTAMPTZ;  -- when specs were last written
```

### Column Semantics

| Column | Meaning |
|--------|---------|
| `specs_source` | Identity of the source that last wrote `beds`, `baths`, `sqft`, `year_built`, `lot_size`, `property_type`. One of: `homeharvest`, `redfin`, `zillow`, `realtor`. |
| `specs_updated_at` | Timestamp of the last spec-column write. Used for staleness detection, not priority. |

These are **spec-group-level** columns, not per-column. All six spec fields
share one source identity. This matches reality: a single API response
provides all spec fields together. Per-column tracking would add 12 columns
for marginal benefit.

### Source Priority Map

Defined as a Python constant in `market_data_service.py`:

```python
SOURCE_PRIORITY: dict[str, int] = {
    "homeharvest": 40,  # MLS-derived, highest accuracy
    "redfin":      30,  # Direct MLS feed
    "zillow":      20,  # Zestimate models + listing data
    "realtor":     10,  # Backup source
}
```

Higher number = higher priority. A source with priority 40 always overwrites
a source with priority 20, regardless of execution order. Same-source refreshes
are allowed to update their own values. Lower-priority sources are still
allowed to fill previously NULL spec fields.

### SQL Pattern Change

**Before (HomeHarvest example — COALESCE):**
```sql
beds = COALESCE(EXCLUDED.beds, property_market.beds)
```

**After (all sources — priority-aware CASE with null-fill preservation):**
```sql
beds = CASE
    WHEN EXCLUDED.beds IS NULL
    THEN property_market.beds
    WHEN property_market.beds IS NULL
    THEN EXCLUDED.beds
    WHEN property_market.specs_source IS NULL
    THEN EXCLUDED.beds
    WHEN property_market.specs_source = :source_name
    THEN EXCLUDED.beds
    WHEN :source_priority > COALESCE(
            (CASE property_market.specs_source
                 WHEN 'homeharvest' THEN 40
                 WHEN 'redfin'      THEN 30
                 WHEN 'zillow'      THEN 20
                 WHEN 'realtor'     THEN 10
                 ELSE 0
             END), 0)
    THEN EXCLUDED.beds
    ELSE property_market.beds
END
```

This is verbose per-column but identical for all six spec columns. A helper
function `_specs_priority_sql(col_name: str) -> str` generates the CASE
expression to avoid repetition.

When the incoming source is allowed to act as the dominant spec writer
(`specs_source IS NULL`, same-source refresh, or higher-priority upgrade),
`specs_source` and `specs_updated_at` are also updated:

```sql
specs_source = CASE
    WHEN property_market.specs_source IS NULL THEN :source_name
    WHEN property_market.specs_source = :source_name THEN :source_name
    WHEN :source_priority > <existing specs_source priority>
    THEN :source_name
    ELSE property_market.specs_source
END,
specs_updated_at = CASE
    WHEN property_market.specs_source IS NULL THEN now()
    WHEN property_market.specs_source = :source_name THEN now()
    WHEN :source_priority > <existing specs_source priority> THEN now()
    THEN now()
    ELSE property_market.specs_updated_at
END
```

This means `specs_source` is the **dominant** spec writer, not exact
per-column provenance. If a lower-priority source fills a NULL gap on a row
already owned by a higher-priority source, the individual column value changes
but `specs_source` stays with the dominant source.

### Backfill Strategy

After the Alembic migration adds the columns (NULL by default), backfill
from existing per-source JSON columns:

```sql
UPDATE property_market
SET specs_source = CASE
        WHEN (
            beds IS NOT NULL
            OR baths IS NOT NULL
            OR sqft IS NOT NULL
            OR year_built IS NOT NULL
            OR lot_size IS NOT NULL
            OR property_type IS NOT NULL
        ) THEN CASE
            WHEN homeharvest_json IS NOT NULL AND homeharvest_json::text != 'null' THEN 'homeharvest'
            WHEN redfin_json     IS NOT NULL AND redfin_json::text != 'null' THEN 'redfin'
            WHEN zillow_json     IS NOT NULL AND zillow_json::text != 'null' THEN 'zillow'
            WHEN realtor_json    IS NOT NULL AND realtor_json::text != 'null' THEN 'realtor'
            ELSE NULL
        END
        ELSE NULL
    END,
    specs_updated_at = CASE
        WHEN (
            beds IS NOT NULL
            OR baths IS NOT NULL
            OR sqft IS NOT NULL
            OR year_built IS NOT NULL
            OR lot_size IS NOT NULL
            OR property_type IS NOT NULL
        ) THEN updated_at
        ELSE NULL
    END
WHERE specs_source IS NULL OR specs_updated_at IS NULL;
```

This is an approximation: it infers the source from which JSON blobs are
present, using the priority order. It won't be perfect for rows where
multiple sources contributed, but it's the best we can do without historical
change data. The backfill runs once as part of the Alembic migration's
`upgrade()` function.

### What Does NOT Change

- `primary_source` column stays as-is (row-level, first-writer-wins). It
  still tracks "who created this row" for provenance. It is not involved in
  spec-priority decisions.
- Valuation columns (`zestimate`, `rent_zestimate`, `list_price`,
  `tax_assessed_value`) keep their current COALESCE patterns. Each has a
  clear single authoritative source (Zillow owns zestimate, Redfin owns
  list_price). No priority conflict exists.
- Photo columns keep their current complex CASE logic (longest valid array
  wins). Photos are not spec data.
- Per-source JSON columns (`zillow_json`, `redfin_json`, etc.) always
  overwrite unconditionally. These are raw payload storage.

---

## Alembic Migration 011

Single migration combining Phase 2 + Phase 3 schema changes. The
`confidence` column on `ori_encumbrances` is a separate concern and will get
its own migration when that feature is designed.

```
alembic/versions/011_data_change_log_and_specs_source.py
```

### Migration Contents

1. `CREATE TABLE data_change_log` with indexes
2. If `property_market` exists, `ALTER TABLE property_market ADD COLUMN specs_source TEXT`
3. If `property_market` exists, `ALTER TABLE property_market ADD COLUMN specs_updated_at TIMESTAMPTZ`
4. Backfill `specs_source` from existing JSON columns (one-time UPDATE, `realtor_json` branch only when that column exists)

`downgrade()` raises `NotImplementedError` per project policy.

---

## Implementation Order

1. **Alembic migration 011** — schema changes + backfill
2. **`UpsertResult.flush_to_log()`** — new method in `src/utils/upsert.py`
3. **Wire flush into market_data_service.py** — call `flush_to_log()` after
   each `compare_after()` in all 4 upsert methods
4. **`_specs_priority_sql()` helper** — generates CASE expression
5. **Rewrite 4 upsert ON CONFLICT SET clauses** — replace COALESCE with
   priority-aware CASE for the 6 spec columns + `specs_source` +
   `specs_updated_at`
6. **Update OverwriteTracker integration** — pass `specs_source` update
   through tracker (tracker already handles `source_column`)
7. **Tests**

---

## Test Plan

### Phase 2 Tests (data_change_log)

1. **`test_flush_to_log_persists_overwrite_events`** — create OverwriteEvents,
   flush, query `data_change_log`, verify rows match.
2. **`test_flush_to_log_noop_when_no_overwrites`** — empty overwrites list
   produces 0 rows.
3. **`test_flush_to_log_fault_isolation`** — if table doesn't exist or
   connection is broken, flush returns 0 without raising.
4. **`test_flush_inside_transaction_is_atomic`** — overwrite event and data
   change land in same commit.

### Phase 3 Tests (specs_source priority)

5. **`test_higher_priority_source_overwrites_specs`** — homeharvest
   overwrites zillow specs.
6. **`test_lower_priority_source_does_not_overwrite_specs`** — realtor does
   not overwrite zillow specs.
7. **`test_same_source_refresh_updates_specs`** — same source can refresh its
   own values without being blocked by the priority gate.
8. **`test_null_specs_source_allows_any_write`** — first writer always
   succeeds when `specs_source IS NULL`.
9. **`test_specs_source_updated_on_overwrite`** — verify `specs_source` and
   `specs_updated_at` change when priority wins.
10. **`test_lower_priority_source_can_fill_null_specs_without_downgrading_specs_source`**
    — preserve completeness while keeping dominant provenance.
11. **`test_valuation_columns_unchanged_by_priority`** — zestimate,
    list_price etc. still use their existing COALESCE logic.
12. **`test_backfill_infers_source_from_json_columns`** — verify the
    migration backfill SQL produces correct `specs_source` values.

### Integration

13. **`test_market_data_worker_end_to_end`** — existing worker test still
    passes with new upsert logic.
14. **`test_overwrite_tracker_logs_and_flushes`** — both `log_overwrites()`
    and `flush_to_log()` fire on the same UpsertResult.
