# Permit Pipeline Diagnostics

This document captures how permit data flows through HillsInspector, where failures
can occur, and how to verify end-to-end completeness.

## Architecture

1. Bulk ingestion:
- County permits: `src/services/CountyPermit.py` writes `county_permits`.
- Tampa permits: `src/services/TampaPermit.py` writes `tampa_accela_records`.

2. Property linkage:
- `src/services/pg_title_chain_controller.py` merges permit rows into
  `foreclosure_title_events` as `COUNTY_PERMIT` and `TAMPA_PERMIT`.

3. UI consumption:
- `app/web/routers/properties.py` reads permit rows only from
  `foreclosure_title_events`, not directly from raw permit tables.

## Failure Modes

### 1. Tampa Accela error page after search

Observed behavior:
- Accela can redirect date-window searches to `Error.aspx`.
- Older behavior logged "missing export button" and returned zero rows.
- Controller did not reliably surface this as a permit-step failure.

Current guardrail:
- `capture_window_export()` now raises `RuntimeError` when search submit lands on
  `Error.aspx` or export controls are missing in a non-empty result flow.

### 2. False-positive Tampa linkage from blank addresses

Observed behavior:
- Foreclosures with `property_address = ''` matched Tampa rows with blank address
  keys, producing large permit counts on unrelated cases.

Current guardrail:
- County and Tampa permit join SQL now require non-blank address strings before
  address-equality matching.

### 3. Zero-row Tampa sync over broad windows

Observed behavior:
- Multi-day windows could return zero rows while stale data remained in
  `tampa_accela_records`.

Current guardrail:
- `_run_tampa_permits()` raises if a 7+ day window ingests zero rows.

## Operational Verification Queries

Use these after permit-related runs:

```sql
-- Raw bulk table counts
SELECT COUNT(*) AS county_rows, MAX(source_ingested_at) AS county_latest
FROM county_permits;

SELECT COUNT(*) AS tampa_rows, MAX(source_ingested_at) AS tampa_latest
FROM tampa_accela_records;

-- Linked permit events used by UI
SELECT event_source, COUNT(*) AS n
FROM foreclosure_title_events
WHERE event_source IN ('COUNTY_PERMIT', 'TAMPA_PERMIT')
GROUP BY event_source;

SELECT event_source, COUNT(DISTINCT foreclosure_id) AS foreclosures
FROM foreclosure_title_events
WHERE event_source IN ('COUNTY_PERMIT', 'TAMPA_PERMIT')
GROUP BY event_source;
```

## Known Scope Limits

- County linkage is mainly folio/address-based; unmatched historical addresses can
  reduce coverage.
- Tampa data depends on Accela export reliability and windowing strategy.
- Permit coverage should be interpreted as "linked permits for current foreclosure
  property keys," not "all permits in county/city systems."
