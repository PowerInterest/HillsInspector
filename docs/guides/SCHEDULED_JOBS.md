# Scheduled Jobs (PG-Controlled, Cron-Triggered)

This project now supports DB-controlled scheduled jobs for Python workers.

## Why

`Controller.py` remains the full orchestration entrypoint, but recurring
collectors can run independently on a schedule so one long controller run does
not block all other updates.

## Control Tables

### `pipeline_job_config`

One row per job:

- `job_name` (PK)
- `enabled`
- `min_interval_sec`
- `max_runtime_sec`
- `singleton`
- `args_json`
- `paused_reason`
- `created_at`, `updated_at`

### `pipeline_job_runs`

Append-only execution history:

- `run_id` (PK)
- `job_name` (FK -> `pipeline_job_config`)
- `triggered_by`
- `started_at`, `finished_at`
- `status` (`running|success|failed|skipped|timed_out`)
- `summary_json`
- `error`

All supported scheduled jobs are seeded into `pipeline_job_config` by Alembic,
so operators can pause or retune them before the first cron execution.

## Runner Command

Run one scheduled job:

```bash
uv run python -m src.tools.run_scheduled_job --job auction_results
```

Force execution (ignore interval/enable gates):

```bash
uv run python -m src.tools.run_scheduled_job --job auction_results --force
```

## Cron Example (Hourly)

```cron
CRON_TZ=America/New_York
0 * * * * cd /opt/HillsInspector && /usr/local/bin/uv run python -m src.tools.run_scheduled_job --job auction_results --triggered-by cron >> logs/cron_auction_results.log 2>&1
```

## Initial Test Job: `auction_results`

The `auction_results` job runs `PgAuctionResultsService` and updates
`foreclosures` with:

- `auction_status`
- `winning_bid`
- `sold_to`
- `buyer_type`

Target scope is active rows near current date, with lookback controlled via
`pipeline_job_config.args_json` (default `{"lookback_days": 3}`).

## Operational Controls

Pause job:

```sql
UPDATE pipeline_job_config
SET enabled = FALSE, paused_reason = 'maintenance'
WHERE job_name = 'auction_results';
```

Resume job:

```sql
UPDATE pipeline_job_config
SET enabled = TRUE, paused_reason = NULL
WHERE job_name = 'auction_results';
```

Change interval to every 30 minutes:

```sql
UPDATE pipeline_job_config
SET min_interval_sec = 1800
WHERE job_name = 'auction_results';
```

Recent run audit:

```sql
SELECT run_id, job_name, status, started_at, finished_at
FROM pipeline_job_runs
WHERE job_name = 'auction_results'
ORDER BY run_id DESC
LIMIT 20;
```

## Bulk Ingestion Schedules

The following are the recommended cron schedules and job names for all bulk data ingestion services. These jobs trigger the heavy PostgreSQL data loading processes.

**1. Hillsborough Clerk of Court (Daily)**
Downloads daily Official Records indexes, case events, and judgments.
```cron
# Every night at 2:00 AM
0 2 * * * cd /opt/HillsInspector && /usr/local/bin/uv run python -m src.tools.run_scheduled_job --job clerk_bulk --triggered-by cron >> logs/cron_clerk.log 2>&1
```

**2. Sunbiz Raw Feed (Daily)**
Mirrors the non-quarterly Sunbiz daily feed and loads raw fixed-width records
into `sunbiz_raw_records`.
```cron
# Every night at 2:30 AM
30 2 * * * cd /opt/HillsInspector && /usr/local/bin/uv run python -m src.tools.run_scheduled_job --job sunbiz_daily --triggered-by cron >> logs/cron_sunbiz_daily.log 2>&1
```

**3. Sunbiz FLR/UCC Liens (Quarterly)**
Downloads massive Federal Lien/UCC datasets. Padded to the 5th to allow State upload delays.
```cron
# 3:00 AM on the 5th of January, April, July, and October
0 3 5 1,4,7,10 * cd /opt/HillsInspector && /usr/local/bin/uv run python -m src.tools.run_scheduled_job --job sunbiz_flr_quarterly --triggered-by cron >> logs/cron_sunbiz_flr.log 2>&1
```

**4. Sunbiz Full Entity Refresh (Quarterly)**
Mirrors the quarterly entity dataset and loads the structured
`sunbiz_entity_*` tables.
```cron
# 4:00 AM on the 5th of January, April, July, and October
0 4 5 1,4,7,10 * cd /opt/HillsInspector && /usr/local/bin/uv run python -m src.tools.run_scheduled_job --job sunbiz_entity_quarterly --triggered-by cron >> logs/cron_sunbiz_quarterly.log 2>&1
```

**5. Florida DOR NAL Tax Roll (Annual)**
Loads the final tax roll, homestead exemptions, and legal descriptions.
```cron
# 1:00 AM on the 15th of October, November, and December (until a new year arrives)
0 1 15 10,11,12 * cd /opt/HillsInspector && /usr/local/bin/uv run python -m src.tools.run_scheduled_job --job dor_nal_annual --triggered-by cron >> logs/cron_dor_nal.log 2>&1
```

**6. HCPA Parcels & Sales (Weekly)**
Updates property sales records and folio-to-strap mappings.
```cron
# Every Sunday at 3:00 AM
0 3 * * 0 cd /opt/HillsInspector && /usr/local/bin/uv run python -m src.tools.run_scheduled_job --job hcpa_bulk --triggered-by cron >> logs/cron_hcpa.log 2>&1
```
