# Scheduled Jobs Implementation

This walkthrough documents the integration of 6 bulk data ingestion pipelines into the native PostgreSQL `pipeline_job_config` job scheduler.

## Overview of Changes

To transition away from raw cron jobs or manual `subprocess` invocations, the [`run_scheduled_job.py`](../../src/tools/run_scheduled_job.py) CLI was expanded. This allows [`PgJobControlService`](../../src/services/pg_job_control_service.py) to safely lock and track the execution of all system bulk loads in the `pipeline_job_runs` table.

### 1. New Handler Wrappers
Added 6 specialized Python wrappers in [`src/tools/run_scheduled_job.py`](../../src/tools/run_scheduled_job.py) to instantiate and execute the bulk services natively:
- [`_run_clerk_bulk_job`](../../src/tools/run_scheduled_job.py) for [`PgClerkBulkService`](../../src/services/pg_clerk_bulk_service.py)
- [`_run_sunbiz_daily_job`](../../src/tools/run_scheduled_job.py) for [`SunbizMirror(mode='daily')`](../../src/services/sunbiz_sync_service.py) plus [`load_sunbiz_raw`](../../sunbiz/pg_loader.py)
- [`_run_sunbiz_flr_quarterly_job`](../../src/tools/run_scheduled_job.py) for [`PgFlrService`](../../src/services/pg_flr_service.py)
- [`_run_sunbiz_entity_quarterly_job`](../../src/tools/run_scheduled_job.py) for [`SunbizMirror(mode='quarterly')`](../../src/services/sunbiz_sync_service.py) plus [`load_sunbiz_entity`](../../sunbiz/pg_loader.py)
- [`_run_dor_nal_annual_job`](../../src/tools/run_scheduled_job.py) for [`PgNalService`](../../src/services/pg_nal_service.py)
- [`_run_hcpa_bulk_job`](../../src/tools/run_scheduled_job.py) for [`load_hcpa_suite`](../../sunbiz/pg_loader.py)

### 2. Job Definitions and Frequencies
Mapped the documented job frequencies directly into `JOB_DEFINITIONS`. [`PgJobControlService`](../../src/services/pg_job_control_service.py) uses these intervals to prevent redundant runs even if triggered explicitly.

- [`clerk_bulk`](../../src/services/pg_pipeline_controller.py): `default_min_interval_sec=86400` (Daily)
- [`sunbiz_daily`](../../src/tools/run_scheduled_job.py): `default_min_interval_sec=86400` (Daily)
- [`sunbiz_flr_quarterly`](../../src/tools/run_scheduled_job.py): `default_min_interval_sec=7776000` (90 Days)
- [`sunbiz_entity_quarterly`](../../src/tools/run_scheduled_job.py): `default_min_interval_sec=7776000` (90 Days)
- [`dor_nal_annual`](../../src/tools/run_scheduled_job.py): `default_min_interval_sec=2419200` (28 Days, allowing October-November-December retries)
- [`hcpa_bulk`](../../src/tools/run_scheduled_job.py): `default_min_interval_sec=604800` (Weekly)

## Notes

- `sunbiz_daily` mirrors the non-quarterly daily Sunbiz feed and loads raw records into PostgreSQL.
- `sunbiz_entity_quarterly` performs both the quarterly file mirror and the PostgreSQL entity load.
- `hcpa_bulk` uses the PostgreSQL HCPA suite loader rather than the legacy SQLite bulk importer.
- `PgJobControlService` now records lock-contention skips and ignores `skipped` runs when enforcing `min_interval_sec`.

## Validation Results
We successfully verified the entrypoint by forcing an ad-hoc run of the daily clerk pipeline:

```bash
uv run python -m src.tools.run_scheduled_job --job clerk_bulk --force
```

The job correctly bypassed the min-interval checks, instantiated the PostgreSQL connection, and began downloading the 73 CSV files from the Hillsborough Clerk's public index.
