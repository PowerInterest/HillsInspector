"""Run one PG-controlled scheduled job.

This CLI is intended to be called by cron/systemd at a fixed cadence. The
actual execution policy (enabled flag, min interval, singleton behavior, args)
is enforced from PostgreSQL by `PgJobControlService`.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from loguru import logger

from src.services.pg_auction_results_service import PgAuctionResultsService
from src.services.pg_job_control_service import JobDefinition, PgJobControlService

from src.services.pg_clerk_bulk_service import PgClerkBulkService
from src.services.pg_clerk_civil_alpha_service import PgClerkCivilAlphaService
from src.services.pg_clerk_criminal_service import PgClerkCriminalService
from sunbiz.sync import SunbizMirror
from sunbiz.sync import (
    DEFAULT_DATA_DIR,
    DEFAULT_HOST,
    DEFAULT_MANIFEST,
    DEFAULT_PASSWORD,
    DEFAULT_PORT,
    DEFAULT_USER,
)
from sunbiz.pg_loader import load_hcpa_suite
from sunbiz.pg_loader import load_sunbiz_entity
from sunbiz.pg_loader import load_sunbiz_raw
from src.services.pg_flr_service import PgFlrService
from src.services.pg_nal_service import PgNalService

_DEFAULT_BATCH_SIZE = 5000
_SUNBIZ_DAILY_ROOT = DEFAULT_DATA_DIR / "public/doc"
_SUNBIZ_DAILY_PATTERN = r"^(?!quarterly/).+"
_SUNBIZ_ENTITY_ROOT = DEFAULT_DATA_DIR / "public/doc/quarterly"
_SUNBIZ_ENTITY_PATTERN = (
    r"(?i)^(cor|gen)/(cordata|corevt|genfile|genevt)\.zip$"
)
_HCPA_DOWNLOAD_DIR = Path("data/bulk_data/hcpa")


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None:
            return None
        parsed = int(value)
        return parsed if parsed > 0 else None
    except (TypeError, ValueError):
        return None


def _int_or_default(value: Any, default: int) -> int:
    try:
        parsed = int(value)
        return parsed if parsed >= 0 else default
    except (TypeError, ValueError):
        return default


def _bool_or_default(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return default


def _require_sync_candidates(job_name: str, sync_stats: dict[str, Any]) -> None:
    if int(sync_stats.get("candidate_files") or 0) > 0:
        return
    raise RuntimeError(
        f"{job_name} matched no remote files; refusing to report success for a no-op"
    )


def _run_auction_results_job(dsn: str, args_json: dict[str, Any]) -> dict[str, Any]:
    service = PgAuctionResultsService(dsn=dsn)
    return service.run(
        date_limit=_int_or_none(args_json.get("date_limit")),
        lookback_days=_int_or_default(args_json.get("lookback_days"), 3),
    )


def _run_clerk_bulk_job(dsn: str, args_json: dict[str, Any]) -> dict[str, Any]:
    service = PgClerkBulkService(dsn=dsn)
    return service.update()


def _run_clerk_criminal_job(dsn: str, args_json: dict[str, Any]) -> dict[str, Any]:
    service = PgClerkCriminalService(dsn=dsn)
    return service.update(
        force_download=_bool_or_default(args_json.get("force_download"), default=False),
    )


def _run_clerk_civil_alpha_job(dsn: str, args_json: dict[str, Any]) -> dict[str, Any]:
    service = PgClerkCivilAlphaService(dsn=dsn)
    return service.update(
        force_download=_bool_or_default(args_json.get("force_download"), default=False),
    )


def _run_sunbiz_daily_job(dsn: str, args_json: dict[str, Any]) -> dict[str, Any]:
    mirror = SunbizMirror(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        username=DEFAULT_USER,
        password=DEFAULT_PASSWORD,
        data_dir=DEFAULT_DATA_DIR,
        manifest_path=DEFAULT_MANIFEST,
        recursive=True,
    )
    sync_stats = mirror.sync(
        mode="daily",
        remote_dirs=None,
        include=None,
        exclude=r"/quarterly/",
        dataset_profile=None,
        modified_since=None,
        max_files=None,
        dry_run=False,
        force=_bool_or_default(args_json.get("force_sync"), default=False),
    )
    _require_sync_candidates("sunbiz_daily", sync_stats)

    load_stats = load_sunbiz_raw(
        dsn=dsn,
        root=_SUNBIZ_DAILY_ROOT,
        pattern=_SUNBIZ_DAILY_PATTERN,
        limit_files=_int_or_none(args_json.get("limit_files")),
        limit_lines=_int_or_none(args_json.get("limit_lines")),
        batch_size=max(1, _int_or_default(args_json.get("batch_size"), _DEFAULT_BATCH_SIZE)),
        skip_unchanged=not _bool_or_default(
            args_json.get("no_skip_unchanged"),
            default=False,
        ),
    )
    if int(load_stats.get("files_discovered") or 0) <= 0:
        raise RuntimeError("sunbiz_daily discovered no daily files to load into PG")
    if (
        int(load_stats.get("files_loaded") or 0) <= 0
        and int(load_stats.get("files_skipped") or 0) <= 0
    ):
        raise RuntimeError("sunbiz_daily completed without loading or skipping any files")

    logger.info("sunbiz_daily sync={} load={}", sync_stats, load_stats)
    return {"success": True, "mode": "daily", "sync": sync_stats, "update": load_stats}


def _run_sunbiz_flr_quarterly_job(dsn: str, args_json: dict[str, Any]) -> dict[str, Any]:
    service = PgFlrService(dsn=dsn)
    return service.update()


def _run_sunbiz_entity_quarterly_job(dsn: str, args_json: dict[str, Any]) -> dict[str, Any]:
    mirror = SunbizMirror(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        username=DEFAULT_USER,
        password=DEFAULT_PASSWORD,
        data_dir=DEFAULT_DATA_DIR,
        manifest_path=DEFAULT_MANIFEST,
        recursive=True,
    )
    sync_stats = mirror.sync(
        mode="quarterly",
        remote_dirs=None,
        include=None,
        exclude=None,
        dataset_profile="entity-quarterly",
        modified_since=None,
        max_files=None,
        dry_run=False,
        force=_bool_or_default(args_json.get("force_sync"), default=False),
    )
    _require_sync_candidates("sunbiz_entity_quarterly", sync_stats)

    load_stats = load_sunbiz_entity(
        dsn=dsn,
        root=_SUNBIZ_ENTITY_ROOT,
        pattern=_SUNBIZ_ENTITY_PATTERN,
        limit_files=_int_or_none(args_json.get("limit_files")),
        limit_lines=_int_or_none(args_json.get("limit_lines")),
        batch_size=max(1, _int_or_default(args_json.get("batch_size"), _DEFAULT_BATCH_SIZE)),
    )
    if int(load_stats.get("files_scanned") or 0) <= 0:
        raise RuntimeError(
            "sunbiz_entity_quarterly scanned no entity files after the quarterly sync"
        )

    logger.info("sunbiz_entity_quarterly sync={} load={}", sync_stats, load_stats)
    return {
        "success": True,
        "mode": "quarterly",
        "sync": sync_stats,
        "update": load_stats,
    }


def _run_dor_nal_annual_job(dsn: str, args_json: dict[str, Any]) -> dict[str, Any]:
    service = PgNalService(dsn=dsn)
    return service.update()


def _run_hcpa_bulk_job(dsn: str, args_json: dict[str, Any]) -> dict[str, Any]:
    stats = load_hcpa_suite(
        dsn=dsn,
        downloads_dir=_HCPA_DOWNLOAD_DIR,
        parcel_file=None,
        allsales_file=None,
        subdivisions_file=None,
        special_districts_file=None,
        latlon_file=None,
        include_latlon=not _bool_or_default(
            args_json.get("skip_latlon"),
            default=False,
        ),
        sync_first=True,
        force_sync=_bool_or_default(args_json.get("force_sync"), default=False),
        batch_size=max(1, _int_or_default(args_json.get("batch_size"), _DEFAULT_BATCH_SIZE)),
        limit_rows=None,
    )
    return {"success": True, "update": stats}


JOB_DEFINITIONS: dict[str, JobDefinition] = {
    "auction_results": JobDefinition(
        name="auction_results",
        handler=_run_auction_results_job,
        default_min_interval_sec=3600,
        default_max_runtime_sec=1800,
        singleton=True,
        default_args_json={"lookback_days": 3},
    ),
    "clerk_bulk": JobDefinition(
        name="clerk_bulk",
        handler=_run_clerk_bulk_job,
        default_min_interval_sec=86400,  # Daily
        default_max_runtime_sec=7200,  # 2 hours
        singleton=True,
    ),
    "clerk_criminal": JobDefinition(
        name="clerk_criminal",
        handler=_run_clerk_criminal_job,
        default_min_interval_sec=604800,  # Weekly
        default_max_runtime_sec=7200,  # 2 hours
        singleton=True,
    ),
    "clerk_civil_alpha": JobDefinition(
        name="clerk_civil_alpha",
        handler=_run_clerk_civil_alpha_job,
        default_min_interval_sec=604800,  # Weekly
        default_max_runtime_sec=7200,  # 2 hours
        singleton=True,
    ),
    "sunbiz_daily": JobDefinition(
        name="sunbiz_daily",
        handler=_run_sunbiz_daily_job,
        default_min_interval_sec=86400,  # Daily
        default_max_runtime_sec=3600,  # 1 hour
        singleton=True,
    ),
    "sunbiz_flr_quarterly": JobDefinition(
        name="sunbiz_flr_quarterly",
        handler=_run_sunbiz_flr_quarterly_job,
        default_min_interval_sec=7776000,  # 90 Days
        default_max_runtime_sec=14400,  # 4 hours
        singleton=True,
    ),
    "sunbiz_entity_quarterly": JobDefinition(
        name="sunbiz_entity_quarterly",
        handler=_run_sunbiz_entity_quarterly_job,
        default_min_interval_sec=7776000,  # 90 Days
        default_max_runtime_sec=14400,  # 4 hours
        singleton=True,
    ),
    "dor_nal_annual": JobDefinition(
        name="dor_nal_annual",
        handler=_run_dor_nal_annual_job,
        default_min_interval_sec=2419200,  # 28 days (allows for Oct, Nov, Dec retries)
        default_max_runtime_sec=7200,  # 2 hours
        singleton=True,
    ),
    "hcpa_bulk": JobDefinition(
        name="hcpa_bulk",
        handler=_run_hcpa_bulk_job,
        default_min_interval_sec=604800,  # Weekly
        default_max_runtime_sec=3600,  # 1 hour
        singleton=True,
    ),
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a PG-controlled scheduled job")
    parser.add_argument(
        "--job",
        choices=sorted(JOB_DEFINITIONS.keys()),
        required=True,
        help="Job name to execute",
    )
    parser.add_argument("--dsn", help="PostgreSQL DSN override")
    parser.add_argument(
        "--triggered-by",
        default="cron",
        help="Run trigger source label stored in pipeline_job_runs.triggered_by",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass enabled/min-interval/singleton gates for this invocation",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    definition = JOB_DEFINITIONS[args.job]

    runner = PgJobControlService(dsn=args.dsn)
    result = runner.run_job(
        definition,
        triggered_by=args.triggered_by,
        force=bool(args.force),
    )

    logger.info("Scheduled job result: {}", result)
    print(json.dumps(result, indent=2, default=str))
    if result.get("status") == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
