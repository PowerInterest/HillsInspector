"""PG-first pipeline controller.

This controller orchestrates PostgreSQL ingestion and refresh steps without any
SQLite dependency.
"""

from __future__ import annotations

import argparse
import datetime as dt
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import text

from src.services.CountyPermit import CountyPermitService
from src.services.TampaPermit import TampaPermitService
from src.services.pg_clerk_bulk_service import PgClerkBulkService
from src.services.pg_flr_service import PgFlrService
from src.services.pg_foreclosure_service import PgForeclosureService
from src.services.pg_nal_service import PgNalService
from src.services.pg_title_chain_controller import ControllerConfig as TitleChainConfig
from src.services.pg_title_chain_controller import TitleChainController
from sunbiz.db import get_engine, resolve_pg_dsn


DEFAULT_HCPA_DOWNLOAD_DIR = Path("data/bulk_data/hcpa")
DEFAULT_SUNBIZ_DATA_DIR = Path("data/sunbiz")
DEFAULT_SUNBIZ_MANIFEST = Path("data/sunbiz/manifest.json")


@dataclass(slots=True)
class ControllerSettings:
    dsn: str | None = None
    force_all: bool = False
    fail_fast: bool = False
    # Step toggles
    skip_hcpa: bool = False
    skip_clerk_bulk: bool = False
    skip_nal: bool = False
    skip_flr: bool = False
    skip_sunbiz_entity: bool = False
    skip_county_permits: bool = False
    skip_tampa_permits: bool = False
    skip_foreclosure_refresh: bool = False
    skip_final_refresh: bool = False
    skip_trust_accounts: bool = False
    skip_title_chain: bool = False
    skip_market_data: bool = False
    # Phase B: per-auction enrichment
    skip_auction_scrape: bool = False
    skip_judgment_extract: bool = False
    skip_ori_search: bool = False
    skip_survival: bool = False
    # Staleness windows
    hcpa_stale_days: int = 7
    clerk_stale_days: int = 7
    nal_stale_days: int = 60
    flr_stale_days: int = 7
    sunbiz_entity_stale_days: int = 90
    county_permit_stale_days: int = 7
    tampa_stale_days: int = 3
    # HCPA options
    hcpa_download_dir: Path = DEFAULT_HCPA_DOWNLOAD_DIR
    include_hcpa_latlon: bool = False
    # Sunbiz entity options
    sunbiz_data_dir: Path = DEFAULT_SUNBIZ_DATA_DIR
    sunbiz_manifest: Path = DEFAULT_SUNBIZ_MANIFEST
    # County permit options
    county_where: str = "1=1"
    county_page_size: int = 2000
    # Tampa permit options
    tampa_lookback_days: int = 30
    tampa_start_date: dt.date | None = None
    tampa_end_date: dt.date | None = None
    tampa_keep_csv: bool = False
    tampa_enrich_limit: int = 250
    # Title-chain scope
    foreclosure_id: int | None = None
    case_number: str | None = None
    active_only: bool = False
    limit: int | None = None
    similarity_threshold: float = 0.68
    # Phase B limits
    auction_limit: int | None = None
    judgment_limit: int | None = None
    ori_limit: int | None = None
    survival_limit: int | None = None


class PgPipelineController:
    BACKGROUND_BULK_STEPS: set[str] = {
        "hcpa_suite",
        "clerk_bulk",
        "dor_nal",
        "sunbiz_flr",
        "sunbiz_entity",
        "county_permits",
        "tampa_permits",
    }

    def __init__(self, settings: ControllerSettings) -> None:
        self.settings = settings
        self.dsn = resolve_pg_dsn(settings.dsn)
        self.engine = get_engine(self.dsn)

    def run(self) -> dict[str, Any]:
        started = time.monotonic()
        summary: dict[str, Any] = {
            "dsn": self._dsn_tag(self.dsn),
            "started_at": dt.datetime.now(dt.UTC).isoformat(),
            "steps": [],
            "failed_steps": 0,
        }

        steps: list[tuple[str, bool, Any]] = [
            ("hcpa_suite", self.settings.skip_hcpa, self._run_hcpa_suite),
            ("clerk_bulk", self.settings.skip_clerk_bulk, self._run_clerk_bulk),
            ("dor_nal", self.settings.skip_nal, self._run_nal),
            ("sunbiz_flr", self.settings.skip_flr, self._run_flr),
            ("sunbiz_entity", self.settings.skip_sunbiz_entity, self._run_sunbiz_entity),
            (
                "county_permits",
                self.settings.skip_county_permits,
                self._run_county_permits,
            ),
            ("tampa_permits", self.settings.skip_tampa_permits, self._run_tampa_permits),
            (
                "foreclosure_refresh",
                self.settings.skip_foreclosure_refresh,
                self._run_foreclosure_refresh,
            ),
            (
                "trust_accounts",
                self.settings.skip_trust_accounts,
                self._run_trust_accounts,
            ),
            ("title_chain", self.settings.skip_title_chain, self._run_title_chain),
            # Phase B: per-auction enrichment (scraping + analysis)
            (
                "auction_scrape",
                self.settings.skip_auction_scrape,
                self._run_auction_scrape,
            ),
            (
                "judgment_extract",
                self.settings.skip_judgment_extract,
                self._run_judgment_extract,
            ),
            (
                "ori_search",
                self.settings.skip_ori_search,
                self._run_ori_search,
            ),
            (
                "survival_analysis",
                self.settings.skip_survival,
                self._run_survival_analysis,
            ),
            # Final refresh: pick up all new Phase B data
            (
                "final_refresh",
                self.settings.skip_final_refresh,
                self._run_final_refresh,
            ),
            # Run market data after core foreclosure analysis so it never blocks
            # auction scrape / judgment / ORI / survival progression.
            ("market_data", self.settings.skip_market_data, self._run_market_data),
        ]

        for name, skip, fn in steps:
            result = self._execute_step(name=name, skip=skip, fn=fn)
            summary["steps"].append(result)
            if result["status"] == "failed":
                summary["failed_steps"] += 1
                if self.settings.fail_fast:
                    break

        summary["elapsed_seconds"] = round(time.monotonic() - started, 2)
        summary["completed_at"] = dt.datetime.now(dt.UTC).isoformat()
        return summary

    def _execute_step(self, name: str, skip: bool, fn: Any) -> dict[str, Any]:
        started = time.monotonic()
        if skip:
            return {
                "name": name,
                "status": "skipped",
                "reason": "flagged_skip",
                "elapsed_seconds": 0.0,
            }

        logger.info(f"Step start: {name}")
        try:
            if self._should_dispatch_bulk_step(name):
                from src.services.controller_step_dispatcher import dispatch_controller_step

                payload = dispatch_controller_step(
                    step_name=name,
                    dsn=self.dsn,
                    force_all=self.settings.force_all,
                )
            else:
                payload = fn()
            payload_dict = payload if isinstance(payload, dict) else {"result": payload}
            failed, failure_reason = self._is_failed_payload(payload_dict)
            if failed:
                status = "failed"
            else:
                status = "skipped" if payload_dict.get("skipped") else "ok"
            result = {
                "name": name,
                "status": status,
                "elapsed_seconds": round(time.monotonic() - started, 2),
                "payload": self._json_safe(payload_dict),
            }
            if failure_reason:
                result["reason"] = failure_reason
            elif isinstance(payload_dict.get("reason"), str) and payload_dict.get("reason"):
                result["reason"] = payload_dict["reason"]

            if status == "failed":
                logger.error(
                    "Step failed (payload): {} reason={} payload={}",
                    name,
                    failure_reason or "unknown",
                    self._json_safe(payload_dict),
                )
            else:
                logger.info(f"Step complete: {name} ({result['status']})")
            return result
        except Exception as exc:
            tb = traceback.format_exc(limit=8)
            logger.error(f"Step failed: {name}: {exc}\n{tb}")
            return {
                "name": name,
                "status": "failed",
                "elapsed_seconds": round(time.monotonic() - started, 2),
                "error": str(exc),
            }

    @staticmethod
    def _is_failed_payload(payload: dict[str, Any]) -> tuple[bool, str | None]:
        if payload.get("success") is False:
            return True, "success_false"
        if payload.get("error") not in (None, ""):
            return True, "error_present"

        update = payload.get("update")
        if isinstance(update, dict):
            if update.get("success") is False:
                return True, "update_success_false"
            if update.get("error") not in (None, ""):
                return True, "update_error_present"

        return False, None

    def _run_hcpa_suite(self) -> dict[str, Any]:
        state = self._get_hcpa_state()
        if not self._should_run(
            force=self.settings.force_all,
            count=state["parcels_count"],
            latest=state["latest_loaded_at"],
            stale_days=self.settings.hcpa_stale_days,
        ):
            return {"skipped": True, "reason": "fresh", "state": state}

        from sunbiz.pg_loader import load_hcpa_suite

        stats = load_hcpa_suite(
            dsn=self.dsn,
            downloads_dir=self.settings.hcpa_download_dir,
            parcel_file=None,
            allsales_file=None,
            subdivisions_file=None,
            special_districts_file=None,
            latlon_file=None,
            include_latlon=self.settings.include_hcpa_latlon,
            sync_first=True,
            force_sync=self.settings.force_all,
            batch_size=5000,
            limit_rows=None,
        )
        # load_hcpa_suite now includes lat/lon cross-fill from hcpa_latlon
        return {"state_before": state, "update": stats}

    def _run_clerk_bulk(self) -> dict[str, Any]:
        svc = PgClerkBulkService(dsn=self.dsn)
        if not svc.available:
            return {"skipped": True, "reason": "service_unavailable"}
        state = svc.get_current_state()
        if not self._should_run(
            force=self.settings.force_all,
            count=state.get("cases_count", 0),
            latest=state.get("latest_loaded_at"),
            stale_days=self.settings.clerk_stale_days,
        ):
            return {"skipped": True, "reason": "fresh", "state": state}
        stats = svc.update(force_download=self.settings.force_all)
        return {"state_before": state, "update": stats}

    def _run_nal(self) -> dict[str, Any]:
        svc = PgNalService(dsn=self.dsn)
        if not svc.available:
            return {"skipped": True, "reason": "service_unavailable"}

        state = svc.get_current_state()
        expected_year = self._expected_nal_year()
        loaded_year = state.get("latest_tax_year")
        has_target_year = loaded_year is not None and loaded_year >= expected_year

        should_run = self.settings.force_all or (not has_target_year)
        if not should_run:
            should_run = self._is_stale(
                state.get("latest_loaded_at"),
                self.settings.nal_stale_days,
            )
        if not should_run:
            return {
                "skipped": True,
                "reason": "fresh",
                "state": state,
                "expected_tax_year": expected_year,
            }

        stats = svc.update(force_download=self.settings.force_all)
        return {
            "state_before": state,
            "expected_tax_year": expected_year,
            "update": stats,
        }

    def _run_flr(self) -> dict[str, Any]:
        svc = PgFlrService(dsn=self.dsn)
        if not svc.available:
            return {"skipped": True, "reason": "service_unavailable"}
        state = svc.get_current_state()
        if not self._should_run(
            force=self.settings.force_all,
            count=state.get("filings_count", 0),
            latest=state.get("latest_loaded_at"),
            stale_days=self.settings.flr_stale_days,
        ):
            return {"skipped": True, "reason": "fresh", "state": state}
        stats = svc.update(skip_sftp=False, force_download=self.settings.force_all)
        return {"state_before": state, "update": stats}

    def _run_sunbiz_entity(self) -> dict[str, Any]:
        state = self._get_sunbiz_entity_state()
        if not self._should_run(
            force=self.settings.force_all,
            count=state["filings_count"],
            latest=state["latest_loaded_at"],
            stale_days=self.settings.sunbiz_entity_stale_days,
        ):
            return {"skipped": True, "reason": "fresh", "state": state}

        from sunbiz.pg_loader import load_sunbiz_entity
        from sunbiz.sync import (
            DEFAULT_HOST,
            DEFAULT_PASSWORD,
            DEFAULT_PORT,
            DEFAULT_USER,
            SunbizMirror,
        )

        mirror = SunbizMirror(
            host=DEFAULT_HOST,
            port=DEFAULT_PORT,
            username=DEFAULT_USER,
            password=DEFAULT_PASSWORD,
            data_dir=self.settings.sunbiz_data_dir,
            manifest_path=self.settings.sunbiz_manifest,
            recursive=True,
        )
        mirror.sync(
            mode="quarterly",
            remote_dirs=None,
            include=None,
            exclude=None,
            dataset_profile="entity-quarterly",
            modified_since=None,
            max_files=None,
            dry_run=False,
            force=self.settings.force_all,
        )
        load_stats = load_sunbiz_entity(
            dsn=self.dsn,
            root=self.settings.sunbiz_data_dir,
            pattern=None,
            limit_files=None,
            limit_lines=None,
            batch_size=5000,
        )
        return {"state_before": state, "update": load_stats}

    def _run_county_permits(self) -> dict[str, Any]:
        state = self._get_table_state("county_permits", "source_ingested_at")
        if not self._should_run(
            force=self.settings.force_all,
            count=state["row_count"],
            latest=state["latest_at"],
            stale_days=self.settings.county_permit_stale_days,
        ):
            return {"skipped": True, "reason": "fresh", "state": state}

        svc = CountyPermitService(
            page_size=self.settings.county_page_size,
            pg_dsn=self.dsn,
        )
        stats = svc.sync_postgres(
            where=self.settings.county_where,
            clear_existing=False,
            page_size=self.settings.county_page_size,
        )
        return {"state_before": state, "update": stats}

    def _run_tampa_permits(self) -> dict[str, Any]:
        state = self._get_table_state("tampa_accela_records", "source_ingested_at")
        if not self._should_run(
            force=self.settings.force_all,
            count=state["row_count"],
            latest=state["latest_at"],
            stale_days=self.settings.tampa_stale_days,
        ):
            return {"skipped": True, "reason": "fresh", "state": state}

        start_date, end_date = self._resolve_tampa_window()
        svc = TampaPermitService(pg_dsn=self.dsn, headless=True)
        sync_stats = svc.sync_date_range(
            start_date=start_date,
            end_date=end_date,
            keep_csv=self.settings.tampa_keep_csv,
        )

        enrich_stats: dict[str, int] | None = None
        if self.settings.tampa_enrich_limit != 0:
            limit = self.settings.tampa_enrich_limit
            enrich_stats = svc.enrich_missing_details(limit=limit if limit > 0 else None)

        return {
            "state_before": state,
            "window": {"start_date": start_date, "end_date": end_date},
            "sync": sync_stats,
            "enrich": enrich_stats,
        }

    def _run_foreclosure_refresh(self) -> dict[str, Any]:
        svc = PgForeclosureService(dsn=self.dsn)
        if not svc.available:
            return {"skipped": True, "reason": "service_unavailable"}
        stats = svc.refresh()
        return {"update": stats}

    def _run_trust_accounts(self) -> dict[str, Any]:
        from src.services.pg_trust_accounts import PgTrustAccountsService

        svc = PgTrustAccountsService(dsn=self.dsn)
        if not svc.available:
            return {
                "skipped": True,
                "reason": "service_unavailable",
                "details": svc.unavailable_reason,
            }
        stats = svc.run(force_reprocess=self.settings.force_all)
        return {"update": stats}

    def _run_title_chain(self) -> dict[str, Any]:
        config = TitleChainConfig(
            dsn=self.dsn,
            foreclosure_id=self.settings.foreclosure_id,
            case_number=self.settings.case_number,
            active_only=self.settings.active_only,
            limit=self.settings.limit,
            similarity_threshold=self.settings.similarity_threshold,
        )
        result = TitleChainController(config).run()
        return {"update": result}

    def _run_market_data(self) -> dict[str, Any]:
        from src.services.market_data_dispatcher import dispatch_market_data_worker

        return dispatch_market_data_worker(dsn=self.dsn)

    def _should_dispatch_bulk_step(self, name: str) -> bool:
        return name in self.BACKGROUND_BULK_STEPS

    # ------------------------------------------------------------------
    # Phase B: per-auction enrichment
    # ------------------------------------------------------------------

    def _run_auction_scrape(self) -> dict[str, Any]:
        from src.services.pg_auction_service import PgAuctionService

        svc = PgAuctionService(dsn=self.dsn)
        return svc.run(limit=self.settings.auction_limit)

    def _run_judgment_extract(self) -> dict[str, Any]:
        from src.services.pg_judgment_service import PgJudgmentService

        svc = PgJudgmentService(dsn=self.dsn)
        return svc.run(limit=self.settings.judgment_limit)

    def _run_ori_search(self) -> dict[str, Any]:
        from src.services.pg_ori_service import PgOriService

        svc = PgOriService(dsn=self.dsn)
        return svc.run(limit=self.settings.ori_limit)

    def _run_survival_analysis(self) -> dict[str, Any]:
        from src.services.pg_survival_service import PgSurvivalService

        svc = PgSurvivalService(dsn=self.dsn)
        return svc.run(limit=self.settings.survival_limit)

    def _run_final_refresh(self) -> dict[str, Any]:
        """Re-run foreclosure refresh to pick up Phase B data."""
        from scripts.refresh_foreclosures import refresh as _refresh

        counts = _refresh(dsn=self.dsn)
        return {"update": counts}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_hcpa_state(self) -> dict[str, Any]:
        state = {
            "tables_exist": False,
            "parcels_count": 0,
            "sales_count": 0,
            "latest_loaded_at": None,
        }
        with self.engine.connect() as conn:
            table_count = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_name IN ('hcpa_bulk_parcels', 'hcpa_allsales')
                    """
                )
            ).scalar() or 0
            state["tables_exist"] = table_count == 2
            if not state["tables_exist"]:
                return state

            state["parcels_count"] = conn.execute(
                text("SELECT COUNT(*) FROM hcpa_bulk_parcels")
            ).scalar() or 0
            state["sales_count"] = conn.execute(
                text("SELECT COUNT(*) FROM hcpa_allsales")
            ).scalar() or 0
            state["latest_loaded_at"] = conn.execute(
                text(
                    """
                    SELECT MAX(loaded_at)
                    FROM ingest_files
                    WHERE source_system = 'hcpa'
                      AND category IN ('bulk_parcels', 'allsales')
                      AND status = 'loaded'
                    """
                )
            ).scalar()
        return state

    def _get_sunbiz_entity_state(self) -> dict[str, Any]:
        state = {
            "tables_exist": False,
            "filings_count": 0,
            "parties_count": 0,
            "events_count": 0,
            "latest_loaded_at": None,
        }
        with self.engine.connect() as conn:
            table_count = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_name IN (
                        'sunbiz_entity_filings',
                        'sunbiz_entity_parties',
                        'sunbiz_entity_events'
                    )
                    """
                )
            ).scalar() or 0
            state["tables_exist"] = table_count == 3
            if not state["tables_exist"]:
                return state

            state["filings_count"] = conn.execute(
                text("SELECT COUNT(*) FROM sunbiz_entity_filings")
            ).scalar() or 0
            state["parties_count"] = conn.execute(
                text("SELECT COUNT(*) FROM sunbiz_entity_parties")
            ).scalar() or 0
            state["events_count"] = conn.execute(
                text("SELECT COUNT(*) FROM sunbiz_entity_events")
            ).scalar() or 0
            state["latest_loaded_at"] = conn.execute(
                text(
                    """
                    SELECT MAX(loaded_at)
                    FROM ingest_files
                    WHERE source_system = 'sunbiz'
                      AND category = 'entity_structured'
                      AND status = 'loaded'
                    """
                )
            ).scalar()
        return state

    def _get_table_state(self, table_name: str, time_col: str) -> dict[str, Any]:
        state = {"table_exists": False, "row_count": 0, "latest_at": None}
        with self.engine.connect() as conn:
            exists = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_name = :table_name
                    """
                ),
                {"table_name": table_name},
            ).scalar() or 0
            state["table_exists"] = exists > 0
            if not state["table_exists"]:
                return state

            state["row_count"] = conn.execute(
                text(f"SELECT COUNT(*) FROM {table_name}")
            ).scalar() or 0
            state["latest_at"] = conn.execute(
                text(f"SELECT MAX({time_col}) FROM {table_name}")
            ).scalar()
        return state

    def _resolve_tampa_window(self) -> tuple[dt.date, dt.date]:
        if self.settings.tampa_start_date and self.settings.tampa_end_date:
            if self.settings.tampa_end_date < self.settings.tampa_start_date:
                raise ValueError("tampa_end_date must be >= tampa_start_date")
            return self.settings.tampa_start_date, self.settings.tampa_end_date

        today = dt.datetime.now(dt.UTC).date()
        start = today - dt.timedelta(days=self.settings.tampa_lookback_days)
        return start, today

    @staticmethod
    def _should_run(
        *,
        force: bool,
        count: int,
        latest: Any,
        stale_days: int,
    ) -> bool:
        if force:
            return True
        if (count or 0) == 0:
            return True
        return PgPipelineController._is_stale(latest, stale_days)

    @staticmethod
    def _is_stale(value: Any, stale_days: int) -> bool:
        if value is None:
            return True
        if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
            value_dt = dt.datetime.combine(value, dt.time.min, tzinfo=dt.UTC)
        elif isinstance(value, dt.datetime):
            value_dt = value if value.tzinfo else value.replace(tzinfo=dt.UTC)
        else:
            return True
        return value_dt < (dt.datetime.now(dt.UTC) - dt.timedelta(days=stale_days))

    @staticmethod
    def _expected_nal_year() -> int:
        now = dt.datetime.now(dt.UTC)
        return now.year if now.month >= 10 else now.year - 1

    @staticmethod
    def _json_safe(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: PgPipelineController._json_safe(v) for k, v in value.items()}
        if isinstance(value, list):
            return [PgPipelineController._json_safe(v) for v in value]
        if isinstance(value, tuple):
            return [PgPipelineController._json_safe(v) for v in value]
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, (dt.date, dt.datetime)):
            return value.isoformat()
        return value

    @staticmethod
    def _dsn_tag(dsn: str) -> str:
        # Keep logs useful without printing credentials.
        if "@" not in dsn:
            return "<configured>"
        return dsn.split("@", 1)[1]


def _parse_date(value: str | None) -> dt.date | None:
    if not value:
        return None
    return dt.datetime.strptime(value, "%Y-%m-%d").date()


def parse_args() -> ControllerSettings:
    parser = argparse.ArgumentParser(
        description="PG-first controller: update loaders + foreclosure refresh + title chain"
    )
    parser.add_argument("--dsn", help="PostgreSQL DSN override")
    parser.add_argument("--force-all", action="store_true", help="Force all loaders to run")
    parser.add_argument("--fail-fast", action="store_true", help="Stop after first failure")

    parser.add_argument("--skip-hcpa", action="store_true")
    parser.add_argument("--skip-clerk-bulk", action="store_true")
    parser.add_argument("--skip-nal", action="store_true")
    parser.add_argument("--skip-flr", action="store_true")
    parser.add_argument("--skip-sunbiz-entity", action="store_true")
    parser.add_argument("--skip-county-permits", action="store_true")
    parser.add_argument("--skip-tampa-permits", action="store_true")
    parser.add_argument("--skip-foreclosure-refresh", action="store_true")
    parser.add_argument("--skip-final-refresh", action="store_true")
    parser.add_argument("--skip-trust-accounts", action="store_true")
    parser.add_argument("--skip-title-chain", action="store_true")
    parser.add_argument("--skip-market-data", action="store_true")
    # Phase B toggles
    parser.add_argument("--skip-auction-scrape", action="store_true")
    parser.add_argument("--skip-judgment-extract", action="store_true")
    parser.add_argument("--skip-ori-search", action="store_true")
    parser.add_argument("--skip-survival", action="store_true")

    parser.add_argument("--hcpa-download-dir", default=str(DEFAULT_HCPA_DOWNLOAD_DIR))
    parser.add_argument("--include-hcpa-latlon", action="store_true")

    parser.add_argument("--sunbiz-data-dir", default=str(DEFAULT_SUNBIZ_DATA_DIR))
    parser.add_argument("--sunbiz-manifest", default=str(DEFAULT_SUNBIZ_MANIFEST))

    parser.add_argument("--county-where", default="1=1")
    parser.add_argument("--county-page-size", type=int, default=2000)

    parser.add_argument("--tampa-lookback-days", type=int, default=30)
    parser.add_argument("--tampa-start-date")
    parser.add_argument("--tampa-end-date")
    parser.add_argument("--tampa-keep-csv", action="store_true")
    parser.add_argument(
        "--tampa-enrich-limit",
        type=int,
        default=250,
        help="0 disables enrichment; negative = all missing",
    )

    parser.add_argument("--foreclosure-id", type=int)
    parser.add_argument("--case-number")
    parser.add_argument("--active-only", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--similarity-threshold", type=float, default=0.68)

    # Phase B limits
    parser.add_argument("--auction-limit", type=int, help="Max auctions per date to scrape")
    parser.add_argument("--judgment-limit", type=int, help="Max PDFs to extract")
    parser.add_argument("--ori-limit", type=int, help="Max foreclosures for ORI search")
    parser.add_argument("--survival-limit", type=int, help="Max foreclosures for survival analysis")

    args = parser.parse_args()

    return ControllerSettings(
        dsn=args.dsn,
        force_all=bool(args.force_all),
        fail_fast=bool(args.fail_fast),
        skip_hcpa=bool(args.skip_hcpa),
        skip_clerk_bulk=bool(args.skip_clerk_bulk),
        skip_nal=bool(args.skip_nal),
        skip_flr=bool(args.skip_flr),
        skip_sunbiz_entity=bool(args.skip_sunbiz_entity),
        skip_county_permits=bool(args.skip_county_permits),
        skip_tampa_permits=bool(args.skip_tampa_permits),
        skip_foreclosure_refresh=bool(args.skip_foreclosure_refresh),
        skip_final_refresh=bool(args.skip_final_refresh),
        skip_trust_accounts=bool(args.skip_trust_accounts),
        skip_title_chain=bool(args.skip_title_chain),
        skip_market_data=bool(args.skip_market_data),
        skip_auction_scrape=bool(args.skip_auction_scrape),
        skip_judgment_extract=bool(args.skip_judgment_extract),
        skip_ori_search=bool(args.skip_ori_search),
        skip_survival=bool(args.skip_survival),
        hcpa_download_dir=Path(args.hcpa_download_dir),
        include_hcpa_latlon=bool(args.include_hcpa_latlon),
        sunbiz_data_dir=Path(args.sunbiz_data_dir),
        sunbiz_manifest=Path(args.sunbiz_manifest),
        county_where=args.county_where,
        county_page_size=args.county_page_size,
        tampa_lookback_days=args.tampa_lookback_days,
        tampa_start_date=_parse_date(args.tampa_start_date),
        tampa_end_date=_parse_date(args.tampa_end_date),
        tampa_keep_csv=bool(args.tampa_keep_csv),
        tampa_enrich_limit=args.tampa_enrich_limit,
        foreclosure_id=args.foreclosure_id,
        case_number=args.case_number,
        active_only=bool(args.active_only),
        limit=args.limit,
        similarity_threshold=args.similarity_threshold,
        auction_limit=args.auction_limit,
        judgment_limit=args.judgment_limit,
        ori_limit=args.ori_limit,
        survival_limit=args.survival_limit,
    )
