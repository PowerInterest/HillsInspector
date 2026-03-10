"""PG-first pipeline controller.

This controller orchestrates PostgreSQL ingestion and refresh steps without any
SQLite dependency.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import text

from src.utils.step_result import StepResult, is_failed_payload

from src.services.CountyPermit import CountyPermitService
from src.services.TampaPermit import TampaPermitService
from src.services.pg_permit_single_pin_service import PgPermitSinglePinService
from src.services.pg_clerk_bulk_service import PgClerkBulkService
from src.services.pg_clerk_civil_alpha_service import PgClerkCivilAlphaService
from src.services.pg_clerk_criminal_service import PgClerkCriminalService
from src.services.pg_flr_service import PgFlrService
from src.services.pg_foreclosure_service import PgForeclosureService
from src.services.pg_nal_service import PgNalService
from src.services.pg_title_chain_controller import ControllerConfig as TitleChainConfig
from src.services.pg_title_chain_controller import TitleChainController
from sunbiz.db import get_engine, resolve_pg_dsn


DEFAULT_HCPA_DOWNLOAD_DIR = Path("data/bulk_data/hcpa")
DEFAULT_SUNBIZ_DATA_DIR = Path("data/sunbiz")
DEFAULT_SUNBIZ_MANIFEST = Path("data/sunbiz/manifest.json")
DEFAULT_SUNBIZ_ENTITY_ROOT = DEFAULT_SUNBIZ_DATA_DIR / "public/doc/quarterly"
SUNBIZ_ENTITY_FILE_PATTERN = (
    r"(?i)^(cor|gen)/(cordata|corevt|genfile|genevt)\.zip$"
)
_TITLE_BREAK_MIN_PASSES = 2
_TITLE_BREAK_MAX_EXTRA_CYCLES = 5


@dataclass(slots=True)
class ControllerSettings:
    dsn: str | None = None
    force_all: bool = False
    fail_fast: bool = False
    background_bulk_steps: bool = False
    background_market_data: bool = False
    # Step toggles
    skip_hcpa: bool = False
    skip_clerk_bulk: bool = False
    skip_clerk_criminal: bool = False
    skip_clerk_civil_alpha: bool = False
    skip_nal: bool = False
    skip_flr: bool = False
    skip_sunbiz_entity: bool = False
    skip_county_permits: bool = False
    skip_tampa_permits: bool = False
    skip_single_pin_permits: bool = False
    skip_foreclosure_refresh: bool = False
    skip_final_refresh: bool = False
    skip_trust_accounts: bool = False
    skip_title_chain: bool = False
    skip_title_breaks: bool = False
    skip_market_data: bool = False
    # Phase B: per-auction enrichment
    skip_auction_scrape: bool = False
    skip_judgment_extract: bool = False
    skip_identifier_recovery: bool = False
    skip_ori_search: bool = False
    skip_municipal_liens: bool = False
    skip_mortgage_extract: bool = False
    skip_survival: bool = False
    skip_encumbrance_audit: bool = False
    skip_encumbrance_recovery: bool = False
    # Market Data specific
    use_windows_chrome: bool = False
    # Staleness windows
    hcpa_stale_days: int = 7
    clerk_stale_days: int = 7
    clerk_criminal_stale_days: int = 7
    clerk_civil_alpha_stale_days: int = 7
    nal_stale_days: int = 60
    flr_stale_days: int = 7
    sunbiz_entity_stale_days: int = 90
    county_permit_stale_days: int = 7
    tampa_stale_days: int = 3
    # HCPA options
    hcpa_download_dir: Path = DEFAULT_HCPA_DOWNLOAD_DIR
    include_hcpa_latlon: bool = True
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
    # Single-pin permit fallback options
    single_pin_permit_limit: int = 25
    single_pin_permit_max_permits: int = 0
    single_pin_permit_timeout_seconds: int = 45
    # Title-chain scope
    foreclosure_id: int | None = None
    case_number: str | None = None
    active_only: bool = True
    limit: int | None = None
    similarity_threshold: float = 0.68
    # Phase B limits
    auction_limit: int | None = None
    judgment_limit: int | None = None
    identifier_recovery_limit: int | None = None
    ori_limit: int | None = None
    mortgage_limit: int | None = None
    survival_limit: int | None = None
    title_breaks_limit: int | None = None


class PgPipelineController:
    BACKGROUND_BULK_STEPS: set[str] = {
        "hcpa_suite",
        "clerk_bulk",
        "clerk_criminal",
        "clerk_civil_alpha",
        "dor_nal",
        "sunbiz_flr",
        "sunbiz_entity",
    }

    def __init__(self, settings: ControllerSettings) -> None:
        self.settings = settings
        self.dsn = resolve_pg_dsn(settings.dsn)
        self.engine = get_engine(self.dsn)
        self._encumbrance_audit_report: Any | None = None

    def run(self) -> dict[str, Any]:
        started = time.monotonic()
        summary: dict[str, Any] = {
            "dsn": self._dsn_tag(self.dsn),
            "started_at": dt.datetime.now(dt.UTC).isoformat(),
            "steps": [],
            "degraded_steps": 0,
            "failed_steps": 0,
        }

        steps: list[tuple[str, bool, Any]] = [
            ("hcpa_suite", self.settings.skip_hcpa, self._run_hcpa_suite),
            ("clerk_bulk", self.settings.skip_clerk_bulk, self._run_clerk_bulk),
            ("clerk_criminal", self.settings.skip_clerk_criminal, self._run_clerk_criminal),
            ("clerk_civil_alpha", self.settings.skip_clerk_civil_alpha, self._run_clerk_civil_alpha),
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
                "single_pin_permits",
                self.settings.skip_single_pin_permits,
                self._run_single_pin_permits,
            ),
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
            ("title_breaks", self.settings.skip_title_breaks, self._run_title_breaks),
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
                "identifier_recovery",
                self.settings.skip_identifier_recovery,
                self._run_identifier_recovery,
            ),
            (
                "ori_search",
                self.settings.skip_ori_search,
                self._run_ori_search,
            ),
            (
                "municipal_liens_phase0",
                self.settings.skip_municipal_liens,
                self._run_municipal_liens_phase0,
            ),
            (
                "mortgage_extract",
                self.settings.skip_mortgage_extract,
                self._run_mortgage_extract,
            ),
            (
                "encumbrance_audit",
                self.settings.skip_encumbrance_audit,
                self._run_encumbrance_audit,
            ),
            (
                "encumbrance_recovery",
                self.settings.skip_encumbrance_recovery,
                self._run_encumbrance_recovery,
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
            summary["steps"].append(result.to_summary_dict())
            if result.status == "failed":
                summary["failed_steps"] += 1
                if self.settings.fail_fast:
                    break
            elif result.status == "degraded":
                summary["degraded_steps"] += 1

        summary["elapsed_seconds"] = round(time.monotonic() - started, 2)
        summary["completed_at"] = dt.datetime.now(dt.UTC).isoformat()
        return summary

    def _execute_step(self, name: str, skip: bool, fn: Any) -> StepResult:
        started = time.monotonic()
        if skip:
            return StepResult(
                step_name=name,
                status="skipped",
                details={"reason": "flagged_skip"},
            )

        logger.info(f"Step start: {name}")
        try:
            if self._should_dispatch_bulk_step(name):
                from src.services.controller_step_dispatcher import dispatch_controller_step

                payload = dispatch_controller_step(
                    step_name=name,
                    dsn=self.dsn,
                    force_all=self.settings.force_all,
                )
                # Background dispatch returns a raw dict; convert to StepResult.
                result = self._step_result_from_payload(name, payload)
            else:
                result = fn()
                if not isinstance(result, StepResult):
                    # Safety net for any step not yet migrated.
                    result = self._step_result_from_payload(name, result)

            elapsed = int((time.monotonic() - started) * 1000)
            result.duration_ms = elapsed

            logger.info(result.log_line())
            if result.status == "failed":
                logger.error(
                    "Step failed (payload): {} details={}",
                    name,
                    self._json_safe(result.details),
                )
            elif result.status == "degraded":
                logger.warning(
                    "Step degraded: {} details={}",
                    name,
                    self._json_safe(result.details),
                )
            return result
        except Exception as exc:
            tb = traceback.format_exc(limit=8)
            logger.error(f"Step failed: {name}: {exc}\n{tb}")
            elapsed = int((time.monotonic() - started) * 1000)
            return StepResult(
                step_name=name,
                status="failed",
                duration_ms=elapsed,
                errors=1,
                details={"error": str(exc)},
            )

    @staticmethod
    def _step_result_from_payload(name: str, payload: Any) -> StepResult:
        """Convert a legacy raw-dict payload into a StepResult.

        Used for background-dispatched bulk steps (which return JSON dicts)
        and as a safety net for any ``_run_*`` method not yet returning
        ``StepResult`` natively.
        """
        from src.utils.step_result import is_failed_payload

        payload_dict = payload if isinstance(payload, dict) else {"result": payload}

        if is_failed_payload(payload_dict):
            return StepResult(
                step_name=name,
                status="failed",
                errors=1,
                details=payload_dict,
            )
        if payload_dict.get("skipped"):
            return StepResult(
                step_name=name,
                status="skipped",
                details=payload_dict,
            )
        if (
            payload_dict.get("degraded") is True
            or payload_dict.get("status") == "degraded"
        ):
            update = payload_dict.get("update")
            if isinstance(update, dict) and (
                update.get("degraded") is True
                or update.get("status") == "degraded"
            ):
                pass  # already matched at top level
            return StepResult(
                step_name=name,
                status="degraded",
                details=payload_dict,
            )

        # Check nested update for degraded too
        update = payload_dict.get("update")
        if isinstance(update, dict) and (
            update.get("degraded") is True
            or update.get("status") == "degraded"
        ):
            return StepResult(
                step_name=name,
                status="degraded",
                details=payload_dict,
            )

        return StepResult(
            step_name=name,
            status="success",
            details=payload_dict,
        )

    def _run_hcpa_suite(self) -> StepResult:
        state = self._get_hcpa_state()
        if not self._should_run(
            force=self.settings.force_all,
            count=state["parcels_count"],
            latest=state["latest_loaded_at"],
            stale_days=self.settings.hcpa_stale_days,
        ):
            return StepResult(
                step_name="hcpa_suite", status="skipped",
                details={"reason": "fresh", "state": state},
            )

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
        inserted = int(stats.get("parcels_inserted", 0))
        updated = int(stats.get("parcels_updated", 0))
        return StepResult(
            step_name="hcpa_suite",
            status="success" if (inserted + updated) > 0 else "noop",
            inserted=inserted, updated=updated,
            details={"state_before": state, "update": stats},
        )

    def _run_clerk_bulk(self) -> StepResult:
        svc = PgClerkBulkService(dsn=self.dsn)
        if not svc.available:
            return StepResult(
                step_name="clerk_bulk", status="skipped",
                details={"reason": "service_unavailable"},
            )
        state = svc.get_current_state()
        if not self._should_run(
            force=self.settings.force_all,
            count=state.get("cases_count", 0),
            latest=state.get("latest_loaded_at"),
            stale_days=self.settings.clerk_stale_days,
        ):
            return StepResult(
                step_name="clerk_bulk", status="skipped",
                details={"reason": "fresh", "state": state},
            )
        stats = svc.update(force_download=self.settings.force_all)
        rows = self._int_from_paths(
            stats,
            "cases.rows_upserted",
            "events.rows_inserted",
            "parties.rows_upserted",
            "disposed.rows_upserted",
            "garnishment.rows_inserted",
            "official_records.rows_upserted",
        )
        return StepResult(
            step_name="clerk_bulk",
            status="success" if rows > 0 else "noop",
            inserted=rows,
            details={"state_before": state, "update": stats},
        )

    def _run_clerk_criminal(self) -> StepResult:
        svc = PgClerkCriminalService(dsn=self.dsn)
        if not svc.available:
            return StepResult(
                step_name="clerk_criminal", status="skipped",
                details={"reason": "service_unavailable"},
            )

        count = 0
        latest = None
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    text("SELECT COUNT(*) FROM clerk_criminal_name_index")
                ).scalar()
                count = row or 0
                ts = conn.execute(
                    text(
                        "SELECT MAX(loaded_at) FROM ingest_files "
                        "WHERE source_system = 'clerk_criminal'"
                    )
                ).scalar()
                latest = ts
        except Exception:
            logger.opt(exception=True).debug(
                "Clerk criminal staleness check failed; forcing refresh"
            )

        if not self._should_run(
            force=self.settings.force_all,
            count=count,
            latest=latest,
            stale_days=self.settings.clerk_criminal_stale_days,
        ):
            return StepResult(
                step_name="clerk_criminal", status="skipped",
                details={"reason": "fresh", "count": count},
            )
        stats = svc.update(force_download=self.settings.force_all)
        rows = self._int_from_paths(stats, "load.rows_inserted")
        return StepResult(
            step_name="clerk_criminal",
            status="success" if rows > 0 else "noop",
            inserted=rows,
            details={"update": stats},
        )

    def _run_clerk_civil_alpha(self) -> StepResult:
        svc = PgClerkCivilAlphaService(dsn=self.dsn)
        if not svc.available:
            return StepResult(
                step_name="clerk_civil_alpha", status="skipped",
                details={"reason": "service_unavailable"},
            )

        count = 0
        latest = None
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT COUNT(*) FROM clerk_civil_parties "
                        "WHERE source_file LIKE 'alpha:%'"
                    )
                ).scalar()
                count = row or 0
                ts = conn.execute(
                    text(
                        "SELECT MAX(loaded_at) FROM ingest_files "
                        "WHERE source_system = 'clerk_civil_alpha'"
                    )
                ).scalar()
                latest = ts
        except Exception:
            logger.opt(exception=True).debug(
                "Clerk civil alpha staleness check failed; forcing refresh"
            )

        if not self._should_run(
            force=self.settings.force_all,
            count=count,
            latest=latest,
            stale_days=self.settings.clerk_civil_alpha_stale_days,
        ):
            return StepResult(
                step_name="clerk_civil_alpha", status="skipped",
                details={"reason": "fresh", "count": count},
            )
        stats = svc.update(force_download=self.settings.force_all)
        rows = self._max_int_from_paths(
            stats,
            "load.cases_upserted",
            "load.parties_upserted",
        )
        return StepResult(
            step_name="clerk_civil_alpha",
            status="success" if rows > 0 else "noop",
            inserted=rows,
            details={"update": stats},
        )

    def _run_nal(self) -> StepResult:
        svc = PgNalService(dsn=self.dsn)
        if not svc.available:
            return StepResult(
                step_name="dor_nal", status="skipped",
                details={"reason": "service_unavailable"},
            )

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
            return StepResult(
                step_name="dor_nal", status="skipped",
                details={
                    "reason": "fresh", "state": state,
                    "expected_tax_year": expected_year,
                },
            )

        stats = svc.update(force_download=self.settings.force_all)
        rows = self._int_from_paths(stats, "load_stats.parcels_upserted")
        return StepResult(
            step_name="dor_nal",
            status="success" if rows > 0 else "noop",
            inserted=rows,
            details={
                "state_before": state, "expected_tax_year": expected_year,
                "update": stats,
            },
        )

    def _run_flr(self) -> StepResult:
        svc = PgFlrService(dsn=self.dsn)
        if not svc.available:
            return StepResult(
                step_name="sunbiz_flr", status="skipped",
                details={"reason": "service_unavailable"},
            )
        state = svc.get_current_state()
        if not self._should_run(
            force=self.settings.force_all,
            count=state.get("filings_count", 0),
            latest=state.get("latest_loaded_at"),
            stale_days=self.settings.flr_stale_days,
        ):
            return StepResult(
                step_name="sunbiz_flr", status="skipped",
                details={"reason": "fresh", "state": state},
            )
        stats = svc.update(skip_sftp=False, force_download=self.settings.force_all)
        rows = self._int_from_paths(
            stats,
            "load_stats.filings_upserted",
            "load_stats.parties_inserted",
            "load_stats.events_inserted",
        )
        return StepResult(
            step_name="sunbiz_flr",
            status="success" if rows > 0 else "noop",
            inserted=rows,
            details={"state_before": state, "update": stats},
        )

    def _run_sunbiz_entity(self) -> StepResult:
        state = self._get_sunbiz_entity_state()
        if not self._should_run(
            force=self.settings.force_all,
            count=state["filings_count"],
            latest=state["latest_loaded_at"],
            stale_days=self.settings.sunbiz_entity_stale_days,
        ):
            return StepResult(
                step_name="sunbiz_entity", status="skipped",
                details={"reason": "fresh", "state": state},
            )

        from sunbiz.pg_loader import load_sunbiz_entity
        from src.scripts.sunbiz_sync_service import (
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
        entity_root = self.settings.sunbiz_data_dir / "public/doc/quarterly"
        load_stats = load_sunbiz_entity(
            dsn=self.dsn,
            root=entity_root,
            pattern=SUNBIZ_ENTITY_FILE_PATTERN,
            limit_files=None,
            limit_lines=None,
            batch_size=5000,
        )
        if int(load_stats.get("files_scanned") or 0) <= 0:
            raise RuntimeError(
                f"sunbiz_entity sync completed but no entity files were scanned under {entity_root}"
            )
        rows = int(load_stats.get("rows_written", 0)) + int(load_stats.get("filings_inserted", 0))
        return StepResult(
            step_name="sunbiz_entity",
            status="success" if rows > 0 else "noop",
            inserted=rows,
            details={"state_before": state, "update": load_stats},
        )

    def _run_county_permits(self) -> StepResult:
        state = self._get_table_state("county_permits", "source_ingested_at")
        if not self._should_run(
            force=self.settings.force_all,
            count=state["row_count"],
            latest=state["latest_at"],
            stale_days=self.settings.county_permit_stale_days,
        ):
            return StepResult(
                step_name="county_permits", status="skipped",
                details={"reason": "fresh", "state": state},
            )

        where = self.settings.county_where or "1=1"
        max_oid = self._get_county_max_object_id()
        if max_oid is not None and not self.settings.force_all:
            where = f"({where}) AND OBJECTID > {max_oid}"
            logger.info(
                "County permits: last OBJECTID in DB is {}, fetching only new records (existing {} rows)",
                max_oid,
                state["row_count"],
            )
        else:
            logger.info("County permits: no existing data or force mode, full ArcGIS layer pull")

        svc = CountyPermitService(
            page_size=self.settings.county_page_size,
            pg_dsn=self.dsn,
        )
        stats = svc.sync_postgres(
            where=where,
            clear_existing=False,
            page_size=self.settings.county_page_size,
        )
        rows = int(stats.get("written", 0)) + int(stats.get("rows_written", 0))
        return StepResult(
            step_name="county_permits",
            status="success" if rows > 0 else "noop",
            inserted=rows,
            details={"state_before": state, "max_oid_before": max_oid, "update": stats},
        )

    def _run_tampa_permits(self) -> StepResult:
        state = self._get_table_state("tampa_accela_records", "source_ingested_at")
        if not self._should_run(
            force=self.settings.force_all,
            count=state["row_count"],
            latest=state["latest_at"],
            stale_days=self.settings.tampa_stale_days,
        ):
            return StepResult(
                step_name="tampa_permits", status="skipped",
                details={"reason": "fresh", "state": state},
            )

        start_date, end_date = self._resolve_tampa_window()
        svc = TampaPermitService(pg_dsn=self.dsn, headless=True)
        sync_stats = svc.sync_date_range(
            start_date=start_date,
            end_date=end_date,
            keep_csv=self.settings.tampa_keep_csv,
        )

        if (
            (end_date - start_date).days >= 7
            and int(sync_stats.get("csv_rows_total") or 0) == 0
            and int(sync_stats.get("written_total") or 0) == 0
        ):
            raise RuntimeError(
                "Tampa permit sync produced zero rows for a 7+ day window; treating as failure to avoid silent stale permit data."
            )

        enrich_stats: dict[str, int] | None = None
        if self.settings.tampa_enrich_limit != 0:
            limit = self.settings.tampa_enrich_limit
            enrich_stats = svc.enrich_missing_details(limit=limit if limit > 0 else None)

        rows = int(sync_stats.get("written_total", 0))
        enrich_errors = int((enrich_stats or {}).get("errors") or 0)
        enrich_selected = int((enrich_stats or {}).get("selected") or 0)
        if enrich_errors > 0 and (rows > 0 or enrich_selected > 0):
            status = "degraded"
        else:
            status = "success" if rows > 0 else "noop"
        return StepResult(
            step_name="tampa_permits",
            status=status,
            inserted=rows,
            errors=enrich_errors,
            details={
                "state_before": state,
                "window": {"start_date": start_date, "end_date": end_date},
                "sync": sync_stats,
                "enrich": enrich_stats,
            },
        )

    def _run_single_pin_permits(self) -> StepResult:
        if self.settings.single_pin_permit_limit <= 0:
            return StepResult(
                step_name="single_pin_permits", status="skipped",
                details={"reason": "disabled_limit"},
            )

        missing_tables = self._missing_tables([
            "foreclosures",
            "hcpa_bulk_parcels",
            "county_permits",
            "tampa_accela_records",
        ])
        if missing_tables:
            raise RuntimeError("single_pin_permits requires missing table(s): " + ", ".join(sorted(missing_tables)))

        candidates = self._select_single_pin_permit_candidates(limit=self.settings.single_pin_permit_limit)
        if not candidates:
            return StepResult(
                step_name="single_pin_permits", status="skipped",
                details={"reason": "no_gaps_detected"},
            )

        pins = [str(row.get("pin")).strip() for row in candidates if row.get("pin") is not None and str(row.get("pin")).strip()]
        pins = list(dict.fromkeys(pins))
        if not pins:
            return StepResult(
                step_name="single_pin_permits", status="skipped",
                details={"reason": "no_valid_pins"},
            )

        max_permits = self.settings.single_pin_permit_max_permits if self.settings.single_pin_permit_max_permits > 0 else None
        svc = PgPermitSinglePinService(
            dsn=self.dsn,
            timeout_seconds=max(5, self.settings.single_pin_permit_timeout_seconds),
            include_accela=True,
            include_arcgis=True,
        )
        sync_stats = svc.sync_pins_to_postgres(
            pins,
            max_permits_per_pin=max_permits,
            fail_on_pin_error=False,
        )

        pins_failed = int(sync_stats.get("pins_failed") or 0)
        pins_targeted = int(sync_stats.get("pins_targeted") or 0)
        failed_pins: list[str] = []
        if pins_failed > 0:
            failed_pins = [e["pin"] for e in sync_stats.get("errors", [])]
            logger.warning(
                "Single-pin permits: {}/{} pins failed — {}. "
                "These properties will not have permit data until the underlying "
                "issue is resolved (e.g. HCPA parcel lookup returning no data).",
                pins_failed,
                pins_targeted,
                ", ".join(failed_pins),
            )
            if pins_failed == pins_targeted and pins_targeted > 0:
                logger.error("All {} targeted pins failed. Marking step as failed.", pins_targeted)
                return StepResult(
                    step_name="single_pin_permits", status="failed",
                    errors=pins_failed,
                    details={
                        "candidate_count": len(candidates),
                        "pins_targeted": len(pins),
                        "pins_failed": pins_failed,
                        "failed_pins": failed_pins,
                        "candidate_sample": candidates[:10],
                        "update": sync_stats,
                        "error": f"All {pins_targeted} targeted pins failed to sync permits",
                    },
                )

        permits_observed = int(sync_stats.get("permits_observed_total") or 0)
        total_writes = int(sync_stats.get("total_writes") or 0)
        if permits_observed > 0 and total_writes == 0:
            raise RuntimeError(
                "single_pin_permits observed permit rows but wrote nothing to permit tables; failing to avoid silent no-op."
            )

        detail = {
            "candidate_count": len(candidates),
            "pins_targeted": len(pins),
            "pins_failed": pins_failed,
            "failed_pins": failed_pins,
            "candidate_sample": candidates[:10],
            "update": sync_stats,
        }
        if 0 < pins_failed < pins_targeted:
            return StepResult(
                step_name="single_pin_permits", status="degraded",
                inserted=total_writes, errors=pins_failed,
                details={**detail, "reason": "partial_pin_failures"},
            )
        return StepResult(
            step_name="single_pin_permits",
            status="success" if total_writes > 0 else "noop",
            inserted=total_writes,
            details=detail,
        )

    def run_single_pin_permits_job(self) -> dict[str, Any]:
        """Public scheduled-job entrypoint for the single-pin permit step."""
        return self._run_single_pin_permits().to_summary_dict()

    def _run_foreclosure_refresh(self) -> StepResult:
        svc = PgForeclosureService(dsn=self.dsn)
        if not svc.available:
            return StepResult(
                step_name="foreclosure_refresh", status="skipped",
                details={"reason": "service_unavailable"},
            )
        stats = svc.refresh()
        rows = self._int_from_paths(
            stats,
            "enriched",
            "strap_resolved",
            "coords_enriched",
            "resale",
            "events_inserted",
            "encumbrances",
            "archived",
            "judgments",
            "rescheduled_reused",
        )
        return StepResult(
            step_name="foreclosure_refresh",
            status="success" if rows > 0 else "noop",
            updated=rows,
            details={"update": stats},
        )

    def _run_trust_accounts(self) -> StepResult:
        from src.services.pg_trust_accounts import PgTrustAccountsService

        svc = PgTrustAccountsService(dsn=self.dsn)
        if not svc.available:
            return StepResult(
                step_name="trust_accounts", status="skipped",
                details={
                    "reason": "service_unavailable",
                    "unavailable_reason": svc.unavailable_reason,
                },
            )
        stats = svc.run(force_reprocess=self.settings.force_all)
        rows = self._int_from_paths(
            stats,
            "rows_upserted",
            "rows_deleted",
            "summary_rows_written",
        )
        return StepResult(
            step_name="trust_accounts",
            status="success" if rows > 0 else "noop",
            updated=rows,
            details={"update": stats},
        )

    def _run_title_chain(self) -> StepResult:
        result = self._run_title_chain_materialization()
        built = (
            int(result.get("chain_rows", 0))
            + int(result.get("summary_rows", 0))
            + int(result.get("events_inserted", 0))
        )
        return StepResult(
            step_name="title_chain",
            status="success" if built > 0 else "noop",
            inserted=built,
            details={"update": result},
        )

    def _run_title_chain_materialization(self) -> dict[str, Any]:
        config = TitleChainConfig(
            dsn=self.dsn,
            foreclosure_id=self.settings.foreclosure_id,
            case_number=self.settings.case_number,
            active_only=self.settings.active_only,
            limit=self.settings.limit,
            similarity_threshold=self.settings.similarity_threshold,
        )
        return TitleChainController(config).run()

    def _run_title_breaks(self) -> StepResult:
        from src.services.pg_title_break_service import PgTitleBreakService

        svc = PgTitleBreakService(dsn=self.dsn)
        pass_results: list[dict[str, Any]] = []
        rebuilds: list[dict[str, Any]] = []
        total_repairs = 0
        total_sentinels = 0
        total_errors = 0
        max_passes = _TITLE_BREAK_MIN_PASSES + _TITLE_BREAK_MAX_EXTRA_CYCLES

        for pass_index in range(1, max_passes + 1):
            logger.info(
                "title_breaks loop pass {}/{} start",
                pass_index,
                max_passes,
            )
            result = svc.run(
                limit=self.settings.title_breaks_limit,
                foreclosure_id=self.settings.foreclosure_id,
                case_number=self.settings.case_number,
            )
            repairs = int(result.get("deeds_inserted", 0)) + int(result.get("backfilled", 0))
            sentinels = int(result.get("sentinels_inserted", 0))
            errors = int(result.get("errors", 0))
            total_repairs += repairs
            total_sentinels += sentinels
            total_errors += errors
            logger.info(
                "title_breaks loop pass {} result: deeds_inserted={} backfilled={} sentinels_inserted={} errors={} repairs={}",
                pass_index,
                int(result.get("deeds_inserted", 0)),
                int(result.get("backfilled", 0)),
                sentinels,
                errors,
                repairs,
            )

            pass_detail = dict(result)
            pass_detail["pass"] = pass_index
            pass_detail["repairs"] = repairs
            pass_detail["state_writes"] = repairs + sentinels

            if repairs > 0:
                rebuild = self._run_title_chain_materialization()
                pass_detail["title_chain_rebuild"] = rebuild
                rebuilds.append(rebuild)
                logger.info(
                    "title_breaks loop pass {} rebuild complete: chain_rows={} summary_rows={} events_inserted={}",
                    pass_index,
                    int(rebuild.get("chain_rows", 0)),
                    int(rebuild.get("summary_rows", 0)),
                    int(rebuild.get("events_inserted", 0)),
                )

            pass_results.append(pass_detail)

            if pass_index < _TITLE_BREAK_MIN_PASSES:
                logger.info(
                    "title_breaks loop pass {} continuing to satisfy minimum pass count {}",
                    pass_index,
                    _TITLE_BREAK_MIN_PASSES,
                )
                continue
            if repairs == 0:
                logger.info(
                    "title_breaks loop stopping after pass {} because repairs == 0",
                    pass_index,
                )
                break
        else:
            logger.warning(
                "title_breaks loop stopped at hard cap after {} passes with total_repairs={}",
                max_passes,
                total_repairs,
            )

        final_result = dict(pass_results[-1]) if pass_results else {
            "skipped": True,
            "reason": "no_passes",
        }
        final_result["passes"] = pass_results
        final_result["pass_count"] = len(pass_results)
        final_result["total_repairs"] = total_repairs
        final_result["total_sentinels_inserted"] = total_sentinels
        final_result["total_errors"] = total_errors
        final_result["rebuild_count"] = len(rebuilds)
        if rebuilds:
            final_result["title_chain_rebuild"] = rebuilds[-1]
        if total_errors > 0 and (total_repairs + total_sentinels) == 0:
            status = "failed"
        elif total_errors > 0 or total_sentinels > 0:
            status = "degraded"
        elif total_repairs > 0:
            status = "success"
        else:
            status = "noop"
        return StepResult(
            step_name="title_breaks",
            status=status,
            updated=total_repairs + total_sentinels,
            errors=total_errors,
            details=final_result,
        )

    def _run_market_data(self) -> StepResult:
        scrapling_result: dict[str, Any] = {}
        try:
            from src.services.pg_market_data_scrapling import (
                PgMarketDataScraplingService,
                _query_properties_needing_market,
            )

            resolved_dsn = self.dsn
            props = _query_properties_needing_market(
                dsn=resolved_dsn,
                limit=self.settings.limit,
                force=self.settings.force_all,
            )
            if props:
                svc = PgMarketDataScraplingService(
                    dsn=resolved_dsn,
                    use_windows_chrome=self.settings.use_windows_chrome,
                    force=self.settings.force_all,
                )
                scrapling_result = asyncio.run(
                    svc.run_batch(props, sources=["realtor"]),
                )
                logger.info("Scrapling Realtor enrichment complete: {}", scrapling_result)
            else:
                scrapling_result = {"skipped": True, "reason": "no_properties_need_market_data"}
        except Exception as exc:
            logger.exception("Scrapling market enrichment failed; continuing with browser worker")
            scrapling_result = {
                "attempted": True,
                "reason": "scrapling_enrichment_failed",
                "warning": str(exc),
            }

        if self.settings.background_market_data:
            from src.services.market_data_dispatcher import dispatch_market_data_worker

            worker_result = dispatch_market_data_worker(
                dsn=self.dsn,
                use_windows_chrome=self.settings.use_windows_chrome,
                force=self.settings.force_all,
            )
        else:
            from src.services.market_data_worker import run_market_data_update

            worker_result = run_market_data_update(
                dsn=self.dsn,
                use_windows_chrome=self.settings.use_windows_chrome,
                force=self.settings.force_all,
            )

        scrapling_work = self._market_work_units(scrapling_result)
        worker_work = self._market_work_units(worker_result)
        total_work = scrapling_work + worker_work
        worker_dispatched = bool(
            isinstance(worker_result, dict)
            and (
                worker_result.get("dispatched") is True
                or worker_result.get("reason")
                == "market_data_worker_dispatched_background"
            )
        )
        any_failed = (
            isinstance(scrapling_result, dict)
            and is_failed_payload(scrapling_result)
        ) or (
            isinstance(worker_result, dict)
            and is_failed_payload(worker_result)
        )
        any_degraded = False
        for payload in (scrapling_result, worker_result):
            if not isinstance(payload, dict):
                continue
            if payload.get("degraded") is True or payload.get("status") == "degraded":
                any_degraded = True
                break
            update = payload.get("update")
            if isinstance(update, dict) and (
                update.get("degraded") is True or update.get("status") == "degraded"
            ):
                any_degraded = True
                break

        if any_failed and total_work == 0:
            status = "failed"
        elif any_failed or any_degraded:
            status = "degraded"
        elif total_work > 0:
            status = "success"
        elif worker_dispatched:
            status = "skipped"
        else:
            status = "noop"

        details: dict[str, Any] = {"scrapling": scrapling_result, "worker": worker_result}
        if worker_dispatched and total_work == 0:
            details["reason"] = "market_data_worker_dispatched_background"
        return StepResult(
            step_name="market_data",
            status=status,
            updated=total_work,
            errors=int(any_failed),
            details=details,
        )

    def _should_dispatch_bulk_step(self, name: str) -> bool:
        return self.settings.background_bulk_steps and name in self.BACKGROUND_BULK_STEPS

    # ------------------------------------------------------------------
    # Phase B: per-auction enrichment
    # ------------------------------------------------------------------

    def _run_auction_scrape(self) -> StepResult:
        from src.services.pg_auction_service import PgAuctionService

        svc = PgAuctionService(dsn=self.dsn)
        result = svc.run(limit=self.settings.auction_limit)
        scraped = self._int_from_paths(result, "auctions_saved")
        return StepResult(
            step_name="auction_scrape",
            status="success" if scraped > 0 else "noop",
            inserted=scraped,
            details=result,
        )

    def _run_judgment_extract(self) -> StepResult:
        from src.services.pg_judgment_service import PgJudgmentService

        svc = PgJudgmentService(dsn=self.dsn)
        result = svc.run(limit=self.settings.judgment_limit)
        updated = self._int_from_paths(result, "judgments_loaded_to_pg", "pdfs_extracted")
        errs = int(result.get("errors", 0))
        return StepResult(
            step_name="judgment_extract",
            status="failed" if errs > 0 and updated == 0 else (
                "success" if updated > 0 else "noop"
            ),
            updated=updated, errors=errs,
            details=result,
        )

    def _run_identifier_recovery(self) -> StepResult:
        from src.services.pg_foreclosure_identifier_recovery_service import (
            PgForeclosureIdentifierRecoveryService,
        )

        svc = PgForeclosureIdentifierRecoveryService(dsn=self.dsn)
        if not svc.available:
            return StepResult(
                step_name="identifier_recovery", status="skipped",
                details={"reason": "service_unavailable"},
            )
        result = svc.run(limit=self.settings.identifier_recovery_limit)
        recovered = self._int_from_paths(result, "rows_updated")
        errors = int(result.get("errors", 0))
        unresolved = int(result.get("unresolved", 0))
        ambiguous = int(result.get("ambiguous", 0))
        if errors > 0 and recovered == 0 and unresolved == 0 and ambiguous == 0:
            status = "failed"
        elif errors > 0 or unresolved > 0 or ambiguous > 0:
            status = "degraded"
        elif recovered > 0:
            status = "success"
        else:
            status = "noop"
        return StepResult(
            step_name="identifier_recovery",
            status=status,
            updated=recovered,
            errors=errors,
            details=result,
        )

    def _run_ori_search(self) -> StepResult:
        from src.services.pg_ori_service import PgOriService

        svc = PgOriService(dsn=self.dsn)
        result = svc.run(limit=self.settings.ori_limit)
        searched = self._int_from_paths(
            result,
            "encumbrances_saved",
            "inferred_saved",
            "satisfactions_linked",
        )
        errs = int(result.get("errors", 0))
        save_skips = int(result.get("save_skips", 0))
        staged_targets = int(result.get("staged_targets", 0))
        if errs > 0 and searched == 0 and save_skips == 0 and staged_targets == 0:
            status = "failed"
        elif errs > 0 or save_skips > 0 or staged_targets > 0:
            status = "degraded"
        elif searched > 0:
            status = "success"
        else:
            status = "noop"
        return StepResult(
            step_name="ori_search",
            status=status,
            updated=searched,
            errors=errs + save_skips,
            details=result,
        )

    def _run_municipal_liens_phase0(self) -> StepResult:
        from src.services.pg_municipal_lien_service import PgMunicipalLienService

        svc = PgMunicipalLienService(dsn=self.dsn)
        result = svc.run_phase0(
            limit=self.settings.limit,
            foreclosure_id=self.settings.foreclosure_id,
            case_number=self.settings.case_number,
            active_only=self.settings.active_only,
        )
        found = self._int_from_paths(result, "findings_written")
        return StepResult(
            step_name="municipal_liens_phase0",
            status="success" if found > 0 else "noop",
            inserted=found,
            details=result,
        )

    def _run_mortgage_extract(self) -> StepResult:
        from src.services.pg_mortgage_extraction_service import PgMortgageExtractionService

        svc = PgMortgageExtractionService(dsn=self.dsn)
        result = svc.run(limit=self.settings.mortgage_limit)
        extracted = self._int_from_paths(result, "mortgages_extracted")
        errs = int(result.get("errors", 0))
        return StepResult(
            step_name="mortgage_extract",
            status="failed" if errs > 0 and extracted == 0 else (
                "success" if extracted > 0 else "noop"
            ),
            updated=extracted, errors=errs,
            details=result,
        )

    def _run_survival_analysis(self) -> StepResult:
        from src.services.pg_survival_service import PgSurvivalService

        svc = PgSurvivalService(dsn=self.dsn)
        result = svc.run(
            limit=self.settings.survival_limit,
            force_reanalysis=True,
        )
        analyzed = int(result.get("analyzed", 0))
        errs = int(result.get("errors", 0))
        return StepResult(
            step_name="survival_analysis",
            status="failed" if errs > 0 and analyzed == 0 else (
                "success" if analyzed > 0 else "noop"
            ),
            updated=analyzed, errors=errs,
            details=result,
        )

    def _run_encumbrance_audit(self) -> StepResult:
        from src.services.audit.pg_audit_encumbrance import run_audit

        report = run_audit(dsn=self.dsn)
        self._encumbrance_audit_report = report

        bucket_counts = {
            summary.bucket: int(summary.count)
            for summary in report.summaries
        }
        open_issues = len(report.hits)
        affected_foreclosures = len({
            int(hit.foreclosure_id)
            for hit in report.hits
        })
        with_strap = int(report.with_strap_count)
        with_encumbrances = int(report.with_encumbrances_count)
        with_survival = int(report.with_survival_count)
        encumbrance_coverage_pct = self._coverage_pct(
            with_encumbrances,
            with_strap,
        )
        survival_coverage_pct = self._coverage_pct(
            with_survival,
            with_strap,
        )

        audit_details = {
            "active_count": int(report.active_count),
            "judged_count": int(report.judged_count),
            "with_strap_count": with_strap,
            "with_encumbrances_count": with_encumbrances,
            "with_survival_count": with_survival,
            "open_issues": open_issues,
            "affected_foreclosures": affected_foreclosures,
            "bucket_counts": bucket_counts,
            "encumbrance_coverage_pct": encumbrance_coverage_pct,
            "survival_coverage_pct": survival_coverage_pct,
            "encumbrance_coverage_target_met": bool(
                encumbrance_coverage_pct is not None
                and encumbrance_coverage_pct >= 80.0
            ),
            "survival_coverage_target_met": bool(
                survival_coverage_pct is not None
                and survival_coverage_pct >= 80.0
            ),
        }
        return StepResult(
            step_name="encumbrance_audit",
            status="success",
            details=audit_details,
        )

    def _run_encumbrance_recovery(self) -> StepResult:
        from src.services.audit.encumbrance_recovery import (
            EncumbranceRecoveryService,
        )

        report = self._encumbrance_audit_report
        if report is None:
            logger.warning("Encumbrance recovery skipped: no audit report available (was audit step skipped?)")
            return StepResult(
                step_name="encumbrance_recovery", status="skipped",
                details={"reason": "no_audit_report"},
            )
        try:
            svc = EncumbranceRecoveryService(dsn=self.dsn)
            result = svc.run(report=report)
            augmented_ids: list[int] = result.get("recovered_foreclosure_ids") or []
            if augmented_ids:
                logger.info(
                    "Clearing step_survival_analyzed for {} recovered foreclosures",
                    len(augmented_ids),
                )
                with self.engine.connect() as conn:
                    conn.execute(
                        text(
                            "UPDATE foreclosures "
                            "SET step_survival_analyzed = NULL "
                            "WHERE foreclosure_id = ANY(:ids) "
                            "AND archived_at IS NULL"
                        ),
                        {"ids": augmented_ids},
                    )
                    conn.commit()
            recovered = int(result.get("recovered", 0)) + len(augmented_ids)
            errors = int(result.get("errors", 0) or 0)
            if result.get("skipped"):
                status = "skipped"
            elif errors > 0 and recovered == 0:
                status = "failed"
            elif result.get("degraded") or errors > 0:
                status = "degraded"
            else:
                status = "success" if recovered > 0 else "noop"
            return StepResult(
                step_name="encumbrance_recovery",
                status=status,
                updated=recovered,
                errors=errors,
                details=result,
            )
        finally:
            self._encumbrance_audit_report = None

    def _run_final_refresh(self) -> StepResult:
        """Re-run foreclosure refresh to pick up Phase B data."""
        from src.scripts.refresh_foreclosures import refresh as _refresh

        counts = _refresh(dsn=self.dsn)
        rows = self._int_from_paths(
            counts,
            "enriched",
            "strap_resolved",
            "coords_enriched",
            "resale",
            "events_inserted",
            "encumbrances",
            "archived",
            "judgments",
            "rescheduled_reused",
        )
        return StepResult(
            step_name="final_refresh",
            status="success" if rows > 0 else "noop",
            updated=rows,
            details={"update": counts},
        )

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
            table_count = (
                conn.execute(
                    text(
                        """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_name IN ('hcpa_bulk_parcels', 'hcpa_allsales')
                    """
                    )
                ).scalar()
                or 0
            )
            state["tables_exist"] = table_count == 2
            if not state["tables_exist"]:
                return state

            state["parcels_count"] = conn.execute(text("SELECT COUNT(*) FROM hcpa_bulk_parcels")).scalar() or 0
            state["sales_count"] = conn.execute(text("SELECT COUNT(*) FROM hcpa_allsales")).scalar() or 0
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
            table_count = (
                conn.execute(
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
                ).scalar()
                or 0
            )
            state["tables_exist"] = table_count == 3
            if not state["tables_exist"]:
                return state

            state["filings_count"] = conn.execute(text("SELECT COUNT(*) FROM sunbiz_entity_filings")).scalar() or 0
            state["parties_count"] = conn.execute(text("SELECT COUNT(*) FROM sunbiz_entity_parties")).scalar() or 0
            state["events_count"] = conn.execute(text("SELECT COUNT(*) FROM sunbiz_entity_events")).scalar() or 0
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
            exists = (
                conn.execute(
                    text(
                        """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_name = :table_name
                    """
                    ),
                    {"table_name": table_name},
                ).scalar()
                or 0
            )
            state["table_exists"] = exists > 0
            if not state["table_exists"]:
                return state

            state["row_count"] = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar() or 0
            state["latest_at"] = conn.execute(text(f"SELECT MAX({time_col}) FROM {table_name}")).scalar()
        return state

    def _missing_tables(self, table_names: list[str]) -> list[str]:
        missing: list[str] = []
        with self.engine.connect() as conn:
            for table_name in table_names:
                exists = (
                    conn.execute(
                        text(
                            """
                            SELECT COUNT(*)
                            FROM information_schema.tables
                            WHERE table_schema = 'public'
                              AND table_name = :table_name
                            """
                        ),
                        {"table_name": table_name},
                    ).scalar()
                    or 0
                )
                if exists == 0:
                    missing.append(table_name)
        return missing

    def _select_single_pin_permit_candidates(self, *, limit: int) -> list[dict[str, Any]]:
        where_clauses = ["1=1"]
        params: dict[str, Any] = {"pin_limit": max(1, int(limit))}

        if self.settings.foreclosure_id is not None:
            where_clauses.append("f.foreclosure_id = :foreclosure_id")
            params["foreclosure_id"] = self.settings.foreclosure_id
        if self.settings.case_number:
            where_clauses.append("f.case_number_raw = :case_number")
            params["case_number"] = self.settings.case_number
        # Always exclude archived rows for permit scraping
        where_clauses.append("f.archived_at IS NULL")

        scope_limit_clause = ""
        if self.settings.limit and self.settings.limit > 0:
            scope_limit_clause = "LIMIT :scope_limit"
            params["scope_limit"] = int(self.settings.limit)

        sql = text(
            f"""
            WITH scope AS (
                SELECT
                    f.foreclosure_id,
                    f.case_number_raw,
                    COALESCE(NULLIF(btrim(f.strap), ''), bp.strap) AS pin,
                    COALESCE(NULLIF(btrim(f.folio), ''), bp.folio) AS folio,
                    COALESCE(NULLIF(btrim(f.property_address), ''), bp.property_address) AS property_address
                FROM foreclosures f
                LEFT JOIN LATERAL (
                    SELECT bp2.folio, bp2.strap, bp2.property_address
                    FROM hcpa_bulk_parcels bp2
                    WHERE (f.strap IS NOT NULL AND bp2.strap = f.strap)
                       OR (f.folio IS NOT NULL AND bp2.folio = f.folio)
                    ORDER BY bp2.source_file_id DESC NULLS LAST
                    LIMIT 1
                ) bp ON TRUE
                WHERE {" AND ".join(where_clauses)}
                ORDER BY f.foreclosure_id
                {scope_limit_clause}
            ),
            scored AS (
                SELECT
                    s.foreclosure_id,
                    s.case_number_raw,
                    s.pin,
                    s.folio,
                    s.property_address,
                    CASE
                        WHEN upper(COALESCE(s.property_address, '')) LIKE '%,%TEMPLE TERRACE%'
                            THEN 'temple_terrace'
                        WHEN upper(COALESCE(s.property_address, '')) LIKE '%,%PLANT CITY%'
                            THEN 'plant_city'
                        WHEN upper(COALESCE(s.property_address, '')) LIKE '%,%TAMPA%'
                            THEN 'tampa'
                        ELSE 'county'
                    END AS jurisdiction_guess,
                    COALESCE(cp.county_total, 0) AS county_total,
                    COALESCE(cp.county_with_value, 0) AS county_with_value,
                    COALESCE(tp.tampa_total, 0) AS tampa_total,
                    COALESCE(tp.tampa_with_value, 0) AS tampa_with_value
                FROM scope s
                LEFT JOIN LATERAL (
                    SELECT
                        COUNT(*) AS county_total,
                        COUNT(*) FILTER (WHERE cp.permit_value IS NOT NULL) AS county_with_value
                    FROM county_permits cp
                    WHERE s.folio IS NOT NULL
                      AND regexp_replace(
                            COALESCE(cp.folio_clean, cp.folio_raw, ''),
                            '[^0-9]',
                            '',
                            'g'
                          ) = regexp_replace(s.folio, '[^0-9]', '', 'g')
                ) cp ON TRUE
                LEFT JOIN LATERAL (
                    SELECT
                        COUNT(*) FILTER (
                            WHERE COALESCE(tr.is_violation, FALSE) = FALSE
                              AND COALESCE(tr.module, '') <> 'Business'
                              AND COALESCE(tr.record_number, '') NOT LIKE 'BTX-%'
                              AND COALESCE(tr.record_type, '') NOT ILIKE 'Tax Receipt%'
                        ) AS tampa_total,
                        COUNT(*) FILTER (
                            WHERE COALESCE(tr.is_violation, FALSE) = FALSE
                              AND COALESCE(tr.module, '') <> 'Business'
                              AND COALESCE(tr.record_number, '') NOT LIKE 'BTX-%'
                              AND COALESCE(tr.record_type, '') NOT ILIKE 'Tax Receipt%'
                              AND tr.estimated_work_cost IS NOT NULL
                        ) AS tampa_with_value
                    FROM tampa_accela_records tr
                    WHERE s.property_address IS NOT NULL
                      AND btrim(s.property_address) <> ''
                      AND btrim(COALESCE(tr.address_normalized, tr.address_raw, '')) <> ''
                      AND upper(trim(
                            split_part(
                                replace(COALESCE(tr.address_normalized, tr.address_raw, ''), E'\\t', ' '),
                                ',',
                                1
                            )
                        )) = upper(trim(split_part(replace(s.property_address, E'\\t', ' '), ',', 1)))
                      AND (
                            NULLIF(btrim(split_part(s.property_address, ',', 2)), '') IS NULL
                         OR (
                                NULLIF(btrim(tr.city), '') IS NOT NULL
                            AND upper(btrim(tr.city)) = upper(btrim(split_part(s.property_address, ',', 2)))
                         )
                         OR (
                                NULLIF(
                                    btrim(
                                        split_part(
                                            COALESCE(tr.address_normalized, tr.address_raw, ''),
                                            ',',
                                            2
                                        )
                                    ),
                                    ''
                                ) IS NOT NULL
                            AND upper(
                                    btrim(
                                        split_part(
                                            COALESCE(tr.address_normalized, tr.address_raw, ''),
                                            ',',
                                            2
                                        )
                                    )
                                ) = upper(btrim(split_part(s.property_address, ',', 2)))
                         )
                      )
                ) tp ON TRUE
            )
            SELECT DISTINCT ON (pin)
                foreclosure_id,
                case_number_raw,
                pin,
                folio,
                property_address,
                jurisdiction_guess,
                county_total,
                county_with_value,
                tampa_total,
                tampa_with_value
            FROM scored
            WHERE pin IS NOT NULL
              AND btrim(pin) <> ''
              AND (
                    (county_total + tampa_total) = 0
                 OR (county_with_value + tampa_with_value) = 0
              )
            ORDER BY pin, foreclosure_id
            LIMIT :pin_limit
            """
        )

        with self.engine.connect() as conn:
            rows = conn.execute(sql, params).mappings().all()
        return [dict(r) for r in rows]

    def _resolve_tampa_window(self) -> tuple[dt.date, dt.date]:
        if self.settings.tampa_start_date and self.settings.tampa_end_date:
            if self.settings.tampa_end_date < self.settings.tampa_start_date:
                raise ValueError("tampa_end_date must be >= tampa_start_date")
            return self.settings.tampa_start_date, self.settings.tampa_end_date

        today = dt.datetime.now(dt.UTC).date()
        fallback_start = today - dt.timedelta(days=self.settings.tampa_lookback_days)

        # Incremental: start from last record_date in DB (with 1-day overlap)
        # instead of re-downloading the full 30-day lookback every run.
        latest_record = None
        try:
            with self.engine.connect() as conn:
                latest_record = conn.execute(text("SELECT MAX(record_date) FROM tampa_accela_records")).scalar()
        except Exception as exc:
            logger.debug(
                "Tampa permits: unable to read tampa_accela_records max record_date yet: {}",
                exc,
            )

        if latest_record is not None:
            incremental_start = latest_record - dt.timedelta(days=1)
            start = max(incremental_start, fallback_start)
            window_days = (today - start).days
            logger.info(
                "Tampa permits: last record_date in DB is {}, fetching {} days ({} -> {})",
                latest_record,
                window_days,
                start,
                today,
            )
        else:
            start = fallback_start
            window_days = (today - start).days
            logger.info(
                "Tampa permits: no existing data, full {} day lookback ({} -> {})",
                window_days,
                start,
                today,
            )

        return start, today

    def _get_county_max_object_id(self) -> int | None:
        try:
            with self.engine.connect() as conn:
                return conn.execute(text("SELECT MAX(source_object_id) FROM county_permits")).scalar()
        except Exception as exc:
            logger.debug(
                "County permits: unable to read county_permits max source_object_id yet: {}",
                exc,
            )
            return None

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
    def _coverage_pct(num: int, den: int) -> float | None:
        if den <= 0:
            return None
        return round((100.0 * num) / den, 2)

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
    def _market_work_units(payload: Any) -> int:
        """Count observable market-data work from worker/service payloads."""
        if not isinstance(payload, dict):
            return 0
        update = payload.get("update")
        result = update if isinstance(update, dict) else payload
        keys = (
            "redfin",
            "zillow",
            "realtor",
            "homeharvest",
            "photos",
            "detail_url_repaired",
        )
        total = 0
        for key in keys:
            try:
                total += int(result.get(key, 0) or 0)
            except (TypeError, ValueError):
                continue
        return total

    @staticmethod
    def _int_from_paths(payload: Any, *paths: str) -> int:
        total = 0
        for path in paths:
            value: Any = payload
            for segment in path.split("."):
                if not isinstance(value, dict):
                    value = None
                    break
                value = value.get(segment)
            try:
                total += int(value or 0)
            except (TypeError, ValueError):
                continue
        return total

    @classmethod
    def _max_int_from_paths(cls, payload: Any, *paths: str) -> int:
        return max((cls._int_from_paths(payload, path) for path in paths), default=0)

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
    parser = argparse.ArgumentParser(description="PG-first controller: update loaders + foreclosure refresh + title chain")
    parser.add_argument("--dsn", help="PostgreSQL DSN override")
    parser.add_argument("--force-all", action="store_true", help="Force all loaders to run")
    parser.add_argument("--fail-fast", action="store_true", help="Stop after first failure")
    parser.add_argument(
        "--background-bulk-steps",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Run selected bulk ingestion steps in background worker processes (default: inline).",
    )
    parser.add_argument(
        "--background-market-data",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Run market-data step in a detached worker process (default: inline).",
    )

    parser.add_argument("--skip-hcpa", action="store_true")
    parser.add_argument("--skip-clerk-bulk", action="store_true")
    parser.add_argument("--skip-clerk-criminal", action="store_true")
    parser.add_argument("--skip-clerk-civil-alpha", action="store_true")
    parser.add_argument("--skip-nal", action="store_true")
    parser.add_argument("--skip-flr", action="store_true")
    parser.add_argument("--skip-sunbiz-entity", action="store_true")
    parser.add_argument("--skip-county-permits", action="store_true")
    parser.add_argument("--skip-tampa-permits", action="store_true")
    parser.add_argument("--skip-single-pin-permits", action="store_true")
    parser.add_argument("--skip-foreclosure-refresh", action="store_true")
    parser.add_argument("--skip-final-refresh", action="store_true")
    parser.add_argument("--skip-trust-accounts", action="store_true")
    parser.add_argument("--skip-title-chain", action="store_true")
    parser.add_argument("--skip-title-breaks", action="store_true")
    parser.add_argument("--skip-market-data", action="store_true")
    parser.add_argument(
        "--use-windows-chrome", action="store_true", help="Connect to Windows Chrome via CDP for Realtor scraping"
    )
    # Phase B toggles
    parser.add_argument("--skip-auction-scrape", action="store_true")
    parser.add_argument("--skip-judgment-extract", action="store_true")
    parser.add_argument("--skip-identifier-recovery", action="store_true")
    parser.add_argument("--skip-ori-search", action="store_true")
    parser.add_argument("--skip-municipal-liens", action="store_true")
    parser.add_argument("--skip-mortgage-extract", action="store_true")
    parser.add_argument("--skip-survival", action="store_true")
    parser.add_argument("--skip-encumbrance-audit", action="store_true")
    parser.add_argument("--skip-encumbrance-recovery", action="store_true")

    parser.add_argument("--hcpa-download-dir", default=str(DEFAULT_HCPA_DOWNLOAD_DIR))
    parser.add_argument(
        "--include-hcpa-latlon",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include HCPA lat/lon file during HCPA load (use --no-include-hcpa-latlon to disable).",
    )

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
    parser.add_argument(
        "--single-pin-permit-limit",
        type=int,
        default=25,
        help="Max foreclosure properties to target in single-pin permit fallback (<=0 disables step).",
    )
    parser.add_argument(
        "--single-pin-permit-max-permits",
        type=int,
        default=0,
        help="Per-pin cap on permits to process (<=0 means all permits from parcel payload).",
    )
    parser.add_argument(
        "--single-pin-permit-timeout-seconds",
        type=int,
        default=45,
        help="HTTP timeout per request for single-pin permit fallback.",
    )

    parser.add_argument("--foreclosure-id", type=int)
    parser.add_argument("--case-number")
    parser.add_argument(
        "--active-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Filter to active (non-archived) foreclosures (default: True; use --no-active-only to include archived)",
    )
    parser.add_argument("--limit", type=int)
    parser.add_argument("--similarity-threshold", type=float, default=0.68)

    # Phase B limits
    parser.add_argument("--auction-limit", type=int, help="Max auctions per date to scrape")
    parser.add_argument("--judgment-limit", type=int, help="Max PDFs to extract")
    parser.add_argument(
        "--identifier-recovery-limit",
        type=int,
        help="Max unresolved foreclosures for identifier recovery (<=0 means all)",
    )
    parser.add_argument("--ori-limit", type=int, help="Max foreclosures for ORI search")
    parser.add_argument("--mortgage-limit", type=int, help="Max mortgage PDFs to extract")
    parser.add_argument("--survival-limit", type=int, help="Max foreclosures for survival analysis")
    parser.add_argument("--title-breaks-limit", type=int, help="Max foreclosures for title break resolution")

    args = parser.parse_args()

    return ControllerSettings(
        dsn=args.dsn,
        force_all=bool(args.force_all),
        fail_fast=bool(args.fail_fast),
        background_bulk_steps=bool(args.background_bulk_steps),
        background_market_data=bool(args.background_market_data),
        skip_hcpa=bool(args.skip_hcpa),
        skip_clerk_bulk=bool(args.skip_clerk_bulk),
        skip_clerk_criminal=bool(args.skip_clerk_criminal),
        skip_clerk_civil_alpha=bool(args.skip_clerk_civil_alpha),
        skip_nal=bool(args.skip_nal),
        skip_flr=bool(args.skip_flr),
        skip_sunbiz_entity=bool(args.skip_sunbiz_entity),
        skip_county_permits=bool(args.skip_county_permits),
        skip_tampa_permits=bool(args.skip_tampa_permits),
        skip_single_pin_permits=bool(args.skip_single_pin_permits),
        skip_foreclosure_refresh=bool(args.skip_foreclosure_refresh),
        skip_final_refresh=bool(args.skip_final_refresh),
        skip_trust_accounts=bool(args.skip_trust_accounts),
        skip_title_chain=bool(args.skip_title_chain),
        skip_title_breaks=bool(args.skip_title_breaks),
        skip_market_data=bool(args.skip_market_data),
        use_windows_chrome=bool(args.use_windows_chrome),
        skip_auction_scrape=bool(args.skip_auction_scrape),
        skip_judgment_extract=bool(args.skip_judgment_extract),
        skip_identifier_recovery=bool(args.skip_identifier_recovery),
        skip_ori_search=bool(args.skip_ori_search),
        skip_municipal_liens=bool(args.skip_municipal_liens),
        skip_mortgage_extract=bool(args.skip_mortgage_extract),
        skip_survival=bool(args.skip_survival),
        skip_encumbrance_audit=bool(args.skip_encumbrance_audit),
        skip_encumbrance_recovery=bool(args.skip_encumbrance_recovery),
        hcpa_download_dir=Path(args.hcpa_download_dir),
        include_hcpa_latlon=args.include_hcpa_latlon,
        sunbiz_data_dir=Path(args.sunbiz_data_dir),
        sunbiz_manifest=Path(args.sunbiz_manifest),
        county_where=args.county_where,
        county_page_size=args.county_page_size,
        tampa_lookback_days=args.tampa_lookback_days,
        tampa_start_date=_parse_date(args.tampa_start_date),
        tampa_end_date=_parse_date(args.tampa_end_date),
        tampa_keep_csv=bool(args.tampa_keep_csv),
        tampa_enrich_limit=args.tampa_enrich_limit,
        single_pin_permit_limit=args.single_pin_permit_limit,
        single_pin_permit_max_permits=args.single_pin_permit_max_permits,
        single_pin_permit_timeout_seconds=args.single_pin_permit_timeout_seconds,
        foreclosure_id=args.foreclosure_id,
        case_number=args.case_number,
        active_only=bool(args.active_only),
        limit=args.limit,
        similarity_threshold=args.similarity_threshold,
        auction_limit=args.auction_limit,
        judgment_limit=args.judgment_limit,
        identifier_recovery_limit=args.identifier_recovery_limit,
        ori_limit=args.ori_limit,
        mortgage_limit=args.mortgage_limit,
        survival_limit=args.survival_limit,
        title_breaks_limit=args.title_breaks_limit,
    )
