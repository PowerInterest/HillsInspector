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
from src.services.pg_permit_single_pin_service import PgPermitSinglePinService
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
    background_bulk_steps: bool = False
    background_market_data: bool = False
    # Step toggles
    skip_hcpa: bool = False
    skip_clerk_bulk: bool = False
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
    skip_mortgage_extract: bool = False
    skip_survival: bool = False
    # Market Data specific
    use_windows_chrome: bool = False
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
        "dor_nal",
        "sunbiz_flr",
        "sunbiz_entity",
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
                "mortgage_extract",
                self.settings.skip_mortgage_extract,
                self._run_mortgage_extract,
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
            status = "failed" if failed else "skipped" if payload_dict.get("skipped") else "ok"
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
        if payload.get("error") not in {None, ""}:
            return True, "error_present"

        update = payload.get("update")
        if isinstance(update, dict):
            if update.get("success") is False:
                return True, "update_success_false"
            if update.get("error") not in {None, ""}:
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

        # Incremental: only fetch ArcGIS records newer than what we already have.
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
            logger.info(
                "County permits: no existing data or force mode, full ArcGIS layer pull"
            )

        svc = CountyPermitService(
            page_size=self.settings.county_page_size,
            pg_dsn=self.dsn,
        )
        stats = svc.sync_postgres(
            where=where,
            clear_existing=False,
            page_size=self.settings.county_page_size,
        )
        return {"state_before": state, "max_oid_before": max_oid, "update": stats}

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

        # Guardrail: a multi-day window should not silently "succeed" with zero
        # ingested rows unless it truly had no records (handled by service logs).
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

        return {
            "state_before": state,
            "window": {"start_date": start_date, "end_date": end_date},
            "sync": sync_stats,
            "enrich": enrich_stats,
        }

    def _run_single_pin_permits(self) -> dict[str, Any]:
        if self.settings.single_pin_permit_limit <= 0:
            return {"skipped": True, "reason": "disabled_limit"}

        missing_tables = self._missing_tables(
            [
                "foreclosures",
                "hcpa_bulk_parcels",
                "county_permits",
                "tampa_accela_records",
            ]
        )
        if missing_tables:
            raise RuntimeError(
                "single_pin_permits requires missing table(s): "
                + ", ".join(sorted(missing_tables))
            )

        candidates = self._select_single_pin_permit_candidates(
            limit=self.settings.single_pin_permit_limit
        )
        if not candidates:
            return {"skipped": True, "reason": "no_gaps_detected"}

        pins = [
            str(row.get("pin")).strip()
            for row in candidates
            if row.get("pin") is not None and str(row.get("pin")).strip()
        ]
        pins = list(dict.fromkeys(pins))
        if not pins:
            return {"skipped": True, "reason": "no_valid_pins"}

        max_permits = (
            self.settings.single_pin_permit_max_permits
            if self.settings.single_pin_permit_max_permits > 0
            else None
        )
        svc = PgPermitSinglePinService(
            dsn=self.dsn,
            timeout_seconds=max(5, self.settings.single_pin_permit_timeout_seconds),
            include_accela=True,
            include_arcgis=True,
        )
        sync_stats = svc.sync_pins_to_postgres(
            pins,
            max_permits_per_pin=max_permits,
            fail_on_pin_error=True,
        )

        permits_observed = int(sync_stats.get("permits_observed_total") or 0)
        total_writes = int(sync_stats.get("total_writes") or 0)
        if permits_observed > 0 and total_writes == 0:
            raise RuntimeError(
                "single_pin_permits observed permit rows but wrote nothing to permit tables; "
                "failing to avoid silent no-op."
            )

        return {
            "candidate_count": len(candidates),
            "pins_targeted": len(pins),
            "candidate_sample": candidates[:10],
            "update": sync_stats,
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
                "success": False,
                "error": "service_unavailable",
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

    def _run_title_breaks(self) -> dict[str, Any]:
        from src.services.pg_title_break_service import PgTitleBreakService

        svc = PgTitleBreakService(dsn=self.dsn)
        return svc.run(limit=self.settings.title_breaks_limit)

    def _run_market_data(self) -> dict[str, Any]:
        if self.settings.background_market_data:
            from src.services.market_data_dispatcher import dispatch_market_data_worker

            return dispatch_market_data_worker(dsn=self.dsn, use_windows_chrome=self.settings.use_windows_chrome)

        from src.services.market_data_worker import run_market_data_update

        return run_market_data_update(dsn=self.dsn, use_windows_chrome=self.settings.use_windows_chrome)

    def _should_dispatch_bulk_step(self, name: str) -> bool:
        return self.settings.background_bulk_steps and name in self.BACKGROUND_BULK_STEPS

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

    def _run_identifier_recovery(self) -> dict[str, Any]:
        from src.services.pg_foreclosure_identifier_recovery_service import (
            PgForeclosureIdentifierRecoveryService,
        )

        svc = PgForeclosureIdentifierRecoveryService(dsn=self.dsn)
        if not svc.available:
            return {"skipped": True, "reason": "service_unavailable"}
        return svc.run(limit=self.settings.identifier_recovery_limit)

    def _run_ori_search(self) -> dict[str, Any]:
        from src.services.pg_ori_service import PgOriService

        svc = PgOriService(dsn=self.dsn)
        return svc.run(limit=self.settings.ori_limit)

    def _run_mortgage_extract(self) -> dict[str, Any]:
        from src.services.pg_mortgage_extraction_service import PgMortgageExtractionService

        svc = PgMortgageExtractionService(dsn=self.dsn)
        return svc.run(limit=self.settings.mortgage_limit)

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
                        COUNT(*) AS tampa_total,
                        COUNT(*) FILTER (WHERE tr.estimated_work_cost IS NOT NULL) AS tampa_with_value
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
                ) tp ON TRUE
            )
            SELECT DISTINCT ON (pin)
                foreclosure_id,
                case_number_raw,
                pin,
                folio,
                property_address,
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
                latest_record = conn.execute(
                    text("SELECT MAX(record_date) FROM tampa_accela_records")
                ).scalar()
        except Exception:
            pass  # Table may not exist yet on fresh DB

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
                return conn.execute(
                    text("SELECT MAX(source_object_id) FROM county_permits")
                ).scalar()
        except Exception:
            return None  # Table may not exist yet on fresh DB

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
    parser.add_argument("--skip-mortgage-extract", action="store_true")
    parser.add_argument("--skip-survival", action="store_true")

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
        skip_mortgage_extract=bool(args.skip_mortgage_extract),
        skip_survival=bool(args.skip_survival),
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
        survival_limit=args.survival_limit,
        title_breaks_limit=args.title_breaks_limit,
    )
