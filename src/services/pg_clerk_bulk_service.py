"""
PostgreSQL Clerk Bulk Data Service -- standalone sync + load.

Downloads monthly civil case/event/party CSVs from the Hillsborough County
Clerk of Court bulk data page and loads them into PostgreSQL.
Also downloads disposed-case and return-of-service/garnishment CSV feeds.

Orchestrator usage (fire-and-forget)::

    from src.services.pg_clerk_bulk_service import PgClerkBulkService

    service = PgClerkBulkService()
    if service.available:
        stats = service.update()
        logger.info(f"Clerk bulk update: {stats}")

The service is idempotent -- it detects which files are already loaded (by
SHA-256) and only downloads/loads new or changed files.
"""

from __future__ import annotations

import datetime as dt
import traceback
from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from sunbiz.db import get_engine, resolve_pg_dsn


# Default directory for clerk bulk CSV downloads
DEFAULT_CLERK_DIR = Path("data/bulk_data/clerk_civil")


class PgClerkBulkService:
    """Standalone service: sync + load Hillsborough Clerk civil bulk data.

    Lifecycle:
        1. ``__init__`` -- connects to PG, verifies tables exist.
        2. ``get_current_state()`` -- queries ingest_files for what is loaded.
        3. ``update()`` -- downloads new CSVs, loads into PG, returns stats.
    """

    def __init__(self, dsn: str | None = None, download_dir: Path | None = None):
        self._available = False
        self._engine = None
        self._dsn: str = ""
        self.download_dir = download_dir or DEFAULT_CLERK_DIR

        try:
            self._dsn = resolve_pg_dsn(dsn)
            self._engine = get_engine(self._dsn)
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self._available = True
            logger.info("PgClerkBulkService: PostgreSQL connected")
        except SQLAlchemyError as exc:
            logger.error(
                f"PgClerkBulkService: PostgreSQL connection failed: {exc}\n"
                f"  DSN: {self._dsn!r}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
        except Exception as exc:
            logger.error(
                f"PgClerkBulkService: unexpected init error: {exc}\n"
                f"  Traceback: {traceback.format_exc()}"
            )

    @property
    def available(self) -> bool:
        return self._available

    # ------------------------------------------------------------------
    # State introspection
    # ------------------------------------------------------------------

    def get_current_state(self) -> dict[str, Any]:
        """Query PG for the current state of clerk bulk data.

        Returns dict with:
            tables_exist: bool
            cases_count: int
            events_count: int
            parties_count: int
            disposed_count: int
            garnishment_count: int
            latest_loaded_at: datetime | None
            loaded_files: list[dict] (source_file, status, loaded_at, row_count)
        """
        state: dict[str, Any] = {
            "tables_exist": False,
            "cases_count": 0,
            "events_count": 0,
            "parties_count": 0,
            "disposed_count": 0,
            "garnishment_count": 0,
            "name_index_count": 0,
            "latest_loaded_at": None,
            "loaded_files": [],
        }
        if not self._available:
            logger.warning("PgClerkBulkService.get_current_state: PG unavailable")
            return state

        try:
            with self._engine.connect() as conn:
                # Check if tables exist
                row = conn.execute(
                    text("""
                        SELECT COUNT(*) FROM information_schema.tables
                        WHERE table_name IN (
                            'clerk_civil_cases', 'clerk_civil_events',
                            'clerk_civil_parties', 'clerk_disposed_cases',
                            'clerk_garnishment_cases', 'clerk_name_index'
                        )
                    """)
                ).scalar()
                state["tables_exist"] = (row or 0) >= 5

                if not state["tables_exist"]:
                    logger.info(
                        "PgClerkBulkService: clerk tables not yet created "
                        "(will be created on first load)"
                    )
                    return state

                # Row counts
                for table, key in [
                    ("clerk_civil_cases", "cases_count"),
                    ("clerk_civil_events", "events_count"),
                    ("clerk_civil_parties", "parties_count"),
                    ("clerk_disposed_cases", "disposed_count"),
                    ("clerk_garnishment_cases", "garnishment_count"),
                    ("clerk_name_index", "name_index_count"),
                ]:
                    try:
                        count = conn.execute(
                            text(f"SELECT COUNT(*) FROM {table}")
                        ).scalar()
                        state[key] = count or 0
                    except SQLAlchemyError as exc:
                        logger.debug(f"PgClerkBulkService: count {table} failed: {exc}")

                # Latest load info from ingest_files
                try:
                    rows = conn.execute(
                        text("""
                            SELECT relative_path, status, loaded_at, row_count
                            FROM ingest_files
                            WHERE source_system = 'clerk_civil'
                            ORDER BY loaded_at DESC NULLS LAST
                            LIMIT 20
                        """)
                    ).fetchall()
                    for r in rows:
                        state["loaded_files"].append({
                            "relative_path": r[0],
                            "status": r[1],
                            "loaded_at": r[2],
                            "row_count": r[3],
                        })
                    if rows and rows[0][2]:
                        state["latest_loaded_at"] = rows[0][2]
                except SQLAlchemyError as exc:
                    logger.debug(
                        f"PgClerkBulkService: ingest_files query failed: {exc}"
                    )

        except SQLAlchemyError as exc:
            logger.error(
                f"PgClerkBulkService.get_current_state: PG read failed: {exc}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
        except Exception as exc:
            logger.error(
                f"PgClerkBulkService.get_current_state: unexpected error: {exc}\n"
                f"  Traceback: {traceback.format_exc()}"
            )

        logger.info(
            f"PgClerkBulkService state: "
            f"cases={state['cases_count']:,}, "
            f"events={state['events_count']:,}, "
            f"parties={state['parties_count']:,}, "
            f"disposed={state['disposed_count']:,}, "
            f"garnishment={state['garnishment_count']:,}, "
            f"loaded_files={len(state['loaded_files'])}"
        )
        return state

    # ------------------------------------------------------------------
    # Update
    # ------------------------------------------------------------------

    def update(self, force_download: bool = False) -> dict[str, Any]:
        """Download new clerk bulk CSVs and load into PostgreSQL.

        This is the orchestrator entry point -- fire and forget.

        Args:
            force_download: Re-download files even if they exist locally.

        Returns:
            Stats dict with download and load results.
        """
        result: dict[str, Any] = {
            "success": False,
            "download": {},
            "cases": {},
            "events": {},
            "parties": {},
            "disposed": {},
            "garnishment": {},
            "error": None,
        }

        if not self._available:
            result["error"] = "PostgreSQL unavailable"
            logger.error(f"PgClerkBulkService.update: {result['error']}")
            return result

        # Import loader functions (deferred to avoid circular imports)
        try:
            from src.services.pg_loader_clerk import (
                download_clerk_bulk,
                init_db,
                load_clerk_cases,
                load_clerk_events,
                load_clerk_parties,
                load_clerk_disposed,
                load_clerk_garnishment,
                load_clerk_name_index,
            )
        except ImportError as exc:
            result["error"] = f"Failed to import pg_loader_clerk: {exc}"
            logger.error(
                f"PgClerkBulkService.update: {result['error']}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
            return result

        started_at = dt.datetime.now(dt.UTC)
        logger.info(
            f"PgClerkBulkService.update: starting "
            f"(download_dir={self.download_dir}, force={force_download})"
        )

        # Step 1: Ensure tables exist
        try:
            init_db(self._dsn)
            logger.info("PgClerkBulkService: tables initialized")
        except Exception as exc:
            result["error"] = f"Table init failed: {exc}"
            logger.error(
                f"PgClerkBulkService.update: {result['error']}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
            return result

        # Step 2: Download latest CSVs from clerk website
        try:
            dl_stats = download_clerk_bulk(
                output_dir=self.download_dir,
                force=force_download,
            )
            result["download"] = dl_stats
            logger.info(
                f"PgClerkBulkService download: "
                f"{dl_stats.get('downloaded', 0)} new, "
                f"{dl_stats.get('skipped', 0)} skipped, "
                f"{dl_stats.get('errors', 0)} errors"
            )
            if dl_stats.get("errors", 0) > 0:
                logger.warning(
                    f"PgClerkBulkService: {dl_stats['errors']} download errors "
                    f"(continuing with available files)"
                )
        except Exception as exc:
            result["error"] = f"Download failed: {exc}"
            logger.error(
                f"PgClerkBulkService.update: download phase failed: {exc}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
            return result

        # Step 3: Load each file type
        load_steps = [
            ("cases", load_clerk_cases),
            ("events", load_clerk_events),
            ("parties", load_clerk_parties),
            ("disposed", load_clerk_disposed),
            ("garnishment", load_clerk_garnishment),
        ]
        all_ok = True
        for step_name, loader_fn in load_steps:
            try:
                logger.info(f"PgClerkBulkService: loading {step_name} ...")
                step_stats = loader_fn(
                    dsn=self._dsn,
                    root=self.download_dir,
                )
                result[step_name] = step_stats
                loaded = step_stats.get("files_loaded", step_stats.get("rows_upserted", 0))
                skipped = step_stats.get("files_skipped", 0)
                logger.info(
                    f"PgClerkBulkService {step_name}: "
                    f"loaded={loaded}, skipped={skipped}"
                )
            except SQLAlchemyError as exc:
                all_ok = False
                result[step_name] = {"error": str(exc)}
                logger.error(
                    f"PgClerkBulkService: PG write failed during {step_name}: {exc}\n"
                    f"  SQL state: {getattr(exc, 'orig', 'N/A')}\n"
                    f"  Traceback: {traceback.format_exc()}"
                )
            except Exception as exc:
                all_ok = False
                result[step_name] = {"error": str(exc)}
                logger.error(
                    f"PgClerkBulkService: {step_name} failed: {exc}\n"
                    f"  Traceback: {traceback.format_exc()}"
                )

        # Step 4: Name index (separate directory, not part of bulk download)
        try:
            logger.info("PgClerkBulkService: loading name_index ...")
            ni_stats = load_clerk_name_index(dsn=self._dsn)
            result["name_index"] = ni_stats
            logger.info(
                f"PgClerkBulkService name_index: "
                f"loaded={ni_stats.get('files_loaded', 0)}, "
                f"rows={ni_stats.get('rows_inserted', 0)}"
            )
        except Exception as exc:
            all_ok = False
            result["name_index"] = {"error": str(exc)}
            logger.error(
                f"PgClerkBulkService: name_index failed: {exc}\n"
                f"  Traceback: {traceback.format_exc()}"
            )

        elapsed = (dt.datetime.now(dt.UTC) - started_at).total_seconds()
        result["success"] = all_ok
        result["elapsed_seconds"] = round(elapsed, 1)

        if all_ok:
            logger.success(
                f"PgClerkBulkService.update completed in {elapsed:.1f}s: "
                f"cases={result['cases'].get('rows_upserted', '?')}, "
                f"events={result['events'].get('rows_inserted', '?')}, "
                f"parties={result['parties'].get('rows_upserted', '?')}, "
                f"garnishment={result['garnishment'].get('rows_inserted', '?')}"
            )
        else:
            logger.warning(
                f"PgClerkBulkService.update completed with errors in {elapsed:.1f}s"
            )

        return result
