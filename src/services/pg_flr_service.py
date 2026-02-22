"""
PostgreSQL Sunbiz FLR Service -- standalone sync + load for UCC/FLR bulk data.

Syncs FLR (Filing, Debtor, Secured, Event) files from the Florida Secretary of
State SFTP server and loads them into PostgreSQL.

The FLR dataset provides:
- UCC financing statement filings (liens against personal property)
- Federal lien registrations
- Debtor and secured party information
- Amendment, continuation, and termination events

This data feeds the UCC lien exposure analysis in the pipeline: if a foreclosure
defendant has active UCC liens, those liens may affect equity estimates.

Orchestrator usage (fire-and-forget)::

    from src.services.pg_flr_service import PgFlrService

    service = PgFlrService()
    if service.available:
        stats = service.update()
        logger.info(f"Sunbiz FLR update: {stats}")

The service detects which FLR files are already loaded (by SHA-256 in
ingest_files) and only downloads/loads new or changed files.

Prerequisites:
    - paramiko installed (``uv add paramiko``) for SFTP access
    - FLR files are on the public Florida DOS SFTP at sftp.floridados.gov
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


# Default root for synced Sunbiz bulk files
DEFAULT_SUNBIZ_DIR = Path("data/sunbiz")
# FLR files live under public/doc/quarterly/FLR/ on the SFTP
FLR_RELATIVE_DIR = Path("public/doc/quarterly")
EXPECTED_FLR_ZIPS = {"flrf.zip", "flrd.zip", "flrs.zip", "flre.zip"}


class PgFlrService:
    """Standalone service: sync FLR from SFTP + load into PostgreSQL.

    Lifecycle:
        1. ``__init__`` -- connects to PG, verifies connectivity.
        2. ``get_current_state()`` -- queries PG for loaded FLR data.
        3. ``update()`` -- syncs from SFTP, loads into PG, returns stats.
    """

    def __init__(self, dsn: str | None = None, data_dir: Path | None = None):
        self._available = False
        self._engine = None
        self._dsn: str = ""
        self.data_dir = data_dir or DEFAULT_SUNBIZ_DIR

        try:
            self._dsn = resolve_pg_dsn(dsn)
            self._engine = get_engine(self._dsn)
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self._available = True
            logger.info("PgFlrService: PostgreSQL connected")
        except SQLAlchemyError as exc:
            logger.error(
                f"PgFlrService: PostgreSQL connection failed: {exc}\n"
                f"  DSN: {self._dsn!r}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
        except Exception as exc:
            logger.error(
                f"PgFlrService: unexpected init error: {exc}\n"
                f"  Traceback: {traceback.format_exc()}"
            )

    @property
    def available(self) -> bool:
        return self._available

    # ------------------------------------------------------------------
    # State introspection
    # ------------------------------------------------------------------

    def get_current_state(self) -> dict[str, Any]:
        """Query PG for the current state of Sunbiz FLR data.

        Returns dict with:
            tables_exist: bool
            filings_count: int
            parties_count: int
            events_count: int
            active_filings: int
            latest_filing_date: date | None
            latest_loaded_at: datetime | None
            loaded_files: list[dict]
            local_flr_files: list[str]  (FLR zips found on disk)
        """
        state: dict[str, Any] = {
            "tables_exist": False,
            "filings_count": 0,
            "parties_count": 0,
            "events_count": 0,
            "active_filings": 0,
            "latest_filing_date": None,
            "latest_loaded_at": None,
            "loaded_files": [],
            "local_flr_files": [],
        }

        if not self._available:
            logger.warning("PgFlrService.get_current_state: PG unavailable")
            return state

        # Check local disk for FLR files
        flr_dir = self.data_dir / FLR_RELATIVE_DIR
        if flr_dir.exists():
            for name in sorted(flr_dir.iterdir()):
                if name.name.lower() in EXPECTED_FLR_ZIPS:
                    state["local_flr_files"].append(str(name))
        # Also check in FLR subdirectory
        flr_subdir = flr_dir / "FLR"
        if flr_subdir.exists():
            for name in sorted(flr_subdir.iterdir()):
                if name.name.lower() in EXPECTED_FLR_ZIPS:
                    state["local_flr_files"].append(str(name))

        try:
            with self._engine.connect() as conn:
                # Check table existence
                row = conn.execute(
                    text("""
                        SELECT COUNT(*) FROM information_schema.tables
                        WHERE table_name IN (
                            'sunbiz_flr_filings',
                            'sunbiz_flr_parties',
                            'sunbiz_flr_events'
                        )
                    """)
                ).scalar()
                state["tables_exist"] = (row or 0) >= 3

                if not state["tables_exist"]:
                    logger.info(
                        "PgFlrService: FLR tables not yet created "
                        "(will be created on first load)"
                    )
                    return state

                # Row counts
                for table, key in [
                    ("sunbiz_flr_filings", "filings_count"),
                    ("sunbiz_flr_parties", "parties_count"),
                    ("sunbiz_flr_events", "events_count"),
                ]:
                    try:
                        count = conn.execute(
                            text(f"SELECT COUNT(*) FROM {table}")
                        ).scalar()
                        state[key] = count or 0
                    except SQLAlchemyError as exc:
                        logger.debug(f"PgFlrService: count {table} failed: {exc}")

                # Active filings count
                try:
                    state["active_filings"] = conn.execute(
                        text("""
                            SELECT COUNT(*) FROM sunbiz_flr_filings
                            WHERE filing_status = 'A'
                        """)
                    ).scalar() or 0
                except SQLAlchemyError as exc:
                    logger.debug(f"PgFlrService: active filings count failed: {exc}")

                # Latest filing date
                try:
                    row = conn.execute(
                        text("""
                            SELECT MAX(filing_date) FROM sunbiz_flr_filings
                        """)
                    ).fetchone()
                    if row and row[0]:
                        state["latest_filing_date"] = row[0]
                except SQLAlchemyError as exc:
                    logger.debug(f"PgFlrService: max filing_date query failed: {exc}")

                # Latest load info from ingest_files
                try:
                    rows = conn.execute(
                        text("""
                            SELECT relative_path, status, loaded_at, row_count
                            FROM ingest_files
                            WHERE source_system = 'sunbiz'
                              AND category = 'flr_structured'
                            ORDER BY loaded_at DESC NULLS LAST
                            LIMIT 10
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
                    logger.debug(f"PgFlrService: ingest_files query failed: {exc}")

        except SQLAlchemyError as exc:
            logger.error(
                f"PgFlrService.get_current_state: PG read failed: {exc}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
        except Exception as exc:
            logger.error(
                f"PgFlrService.get_current_state: unexpected error: {exc}\n"
                f"  Traceback: {traceback.format_exc()}"
            )

        logger.info(
            f"PgFlrService state: "
            f"filings={state['filings_count']:,}, "
            f"parties={state['parties_count']:,}, "
            f"events={state['events_count']:,}, "
            f"active={state['active_filings']:,}, "
            f"latest_filing={state['latest_filing_date']}, "
            f"local_files={len(state['local_flr_files'])}"
        )
        return state

    # ------------------------------------------------------------------
    # SFTP Sync
    # ------------------------------------------------------------------

    def _sync_flr_from_sftp(self, force: bool = False) -> dict[str, Any]:
        """Download FLR ZIP files from Florida DOS SFTP.

        Uses sunbiz/sync.py's SunbizMirror for SFTP access.

        Returns dict with: downloaded, skipped, errors, files.
        """
        sync_result: dict[str, Any] = {
            "downloaded": 0,
            "skipped": 0,
            "errors": 0,
            "files": [],
        }

        try:
            from sunbiz.sync import (
                DEFAULT_HOST,
                DEFAULT_MANIFEST,
                DEFAULT_PASSWORD,
                DEFAULT_PORT,
                DEFAULT_USER,
                SunbizMirror,
            )
        except ImportError as exc:
            logger.error(
                f"PgFlrService: Failed to import SunbizMirror "
                f"(is paramiko installed?): {exc}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
            sync_result["errors"] = 1
            return sync_result

        try:
            mirror = SunbizMirror(
                host=DEFAULT_HOST,
                port=DEFAULT_PORT,
                username=DEFAULT_USER,
                password=DEFAULT_PASSWORD,
                data_dir=self.data_dir,
                manifest_path=DEFAULT_MANIFEST,
                recursive=True,
            )
            logger.info(
                f"PgFlrService: syncing FLR files from SFTP "
                f"(data_dir={self.data_dir})"
            )

            mirror.sync(
                mode="quarterly",
                remote_dirs=None,
                include="FLR/flr",
                exclude=None,
                dataset_profile=None,
                modified_since=None,
                max_files=None,
                dry_run=False,
                force=force,
            )

            # Check what we got on disk
            flr_dir = self.data_dir / FLR_RELATIVE_DIR / "FLR"
            if flr_dir.exists():
                for entry in flr_dir.iterdir():
                    if entry.name.lower() in EXPECTED_FLR_ZIPS:
                        sync_result["files"].append(str(entry))
                        sync_result["downloaded"] += 1
            else:
                # Also check parent directory
                parent = self.data_dir / FLR_RELATIVE_DIR
                if parent.exists():
                    for entry in parent.iterdir():
                        if entry.name.lower() in EXPECTED_FLR_ZIPS:
                            sync_result["files"].append(str(entry))
                            sync_result["downloaded"] += 1

            found_names = {Path(f).name.lower() for f in sync_result["files"]}
            missing = EXPECTED_FLR_ZIPS - found_names
            if missing:
                logger.warning(
                    f"PgFlrService: SFTP sync complete but missing expected files: "
                    f"{sorted(missing)}.  "
                    f"Found: {sorted(found_names)}.  "
                    f"Party/event tables may remain empty."
                )

            logger.info(
                f"PgFlrService SFTP sync: "
                f"{len(sync_result['files'])} FLR files available"
            )

        except Exception as exc:
            sync_result["errors"] = 1
            logger.error(
                f"PgFlrService: SFTP sync failed: {exc}\n"
                f"  Traceback: {traceback.format_exc()}"
            )

        return sync_result

    def _find_flr_root(self) -> Path | None:
        """Find the directory containing FLR zip files."""
        candidates = [
            self.data_dir / FLR_RELATIVE_DIR / "FLR",
            self.data_dir / FLR_RELATIVE_DIR,
            self.data_dir / "public" / "doc" / "FLR",
            self.data_dir / "public" / "doc",
        ]
        for d in candidates:
            if d.exists():
                zips = [f for f in d.iterdir() if f.name.lower() in EXPECTED_FLR_ZIPS]
                if zips:
                    logger.debug(f"PgFlrService: FLR root found at {d} ({len(zips)} zips)")
                    return d
        return None

    # ------------------------------------------------------------------
    # Update
    # ------------------------------------------------------------------

    def update(
        self,
        skip_sftp: bool = False,
        force_download: bool = False,
    ) -> dict[str, Any]:
        """Sync FLR files from SFTP and load into PostgreSQL.

        This is the orchestrator entry point -- fire and forget.

        Args:
            skip_sftp: If True, skip SFTP download and only load files
                       already on disk.  Useful when SFTP is unavailable
                       but files were manually placed.
            force_download: Re-download files even if they exist locally.

        Returns:
            Stats dict with sync and load results.
        """
        result: dict[str, Any] = {
            "success": False,
            "sftp_sync": {},
            "load_stats": {},
            "error": None,
        }

        if not self._available:
            result["error"] = "PostgreSQL unavailable"
            logger.error(f"PgFlrService.update: {result['error']}")
            return result

        # Import loader functions
        try:
            from sunbiz.pg_loader import load_sunbiz_flr
        except ImportError as exc:
            result["error"] = f"Failed to import pg_loader: {exc}"
            logger.error(
                f"PgFlrService.update: {result['error']}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
            return result

        started_at = dt.datetime.now(dt.UTC)
        logger.info(
            f"PgFlrService.update: starting "
            f"(skip_sftp={skip_sftp}, force={force_download}, "
            f"data_dir={self.data_dir})"
        )

        # Step 1: Ensure tables exist
        try:
            from sunbiz.models import Base
            engine = get_engine(self._dsn)
            Base.metadata.create_all(bind=engine)
            logger.info("PgFlrService: tables initialized")
        except Exception as exc:
            result["error"] = f"Table init failed: {exc}"
            logger.error(
                f"PgFlrService.update: {result['error']}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
            return result

        # Step 2: Sync from SFTP (unless skipped)
        if not skip_sftp:
            sftp_result = self._sync_flr_from_sftp(force=force_download)
            result["sftp_sync"] = sftp_result
            if sftp_result.get("errors", 0) > 0 and not sftp_result.get("files"):
                # SFTP failed and no files on disk -- check if we have local files
                flr_root = self._find_flr_root()
                if flr_root is None:
                    result["error"] = (
                        "SFTP sync failed and no FLR files found on disk. "
                        "Run 'uv run python sunbiz/sync.py sync --mode quarterly "
                        "--pattern FLR/flr' manually."
                    )
                    logger.error(f"PgFlrService.update: {result['error']}")
                    return result
                logger.warning(
                    "PgFlrService: SFTP failed but found local FLR files, "
                    "proceeding with those"
                )
        else:
            logger.info("PgFlrService: skipping SFTP sync (skip_sftp=True)")

        # Step 3: Find FLR root directory
        flr_root = self._find_flr_root()
        if flr_root is None:
            result["error"] = (
                f"No FLR files found under {self.data_dir}. "
                "Expected flrf.zip, flrd.zip, flrs.zip, flre.zip"
            )
            logger.error(f"PgFlrService.update: {result['error']}")
            return result

        # Log what we found
        found_zips = [
            f.name for f in flr_root.iterdir()
            if f.name.lower() in EXPECTED_FLR_ZIPS
        ]
        missing_zips = EXPECTED_FLR_ZIPS - {z.lower() for z in found_zips}
        logger.info(
            f"PgFlrService: loading from {flr_root}: "
            f"found={sorted(found_zips)}"
        )
        if missing_zips:
            logger.warning(
                f"PgFlrService: missing FLR zips: {sorted(missing_zips)}. "
                f"Party/event data will be incomplete."
            )

        # Step 4: Load into PostgreSQL
        try:
            load_stats = load_sunbiz_flr(
                dsn=self._dsn,
                root=flr_root,
                pattern=None,
                limit_files=None,
                limit_lines=None,
                batch_size=2000,
            )
            result["load_stats"] = load_stats
            result["success"] = True

            elapsed = (dt.datetime.now(dt.UTC) - started_at).total_seconds()
            result["elapsed_seconds"] = round(elapsed, 1)

            logger.success(
                f"PgFlrService.update completed in {elapsed:.1f}s: "
                f"filings={load_stats.get('filings_upserted', 0):,}, "
                f"parties={load_stats.get('parties_inserted', 0):,}, "
                f"events={load_stats.get('events_inserted', 0):,}"
            )

        except SQLAlchemyError as exc:
            result["error"] = f"PG write failed: {exc}"
            logger.error(
                f"PgFlrService.update: PG write failed during load: {exc}\n"
                f"  SQL state: {getattr(exc, 'orig', 'N/A')}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
        except Exception as exc:
            result["error"] = f"Load failed: {exc}"
            logger.error(
                f"PgFlrService.update: load phase failed: {exc}\n"
                f"  Traceback: {traceback.format_exc()}"
            )

        return result
