"""PostgreSQL-backed criminal name index bulk service.

Downloads and loads Hillsborough County Clerk criminal name index data
(Circuit + County) from https://publicrec.hillsclerk.com/Criminal/name_index/.
The index is a pipe-delimited alphabetical listing covering all criminal cases,
including charges, dispositions, sentencing, and party details.

This service follows the same pattern as PgClerkBulkService for civil data.
It is registered as a scheduled job ('clerk_criminal') and runs as a Phase A
bulk step in PgPipelineController.

Orchestrator usage (fire-and-forget)::

    from src.services.pg_clerk_criminal_service import PgClerkCriminalService

    service = PgClerkCriminalService()
    if service.available:
        stats = service.update()
        logger.info(f"Clerk criminal update: {stats}")

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


# Default directory for clerk criminal name index downloads
DEFAULT_CLERK_DIR = Path("data/bulk_data/clerk_criminal_name_index")


class PgClerkCriminalService:
    """Standalone service: sync + load Hillsborough Clerk criminal name index.

    Lifecycle:
        1. ``__init__`` -- connects to PG, verifies connectivity.
        2. ``update()`` -- downloads new TXT files, loads into PG, returns stats.
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
            logger.info("PgClerkCriminalService: PostgreSQL connected")
        except SQLAlchemyError as exc:
            logger.error(
                f"PgClerkCriminalService: PostgreSQL connection failed: {exc}\n"
                f"  DSN: {self._dsn!r}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
        except Exception as exc:
            logger.error(
                f"PgClerkCriminalService: unexpected init error: {exc}\n"
                f"  Traceback: {traceback.format_exc()}"
            )

    @property
    def available(self) -> bool:
        return self._available

    # ------------------------------------------------------------------
    # Update
    # ------------------------------------------------------------------

    def update(self, force_download: bool = False) -> dict[str, Any]:
        """Download new clerk criminal name index files and load into PostgreSQL.

        This is the orchestrator entry point -- fire and forget.

        Args:
            force_download: Re-download files even if they exist locally.

        Returns:
            Stats dict with download and load results.
        """
        result: dict[str, Any] = {
            "success": False,
            "download": {},
            "load": {},
            "error": None,
            "elapsed_seconds": 0.0,
        }

        if not self._available:
            result["error"] = "PostgreSQL unavailable"
            logger.error(f"PgClerkCriminalService.update: {result['error']}")
            return result

        # Import loader functions (deferred to avoid circular imports)
        try:
            from src.services.pg_loader_clerk import (
                download_clerk_criminal_name_index,
                init_db,
                load_clerk_criminal_name_index,
            )
        except ImportError as exc:
            result["error"] = f"Failed to import pg_loader_clerk: {exc}"
            logger.error(
                f"PgClerkCriminalService.update: {result['error']}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
            return result

        started_at = dt.datetime.now(dt.UTC)
        logger.info(
            f"PgClerkCriminalService.update: starting "
            f"(download_dir={self.download_dir}, force={force_download})"
        )

        # Step 1: Ensure tables exist
        try:
            init_db(self._dsn)
            logger.info("PgClerkCriminalService: tables initialized")
        except Exception as exc:
            result["error"] = f"Table init failed: {exc}"
            logger.error(
                f"PgClerkCriminalService.update: {result['error']}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
            return result

        # Step 2: Download latest TXT files from clerk website
        try:
            dl_stats = download_clerk_criminal_name_index(
                output_dir=self.download_dir,
                force=force_download,
            )
            result["download"] = dl_stats
            logger.info(
                f"PgClerkCriminalService download: "
                f"{dl_stats.get('downloaded', 0)} new, "
                f"{dl_stats.get('skipped', 0)} skipped, "
                f"{dl_stats.get('errors', 0)} errors"
            )
            if int(dl_stats.get("errors", 0) or 0) > 0:
                logger.warning(
                    f"PgClerkCriminalService: {dl_stats['errors']} download errors "
                    f"(continuing with available files)"
                )
        except Exception as exc:
            result["error"] = f"Download failed: {exc}"
            logger.error(
                f"PgClerkCriminalService.update: download phase failed: {exc}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
            return result

        # Step 3: Load downloaded files into PostgreSQL
        try:
            logger.info("PgClerkCriminalService: loading criminal name index ...")
            load_stats = load_clerk_criminal_name_index(
                dsn=self._dsn,
                root=self.download_dir,
            )
            result["load"] = load_stats
            logger.info(
                f"PgClerkCriminalService load: "
                f"loaded={load_stats.get('files_loaded', 0)}, "
                f"rows={load_stats.get('rows_inserted', 0)}"
            )
        except SQLAlchemyError as exc:
            result["error"] = f"PG write failed during load: {exc}"
            logger.error(
                f"PgClerkCriminalService: PG write failed during load: {exc}\n"
                f"  SQL state: {getattr(exc, 'orig', 'N/A')}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
            return result
        except Exception as exc:
            result["error"] = f"Load failed: {exc}"
            logger.error(
                f"PgClerkCriminalService: load failed: {exc}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
            return result

        elapsed = (dt.datetime.now(dt.UTC) - started_at).total_seconds()
        download_errors = int(result["download"].get("errors", 0) or 0)
        result["success"] = download_errors == 0
        result["elapsed_seconds"] = round(elapsed, 1)

        if result["success"]:
            logger.success(
                f"PgClerkCriminalService.update completed in {elapsed:.1f}s: "
                f"downloaded={result['download'].get('downloaded', '?')}, "
                f"loaded={result['load'].get('files_loaded', '?')}, "
                f"rows={result['load'].get('rows_inserted', '?')}"
            )
        else:
            logger.warning(
                f"PgClerkCriminalService.update completed with errors in {elapsed:.1f}s"
            )

        return result
