"""
PostgreSQL DOR NAL Service -- standalone sync + load for Florida tax data.

Downloads the Hillsborough County NAL (Name-Address-Legal) file from the
Florida Department of Revenue data portal and loads it into PostgreSQL.

The NAL file is the official property tax assessment roll.  It provides:
- Homestead exemption status (critical for lien survival analysis)
- 44 exemption type breakdowns
- Assessed/taxable values (school & non-school)
- Millage rates by taxing authority
- Estimated annual property tax
- Full legal descriptions

Orchestrator usage (fire-and-forget)::

    from src.services.pg_nal_service import PgNalService

    service = PgNalService()
    if service.available:
        stats = service.update()
        logger.info(f"DOR NAL update: {stats}")

The service checks which tax years are already loaded and only downloads/loads
newer data.  NAL files are annual (Final rolls typically available Oct-Jan).
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


DEFAULT_NAL_DIR = Path("data/bulk_data/dor_nal")


class PgNalService:
    """Standalone service: sync + load Florida DOR NAL tax data.

    Lifecycle:
        1. ``__init__`` -- connects to PG, verifies connectivity.
        2. ``get_current_state()`` -- queries dor_nal_parcels for loaded years.
        3. ``update()`` -- downloads latest NAL, loads into PG, returns stats.
    """

    def __init__(self, dsn: str | None = None, download_dir: Path | None = None):
        self._available = False
        self._engine = None
        self._dsn: str = ""
        self.download_dir = download_dir or DEFAULT_NAL_DIR

        try:
            self._dsn = resolve_pg_dsn(dsn)
            self._engine = get_engine(self._dsn)
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self._available = True
            logger.info("PgNalService: PostgreSQL connected")
        except SQLAlchemyError as exc:
            logger.error(
                f"PgNalService: PostgreSQL connection failed: {exc}\n"
                f"  DSN: {self._dsn!r}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
        except Exception as exc:
            logger.error(
                f"PgNalService: unexpected init error: {exc}\n"
                f"  Traceback: {traceback.format_exc()}"
            )

    @property
    def available(self) -> bool:
        return self._available

    # ------------------------------------------------------------------
    # State introspection
    # ------------------------------------------------------------------

    def get_current_state(self) -> dict[str, Any]:
        """Query PG for the current state of DOR NAL data.

        Returns dict with:
            table_exists: bool
            total_parcels: int
            tax_years_loaded: list[int]  (sorted descending)
            latest_tax_year: int | None
            latest_loaded_at: datetime | None
            parcels_by_year: dict[int, int]
            homestead_count: int
            folio_mapped_count: int
        """
        state: dict[str, Any] = {
            "table_exists": False,
            "total_parcels": 0,
            "tax_years_loaded": [],
            "latest_tax_year": None,
            "latest_loaded_at": None,
            "parcels_by_year": {},
            "homestead_count": 0,
            "folio_mapped_count": 0,
        }
        if not self._available:
            logger.warning("PgNalService.get_current_state: PG unavailable")
            return state

        try:
            with self._engine.connect() as conn:
                # Check table existence
                exists = conn.execute(
                    text("""
                        SELECT COUNT(*) FROM information_schema.tables
                        WHERE table_name = 'dor_nal_parcels'
                    """)
                ).scalar()
                state["table_exists"] = (exists or 0) > 0

                if not state["table_exists"]:
                    logger.info(
                        "PgNalService: dor_nal_parcels table not yet created "
                        "(will be created on first load)"
                    )
                    return state

                # Total count
                state["total_parcels"] = conn.execute(
                    text("SELECT COUNT(*) FROM dor_nal_parcels")
                ).scalar() or 0

                # Years loaded with counts
                rows = conn.execute(
                    text("""
                        SELECT tax_year, COUNT(*) AS cnt
                        FROM dor_nal_parcels
                        GROUP BY tax_year
                        ORDER BY tax_year DESC
                    """)
                ).fetchall()
                for r in rows:
                    year, cnt = int(r[0]), r[1]
                    state["tax_years_loaded"].append(year)
                    state["parcels_by_year"][year] = cnt

                if state["tax_years_loaded"]:
                    state["latest_tax_year"] = state["tax_years_loaded"][0]

                # Homestead count (latest year)
                if state["latest_tax_year"]:
                    state["homestead_count"] = conn.execute(
                        text("""
                            SELECT COUNT(*) FROM dor_nal_parcels
                            WHERE tax_year = :yr AND homestead_exempt = true
                        """),
                        {"yr": state["latest_tax_year"]},
                    ).scalar() or 0

                    # Folio mapped count
                    state["folio_mapped_count"] = conn.execute(
                        text("""
                            SELECT COUNT(*) FROM dor_nal_parcels
                            WHERE tax_year = :yr AND folio IS NOT NULL
                        """),
                        {"yr": state["latest_tax_year"]},
                    ).scalar() or 0

                # Latest load timestamp from ingest_files
                try:
                    row = conn.execute(
                        text("""
                            SELECT loaded_at FROM ingest_files
                            WHERE category = 'dor_nal'
                              AND status = 'loaded'
                            ORDER BY loaded_at DESC
                            LIMIT 1
                        """)
                    ).fetchone()
                    if row and row[0]:
                        state["latest_loaded_at"] = row[0]
                except SQLAlchemyError as exc:
                    logger.debug(f"PgNalService: ingest_files query failed: {exc}")

        except SQLAlchemyError as exc:
            logger.error(
                f"PgNalService.get_current_state: PG read failed: {exc}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
        except Exception as exc:
            logger.error(
                f"PgNalService.get_current_state: unexpected error: {exc}\n"
                f"  Traceback: {traceback.format_exc()}"
            )

        logger.info(
            f"PgNalService state: "
            f"total={state['total_parcels']:,}, "
            f"years={state['tax_years_loaded']}, "
            f"homestead={state['homestead_count']:,}, "
            f"folio_mapped={state['folio_mapped_count']:,}"
        )
        return state

    # ------------------------------------------------------------------
    # Update
    # ------------------------------------------------------------------

    def _determine_target_year(self, loaded_years: list[int]) -> int:
        """Determine which tax year to download.

        Strategy:
        - The DOR Final NAL for year N is typically available Oct-Jan of year N+1.
        - If current month is >= October, try current year's Final NAL.
        - Otherwise, try previous year's Final NAL.
        - If that year is already loaded, return it anyway (will be skipped
          at the file-level by SHA check).
        """
        now = dt.datetime.now(dt.UTC)
        target = now.year if now.month >= 10 else now.year - 1

        if target in loaded_years:
            logger.info(
                f"PgNalService: tax year {target} already loaded "
                f"(will check for file changes)"
            )

        return target

    def update(
        self,
        tax_year: int | None = None,
        force_download: bool = False,
    ) -> dict[str, Any]:
        """Download and load the latest DOR NAL data into PostgreSQL.

        This is the orchestrator entry point -- fire and forget.

        Args:
            tax_year: Explicit tax year to load. If None, auto-detects.
            force_download: Re-download even if ZIP exists locally.

        Returns:
            Stats dict with download and load results.
        """
        result: dict[str, Any] = {
            "success": False,
            "target_tax_year": None,
            "already_loaded": False,
            "download_path": None,
            "load_stats": {},
            "error": None,
        }

        if not self._available:
            result["error"] = "PostgreSQL unavailable"
            logger.error(f"PgNalService.update: {result['error']}")
            return result

        # Import loader functions (deferred to avoid circular imports at module level)
        try:
            from sunbiz.pg_loader import download_dor_nal, load_dor_nal
        except ImportError as exc:
            result["error"] = f"Failed to import pg_loader: {exc}"
            logger.error(
                f"PgNalService.update: {result['error']}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
            return result

        started_at = dt.datetime.now(dt.UTC)

        # Step 1: Determine target year
        current_state = self.get_current_state()
        if tax_year is None:
            tax_year = self._determine_target_year(current_state["tax_years_loaded"])
        result["target_tax_year"] = tax_year

        logger.info(
            f"PgNalService.update: target tax_year={tax_year}, "
            f"force_download={force_download}, "
            f"download_dir={self.download_dir}"
        )

        # Step 2: Ensure tables exist
        try:
            from sunbiz.models import Base
            engine = get_engine(self._dsn)
            Base.metadata.create_all(bind=engine)
            logger.info("PgNalService: tables initialized")
        except Exception as exc:
            result["error"] = f"Table init failed: {exc}"
            logger.error(
                f"PgNalService.update: {result['error']}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
            return result

        # Step 3: Download NAL ZIP
        try:
            zip_path = download_dor_nal(
                output_dir=self.download_dir,
                tax_year=tax_year,
                roll_type="F",
                force=force_download,
            )
            result["download_path"] = str(zip_path)
            logger.info(
                f"PgNalService: NAL ZIP at {zip_path} "
                f"({zip_path.stat().st_size:,} bytes)"
            )
        except Exception as exc:
            # Try preliminary roll if final is not yet available
            logger.warning(
                f"PgNalService: Final NAL download failed: {exc}. "
                f"Trying Preliminary roll ..."
            )
            try:
                zip_path = download_dor_nal(
                    output_dir=self.download_dir,
                    tax_year=tax_year,
                    roll_type="P",
                    force=force_download,
                )
                result["download_path"] = str(zip_path)
                logger.info(
                    f"PgNalService: Preliminary NAL ZIP at {zip_path} "
                    f"({zip_path.stat().st_size:,} bytes)"
                )
            except Exception as exc2:
                result["error"] = (
                    f"Download failed for both Final and Preliminary rolls: "
                    f"Final: {exc}, Preliminary: {exc2}"
                )
                logger.error(
                    f"PgNalService.update: {result['error']}\n"
                    f"  Traceback: {traceback.format_exc()}"
                )
                return result

        # Step 4: Load into PostgreSQL
        try:
            load_stats = load_dor_nal(
                dsn=self._dsn,
                nal_zip=zip_path,
                tax_year=tax_year,
            )
            result["load_stats"] = load_stats
            result["success"] = True

            elapsed = (dt.datetime.now(dt.UTC) - started_at).total_seconds()
            result["elapsed_seconds"] = round(elapsed, 1)

            logger.success(
                f"PgNalService.update completed in {elapsed:.1f}s: "
                f"tax_year={tax_year}, "
                f"parcels_upserted={load_stats.get('parcels_upserted', 0):,}, "
                f"folio_mapped={load_stats.get('folio_mapped', 0):,}, "
                f"skipped_other_county={load_stats.get('skipped_other_county', 0):,}"
            )

        except SQLAlchemyError as exc:
            result["error"] = f"PG write failed: {exc}"
            logger.error(
                f"PgNalService.update: PG write failed during load: {exc}\n"
                f"  SQL state: {getattr(exc, 'orig', 'N/A')}\n"
                f"  Traceback: {traceback.format_exc()}"
            )
        except Exception as exc:
            result["error"] = f"Load failed: {exc}"
            logger.error(
                f"PgNalService.update: load phase failed: {exc}\n"
                f"  Traceback: {traceback.format_exc()}"
            )

        return result
