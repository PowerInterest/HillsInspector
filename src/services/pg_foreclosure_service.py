"""
Read/write service for the ``foreclosures`` hub table in PostgreSQL.

Follows the same pattern as PgSalesService: graceful degradation when PG
is unavailable, raw SQL via ``engine.connect()``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datetime import date

from loguru import logger
from sqlalchemy import text

from sunbiz.db import get_engine, resolve_pg_dsn


class PgForeclosureService:
    """Service layer over the ``foreclosures`` and ``foreclosure_events`` tables."""

    def __init__(self, dsn: str | None = None) -> None:
        self._available = False
        try:
            resolved = resolve_pg_dsn(dsn)
            self._engine = get_engine(resolved)
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1 FROM foreclosures LIMIT 0"))
            self._available = True
            logger.info("PgForeclosureService connected")
        except Exception as e:
            logger.warning(f"PgForeclosureService unavailable: {e}")
            self._engine = None  # type: ignore[assignment]

    @property
    def available(self) -> bool:
        return self._available

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def get_foreclosures(
        self,
        *,
        auction_date_from: date | None = None,
        auction_date_to: date | None = None,
        status: str | None = None,
        active_only: bool = False,
        strap: str | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Paginated list of foreclosures with optional filters."""
        if not self._available:
            return []
        clauses: list[str] = []
        params: dict[str, Any] = {"lim": limit, "off": offset}

        if auction_date_from:
            clauses.append("auction_date >= :d_from")
            params["d_from"] = auction_date_from
        if auction_date_to:
            clauses.append("auction_date <= :d_to")
            params["d_to"] = auction_date_to
        if status:
            clauses.append("LOWER(auction_status) = LOWER(:status)")
            params["status"] = status
        if active_only:
            clauses.append("archived_at IS NULL")
        if strap:
            clauses.append("strap = :strap")
            params["strap"] = strap

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"""
            SELECT * FROM foreclosures
            {where}
            ORDER BY auction_date DESC, foreclosure_id DESC
            LIMIT :lim OFFSET :off
        """
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(text(sql), params).mappings().fetchall()
                return [dict(r) for r in rows]
        except Exception as e:
            logger.warning(f"get_foreclosures failed: {e}")
            return []

    def get_foreclosure(self, foreclosure_id: int) -> dict[str, Any] | None:
        """Single foreclosure by PK."""
        if not self._available:
            return None
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text("SELECT * FROM foreclosures WHERE foreclosure_id = :id"),
                    {"id": foreclosure_id},
                ).mappings().fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.warning(f"get_foreclosure({foreclosure_id}) failed: {e}")
            return None

    def get_by_case(
        self, case_number: str, auction_date: date | None = None
    ) -> dict[str, Any] | None:
        """Look up by raw case number (+ optional auction date)."""
        if not self._available or not case_number:
            return None
        try:
            with self._engine.connect() as conn:
                if auction_date:
                    row = conn.execute(
                        text(
                            "SELECT * FROM foreclosures "
                            "WHERE case_number_raw = :cn AND auction_date = :ad"
                        ),
                        {"cn": case_number, "ad": auction_date},
                    ).mappings().fetchone()
                else:
                    row = conn.execute(
                        text(
                            "SELECT * FROM foreclosures "
                            "WHERE case_number_raw = :cn "
                            "ORDER BY auction_date DESC LIMIT 1"
                        ),
                        {"cn": case_number},
                    ).mappings().fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.warning(f"get_by_case({case_number}) failed: {e}")
            return None

    def get_timeline(self, strap: str) -> list[dict[str, Any]]:
        """Full property timeline from the property_timeline view."""
        if not self._available or not strap:
            return []
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT * FROM property_timeline "
                        "WHERE strap = :strap ORDER BY event_date"
                    ),
                    {"strap": strap},
                ).mappings().fetchall()
                return [dict(r) for r in rows]
        except Exception as e:
            logger.warning(f"get_timeline({strap}) failed: {e}")
            return []

    def get_events(self, foreclosure_id: int) -> list[dict[str, Any]]:
        """Docket events for a single foreclosure."""
        if not self._available:
            return []
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT * FROM foreclosure_events "
                        "WHERE foreclosure_id = :fid ORDER BY event_date"
                    ),
                    {"fid": foreclosure_id},
                ).mappings().fetchall()
                return [dict(r) for r in rows]
        except Exception as e:
            logger.warning(f"get_events({foreclosure_id}) failed: {e}")
            return []

    def get_stats(self) -> dict[str, Any]:
        """Aggregate stats across all foreclosures."""
        if not self._available:
            return {}
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text("""
                        SELECT
                            COUNT(*)                                          AS total,
                            COUNT(*) FILTER (WHERE archived_at IS NULL)       AS active,
                            COUNT(*) FILTER (WHERE case_number_norm IS NOT NULL) AS normalized,
                            COUNT(*) FILTER (WHERE strap IS NOT NULL AND folio IS NOT NULL)
                                                                              AS cross_filled,
                            COUNT(*) FILTER (WHERE hold_days IS NOT NULL)     AS with_resale,
                            COUNT(*) FILTER (WHERE is_foreclosure)            AS confirmed_foreclosure,
                            ROUND(AVG(roi) FILTER (WHERE roi IS NOT NULL), 4) AS avg_roi,
                            ROUND(AVG(hold_days) FILTER (WHERE hold_days IS NOT NULL)) AS avg_hold_days,
                            COUNT(DISTINCT strap) FILTER (WHERE strap IS NOT NULL)
                                                                              AS unique_properties,
                            (SELECT COUNT(*) FROM foreclosure_events)         AS total_events
                        FROM foreclosures
                    """)
                ).mappings().fetchone()
                return dict(row) if row else {}
        except Exception as e:
            logger.warning(f"get_stats failed: {e}")
            return {}

    # ------------------------------------------------------------------
    # Writes (for pipeline integration)
    # ------------------------------------------------------------------

    def update_pipeline_step(
        self,
        case_number_raw: str,
        auction_date: date,
        step: str,
        value: Any = None,
    ) -> bool:
        """Mark a pipeline step timestamp on a foreclosure row.

        ``step`` must be one of: step_pdf_downloaded, step_judgment_extracted,
        step_ori_searched, step_survival_analyzed.
        """
        allowed = {
            "step_pdf_downloaded",
            "step_judgment_extracted",
            "step_ori_searched",
            "step_survival_analyzed",
        }
        if step not in allowed:
            logger.warning(f"update_pipeline_step: unknown step {step!r}")
            return False
        if not self._available:
            return False
        try:
            with self._engine.begin() as conn:
                r = conn.execute(
                    text(
                        f"UPDATE foreclosures SET {step} = COALESCE(:val, now()) "
                        "WHERE case_number_raw = :cn AND auction_date = :ad"
                    ),
                    {"val": value, "cn": case_number_raw, "ad": auction_date},
                )
                return r.rowcount > 0
        except Exception as e:
            logger.warning(f"update_pipeline_step failed: {e}")
            return False

    def update_judgment_data(
        self,
        case_number_raw: str,
        auction_date: date,
        judgment_data: dict[str, Any],
        pdf_path: str | None = None,
    ) -> bool:
        """Store extracted judgment JSON on a foreclosure row."""
        if not self._available:
            return False
        try:
            import json

            with self._engine.begin() as conn:
                r = conn.execute(
                    text(
                        "UPDATE foreclosures "
                        "SET judgment_data = :jd::jsonb, "
                        "    pdf_path = COALESCE(:pp, pdf_path), "
                        "    step_judgment_extracted = now() "
                        "WHERE case_number_raw = :cn AND auction_date = :ad"
                    ),
                    {
                        "jd": json.dumps(judgment_data),
                        "pp": pdf_path,
                        "cn": case_number_raw,
                        "ad": auction_date,
                    },
                )
                return r.rowcount > 0
        except Exception as e:
            logger.warning(f"update_judgment_data failed: {e}")
            return False

    # ------------------------------------------------------------------
    # Refresh (delegates to the refresh script logic)
    # ------------------------------------------------------------------

    def needs_encumbrance_sync(self) -> bool:
        """Check if ori_encumbrances is empty (needs SQLite sync)."""
        if not self._available:
            return False
        try:
            with self._engine.connect() as conn:
                count = conn.execute(
                    text("SELECT COUNT(*) FROM ori_encumbrances")
                ).scalar() or 0
                return count == 0
        except Exception as exc:
            logger.warning(f"needs_encumbrance_sync check failed: {exc}")
            return False

    def refresh(self, sync_encumbrances: bool = False) -> dict[str, int]:
        """Run the full idempotent refresh. Returns rowcounts per step."""
        from scripts.refresh_foreclosures import refresh as _refresh

        return _refresh(sync_encumbrances=sync_encumbrances)
