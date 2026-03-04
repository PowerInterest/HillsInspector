"""
Foreclosure read-only queries against PostgreSQL (hills_sunbiz) using SQLAlchemy ORM.

Provides:
- Case number lookup by identifier (strap, folio, or case_number_raw)
- Multi-tier folio resolution (foreclosures → hcpa_bulk_parcels → hcpa_allsales)
- Permit lookup from foreclosure_title_events

Data source: foreclosures table (hub), hcpa_bulk_parcels, hcpa_allsales,
and foreclosure_title_events.  ``foreclosures_history`` is a VIEW over
``foreclosures WHERE archived_at IS NOT NULL`` — queries use the same
Foreclosure model with an extra filter.

This module lives in src/db/ as a pure query layer.  The web app and pipeline
both import from here via the ``get_foreclosure_queries()`` singleton factory.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from loguru import logger
from sqlalchemy import case, func, or_, select, union_all

from sunbiz.db import get_engine, resolve_pg_dsn
from sunbiz.models import (
    Foreclosure,
    ForeclosureTitleEvent,
    HcpaAllSale,
    HcpaBulkParcel,
)

if TYPE_CHECKING:
    from sqlalchemy.sql import ColumnElement

F = Foreclosure
FTE = ForeclosureTitleEvent


def _identifier_filter(identifier: str) -> ColumnElement[bool]:
    """Match a property by case_number_raw, strap, or folio."""
    return or_(
        F.case_number_raw == identifier,
        F.strap == identifier,
        F.folio == identifier,
    )


class ForeclosureQueries:
    """Read-only query service for foreclosure-related data from PostgreSQL.

    Graceful degradation: if PostgreSQL is unreachable at init time, all
    methods return empty results rather than raising.
    """

    def __init__(self, dsn: str | None = None):
        self._available = False
        self._engine = None
        try:
            from sqlalchemy import text as sa_text

            resolved = resolve_pg_dsn(dsn)
            self._engine = get_engine(resolved)
            with self._engine.connect() as conn:
                conn.execute(sa_text("SELECT 1"))
            self._available = True
            logger.info("ForeclosureQueries connected")
        except Exception as e:
            logger.warning(f"ForeclosureQueries unavailable: {e}")
            self._engine = None

    @property
    def available(self) -> bool:
        return self._available

    # ------------------------------------------------------------------
    # Case number lookup
    # ------------------------------------------------------------------

    def get_case_numbers(self, identifier: str) -> list[str]:
        """Return distinct case numbers matching strap, folio, or case_number_raw.

        Searches both active foreclosures and archived (history view) rows.
        """
        if not self._available or not identifier:
            return []

        # Active rows
        active = select(F.case_number_raw.label("case_number")).where(
            _identifier_filter(identifier)
        )
        # Archived rows (foreclosures_history is WHERE archived_at IS NOT NULL)
        archived = select(F.case_number_raw.label("case_number")).where(
            _identifier_filter(identifier),
            F.archived_at.is_not(None),
        )

        combined = union_all(active, archived).subquery("t")
        cn = combined.c.case_number
        stmt = (
            select(cn)
            .where(cn.is_not(None), func.btrim(cn) != "")
            .distinct()
            .order_by(cn)
        )

        try:
            with self._engine.connect() as conn:
                rows = conn.execute(stmt).fetchall()
            return [str(r[0]) for r in rows if r and r[0]]
        except Exception:
            logger.exception("get_case_numbers query failed")
            return []

    # ------------------------------------------------------------------
    # Folio resolution (4-tier fallback)
    # ------------------------------------------------------------------

    def resolve_folio(
        self,
        identifier: str | None,
        case_number: str | None = None,
    ) -> str | None:
        """Resolve an identifier to a 10-digit folio using 4 fallback tiers.

        Tier 1: Match by case_number in foreclosures (active + archived)
        Tier 2: Match by identifier in foreclosures (active + archived)
        Tier 3: Match by strap/folio in hcpa_bulk_parcels
        Tier 4: Match by folio in hcpa_allsales
        """
        if not self._available:
            return None

        try:
            with self._engine.connect() as conn:
                # Tier 1: case_number match
                if case_number:
                    result = self._resolve_by_case_number(conn, case_number)
                    if result:
                        return result

                if not identifier:
                    return None

                # Tier 2: identifier match in foreclosures
                result = self._resolve_by_identifier(conn, identifier)
                if result:
                    return result

                # Tier 3: hcpa_bulk_parcels
                result = self._resolve_via_bulk_parcels(conn, identifier)
                if result:
                    return result

                # Tier 4: hcpa_allsales
                result = self._resolve_via_allsales(conn, identifier)
                if result:
                    return result

            return None
        except Exception:
            logger.exception("resolve_folio failed")
            return None

    def _resolve_by_case_number(self, conn: Any, case_number: str) -> str | None:
        """Tier 1: Find folio by case_number_raw or case_number_norm."""
        active = select(F.folio, F.auction_date).where(
            or_(F.case_number_raw == case_number, F.case_number_norm == case_number)
        )
        archived = select(F.folio, F.auction_date).where(
            or_(F.case_number_raw == case_number, F.case_number_norm == case_number),
            F.archived_at.is_not(None),
        )
        combined = union_all(active, archived).subquery("x")
        stmt = (
            select(combined.c.folio)
            .where(combined.c.folio.is_not(None), func.btrim(combined.c.folio) != "")
            .order_by(combined.c.auction_date.desc().nulls_last())
            .limit(1)
        )
        row = conn.execute(stmt).fetchone()
        return str(row[0]) if row and row[0] else None

    def _resolve_by_identifier(self, conn: Any, identifier: str) -> str | None:
        """Tier 2: Find folio by matching strap/folio/case_number in foreclosures."""
        ident_filter = or_(
            F.folio == identifier,
            F.strap == identifier,
            F.case_number_raw == identifier,
            F.case_number_norm == identifier,
        )
        active = select(F.folio, F.auction_date).where(ident_filter)
        archived = select(F.folio, F.auction_date).where(
            ident_filter, F.archived_at.is_not(None)
        )
        combined = union_all(active, archived).subquery("x")
        stmt = (
            select(combined.c.folio)
            .where(combined.c.folio.is_not(None), func.btrim(combined.c.folio) != "")
            .order_by(combined.c.auction_date.desc().nulls_last())
            .limit(1)
        )
        row = conn.execute(stmt).fetchone()
        return str(row[0]) if row and row[0] else None

    def _resolve_via_bulk_parcels(self, conn: Any, identifier: str) -> str | None:
        """Tier 3: Find folio in hcpa_bulk_parcels by strap or folio."""
        stmt = (
            select(HcpaBulkParcel.folio)
            .where(
                or_(
                    HcpaBulkParcel.strap == identifier,
                    HcpaBulkParcel.folio == identifier,
                )
            )
            .limit(1)
        )
        row = conn.execute(stmt).fetchone()
        return str(row[0]) if row and row[0] else None

    def _resolve_via_allsales(self, conn: Any, identifier: str) -> str | None:
        """Tier 4: Find folio in hcpa_allsales."""
        stmt = (
            select(HcpaAllSale.folio)
            .where(HcpaAllSale.folio == identifier)
            .limit(1)
        )
        row = conn.execute(stmt).fetchone()
        return str(row[0]) if row and row[0] else None

    # ------------------------------------------------------------------
    # Permits from title events
    # ------------------------------------------------------------------

    def get_permits(self, foreclosure_id: int) -> list[dict[str, Any]]:
        """Return permit records from foreclosure_title_events."""
        if not self._available or not foreclosure_id:
            return []

        status_expr = case(
            (
                FTE.description.regexp_match(
                    r"(closed|complete|final|expired)", flags="i"
                ),
                "Closed",
            ),
            else_="Open",
        ).label("status")

        stmt = (
            select(
                FTE.event_date.label("issue_date"),
                FTE.instrument_number.label("permit_number"),
                FTE.event_subtype.label("permit_type"),
                FTE.description,
                FTE.amount.label("estimated_cost"),
                status_expr,
            )
            .where(FTE.foreclosure_id == foreclosure_id)
            .where(FTE.event_source.in_(["COUNTY_PERMIT", "TAMPA_PERMIT"]))
            .order_by(FTE.event_date.desc())
        )
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(stmt).mappings().fetchall()
            return [dict(r) for r in rows]
        except Exception:
            logger.exception("get_permits query failed")
            return []


# ---------------------------------------------------------------------------
# Singleton factory
# ---------------------------------------------------------------------------

_instance: ForeclosureQueries | None = None


def get_foreclosure_queries(dsn: str | None = None) -> ForeclosureQueries:
    """Return (or create) the singleton ForeclosureQueries instance."""
    global _instance
    if _instance is None:
        _instance = ForeclosureQueries(dsn)
    return _instance
