"""
Tax/NAL read-only queries against PostgreSQL (hills_sunbiz) using SQLAlchemy ORM.

Provides:
- Current tax year detail (valuations, exemptions, millage) for a property
- Multi-year tax history with DISTINCT ON (tax_year) dedup

Data source: dor_nal_parcels table loaded from Florida DOR NAL CSV files.
This module lives in src/db/ as a pure query layer.  The web app delegates
its tax-tab SQL here via the ``get_tax_queries()`` singleton factory.
"""

from __future__ import annotations

from typing import Any

from loguru import logger
from sqlalchemy import func, or_, select

from sunbiz.db import get_engine, resolve_pg_dsn
from sunbiz.models import DorNalParcel

D = DorNalParcel


def _id_filter(ids: list[str]):
    """Build an OR filter matching folio, strap, or parcel_id against *ids*."""
    return or_(
        D.folio.in_(ids),
        D.strap.in_(ids),
        D.parcel_id.in_(ids),
    )


class TaxQueries:
    """Read-only query service for DOR NAL tax data from PostgreSQL.

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
            logger.info("TaxQueries connected")
        except Exception as e:
            logger.warning(f"TaxQueries unavailable: {e}")
            self._engine = None

    @property
    def available(self) -> bool:
        return self._available

    # ------------------------------------------------------------------
    # Current year full detail
    # ------------------------------------------------------------------

    def get_current_year(
        self,
        *,
        strap: str | None,
        folio: str | None,
        identifier: str,
    ) -> dict[str, Any] | None:
        """Return the most recent tax year record for a property."""
        if not self._available:
            return None
        ids = [v for v in (folio, strap, identifier) if v]
        if not ids:
            return None

        stmt = (
            select(
                D.tax_year,
                D.homestead_exempt,
                D.estimated_annual_tax,
                D.just_value,
                D.just_value_homestead,
                D.assessed_value_school,
                D.assessed_value_nonschool,
                D.assessed_value_homestead,
                D.taxable_value_school,
                D.taxable_value_nonschool,
                D.homestead_exempt_value,
                D.widow_exempt,
                D.widow_exempt_value,
                D.disability_exempt,
                D.disability_exempt_value,
                D.veteran_exempt,
                D.veteran_exempt_value,
                D.ag_exempt,
                D.ag_exempt_value,
                D.soh_differential,
                D.total_millage,
                D.county_millage,
                D.school_millage,
                D.city_millage,
                D.property_use_code,
            )
            .where(_id_filter(ids))
            .order_by(D.tax_year.desc())
            .limit(1)
        )
        try:
            with self._engine.connect() as conn:
                row = conn.execute(stmt).mappings().fetchone()
            return dict(row) if row else None
        except Exception:
            logger.exception("get_current_year query failed")
            return None

    # ------------------------------------------------------------------
    # Tax history (all years, deduped by tax_year)
    # ------------------------------------------------------------------

    def get_history(
        self,
        *,
        strap: str | None,
        folio: str | None,
        identifier: str,
    ) -> list[dict[str, Any]]:
        """Return per-year tax history sorted descending.

        Uses PG ``DISTINCT ON (tax_year)`` to deduplicate when multiple
        NAL records exist for the same year.
        """
        if not self._available:
            return []
        ids = [v for v in (folio, strap, identifier) if v]
        if not ids:
            return []

        assessed = func.greatest(D.assessed_value_school, D.assessed_value_nonschool)
        taxable = func.greatest(D.taxable_value_school, D.taxable_value_nonschool)

        stmt = (
            select(
                D.tax_year,
                D.just_value,
                assessed.label("assessed_value"),
                taxable.label("taxable_value"),
                D.estimated_annual_tax,
                D.homestead_exempt,
                D.total_millage,
            )
            .distinct(D.tax_year)
            .where(_id_filter(ids))
            .order_by(D.tax_year.desc(), D.id.desc())
        )
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(stmt).mappings().fetchall()
            return [dict(r) for r in rows]
        except Exception:
            logger.exception("get_history query failed")
            return []


# ---------------------------------------------------------------------------
# Singleton factory
# ---------------------------------------------------------------------------

_instance: TaxQueries | None = None


def get_tax_queries(dsn: str | None = None) -> TaxQueries:
    """Return (or create) the singleton TaxQueries instance."""
    global _instance
    if _instance is None:
        _instance = TaxQueries(dsn)
    return _instance
