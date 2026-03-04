"""
Encumbrance read-only queries against PostgreSQL (hills_sunbiz) using SQLAlchemy ORM.

Provides:
- General encumbrances lookup by strap/folio (excludes NOCs)
- Tax lien specific queries (IRS, tax collector, corporate tax liens)
- Notice of Commencement (NOC) queries
- Encumbrance summary statistics for risk assessment

Data source: ori_encumbrances table populated by the ORI pipeline scraper.
This module lives in src/db/ as a pure query layer.  The web app and pipeline
both import from here via the ``get_encumbrance_queries()`` singleton factory.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from loguru import logger
from sqlalchemy import Text, case, cast, func, or_, select

from sunbiz.db import get_engine, resolve_pg_dsn
from sunbiz.models import OriEncumbrance

if TYPE_CHECKING:
    from sqlalchemy.sql import ColumnElement

# Alias for brevity
OE = OriEncumbrance


def _owner_clause(strap: str | None, folio: str | None) -> ColumnElement[bool]:
    """Build an OR filter matching either strap or folio (or both)."""
    parts: list[ColumnElement[bool]] = []
    if strap:
        parts.append(OE.strap == strap)
    if folio:
        parts.append(OE.folio == folio)
    if not parts:
        # No identifier — will never match
        return OE.id < 0
    return or_(*parts)


# Reusable creditor expression: mortgage → party2, else → party1
_enc_type_text = cast(OE.encumbrance_type, Text)

_creditor_expr = case(
    (
        _enc_type_text == "mortgage",
        func.coalesce(func.nullif(OE.party2, ""), func.nullif(OE.party1, ""), ""),
    ),
    else_=func.coalesce(func.nullif(OE.party1, ""), func.nullif(OE.party2, ""), ""),
).label("creditor")


class EncumbranceQueries:
    """Read-only query service for ORI encumbrances from PostgreSQL.

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
            logger.info("EncumbranceQueries connected")
        except Exception as e:
            logger.warning(f"EncumbranceQueries unavailable: {e}")
            self._engine = None

    @property
    def available(self) -> bool:
        return self._available

    # ------------------------------------------------------------------
    # General encumbrances (excludes NOCs)
    # ------------------------------------------------------------------

    def get_encumbrances(
        self,
        *,
        strap: str | None,
        folio: str | None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """Return encumbrances for a property, excluding NOC doc types."""
        if not self._available or (not strap and not folio):
            return []
        lim = max(1, min(limit, 2000))
        stmt = (
            select(
                OE.id,
                OE.recording_date,
                OE.encumbrance_type,
                OE.amount,
                OE.amount_confidence,
                OE.survival_status,
                OE.survival_reason,
                func.coalesce(OE.is_satisfied, False).label("is_satisfied"),
                OE.instrument_number.label("instrument"),
                OE.instrument_number,
                OE.book,
                OE.page,
                OE.case_number,
                OE.party1,
                OE.party2,
                OE.raw_document_type,
                _creditor_expr,
            )
            .where(_owner_clause(strap, folio))
            .where(_enc_type_text != "noc")
            .order_by(OE.recording_date.desc().nulls_last(), OE.id.desc())
            .limit(lim)
        )
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(stmt).mappings().fetchall()
            return [dict(r) for r in rows]
        except Exception:
            logger.exception("get_encumbrances query failed")
            return []

    # ------------------------------------------------------------------
    # Tax liens (IRS, tax collector, corporate tax liens)
    # ------------------------------------------------------------------

    def get_tax_liens(
        self,
        *,
        strap: str | None,
        folio: str | None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """Return tax-related liens (IRS, tax collector, LNCORPTX)."""
        if not self._available or (not strap and not folio):
            return []
        lim = max(1, min(limit, 1000))

        upper_doc = func.upper(func.coalesce(OE.raw_document_type, ""))
        upper_p1 = func.upper(func.coalesce(OE.party1, ""))
        upper_p2 = func.upper(func.coalesce(OE.party2, ""))

        tax_filter = or_(
            upper_doc.like("%LNCORPTX%"),
            upper_doc.like("%TAX LIEN%"),
            upper_doc.like("%(TL)%"),
            upper_p1.like("%INTERNAL REVENUE%"),
            upper_p1.like("% IRS %"),
            upper_p2.like("%INTERNAL REVENUE%"),
            upper_p2.like("% IRS %"),
            upper_p1.like("%TAX COLLECTOR%"),
            upper_p2.like("%TAX COLLECTOR%"),
        )
        exclude_filter = ~or_(
            upper_doc.like("%MORTGAGE%"),
            upper_doc.like("%ASSIGNMENT/TAXES%"),
        )

        stmt = (
            select(
                OE.id,
                OE.recording_date,
                OE.encumbrance_type,
                OE.raw_document_type,
                OE.amount,
                OE.survival_status,
                OE.party1,
                OE.party2,
                OE.instrument_number,
                _creditor_expr,
            )
            .where(_owner_clause(strap, folio))
            .where(tax_filter)
            .where(exclude_filter)
            .order_by(OE.recording_date.desc().nulls_last(), OE.id.desc())
            .limit(lim)
        )
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(stmt).mappings().fetchall()
            return [dict(r) for r in rows]
        except Exception:
            logger.exception("get_tax_liens query failed")
            return []

    # ------------------------------------------------------------------
    # Notices of Commencement (NOCs)
    # ------------------------------------------------------------------

    def get_nocs(
        self,
        *,
        strap: str | None,
        folio: str | None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return Notice of Commencement records."""
        if not self._available or (not strap and not folio):
            return []
        lim = max(1, min(limit, 1000))

        upper_doc = func.upper(func.coalesce(OE.raw_document_type, ""))
        noc_filter = or_(
            upper_doc.like("%(NOC)%"),
            upper_doc.like("%NOTICE OF COMMENCEMENT%"),
            upper_doc.like("NOC%"),
        )

        stmt = (
            select(
                OE.id.label("encumbrance_id"),
                OE.recording_date,
                OE.instrument_number,
                OE.party1,
                OE.party2,
                OE.legal_description,
                OE.raw_document_type,
            )
            .where(_owner_clause(strap, folio))
            .where(noc_filter)
            .order_by(OE.recording_date.desc().nulls_last(), OE.id.desc())
            .limit(lim)
        )
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(stmt).mappings().fetchall()
            return [dict(r) for r in rows]
        except Exception:
            logger.exception("get_nocs query failed")
            return []

    # ------------------------------------------------------------------
    # Summarize encumbrances (pure Python — no SQL)
    # ------------------------------------------------------------------

    @staticmethod
    def summarize(encumbrances: list[dict[str, Any]]) -> dict[str, Any]:
        """Compute risk summary statistics from a list of encumbrance dicts.

        Treats UNCERTAIN as risk-bearing until proven extinguished.
        """
        risk_statuses = {"SURVIVED", "UNCERTAIN"}
        liens_total = 0
        liens_survived = 0
        liens_uncertain = 0
        liens_total_amount = 0.0
        surviving_unknown_amount = 0

        for enc in encumbrances:
            if bool(enc.get("is_satisfied")):
                continue
            liens_total += 1
            status = str(enc.get("survival_status") or "").strip().upper()
            if status == "SURVIVED":
                liens_survived += 1
            elif status == "UNCERTAIN":
                liens_uncertain += 1
            if status in risk_statuses:
                amount = enc.get("amount")
                if amount is None:
                    surviving_unknown_amount += 1
                else:
                    try:
                        liens_total_amount += float(amount)
                    except (TypeError, ValueError):
                        surviving_unknown_amount += 1

        liens_surviving = liens_survived + liens_uncertain
        return {
            "liens_total": liens_total,
            "liens_survived": liens_survived,
            "liens_uncertain": liens_uncertain,
            "liens_surviving": liens_surviving,
            "liens_total_amount": liens_total_amount,
            "surviving_unknown_amount": surviving_unknown_amount,
        }


# ---------------------------------------------------------------------------
# Singleton factory
# ---------------------------------------------------------------------------

_instance: EncumbranceQueries | None = None


def get_encumbrance_queries(dsn: str | None = None) -> EncumbranceQueries:
    """Return (or create) the singleton EncumbranceQueries instance."""
    global _instance
    if _instance is None:
        _instance = EncumbranceQueries(dsn)
    return _instance
