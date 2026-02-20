"""
PostgreSQL Sales Service — read-only queries against hills_sunbiz.

Provides:
- Sales chain lookup (hcpa_allsales)
- Strap-to-folio resolution (hcpa_bulk_parcels)
- Fuzzy defendant name resolution (resolve_property_by_name PG function)
- Instrument references for ORI search seeding
"""

from __future__ import annotations

from loguru import logger
from sqlalchemy import text

from sunbiz.db import get_engine, resolve_pg_dsn

# Sale type code → human-readable deed type (for OwnershipPeriod.acquisition_doc_type)
SALE_TYPE_MAP = {
    "WD": "WARRANTY DEED",
    "QC": "QUIT CLAIM DEED",
    "FD": "FORECLOSURE DEED",
    "TD": "TAX DEED",
    "CT": "CERTIFICATE OF TITLE",
    "DD": "DEED",
    "TR": "TRUSTEES DEED",
    "PR": "PERSONAL REPRESENTATIVES DEED",
    "GD": "GUARDIAN DEED",
    "CD": "COMMITTEE DEED",
    "SD": "SHERIFFS DEED",
}


class PgSalesService:
    """Read-only service for PostgreSQL hills_sunbiz queries."""

    def __init__(self, dsn: str | None = None):
        self._available = False
        try:
            resolved = resolve_pg_dsn(dsn)
            self._engine = get_engine(resolved)
            # Quick connectivity test
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self._available = True
            logger.info("PostgreSQL sales service connected")
        except Exception as e:
            logger.warning(f"PostgreSQL sales service unavailable: {e}")
            self._engine = None

    @property
    def available(self) -> bool:
        return self._available

    # ------------------------------------------------------------------
    # Strap / folio resolution
    # ------------------------------------------------------------------

    def resolve_strap_to_folio(self, strap: str) -> str | None:
        """Convert a pipeline strap (auctions.parcel_id) to 10-digit PG folio."""
        if not self._available or not strap:
            return None
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text("SELECT strap_to_folio(:strap)"),
                    {"strap": strap},
                ).fetchone()
                return row[0] if row else None
        except Exception as e:
            logger.debug(f"resolve_strap_to_folio({strap}) failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Sales chain
    # ------------------------------------------------------------------

    def get_sales_chain(self, pg_folio: str) -> list[dict]:
        """Get all sales for a 10-digit PG folio, ordered by date."""
        if not self._available or not pg_folio:
            return []
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT sale_date, sale_type, sale_amount,
                               grantor, grantee,
                               or_book, or_page, doc_num,
                               qualification_code
                        FROM hcpa_allsales
                        WHERE folio = :folio
                        ORDER BY sale_date
                    """),
                    {"folio": pg_folio},
                ).fetchall()
                return [
                    {
                        "sale_date": row[0],
                        "sale_type": row[1],
                        "sale_amount": float(row[2]) if row[2] else 0.0,
                        "grantor": row[3] or "",
                        "grantee": row[4] or "",
                        "or_book": row[5] or "",
                        "or_page": row[6] or "",
                        "doc_num": row[7] or "",
                        "qualification_code": row[8] or "",
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.warning(f"get_sales_chain({pg_folio}) failed: {e}")
            return []

    def get_sale_instruments(self, pg_folio: str) -> list[dict]:
        """Get instrument references (doc_num, book/page) for ORI search seeding."""
        if not self._available or not pg_folio:
            return []
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT doc_num, or_book, or_page, grantor, grantee
                        FROM hcpa_allsales
                        WHERE folio = :folio
                          AND (doc_num IS NOT NULL AND doc_num != ''
                               OR (or_book IS NOT NULL AND or_book != ''
                                   AND or_page IS NOT NULL AND or_page != ''))
                        ORDER BY sale_date
                    """),
                    {"folio": pg_folio},
                ).fetchall()
                return [
                    {
                        "doc_num": row[0] or "",
                        "or_book": row[1] or "",
                        "or_page": row[2] or "",
                        "grantor": row[3] or "",
                        "grantee": row[4] or "",
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.debug(f"get_sale_instruments({pg_folio}) failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Fuzzy name resolution
    # ------------------------------------------------------------------

    def resolve_property_by_name(
        self,
        defendant_name: str,
        plaintiff_hint: str | None = None,
        threshold: float = 0.3,
    ) -> list[dict]:
        """
        Resolve a defendant name to property folio(s) via PG fuzzy matching.

        Returns list of dicts with: folio, strap, property_address, owner_name,
        match_method, match_score — sorted by match_score DESC.
        """
        if not self._available or not defendant_name:
            return []
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT folio, strap, property_address, city,
                               owner_name, legal_description,
                               match_method, match_score
                        FROM resolve_property_by_name(
                            CAST(:name AS text),
                            CAST(:hint AS text),
                            CAST(:threshold AS real)
                        )
                        ORDER BY match_score DESC
                        LIMIT 10
                    """),
                    {
                        "name": defendant_name,
                        "hint": plaintiff_hint,
                        "threshold": threshold,
                    },
                ).fetchall()
                return [
                    {
                        "folio": row[0],
                        "strap": row[1],
                        "property_address": row[2],
                        "city": row[3],
                        "owner_name": row[4],
                        "legal_description": row[5],
                        "match_method": row[6],
                        "match_score": float(row[7]) if row[7] else 0.0,
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.warning(f"resolve_property_by_name({defendant_name!r}) failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Bulk parcel lookup
    # ------------------------------------------------------------------

    def get_bulk_parcel(self, strap: str) -> dict | None:
        """Get parcel data including legal description by strap."""
        if not self._available or not strap:
            return None
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text("""
                        SELECT folio, strap, owner_name, property_address,
                               city, zip_code, raw_legal1, raw_legal2,
                               raw_legal3, raw_legal4, units
                        FROM hcpa_bulk_parcels
                        WHERE strap = :strap
                        LIMIT 1
                    """),
                    {"strap": strap},
                ).fetchone()
                if not row:
                    return None
                return {
                    "folio": row[0],
                    "strap": row[1],
                    "owner_name": row[2],
                    "property_address": row[3],
                    "city": row[4],
                    "zip_code": row[5],
                    "raw_legal1": row[6],
                    "raw_legal2": row[7],
                    "raw_legal3": row[8],
                    "raw_legal4": row[9],
                    "units": row[10],
                }
        except Exception as e:
            logger.debug(f"get_bulk_parcel({strap}) failed: {e}")
            return None
