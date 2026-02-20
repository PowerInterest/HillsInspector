"""
PostgreSQL Tax Service -- read-only queries against dor_nal_parcels in hills_sunbiz.

Provides:
- Homestead exemption check (critical for lien survival analysis)
- Estimated annual property tax
- Exemption flags (widow, disability, veteran, agricultural)
- Assessed/taxable value lookups
- Millage rate data
"""

from __future__ import annotations

from loguru import logger
from sqlalchemy import text

from sunbiz.db import get_engine, resolve_pg_dsn


class PgTaxService:
    """Read-only service for DOR NAL tax/exemption data in PostgreSQL."""

    def __init__(self, dsn: str | None = None):
        self._available = False
        try:
            resolved = resolve_pg_dsn(dsn)
            self._engine = get_engine(resolved)
            # Quick connectivity + table existence test
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1 FROM dor_nal_parcels LIMIT 0"))
            self._available = True
            logger.info("PostgreSQL tax service connected (dor_nal_parcels)")
        except Exception as e:
            logger.warning(f"PostgreSQL tax service unavailable: {e}")
            self._engine = None

    @property
    def available(self) -> bool:
        return self._available

    # ------------------------------------------------------------------
    # Core lookups
    # ------------------------------------------------------------------

    def get_tax_info(self, strap: str) -> dict | None:
        """Get tax/exemption data for a property by strap (pipeline parcel_id).

        Returns a dict with homestead status, values, exemption flags, millage
        rates, and estimated annual tax. Returns None if data unavailable.
        """
        if not self._available or not strap:
            return None
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text("""
                        SELECT
                            county_code, parcel_id, folio, strap, tax_year,
                            owner_name, property_address, city, zip_code,
                            property_use_code,
                            just_value, just_value_homestead,
                            assessed_value_school, assessed_value_nonschool,
                            assessed_value_homestead,
                            taxable_value_school, taxable_value_nonschool,
                            homestead_exempt, homestead_exempt_value,
                            widow_exempt, widow_exempt_value,
                            disability_exempt, disability_exempt_value,
                            veteran_exempt, veteran_exempt_value,
                            ag_exempt, ag_exempt_value,
                            soh_differential,
                            total_millage, county_millage, school_millage, city_millage,
                            estimated_annual_tax,
                            legal_description
                        FROM dor_nal_parcels
                        WHERE strap = :strap
                        ORDER BY tax_year DESC
                        LIMIT 1
                    """),
                    {"strap": strap},
                ).fetchone()
                if not row:
                    return None
                return self._row_to_dict(row)
        except Exception as e:
            logger.debug(f"get_tax_info({strap}) failed: {e}")
            return None

    def get_tax_by_folio(self, folio: str) -> dict | None:
        """Same as get_tax_info but lookup by 10-digit PG folio instead of strap."""
        if not self._available or not folio:
            return None
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text("""
                        SELECT
                            county_code, parcel_id, folio, strap, tax_year,
                            owner_name, property_address, city, zip_code,
                            property_use_code,
                            just_value, just_value_homestead,
                            assessed_value_school, assessed_value_nonschool,
                            assessed_value_homestead,
                            taxable_value_school, taxable_value_nonschool,
                            homestead_exempt, homestead_exempt_value,
                            widow_exempt, widow_exempt_value,
                            disability_exempt, disability_exempt_value,
                            veteran_exempt, veteran_exempt_value,
                            ag_exempt, ag_exempt_value,
                            soh_differential,
                            total_millage, county_millage, school_millage, city_millage,
                            estimated_annual_tax,
                            legal_description
                        FROM dor_nal_parcels
                        WHERE folio = :folio
                        ORDER BY tax_year DESC
                        LIMIT 1
                    """),
                    {"folio": folio},
                ).fetchone()
                if not row:
                    return None
                return self._row_to_dict(row)
        except Exception as e:
            logger.debug(f"get_tax_by_folio({folio}) failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Quick checks
    # ------------------------------------------------------------------

    def is_homestead(self, strap: str) -> bool | None:
        """Quick homestead check for a property by strap.

        Returns True/False if data exists, None if data unavailable.
        Critical for lien survival analysis -- homestead affects lien priority.
        """
        if not self._available or not strap:
            return None
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text("""
                        SELECT homestead_exempt
                        FROM dor_nal_parcels
                        WHERE strap = :strap
                        ORDER BY tax_year DESC
                        LIMIT 1
                    """),
                    {"strap": strap},
                ).fetchone()
                if row is None:
                    return None
                return bool(row[0])
        except Exception as e:
            logger.debug(f"is_homestead({strap}) failed: {e}")
            return None

    def get_estimated_tax(self, strap: str) -> float | None:
        """Get estimated annual property tax by strap.

        Returns the dollar amount or None if data unavailable.
        """
        if not self._available or not strap:
            return None
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text("""
                        SELECT estimated_annual_tax
                        FROM dor_nal_parcels
                        WHERE strap = :strap
                        ORDER BY tax_year DESC
                        LIMIT 1
                    """),
                    {"strap": strap},
                ).fetchone()
                if row is None or row[0] is None:
                    return None
                return float(row[0])
        except Exception as e:
            logger.debug(f"get_estimated_tax({strap}) failed: {e}")
            return None

    def get_exemptions(self, strap: str) -> dict:
        """Get all exemption flags for a property by strap.

        Returns a dict of boolean flags. Returns empty dict if data unavailable.
        """
        if not self._available or not strap:
            return {}
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text("""
                        SELECT homestead_exempt, widow_exempt, disability_exempt,
                               veteran_exempt, ag_exempt
                        FROM dor_nal_parcels
                        WHERE strap = :strap
                        ORDER BY tax_year DESC
                        LIMIT 1
                    """),
                    {"strap": strap},
                ).fetchone()
                if row is None:
                    return {}
                return {
                    "homestead": bool(row[0]) if row[0] is not None else False,
                    "widow": bool(row[1]) if row[1] is not None else False,
                    "disability": bool(row[2]) if row[2] is not None else False,
                    "veteran": bool(row[3]) if row[3] is not None else False,
                    "ag": bool(row[4]) if row[4] is not None else False,
                }
        except Exception as e:
            logger.debug(f"get_exemptions({strap}) failed: {e}")
            return {}

    # ------------------------------------------------------------------
    # Bulk / analytical queries
    # ------------------------------------------------------------------

    def get_high_tax_properties(self, min_tax: float = 10000) -> list[dict]:
        """Properties with estimated annual tax above a threshold.

        Useful for identifying high-value foreclosure targets.
        """
        if not self._available:
            return []
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT DISTINCT ON (strap)
                            strap, folio, owner_name, property_address,
                            city, zip_code, just_value,
                            taxable_value_nonschool, estimated_annual_tax,
                            homestead_exempt, property_use_code
                        FROM dor_nal_parcels
                        WHERE estimated_annual_tax >= :min_tax
                          AND strap IS NOT NULL
                        ORDER BY strap, tax_year DESC
                    """),
                    {"min_tax": min_tax},
                ).fetchall()
                return [
                    {
                        "strap": row[0],
                        "folio": row[1],
                        "owner_name": row[2],
                        "property_address": row[3],
                        "city": row[4],
                        "zip_code": row[5],
                        "just_value": float(row[6]) if row[6] else None,
                        "taxable_value": float(row[7]) if row[7] else None,
                        "estimated_annual_tax": float(row[8]) if row[8] else None,
                        "homestead_exempt": bool(row[9]) if row[9] is not None else None,
                        "property_use_code": row[10],
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.warning(f"get_high_tax_properties(min_tax={min_tax}) failed: {e}")
            return []

    def get_homestead_properties_in_foreclosure(self, folios: list[str]) -> list[str]:
        """Given a list of folios, return those that are homesteaded.

        Critical for survival analysis -- homestead exemption affects which
        liens survive a foreclosure sale. Florida Constitution Art. X, Sec. 4
        provides unlimited homestead protection against forced sale by most
        creditors (except mortgage, tax, and certain other liens).

        Args:
            folios: List of 10-digit PG folio strings.

        Returns:
            Subset of input folios where homestead_exempt is True.
        """
        if not self._available or not folios:
            return []
        try:
            with self._engine.connect() as conn:
                # Use ANY(:folios) for efficient IN-list query
                rows = conn.execute(
                    text("""
                        SELECT DISTINCT folio
                        FROM dor_nal_parcels
                        WHERE folio = ANY(:folios)
                          AND homestead_exempt = true
                        ORDER BY folio
                    """),
                    {"folios": folios},
                ).fetchall()
                return [row[0] for row in rows]
        except Exception as e:
            logger.warning(
                f"get_homestead_properties_in_foreclosure({len(folios)} folios) failed: {e}"
            )
            return []

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_dict(row) -> dict:
        """Convert a database row tuple to a structured dict."""
        return {
            "county_code": row[0],
            "parcel_id": row[1],
            "folio": row[2],
            "strap": row[3],
            "tax_year": row[4],
            "owner_name": row[5],
            "property_address": row[6],
            "city": row[7],
            "zip_code": row[8],
            "property_use_code": row[9],
            "just_value": float(row[10]) if row[10] else None,
            "just_value_homestead": float(row[11]) if row[11] else None,
            "assessed_value_school": float(row[12]) if row[12] else None,
            "assessed_value_nonschool": float(row[13]) if row[13] else None,
            "assessed_value_homestead": float(row[14]) if row[14] else None,
            "taxable_value_school": float(row[15]) if row[15] else None,
            "taxable_value_nonschool": float(row[16]) if row[16] else None,
            "homestead_exempt": bool(row[17]) if row[17] is not None else None,
            "homestead_exempt_value": float(row[18]) if row[18] else None,
            "widow_exempt": bool(row[19]) if row[19] is not None else None,
            "widow_exempt_value": float(row[20]) if row[20] else None,
            "disability_exempt": bool(row[21]) if row[21] is not None else None,
            "disability_exempt_value": float(row[22]) if row[22] else None,
            "veteran_exempt": bool(row[23]) if row[23] is not None else None,
            "veteran_exempt_value": float(row[24]) if row[24] else None,
            "ag_exempt": bool(row[25]) if row[25] is not None else None,
            "ag_exempt_value": float(row[26]) if row[26] else None,
            "soh_differential": float(row[27]) if row[27] else None,
            "total_millage": float(row[28]) if row[28] else None,
            "county_millage": float(row[29]) if row[29] else None,
            "school_millage": float(row[30]) if row[30] else None,
            "city_millage": float(row[31]) if row[31] else None,
            "estimated_annual_tax": float(row[32]) if row[32] else None,
            "legal_description": row[33],
        }
