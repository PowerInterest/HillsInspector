"""
PostgreSQL-backed queries for the web dashboard.

Provides fuzzy search, county-wide analytics, comparable sales, subdivision
info, and multi-unit detection.  All methods degrade gracefully when PG is
unavailable (return empty lists / dicts / None).

Connection pattern mirrors src/services/pg_sales_service.py.
"""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Any

from src.utils.time import today_local

from loguru import logger
from sqlalchemy import text

from sunbiz.db import get_engine, resolve_pg_dsn

# Sale type code -> human-readable deed type
SALE_TYPE_MAP: dict[str, str] = {
    "WD": "Warranty Deed",
    "QC": "Quit Claim",
    "FD": "Foreclosure Deed",
    "TD": "Tax Deed",
    "CT": "Certificate of Title",
    "DD": "Deed",
    "TR": "Trustees Deed",
    "PR": "Personal Rep Deed",
    "GD": "Guardian Deed",
    "SD": "Sheriffs Deed",
}


class PgDashboardQueries:
    """PG-backed queries for the web dashboard.

    Instantiate once at module level; the ``available`` property tells
    callers whether PG can be reached.  Every public method returns a
    safe fallback (empty list, None, etc.) when PG is down.
    """

    def __init__(self) -> None:
        self._available = False
        self._engine = None
        try:
            dsn = resolve_pg_dsn()
            self._engine = get_engine(dsn)
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self._available = True
            logger.info("PG dashboard queries connected")
        except Exception as e:
            logger.warning(f"PG dashboard queries unavailable: {e}")

    @property
    def available(self) -> bool:
        return self._available

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------

    def _rows_to_dicts(self, rows) -> list[dict[str, Any]]:
        """Convert SQLAlchemy Row objects to plain dicts."""
        return [dict(row._mapping) for row in rows]  # noqa: SLF001

    def _resolve_pg_folio(self, conn, strap: str) -> str | None:
        """Resolve a pipeline strap to the 10-digit PG folio."""
        if not strap:
            return None
        row = conn.execute(
            text("SELECT strap_to_folio(:strap)"),
            {"strap": strap},
        ).fetchone()
        return row[0] if row else None

    def _get_parcel_info(self, conn, pg_folio: str) -> dict[str, Any] | None:
        """Get key parcel attributes for a PG folio."""
        row = conn.execute(
            text("""
                SELECT folio, strap, owner_name, property_address, city,
                       zip_code, land_use_desc, year_built, beds, baths,
                       heated_area, lot_size, just_value, market_value,
                       units, buildings, raw_legal1, raw_legal2,
                       last_sale_date, last_sale_price
                FROM hcpa_bulk_parcels
                WHERE folio = :folio
                LIMIT 1
            """),
            {"folio": pg_folio},
        ).fetchone()
        return dict(row._mapping) if row else None  # noqa: SLF001

    # =================================================================
    # Fuzzy Search
    # =================================================================

    def search_properties_fuzzy(
        self, query: str, limit: int = 25
    ) -> list[dict[str, Any]]:
        """Fuzzy search across owner name, address, and folio using pg_trgm.

        Returns list of dicts with:
            folio, strap, owner_name, property_address, city, just_value,
            similarity_score
        """
        if not self._available or not query or len(query) < 2:
            return []
        try:
            with self._engine.connect() as conn:
                # Try exact folio/strap match first
                exact = conn.execute(
                    text("""
                        SELECT folio, strap, owner_name, property_address,
                               city, just_value, 1.0::real AS similarity_score
                        FROM hcpa_bulk_parcels
                        WHERE folio = :q OR strap = :q
                        LIMIT 1
                    """),
                    {"q": query.strip()},
                ).fetchall()
                if exact:
                    return self._rows_to_dicts(exact)

                # Trigram fuzzy search on owner_name and property_address
                rows = conn.execute(
                    text("""
                        SELECT folio, strap, owner_name, property_address,
                               city, just_value,
                               GREATEST(
                                   similarity(owner_name, :q),
                                   similarity(property_address, :q)
                               ) AS similarity_score
                        FROM hcpa_bulk_parcels
                        WHERE owner_name % :q
                           OR property_address % :q
                        ORDER BY similarity_score DESC
                        LIMIT :lim
                    """),
                    {"q": query.upper(), "lim": limit},
                ).fetchall()
                return self._rows_to_dicts(rows)
        except Exception as e:
            logger.warning(f"search_properties_fuzzy failed: {e}")
            return []

    def resolve_by_name(
        self, name: str, threshold: float = 0.3
    ) -> list[dict[str, Any]]:
        """Use resolve_property_by_name() PG function for defendant/owner lookup."""
        if not self._available or not name:
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
                            NULL,
                            CAST(:threshold AS real)
                        )
                        ORDER BY match_score DESC
                        LIMIT 25
                    """),
                    {"name": name, "threshold": threshold},
                ).fetchall()
                return self._rows_to_dicts(rows)
        except Exception as e:
            logger.warning(f"resolve_by_name failed: {e}")
            return []

    # =================================================================
    # County-Wide Analytics
    # =================================================================

    def get_property_stats_by_zip(self) -> list[dict[str, Any]]:
        """Property distribution stats by zip code from bulk_parcels.

        Returns: zip_code, property_count, avg_just_value, median_just_value,
                 total_just_value
        """
        if not self._available:
            return []
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT
                            zip_code,
                            COUNT(*) AS property_count,
                            ROUND(AVG(just_value)::numeric, 0) AS avg_just_value,
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY just_value)
                                AS median_just_value,
                            SUM(just_value) AS total_just_value
                        FROM hcpa_bulk_parcels
                        WHERE zip_code IS NOT NULL
                          AND zip_code != ''
                          AND just_value > 0
                        GROUP BY zip_code
                        ORDER BY property_count DESC
                        LIMIT 50
                    """)
                ).fetchall()
                return self._rows_to_dicts(rows)
        except Exception as e:
            logger.warning(f"get_property_stats_by_zip failed: {e}")
            return []

    def get_sales_volume_by_month(
        self,
        folio: str | None = None,
        months: int = 24,
        zip_code: str | None = None,
    ) -> list[dict[str, Any]]:
        """Monthly sales volume and median price from hcpa_allsales.

        If zip_code is provided, filter directly by zip.
        Else if folio (pipeline strap) is provided, filter to that property's
        zip code for neighbourhood context.

        Returns: month, sale_count, median_price, total_volume
        """
        if not self._available:
            return []
        try:
            with self._engine.connect() as conn:
                cutoff = (today_local() - timedelta(days=months * 30)).isoformat()
                params: dict[str, Any] = {"cutoff": cutoff}

                zip_filter = ""
                if zip_code:
                    zip_filter = """
                        AND a.folio IN (
                            SELECT bp.folio FROM hcpa_bulk_parcels bp
                            WHERE bp.zip_code = :zip
                        )
                    """
                    params["zip"] = zip_code
                elif folio:
                    pg_folio = self._resolve_pg_folio(conn, folio)
                    if pg_folio:
                        info = self._get_parcel_info(conn, pg_folio)
                        if info and info.get("zip_code"):
                            zip_filter = """
                                AND a.folio IN (
                                    SELECT bp.folio FROM hcpa_bulk_parcels bp
                                    WHERE bp.zip_code = :zip
                                )
                            """
                            params["zip"] = info["zip_code"]

                rows = conn.execute(
                    text(f"""
                        SELECT
                            TO_CHAR(a.sale_date, 'YYYY-MM') AS month,
                            COUNT(*) AS sale_count,
                            PERCENTILE_CONT(0.5) WITHIN GROUP
                                (ORDER BY a.sale_amount) AS median_price,
                            SUM(a.sale_amount) AS total_volume
                        FROM hcpa_allsales a
                        WHERE a.sale_date >= CAST(:cutoff AS date)
                          AND a.sale_amount > 0
                          AND a.qualification_code = 'Q'
                          {zip_filter}
                        GROUP BY TO_CHAR(a.sale_date, 'YYYY-MM')
                        ORDER BY month
                    """),
                    params,
                ).fetchall()
                return self._rows_to_dicts(rows)
        except Exception as e:
            logger.warning(f"get_sales_volume_by_month failed: {e}")
            return []

    def get_property_value_distribution(
        self, zip_code: str | None = None
    ) -> list[dict[str, Any]]:
        """Distribution of just_value in buckets for histogram display.

        Returns: bucket_label, bucket_min, bucket_max, property_count
        """
        if not self._available:
            return []
        try:
            with self._engine.connect() as conn:
                zip_clause = ""
                params: dict[str, Any] = {}
                if zip_code:
                    zip_clause = "AND zip_code = :zip"
                    params["zip"] = zip_code

                rows = conn.execute(
                    text(f"""
                        SELECT
                            CASE
                                WHEN just_value < 100000 THEN 'Under $100K'
                                WHEN just_value < 200000 THEN '$100K-$200K'
                                WHEN just_value < 300000 THEN '$200K-$300K'
                                WHEN just_value < 400000 THEN '$300K-$400K'
                                WHEN just_value < 500000 THEN '$400K-$500K'
                                WHEN just_value < 750000 THEN '$500K-$750K'
                                WHEN just_value < 1000000 THEN '$750K-$1M'
                                ELSE '$1M+'
                            END AS bucket_label,
                            CASE
                                WHEN just_value < 100000 THEN 0
                                WHEN just_value < 200000 THEN 100000
                                WHEN just_value < 300000 THEN 200000
                                WHEN just_value < 400000 THEN 300000
                                WHEN just_value < 500000 THEN 400000
                                WHEN just_value < 750000 THEN 500000
                                WHEN just_value < 1000000 THEN 750000
                                ELSE 1000000
                            END AS bucket_min,
                            COUNT(*) AS property_count
                        FROM hcpa_bulk_parcels
                        WHERE just_value > 0
                          {zip_clause}
                        GROUP BY bucket_label, bucket_min
                        ORDER BY bucket_min
                    """),
                    params,
                ).fetchall()
                return self._rows_to_dicts(rows)
        except Exception as e:
            logger.warning(f"get_property_value_distribution failed: {e}")
            return []

    # =================================================================
    # Comparable Sales
    # =================================================================

    def get_comparable_sales(
        self, folio: str, years: int = 3, limit: int = 10
    ) -> list[dict[str, Any]]:
        """Find recent qualified sales of similar properties near the target.

        Strategy: same subdivision first, then same zip code, matching by
        land use.

        Args:
            folio: Pipeline strap (auctions.folio / parcel_id format)
            years: How many years back to search
            limit: Max results

        Returns list of dicts with:
            sale_date, sale_amount, sale_type, sale_type_desc, grantor,
            grantee, property_address, city, zip_code, beds, baths,
            heated_area, year_built, just_value
        """
        if not self._available or not folio:
            return []
        try:
            with self._engine.connect() as conn:
                pg_folio = self._resolve_pg_folio(conn, folio)
                if not pg_folio:
                    return []

                info = self._get_parcel_info(conn, pg_folio)
                if not info:
                    return []

                cutoff = (
                    today_local() - timedelta(days=years * 365)
                ).isoformat()

                # First try: same subdivision (via raw_legal1 prefix)
                comps: list[dict[str, Any]] = []
                legal1 = (info.get("raw_legal1") or "").strip()

                if legal1:
                    # Extract subdivision name (first line of legal desc
                    # typically starts with subdivision name)
                    sub_prefix = legal1.split(" LOT")[0].split(" BLK")[0].strip()
                    if len(sub_prefix) > 5:
                        rows = conn.execute(
                            text("""
                                SELECT
                                    a.sale_date, a.sale_amount, a.sale_type,
                                    a.grantor, a.grantee,
                                    bp.property_address, bp.city, bp.zip_code,
                                    bp.beds, bp.baths, bp.heated_area,
                                    bp.year_built, bp.just_value
                                FROM hcpa_allsales a
                                JOIN hcpa_bulk_parcels bp ON a.folio = bp.folio
                                WHERE a.sale_date >= CAST(:cutoff AS date)
                                  AND a.sale_amount > 0
                                  AND a.qualification_code = 'Q'
                                  AND a.folio != :folio
                                  AND bp.raw_legal1 ILIKE :sub_prefix || '%%'
                                ORDER BY a.sale_date DESC
                                LIMIT :lim
                            """),
                            {
                                "cutoff": cutoff,
                                "folio": pg_folio,
                                "sub_prefix": sub_prefix,
                                "lim": limit,
                            },
                        ).fetchall()
                        comps = self._rows_to_dicts(rows)

                # Second try: same zip + land use if we need more
                if len(comps) < limit and info.get("zip_code"):
                    remaining = limit - len(comps)
                    seen_folios = {c.get("folio") for c in comps}

                    land_use_clause = ""
                    params: dict[str, Any] = {
                        "cutoff": cutoff,
                        "folio": pg_folio,
                        "zip": info["zip_code"],
                        "lim": remaining,
                    }
                    if info.get("land_use_desc"):
                        land_use_clause = "AND bp.land_use_desc = :land_use"
                        params["land_use"] = info["land_use_desc"]

                    rows = conn.execute(
                        text(f"""
                            SELECT
                                a.sale_date, a.sale_amount, a.sale_type,
                                a.grantor, a.grantee,
                                bp.property_address, bp.city, bp.zip_code,
                                bp.beds, bp.baths, bp.heated_area,
                                bp.year_built, bp.just_value, a.folio
                            FROM hcpa_allsales a
                            JOIN hcpa_bulk_parcels bp ON a.folio = bp.folio
                            WHERE a.sale_date >= CAST(:cutoff AS date)
                              AND a.sale_amount > 0
                              AND a.qualification_code = 'Q'
                              AND a.folio != :folio
                              AND bp.zip_code = :zip
                              {land_use_clause}
                            ORDER BY a.sale_date DESC
                            LIMIT :lim
                        """),
                        params,
                    ).fetchall()

                    for row in self._rows_to_dicts(rows):
                        if row.get("folio") not in seen_folios:
                            comps.append(row)

                # Add human-readable sale type
                for c in comps:
                    c["sale_type_desc"] = SALE_TYPE_MAP.get(
                        c.get("sale_type", ""), c.get("sale_type", "")
                    )

                return comps[:limit]
        except Exception as e:
            logger.warning(f"get_comparable_sales({folio}) failed: {e}")
            return []

    def get_sales_history(self, folio: str) -> list[dict[str, Any]]:
        """Complete sales chain for a property from hcpa_allsales.

        Args:
            folio: Pipeline strap (auctions.folio / parcel_id format)

        Returns list of dicts with sale_date, sale_amount, sale_type,
            sale_type_desc, grantor, grantee, or_book, or_page, doc_num,
            qualification_code
        """
        if not self._available or not folio:
            return []
        try:
            with self._engine.connect() as conn:
                pg_folio = self._resolve_pg_folio(conn, folio)
                if not pg_folio:
                    return []

                rows = conn.execute(
                    text("""
                        SELECT sale_date, sale_type, sale_amount,
                               grantor, grantee,
                               or_book, or_page, doc_num,
                               qualification_code
                        FROM hcpa_allsales
                        WHERE folio = :folio
                        ORDER BY sale_date DESC
                    """),
                    {"folio": pg_folio},
                ).fetchall()

                results = self._rows_to_dicts(rows)
                for r in results:
                    r["sale_type_desc"] = SALE_TYPE_MAP.get(
                        r.get("sale_type", ""), r.get("sale_type", "")
                    )
                return results
        except Exception as e:
            logger.warning(f"get_sales_history({folio}) failed: {e}")
            return []

    # =================================================================
    # Subdivision Analytics
    # =================================================================

    def get_subdivision_info(self, folio: str) -> dict[str, Any] | None:
        """Get subdivision name, plat book/page for a property.

        Joins hcpa_bulk_parcels -> hcpa_allsales.sub_code ->
        hcpa_parcel_sub_names.

        Args:
            folio: Pipeline strap format

        Returns dict with sub_code, sub_name, plat_bk, page, or None
        """
        if not self._available or not folio:
            return None
        try:
            with self._engine.connect() as conn:
                pg_folio = self._resolve_pg_folio(conn, folio)
                if not pg_folio:
                    return None

                # Get subdivision code from the most recent sale
                row = conn.execute(
                    text("""
                        SELECT DISTINCT a.sub_code, s.sub_name, s.plat_bk, s.page
                        FROM hcpa_allsales a
                        JOIN hcpa_parcel_sub_names s ON a.sub_code = s.sub_code
                        WHERE a.folio = :folio
                          AND a.sub_code IS NOT NULL
                          AND a.sub_code != ''
                        ORDER BY a.sub_code
                        LIMIT 1
                    """),
                    {"folio": pg_folio},
                ).fetchone()

                if row:
                    return dict(row._mapping)  # noqa: SLF001

                # Fallback: try to match subdivision from legal description
                parcel = self._get_parcel_info(conn, pg_folio)
                if parcel and parcel.get("raw_legal1"):
                    legal1 = parcel["raw_legal1"].strip()
                    # Take first word-group before LOT/BLK/UNIT
                    sub_name = legal1.split(" LOT")[0].split(" BLK")[0].split(" UNIT")[0].strip()
                    if len(sub_name) > 3:
                        match = conn.execute(
                            text("""
                                SELECT sub_code, sub_name, plat_bk, page
                                FROM hcpa_parcel_sub_names
                                WHERE sub_name ILIKE :name || '%%'
                                LIMIT 1
                            """),
                            {"name": sub_name},
                        ).fetchone()
                        if match:
                            return dict(match._mapping)  # noqa: SLF001
                return None
        except Exception as e:
            logger.warning(f"get_subdivision_info({folio}) failed: {e}")
            return None

    def get_subdivision_properties(
        self, subdivision_code: str, limit: int = 50
    ) -> list[dict[str, Any]]:
        """All properties in a subdivision with valuations.

        Returns list of dicts with folio, strap, property_address, owner_name,
            beds, baths, heated_area, year_built, just_value, market_value
        """
        if not self._available or not subdivision_code:
            return []
        try:
            with self._engine.connect() as conn:
                # Get folios in this subdivision from sales
                rows = conn.execute(
                    text("""
                        SELECT DISTINCT bp.folio, bp.strap,
                               bp.property_address, bp.owner_name,
                               bp.beds, bp.baths, bp.heated_area,
                               bp.year_built, bp.just_value, bp.market_value
                        FROM hcpa_allsales a
                        JOIN hcpa_bulk_parcels bp ON a.folio = bp.folio
                        WHERE a.sub_code = :sub_code
                        ORDER BY bp.property_address
                        LIMIT :lim
                    """),
                    {"sub_code": subdivision_code, "lim": limit},
                ).fetchall()
                return self._rows_to_dicts(rows)
        except Exception as e:
            logger.warning(f"get_subdivision_properties failed: {e}")
            return []

    # =================================================================
    # Multi-Unit Detection
    # =================================================================

    def is_multi_unit(self, strap: str) -> dict[str, Any] | None:
        """Check if property is multi-unit from bulk_parcels.

        Returns dict with folio, strap, units, buildings, land_use_desc
        or None if not multi-unit / not found.
        """
        if not self._available or not strap:
            return None
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text("""
                        SELECT folio, strap, units, buildings, land_use_desc
                        FROM hcpa_bulk_parcels
                        WHERE strap = :strap
                          AND (units > 1 OR land_use_desc ILIKE '%%MULTI%%'
                               OR land_use_desc ILIKE '%%CONDO%%')
                        LIMIT 1
                    """),
                    {"strap": strap},
                ).fetchone()
                return dict(row._mapping) if row else None  # noqa: SLF001
        except Exception as e:
            logger.warning(f"is_multi_unit({strap}) failed: {e}")
            return None

    def get_units_for_property(self, folio: str) -> list[dict[str, Any]]:
        """If multi-unit, get all unit folios/straps sharing the same address.

        Uses property_address matching on bulk_parcels to find sibling units.

        Args:
            folio: Pipeline strap format

        Returns list of dicts with folio, strap, owner_name, units, just_value
        """
        if not self._available or not folio:
            return []
        try:
            with self._engine.connect() as conn:
                # Get the property address first
                base = conn.execute(
                    text("""
                        SELECT property_address, city, zip_code
                        FROM hcpa_bulk_parcels
                        WHERE strap = :strap
                        LIMIT 1
                    """),
                    {"strap": folio},
                ).fetchone()
                if not base or not base[0]:
                    return []

                address = base[0].strip()
                # Strip unit/apt suffixes to find siblings
                # e.g. "123 MAIN ST APT 101" -> "123 MAIN ST"
                base_address = re.split(
                    r'\s+(APT|UNIT|STE|#)\s*', address, flags=re.IGNORECASE
                )[0].strip()

                if len(base_address) < 5:
                    return []

                rows = conn.execute(
                    text("""
                        SELECT folio, strap, owner_name, property_address,
                               units, just_value
                        FROM hcpa_bulk_parcels
                        WHERE property_address ILIKE :base || '%%'
                          AND city = :city
                        ORDER BY property_address
                        LIMIT 50
                    """),
                    {"base": base_address, "city": base[1]},
                ).fetchall()
                return self._rows_to_dicts(rows)
        except Exception as e:
            logger.warning(f"get_units_for_property({folio}) failed: {e}")
            return []

    # =================================================================
    # Parcel Lookup (direct)
    # =================================================================

    def get_parcel_by_strap(self, strap: str) -> dict[str, Any] | None:
        """Get full parcel info by pipeline strap."""
        if not self._available or not strap:
            return None
        try:
            with self._engine.connect() as conn:
                pg_folio = self._resolve_pg_folio(conn, strap)
                if not pg_folio:
                    return None
                return self._get_parcel_info(conn, pg_folio)
        except Exception as e:
            logger.warning(f"get_parcel_by_strap({strap}) failed: {e}")
            return None

    # =================================================================
    # Market Snapshot (from property_market)
    # =================================================================

    def get_pg_market_snapshot(self, strap: str) -> dict[str, Any] | None:
        """Consolidated market snapshot from PG property_market table.

        Returns the full row as a dict, with ``photo_local_paths`` and
        ``photo_cdn_urls`` already decoded from JSONB.
        """
        if not self._available or not strap:
            return None
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text("""
                        SELECT strap, folio, case_number,
                               zestimate, rent_zestimate, list_price, tax_assessed_value,
                               beds, baths, sqft, year_built, lot_size, property_type,
                               listing_status, detail_url,
                               photo_local_paths, photo_cdn_urls,
                               zillow_json, redfin_json, homeharvest_json,
                               primary_source, updated_at
                        FROM property_market
                        WHERE strap = :strap
                        LIMIT 1
                    """),
                    {"strap": strap},
                ).fetchone()
                if not row:
                    return None
                d = dict(row._mapping)  # noqa: SLF001
                # Ensure JSONB arrays are Python lists
                for key in ("photo_local_paths", "photo_cdn_urls"):
                    if d.get(key) is None:
                        d[key] = []
                return d
        except Exception as e:
            logger.warning(f"get_pg_market_snapshot({strap}) failed: {e}")
            return None

    def get_pg_bulk_thumbnails(self, straps: list[str]) -> dict[str, str]:
        """Batch lookup: strap -> first local photo path from PG property_market.

        Returns ``{strap: "Foreclosure/{case}/photos/001_abc.jpg"}`` for properties
        that have at least one local photo.
        """
        if not self._available or not straps:
            return {}
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT strap, photo_local_paths->>0 AS thumb
                        FROM property_market
                        WHERE strap = ANY(:straps)
                          AND jsonb_array_length(COALESCE(photo_local_paths, '[]'::jsonb)) > 0
                    """),
                    {"straps": straps},
                ).fetchall()
                return {r[0]: r[1] for r in rows if r[1]}
        except Exception as e:
            logger.warning(f"get_pg_bulk_thumbnails failed: {e}")
            return {}

    def get_foreclosure_deed_stats(
        self, months: int = 12
    ) -> list[dict[str, Any]]:
        """Foreclosure deed volume by month.

        Returns: month, fd_count, td_count, avg_fd_amount
        """
        if not self._available:
            return []
        try:
            with self._engine.connect() as conn:
                cutoff = (
                    today_local() - timedelta(days=months * 30)
                ).isoformat()
                rows = conn.execute(
                    text("""
                        SELECT
                            TO_CHAR(sale_date, 'YYYY-MM') AS month,
                            SUM(CASE WHEN sale_type = 'FD' THEN 1 ELSE 0 END)
                                AS fd_count,
                            SUM(CASE WHEN sale_type = 'TD' THEN 1 ELSE 0 END)
                                AS td_count,
                            ROUND(AVG(CASE WHEN sale_type = 'FD'
                                  AND sale_amount > 0
                                  THEN sale_amount END)::numeric, 0)
                                AS avg_fd_amount
                        FROM hcpa_allsales
                        WHERE sale_date >= CAST(:cutoff AS date)
                          AND sale_type IN ('FD', 'TD')
                        GROUP BY TO_CHAR(sale_date, 'YYYY-MM')
                        ORDER BY month
                    """),
                    {"cutoff": cutoff},
                ).fetchall()
                return self._rows_to_dicts(rows)
        except Exception as e:
            logger.warning(f"get_foreclosure_deed_stats failed: {e}")
            return []


# ---------------------------------------------------------------------------
# Module-level singleton (lazy init on first import)
# ---------------------------------------------------------------------------

_instance: PgDashboardQueries | None = None


def get_pg_queries() -> PgDashboardQueries:
    """Get or create the module-level PgDashboardQueries singleton."""
    global _instance
    if _instance is None:
        _instance = PgDashboardQueries()
    return _instance
