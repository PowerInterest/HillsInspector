"""
PG Hydration Service — batch-queries PostgreSQL and populates SQLite.

Runs ONCE before the per-property loop, replacing HCPA GIS scraping
(Step 3) for properties where PG data is available. Populates:
- parcels: address, owner, legal desc, specs, values, homestead, tax data
- sales_history: full deed chain from hcpa_allsales
- clerk case status from clerk_civil_cases
- federal lien count from sunbiz_flr_filings

Graceful degradation: if PG is unavailable, logs a warning and returns
empty stats — pipeline falls back to HCPA GIS scraping as before.
"""

from __future__ import annotations

from loguru import logger
from sqlalchemy import text

from sunbiz.db import get_engine, resolve_pg_dsn


class PgHydrateService:
    """Batch PG hydration: query PG once, write to SQLite for all auctions."""

    def __init__(self, db, dsn: str | None = None):
        """
        Args:
            db: PropertyDB instance for SQLite writes.
            dsn: Optional PG DSN override.
        """
        self.db = db
        self._available = False
        try:
            resolved = resolve_pg_dsn(dsn)
            self._engine = get_engine(resolved)
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1 FROM hcpa_bulk_parcels LIMIT 0"))
            self._available = True
            logger.info("PG hydration service connected")
        except Exception as e:
            logger.warning(f"PG hydration service unavailable: {e}")
            self._engine = None

    @property
    def available(self) -> bool:
        return self._available

    def hydrate_auctions(self, auctions: list[dict]) -> dict:
        """Batch-hydrate all auctions from PG data.

        Args:
            auctions: List of auction dicts, each with at least
                'case_number' and 'parcel_id' (strap format).

        Returns:
            Stats dict: parcels_hydrated, sales_inserted, homesteaded,
            clerk_statuses, federal_liens.
        """
        if not self._available:
            logger.warning("PG hydration skipped — PostgreSQL unavailable")
            return {"parcels_hydrated": 0, "pg_available": False}

        # Collect unique straps and case numbers
        strap_to_cases: dict[str, list[str]] = {}
        for a in auctions:
            strap = a.get("parcel_id") or a.get("folio")
            case = a.get("case_number")
            if strap and case:
                strap_to_cases.setdefault(strap, []).append(case)

        straps = list(strap_to_cases.keys())
        if not straps:
            logger.info("PG hydration: no valid straps to hydrate")
            return {"parcels_hydrated": 0, "no_straps": True}

        logger.info(f"PG hydration: hydrating {len(straps)} properties...")

        stats = {
            "parcels_hydrated": 0,
            "sales_inserted": 0,
            "homesteaded": 0,
            "clerk_statuses": 0,
            "federal_liens": 0,
        }

        with self._engine.connect() as pg_conn:
            # 1. Bulk parcels: address, owner, specs, values, legal desc
            parcel_rows = self._fetch_bulk_parcels(pg_conn, straps)

            # 2. Tax/NAL data: homestead, exemptions, estimated tax
            tax_rows = self._fetch_nal_tax(pg_conn, straps)

            # 3. Sales history
            sales_by_strap = self._fetch_sales(pg_conn, straps)

            # 4. Clerk case status
            case_numbers = []
            for cases in strap_to_cases.values():
                case_numbers.extend(cases)
            clerk_statuses = self._fetch_clerk_statuses(pg_conn, case_numbers)

            # 5. Federal lien counts by owner name
            owner_names: list[str] = [r["owner_name"] for r in parcel_rows.values() if r.get("owner_name")]
            federal_liens = self._fetch_federal_lien_counts(pg_conn, owner_names)

        # Merge and write to SQLite
        upsert_rows = []
        for strap in straps:
            parcel = parcel_rows.get(strap, {})
            tax = tax_rows.get(strap, {})

            # The pipeline uses strap as the folio (primary key) in SQLite
            folio = strap

            row = {
                "folio": folio,
                "strap": strap,
                "owner_name": parcel.get("owner_name"),
                "property_address": parcel.get("property_address"),
                "city": parcel.get("city"),
                "zip_code": parcel.get("zip_code"),
                "legal_description": parcel.get("raw_legal1"),
                "year_built": parcel.get("year_built"),
                "beds": parcel.get("beds"),
                "baths": parcel.get("baths"),
                "heated_area": parcel.get("heated_area"),
                "lot_size": parcel.get("lot_size"),
                "assessed_value": parcel.get("assessed_value"),
                "market_value": parcel.get("market_value"),
                # Tax / NAL data
                "just_value": tax.get("just_value"),
                "taxable_value": tax.get("taxable_value"),
                "homestead_exempt": 1 if tax.get("homestead_exempt") else 0,
                "homestead_exempt_value": tax.get("homestead_exempt_value"),
                "estimated_annual_tax": tax.get("estimated_annual_tax"),
                "soh_differential": tax.get("soh_differential"),
                "property_use_code": tax.get("property_use_code"),
                # Clerk status (pick first matching case)
                "clerk_case_status": None,
                # Federal liens
                "federal_lien_count": 0,
            }

            # Clerk status: find status for any case associated with this strap
            for case_num in strap_to_cases.get(strap, []):
                status = clerk_statuses.get(case_num)
                if status:
                    row["clerk_case_status"] = status
                    stats["clerk_statuses"] += 1
                    break

            # Federal lien count by owner name
            owner = parcel.get("owner_name")
            if owner and owner in federal_liens:
                row["federal_lien_count"] = federal_liens[owner]
                if federal_liens[owner] > 0:
                    stats["federal_liens"] += 1

            if tax.get("homestead_exempt"):
                stats["homesteaded"] += 1

            upsert_rows.append(row)

        # Write to SQLite
        stats["parcels_hydrated"] = self.db.batch_upsert_parcels_from_pg(upsert_rows)

        # Sales history
        for strap, sales in sales_by_strap.items():
            folio = strap
            inserted = self.db.batch_insert_sales_from_pg(folio, sales)
            stats["sales_inserted"] += inserted

        self.db.checkpoint()

        logger.success(
            f"PG hydration complete: {stats['parcels_hydrated']} parcels, "
            f"{stats['sales_inserted']} sales, "
            f"{stats['homesteaded']} homesteaded, "
            f"{stats['clerk_statuses']} clerk statuses, "
            f"{stats['federal_liens']} with federal liens"
        )
        return stats

    # ------------------------------------------------------------------
    # PG query methods
    # ------------------------------------------------------------------

    def _fetch_bulk_parcels(self, pg_conn, straps: list[str]) -> dict[str, dict]:
        """Fetch parcel data from hcpa_bulk_parcels by strap."""
        if not straps:
            return {}
        try:
            rows = pg_conn.execute(
                text("""
                    SELECT strap, owner_name, property_address, city, zip_code,
                           raw_legal1, year_built, beds, baths,
                           heated_area, lot_size,
                           assessed_value, market_value
                    FROM hcpa_bulk_parcels
                    WHERE strap = ANY(:straps)
                """),
                {"straps": straps},
            ).fetchall()
            result = {}
            for r in rows:
                result[r[0]] = {
                    "strap": r[0],
                    "owner_name": r[1],
                    "property_address": r[2],
                    "city": r[3],
                    "zip_code": r[4],
                    "raw_legal1": r[5],
                    "year_built": int(r[6]) if r[6] is not None else None,
                    "beds": float(r[7]) if r[7] is not None else None,
                    "baths": float(r[8]) if r[8] is not None else None,
                    "heated_area": float(r[9]) if r[9] is not None else None,
                    "lot_size": float(r[10]) if r[10] is not None else None,
                    "assessed_value": float(r[11]) if r[11] is not None else None,
                    "market_value": float(r[12]) if r[12] is not None else None,
                }
            logger.debug(f"PG hydration: fetched {len(result)} bulk parcels")
            return result
        except Exception as e:
            logger.warning(f"PG hydration: bulk parcels query failed: {e}")
            return {}

    def _fetch_nal_tax(self, pg_conn, straps: list[str]) -> dict[str, dict]:
        """Fetch tax/exemption data from dor_nal_parcels by strap."""
        if not straps:
            return {}
        try:
            rows = pg_conn.execute(
                text("""
                    SELECT DISTINCT ON (strap)
                        strap, homestead_exempt, homestead_exempt_value,
                        just_value, taxable_value_nonschool,
                        estimated_annual_tax, soh_differential,
                        property_use_code
                    FROM dor_nal_parcels
                    WHERE strap = ANY(:straps)
                    ORDER BY strap, tax_year DESC
                """),
                {"straps": straps},
            ).fetchall()
            result = {}
            for r in rows:
                result[r[0]] = {
                    "homestead_exempt": bool(r[1]) if r[1] is not None else False,
                    "homestead_exempt_value": float(r[2]) if r[2] is not None else None,
                    "just_value": float(r[3]) if r[3] is not None else None,
                    "taxable_value": float(r[4]) if r[4] is not None else None,
                    "estimated_annual_tax": float(r[5]) if r[5] is not None else None,
                    "soh_differential": float(r[6]) if r[6] is not None else None,
                    "property_use_code": r[7],
                }
            logger.debug(f"PG hydration: fetched {len(result)} NAL tax records")
            return result
        except Exception as e:
            logger.warning(f"PG hydration: NAL tax query failed: {e}")
            return {}

    def _fetch_sales(self, pg_conn, straps: list[str]) -> dict[str, list[dict]]:
        """Fetch sales history from hcpa_allsales by strap (via bulk_parcels folio join)."""
        if not straps:
            return {}
        try:
            # Pipeline uses strap as folio, but hcpa_allsales uses 10-digit folio.
            # Join through hcpa_bulk_parcels to translate strap → PG folio.
            rows = pg_conn.execute(
                text("""
                    SELECT bp.strap,
                           s.sale_date, s.sale_type, s.sale_amount,
                           s.qualification_code, s.grantor, s.grantee,
                           s.or_book, s.or_page, s.doc_num
                    FROM hcpa_bulk_parcels bp
                    JOIN hcpa_allsales s ON s.folio = bp.folio
                    WHERE bp.strap = ANY(:straps)
                    ORDER BY bp.strap, s.sale_date DESC
                """),
                {"straps": straps},
            ).fetchall()
            result: dict[str, list[dict]] = {}
            for r in rows:
                strap = r[0]
                sale = {
                    "strap": strap,
                    "sale_date": str(r[1]) if r[1] else None,
                    "sale_type": r[2],
                    "sale_price": float(r[3]) if r[3] is not None else None,
                    "qualified": r[4],
                    "grantor": r[5],
                    "grantee": r[6],
                    "or_book": r[7],
                    "or_page": r[8],
                    "instrument": r[9],
                }
                result.setdefault(strap, []).append(sale)
            total_sales = sum(len(v) for v in result.values())
            logger.debug(f"PG hydration: fetched {total_sales} sales for {len(result)} properties")
            return result
        except Exception as e:
            logger.warning(f"PG hydration: sales query failed: {e}")
            return {}

    def _fetch_clerk_statuses(
        self, pg_conn, case_numbers: list[str]
    ) -> dict[str, str]:
        """Fetch case status from clerk_civil_cases for pipeline case numbers.

        Converts pipeline format (29YYYYTTNNNNNN) to clerk format (YY-TT-NNNNNN).
        """
        if not case_numbers:
            return {}

        # Convert pipeline case numbers to clerk format
        clerk_map: dict[str, str] = {}  # clerk_num → pipeline_num
        for cn in case_numbers:
            clerk_num = self._normalize_case_number(cn)
            if clerk_num:
                clerk_map[clerk_num] = cn

        if not clerk_map:
            return {}

        try:
            clerk_nums = list(clerk_map.keys())
            rows = pg_conn.execute(
                text("""
                    SELECT case_number, case_status
                    FROM clerk_civil_cases
                    WHERE case_number = ANY(:nums)
                """),
                {"nums": clerk_nums},
            ).fetchall()
            result: dict[str, str] = {}
            for r in rows:
                pipeline_cn = clerk_map.get(r[0])
                if pipeline_cn and r[1]:
                    result[pipeline_cn] = r[1]
            logger.debug(f"PG hydration: fetched {len(result)} clerk case statuses")
            return result
        except Exception as e:
            logger.warning(f"PG hydration: clerk status query failed: {e}")
            return {}

    def _fetch_federal_lien_counts(
        self, pg_conn, owner_names: list[str]
    ) -> dict[str, int]:
        """Count active federal liens per owner name from sunbiz_flr tables."""
        if not owner_names:
            return {}
        try:
            # Batch query: count active federal filings per debtor name
            rows = pg_conn.execute(
                text("""
                    SELECT dp.name, COUNT(DISTINCT f.doc_number)
                    FROM sunbiz_flr_parties dp
                    JOIN sunbiz_flr_filings f ON f.doc_number = dp.doc_number
                    WHERE dp.party_role = 'debtor'
                      AND dp.name = ANY(:names)
                      AND f.filing_type = 'F'
                      AND f.filing_status = 'A'
                    GROUP BY dp.name
                """),
                {"names": [n.upper() for n in owner_names]},
            ).fetchall()
            result = {r[0]: int(r[1]) for r in rows}
            if result:
                logger.debug(
                    f"PG hydration: found {sum(result.values())} federal liens "
                    f"for {len(result)} owners"
                )
            return result
        except Exception as e:
            logger.warning(f"PG hydration: federal lien query failed: {e}")
            return {}

    @staticmethod
    def _normalize_case_number(case_number: str) -> str | None:
        """Convert pipeline case_number (29YYYYTTNNNNNN) to clerk format (YY-TT-NNNNNN)."""
        if not case_number:
            return None
        if "-" in case_number and len(case_number) <= 16:
            return case_number
        cleaned = case_number.strip()
        if len(cleaned) >= 14 and cleaned[:2] == "29":
            year_short = cleaned[4:6]
            case_type = cleaned[6:8]
            seq = cleaned[8:14]
            return f"{year_short}-{case_type}-{seq}"
        return None
