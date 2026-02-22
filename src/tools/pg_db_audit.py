"""
PostgreSQL Database Audit Script for HillsInspector Pipeline.
Provides comprehensive statistics on data completeness across the PG schema.
"""

import sys
from pathlib import Path

from loguru import logger

# Add project root to sys.path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from sqlalchemy import text
from sunbiz.db import get_engine, resolve_pg_dsn
from src.utils.time import today_local


def _fetchone_value(conn, query: str, params: dict | None = None, default: int = 0) -> int:
    try:
        result = conn.execute(text(query), params or {}).scalar()
        return result if result is not None else default
    except Exception as e:
        logger.error(f"Error fetching value for query '{query}': {e}")
        return default


def _fetchone_row(conn, query: str, params: dict | None = None, default=None):
    try:
        row = conn.execute(text(query), params or {}).mappings().first()
        return dict(row) if row is not None else default
    except Exception as e:
        logger.error(f"Error fetching row for query '{query}': {e}")
        return default


def _table_exists(conn, table_name: str) -> bool:
    try:
        row = conn.execute(
            text("SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=:name"),
            {"name": table_name},
        ).scalar()
        return bool(row)
    except Exception as e:
        logger.error(f"Error checking if table {table_name} exists: {e}")
        return False


def audit_database():
    try:
        engine = get_engine(resolve_pg_dsn())
        with engine.connect() as conn:
            logger.info("=" * 60)
            logger.info("         HILLSINSPECTOR POSTGRESQL API DB AUDIT REPORT")
            logger.info("=" * 60)

            # ==========================================================================
            # FORECLOSURES
            # ==========================================================================
            logger.info("\n[STEP 1] FORECLOSURES")
            logger.info("-" * 40)
            try:
                total_auctions = _fetchone_value(conn, "SELECT COUNT(*) FROM foreclosures")
                logger.info(f"Total Foreclosures: {total_auctions}")

                by_type = (
                    conn.execute(
                        text("""
                    SELECT auction_type, COUNT(*) as cnt
                    FROM foreclosures
                    GROUP BY auction_type
                """)
                    )
                    .mappings()
                    .all()
                )
                for row in by_type:
                    logger.info(f"  - {row['auction_type'] or 'UNKNOWN'}: {row['cnt']}")

                date_range = _fetchone_row(
                    conn,
                    "SELECT MIN(auction_date) as min_d, MAX(auction_date) as max_d FROM foreclosures",
                    default={"min_d": None, "max_d": None},
                )
                logger.info(f"Date Range: {date_range['min_d']} to {date_range['max_d']}")

                today = today_local()
                upcoming = _fetchone_value(conn, "SELECT COUNT(*) FROM foreclosures WHERE auction_date >= :t", {"t": str(today)})
                past = _fetchone_value(conn, "SELECT COUNT(*) FROM foreclosures WHERE auction_date < :t", {"t": str(today)})
                logger.info(f"Upcoming: {upcoming} | Past: {past}")

                valid_parcels = _fetchone_value(
                    conn,
                    """
                    SELECT COUNT(*) FROM foreclosures
                    WHERE strap IS NOT NULL
                      AND strap != ''
                      AND LOWER(strap) NOT IN ('n/a', 'none', 'unknown', 'property appraiser')
                    """,
                )
                logger.info(f"With Valid Parcel Strap: {valid_parcels} ({total_auctions - valid_parcels} invalid/missing)")

            except Exception as e:
                logger.error(f"Error: {e}")

            # ==========================================================================
            # PIPELINE STATE
            # ==========================================================================
            logger.info("\n[STEP 2] PIPELINE EXTRACTION STATUS")
            logger.info("-" * 40)
            try:
                has_pdf = _fetchone_value(conn, "SELECT COUNT(*) FROM foreclosures WHERE pdf_path IS NOT NULL")
                has_judgment_data = _fetchone_value(conn, "SELECT COUNT(*) FROM foreclosures WHERE judgment_data IS NOT NULL")
                has_amounts = _fetchone_value(conn, "SELECT COUNT(*) FROM foreclosures WHERE final_judgment_amount IS NOT NULL")

                logger.info(f"Has PDF Downloaded: {has_pdf}")
                logger.info(f"Has Judgment JSON Data: {has_judgment_data}")
                logger.info(f"Has Judgment Amount: {has_amounts}")
            except Exception as e:
                logger.error(f"Error: {e}")

            # ==========================================================================
            # BULK ENRICHMENT (PARCELS)
            # ==========================================================================
            logger.info("\n[STEP 3] HCPA BULK PARCELS")
            logger.info("-" * 40)
            try:
                if _table_exists(conn, "hcpa_bulk_parcels"):
                    total_parcels = _fetchone_value(conn, "SELECT COUNT(*) FROM hcpa_bulk_parcels")
                    logger.info(f"Total Parcels: {total_parcels}")

                    has_owner = _fetchone_value(
                        conn, "SELECT COUNT(*) FROM hcpa_bulk_parcels WHERE owner_name IS NOT NULL AND owner_name != ''"
                    )
                    has_address = _fetchone_value(
                        conn,
                        "SELECT COUNT(*) FROM hcpa_bulk_parcels WHERE property_address IS NOT NULL AND property_address != ''",
                    )
                    has_coords = _fetchone_value(conn, "SELECT COUNT(*) FROM hcpa_latlon")
                    has_legal = _fetchone_value(
                        conn, "SELECT COUNT(*) FROM hcpa_bulk_parcels WHERE raw_legal1 IS NOT NULL AND raw_legal1 != ''"
                    )

                    logger.info(f"With Owner Name: {has_owner} ({total_parcels - has_owner} missing)")
                    logger.info(f"With Address: {has_address} ({total_parcels - has_address} missing)")
                    logger.info(f"With Coordinates (inc. hcpa_latlon): {has_coords}")
                    logger.info(f"With Legal Description: {has_legal} ({total_parcels - has_legal} missing)")
                else:
                    logger.warning("Table 'hcpa_bulk_parcels' does not exist in schema.")
            except Exception as e:
                logger.error(f"Error: {e}")

            # ==========================================================================
            # MARKET DATA
            # ==========================================================================
            logger.info("\n[STEP 4] PROPERTY MARKET & SALES")
            logger.info("-" * 40)
            try:
                if _table_exists(conn, "property_market"):
                    total_market = _fetchone_value(conn, "SELECT COUNT(*) FROM property_market")
                    with_zestimate = _fetchone_value(conn, "SELECT COUNT(*) FROM property_market WHERE zestimate IS NOT NULL")
                    logger.info(f"Total Property Market Records: {total_market}")
                    logger.info(f"With Zestimate: {with_zestimate}")
                else:
                    logger.warning("Table 'property_market' does not exist in schema.")

                if _table_exists(conn, "hcpa_allsales"):
                    total_sales = _fetchone_value(conn, "SELECT COUNT(*) FROM hcpa_allsales")
                    logger.info(f"Total HCPA Sales Records: {total_sales}")
                else:
                    logger.warning("Table 'hcpa_allsales' does not exist in schema.")
            except Exception as e:
                logger.error(f"Error: {e}")

            # ==========================================================================
            # CIVIL CASES & TRUST ACCOUNTS
            # ==========================================================================
            logger.info("\n[STEP 5] CLERK CIVIL CASES & TRUST ACCOUNTS")
            logger.info("-" * 40)
            try:
                if _table_exists(conn, "clerk_civil_cases"):
                    total_cases = _fetchone_value(conn, "SELECT COUNT(*) FROM clerk_civil_cases")
                    foreclosures = _fetchone_value(conn, "SELECT COUNT(*) FROM clerk_civil_cases WHERE is_foreclosure = true")
                    logger.info(f"Total Civil Cases: {total_cases}")
                    logger.info(f"Foreclosure Cases: {foreclosures}")
                else:
                    logger.warning("Table 'clerk_civil_cases' does not exist in schema.")

                if _table_exists(conn, "TrustAccount"):
                    total_trust = _fetchone_value(conn, 'SELECT COUNT(*) FROM "TrustAccount"')
                    logger.info(f"Trust Account Snapshots: {total_trust}")
                else:
                    logger.warning("Table 'TrustAccount' does not exist in schema.")
            except Exception as e:
                logger.error(f"Error: {e}")

            # ==========================================================================
            # ENCUMBRANCES
            # ==========================================================================
            logger.info("\n[STEP 6] ORI AND SUNBIZ ENCUMBRANCES")
            logger.info("-" * 40)
            try:
                if _table_exists(conn, "ori_encumbrances"):
                    total_ori = _fetchone_value(conn, "SELECT COUNT(*) FROM ori_encumbrances")
                    logger.info(f"Total ORI Encumbrances: {total_ori}")
                else:
                    logger.warning("Table 'ori_encumbrances' does not exist in schema.")

                if _table_exists(conn, "sunbiz_flr_filings"):
                    total_ucc = _fetchone_value(conn, "SELECT COUNT(*) FROM sunbiz_flr_filings")
                    total_ucc_parties = _fetchone_value(conn, "SELECT COUNT(*) FROM sunbiz_flr_parties")
                    logger.info(f"Total UCC Filings: {total_ucc}")
                    logger.info(f"Total UCC Parties: {total_ucc_parties}")
                else:
                    logger.warning("Table 'sunbiz_flr_filings' does not exist in schema.")
            except Exception as e:
                logger.error(f"Error: {e}")

            logger.info("\n" + "=" * 60)
            logger.info("         AUDIT COMPLETE")
            logger.info("=" * 60)

    except Exception as e:
        logger.error(f"CRITICAL: Failed to connect to database or execute audit: {e}")
        sys.exit(1)


if __name__ == "__main__":
    audit_database()
