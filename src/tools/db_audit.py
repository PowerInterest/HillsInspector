"""PostgreSQL Database Audit — single-file state-of-the-database report.

Covers:
  - Active foreclosure counts and auction date breakdown
  - Pipeline step completion rates (the CLAUDE.md completeness gates)
  - Bulk table health (HCPA parcels, clerk cases, NAL, Sunbiz, permits)
  - Market data and sales coverage
  - Encumbrance and survival analysis status
  - Job control run history (if tables exist)

Usage:
  uv run python -m src.tools.db_audit
  uv run python -m src.tools.db_audit --dsn postgresql://...
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from sqlalchemy import text  # noqa: E402
from sunbiz.db import get_engine, resolve_pg_dsn  # noqa: E402


def _path_exists(path_value: str | None) -> bool:
    if not path_value:
        return False
    try:
        return Path(path_value).exists()
    except OSError:
        return False


def _count_existing_paths(path_values: list[str | None]) -> int:
    return sum(1 for path_value in path_values if _path_exists(path_value))


def _val(conn, query: str, params: dict | None = None, default: int = 0) -> int:
    try:
        result = conn.execute(text(query), params or {}).scalar()
        return result if result is not None else default
    except Exception as e:
        conn.rollback()
        logger.error(f"Query error: {e}")
        return default


def _row(conn, query: str, params: dict | None = None) -> dict | None:
    try:
        row = conn.execute(text(query), params or {}).mappings().first()
        return dict(row) if row else None
    except Exception as e:
        conn.rollback()
        logger.error(f"Query error: {e}")
        return None


def _rows(conn, query: str, params: dict | None = None) -> list[dict]:
    try:
        return [dict(r) for r in conn.execute(text(query), params or {}).mappings().all()]
    except Exception as e:
        conn.rollback()
        logger.error(f"Query error: {e}")
        return []


def _has_table(conn, name: str) -> bool:
    try:
        return bool(
            conn.execute(
                text("SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=:n"),
                {"n": name},
            ).scalar()
        )
    except Exception as e:
        conn.rollback()
        logger.error(f"Table check error: {e}")
        return False


def _pct(num: int, den: int) -> str:
    if den == 0:
        return "N/A"
    return f"{100.0 * num / den:.1f}%"


def _section(title: str) -> None:
    logger.info("")
    logger.info(f"{'─' * 60}")
    logger.info(f"  {title}")
    logger.info(f"{'─' * 60}")


def audit_database(dsn: str | None = None) -> None:
    engine = get_engine(resolve_pg_dsn(dsn))

    with engine.connect() as conn:
        logger.info("=" * 60)
        logger.info("       HILLSINSPECTOR DATABASE AUDIT REPORT")
        logger.info("=" * 60)

        # ==================================================================
        # 1. FORECLOSURE OVERVIEW
        # ==================================================================
        _section("FORECLOSURES")

        total = _val(conn, "SELECT COUNT(*) FROM foreclosures")
        active = _val(conn, "SELECT COUNT(*) FROM foreclosures WHERE archived_at IS NULL")
        archived = total - active
        logger.info(f"Total rows: {total}  (active: {active}, archived: {archived})")

        by_type = _rows(
            conn,
            """
            SELECT COALESCE(auction_type, 'UNKNOWN') AS atype, COUNT(*) AS cnt
            FROM foreclosures WHERE archived_at IS NULL
            GROUP BY auction_type ORDER BY cnt DESC
        """,
        )
        for r in by_type:
            logger.info(f"  {r['atype']}: {r['cnt']}")

        dr = _row(
            conn,
            """
            SELECT MIN(auction_date) AS min_d, MAX(auction_date) AS max_d
            FROM foreclosures WHERE archived_at IS NULL
        """,
        )
        if dr:
            logger.info(f"Auction date range: {dr['min_d']} → {dr['max_d']}")

        upcoming = _val(conn, "SELECT COUNT(*) FROM foreclosures WHERE archived_at IS NULL AND auction_date >= CURRENT_DATE")
        past = _val(conn, "SELECT COUNT(*) FROM foreclosures WHERE archived_at IS NULL AND auction_date < CURRENT_DATE")
        logger.info(f"Upcoming: {upcoming}  |  Past: {past}")

        valid_strap = _val(
            conn,
            """
            SELECT COUNT(*) FROM foreclosures
            WHERE archived_at IS NULL AND strap IS NOT NULL AND strap != ''
        """,
        )
        logger.info(f"With valid strap: {valid_strap}/{active} ({_pct(valid_strap, active)})")

        # ==================================================================
        # 2. PIPELINE STEP COMPLETION (COMPLETENESS GATES)
        # ==================================================================
        _section("PIPELINE STEP COMPLETION (active foreclosures)")

        if active == 0:
            logger.warning("No active foreclosures — skipping step rates")
        else:
            pdf_paths = _rows(
                conn,
                """
                SELECT pdf_path
                FROM foreclosures
                WHERE archived_at IS NULL
                """,
            )
            pdf_on_disk = _count_existing_paths([r.get("pdf_path") for r in pdf_paths])
            logger.info(f"  Judgment PDF on disk: {pdf_on_disk}/{active} ({_pct(pdf_on_disk, active)})")

            pdf_flagged = _val(
                conn,
                """
                SELECT COUNT(*)
                FROM foreclosures
                WHERE archived_at IS NULL AND step_pdf_downloaded IS NOT NULL
                """,
            )
            logger.info(f"  PDF step flag:       {pdf_flagged}/{active} ({_pct(pdf_flagged, active)})")

            steps = [
                ("step_judgment_extracted", "Judgment extracted"),
                ("step_ori_searched", "ORI searched"),
                ("step_survival_analyzed", "Survival analyzed"),
            ]
            for col, label in steps:
                done = _val(conn, f"SELECT COUNT(*) FROM foreclosures WHERE archived_at IS NULL AND {col} IS NOT NULL")
                logger.info(f"  {label}: {done}/{active} ({_pct(done, active)})")

            # CLAUDE.md completeness gates
            logger.info("")
            logger.info("  Completeness gates (CLAUDE.md targets):")

            with_jd = _val(conn, "SELECT COUNT(*) FROM foreclosures WHERE archived_at IS NULL AND judgment_data IS NOT NULL")
            logger.info(f"  Judgment data:    {with_jd}/{active} ({_pct(with_jd, active)}) — target ≥90%")

            with_jd_strap = _val(
                conn,
                """
                SELECT COUNT(*) FROM foreclosures
                WHERE archived_at IS NULL AND judgment_data IS NOT NULL AND strap IS NOT NULL
            """,
            )

            chain_covered = (
                _val(
                    conn,
                    """
                SELECT COUNT(DISTINCT f.foreclosure_id)
                FROM foreclosures f
                JOIN foreclosure_title_chain c ON c.foreclosure_id = f.foreclosure_id
                WHERE f.archived_at IS NULL AND f.judgment_data IS NOT NULL
            """,
                )
                if _has_table(conn, "foreclosure_title_chain")
                else 0
            )
            logger.info(f"  Chain coverage:   {chain_covered}/{with_jd} ({_pct(chain_covered, with_jd)}) — target ≥80%")

            enc_covered = (
                _val(
                    conn,
                    """
                SELECT COUNT(DISTINCT f.foreclosure_id)
                FROM foreclosures f
                JOIN ori_encumbrances oe ON oe.strap = f.strap
                WHERE f.archived_at IS NULL AND f.judgment_data IS NOT NULL AND f.strap IS NOT NULL
            """,
                )
                if _has_table(conn, "ori_encumbrances")
                else 0
            )
            logger.info(f"  Encumbrance cov:  {enc_covered}/{with_jd_strap} ({_pct(enc_covered, with_jd_strap)}) — target ≥80%")

            surv_covered = (
                _val(
                    conn,
                    """
                SELECT COUNT(DISTINCT f.foreclosure_id)
                FROM foreclosures f
                JOIN ori_encumbrances oe ON oe.strap = f.strap
                WHERE f.archived_at IS NULL AND f.judgment_data IS NOT NULL
                  AND f.strap IS NOT NULL AND oe.survival_status IS NOT NULL
            """,
                )
                if _has_table(conn, "ori_encumbrances")
                else 0
            )
            logger.info(f"  Survival cov:     {surv_covered}/{with_jd_strap} ({_pct(surv_covered, with_jd_strap)}) — target ≥80%")

        # ==================================================================
        # 3. BULK TABLE HEALTH
        # ==================================================================
        _section("BULK TABLES")

        bulk_tables = [
            ("hcpa_bulk_parcels", "HCPA parcels"),
            ("hcpa_allsales", "HCPA sales"),
            ("hcpa_latlon", "HCPA lat/lon"),
            ("clerk_civil_cases", "Clerk civil cases"),
            ("clerk_civil_events", "Clerk events"),
            ("dor_nal_parcels", "DOR NAL parcels"),
            ("sunbiz_flr_filings", "Sunbiz UCC filings"),
            ("sunbiz_entity_cordata", "Sunbiz corp data"),
            ("county_permits", "County permits"),
            ("tampa_accela_records", "Tampa permits"),
        ]
        for tbl, label in bulk_tables:
            if _has_table(conn, tbl):
                cnt = _val(conn, f"SELECT COUNT(*) FROM {tbl}")
                logger.info(f"  {label} ({tbl}): {cnt:,}")
            else:
                logger.info(f"  {label} ({tbl}): TABLE MISSING")

        # ==================================================================
        # 4. MARKET DATA
        # ==================================================================
        _section("MARKET DATA")

        if _has_table(conn, "property_market"):
            mkt_total = _val(conn, "SELECT COUNT(*) FROM property_market")
            mkt_zest = _val(conn, "SELECT COUNT(*) FROM property_market WHERE zestimate IS NOT NULL")
            mkt_photos = _val(
                conn, "SELECT COUNT(*) FROM property_market WHERE photo_cdn_urls IS NOT NULL AND photo_cdn_urls != '[]'::jsonb"
            )
            logger.info(f"  Property market rows: {mkt_total:,}")
            logger.info(f"  With zestimate: {mkt_zest:,}")
            logger.info(f"  With photos: {mkt_photos:,}")
        else:
            logger.info("  property_market: TABLE MISSING")

        # ==================================================================
        # 5. ENCUMBRANCES & SURVIVAL
        # ==================================================================
        _section("ENCUMBRANCES & SURVIVAL")

        if _has_table(conn, "ori_encumbrances"):
            total_enc = _val(conn, "SELECT COUNT(*) FROM ori_encumbrances")
            by_status = _rows(
                conn,
                """
                SELECT COALESCE(survival_status, 'PENDING') AS status, COUNT(*) AS cnt
                FROM ori_encumbrances GROUP BY survival_status ORDER BY cnt DESC
            """,
            )
            logger.info(f"  Total encumbrances: {total_enc:,}")
            for r in by_status:
                logger.info(f"    {r['status']}: {r['cnt']:,}")

            distinct_straps = _val(conn, "SELECT COUNT(DISTINCT strap) FROM ori_encumbrances")
            logger.info(f"  Distinct straps: {distinct_straps:,}")
        else:
            logger.info("  ori_encumbrances: TABLE MISSING")

        # ==================================================================
        # 6. TITLE CHAIN
        # ==================================================================
        _section("TITLE CHAIN")

        if _has_table(conn, "foreclosure_title_chain"):
            chain_rows = _val(conn, "SELECT COUNT(*) FROM foreclosure_title_chain")
            chain_props = _val(conn, "SELECT COUNT(DISTINCT foreclosure_id) FROM foreclosure_title_chain")
            logger.info(f"  Chain rows: {chain_rows:,}  covering {chain_props} foreclosures")
        else:
            logger.info("  foreclosure_title_chain: TABLE MISSING")

        if _has_table(conn, "foreclosure_title_summary"):
            summ_rows = _val(conn, "SELECT COUNT(*) FROM foreclosure_title_summary")
            logger.info(f"  Title summaries: {summ_rows:,}")

        # ==================================================================
        # 7. TRUST ACCOUNTS
        # ==================================================================
        _section("TRUST ACCOUNTS")

        if _has_table(conn, "TrustAccount"):
            trust = _val(conn, 'SELECT COUNT(*) FROM "TrustAccount"')
            logger.info(f"  Trust account snapshots: {trust:,}")
        else:
            logger.info("  TrustAccount: TABLE MISSING")

        # ==================================================================
        # 8. JOB CONTROL (scheduled jobs)
        # ==================================================================
        if _has_table(conn, "pipeline_job_runs"):
            _section("RECENT JOB RUNS (last 10)")
            recent = _rows(
                conn,
                """
                SELECT job_name, status, triggered_by,
                       started_at, finished_at,
                       EXTRACT(EPOCH FROM (finished_at - started_at))::int AS duration_sec
                FROM pipeline_job_runs
                ORDER BY started_at DESC LIMIT 10
            """,
            )
            for r in recent:
                dur = f"{r['duration_sec']}s" if r.get("duration_sec") is not None else "running"
                logger.info(f"  {r['job_name']:30s} {r['status']:8s} {dur:>8s}  ({r['triggered_by']}, {r['started_at']})")

        # ==================================================================
        logger.info("")
        logger.info("=" * 60)
        logger.info("       AUDIT COMPLETE")
        logger.info("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(description="HillsInspector PostgreSQL database audit")
    parser.add_argument("--dsn", help="PostgreSQL DSN override")
    args = parser.parse_args()

    try:
        audit_database(dsn=args.dsn)
    except Exception as e:
        logger.error(f"CRITICAL: Audit failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
