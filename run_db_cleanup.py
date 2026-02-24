from sunbiz.db import get_engine, resolve_pg_dsn
from sqlalchemy import text
from rapidfuzz import fuzz
from loguru import logger
import sys


def main():
    logger.remove()
    logger.add(sys.stdout, colorize=True, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{message}</level>")

    engine = get_engine(resolve_pg_dsn(None))
    logger.info("Starting The Great Purge of False Positives...")

    query = """
        SELECT 
            e.id,
            e.folio,
            e.strap,
            e.parties_one_json, 
            e.parties_two_json, 
            e.party1, 
            e.party2,
            e.legal_description,
            e.case_number AS doc_case_number,
            f.owner_name,
            f.case_number_norm AS prop_case_number,
            f.property_address
        FROM ori_encumbrances e
        JOIN foreclosures f ON (e.folio = f.folio OR e.strap = f.strap)
    """

    with engine.connect() as conn:
        rows = conn.execute(text(query)).mappings().fetchall()

    logger.info(f"Loaded {len(rows)} encumbrances joined with foreclosures to evaluate.")

    to_delete_ids = []
    affected_folios = set()
    affected_straps = set()

    for row in rows:
        owner_name = (row.get("owner_name") or "").strip().upper()
        if not owner_name:
            continue

        parties_text = f"{row.get('party1') or ''} {row.get('party2') or ''}".upper()

        doc_case = (row.get("doc_case_number") or "").strip().upper()
        prop_case = (row.get("prop_case_number") or "").strip().upper()
        if doc_case and prop_case and doc_case == prop_case:
            continue

        if fuzz.token_set_ratio(owner_name, parties_text) < 80:
            to_delete_ids.append(row["id"])
            if row.get("folio"):
                affected_folios.add(row["folio"])
            if row.get("strap"):
                affected_straps.add(row["strap"])

    logger.warning(f"Found {len(to_delete_ids)} false-positive records to delete.")
    logger.warning(f"Affected foreclosures: {len(affected_folios)} folios, {len(affected_straps)} straps.")

    if to_delete_ids:
        with engine.begin() as conn:
            chunk_size = 5000
            deleted = 0
            for i in range(0, len(to_delete_ids), chunk_size):
                chunk = list(to_delete_ids[i : i + chunk_size])
                res = conn.execute(text("DELETE FROM ori_encumbrances WHERE id = ANY(:ids)"), {"ids": chunk})
                deleted += res.rowcount

            logger.info(f"Deleted {deleted} rows from ori_encumbrances.")

            f_reset = 0
            if affected_folios:
                folio_chunks = list(affected_folios)
                for i in range(0, len(folio_chunks), chunk_size):
                    chunk = folio_chunks[i : i + chunk_size]
                    res = conn.execute(
                        text("""
                        UPDATE foreclosures 
                        SET step_ori_searched = NULL 
                        WHERE folio = ANY(:folios)
                    """),
                        {"folios": chunk},
                    )
                    f_reset += res.rowcount

            if affected_straps:
                strap_chunks = list(affected_straps)
                for i in range(0, len(strap_chunks), chunk_size):
                    chunk = strap_chunks[i : i + chunk_size]
                    res = conn.execute(
                        text("""
                        UPDATE foreclosures 
                        SET step_ori_searched = NULL 
                        WHERE strap = ANY(:straps)
                    """),
                        {"straps": chunk},
                    )
                    f_reset += res.rowcount

            logger.success(f"Reset step_ori_searched on {f_reset} foreclosures.")
    else:
        logger.info("No records to delete.")


if __name__ == "__main__":
    main()
