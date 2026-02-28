# src/tools/purge_fuzzy_encumbrances.py
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from sunbiz.db import get_engine, resolve_pg_dsn
from sqlalchemy import text
from src.services.pg_ori_service import PgOriService
from loguru import logger


def main():
    engine = get_engine(resolve_pg_dsn(None))

    with engine.connect() as conn:
        logger.info("Fetching property tokens from hcpa_allsales and foreclosures...")

        rows = conn.execute(
            text("""
            SELECT 
                f.foreclosure_id, f.case_number_norm, f.case_number_raw,
                f.strap, f.folio, f.property_address,
                bp.owner_name, bp.raw_legal1 as legal1, bp.raw_legal2 as legal2, 
                bp.raw_legal3 as legal3, bp.raw_legal4 as legal4
            FROM foreclosures f
            LEFT JOIN hcpa_bulk_parcels bp ON f.strap = bp.strap
            WHERE f.archived_at IS NULL
        """)
        ).fetchall()

        svc = PgOriService()
        prop_map = {}
        for r in rows:
            target = {
                "case_number": r.case_number_norm or r.case_number_raw,
                "strap": r.strap or "",
                "folio": r.folio or "",
                "property_address": r.property_address or "",
                "owner_name": r.owner_name or "",
                "legal1": r.legal1 or "",
                "legal2": r.legal2 or "",
                "legal3": r.legal3 or "",
                "legal4": r.legal4 or "",
            }
            chain = svc.get_ownership_chain(target["strap"])
            tokens = svc.build_property_tokens(target, chain)
            if target["folio"]:
                prop_map[target["folio"]] = tokens
            if target["strap"]:
                prop_map[target["strap"]] = tokens

        logger.info("Fetching all ori_encumbrances...")
        encs = conn.execute(
            text("""
            SELECT id, folio, strap, party1, party2, parties_one_json, parties_two_json, 
                   raw_document_type, legal_description, case_number
            FROM ori_encumbrances
        """)
        ).fetchall()

        to_delete = []
        for e in encs:
            folio = e.folio or ""
            strap = e.strap or ""
            tokens = prop_map.get(folio) or prop_map.get(strap)
            if not tokens:
                # Keep if no property found (safer)
                continue

            p1_list = e.parties_one_json if isinstance(e.parties_one_json, list) else []
            p2_list = e.parties_two_json if isinstance(e.parties_two_json, list) else []

            doc = {
                "Legal": e.legal_description or "",
                "party1": e.party1 or "",
                "party2": e.party2 or "",
                "PartiesOne": p1_list,
                "PartiesTwo": p2_list,
                "DocType": e.raw_document_type or "",
                "CaseNum": e.case_number or "",
            }

            if not svc.matches_property(doc, tokens):
                to_delete.append(e.id)

        logger.info(f"Found {len(to_delete)} false-positive encumbrances to delete.")

        if to_delete:
            with engine.begin() as tx:
                # Update foreclosures status first to safely decouple it
                logger.info("Resetting step_ori_searched for affected foreclosures...")
                tx.execute(
                    text("""
                    UPDATE foreclosures
                    SET step_ori_searched = NULL
                    WHERE folio IN (
                        SELECT COALESCE(folio, '') FROM ori_encumbrances WHERE id = ANY(:ids)
                    ) OR strap IN (
                        SELECT COALESCE(strap, '') FROM ori_encumbrances WHERE id = ANY(:ids)
                    )
                """),
                    {"ids": to_delete},
                )

                logger.info("Batch deleting encumbrances...")
                chunk_size = 1000
                for i in range(0, len(to_delete), chunk_size):
                    chunk = to_delete[i : i + chunk_size]
                    tx.execute(text("DELETE FROM ori_encumbrances WHERE id = ANY(:ids)"), {"ids": chunk})

        logger.info(f"Successfully deleted {len(to_delete)} rows and reset pipeline status.")


if __name__ == "__main__":
    main()
