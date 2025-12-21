from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

import duckdb
from loguru import logger

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.models.property import Property  # noqa: E402
from src.services.ingestion_service import IngestionService  # noqa: E402
from src.utils.legal_description import generate_search_permutations, parse_legal_description  # noqa: E402


def _is_valid_folio(folio: str | None) -> bool:
    if not folio:
        return False
    f = folio.strip().lower()
    if f in {"", "unknown", "n/a", "none", "na", "multiple parcel"}:
        return False
    if len(f) < 6:
        return False
    return any(c.isdigit() for c in f)


def _get_parcel_data(conn: duckdb.DuckDBPyConnection, folio: str) -> tuple[Optional[str], Optional[str]]:
    row = conn.execute(
        """
        SELECT legal_description, owner_name
        FROM parcels
        WHERE folio = ?
        """,
        [folio],
    ).fetchone()
    if not row:
        return None, None
    legal, owner = row
    return (str(legal).strip() if legal else None), (str(owner).strip() if owner else None)


def _get_auction_data(
    conn: duckdb.DuckDBPyConnection, folio: str
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    row = conn.execute(
        """
        SELECT case_number, property_address, CAST(auction_date AS VARCHAR)
        FROM auctions
        WHERE parcel_id = ?
        ORDER BY auction_date DESC
        LIMIT 1
        """,
        [folio],
    ).fetchone()
    if not row:
        return None, None, None
    case_number, address, auction_date = row
    return (
        str(case_number).strip() if case_number else None,
        str(address).strip() if address else None,
        str(auction_date).strip() if auction_date else None,
    )


def _select_broken_folios(conn: duckdb.DuckDBPyConnection) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT c.folio
        FROM chain_of_title c
        WHERE upper(coalesce(c.link_status, '')) = 'BROKEN'
        ORDER BY c.folio
        """
    ).fetchall()
    return [str(r[0]) for r in rows if _is_valid_folio(str(r[0]))]


def _delete_existing_records(db_path: Path, folio: str) -> None:
    conn = duckdb.connect(str(db_path), read_only=False)
    try:
        conn.execute("DELETE FROM documents WHERE folio = ?", [folio])
        conn.execute("DELETE FROM chain_of_title WHERE folio = ?", [folio])
        conn.execute("UPDATE encumbrances SET chain_period_id = NULL WHERE folio = ?", [folio])
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Re-ingest ORI documents for folios with BROKEN chain links, then rebuild chain."
    )
    parser.add_argument("--db", default="data/property_master.db", help="DuckDB path")
    parser.add_argument("--limit", type=int, default=0, help="Optional limit (0 = no limit)")
    parser.add_argument("--dry-run", action="store_true", help="Log what would be done")
    parser.add_argument(
        "--folios",
        nargs="*",
        default=None,
        help="Optional explicit folio list (overrides BROKEN selection).",
    )
    args = parser.parse_args()

    db_path = Path(args.db).resolve()

    ro = duckdb.connect(str(db_path), read_only=False)
    try:
        targets = args.folios or _select_broken_folios(ro)
    finally:
        ro.close()

    if args.limit and args.limit > 0:
        targets = targets[: args.limit]

    logger.info("Targets: {n}", n=len(targets))
    if not targets:
        return

    ingestion = IngestionService(analyze_pdfs=False)

    for folio in targets:
        conn = duckdb.connect(str(db_path), read_only=False)
        try:
            legal, owner = _get_parcel_data(conn, folio)
            case_number, address, auction_date = _get_auction_data(conn, folio)
        finally:
            conn.close()

        if not legal:
            logger.warning("Skipping {folio} - missing parcels.legal_description", folio=folio)
            continue

        parsed = parse_legal_description(legal)
        search_terms = generate_search_permutations(parsed)
        if parsed.lot or parsed.block:
            search_terms.append(
                (
                    "__filter__",
                    {
                        "lot": parsed.lots or ([parsed.lot] if parsed.lot else None),
                        "block": parsed.block,
                        "subdivision": parsed.subdivision,
                        "require_all_lots": len(parsed.lots or []) > 1,
                    },
                )
            )

        logger.info(
            "Re-ingesting {folio} case={case} addr={addr} terms={terms}",
            folio=folio,
            case=case_number,
            addr=address,
            terms=[t for t in search_terms if isinstance(t, str)][:3],
        )

        if args.dry_run:
            continue

        _delete_existing_records(db_path, folio)

        prop = Property(
            case_number=case_number or folio,
            parcel_id=folio,
            address=address or "",
            auction_date=auction_date,
            legal_description=legal,
        )
        prop.owner_name = owner
        prop.legal_search_terms = search_terms

        try:
            ingestion.ingest_property(prop)
        except Exception as exc:
            logger.exception("Re-ingest failed for {folio}: {err}", folio=folio, err=exc)

    # Report remaining BROKEN links
    conn = duckdb.connect(str(db_path), read_only=False)
    try:
        remaining = conn.execute(
            "SELECT COUNT(*) FROM chain_of_title WHERE upper(coalesce(link_status,''))='BROKEN'"
        ).fetchone()[0]
    finally:
        conn.close()
    logger.success("Remaining BROKEN chain rows: {n}", n=remaining)


if __name__ == "__main__":
    main()
