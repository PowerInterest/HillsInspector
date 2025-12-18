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

from src.models.property import Property
from src.services.ingestion_service import IngestionService
from src.utils.legal_description import combine_legal_fields, generate_search_permutations, parse_legal_description


def _is_valid_folio(folio: str | None) -> bool:
    if not folio:
        return False
    f = folio.strip().lower()
    if f in {"", "unknown", "n/a", "none", "na", "multiple parcel"}:
        return False
    if len(f) < 6:
        return False
    return any(c.isdigit() for c in f)


def _get_primary_legal(conn: duckdb.DuckDBPyConnection, folio: str) -> tuple[Optional[str], str]:
    # Prefer parcels.legal_description if present (HCPA cleaned) or judgment legal.
    try:
        row = conn.execute(
            """
            SELECT legal_description, judgment_legal_description
            FROM parcels
            WHERE folio = ?
            """,
            [folio],
        ).fetchone()
        if row:
            if row[0]:
                return str(row[0]).strip(), "PARCELS_HCPA"
            if row[1]:
                return str(row[1]).strip(), "PARCELS_JUDGMENT"
    except Exception as exc:
        logger.debug("Failed parcels legal lookup for {folio}: {err}", folio=folio, err=exc)

    # Fall back to bulk raw legal fields.
    try:
        bp = conn.execute(
            """
            SELECT raw_legal1, raw_legal2, raw_legal3, raw_legal4
            FROM bulk_parcels
            WHERE strap = ?
            """,
            [folio],
        ).fetchone()
        if bp:
            legal = combine_legal_fields(bp[0], bp[1], bp[2], bp[3]).strip()
            if legal:
                return legal, "BULK_RAW_LEGAL"
    except Exception as exc:
        logger.debug("Failed bulk legal lookup for {folio}: {err}", folio=folio, err=exc)

    return None, "NONE"

def _get_judgment_legal(conn: duckdb.DuckDBPyConnection, folio: str) -> Optional[str]:
    try:
        row = conn.execute(
            "SELECT judgment_legal_description FROM parcels WHERE folio = ?",
            [folio],
        ).fetchone()
        if row and row[0]:
            return str(row[0]).strip()
    except Exception:
        return None
    return None

def _get_owner_name(conn: duckdb.DuckDBPyConnection, folio: str) -> Optional[str]:
    try:
        row = conn.execute(
            "SELECT owner_name FROM bulk_parcels WHERE strap = ?",
            [folio],
        ).fetchone()
        if row and row[0]:
            return str(row[0]).strip()
    except Exception as exc:
        logger.debug("Failed owner_name lookup for {folio}: {err}", folio=folio, err=exc)
    return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill ORI documents (and chain) for auction folios missing chain_of_title."
    )
    parser.add_argument("--db", default="data/property_master.db", help="DuckDB path")
    parser.add_argument("--limit", type=int, default=0, help="Optional limit (0 = no limit)")
    parser.add_argument("--dry-run", action="store_true", help="Log what would be done")
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    conn = duckdb.connect(str(db_path), read_only=False)

    # Find auction folios missing chain_of_title.
    missing = conn.execute(
        """
        WITH auction_folios AS (
            SELECT DISTINCT parcel_id AS folio, case_number, auction_date, property_address
            FROM auctions
            WHERE parcel_id IS NOT NULL
        )
        SELECT af.folio, af.case_number, af.auction_date, af.property_address
        FROM auction_folios af
        LEFT JOIN (SELECT DISTINCT folio FROM chain_of_title) c
        ON af.folio = c.folio
        WHERE c.folio IS NULL
        ORDER BY af.folio
        """
    ).fetchall()

    targets = []
    for folio, case_number, auction_date, address in missing:
        folio_str = str(folio) if folio is not None else None
        if not _is_valid_folio(folio_str):
            continue
        # Only backfill those that have no documents yet; otherwise use rebuild script.
        doc_count = conn.execute(
            "SELECT COUNT(*) FROM documents WHERE folio = ?",
            [folio_str],
        ).fetchone()[0]
        if doc_count > 0:
            continue
        targets.append((folio_str, str(case_number), auction_date, address))

    if args.limit and args.limit > 0:
        targets = targets[: args.limit]

    logger.info("Targets (valid folio, missing chain, no docs): {n}", n=len(targets))

    ingestion = IngestionService(analyze_pdfs=False)

    for folio, case_number, auction_date, address in targets:
        legal, source = _get_primary_legal(conn, folio)
        if not legal:
            logger.warning(
                "Skipping {folio} (case {case}) - no legal found (source={src})",
                folio=folio,
                case=case_number,
                src=source,
            )
            continue

        parsed = parse_legal_description(legal)
        search_terms = generate_search_permutations(parsed)
        if parsed.lot or parsed.block:
            lot_filter = parsed.lots or ([parsed.lot] if parsed.lot else None)
            search_terms.append(
                (
                    "__filter__",
                    {
                        "lot": lot_filter,
                        "block": parsed.block,
                        "subdivision": parsed.subdivision,
                        "require_all_lots": isinstance(lot_filter, list) and len(lot_filter) > 1,
                    },
                )
            )

        # Also try judgment legal if present (often includes STR text that ORI indexes well).
        judgment_legal = _get_judgment_legal(conn, folio)
        if judgment_legal and judgment_legal.strip().upper() != legal.strip().upper():
            parsed_j = parse_legal_description(judgment_legal)
            for t in generate_search_permutations(parsed_j):
                if t not in search_terms:
                    search_terms.append(t)
            if parsed_j.lot or parsed_j.block:
                lot_filter = parsed_j.lots or ([parsed_j.lot] if parsed_j.lot else None)
                filt = (
                    "__filter__",
                    {
                        "lot": lot_filter,
                        "block": parsed_j.block,
                        "subdivision": parsed_j.subdivision,
                        "require_all_lots": isinstance(lot_filter, list) and len(lot_filter) > 1,
                    },
                )
                if filt not in search_terms:
                    search_terms.append(filt)

        if not search_terms:
            prefix = legal.upper().strip()[:60]
            if prefix:
                search_terms = [f"{prefix}*"]

        logger.info(
            "Ingesting ORI for {folio} (case {case}) legal_source={src} terms={terms}",
            folio=folio,
            case=case_number,
            src=source,
            terms=search_terms[:3],
        )

        if args.dry_run:
            continue

        prop = Property(
            case_number=case_number,
            parcel_id=folio,
            address=address or "",
            auction_date=auction_date,
            legal_description=legal,
        )
        prop.legal_search_terms = search_terms
        prop.owner_name = _get_owner_name(conn, folio)

        try:
            ingestion.ingest_property(prop)
        except Exception as exc:
            logger.exception("Backfill ingestion failed for {folio}: {err}", folio=folio, err=exc)

    # Report remaining missing chains (valid folios only)
    remaining = conn.execute(
        """
        WITH auction_folios AS (
            SELECT DISTINCT parcel_id AS folio
            FROM auctions
            WHERE parcel_id IS NOT NULL
        )
        SELECT COUNT(*)
        FROM auction_folios af
        LEFT JOIN (SELECT DISTINCT folio FROM chain_of_title) c
        ON af.folio = c.folio
        WHERE c.folio IS NULL
          AND af.folio IS NOT NULL
          AND LENGTH(TRIM(CAST(af.folio AS VARCHAR))) > 0
          AND LOWER(TRIM(CAST(af.folio AS VARCHAR))) NOT IN ('multiple parcel')
        """
    ).fetchone()[0]
    logger.success("Remaining auction folios missing chain_of_title: {n}", n=remaining)


if __name__ == "__main__":
    main()
