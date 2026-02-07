from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
from loguru import logger
from src.utils.time import ensure_duckdb_utc

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.title_chain_service import TitleChainService  # noqa: E402


def _connect_writable(db_path: Path) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(db_path), read_only=False)
    ensure_duckdb_utc(conn)
    return conn


def _ensure_chain_columns(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("ALTER TABLE chain_of_title ADD COLUMN IF NOT EXISTS link_status VARCHAR")
    conn.execute("ALTER TABLE chain_of_title ADD COLUMN IF NOT EXISTS confidence_score FLOAT")


def _select_target_folios(
    conn: duckdb.DuckDBPyConnection,
    only_failed: bool,
    only_auctions: bool,
) -> List[str]:
    folio_filter = ""
    if only_auctions:
        folio_filter = "AND d.folio IN (SELECT DISTINCT parcel_id FROM auctions WHERE parcel_id IS NOT NULL)"

    if not only_failed:
        rows = conn.execute(
            f"""
            SELECT DISTINCT d.folio
            FROM documents d
            WHERE d.folio IS NOT NULL
            {folio_filter}
            ORDER BY d.folio
            """
        ).fetchall()
        return [r[0] for r in rows]

    rows = conn.execute(
        f"""
        WITH doc_folios AS (
            SELECT DISTINCT d.folio
            FROM documents d
            WHERE d.folio IS NOT NULL
            {folio_filter}
        ),
        chain_folios AS (
            SELECT DISTINCT folio
            FROM chain_of_title
        ),
        chain_bad AS (
            SELECT DISTINCT folio
            FROM chain_of_title
            WHERE owner_name ILIKE 'Unknown%%'
               OR acquisition_date IS NULL
               OR upper(coalesce(link_status, '')) = 'BROKEN'
        )
        SELECT DISTINCT df.folio
        FROM doc_folios df
        LEFT JOIN chain_folios cf ON df.folio = cf.folio
        LEFT JOIN chain_bad cb ON df.folio = cb.folio
        WHERE cf.folio IS NULL OR cb.folio IS NOT NULL
        ORDER BY df.folio
        """
    ).fetchall()
    return [r[0] for r in rows]


def _fetch_documents_for_folio(
    conn: duckdb.DuckDBPyConnection, folio: str
) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            folio,
            document_type,
            recording_date,
            book,
            page,
            instrument_number,
            party1,
            party2,
            legal_description,
            ocr_text,
            extracted_data,
            sales_price
        FROM documents
        WHERE folio = ?
        ORDER BY recording_date
        """,
        [folio],
    ).fetchall()
    cols = [d[0] for d in conn.description]
    docs: List[Dict[str, Any]] = [dict(zip(cols, r, strict=True)) for r in rows]

    # Normalize keys to what TitleChainService expects.
    normalized: List[Dict[str, Any]] = []
    for d in docs:
        normalized.append(
            {
                "folio": d.get("folio"),
                "doc_type": d.get("document_type"),
                "document_type": d.get("document_type"),
                "recording_date": d.get("recording_date"),
                "book": d.get("book"),
                "page": d.get("page"),
                "instrument_number": d.get("instrument_number"),
                "party1": d.get("party1"),
                "party2": d.get("party2"),
                "legal_description": d.get("legal_description"),
                "ocr_text": d.get("ocr_text"),
                "extracted_data": d.get("extracted_data"),
                "sales_price": d.get("sales_price"),
            }
        )
    return normalized


def _write_chain_for_folio(
    conn: duckdb.DuckDBPyConnection,
    folio: str,
    timeline: List[Dict[str, Any]],
    dry_run: bool,
) -> None:
    if dry_run:
        return

    conn.execute("DELETE FROM chain_of_title WHERE folio = ?", [folio])
    conn.execute("UPDATE encumbrances SET chain_period_id = NULL WHERE folio = ?", [folio])

    for period in timeline:
        conn.execute(
            """
            INSERT INTO chain_of_title (
                folio,
                owner_name,
                acquired_from,
                acquisition_date,
                disposition_date,
                acquisition_instrument,
                acquisition_doc_type,
                acquisition_price,
                link_status,
                confidence_score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                folio,
                period.get("owner"),
                period.get("acquired_from"),
                period.get("acquisition_date"),
                period.get("disposition_date"),
                period.get("acquisition_instrument"),
                period.get("acquisition_doc_type"),
                period.get("acquisition_price"),
                period.get("link_status"),
                period.get("confidence_score"),
            ],
        )


def _try_copy_if_locked(db_path: Path) -> Tuple[Path, Optional[str]]:
    try:
        conn = _connect_writable(db_path)
        conn.close()
        return db_path, None
    except Exception as exc:
        msg = str(exc)
        if "Conflicting lock is held" not in msg:
            raise

        copy_path = db_path.with_name(db_path.stem + "_chain_rebuild.db")
        copy_path.write_bytes(db_path.read_bytes())
        return copy_path, msg


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild chain_of_title from existing documents.")
    parser.add_argument("--db", default="data/property_master.db", help="Path to DuckDB file")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Rebuild for all folios that have documents (default: only failed/unknown/missing).",
    )
    parser.add_argument(
        "--include-non-auctions",
        action="store_true",
        help="Include folios not present in auctions table (default: auctions only).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Compute and report only.")
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    target_db, lock_msg = _try_copy_if_locked(db_path)
    if lock_msg:
        logger.warning(
            "DB is locked for writes; running against copy: {copy} (original error: {err})",
            copy=str(target_db),
            err=lock_msg,
        )

    conn = _connect_writable(target_db)
    _ensure_chain_columns(conn)

    only_failed = not args.all
    only_auctions = not args.include_non_auctions
    folios = _select_target_folios(conn, only_failed=only_failed, only_auctions=only_auctions)
    logger.info("Target folios: {n}", n=len(folios))

    svc = TitleChainService()
    updated = 0
    skipped_no_docs = 0
    skipped_empty_chain = 0

    for folio in folios:
        docs = _fetch_documents_for_folio(conn, folio)
        if not docs:
            skipped_no_docs += 1
            continue

        analysis = svc.build_chain_and_analyze(docs)
        timeline = analysis.get("ownership_timeline") or []
        if not timeline:
            skipped_empty_chain += 1
            continue

        _write_chain_for_folio(conn, folio, timeline, dry_run=args.dry_run)
        updated += 1

        summary = analysis.get("summary") or {}
        logger.info(
            "{folio}: periods={periods} gaps={gaps} owner={owner}",
            folio=folio,
            periods=len(timeline),
            gaps=summary.get("gaps_found"),
            owner=summary.get("current_owner"),
        )

    if not args.dry_run:
        conn.close()

    logger.success(
        "Done. updated={updated} skipped_no_docs={skipped_no_docs} skipped_empty_chain={skipped_empty_chain} db={db}",
        updated=updated,
        skipped_no_docs=skipped_no_docs,
        skipped_empty_chain=skipped_empty_chain,
        db=str(target_db),
    )


if __name__ == "__main__":
    main()
