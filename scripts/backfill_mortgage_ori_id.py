"""Backfill ori_id for existing mortgage encumbrances.

Root cause: `_search_official_records_pg` discovers mortgages from the local
`official_records_daily_instruments` PG table, which has no PAV document ID.
The PAV API path (which does return `ID`) is only triggered during iterative
expansion — so initial seeds from the local DB path land with `ori_id = NULL`.

This script queries the PAV API by each mortgage's instrument number and
writes the resulting PAV `ID` back to `ori_encumbrances.ori_id`.

Run:
    uv run python scripts/backfill_mortgage_ori_id.py
    uv run python scripts/backfill_mortgage_ori_id.py --dry-run
    uv run python scripts/backfill_mortgage_ori_id.py --limit 10
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from loguru import logger
from sqlalchemy import text

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from sunbiz.db import get_engine, resolve_pg_dsn

_PAV_KEYWORD_URL = "https://publicaccess.hillsclerk.com/PAVDirectSearch/api/CustomQuery/KeywordSearch"
_PAV_HEADERS = {
    "Content-Type": "application/json",
    "Origin": "https://publicaccess.hillsclerk.com",
    "Referer": "https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html",
}


def _pav_lookup_instrument(session: object, instrument: str) -> str | None:
    """Query PAV by Instrument Number (Query 108) and return the internal doc ID."""

    # QueryID=320 = "Instrument #" search (same as _search_instrument_pav)
    # KeywordName Id=1006 is the PAV instrument number field.
    payload = {
        "QueryID": 320,
        "Keywords": [{"Id": 1006, "Value": instrument}],
        "QueryLimit": 5,
    }
    try:
        resp = session.post(_PAV_KEYWORD_URL, json=payload, timeout=30)  # type: ignore[union-attr]
        if resp.status_code != 200:
            logger.warning(f"PAV HTTP {resp.status_code} for instrument {instrument}")
            return None
        data = resp.json()
        rows = data.get("Data") or []
        if not rows:
            logger.debug(f"PAV: no results for instrument {instrument}")
            return None
        doc_id = rows[0].get("ID")
        if not doc_id:
            logger.debug(f"PAV result missing ID for instrument {instrument}")
            return None
        return str(doc_id)
    except Exception as exc:
        logger.warning(f"PAV lookup error for {instrument}: {exc}")
        return None


def main() -> None:
    import requests  # type: ignore[import-untyped]

    parser = argparse.ArgumentParser(description="Backfill ori_id for mortgages missing a PAV document ID")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing to DB")
    parser.add_argument("--limit", type=int, default=None, help="Max mortgages to process")
    parser.add_argument("--delay", type=float, default=0.5, help="Seconds to wait between PAV calls")
    args = parser.parse_args()

    engine = get_engine(resolve_pg_dsn(None))

    with engine.connect() as conn:
        query = """
            SELECT id, instrument_number
            FROM ori_encumbrances
            WHERE encumbrance_type = 'mortgage'
              AND ori_id IS NULL
              AND instrument_number IS NOT NULL
              AND instrument_number NOT LIKE 'INFERRED-%'
            ORDER BY id DESC
        """
        if args.limit:
            query += f" LIMIT {args.limit}"
        rows = conn.execute(text(query)).fetchall()

    logger.info(f"Found {len(rows)} mortgages with ori_id = NULL to backfill.")

    session = requests.Session()
    session.headers.update(_PAV_HEADERS)

    found = 0
    not_found = 0
    errors = 0

    for i, (enc_id, instrument) in enumerate(rows):
        logger.info(f"[{i + 1}/{len(rows)}] Looking up PAV ID for instrument {instrument}...")
        doc_id = _pav_lookup_instrument(session, instrument)

        if doc_id:
            logger.success(f"  → Found ID={doc_id}")
            found += 1
            if not args.dry_run:
                with engine.begin() as wconn:
                    wconn.execute(
                        text("UPDATE ori_encumbrances SET ori_id = :oid, updated_at = now() WHERE id = :enc_id"),
                        {"oid": doc_id, "enc_id": enc_id},
                    )
        else:
            logger.debug(f"  → No ID found for {instrument}")
            not_found += 1

        if args.delay and i < len(rows) - 1:
            time.sleep(args.delay)

    logger.info(f"Backfill complete. found={found} not_found={not_found} errors={errors} dry_run={args.dry_run}")


if __name__ == "__main__":
    main()
