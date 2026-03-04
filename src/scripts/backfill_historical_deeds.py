#!/usr/bin/env python3
"""
Backfills missing party names for historical deeds.
Queries `hcpa_allsales` for rows with NULL grantor/grantee and valid `doc_num`.
Calls the PAV instrument search API to fetch parties, then inserts the result into
`official_records_daily_instruments` to be picked up by `fn_title_chain`.
"""

import argparse
import time
import sys
from pathlib import Path

import requests
from loguru import logger
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

try:
    from src.services.models_clerk import OfficialRecordsDailyInstrument
    from sunbiz.db import get_engine, resolve_pg_dsn
except ModuleNotFoundError:
    REPO_ROOT = Path(__file__).resolve().parents[1]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from src.services.models_clerk import OfficialRecordsDailyInstrument
    from sunbiz.db import get_engine, resolve_pg_dsn

# PAV CustomQuery endpoint (same as pg_ori_service)
_PAV_URL = (
    "https://publicaccess.hillsclerk.com"
    "/PAVDirectSearch/api/CustomQuery/KeywordSearch"
)
_PAV_HEADERS = {
    "Content-Type": "application/json",
    "Origin": "https://publicaccess.hillsclerk.com",
    "Referer": "https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html",
}
_PAV_TIMEOUT = 30


def _search_instrument(session: requests.Session, instrument: str) -> dict | None:
    """Search PAV for a single instrument number. Returns parsed doc or None."""
    payload = {
        "QueryID": 320,
        "Keywords": [{"Id": 1006, "Value": instrument}],
        "QueryLimit": 50,
    }
    for attempt in range(1, 4):
        try:
            resp = session.post(_PAV_URL, json=payload, headers=_PAV_HEADERS, timeout=_PAV_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                rows = data.get("Data") or []
                return _parse_rows(rows, instrument)
            if resp.status_code == 429:
                logger.warning(f"PAV 429 on {instrument}, attempt {attempt}/3")
                time.sleep(2 * attempt)
                continue
            logger.warning(f"PAV HTTP {resp.status_code} on {instrument}")
            return None
        except requests.RequestException as e:
            logger.warning(f"PAV request error on {instrument}: {e}")
            time.sleep(1)
    logger.error(f"PAV failed after retries: {instrument}")
    return None


def _parse_rows(rows: list[dict], target_instrument: str) -> dict | None:
    """Parse PAV response rows into a single doc with parties."""
    parties_from: list[str] = []
    parties_to: list[str] = []
    doc_type = ""
    record_date = ""

    for row in rows:
        cols = row.get("DisplayColumnValues") or []
        if len(cols) < 9:
            continue
        values = [str(col.get("Value") or "").strip() for col in cols[:9]]
        values.extend([""] * (9 - len(values)))

        person_type = values[0].upper()
        name = values[1]
        instrument = values[8]

        if instrument != target_instrument:
            continue

        if not doc_type:
            doc_type = values[3]
        if not record_date:
            record_date = values[2]

        if name:
            if "2" in person_type or "GRANTEE" in person_type:
                if name not in parties_to:
                    parties_to.append(name)
            elif name not in parties_from:
                parties_from.append(name)

    if not parties_from and not parties_to:
        return None

    return {
        "doc_type": doc_type,
        "record_date": record_date,
        "parties_from": parties_from,
        "parties_to": parties_to,
    }


def backfill(dsn: str | None, limit: int = 100):
    engine = get_engine(resolve_pg_dsn(dsn))
    session = requests.Session()

    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT DISTINCT s.doc_num, s.sale_date
                FROM hcpa_allsales s
                LEFT JOIN official_records_daily_instruments p
                  ON s.doc_num = p.instrument_number
                WHERE (s.grantor IS NULL OR s.grantee IS NULL)
                  AND s.doc_num IS NOT NULL
                  AND trim(s.doc_num) <> ''
                  AND p.id IS NULL
                ORDER BY s.sale_date DESC NULLS LAST
                LIMIT :limit
            """),
            {"limit": limit},
        ).fetchall()

    if not rows:
        logger.info("No missing deeds found to backfill.")
        return

    logger.info(f"Found {len(rows)} missing deeds to fetch from ORI.")

    success = 0
    for i, row in enumerate(rows):
        doc_num = row.doc_num
        logger.info(f"[{i+1}/{len(rows)}] Fetching instrument {doc_num}...")

        result = _search_instrument(session, doc_num)
        if result is None:
            logger.warning(f"  No parties found for {doc_num}")
            time.sleep(1.0)
            continue

        from_text = "; ".join(result["parties_from"])[:1000] or None
        to_text = "; ".join(result["parties_to"])[:1000] or None

        stmt = (
            pg_insert(OfficialRecordsDailyInstrument)
            .values(
                snapshot_date=row.sale_date or "1900-01-01",
                instrument_number=doc_num,
                doc_type=result["doc_type"] or None,
                recording_date=result["record_date"] or None,
                parties_from_text=from_text,
                parties_to_text=to_text,
            )
            .on_conflict_do_nothing(index_elements=["instrument_number"])
        )

        with engine.begin() as conn:
            conn.execute(stmt)

        success += 1
        logger.success(
            f"  Inserted {doc_num}: from=({len(result['parties_from'])}) "
            f"to=({len(result['parties_to'])})"
        )

        # Rate limit — be polite to the clerk API
        time.sleep(1.0)

    logger.info(f"Backfill complete: {success}/{len(rows)} records enriched.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", help="PostgreSQL DSN")
    parser.add_argument("--limit", type=int, default=100, help="Max records to fetch (default: 100)")
    args = parser.parse_args()
    backfill(args.dsn, args.limit)


if __name__ == "__main__":
    main()
