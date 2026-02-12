#!/usr/bin/env python3
"""Build data/history.db in the schema expected by app/web/routers/history.py.

Input is the HillsForeclosures benchmark dataset produced by
scripts/hills_benchmark_extract.py (JSONL records).

This is intended for read-only web benchmarking; it does not touch the pipeline DB.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--input-jsonl",
        type=Path,
        default=Path("data/temp/hills_benchmark/20260211_223119/hills_listings.jsonl"),
        help="Path to hills_listings.jsonl",
    )
    p.add_argument(
        "--out-db",
        type=Path,
        default=Path("data/history.db"),
        help="Output SQLite DB path (default data/history.db)",
    )
    p.add_argument(
        "--replace",
        action="store_true",
        help="Replace out-db if it exists",
    )
    return p.parse_args()


def _strip_ordinal_day(value: str) -> str:
    # February 11th, 2026 -> February 11, 2026
    return re.sub(r"(\b\d{1,2})(st|nd|rd|th)\b", r"\1", value)


def parse_hills_auction_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = _strip_ordinal_day(value.strip())
    try:
        dt = datetime.strptime(text, "%B %d, %Y")
    except ValueError:
        return None
    return dt.strftime("%Y-%m-%d")


def parse_money(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value)
    m = re.search(r"-?\$?\s*([0-9][0-9,]*)(?:\.(\d+))?", text)
    if not m:
        return None
    whole = m.group(1).replace(",", "")
    frac = m.group(2) or "0"
    try:
        return float(f"{whole}.{frac}")
    except ValueError:
        return None


def clean_address(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = value.replace("\t", " ")
    text = re.sub(r"\s+", " ", text).strip()
    text = text.replace(" Foreclosure Information", "").strip()
    # Sometimes the crawler captured the H1 with trailing words; keep only first clause.
    return text


def buyer_type_from_record(rec: dict[str, Any]) -> str:
    # Router expects buyer_type == 'Third Party' for investor stats.
    bidder = (
        (((rec.get("section_data") or {}).get("Auction Details") or {}).get("Bidders Name"))
        or ""
    )
    bidder_norm = bidder.strip().lower()
    if bidder_norm == "plaintiff":
        return "Plaintiff"
    if bidder_norm:
        return "Third Party"
    return "Unknown"


def init_db(path: Path, replace: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if replace and path.exists():
        path.unlink()
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(
            """
            PRAGMA journal_mode=DELETE;
            PRAGMA synchronous=NORMAL;

            DROP TABLE IF EXISTS auctions;
            DROP TABLE IF EXISTS resales;

            CREATE TABLE auctions (
                auction_id INTEGER PRIMARY KEY,
                auction_date TEXT,
                case_number TEXT,
                property_address TEXT,
                sold_to TEXT,
                winning_bid REAL,
                final_judgment_amount REAL,
                status TEXT,
                pdf_url TEXT,
                buyer_type TEXT
            );

            CREATE TABLE resales (
                auction_id INTEGER,
                sale_date TEXT,
                sale_price REAL,
                gross_profit REAL,
                roi REAL,
                hold_time_days INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_auctions_date ON auctions(auction_date);
            CREATE INDEX IF NOT EXISTS idx_auctions_case ON auctions(case_number);
            CREATE INDEX IF NOT EXISTS idx_resales_auction ON resales(auction_id);
            """
        )
        conn.commit()
    finally:
        conn.close()


def load_records(jsonl: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with jsonl.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def insert_auctions(db_path: Path, records: list[dict[str, Any]]) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        rows = []
        for rec in records:
            pid = rec.get("property_id")
            try:
                auction_id = int(pid)
            except Exception:
                continue

            auction_date = parse_hills_auction_date(rec.get("auction_date"))
            case_number = rec.get("case_number")
            address = clean_address(rec.get("full_address"))
            sold_to = rec.get("winner_name")
            winning_bid = parse_money(rec.get("winning_bid"))

            # Hills data has a final judgment reference (not amount). Leave amount null.
            final_judgment_amount = None
            auction_status = rec.get("auction_status")
            auction_type = rec.get("auction_type")
            status = " ".join([p for p in [auction_type, auction_status] if p]).strip() or "Unknown"

            # The history UI links case_number -> pdf_url. Use property page URL for now.
            pdf_url = rec.get("url")
            buyer_type = buyer_type_from_record(rec)

            rows.append(
                (
                    auction_id,
                    auction_date,
                    case_number,
                    address,
                    sold_to,
                    winning_bid,
                    final_judgment_amount,
                    status,
                    pdf_url,
                    buyer_type,
                )
            )

        cur.executemany(
            """
            INSERT OR REPLACE INTO auctions(
                auction_id, auction_date, case_number, property_address, sold_to,
                winning_bid, final_judgment_amount, status, pdf_url, buyer_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

        # resales table intentionally left empty for this benchmark dataset.
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    args = parse_args()
    init_db(args.out_db, replace=args.replace)
    records = load_records(args.input_jsonl)
    insert_auctions(args.out_db, records)
    print(f"Wrote history DB: {args.out_db} (auctions={len(records)})")


if __name__ == "__main__":
    main()

