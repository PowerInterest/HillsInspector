import asyncio
import sys
from argparse import ArgumentParser
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import duckdb
from loguru import logger
import time

# Ensure project root is on sys.path when running as a script.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.history.scrape_history import HistoricalScraper
from src.history.db_init import ensure_history_schema
from src.utils.time import ensure_duckdb_utc


DB_PATH = Path("data/history.db")


def _parse_dates(args) -> list[date]:
    dates: list[date] = []
    if args.dates:
        for raw in args.dates.split(","):
            raw = raw.strip()
            if raw:
                dates.append(date.fromisoformat(raw))
        return dates

    if args.start and args.end:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
        current = start
        while current <= end:
            if current.weekday() < 5:
                dates.append(current)
            current += timedelta(days=1)
        return dates

    if args.date:
        return [date.fromisoformat(args.date)]

    return []


def _load_problem_dates(limit: int | None = None) -> list[date]:
    if not DB_PATH.exists():
        raise SystemExit(f"history DB not found: {DB_PATH}")
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    ensure_duckdb_utc(conn)
    try:
        query = """
            SELECT DISTINCT auction_date
            FROM auctions
            WHERE auction_date IS NOT NULL
              AND (winning_bid IS NULL OR sold_to IS NULL OR status IS NULL OR status = 'Unknown')
            ORDER BY auction_date
        """
        rows = conn.execute(query).fetchall()
        dates = [r[0].date() if hasattr(r[0], "date") else r[0] for r in rows]
        if limit:
            dates = dates[:limit]
        return dates
    finally:
        conn.close()


async def _scrape_and_save(scraper: HistoricalScraper, target_date: date, dry_run: bool) -> dict:
    auctions = await scraper.scrape_single_date(target_date)
    if auctions is None:
        return {"date": target_date, "status": "failed", "count": 0}
    if dry_run:
        return {"date": target_date, "status": "dry_run", "count": len(auctions)}

    if auctions:
        scraper.save_batch(auctions)
        scraper.mark_date_done(target_date, "Success")
        return {"date": target_date, "status": "saved", "count": len(auctions)}

    scraper.mark_date_done(target_date, "Empty")
    return {"date": target_date, "status": "empty", "count": 0}


async def main() -> None:
    ensure_history_schema(DB_PATH)
    parser = ArgumentParser(description="Re-scrape history dates to fix missing fields.")
    parser.add_argument("--date", default="", help="YYYY-MM-DD (single date)")
    parser.add_argument("--dates", default="", help="Comma-separated list of YYYY-MM-DD dates")
    parser.add_argument("--start", default="", help="YYYY-MM-DD (range start)")
    parser.add_argument("--end", default="", help="YYYY-MM-DD (range end)")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of dates when auto-selecting")
    parser.add_argument("--dry-run", action="store_true", help="Scrape but do not write to DB")
    parser.add_argument("--delay", type=int, default=0, help="Wait X seconds between each date to avoid blocking")
    args = parser.parse_args()

    if args.date or args.dates or (args.start and args.end):
        dates = _parse_dates(args)
    else:
        dates = _load_problem_dates(limit=args.limit or None)

    if not dates:
        logger.info("No dates to process.")
        return

    logger.info("Processing %d dates...", len(dates))

    scraper = HistoricalScraper(max_concurrent=1)
    results: list[dict] = []
    try:
        for d in dates:
            logger.info("Re-scraping %s", d.isoformat())
            result = await _scrape_and_save(scraper, d, args.dry_run)
            results.append(result)
            if args.delay and not args.dry_run:
                logger.info("Sleeping %d seconds before next date...", args.delay)
                time.sleep(args.delay)
    finally:
        await scraper.close_browser()

    summary = defaultdict(int)
    for r in results:
        summary[r["status"]] += 1

    logger.info("Summary: %s", dict(summary))


if __name__ == "__main__":
    asyncio.run(main())
