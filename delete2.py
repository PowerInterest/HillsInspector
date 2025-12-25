import asyncio
from datetime import date, timedelta
from argparse import ArgumentParser

from src.history.scrape_history import HistoricalScraper


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

    return [date.fromisoformat(args.date)]


async def main() -> None:
    parser = ArgumentParser()
    parser.add_argument("--date", default="2025-12-10", help="YYYY-MM-DD")
    parser.add_argument(
        "--dates",
        default="",
        help="Comma-separated list of YYYY-MM-DD dates (overrides --date).",
    )
    parser.add_argument("--start", default="", help="YYYY-MM-DD (range start)")
    parser.add_argument("--end", default="", help="YYYY-MM-DD (range end)")
    args = parser.parse_args()

    target_dates = _parse_dates(args)

    scraper = HistoricalScraper(max_concurrent=1)
    try:
        for target_date in target_dates:
            auctions = await scraper.scrape_single_date(target_date)
            count = 0 if auctions is None else len(auctions)
            winning_bid = 0 if not auctions else sum(1 for a in auctions if a.get("winning_bid"))
            sold_to = 0 if not auctions else sum(1 for a in auctions if a.get("sold_to"))
            print(f"{target_date.isoformat()} auctions={count}")
            print(f"{target_date.isoformat()} winning_bid_nonnull={winning_bid}")
            print(f"{target_date.isoformat()} sold_to_nonnull={sold_to}")
    finally:
        await scraper.close_browser()


if __name__ == "__main__":
    asyncio.run(main())
