import argparse
import asyncio
import sys
from pathlib import Path
from loguru import logger

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.history.scrape_history import HistoricalScraper
from src.utils.logging_config import configure_logger

configure_logger(log_file="history_pipeline.log")


async def run_backfill(limit_dates: int | None, headless: bool) -> None:
    scraper = HistoricalScraper(
        max_concurrent=1,
        headless=headless,
        browser_names=["chromium", "firefox", "webkit"],
    )
    try:
        await scraper.backfill_missing_winning_bids(limit_dates=limit_dates)
    finally:
        await scraper.close_browser()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill missing winning_bid values in data/history.db",
    )
    parser.add_argument(
        "--limit-dates",
        type=int,
        default=None,
        help="Maximum number of auction dates to re-scrape (for testing).",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run browser in headed mode.",
    )
    args = parser.parse_args()

    logger.info(
        "Starting winning bid backfill (limit_dates=%s, headed=%s)",
        args.limit_dates,
        args.headed,
    )
    asyncio.run(run_backfill(args.limit_dates, headless=not args.headed))


if __name__ == "__main__":
    main()
