
from datetime import date, timedelta
import argparse

from loguru import logger

from src.orchestrator import _display_status_summary
from src.db.operations import PropertyDB
from src.utils.time import today_local


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}', expected YYYY-MM-DD"
        ) from exc


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check pipeline status summary over a date window."
    )
    parser.add_argument(
        "--start-date",
        type=_parse_date,
        default=None,
        help="Window start date (YYYY-MM-DD), default=today-2d",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=60,
        help="Window length in days, default=60",
    )
    args = parser.parse_args()

    try:
        start_check = args.start_date or (today_local() - timedelta(days=2))
        if args.days <= 0:
            raise ValueError("--days must be > 0")
        end_check = start_check + timedelta(days=args.days)

        logger.info(f"Checking status from {start_check} to {end_check}")

        with PropertyDB() as db:
            _display_status_summary(db, start_check, end_check)

    except Exception as exc:
        logger.exception(f"Error checking status summary: {exc}")
        raise

if __name__ == "__main__":
    main()
