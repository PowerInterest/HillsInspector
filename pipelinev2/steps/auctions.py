from __future__ import annotations

import asyncio
from datetime import date, timedelta
from pathlib import Path

from src.scrapers.auction_scraper import AuctionScraper

from pipelinev2.state import RunContext, StepResult
from pipelinev2.steps.base import timed_step
from pipelinev2.services import get_data_dir, get_db, get_storage

STEP_NAME = "auctions"


def run(context: RunContext) -> StepResult:
    with timed_step(STEP_NAME) as elapsed_ms:
        db = get_db(context)
        storage = get_storage(context, db=db)
        scraper = AuctionScraper(storage=storage, process_final_judgments=False)
        data_dir = get_data_dir(context)

        def _save_to_inbox_v2(prop):
            import polars as pl

            case_dir = data_dir / "Foreclosure" / prop.case_number
            case_dir.mkdir(parents=True, exist_ok=True)
            data = {
                "case_number": prop.case_number,
                "parcel_id": prop.parcel_id,
                "address": prop.address,
                "city": prop.city,
                "zip_code": prop.zip_code,
                "assessed_value": prop.assessed_value,
                "final_judgment_amount": prop.final_judgment_amount,
                "auction_date": str(prop.auction_date) if prop.auction_date else None,
                "auction_type": prop.auction_type,
                "plaintiff": prop.plaintiff,
                "defendant": prop.defendant,
                "instrument_number": prop.instrument_number,
                "legal_description": prop.legal_description,
                "scraped_at": str(date.today()),
            }
            try:
                df = pl.DataFrame([data])
                output_path = case_dir / "auction.parquet"
                df.write_parquet(output_path)
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(f"Failed to save parquet for {prop.case_number}: {exc}") from exc

        scraper.save_to_inbox = _save_to_inbox_v2

        total_scraped = 0
        days_scraped = 0
        days_skipped = 0
        days_weekend = 0
        days_already_scraped = 0
        days_existing_auctions = 0

        async def _run():
            nonlocal total_scraped, days_scraped, days_skipped
            nonlocal days_weekend, days_already_scraped, days_existing_auctions
            current = context.start_date
            while current <= context.end_date:
                if current.weekday() >= 5:
                    days_skipped += 1
                    days_weekend += 1
                    current += timedelta(days=1)
                    continue
                count = db.get_auction_count_by_date(current)
                if count == 0 and db.was_auction_scraped(current, "foreclosure"):
                    days_skipped += 1
                    days_already_scraped += 1
                elif count == 0:
                    props = await scraper.scrape_date(
                        current,
                        fast_fail=True,
                        max_properties=context.auction_limit if context.auction_limit and context.auction_limit > 0 else None,
                    )
                    db.record_auction_scrape(current, "foreclosure", len(props))
                    total_scraped += len(props)
                    days_scraped += 1
                else:
                    days_skipped += 1
                    days_existing_auctions += 1
                current += timedelta(days=1)

        asyncio.run(_run())

        return StepResult(
            step=STEP_NAME,
            duration_ms=elapsed_ms(),
            processed=total_scraped,
            succeeded=total_scraped,
            skipped=days_skipped,
            artifacts={
                "days_scraped": days_scraped,
                "days_skipped": days_skipped,
                "days_weekend": days_weekend,
                "days_already_scraped_zero": days_already_scraped,
                "days_existing_auctions": days_existing_auctions,
            },
        )
