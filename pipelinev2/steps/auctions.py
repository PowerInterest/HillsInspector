from __future__ import annotations

import asyncio
from datetime import timedelta

from src.scrapers.auction_scraper import AuctionScraper

from pipelinev2.state import RunContext, StepResult
from pipelinev2.steps.base import timed_step
from pipelinev2.services import get_db, get_storage

STEP_NAME = "auctions"


def run(context: RunContext) -> StepResult:
    with timed_step(STEP_NAME) as elapsed_ms:
        db = get_db(context)
        storage = get_storage(context, db=db)
        scraper = AuctionScraper(storage=storage, process_final_judgments=False)

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
