from __future__ import annotations

import asyncio

from src.scrapers.tax_deed_scraper import TaxDeedScraper

from pipelinev2.state import RunContext, StepResult
from pipelinev2.steps.base import timed_step
from pipelinev2.services import get_db

STEP_NAME = "tax_deeds"


def run(context: RunContext) -> StepResult:
    with timed_step(STEP_NAME) as elapsed_ms:
        db = get_db(context)
        scraped = 0

        async def _run():
            nonlocal scraped
            scraper = TaxDeedScraper()
            props = await scraper.scrape_all(context.start_date, context.end_date)
            for p in props:
                db.upsert_auction(p)
                db.upsert_status(
                    case_number=p.case_number,
                    parcel_id=p.parcel_id,
                    auction_date=p.auction_date,
                    auction_type="TAX_DEED",
                )
                db.mark_status_step_complete(p.case_number, "step_auction_scraped", 1)
            scraped = len(props)

        asyncio.run(_run())

        return StepResult(
            step=STEP_NAME,
            duration_ms=elapsed_ms(),
            processed=scraped,
            succeeded=scraped,
            skipped=1 if scraped == 0 else 0,
        )
