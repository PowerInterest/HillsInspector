"""
Workflow: Attempt a stealth scrape of auction.com for one property from the DB.

Steps:
- Grab a property address from the auctions table (prefer Tampa/FL if present).
- Launch Playwright with stealth and search auction.com for that address.
- Save a screenshot of the results (or failure state) under logs/auction_com/.
"""

from __future__ import annotations

import asyncio

from loguru import logger

from src.db.operations import PropertyDB
from src.scrapers.auction_com_scraper import AuctionComScraper


def pick_address(db: PropertyDB) -> str:
    conn = db.connect()
    row = conn.execute(
        """
        SELECT property_address
        FROM auctions
        WHERE property_address IS NOT NULL
          AND property_address != ''
          AND (LOWER(property_address) LIKE '%tampa%' OR LOWER(property_address) LIKE '%fl%')
        LIMIT 1
        """
    ).fetchone()
    if row and row[0]:
        return row[0]
    fallback = conn.execute(
        "SELECT property_address FROM auctions WHERE property_address IS NOT NULL AND property_address != '' LIMIT 1"
    ).fetchone()
    if fallback and fallback[0]:
        return fallback[0]
    raise RuntimeError("No property_address found in auctions table.")


async def main(headless: bool = True):
    logger.info("Starting Auction.com stealth workflow")
    db = PropertyDB()
    address = pick_address(db)
    logger.info(f"Using property address: {address}")

    scraper = AuctionComScraper(headless=headless)
    result = await scraper.search_address(address)

    if result.success:
        logger.success(f"Listings found: {result.listing_count} (screenshot: {result.screenshot_path})")
    else:
        logger.warning(f"Search failed: {result.error} (screenshot: {result.screenshot_path})")
        if result.note:
            logger.info(f"Note: {result.note}")


if __name__ == "__main__":
    asyncio.run(main())
