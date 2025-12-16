"""
Test HCPA enrichment during auction scrape and compare with bulk data.

This script:
1. Scrapes a few auctions from an upcoming date
2. Enriches each auction with HCPA data immediately
3. Compares HCPA data with bulk parcel data
4. Reports differences

Usage:
    uv run python scripts/test_hcpa_enrichment.py --max 3
"""

import asyncio
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from loguru import logger

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Use read-only mode if main DB is locked - set env before imports
os.environ["HILLS_SCRAPER_STORAGE_SKIP_INIT"] = "1"

from src.scrapers.auction_scraper import AuctionScraper
from src.db.operations import PropertyDB


# Configure logging
logger.remove()
logger.add(
    sys.stderr,
    level="INFO",
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
)


def compare_with_bulk_data(prop, db) -> dict:
    """Compare scraped property with bulk data in database."""
    comparison = {
        "folio": prop.parcel_id,
        "case_number": prop.case_number,
        "hcpa_scrape": {},
        "bulk_data": {},
        "differences": [],
    }

    if not prop.parcel_id:
        comparison["differences"].append("No parcel_id to compare")
        return comparison

    # Get bulk data from parcels table
    try:
        bulk = db.conn.execute("""
            SELECT
                folio,
                owner_name,
                property_address,
                city,
                zip_code,
                year_built,
                beds,
                baths,
                heated_area,
                assessed_value,
                raw_legal1,
                raw_legal2,
                raw_legal3,
                raw_legal4
            FROM parcels
            WHERE folio = ?
        """, [prop.parcel_id]).fetchone()
    except Exception as e:
        comparison["differences"].append(f"DB error: {e}")
        return comparison

    if not bulk:
        comparison["differences"].append(f"Folio {prop.parcel_id} not found in bulk data")
        return comparison

    # Store bulk data
    comparison["bulk_data"] = {
        "folio": bulk[0],
        "owner_name": bulk[1],
        "address": bulk[2],
        "city": bulk[3],
        "zip_code": bulk[4],
        "year_built": bulk[5],
        "beds": bulk[6],
        "baths": bulk[7],
        "heated_area": bulk[8],
        "assessed_value": bulk[9],
        "legal_description": " ".join(filter(None, [bulk[10], bulk[11], bulk[12], bulk[13]])),
    }

    # Store HCPA scrape data
    comparison["hcpa_scrape"] = {
        "address": prop.address,
        "year_built": prop.year_built,
        "legal_description": prop.legal_description,
        "sales_history_count": len(prop.sales_history) if prop.sales_history else 0,
        "image_url": prop.image_url,
    }

    # Compare fields
    if comparison["bulk_data"]["legal_description"] != comparison["hcpa_scrape"]["legal_description"]:
        comparison["differences"].append({
            "field": "legal_description",
            "bulk": comparison["bulk_data"]["legal_description"][:100] if comparison["bulk_data"]["legal_description"] else None,
            "hcpa": comparison["hcpa_scrape"]["legal_description"][:100] if comparison["hcpa_scrape"]["legal_description"] else None,
        })

    if comparison["bulk_data"]["year_built"] != comparison["hcpa_scrape"]["year_built"]:
        comparison["differences"].append({
            "field": "year_built",
            "bulk": comparison["bulk_data"]["year_built"],
            "hcpa": comparison["hcpa_scrape"]["year_built"],
        })

    # Check if HCPA gave us data bulk didn't have
    if comparison["hcpa_scrape"]["sales_history_count"] > 0:
        comparison["differences"].append({
            "field": "sales_history",
            "note": f"HCPA provided {comparison['hcpa_scrape']['sales_history_count']} sales records",
        })

    if comparison["hcpa_scrape"]["image_url"] and not comparison["bulk_data"].get("image_url"):
        comparison["differences"].append({
            "field": "image_url",
            "note": "HCPA provided image URL (not in bulk)",
        })

    return comparison


async def main(max_properties: int = 3):
    logger.info("=" * 60)
    logger.info("HCPA ENRICHMENT TEST")
    logger.info("=" * 60)

    # Find next weekday for auction
    today = datetime.now(tz=timezone.utc).date()
    target = today + timedelta(days=1)
    while target.weekday() >= 5:  # Skip weekends
        target += timedelta(days=1)

    logger.info(f"Scraping auctions for {target}")

    scraper = AuctionScraper()

    # Connect to DB in read-only mode to avoid lock conflicts
    import duckdb
    db = PropertyDB()
    db.conn = duckdb.connect(db.db_path, read_only=True)

    try:
        # Scrape with limit
        properties = await scraper.scrape_date(target, fast_fail=True, max_properties=max_properties)

        logger.info(f"Scraped {len(properties)} properties")

        # Compare each with bulk data
        for prop in properties:
            logger.info("-" * 40)
            logger.info(f"Property: {prop.case_number}")
            logger.info(f"  Parcel ID: {prop.parcel_id}")
            logger.info(f"  Address: {prop.address}")
            logger.info(f"  HCPA URL: {prop.hcpa_url}")
            logger.info(f"  Legal (from HCPA): {prop.legal_description[:80] if prop.legal_description else 'N/A'}...")
            logger.info(f"  Year Built: {prop.year_built}")
            logger.info(f"  Sales History: {len(prop.sales_history) if prop.sales_history else 0} records")

            comparison = compare_with_bulk_data(prop, db)

            if comparison["differences"]:
                logger.warning("  Differences found:")
                for diff in comparison["differences"]:
                    if isinstance(diff, dict):
                        logger.warning(f"    - {diff.get('field', 'unknown')}: bulk={diff.get('bulk')}, hcpa={diff.get('hcpa')}, note={diff.get('note', '')}")
                    else:
                        logger.warning(f"    - {diff}")
            else:
                logger.success("  No differences found - data matches!")

    finally:
        db.close()

    logger.info("=" * 60)
    logger.info("TEST COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Test HCPA enrichment")
    parser.add_argument("--max", type=int, default=3, help="Max properties to scrape")

    args = parser.parse_args()

    asyncio.run(main(max_properties=args.max))
