import asyncio
import json
import sys
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.scripts.photo_metrics import get_market_photo_metrics  # noqa: E402
from src.services.market_data_service import MarketDataService  # noqa: E402
from sunbiz.db import get_engine, resolve_pg_dsn  # noqa: E402


def get_straps_needing_backfill():
    engine = get_engine(resolve_pg_dsn())
    query = """
    SELECT strap, property_address, case_number_raw as case_number
    FROM foreclosures
    WHERE property_address IS NOT NULL AND TRIM(property_address) <> ''
    """
    with engine.connect() as conn:
        rows = conn.execute(text(query)).fetchall()

        # Build property dicts
        return [{"strap": r.strap, "property_address": r.property_address, "case_number": r.case_number} for r in rows]


def test():
    """Run a single test download against the database."""
    print("Metrics BEFORE:")
    metrics = get_market_photo_metrics()
    print(json.dumps(metrics, indent=2))

    props = get_straps_needing_backfill()
    print(f"Total foreclosures: {len(props)}")

    svc = MarketDataService(use_windows_chrome=False)
    # sources=[] means skip ALL scraping sources, JUST do photos for whatever is generated in need_market!
    # Wait, need_market is only built from sources requested.
    # If sources is empty, need_market won't be populated properly if needs_photos is built off sources.
    # Ah! `run_batch` accepts `sources=["photos_only"]` but photo download doesn't require "photos_only" to be in sources... wait... "photos_only" is ignored by others.
    # Actually wait: MarketDataService doesn't accept "photos_only".
    # I can just call `_download_all_photos` directly!

    print("\nDownloading photos directly...")
    downloaded = svc._download_all_photos(props)  # noqa: SLF001

    print(f"\nDownloaded: {downloaded}")

    print("\nMetrics AFTER:")
    print(json.dumps(get_market_photo_metrics(), indent=2))


if __name__ == "__main__":
    asyncio.run(test())
