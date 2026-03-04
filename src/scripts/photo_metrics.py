"""
Market Photo Coverage Metrics

Defines the 4 key questions for property_market photo status to ensure
consistent reporting across the application.
"""

import json
import sys
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sunbiz.db import get_engine, resolve_pg_dsn  # noqa: E402


def get_market_photo_metrics() -> dict:
    """Returns the core photo coverage metrics for the property_market table."""
    engine = get_engine(resolve_pg_dsn())

    query = """
    WITH stats AS (
        SELECT 
            -- 1. "How many properties have remote photos available?"
            COUNT(*) FILTER (
                WHERE photo_cdn_urls IS NOT NULL
                  AND jsonb_typeof(photo_cdn_urls) = 'array'
                  AND jsonb_array_length(photo_cdn_urls) > 0
            ) as has_remote,
            
            -- 2. "How many properties have saved local pictures on disk?"
            COUNT(*) FILTER (
                WHERE photo_local_paths IS NOT NULL
                  AND jsonb_typeof(photo_local_paths) = 'array'
                  AND jsonb_array_length(photo_local_paths) > 0
            ) as has_local,
            
            -- 3. "How many properties are missing all pictures?"
            COUNT(*) FILTER (
                WHERE (
                    photo_cdn_urls IS NULL
                    OR jsonb_typeof(photo_cdn_urls) != 'array'
                    OR jsonb_array_length(photo_cdn_urls) = 0
                )
                  AND (
                    photo_local_paths IS NULL
                    OR jsonb_typeof(photo_local_paths) != 'array'
                    OR jsonb_array_length(photo_local_paths) = 0
                )
            ) as fully_missing,
                             
            -- 4. "How many properties still need local photo backfill?"
            -- This happens when we have remote URLs, but local paths are either null, empty, 
            -- or less than the target cache size (15) and less than the total remote count.
            COUNT(*) FILTER (
                WHERE photo_cdn_urls IS NOT NULL
                AND jsonb_typeof(photo_cdn_urls) = 'array'
                AND jsonb_array_length(photo_cdn_urls) > 0 
                AND (
                    photo_local_paths IS NULL 
                    OR jsonb_typeof(photo_local_paths) != 'array'
                    OR jsonb_array_length(photo_local_paths) = 0
                    OR (jsonb_array_length(photo_local_paths) < 15 AND jsonb_array_length(photo_local_paths) < jsonb_array_length(photo_cdn_urls))
                )
            ) as needs_backfill,
            
            -- Bonus: total tracked properties in market data
            COUNT(*) as total_market_rows
        FROM property_market
    )
    SELECT * FROM stats;
    """

    with engine.connect() as conn:
        row = conn.execute(text(query)).mappings().one_or_none()
        return dict(row) if row else {}


if __name__ == "__main__":
    metrics = get_market_photo_metrics()
    print(json.dumps(metrics, indent=2))
