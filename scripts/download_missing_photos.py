"""One-off script to download photos for properties that have CDN URLs but no local paths."""

from __future__ import annotations

from sqlalchemy import text

from src.services.market_data_service import MarketDataService
from sunbiz.db import get_engine, resolve_pg_dsn


def main() -> None:
    dsn = resolve_pg_dsn(None)
    engine = get_engine(dsn)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT pm.strap, f.case_number_raw as case_number "
                "FROM property_market pm "
                "JOIN foreclosures f ON f.strap = pm.strap AND f.archived_at IS NULL "
                "WHERE pm.photo_cdn_urls IS NOT NULL "
                "  AND jsonb_array_length(pm.photo_cdn_urls) > 0 "
                "  AND (pm.photo_local_paths IS NULL "
                "       OR jsonb_array_length(pm.photo_local_paths) = 0)"
            )
        ).fetchall()

    props = [{"strap": r[0], "case_number": r[1]} for r in rows]
    print(f"Found {len(props)} properties needing photo download")

    if not props:
        return

    svc = MarketDataService(dsn=dsn)
    count = svc.download_all_photos(props)
    print(f"Downloaded {count} new photos")


if __name__ == "__main__":
    main()
