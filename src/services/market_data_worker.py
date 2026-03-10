"""Standalone market-data worker for PG foreclosures."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from loguru import logger
from sqlalchemy import text

from src.scripts.refresh_foreclosures import refresh as refresh_foreclosures
from src.services.market_data_service import (
    MarketDataService,
    _sql_source_has_market_content,
)
from src.utils.step_result import is_failed_payload
from sunbiz.db import get_engine, resolve_pg_dsn


def _query_properties_needing_market(
    dsn: str,
    limit: int | None = None,
    *,
    force: bool = False,
) -> list[dict[str, Any]]:
    redfin_has_content = _sql_source_has_market_content("pm.redfin_json", source="redfin")
    zillow_has_content = _sql_source_has_market_content("pm.zillow_json", source="zillow")
    homeharvest_has_content = _sql_source_has_market_content(
        "pm.homeharvest_json",
        source="homeharvest",
    )
    query = """
        SELECT f.strap, f.folio, f.case_number_raw AS case_number, f.property_address
        FROM foreclosures f
        LEFT JOIN property_market pm ON f.strap = pm.strap
        WHERE f.strap IS NOT NULL
          AND f.property_address IS NOT NULL
          AND f.archived_at IS NULL
    """
    if not force:
        query += f"""
          AND (pm.strap IS NULL
               OR pm.redfin_json IS NULL
               OR pm.redfin_json::text = 'null'
               OR (
                    COALESCE(pm.redfin_json->>'_found', 'true') <> 'false'
                    AND NOT {redfin_has_content}
                  )
               OR pm.zillow_json IS NULL
               OR pm.zillow_json::text = 'null'
               OR (
                    COALESCE(pm.zillow_json->>'_found', 'true') <> 'false'
                    AND NOT {zillow_has_content}
                  )
               OR pm.homeharvest_json IS NULL
               OR pm.homeharvest_json::text = 'null'
               OR (
                    COALESCE(pm.homeharvest_json->>'_found', 'true') <> 'false'
                    AND NOT {homeharvest_has_content}
                  ))
        """
    query += """
        ORDER BY f.auction_date DESC
    """
    params: dict[str, Any] = {}
    if limit and limit > 0:
        query += "\n LIMIT :limit"
        params["limit"] = int(limit)

    engine = get_engine(dsn)
    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()
    return [dict(row._mapping) for row in rows]  # noqa: SLF001


def run_market_data_update(
    dsn: str | None = None,
    limit: int | None = None,
    use_windows_chrome: bool = False,
    force: bool = False,
) -> dict[str, Any]:
    resolved_dsn = resolve_pg_dsn(dsn)
    query_kwargs: dict[str, Any] = {"dsn": resolved_dsn, "limit": limit}
    if force:
        query_kwargs["force"] = True
    properties = _query_properties_needing_market(**query_kwargs)
    if not properties:
        return {"skipped": True, "reason": "no_properties_need_market_data"}

    logger.info(f"Market data worker: {len(properties)} foreclosures need market data")

    service = MarketDataService(dsn=resolved_dsn, use_windows_chrome=use_windows_chrome)
    result = asyncio.run(service.run_batch(properties))
    if result.get("error"):
        return {
            "properties_queried": len(properties),
            "update": result,
            "error": result["error"],
        }
    output: dict[str, Any] = {"properties_queried": len(properties), "update": result}
    if result.get("degraded") is True or result.get("status") == "degraded":
        output["status"] = "degraded"

    try:
        refresh_counts = refresh_foreclosures(dsn=resolved_dsn)
        result["foreclosure_refresh"] = refresh_counts
    except Exception as exc:
        logger.warning(f"Post-market foreclosure refresh failed: {exc}")

    return output


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Standalone Market Data Worker")
    parser.add_argument("--use-windows-chrome", action="store_true", help="Connect to Windows Chrome via CDP")
    parser.add_argument("--force", action="store_true", help="Process all eligible active foreclosures")
    args = parser.parse_args()

    result = run_market_data_update(
        use_windows_chrome=args.use_windows_chrome,
        force=args.force,
    )
    logger.info(f"Market data worker complete: {result}")
    print(json.dumps(result, indent=2, default=str))
    if is_failed_payload(result):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
