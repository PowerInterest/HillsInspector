"""Standalone market-data worker for PG foreclosures."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from loguru import logger
from sqlalchemy import text

from scripts.refresh_foreclosures import refresh as refresh_foreclosures
from src.services.market_data_service import MarketDataService
from sunbiz.db import get_engine, resolve_pg_dsn


def _query_properties_needing_market(
    dsn: str,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    query = """
        SELECT f.strap, f.folio, f.case_number_raw AS case_number, f.property_address
        FROM foreclosures f
        LEFT JOIN property_market pm ON f.strap = pm.strap
        WHERE f.strap IS NOT NULL
          AND f.property_address IS NOT NULL
          AND (pm.strap IS NULL
               OR NOT (pm.redfin_json IS NOT NULL
                       AND pm.zillow_json IS NOT NULL
                       AND pm.homeharvest_json IS NOT NULL))
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
) -> dict[str, Any]:
    resolved_dsn = resolve_pg_dsn(dsn)
    properties = _query_properties_needing_market(dsn=resolved_dsn, limit=limit)
    if not properties:
        return {"skipped": True, "reason": "no_properties_need_market_data"}

    logger.info(f"Market data worker: {len(properties)} foreclosures need market data")

    service = MarketDataService(dsn=resolved_dsn)
    result = asyncio.run(service.run_batch(properties))
    if result.get("error"):
        return {
            "properties_queried": len(properties),
            "update": result,
            "error": result["error"],
        }

    try:
        refresh_counts = refresh_foreclosures(dsn=resolved_dsn)
        result["foreclosure_refresh"] = refresh_counts
    except Exception as exc:
        logger.warning(f"Post-market foreclosure refresh failed: {exc}")

    return {"properties_queried": len(properties), "update": result}


def _payload_failed(payload: dict[str, Any]) -> bool:
    if payload.get("success") is False:
        return True
    if payload.get("error") not in (None, ""):
        return True

    update = payload.get("update")
    if isinstance(update, dict):
        if update.get("success") is False:
            return True
        if update.get("error") not in (None, ""):
            return True
    return False


def main() -> None:
    result = run_market_data_update()
    logger.info(f"Market data worker complete: {result}")
    print(json.dumps(result, indent=2, default=str))
    if _payload_failed(result):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
