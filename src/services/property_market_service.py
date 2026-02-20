"""
Property Market Service — photo download & market consolidation.

Consolidated into MarketDataService for the PG-only pipeline.
This module is kept as a standalone CLI entry point for backwards compatibility.

Usage::

    uv run python -m src.services.property_market_service --limit 5
"""

from __future__ import annotations

import asyncio

from loguru import logger

from src.services.market_data_service import MarketDataService, _query_properties_needing_market


# ---------------------------------------------------------------------------
# Standalone CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Download photos and consolidate market data to PG",
    )
    parser.add_argument("--limit", type=int, default=0, help="Max properties")
    parser.add_argument("--dsn", help="PostgreSQL DSN override")
    args = parser.parse_args()

    props = _query_properties_needing_market(dsn=args.dsn, limit=args.limit)
    logger.info(f"Found {len(props)} properties for market data")

    svc = MarketDataService(dsn=args.dsn)
    asyncio.run(svc.run_batch(props))
