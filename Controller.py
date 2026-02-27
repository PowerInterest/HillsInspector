"""PG-first entrypoint controller — single pipeline for the entire project.

Phase A (bulk data refresh — idempotent, no per-property scraping):
  1. HCPA suite (parcels, sales, subdivisions)
  2. Clerk bulk (cases, events)
  3. DOR NAL (tax data)
  4. Sunbiz UCC (SFTP filings)
  5. Sunbiz entities
  6. County permits (API)
  7. Tampa permits (Accela scrape)
  8. Single-pin permit gap fill (targeted HCPA/Accela/ArcGIS pull)
  9. Foreclosure refresh (join all bulk → hub table)
  10. Trust accounts
  11. Title chain (PG chain builder)

Phase B (per-auction enrichment — scraping + analysis):
  12. Scrape upcoming auctions (Playwright → clerk website → PG)
  13. Extract judgment PDFs (VisionService → disk JSON → PG)
  14. Recover missing strap/folio from final-judgment data
  15. ORI document search (Playwright → ORI website → PG ori_encumbrances)
  16. Lien survival analysis (pure computation → PG ori_encumbrances)
  17. Final refresh (pick up Phase B data)
  18. Market data (Redfin/Zillow/HomeHarvest; optional background worker)

Usage:
  uv run Controller.py                           # Full pipeline
  uv run Controller.py --skip-hcpa --skip-nal    # Skip bulk steps
  uv run Controller.py --skip-auction-scrape     # Skip Phase B scraping
  uv run Controller.py --ori-limit 10            # Limit ORI to 10 properties
"""

from __future__ import annotations

import json
import sys
from dataclasses import asdict

from sqlalchemy import text
from sunbiz.db import get_engine, resolve_pg_dsn

from dotenv import load_dotenv
from loguru import logger

from src.services.pg_pipeline_controller import PgPipelineController, parse_args

load_dotenv()


def main() -> None:
    settings = parse_args()
    logger.info(f"PG controller start: {asdict(settings)}")

    # Check PostgreSQL connection before starting
    try:
        dsn = resolve_pg_dsn(settings.dsn)
        engine = get_engine(dsn)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as e:
        logger.error(f"PostgreSQL connection failed: {e}")
        logger.error("Please ensure the PostgreSQL server is running and accepting TCP/IP connections.")
        sys.exit(1)

    controller = PgPipelineController(settings)
    result = controller.run()

    logger.info("PG controller complete")
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
