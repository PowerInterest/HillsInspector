"""PG-first entrypoint controller — single pipeline for the entire project.

Phase A (bulk data refresh — idempotent, no per-property scraping):
  1. HCPA suite (parcels, sales, subdivisions)
  2. Clerk bulk (cases, events)
  3. Clerk criminal index
  4. Clerk civil alpha index
  5. DOR NAL (tax data)
  6. Sunbiz UCC (SFTP filings)
  7. Sunbiz entities
  8. County permits (API)
  9. Tampa permits (Accela scrape)
  10. Single-pin permit gap fill (targeted HCPA/Accela/ArcGIS pull)
  11. Foreclosure refresh (join all bulk → hub table)
  12. Trust accounts
  13. Title chain (PG chain builder)
  14. Title breaks (targeted reconciliation)

Phase B (per-auction enrichment — scraping + analysis):
  15. Scrape upcoming auctions (Playwright → clerk website → PG)
  16. Extract judgment PDFs (VisionService → disk JSON → PG)
  17. Recover missing strap/folio from final-judgment data
  18. ORI document search (Playwright → ORI website → PG ori_encumbrances)
  19. Mortgage extraction from ORI-backed docs
  20. Lien survival analysis (pure computation → PG ori_encumbrances)
  21. Encumbrance audit (read-only issue and coverage metrics)
  22. Encumbrance recovery (targeted backfill through existing writers)
  23. Final refresh (pick up Phase B data)
  24. Market data (Redfin/Zillow/HomeHarvest; optional background worker)

Usage:
  uv run Controller.py                           # Full pipeline
  uv run Controller.py --skip-hcpa --skip-nal    # Skip bulk steps
  uv run Controller.py --skip-auction-scrape     # Skip Phase B scraping
  uv run Controller.py --ori-limit 10            # Limit ORI to 10 properties
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path

from sqlalchemy import text
from sunbiz.db import get_engine, resolve_pg_dsn

from dotenv import load_dotenv
from loguru import logger

from src.services.pg_pipeline_controller import PgPipelineController, parse_args
from src.utils.logging_config import configure_logger

load_dotenv()


def _build_controller_run_id() -> str:
    started_at = dt.datetime.now(dt.UTC)
    return f"{started_at.strftime('%Y%m%dT%H%M%SZ')}-pid{os.getpid()}"


def _configure_controller_run_logging(run_id: str) -> Path:
    run_log = Path("controller_runs") / f"controller-{run_id}.log"
    configure_logger(extra_log_files=[run_log])
    return Path("logs") / run_log


def main() -> None:
    settings = parse_args()
    run_id = _build_controller_run_id()
    run_log_path = _configure_controller_run_logging(run_id)

    with logger.contextualize(run_id=run_id):
        logger.info("PG controller run log: {}", run_log_path)
        logger.info("PG controller start: {}", asdict(settings))

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
