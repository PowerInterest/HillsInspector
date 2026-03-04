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
  17. Encumbrance audit (read-only coverage/signals over PG foreclosure data)
  18. Encumbrance recovery (targeted ORI/mortgage/survival retries from audit)
  19. Final refresh (pick up Phase B data)
  20. Market data (Redfin/Zillow/HomeHarvest; optional background worker)

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
import shutil
import subprocess
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


EXPECTED_CRON_JOBS = [
    "auction_results",
    "trust_accounts",
    "clerk_bulk",
    "sunbiz_daily",
    "county_permits",
    "tampa_permits",
    "market_data",
    "single_pin_permits",
    "hcpa_bulk",
    "clerk_criminal",
    "clerk_civil_alpha",
    "sunbiz_flr_quarterly",
    "sunbiz_entity_quarterly",
    "dor_nal_annual",
]


def _check_crontab_health() -> None:
    """Verify all expected scheduled jobs are present in the user's crontab."""
    crontab_bin = shutil.which("crontab")
    if not crontab_bin:
        logger.warning("`crontab` binary not found; skipping scheduled-job health check.")
        return

    try:
        result = subprocess.run(
            [crontab_bin, "-l"], capture_output=True, text=True, timeout=5, check=False
        )
        crontab_text = result.stdout if result.returncode == 0 else ""
    except Exception:
        crontab_text = ""

    if not crontab_text.strip():
        logger.error(
            "CRONTAB IS EMPTY — no scheduled jobs are configured. "
            "Bulk data (clerk, permits, market, trust) will NOT refresh automatically. "
            "See docs/guides/SCHEDULED_JOBS.md for setup instructions."
        )
        return

    missing = [
        job for job in EXPECTED_CRON_JOBS
        if f"--job {job}" not in crontab_text
    ]
    if missing:
        logger.warning(
            "Crontab is missing {} of {} scheduled jobs: {}. "
            "See docs/guides/SCHEDULED_JOBS.md.",
            len(missing), len(EXPECTED_CRON_JOBS), ", ".join(missing),
        )
    else:
        logger.info(
            "Crontab OK: all {} scheduled jobs are configured.", len(EXPECTED_CRON_JOBS)
        )


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

        _check_crontab_health()

        controller = PgPipelineController(settings)
        result = controller.run()

        logger.info("PG controller complete")
        print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
