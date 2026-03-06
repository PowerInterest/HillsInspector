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
  19. Municipal lien Phase 0 (recorded utility-lien detection → PG findings)
  20. Mortgage extraction from ORI-backed docs
  21. Lien survival analysis (pure computation → PG ori_encumbrances)
  22. Encumbrance audit (read-only issue and coverage metrics)
  23. Encumbrance recovery (targeted backfill through existing writers)
  24. Final refresh (pick up Phase B data)
  25. Market data (Redfin/Zillow/HomeHarvest; optional background worker)

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
import re
import shutil
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import text
from sunbiz.db import get_engine, resolve_pg_dsn

from dotenv import load_dotenv
from loguru import logger

from src.services.pg_pipeline_controller import PgPipelineController, parse_args
from src.utils.logging_config import configure_logger

load_dotenv()

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


def _build_controller_run_id() -> str:
    started_at = dt.datetime.now(dt.UTC)
    return f"{started_at.strftime('%Y%m%dT%H%M%SZ')}-pid{os.getpid()}"


def _configure_controller_run_logging(run_id: str) -> Path:
    run_log = Path("controller_runs") / f"controller-{run_id}.log"
    configure_logger(extra_log_files=[run_log])
    return Path("logs") / run_log


def _read_alembic_revision(engine: Engine) -> str | None:
    try:
        with engine.connect() as conn:
            return conn.execute(
                text("SELECT version_num FROM alembic_version LIMIT 1")
            ).scalar_one_or_none()
    except Exception:
        logger.warning("Could not read alembic_version table (may not exist yet)")
        return None


def _run_alembic_command(dsn: str, *args: str) -> str:
    config_path = Path(__file__).resolve().parent / "alembic.ini"
    alembic_exe = shutil.which("alembic")
    if not alembic_exe:
        raise RuntimeError("`alembic` executable was not found on PATH.")
    env = os.environ.copy()
    env["SUNBIZ_PG_DSN"] = dsn
    result = subprocess.run(
        [alembic_exe, "-c", str(config_path), *args],
        cwd=config_path.parent,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    output = f"{result.stdout}\n{result.stderr}".strip()
    if result.returncode != 0:
        raise RuntimeError(
            f"alembic {' '.join(args)} failed with exit code {result.returncode}\n{output}"
        )
    return output


def _get_alembic_head_revision(dsn: str) -> str:
    output = _run_alembic_command(dsn, "heads")
    heads = []
    for line in output.splitlines():
        match = re.match(r"^\s*([0-9a-z_]+)\s+\(head\)\s*$", line.strip())
        if match:
            heads.append(match.group(1))
    if not heads:
        raise RuntimeError(f"Unable to parse alembic heads output:\n{output}")
    if len(heads) != 1:
        raise RuntimeError(f"Expected exactly one alembic head, got {heads}")
    return heads[0]


def _upgrade_pg_schema_to_head(dsn: str, engine: Engine) -> tuple[str | None, str | None, str]:
    before_revision = _read_alembic_revision(engine)
    head_revision = _get_alembic_head_revision(dsn)
    _run_alembic_command(dsn, "upgrade", "head")
    after_revision = _read_alembic_revision(engine)
    return before_revision, after_revision, head_revision


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

        # Keep database schema at Alembic head before pipeline execution.
        try:
            before_revision, after_revision, head_revision = _upgrade_pg_schema_to_head(
                dsn=dsn,
                engine=engine,
            )
            logger.info(
                "Alembic revision sync: before={} after={} head={}",
                before_revision or "none",
                after_revision or "none",
                head_revision,
            )
            if after_revision != head_revision:
                logger.error(
                    "Alembic revision mismatch after upgrade (after={} head={}).",
                    after_revision,
                    head_revision,
                )
                sys.exit(1)
        except Exception as e:
            logger.error(f"Alembic upgrade failed: {e}")
            logger.error(
                "Resolve migration errors and rerun. If this is a new database, initialize core schema first with "
                "`uv run python -m src.db.migrations.create_foreclosures`."
            )
            sys.exit(1)

        controller = PgPipelineController(settings)
        result = controller.run()

        logger.info("PG controller complete")
        print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
