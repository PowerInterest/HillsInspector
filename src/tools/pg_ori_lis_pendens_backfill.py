"""Backfill live ORI Lis Pendens coverage for active foreclosure cases.

Architectural purpose:
- Provide a repeatable maintenance CLI for active judged foreclosure cases that
  still have no persisted `lis_pendens` row in `ori_encumbrances`.
- Reuse `PgOriService`'s LP-gap selection and live case-search retry logic
  instead of one-off SQL scripts.

How it fits in the broader system:
- This complements the main `PgOriService.run()` flow by retrying LP recovery
  even after `foreclosures.step_ori_searched` has been set.
- The command stays within the existing PostgreSQL schema and relies on current
  `foreclosures` and `ori_encumbrances` tables for state.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from loguru import logger

try:
    from src.services.pg_ori_service import PgOriService
except ModuleNotFoundError:
    REPO_ROOT = Path(__file__).resolve().parents[2]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from src.services.pg_ori_service import PgOriService


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill live ORI lis pendens coverage for active judged foreclosure "
            "cases that still have no persisted LP."
        )
    )
    parser.add_argument("--dsn", help="PostgreSQL DSN")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of target foreclosures to process",
    )
    parser.add_argument(
        "--foreclosure-id",
        dest="foreclosure_ids",
        action="append",
        type=int,
        default=None,
        help="Restrict the run to a specific foreclosure_id (repeatable)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Probe ORI and report LP matches without saving to PostgreSQL",
    )
    parser.add_argument(
        "--include-never-searched",
        action="store_true",
        help="Include active cases where step_ori_searched is still NULL",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the full result payload as JSON",
    )
    return parser


def _log_summary(result: dict[str, Any]) -> None:
    if result.get("skipped"):
        logger.info(result["reason"])
        return

    logger.info(
        "LP backfill: targets={} live_hits={} docs_found={} saved={} errors={} "
        "remaining_before={} remaining_after={} dry_run={}",
        result.get("targets"),
        result.get("targets_with_lp_docs"),
        result.get("total_lp_docs_found"),
        result.get("total_saved"),
        result.get("errors"),
        result.get("remaining_lp_gaps_before"),
        result.get("remaining_lp_gaps_after"),
        result.get("dry_run"),
    )


def main() -> None:
    args = _build_parser().parse_args()
    service = PgOriService(dsn=args.dsn)
    result = service.run_lis_pendens_backfill(
        limit=args.limit,
        foreclosure_ids=args.foreclosure_ids,
        dry_run=bool(args.dry_run),
        require_ori_searched=not bool(args.include_never_searched),
    )
    _log_summary(result)
    if args.json:
        print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
