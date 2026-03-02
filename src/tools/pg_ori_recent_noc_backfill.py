"""Backfill live NOCs for recent permit-backed active foreclosures.

Architectural purpose:
- Provide a repeatable maintenance CLI for the narrow backlog where a property
  has recent permit activity but still has no persisted Notice of Commencement
  in `ori_encumbrances`.
- Reuse `PgOriService`'s guarded live NOC fallback instead of ad hoc notebooks
  or shell snippets.

How it fits in the broader system:
- This is a targeted ORI maintenance tool, not the main ORI ingestion step.
- It complements `PgOriService.run()` by re-probing already-searched active
  foreclosures when local seed data and normal discovery missed a NOC.
- The command is intentionally bounded and summary-oriented so it can be run
  operationally and audited after each pass.
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
            "Backfill live PAV Notices of Commencement for active foreclosures "
            "that have recent permit signal but no persisted NOC."
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
        help="Probe live PAV and report matches without saving to PostgreSQL",
    )
    parser.add_argument(
        "--include-never-searched",
        action="store_true",
        help="Include active foreclosures where step_ori_searched is still NULL",
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
        "Recent permit NOC backfill: targets={} live_hits={} docs_found={} saved={} "
        "errors={} remaining_before={} remaining_after={} dry_run={}",
        result.get("targets"),
        result.get("targets_with_live_noc"),
        result.get("total_noc_docs_found"),
        result.get("total_saved"),
        result.get("errors"),
        result.get("remaining_recent_permit_no_noc_before"),
        result.get("remaining_recent_permit_no_noc_after"),
        result.get("dry_run"),
    )


def main() -> None:
    args = _build_parser().parse_args()
    service = PgOriService(dsn=args.dsn)
    result = service.run_recent_permit_noc_backfill(
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
