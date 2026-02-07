from __future__ import annotations

from pathlib import Path

from src.ingest.inbox_scanner import InboxScanner

from pipelinev2.state import RunContext, StepResult
from pipelinev2.steps.base import timed_step
from pipelinev2.services import get_db

STEP_NAME = "inbox_ingest"


def run(context: RunContext) -> StepResult:
    with timed_step(STEP_NAME) as elapsed_ms:
        db = get_db(context)
        base_dir = Path("data/Foreclosure")
        initial_files = list(base_dir.glob("*/auction.parquet"))
        initial_count = len(initial_files)

        if initial_count == 0:
            return StepResult(
                step=STEP_NAME,
                duration_ms=elapsed_ms(),
                processed=0,
                skipped=1,
                artifacts={"initial_files": 0, "remaining_files": 0},
            )

        InboxScanner(db=db).scan_and_ingest()

        remaining_files = list(base_dir.glob("*/auction.parquet"))
        remaining_count = len(remaining_files)
        succeeded = max(initial_count - remaining_count, 0)
        failed = remaining_count

        return StepResult(
            step=STEP_NAME,
            duration_ms=elapsed_ms(),
            processed=initial_count,
            succeeded=succeeded,
            failed=failed,
            artifacts={
                "initial_files": initial_count,
                "remaining_files": remaining_count,
                "files_ingested": succeeded,
            },
        )
