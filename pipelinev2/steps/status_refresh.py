from __future__ import annotations

from pipelinev2.state import RunContext, StepResult
from pipelinev2.steps.base import timed_step
from pipelinev2.services import get_db

STEP_NAME = "status_refresh"


def run(context: RunContext) -> StepResult:
    with timed_step(STEP_NAME) as elapsed_ms:
        db = get_db(context)
        db.ensure_status_table()
        db.initialize_status_from_auctions()
        db.backfill_status_steps(context.start_date, context.end_date)
        db.refresh_status_completion_for_range(context.start_date, context.end_date)

        summary = db.get_status_summary(context.start_date, context.end_date)
        by_status = summary.get("by_status", {})

        return StepResult(
            step=STEP_NAME,
            duration_ms=elapsed_ms(),
            processed=summary.get("total", 0),
            succeeded=by_status.get("completed", 0),
            failed=by_status.get("failed", 0),
            skipped=by_status.get("skipped", 0),
            artifacts={"status_summary": summary},
        )
