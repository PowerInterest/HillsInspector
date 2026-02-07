from __future__ import annotations

import asyncio

from src.models.property import Property

from pipelinev2.state import RunContext, StepResult
from pipelinev2.steps.base import timed_step
from pipelinev2.services import fetch_step_cases, run_with_orchestrator, summarize_step_outcomes

STEP_NAME = "survival"


def run(context: RunContext) -> StepResult:
    with timed_step(STEP_NAME) as elapsed_ms:
        cases = fetch_step_cases(context, "step_survival_analyzed")
        if not cases:
            return StepResult(step=STEP_NAME, duration_ms=elapsed_ms(), skipped=1)

        skipped_missing_case = 0
        skipped_missing_parcel = 0
        attempted = 0

        async def _run(orchestrator):
            nonlocal skipped_missing_case, skipped_missing_parcel, attempted
            for row in cases:
                case_number = row.get("case_number")
                parcel_id = row.get("parcel_id")
                if not case_number or not parcel_id:
                    if not case_number:
                        skipped_missing_case += 1
                    if not parcel_id:
                        skipped_missing_parcel += 1
                    continue
                address = row.get("address") or row.get("property_address") or "Unknown"
                prop = Property(case_number=case_number, parcel_id=parcel_id, address=address)
                attempted += 1
                await orchestrator._run_survival_analysis(case_number, prop)

        asyncio.run(run_with_orchestrator(context, _run))

        summary = summarize_step_outcomes(
            context,
            [row.get("case_number") for row in cases if row.get("case_number")],
            step_column="step_survival_analyzed",
            error_step=6,
        )

        return StepResult(
            step=STEP_NAME,
            duration_ms=elapsed_ms(),
            processed=len(cases),
            succeeded=summary["completed"],
            failed=summary["failed"],
            skipped=skipped_missing_case + skipped_missing_parcel,
            artifacts={
                "skipped_missing_case": skipped_missing_case,
                "skipped_missing_parcel": skipped_missing_parcel,
                "attempted": attempted,
                "status_summary": summary,
            },
        )
