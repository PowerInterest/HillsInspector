from __future__ import annotations

from pipelinev2.state import RunContext, StepResult
from pipelinev2.steps.base import timed_step

STEP_NAME = "tax_deeds_skipped"


def run(context: RunContext) -> StepResult:
    # Step intentionally skipped when skip_tax_deeds is True.
    with timed_step(STEP_NAME) as elapsed_ms:
        return StepResult(step=STEP_NAME, duration_ms=elapsed_ms(), skipped=1)
