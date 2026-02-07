"""Prototype runner that sequences pipeline steps without touching production code.

This file wires the plan into a clean structure while delegating real work
to placeholder step stubs in ``pipelinev2.steps``.
"""

from __future__ import annotations

import uuid
from datetime import date
from typing import Iterable, List

from loguru import logger

from pipelinev2.state import RunContext, StepResult
from pipelinev2 import steps
from pipelinev2.logging import step_started, step_finished


def _default_run_id() -> str:
    return uuid.uuid4().hex[:8]


def build_context(
    *,
    start_date: date,
    end_date: date,
    start_step: int = 1,
    skip_tax_deeds: bool = False,
    retry_failed: bool = False,
    max_retries: int = 3,
    geocode_missing_parcels: bool = True,
    geocode_limit: int | None = 25,
    auction_limit: int | None = None,
    run_id: str | None = None,
) -> RunContext:
    return RunContext(
        start_date=start_date,
        end_date=end_date,
        start_step=start_step,
        skip_tax_deeds=skip_tax_deeds,
        retry_failed=retry_failed,
        max_retries=max_retries,
        geocode_missing_parcels=geocode_missing_parcels,
        geocode_limit=geocode_limit,
        auction_limit=auction_limit,
        run_id=run_id or _default_run_id(),
    )


def step_sequence(skip_tax_deeds: bool) -> List[steps.StepModule]:
    """Return ordered step modules, respecting skip flags."""
    seq: List[steps.StepModule] = [
        steps.auctions,
        steps.inbox_ingest,
        steps.tax_deeds if not skip_tax_deeds else steps.skip_tax,
        steps.judgment_extract,
        steps.bulk_enrich,
        steps.hcpa_enrich,
        steps.tax,
        steps.market,
        steps.flood,
        steps.ori_iterative,
        steps.survival,
        steps.permits,
        steps.geocode,
        steps.status_refresh,
    ]
    return seq


def run_full_update(context: RunContext) -> List[StepResult]:
    """Execute all steps (no side effects in production paths)."""
    results: List[StepResult] = []

    logger.info(
        "pipeline_run_start",
        run_id=context.run_id,
        start_date=context.start_date,
        end_date=context.end_date,
        start_step=context.start_step,
    )

    for idx, module in enumerate(step_sequence(context.skip_tax_deeds), start=1):
        if idx < context.start_step:
            continue
        step_started(module.STEP_NAME, context.run_id)
        result = module.run(context)
        step_finished(
            module.STEP_NAME,
            {
                "duration_ms": result.duration_ms,
                "processed": result.processed,
                "succeeded": result.succeeded,
                "failed": result.failed,
                "skipped": result.skipped,
            },
            context.run_id,
        )
        results.append(result)

    logger.info("pipeline_run_end", run_id=context.run_id)
    return results


__all__ = ["run_full_update", "build_context"]
