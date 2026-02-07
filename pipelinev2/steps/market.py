from __future__ import annotations

import asyncio

from src.models.property import Property

from pipelinev2.state import RunContext, StepResult
from pipelinev2.steps.base import timed_step
from pipelinev2.services import fetch_step_cases, run_with_orchestrator, summarize_multi_step_outcomes

STEP_NAME = "market"


def run(context: RunContext) -> StepResult:
    with timed_step(STEP_NAME) as elapsed_ms:
        market_cases = fetch_step_cases(context, "step_market_fetched")
        home_cases = fetch_step_cases(context, "step_homeharvest_enriched")

        if not market_cases and not home_cases:
            return StepResult(step=STEP_NAME, duration_ms=elapsed_ms(), skipped=1)

        case_map: dict[str, dict] = {}
        for row in market_cases:
            case_number = row.get("case_number")
            if case_number:
                case_map[case_number] = row
        for row in home_cases:
            case_number = row.get("case_number")
            if case_number:
                case_map.setdefault(case_number, row)

        market_missing = {row.get("case_number") for row in market_cases if row.get("case_number")}
        home_missing = {row.get("case_number") for row in home_cases if row.get("case_number")}

        skipped_missing_case = 0
        skipped_missing_parcel = 0
        skipped_missing_address = 0
        attempted = 0

        async def _run(orchestrator):
            nonlocal skipped_missing_case, skipped_missing_parcel, skipped_missing_address, attempted
            for case_number, row in case_map.items():
                parcel_id = row.get("parcel_id")
                address = row.get("address") or row.get("property_address") or "Unknown"
                if not case_number or not parcel_id or address == "Unknown":
                    if not case_number:
                        skipped_missing_case += 1
                    if not parcel_id:
                        skipped_missing_parcel += 1
                    if address == "Unknown":
                        skipped_missing_address += 1
                    continue
                if case_number in market_missing:
                    await orchestrator._run_market_scraper(case_number, parcel_id, address)
                if case_number in home_missing:
                    prop = Property(case_number=case_number, parcel_id=parcel_id, address=address)
                    await orchestrator._run_homeharvest(prop)
                attempted += 1

        asyncio.run(run_with_orchestrator(context, _run))

        processed = len(case_map)
        summary = summarize_multi_step_outcomes(
            context,
            list(case_map.keys()),
            step_columns=["step_market_fetched", "step_homeharvest_enriched"],
            error_steps={"step_market_fetched": 9, "step_homeharvest_enriched": 3},
        )

        return StepResult(
            step=STEP_NAME,
            duration_ms=elapsed_ms(),
            processed=processed,
            succeeded=summary.get("completed_any", 0),
            failed=summary["failed_by_step"].get("step_market_fetched", 0)
            + summary["failed_by_step"].get("step_homeharvest_enriched", 0),
            skipped=skipped_missing_case + skipped_missing_parcel + skipped_missing_address,
            artifacts={
                "skipped_missing_case": skipped_missing_case,
                "skipped_missing_parcel": skipped_missing_parcel,
                "skipped_missing_address": skipped_missing_address,
                "attempted": attempted,
                "status_summary": summary,
            },
        )
