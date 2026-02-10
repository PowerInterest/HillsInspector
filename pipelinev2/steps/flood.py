from __future__ import annotations

import asyncio

from pipelinev2.state import RunContext, StepResult
from pipelinev2.steps.base import timed_step
from pipelinev2.services import fetch_step_cases, get_db, run_with_orchestrator, summarize_step_outcomes

STEP_NAME = "flood"


def run(context: RunContext) -> StepResult:
    with timed_step(STEP_NAME) as elapsed_ms:
        db = get_db(context)
        cases = fetch_step_cases(context, "step_flood_checked")
        if not cases:
            return StepResult(step=STEP_NAME, duration_ms=elapsed_ms(), skipped=1)

        skipped_missing_case = 0
        skipped_missing_parcel = 0
        skipped_missing_address = 0
        skipped_missing_property = 0
        skipped_missing_coords = 0
        skipped_invalid_coords = 0
        attempted = 0

        async def _run(orchestrator):
            nonlocal skipped_missing_case
            nonlocal skipped_missing_parcel
            nonlocal skipped_missing_address
            nonlocal skipped_missing_property
            nonlocal skipped_missing_coords
            nonlocal skipped_invalid_coords
            nonlocal attempted
            for row in cases:
                case_number = row.get("case_number")
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
                prop = db.get_property(parcel_id)
                if not prop:
                    skipped_missing_property += 1
                    db.mark_status_step_complete(case_number, "step_flood_checked", 8)
                    db.mark_step_complete(case_number, "needs_flood_check")
                    continue

                lat = prop.get("latitude") if isinstance(prop, dict) else getattr(prop, "latitude", None)
                lon = prop.get("longitude") if isinstance(prop, dict) else getattr(prop, "longitude", None)
                if lat is None or lon is None:
                    skipped_missing_coords += 1
                    db.mark_status_step_complete(case_number, "step_flood_checked", 8)
                    db.mark_step_complete(case_number, "needs_flood_check")
                    continue
                try:
                    float(lat)
                    float(lon)
                except (TypeError, ValueError):
                    skipped_invalid_coords += 1
                    db.mark_status_step_complete(case_number, "step_flood_checked", 8)
                    db.mark_step_complete(case_number, "needs_flood_check")
                    continue

                attempted += 1
                await orchestrator._run_fema_checker(case_number, parcel_id, address)

        asyncio.run(run_with_orchestrator(context, _run))

        summary = summarize_step_outcomes(
            context,
            [row.get("case_number") for row in cases if row.get("case_number")],
            step_column="step_flood_checked",
            error_step=8,
        )

        skipped_marked_complete = (
            skipped_missing_property + skipped_missing_coords + skipped_invalid_coords
        )
        completed_effective = max(summary["completed"] - skipped_marked_complete, 0)

        return StepResult(
            step=STEP_NAME,
            duration_ms=elapsed_ms(),
            processed=len(cases),
            succeeded=completed_effective,
            failed=summary["failed"],
            skipped=(
                skipped_missing_case
                + skipped_missing_parcel
                + skipped_missing_address
                + skipped_missing_property
                + skipped_missing_coords
                + skipped_invalid_coords
            ),
            artifacts={
                "skipped_missing_case": skipped_missing_case,
                "skipped_missing_parcel": skipped_missing_parcel,
                "skipped_missing_address": skipped_missing_address,
                "skipped_missing_property": skipped_missing_property,
                "skipped_missing_coords": skipped_missing_coords,
                "skipped_invalid_coords": skipped_invalid_coords,
                "attempted": attempted,
                "status_summary": summary,
            },
        )
