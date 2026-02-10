from __future__ import annotations

import asyncio
import json

from src.models.property import Property
from src.utils.legal_description import (
    build_ori_search_terms,
    combine_legal_fields,
    parse_legal_description,
)

from pipelinev2.state import RunContext, StepResult
from pipelinev2.steps.base import timed_step
from pipelinev2.services import fetch_step_cases, run_with_orchestrator, summarize_step_outcomes

STEP_NAME = "ori_iterative"


def run(context: RunContext) -> StepResult:
    with timed_step(STEP_NAME) as elapsed_ms:
        cases = fetch_step_cases(context, "step_ori_ingested")
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
                db = orchestrator.db

                auction = db.get_auction_by_case(case_number) or {}
                raw_judgment = auction.get("extracted_judgment_data")
                judgment_payload = {}
                if isinstance(raw_judgment, dict):
                    judgment_payload = raw_judgment
                elif isinstance(raw_judgment, str) and raw_judgment.strip():
                    try:
                        judgment_payload = json.loads(raw_judgment)
                    except json.JSONDecodeError:
                        judgment_payload = {}

                judgment_legal = None
                if isinstance(judgment_payload, dict):
                    judgment_legal = judgment_payload.get("legal_description") or None

                bulk_row = db.connect().execute(
                    """
                    SELECT raw_legal1, raw_legal2, raw_legal3, raw_legal4
                    FROM bulk_parcels
                    WHERE folio = ? OR strap = ?
                    LIMIT 1
                    """,
                    [parcel_id, parcel_id],
                ).fetchone()
                bulk = dict(bulk_row) if bulk_row else {}
                bulk_legal = combine_legal_fields(
                    bulk.get("raw_legal1") or "",
                    bulk.get("raw_legal2"),
                    bulk.get("raw_legal3"),
                    bulk.get("raw_legal4"),
                )

                primary_legal = judgment_legal or bulk_legal or row.get("legal_description")
                terms = build_ori_search_terms(
                    parcel_id,
                    bulk.get("raw_legal1"),
                    bulk.get("raw_legal2"),
                    bulk.get("raw_legal3"),
                    bulk.get("raw_legal4"),
                    judgment_legal=judgment_legal,
                )
                if not terms and primary_legal:
                    prefix = primary_legal.strip().upper()[:60]
                    if prefix:
                        terms = [f"{prefix}*"]

                filter_info = None
                if primary_legal:
                    parsed = parse_legal_description(primary_legal)
                    lot_filter = parsed.lots or ([parsed.lot] if parsed.lot else None)
                    if lot_filter or parsed.block:
                        filter_info = {
                            "lot": lot_filter,
                            "block": parsed.block,
                            "subdivision": parsed.subdivision,
                            "require_all_lots": isinstance(lot_filter, list)
                            and len(lot_filter) > 1,
                        }
                if filter_info:
                    terms = list(terms)
                    terms.append(("__filter__", filter_info))

                prop = Property(
                    case_number=case_number,
                    parcel_id=parcel_id,
                    address=address,
                    owner_name=row.get("owner_name"),
                    legal_description=primary_legal or row.get("legal_description"),
                    plaintiff=row.get("plaintiff"),
                    defendant=row.get("defendant"),
                )
                prop.legal_search_terms = terms
                if judgment_payload:
                    prop.judgment_payload = judgment_payload
                attempted += 1
                try:
                    await orchestrator.ingestion_service.ingest_property_async(prop)
                    if orchestrator.db.folio_has_chain_of_title(parcel_id):
                        await orchestrator.db_writer.enqueue(
                            "generic_call",
                            {
                                "func": orchestrator.db.mark_status_step_complete,
                                "args": [case_number, "step_ori_ingested", 5],
                            },
                        )
                    else:
                        await orchestrator.db_writer.enqueue(
                            "generic_call",
                            {
                                "func": orchestrator.db.mark_status_failed,
                                "args": [
                                    case_number,
                                    "ORI ingestion produced no chain of title",
                                    5,
                                ],
                            },
                        )
                except Exception as exc:  # noqa: BLE001
                    await orchestrator.db_writer.enqueue(
                        "generic_call",
                        {
                            "func": orchestrator.db.mark_status_failed,
                            "args": [case_number, str(exc)[:200], 5],
                        },
                    )

        asyncio.run(run_with_orchestrator(context, _run))

        summary = summarize_step_outcomes(
            context,
            [row.get("case_number") for row in cases if row.get("case_number")],
            step_column="step_ori_ingested",
            error_step=5,
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
