"""Service helpers for pipeline v2 prototype (isolated from production code changes)."""

from __future__ import annotations

from typing import Any, Dict, List

from src.db.operations import PropertyDB
from src.db.writer import DatabaseWriter
from src.services.scraper_storage import ScraperStorage
from src.orchestrator import PipelineOrchestrator

from pipelinev2.state import RunContext


VALID_STEP_COLUMNS = {
    "step_auction_scraped",
    "step_pdf_downloaded",
    "step_judgment_extracted",
    "step_bulk_enriched",
    "step_homeharvest_enriched",
    "step_hcpa_enriched",
    "step_ori_ingested",
    "step_survival_analyzed",
    "step_permits_checked",
    "step_flood_checked",
    "step_market_fetched",
    "step_tax_checked",
}


def _chunked(values: list[str], size: int = 900):
    for i in range(0, len(values), size):
        yield values[i : i + size]


def get_db(context: RunContext) -> PropertyDB:
    db = context.services.get("db")
    if db is None:
        db = PropertyDB()
        context.services["db"] = db
    return db


def get_storage(context: RunContext, db: PropertyDB | None = None) -> ScraperStorage:
    storage = context.services.get("storage")
    if storage is None:
        db = db or get_db(context)
        storage = ScraperStorage(db_path=db.db_path, db=db)
        context.services["storage"] = storage
    return storage


def fetch_step_cases(context: RunContext, step_column: str) -> List[Dict[str, Any]]:
    if step_column not in VALID_STEP_COLUMNS:
        raise ValueError(f"Invalid step column: {step_column}")

    db = get_db(context)
    conn = db.connect()
    auction_type_filter = ""
    params: list[Any] = [context.start_date, context.end_date, context.retry_failed, context.max_retries]
    if context.skip_tax_deeds:
        auction_type_filter = (
            "AND COALESCE(UPPER(REPLACE(n.auction_type, ' ', '_')), '') != 'TAX_DEED'"
        )

    query = f"""
        WITH normalized AS (
            SELECT
                a.*,
                COALESCE(a.parcel_id, a.folio) AS parcel_id_norm,
                COALESCE(a.property_address, p.property_address) AS address,
                p.owner_name AS owner_name,
                p.legal_description AS legal_description,
                p.property_address AS property_address,
                normalize_date(a.auction_date) AS auction_date_norm
            FROM auctions a
            LEFT JOIN parcels p
                ON p.folio = COALESCE(a.parcel_id, a.folio)
        )
        SELECT
            n.case_number,
            n.parcel_id_norm AS parcel_id,
            n.address,
            n.owner_name,
            n.legal_description,
            n.plaintiff,
            n.defendant,
            n.auction_type,
            n.property_address
        FROM normalized n
        LEFT JOIN status s ON s.case_number = n.case_number
        WHERE n.auction_date_norm BETWEEN ? AND ?
          AND COALESCE(s.pipeline_status, 'pending') NOT IN ('completed', 'skipped')
          AND (COALESCE(s.pipeline_status, 'pending') != 'failed' OR ?)
          AND COALESCE(s.retry_count, 0) < ?
          {auction_type_filter}
          AND s.{step_column} IS NULL
        ORDER BY n.auction_date_norm, n.case_number
    """
    rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def fetch_status_rows(
    context: RunContext,
    case_numbers: list[str],
    step_columns: list[str] | None = None,
) -> list[dict]:
    if not case_numbers:
        return []
    step_columns = step_columns or []
    for col in step_columns:
        if col not in VALID_STEP_COLUMNS:
            raise ValueError(f"Invalid step column: {col}")
    db = get_db(context)
    db.ensure_status_table()
    rows: list[dict] = []
    base_cols = ["case_number", "pipeline_status", "last_error", "error_step"]
    select_cols = base_cols + step_columns
    select_clause = ", ".join(select_cols)
    for chunk in _chunked(case_numbers):
        placeholders = ",".join(["?"] * len(chunk))
        query = f"SELECT {select_clause} FROM status WHERE case_number IN ({placeholders})"
        chunk_rows = db.connect().execute(query, chunk).fetchall()
        rows.extend([dict(r) for r in chunk_rows])
    return rows


def summarize_step_outcomes(
    context: RunContext,
    case_numbers: list[str],
    step_column: str,
    error_step: int | None,
) -> dict:
    rows = fetch_status_rows(context, case_numbers, step_columns=[step_column])
    completed = 0
    failed = 0
    skipped_status = 0
    pending = 0
    error_reasons: dict[str, int] = {}
    for row in rows:
        step_done = row.get(step_column)
        if step_done is not None:
            completed += 1
        status = row.get("pipeline_status")
        if status == "failed" and (error_step is None or row.get("error_step") == error_step):
            failed += 1
            reason = (row.get("last_error") or "Unknown error").strip()
            error_reasons[reason] = error_reasons.get(reason, 0) + 1
        elif status == "skipped":
            skipped_status += 1
    pending = max(len(rows) - completed - failed - skipped_status, 0)
    return {
        "total": len(rows),
        "completed": completed,
        "failed": failed,
        "skipped_status": skipped_status,
        "pending": pending,
        "error_reasons": error_reasons,
    }


def summarize_multi_step_outcomes(
    context: RunContext,
    case_numbers: list[str],
    step_columns: list[str],
    error_steps: dict[str, int] | None = None,
) -> dict:
    rows = fetch_status_rows(context, case_numbers, step_columns=step_columns)
    error_steps = error_steps or {}
    completed_by_step = {col: 0 for col in step_columns}
    failed_by_step: dict[str, int] = {col: 0 for col in step_columns}
    error_reasons: dict[str, dict[str, int]] = {col: {} for col in step_columns}
    skipped_status = 0
    pending = 0
    completed_any = 0

    for row in rows:
        status = row.get("pipeline_status")
        if status == "skipped":
            skipped_status += 1
        for col in step_columns:
            if row.get(col) is not None:
                completed_by_step[col] += 1
            step_num = error_steps.get(col)
            if status == "failed" and (step_num is None or row.get("error_step") == step_num):
                failed_by_step[col] += 1
                reason = (row.get("last_error") or "Unknown error").strip()
                bucket = error_reasons[col]
                bucket[reason] = bucket.get(reason, 0) + 1

    # pending is coarse here: not completed in any step and not failed/skipped
    total = len(rows)
    any_completed = set()
    for row in rows:
        if any(row.get(col) is not None for col in step_columns):
            any_completed.add(row.get("case_number"))
    completed_any = len(any_completed)
    pending = max(total - completed_any - skipped_status, 0)

    return {
        "total": total,
        "completed_by_step": completed_by_step,
        "completed_any": completed_any,
        "failed_by_step": failed_by_step,
        "skipped_status": skipped_status,
        "pending": pending,
        "error_reasons": error_reasons,
    }


async def run_with_orchestrator(context: RunContext, coro_fn):
    db = get_db(context)
    storage = get_storage(context, db=db)
    writer = DatabaseWriter(db=db)
    orchestrator = PipelineOrchestrator(db_writer=writer, db=db, storage=storage)

    await writer.start()
    try:
        result = await coro_fn(orchestrator)
    finally:
        try:
            await writer.stop()
        finally:
            await orchestrator.ingestion_service.shutdown()
            orchestrator._close_v2_conn()
    return result
