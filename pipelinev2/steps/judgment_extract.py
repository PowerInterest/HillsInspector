from __future__ import annotations

import json
import time
from pathlib import Path

import polars as pl
from loguru import logger

from src.services.final_judgment_processor import FinalJudgmentProcessor

from pipelinev2.state import RunContext, StepResult
from pipelinev2.steps.base import timed_step
from pipelinev2.services import get_db

STEP_NAME = "judgment_extract"


def run(context: RunContext) -> StepResult:
    with timed_step(STEP_NAME) as elapsed_ms:
        db = get_db(context)
        judgment_processor = FinalJudgmentProcessor()

        params: list[object] = [context.retry_failed, context.max_retries]
        auctions = db.execute_query(
            """
            SELECT a.* FROM auctions a
            LEFT JOIN status s ON s.case_number = a.case_number
            WHERE a.parcel_id IS NOT NULL
              AND a.extracted_judgment_data IS NULL
              AND COALESCE(s.pipeline_status, 'pending') != 'skipped'
              AND s.step_judgment_extracted IS NULL
              AND s.step_pdf_downloaded IS NOT NULL
              AND (COALESCE(s.pipeline_status, 'pending') != 'failed' OR ?)
              AND COALESCE(s.retry_count, 0) < ?
            """,
            tuple(params),
        )

        extracted_count = 0
        skipped_checkpoint = 0
        missing_pdf = 0
        no_structured = 0
        processing_failed = 0
        processed_since_checkpoint = 0
        judgment_rows: list[dict] = []
        judgment_dir = Path("data/judgments")
        judgment_dir.mkdir(parents=True, exist_ok=True)
        checkpoint_path = judgment_dir / "judgment_extracts_checkpoint.parquet"
        final_path = judgment_dir / "judgment_extracts_final.parquet"
        last_flush = time.monotonic()
        processed_case_numbers: set[str] = set()

        def _atomic_write_parquet(df: pl.DataFrame, target_path: Path) -> None:
            tmp_path = target_path.with_suffix(target_path.suffix + ".tmp")
            df.write_parquet(tmp_path)
            tmp_path.replace(target_path)

        def _flush_checkpoint() -> None:
            if not judgment_rows:
                return
            df = pl.DataFrame(judgment_rows)
            _atomic_write_parquet(df, checkpoint_path)
            logger.info(f"Wrote judgment checkpoint: {checkpoint_path}")

        if checkpoint_path.exists():
            try:
                existing_df = pl.read_parquet(checkpoint_path)
                if "case_number" in existing_df.columns:
                    processed_case_numbers = set(
                        existing_df["case_number"].drop_nulls().unique().to_list()
                    )
                    judgment_rows = existing_df.to_dicts()
                    logger.info(
                        f"Loaded judgment checkpoint with {len(judgment_rows)} rows; "
                        f"skipping {len(processed_case_numbers)} case_numbers"
                    )
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"Failed to load judgment checkpoint: {exc}")

        for auction in auctions:
            case_number = auction["case_number"]
            parcel_id = (auction.get("parcel_id") or "").strip()

            if case_number in processed_case_numbers:
                skipped_checkpoint += 1
                continue

            sanitized_folio = parcel_id.replace("/", "_").replace("\\", "_").replace(":", "_")
            base_dir = Path("data/properties") / sanitized_folio / "documents"
            pdf_paths = list(base_dir.glob("final_judgment*.pdf")) if base_dir.exists() else []

            if not pdf_paths:
                legacy_path = Path(f"data/pdfs/final_judgments/{case_number}_final_judgment.pdf")
                if legacy_path.exists():
                    pdf_paths = [legacy_path]

            if not pdf_paths:
                db.mark_status_failed(
                    case_number,
                    "Final judgment PDF not found on disk",
                    error_step=2,
                )
                missing_pdf += 1
                continue

            pdf_path = pdf_paths[0]
            logger.info(f"Processing judgment from {pdf_path.name}...")
            try:
                result = judgment_processor.process_pdf(str(pdf_path), case_number)
                if result:
                    amounts = judgment_processor.extract_key_amounts(result)
                    payload = {
                        **result,
                        **amounts,
                        "extracted_judgment_data": json.dumps(result),
                        "raw_judgment_text": result.get("raw_text", ""),
                    }
                    judgment_rows.append(
                        {
                            "case_number": case_number,
                            "parcel_id": parcel_id,
                            **payload,
                        }
                    )
                    processed_case_numbers.add(case_number)
                    extracted_count += 1
                else:
                    logger.warning(f"No structured data extracted for {case_number}")
                    db.mark_status_failed(
                        case_number,
                        "Vision service returned no structured data",
                        error_step=2,
                    )
                    no_structured += 1
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"Failed to process judgment for {case_number}: {exc}")
                db.mark_status_failed(case_number, str(exc)[:200], error_step=2)
                processing_failed += 1

            processed_since_checkpoint += 1
            if processed_since_checkpoint >= 10:
                db.checkpoint()
                processed_since_checkpoint = 0
            if time.monotonic() - last_flush >= 600:
                try:
                    _flush_checkpoint()
                except Exception as exc:  # noqa: BLE001
                    logger.error(f"Judgment checkpoint write failed: {exc}")
                    raise
                last_flush = time.monotonic()

        if judgment_rows:
            final_df = pl.DataFrame(judgment_rows)
            _atomic_write_parquet(final_df, final_path)
            logger.info(f"Wrote final judgment parquet: {final_path}")
            for row in final_df.iter_rows(named=True):
                case_number = row["case_number"]
                payload = {
                    "plaintiff": row.get("plaintiff"),
                    "defendant": row.get("defendant"),
                    "foreclosure_type": row.get("foreclosure_type"),
                    "judgment_date": row.get("judgment_date"),
                    "lis_pendens_date": row.get("lis_pendens_date"),
                    "foreclosure_sale_date": row.get("foreclosure_sale_date"),
                    "total_judgment_amount": row.get("total_judgment_amount"),
                    "principal_amount": row.get("principal_amount"),
                    "interest_amount": row.get("interest_amount"),
                    "attorney_fees": row.get("attorney_fees"),
                    "court_costs": row.get("court_costs"),
                    "original_mortgage_amount": row.get("original_mortgage_amount"),
                    "original_mortgage_date": row.get("original_mortgage_date"),
                    "monthly_payment": row.get("monthly_payment"),
                    "default_date": row.get("default_date"),
                    "extracted_judgment_data": row.get("extracted_judgment_data"),
                    "raw_judgment_text": row.get("raw_judgment_text"),
                }
                db.update_judgment_data(case_number, payload)
                db.mark_step_complete(case_number, "needs_judgment_extraction")
                db.mark_status_step_complete(case_number, "step_judgment_extracted", 2)

        db.checkpoint()

        total_failed = missing_pdf + no_structured + processing_failed
        return StepResult(
            step=STEP_NAME,
            duration_ms=elapsed_ms(),
            processed=len(auctions),
            succeeded=extracted_count,
            failed=total_failed,
            skipped=skipped_checkpoint,
            artifacts={
                "extracted_count": extracted_count,
                "missing_pdf": missing_pdf,
                "no_structured": no_structured,
                "processing_failed": processing_failed,
                "skipped_checkpoint": skipped_checkpoint,
                "checkpoint_path": str(checkpoint_path),
                "final_path": str(final_path),
            },
        )
