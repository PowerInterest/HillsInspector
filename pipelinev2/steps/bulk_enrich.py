from __future__ import annotations

import json
from loguru import logger

from src.ingest.bulk_parcel_ingest import enrich_auctions_from_bulk

from pipelinev2.state import RunContext, StepResult
from pipelinev2.steps.base import timed_step
from pipelinev2.services import get_db

STEP_NAME = "bulk_enrich"


def run(context: RunContext) -> StepResult:
    with timed_step(STEP_NAME) as elapsed_ms:
        db = get_db(context)
        enrichment_stats = {}
        auctions_in_range = []
        try:
            enrichment_stats = enrich_auctions_from_bulk(conn=db.conn)
            auctions_in_range = db.get_auctions_by_date_range(context.start_date, context.end_date)
            for auction in auctions_in_range:
                db.mark_status_step_complete(
                    auction["case_number"],
                    "step_bulk_enriched",
                    3,
                )
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Bulk enrichment failed: {exc}")

        try:
            auctions_with_judgment = db.execute_query(
                """
                SELECT parcel_id, extracted_judgment_data FROM auctions
                WHERE parcel_id IS NOT NULL AND extracted_judgment_data IS NOT NULL
                """
            )
            for row in auctions_with_judgment:
                folio = row["parcel_id"]
                try:
                    judgment_data = json.loads(row["extracted_judgment_data"])
                    legal_desc = judgment_data.get("legal_description")
                    if legal_desc:
                        conn = db.connect()
                        conn.execute(
                            "ALTER TABLE parcels ADD COLUMN IF NOT EXISTS judgment_legal_description VARCHAR"
                        )
                        conn.execute(
                            "INSERT OR IGNORE INTO parcels (folio) VALUES (?)",
                            [folio],
                        )
                        conn.execute(
                            """
                            UPDATE parcels SET
                                judgment_legal_description = ?,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE folio = ?
                            """,
                            [legal_desc, folio],
                        )
                except Exception as exc:  # noqa: BLE001
                    logger.debug(f"Could not update judgment legal for {folio}: {exc}")
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"Could not update judgment legal descriptions: {exc}")

        db.checkpoint()

        return StepResult(
            step=STEP_NAME,
            duration_ms=elapsed_ms(),
            processed=len(auctions_in_range),
            succeeded=enrichment_stats.get("auctions_updated", len(auctions_in_range)) if isinstance(enrichment_stats, dict) else len(auctions_in_range),
            artifacts={
                "enrichment_stats": enrichment_stats,
                "auctions_in_range": len(auctions_in_range),
            },
        )
