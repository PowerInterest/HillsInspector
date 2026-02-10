from __future__ import annotations

import json
from contextlib import suppress
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
            # Close any active connection to avoid SQLite locking during bulk ingest.
            try:
                conn = db.conn
                with suppress(Exception):
                    conn.commit()
                with suppress(Exception):
                    conn.close()
                db.conn = None
            except Exception:
                pass

            enrichment_stats = enrich_auctions_from_bulk(db_path=db.db_path, conn=None)
            db.connect()
            auctions_in_range = db.execute_query(
                """
                SELECT
                    a.case_number,
                    COALESCE(a.parcel_id, a.folio) AS parcel_id,
                    COALESCE(a.property_address, p.property_address) AS address,
                    normalize_date(a.auction_date) AS auction_date_norm
                FROM auctions a
                LEFT JOIN parcels p
                    ON p.folio = COALESCE(a.parcel_id, a.folio)
                WHERE normalize_date(a.auction_date) BETWEEN ? AND ?
                """,
                (context.start_date, context.end_date),
            )
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
