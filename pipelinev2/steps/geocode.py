from __future__ import annotations

import re

from src.services.geocoder import geocode_address

from pipelinev2.state import RunContext, StepResult
from pipelinev2.steps.base import timed_step
from pipelinev2.services import get_db, get_storage

STEP_NAME = "geocode"


def run(context: RunContext) -> StepResult:
    with timed_step(STEP_NAME) as elapsed_ms:
        db = get_db(context)
        storage = get_storage(context, db=db)
        if not context.geocode_missing_parcels:
            return StepResult(
                step=STEP_NAME,
                duration_ms=elapsed_ms(),
                skipped=1,
                artifacts={"reason": "geocode_missing_parcels disabled"},
            )

        db.ensure_geocode_columns()
        query = """
            WITH normalized AS (
                SELECT
                    p.folio,
                    p.property_address,
                    p.city,
                    p.zip_code,
                    normalize_date(a.auction_date) AS auction_date_norm,
                    p.latitude,
                    p.longitude
                FROM parcels p
                JOIN auctions a
                  ON COALESCE(a.parcel_id, a.folio) = p.folio
            )
            SELECT DISTINCT
                folio,
                property_address,
                city,
                zip_code
            FROM normalized
            WHERE (latitude IS NULL OR longitude IS NULL)
              AND property_address IS NOT NULL
              AND property_address != ''
              AND LOWER(property_address) NOT IN ('unknown', 'n/a', 'none')
              AND auction_date_norm >= ?
              AND auction_date_norm <= ?
        """
        params: list[object] = [context.start_date, context.end_date]
        if context.geocode_limit is not None:
            query += " LIMIT ?"
            params.append(context.geocode_limit)

        try:
            rows = db.execute_query(query, tuple(params))
        except Exception as exc:  # noqa: BLE001
            rows = []
            return StepResult(step=STEP_NAME, duration_ms=elapsed_ms(), failed=1, artifacts={"error": str(exc)})

        updated = 0
        for row in rows:
            folio = row.get("folio")
            address = (row.get("property_address") or "").strip()
            if not folio or not address:
                continue

            if re.search(r",\s*FL[\s\-]", address, re.IGNORECASE):
                full_address = re.sub(r"FL-\s*", "FL ", address)
            else:
                city = (row.get("city") or "Tampa").strip()
                zip_code = (row.get("zip_code") or "").strip()
                full_address = f"{address}, {city}, FL {zip_code}".strip()

            coords = geocode_address(full_address)
            if not coords:
                storage.record_scrape(
                    property_id=str(folio),
                    scraper="geocode",
                    success=False,
                    error="No geocode result",
                    vision_data={"address": full_address},
                    prompt_version="v1",
                )
                continue

            lat, lon = coords
            db.update_parcel_coordinates(str(folio), lat, lon)
            updated += 1
            storage.record_scrape(
                property_id=str(folio),
                scraper="geocode",
                success=True,
                vision_data={"address": full_address, "latitude": lat, "longitude": lon},
                prompt_version="v1",
            )

        db.checkpoint()

        return StepResult(
            step=STEP_NAME,
            duration_ms=elapsed_ms(),
            processed=len(rows),
            succeeded=updated,
            failed=max(len(rows) - updated, 0),
            artifacts={"total_rows": len(rows), "updated": updated},
        )
