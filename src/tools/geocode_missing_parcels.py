"""
Backfill parcels missing latitude/longitude from local bulk_parcels data.
"""
from __future__ import annotations

import argparse
from loguru import logger

from src.db.operations import PropertyDB


def geocode_missing(limit: int | None = None):
    """Backfill missing coordinates from bulk_parcels (no external geocoder)."""
    db = PropertyDB()
    db.ensure_geocode_columns()
    conn = db.connect()
    query = """
        SELECT
            p.folio,
            p.parcel_id,
            COALESCE(bp_strap.latitude, bp_folio.latitude) AS latitude,
            COALESCE(bp_strap.longitude, bp_folio.longitude) AS longitude,
            COALESCE(bp_strap.property_address, bp_folio.property_address, p.property_address) AS source_address
        FROM parcels p
        LEFT JOIN bulk_parcels bp_strap ON bp_strap.strap = p.folio
        LEFT JOIN bulk_parcels bp_folio ON bp_folio.folio = p.folio
        WHERE p.folio IS NOT NULL
          AND (p.latitude IS NULL OR p.longitude IS NULL)
    """
    params = []
    if limit:
        query += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(query, params).fetchall()
    logger.info(f"Found {len(rows)} parcels needing coordinate backfill")

    updated = 0
    unresolved = 0
    for folio, parcel_id, lat, lon, source_address in rows:
        if lat is None or lon is None:
            unresolved += 1
            logger.warning(
                f"No bulk_parcels coordinates for folio={folio} address={source_address!r}"
            )
            continue
        try:
            lat_val = float(lat)
            lon_val = float(lon)
            db.update_parcel_coordinates(parcel_id or folio, lat_val, lon_val)
            updated += 1
            logger.info(f"Backfilled folio={folio} -> ({lat_val}, {lon_val})")
        except Exception:
            unresolved += 1
            logger.exception(
                f"Failed coordinate update for folio={folio} parcel_id={parcel_id}"
            )

    logger.success(
        f"Coordinate backfill complete: updated={updated}, unresolved={unresolved}, total={len(rows)}"
    )


def main():
    parser = argparse.ArgumentParser(description="Geocode missing parcel coordinates")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of parcels to geocode")
    args = parser.parse_args()
    geocode_missing(args.limit)


if __name__ == "__main__":
    main()
