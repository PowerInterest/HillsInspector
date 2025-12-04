"""
Geocode parcels missing latitude/longitude and store results in DuckDB.
Uses Nominatim (OpenStreetMap) with simple on-disk caching.
"""
from __future__ import annotations

import argparse
from loguru import logger

from src.db.operations import PropertyDB
from src.services.geocoder import geocode_address


def geocode_missing(limit: int | None = None):
    db = PropertyDB()
    db.ensure_geocode_columns()
    conn = db.connect()
    query = """
        SELECT folio, parcel_id, property_address
        FROM parcels
        WHERE (latitude IS NULL OR longitude IS NULL)
          AND property_address IS NOT NULL
    """
    params = []
    if limit:
        query += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(query, params).fetchall()
    logger.info(f"Found {len(rows)} parcels needing geocode")

    updated = 0
    for folio, parcel_id, address in rows:
        coords = geocode_address(address)
        if coords:
            lat, lon = coords
            db.update_parcel_coordinates(parcel_id or folio, lat, lon)
            updated += 1
            logger.info(f"Geocoded {address} -> ({lat}, {lon})")

    logger.success(f"Updated {updated} parcels with coordinates")


def main():
    parser = argparse.ArgumentParser(description="Geocode missing parcel coordinates")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of parcels to geocode")
    args = parser.parse_args()
    geocode_missing(args.limit)


if __name__ == "__main__":
    main()
