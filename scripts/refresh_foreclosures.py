"""
Idempotent refresh: INSERT ... ON CONFLICT DO UPDATE into foreclosures.

Sources:
  - foreclosures_history   (seed rows)
  - hcpa_bulk_parcels      (property enrichment)
  - hcpa_latlon            (coordinates)
  - clerk_civil_cases      (case metadata)
  - clerk_civil_events     (docket timeline → foreclosure_events)
  - dor_nal_parcels        (tax / homestead)
  - property_market        (Zillow / listing)
  - hcpa_allsales          (resale analytics)
  - sunbiz_flr_*           (UCC exposure)

Run:  uv run python scripts/refresh_foreclosures.py
      uv run python scripts/refresh_foreclosures.py --migrate   # create tables first
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from loguru import logger
from sqlalchemy import text

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from sunbiz.db import get_engine, resolve_pg_dsn

FORECLOSURE_DATA_DIR = Path("data/Foreclosure")

# ---------------------------------------------------------------------------
# Step 1 — Seed / update from foreclosures_history + enrichment joins
# ---------------------------------------------------------------------------

UPSERT_SQL = """
INSERT INTO foreclosures (
    listing_id, case_number_raw, auction_date, auction_status,
    folio, strap, property_address, latitude, longitude,
    winning_bid, final_judgment_amount, appraised_value, sold_to, buyer_type,
    owner_name, land_use, year_built, beds, baths, heated_area,
    market_value, assessed_value,
    clerk_case_type, clerk_case_status, filing_date, judgment_date, is_foreclosure,
    homestead_exempt, estimated_annual_tax,
    zestimate, list_price, listing_status
)
SELECT
    hs.listing_id::TEXT,
    hs.case_number_raw,
    hs.auction_date,
    hs.auction_status,
    COALESCE(hs.folio, bp.folio),
    COALESCE(hs.strap, bp.strap),
    COALESCE(bp.property_address, hs.property_address),
    COALESCE(hs.latitude, bp.latitude, ll.latitude),
    COALESCE(hs.longitude, bp.longitude, ll.longitude),
    hs.winning_bid,
    hs.final_judgment_amount,
    hs.appraised_value,
    hs.sold_to,
    hs.buyer_type,
    bp.owner_name,
    bp.land_use_desc,
    COALESCE(bp.year_built, hs.year_built),
    COALESCE(bp.beds, hs.beds),
    COALESCE(bp.baths, hs.baths),
    bp.heated_area,
    bp.market_value,
    bp.assessed_value,
    cc.case_type,
    cc.case_status,
    cc.filing_date,
    cc.judgment_date,
    cc.is_foreclosure,
    dn.homestead_exempt,
    dn.estimated_annual_tax,
    pm.zestimate,
    pm.list_price,
    pm.listing_status
FROM (
    SELECT DISTINCT ON (case_number_raw, auction_date) *
    FROM foreclosures_history
    WHERE case_number_raw IS NOT NULL
      AND auction_date IS NOT NULL
    ORDER BY case_number_raw, auction_date, updated_at DESC NULLS LAST, foreclosure_id DESC
) hs
-- Property enrichment (latest bulk parcel row per strap)
LEFT JOIN LATERAL (
    SELECT bp2.folio, bp2.strap, bp2.property_address, bp2.owner_name,
           bp2.land_use_desc, bp2.year_built, bp2.beds, bp2.baths,
           bp2.heated_area, bp2.market_value, bp2.assessed_value,
           bp2.latitude, bp2.longitude
    FROM hcpa_bulk_parcels bp2
    WHERE (hs.strap IS NOT NULL AND bp2.strap = hs.strap)
       OR (hs.strap IS NULL AND hs.folio IS NOT NULL AND bp2.folio = hs.folio)
    ORDER BY bp2.source_file_id DESC
    LIMIT 1
) bp ON TRUE
-- Coordinates fallback
LEFT JOIN hcpa_latlon ll ON COALESCE(hs.folio, bp.folio) = ll.folio
-- Clerk case metadata (join on normalized case number)
LEFT JOIN clerk_civil_cases cc
    ON normalize_case_number_fn(hs.case_number_raw) = cc.case_number
-- Tax data (latest tax year per strap)
LEFT JOIN LATERAL (
    SELECT dn2.homestead_exempt, dn2.estimated_annual_tax
    FROM dor_nal_parcels dn2
    WHERE dn2.strap = COALESCE(hs.strap, bp.strap)
    ORDER BY dn2.tax_year DESC
    LIMIT 1
) dn ON COALESCE(hs.strap, bp.strap) IS NOT NULL
-- Market snapshot
LEFT JOIN property_market pm
    ON COALESCE(hs.strap, bp.strap) = pm.strap

ON CONFLICT (case_number_raw, auction_date) DO UPDATE SET
    auction_status        = EXCLUDED.auction_status,
    listing_id            = COALESCE(EXCLUDED.listing_id, foreclosures.listing_id),
    folio                 = COALESCE(EXCLUDED.folio, foreclosures.folio),
    strap                 = COALESCE(EXCLUDED.strap, foreclosures.strap),
    property_address      = COALESCE(EXCLUDED.property_address, foreclosures.property_address),
    latitude              = COALESCE(EXCLUDED.latitude, foreclosures.latitude),
    longitude             = COALESCE(EXCLUDED.longitude, foreclosures.longitude),
    winning_bid           = COALESCE(EXCLUDED.winning_bid, foreclosures.winning_bid),
    final_judgment_amount = COALESCE(EXCLUDED.final_judgment_amount, foreclosures.final_judgment_amount),
    appraised_value       = COALESCE(EXCLUDED.appraised_value, foreclosures.appraised_value),
    sold_to               = COALESCE(EXCLUDED.sold_to, foreclosures.sold_to),
    buyer_type            = COALESCE(EXCLUDED.buyer_type, foreclosures.buyer_type),
    owner_name            = COALESCE(EXCLUDED.owner_name, foreclosures.owner_name),
    land_use              = COALESCE(EXCLUDED.land_use, foreclosures.land_use),
    year_built            = COALESCE(EXCLUDED.year_built, foreclosures.year_built),
    beds                  = COALESCE(EXCLUDED.beds, foreclosures.beds),
    baths                 = COALESCE(EXCLUDED.baths, foreclosures.baths),
    heated_area           = COALESCE(EXCLUDED.heated_area, foreclosures.heated_area),
    market_value          = EXCLUDED.market_value,
    assessed_value        = EXCLUDED.assessed_value,
    clerk_case_type       = COALESCE(EXCLUDED.clerk_case_type, foreclosures.clerk_case_type),
    clerk_case_status     = COALESCE(EXCLUDED.clerk_case_status, foreclosures.clerk_case_status),
    filing_date           = COALESCE(EXCLUDED.filing_date, foreclosures.filing_date),
    judgment_date         = COALESCE(EXCLUDED.judgment_date, foreclosures.judgment_date),
    is_foreclosure        = COALESCE(EXCLUDED.is_foreclosure, foreclosures.is_foreclosure),
    homestead_exempt      = EXCLUDED.homestead_exempt,
    estimated_annual_tax  = EXCLUDED.estimated_annual_tax,
    zestimate             = EXCLUDED.zestimate,
    list_price            = EXCLUDED.list_price,
    listing_status        = EXCLUDED.listing_status;
"""

# ---------------------------------------------------------------------------
# Step 1.5 — Resolve strap/folio via address matching
# ---------------------------------------------------------------------------

RESOLVE_STRAP_SQL = """
UPDATE foreclosures f SET
    strap = sub.strap,
    folio = sub.folio
FROM (
    SELECT DISTINCT ON (f2.foreclosure_id)
           f2.foreclosure_id, bp.strap, bp.folio
    FROM foreclosures f2
    JOIN hcpa_bulk_parcels bp
        ON UPPER(TRIM(SPLIT_PART(
               REPLACE(f2.property_address, E'\t', ' '), ',', 1
           ))) = bp.property_address
    WHERE f2.strap IS NULL
      AND f2.property_address IS NOT NULL
    ORDER BY f2.foreclosure_id, bp.source_file_id DESC
) sub
WHERE sub.foreclosure_id = f.foreclosure_id;
"""

# ---------------------------------------------------------------------------
# Step 1.6 — Backfill coordinates + core property fields from HCPA parcel tables
# ---------------------------------------------------------------------------

ENRICH_COORDS_PROPERTY_SQL = """
UPDATE foreclosures f SET
    latitude = COALESCE(f.latitude, src.latitude, src.latitude_latlon),
    longitude = COALESCE(f.longitude, src.longitude, src.longitude_latlon),
    owner_name = COALESCE(f.owner_name, src.owner_name),
    land_use = COALESCE(f.land_use, src.land_use_desc),
    year_built = COALESCE(f.year_built, src.year_built),
    beds = COALESCE(f.beds, src.beds),
    baths = COALESCE(f.baths, src.baths),
    heated_area = COALESCE(f.heated_area, src.heated_area),
    market_value = COALESCE(f.market_value, src.market_value),
    assessed_value = COALESCE(f.assessed_value, src.assessed_value),
    property_address = COALESCE(f.property_address, src.property_address)
FROM (
    SELECT DISTINCT ON (f2.foreclosure_id)
           f2.foreclosure_id,
           bp.owner_name,
           bp.land_use_desc,
           bp.year_built,
           bp.beds,
           bp.baths,
           bp.heated_area,
           bp.market_value,
           bp.assessed_value,
           bp.property_address,
           bp.latitude,
           bp.longitude,
           ll.latitude AS latitude_latlon,
           ll.longitude AS longitude_latlon
    FROM foreclosures f2
    JOIN hcpa_bulk_parcels bp
      ON (f2.strap IS NOT NULL AND bp.strap = f2.strap)
      OR (f2.folio IS NOT NULL AND bp.folio = f2.folio)
    LEFT JOIN hcpa_latlon ll ON bp.folio = ll.folio
    ORDER BY f2.foreclosure_id, bp.source_file_id DESC NULLS LAST
) src
WHERE src.foreclosure_id = f.foreclosure_id
  AND (
      f.latitude IS NULL
      OR f.longitude IS NULL
      OR f.owner_name IS NULL
      OR f.land_use IS NULL
      OR f.year_built IS NULL
      OR f.beds IS NULL
      OR f.baths IS NULL
      OR f.heated_area IS NULL
      OR f.market_value IS NULL
      OR f.assessed_value IS NULL
      OR f.property_address IS NULL
  );
"""

# ---------------------------------------------------------------------------
# Step 2 — Compute first valid resale from hcpa_allsales
# ---------------------------------------------------------------------------

RESALE_SQL = """
UPDATE foreclosures f SET
    first_valid_resale_date  = sub.sale_date,
    first_valid_resale_price = sub.sale_amount,
    hold_days                = sub.sale_date - f.auction_date,
    resale_profit            = sub.sale_amount - COALESCE(f.winning_bid, 0),
    roi = CASE
        WHEN f.winning_bid > 0
        THEN (sub.sale_amount - f.winning_bid) / f.winning_bid
    END
FROM (
    SELECT DISTINCT ON (f2.foreclosure_id)
           f2.foreclosure_id, s.sale_date, s.sale_amount
    FROM foreclosures f2
    JOIN hcpa_bulk_parcels bp ON f2.strap = bp.strap
    JOIN hcpa_allsales s ON bp.folio = s.folio
    WHERE s.sale_date > f2.auction_date
      AND s.sale_amount > 0
      AND s.sale_type IN ('WD','QC','TR','FD','DD','CT','CD')
    ORDER BY f2.foreclosure_id, s.sale_date
) sub
WHERE sub.foreclosure_id = f.foreclosure_id;
"""

# ---------------------------------------------------------------------------
# Step 3 — Populate foreclosure_events from clerk docket
#           Delete + re-insert (events are immutable clerk data)
# ---------------------------------------------------------------------------

EVENTS_DELETE_SQL = """
DELETE FROM foreclosure_events fe
USING foreclosures f
WHERE fe.foreclosure_id = f.foreclosure_id
  AND f.case_number_norm IS NOT NULL;
"""

EVENTS_INSERT_SQL = """
INSERT INTO foreclosure_events
    (foreclosure_id, event_date, event_code, event_description, party_name)
SELECT DISTINCT
    f.foreclosure_id,
    e.event_date,
    e.event_code,
    e.event_description,
    e.party_last_name
FROM foreclosures f
JOIN clerk_civil_events e ON f.case_number_norm = e.case_number
WHERE f.case_number_norm IS NOT NULL;
"""

# ---------------------------------------------------------------------------
# Step 4 — Encumbrance counts from ori_encumbrances
# ---------------------------------------------------------------------------

ENCUMBRANCE_SQL = """
UPDATE foreclosures f SET
    encumbrance_count             = sub.total,
    unsatisfied_encumbrance_count = sub.unsatisfied
FROM (
    SELECT oe.strap,
           COUNT(*)                              AS total,
           COUNT(*) FILTER (WHERE NOT oe.is_satisfied
                            AND oe.survival_status NOT IN ('SATISFIED','EXPIRED','HISTORICAL','EXTINGUISHED'))
                                                 AS unsatisfied
    FROM ori_encumbrances oe
    WHERE oe.strap IS NOT NULL
    GROUP BY oe.strap
) sub
WHERE sub.strap = f.strap;
"""

# ---------------------------------------------------------------------------
# Step 4.5 — UCC exposure from sunbiz_flr
# ---------------------------------------------------------------------------

UCC_SQL = """
UPDATE foreclosures f SET
    has_ucc_liens = sub.has_liens,
    ucc_active_count = sub.active_count
FROM (
    SELECT f2.foreclosure_id,
           (COUNT(DISTINCT fl.doc_number) > 0) AS has_liens,
           COUNT(DISTINCT fl.doc_number)::INT AS active_count
    FROM foreclosures f2
    JOIN hcpa_bulk_parcels bp ON f2.strap = bp.strap
    LEFT JOIN sunbiz_flr_parties p
        ON p.party_role = 'D'
        AND p.name % bp.owner_name
        AND similarity(p.name, bp.owner_name) > 0.4
    LEFT JOIN sunbiz_flr_filings fl
        ON p.doc_number = fl.doc_number
        AND fl.filing_status = 'A'
        AND (fl.expiration_date IS NULL OR fl.expiration_date >= CURRENT_DATE)
    WHERE f2.strap IS NOT NULL
      AND bp.owner_name IS NOT NULL
    GROUP BY f2.foreclosure_id
) sub
WHERE sub.foreclosure_id = f.foreclosure_id;
"""

# ---------------------------------------------------------------------------
# Step 5 — Archive past auctions
# ---------------------------------------------------------------------------

ARCHIVE_SQL = """
UPDATE foreclosures SET archived_at = now()
WHERE auction_date < CURRENT_DATE
  AND archived_at IS NULL
  AND auction_status IS NOT NULL;
"""

# ---------------------------------------------------------------------------
# Step 5.5 — Age-out sync into foreclosures_history
# ---------------------------------------------------------------------------

ENSURE_HISTORY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS foreclosures_history (
    LIKE foreclosures INCLUDING DEFAULTS INCLUDING CONSTRAINTS
);
"""

ENSURE_HISTORY_COLUMN_SQL = """
ALTER TABLE foreclosures_history
ADD COLUMN IF NOT EXISTS moved_to_history_at TIMESTAMPTZ NOT NULL DEFAULT now();
"""

ENSURE_HISTORY_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_fch_case_date_unique
ON foreclosures_history(case_number_raw, auction_date);
"""

HISTORY_SYNC_SQL = """
INSERT INTO foreclosures_history
SELECT f.*, now() AS moved_to_history_at
FROM foreclosures f
WHERE f.auction_date < CURRENT_DATE
ON CONFLICT (case_number_raw, auction_date) DO UPDATE
SET
    auction_status = EXCLUDED.auction_status,
    archived_at = COALESCE(EXCLUDED.archived_at, foreclosures_history.archived_at),
    updated_at = EXCLUDED.updated_at,
    moved_to_history_at = now();
"""


# ---------------------------------------------------------------------------
# Step 6 — Load judgment extractions from disk JSON files
# ---------------------------------------------------------------------------


def _load_judgment_data(conn: object) -> int:
    """Scan data/Foreclosure/*/documents/*_extracted.json and push into PG.

    Matches by case_number first, then falls back to strap from the JSON's
    parcel_id field.
    """
    if not FORECLOSURE_DATA_DIR.exists():
        return 0

    # Build lookup maps
    rows = conn.execute(  # type: ignore[union-attr]
        text("SELECT foreclosure_id, case_number_raw, strap FROM foreclosures")
    ).fetchall()
    case_map: dict[str, int] = {r[1]: r[0] for r in rows}
    strap_map: dict[str, int] = {r[2]: r[0] for r in rows if r[2]}

    updated = 0
    parse_errors = 0
    unmatched = 0
    for json_path in FORECLOSURE_DATA_DIR.rglob("*_extracted.json"):
        case_number = json_path.parent.parent.name

        try:
            jd = json.loads(json_path.read_text())
        except (json.JSONDecodeError, OSError) as exc:
            parse_errors += 1
            logger.warning("Skipping invalid judgment JSON {}: {}", json_path, exc)
            continue

        # Match: case_number first, then strap from JSON parcel_id
        fid = case_map.get(case_number)
        if not fid:
            parcel_id = jd.get("parcel_id", "")
            if parcel_id:
                fid = strap_map.get(parcel_id)
        if not fid:
            unmatched += 1
            continue

        pdf_path = None
        for p in json_path.parent.glob("*.pdf"):
            pdf_path = str(p)
            break

        fja = jd.get("total_judgment_amount")

        conn.execute(  # type: ignore[union-attr]
            text(
                "UPDATE foreclosures SET "
                "  judgment_data = CAST(:jd AS jsonb), "
                "  pdf_path = COALESCE(:pp, pdf_path), "
                "  final_judgment_amount = COALESCE(:fja, final_judgment_amount), "
                "  step_judgment_extracted = COALESCE(step_judgment_extracted, now()) "
                "WHERE foreclosure_id = :fid"
            ),
            {"jd": json.dumps(jd), "pp": pdf_path, "fja": fja, "fid": fid},
        )
        updated += 1

    if parse_errors:
        logger.warning(
            "Judgment extraction ingest skipped {} unreadable JSON files",
            parse_errors,
        )
    if unmatched:
        logger.info(
            "Judgment extraction ingest had {} files that did not match foreclosures",
            unmatched,
        )

    return updated


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def _count_missing_coords(conn: object) -> int:
    return (
        conn.execute(  # type: ignore[union-attr]
            text(
                """
                SELECT COUNT(*)
                FROM foreclosures
                WHERE latitude IS NULL OR longitude IS NULL
                """
            )
        ).scalar()
        or 0
    )


def _log_unresolved_coord_sample(conn: object, *, limit: int = 10) -> None:
    rows = conn.execute(  # type: ignore[union-attr]
        text(
            """
            SELECT foreclosure_id, strap, folio, property_address, auction_date
            FROM foreclosures
            WHERE latitude IS NULL OR longitude IS NULL
            ORDER BY auction_date DESC NULLS LAST, foreclosure_id DESC
            LIMIT :limit
            """
        ),
        {"limit": limit},
    ).fetchall()
    if not rows:
        return
    logger.warning(f"Unresolved coordinate sample (up to {limit}):")
    for r in rows:
        d = dict(r._mapping)  # noqa: SLF001
        logger.warning(
            "  - ID: {id} | Strap: {strap} | Folio: {folio} | Date: {date} | Address: {addr}",
            id=d.get("foreclosure_id", "N/A"),
            strap=d.get("strap") or "None",
            folio=d.get("folio") or "None",
            date=d.get("auction_date", "N/A"),
            addr=d.get("property_address") or "None",
        )


def refresh(dsn: str | None = None) -> dict[str, int]:
    """Run all refresh steps. Returns rowcounts per step."""
    engine = get_engine(resolve_pg_dsn(dsn))
    counts: dict[str, int] = {}

    t0 = time.monotonic()
    with engine.begin() as conn:
        # Step 1: Seed / update from foreclosures_history
        r = conn.execute(text(UPSERT_SQL))
        counts["upserted"] = r.rowcount
        logger.info(f"Step 1: upserted {r.rowcount} foreclosures")

        # Step 1.5: Resolve strap/folio from address
        r = conn.execute(text(RESOLVE_STRAP_SQL))
        counts["strap_resolved"] = r.rowcount
        logger.info(f"Step 1.5: resolved strap for {r.rowcount} rows via address")

        # Step 1.6: Backfill coordinates + property enrichment from HCPA tables
        missing_before = _count_missing_coords(conn)
        counts["coords_missing_before"] = missing_before
        r = conn.execute(text(ENRICH_COORDS_PROPERTY_SQL))
        counts["coords_enriched"] = r.rowcount
        missing_after = _count_missing_coords(conn)
        counts["coords_missing_after"] = missing_after
        logger.info(
            "Step 1.6: enriched coords/property for {} rows; missing_coords {} -> {}",
            r.rowcount,
            missing_before,
            missing_after,
        )
        if missing_after > 0:
            _log_unresolved_coord_sample(conn, limit=10)

        # Step 2: Resale analytics
        r = conn.execute(text(RESALE_SQL))
        counts["resale"] = r.rowcount
        logger.info(f"Step 2: computed resale for {r.rowcount} rows")

        # Step 3: Docket events
        r = conn.execute(text(EVENTS_DELETE_SQL))
        counts["events_deleted"] = r.rowcount
        r = conn.execute(text(EVENTS_INSERT_SQL))
        counts["events_inserted"] = r.rowcount
        logger.info(f"Step 3: refreshed {r.rowcount} docket events")

        # Step 4: Encumbrance counts
        r = conn.execute(text(ENCUMBRANCE_SQL))
        counts["encumbrances"] = r.rowcount
        logger.info(f"Step 4: updated encumbrance counts for {r.rowcount} rows")

        # Step 4.5: UCC exposure
        try:
            r = conn.execute(text(UCC_SQL))
            counts["ucc_exposure"] = r.rowcount
            logger.info(f"Step 4.5: computed UCC exposure for {r.rowcount} rows")
        except Exception as exc:
            logger.warning(f"Step 4.5: UCC exposure failed (pg_trgm extension needed?): {exc}")
            counts["ucc_exposure"] = 0

        # Step 5: Archive
        r = conn.execute(text(ARCHIVE_SQL))
        counts["archived"] = r.rowcount
        logger.info(f"Step 5: archived {r.rowcount} past auctions")

        # Step 5.5: Sync archived rows into history table
        conn.execute(text(ENSURE_HISTORY_TABLE_SQL))
        conn.execute(text(ENSURE_HISTORY_COLUMN_SQL))
        conn.execute(text(ENSURE_HISTORY_INDEX_SQL))
        r = conn.execute(text(HISTORY_SYNC_SQL))
        counts["history_synced"] = r.rowcount
        logger.info(f"Step 5.5: synced {r.rowcount} rows into foreclosures_history")

        # Step 6: Judgment data from disk
        n = _load_judgment_data(conn)
        counts["judgments"] = n
        logger.info(f"Step 6: loaded {n} judgment extractions from disk")
        upcoming = (
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM foreclosures
                    WHERE auction_date >= CURRENT_DATE
                      AND archived_at IS NULL
                    """
                )
            ).scalar()
            or 0
        )
        counts["upcoming_auctions"] = int(upcoming)
        logger.info(
            "Step 7: {} upcoming auctions currently present in foreclosures",
            upcoming,
        )

    elapsed = time.monotonic() - t0
    logger.info(f"Refresh complete in {elapsed:.1f}s — {counts}")
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh foreclosures table from PG reference data")
    parser.add_argument("--dsn", help="PostgreSQL DSN (default from env / sunbiz.db)")
    parser.add_argument(
        "--migrate",
        action="store_true",
        help="Run DDL migration before refreshing",
    )
    args = parser.parse_args()

    if args.migrate:
        from src.db.migrations.create_foreclosures import migrate

        migrate(dsn=args.dsn)

    refresh(dsn=args.dsn)


if __name__ == "__main__":
    main()
