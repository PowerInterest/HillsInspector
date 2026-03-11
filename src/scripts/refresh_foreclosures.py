"""
Idempotent refresh: enrich foreclosures rows from bulk reference data.

Sources:
  - hcpa_bulk_parcels      (property enrichment)
  - hcpa_latlon            (coordinates)
  - clerk_civil_cases      (case metadata)
  - clerk_civil_events     (docket timeline -> foreclosure_events)
  - dor_nal_parcels        (tax / homestead)
  - property_market        (Zillow / listing)
  - hcpa_allsales          (resale analytics)
  - sunbiz_flr_*           (UCC exposure)

Run:  uv run python scripts/refresh_foreclosures.py
      uv run python scripts/refresh_foreclosures.py --migrate   # create tables first
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import text

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from sunbiz.db import get_engine, resolve_pg_dsn

FORECLOSURE_DATA_DIR = Path("data/Foreclosure")

# ---------------------------------------------------------------------------
# Step 1 — Enrich foreclosure rows in place from bulk reference data
# ---------------------------------------------------------------------------

ENRICH_BASE_SQL = """
UPDATE foreclosures f SET
    folio                 = COALESCE(f.folio, bp.folio),
    strap                 = COALESCE(bp.strap, f.strap),
    property_address      = COALESCE(bp.property_address, f.property_address),
    latitude              = COALESCE(f.latitude, bp.latitude, ll.latitude),
    longitude             = COALESCE(f.longitude, bp.longitude, ll.longitude),
    owner_name            = COALESCE(bp.owner_name, f.owner_name),
    land_use              = COALESCE(bp.land_use_desc, f.land_use),
    year_built            = COALESCE(bp.year_built, f.year_built),
    beds                  = COALESCE(bp.beds, f.beds),
    baths                 = COALESCE(bp.baths, f.baths),
    heated_area           = COALESCE(bp.heated_area, f.heated_area),
    market_value          = COALESCE(bp.market_value, f.market_value),
    assessed_value        = COALESCE(bp.assessed_value, f.assessed_value),
    clerk_case_type       = COALESCE(cc.case_type, f.clerk_case_type),
    clerk_case_status     = COALESCE(cc.case_status, f.clerk_case_status),
    filing_date           = COALESCE(cc.filing_date, f.filing_date),
    judgment_date         = COALESCE(cc.judgment_date, f.judgment_date),
    is_foreclosure        = COALESCE(cc.is_foreclosure, f.is_foreclosure),
    homestead_exempt      = COALESCE(dn.homestead_exempt, f.homestead_exempt),
    estimated_annual_tax  = COALESCE(dn.estimated_annual_tax, f.estimated_annual_tax),
    zestimate             = COALESCE(pm.zestimate, f.zestimate),
    list_price            = COALESCE(pm.list_price, f.list_price),
    listing_status        = COALESCE(pm.listing_status, f.listing_status)
FROM foreclosures f2
-- Property enrichment (prefer exact strap+folio match, then current strap,
-- then folio-only repair when the stored strap is missing or invalid)
LEFT JOIN LATERAL (
    SELECT bp2.folio, bp2.strap, bp2.property_address, bp2.owner_name,
           bp2.land_use_desc, bp2.year_built, bp2.beds, bp2.baths,
           bp2.heated_area, bp2.market_value, bp2.assessed_value,
           bp2.latitude, bp2.longitude
    FROM hcpa_bulk_parcels bp2
    WHERE (f2.strap IS NOT NULL AND bp2.strap = f2.strap)
       OR (f2.folio IS NOT NULL AND bp2.folio = f2.folio)
    ORDER BY
        CASE
            WHEN f2.strap IS NOT NULL
             AND f2.folio IS NOT NULL
             AND bp2.strap = f2.strap
             AND bp2.folio = f2.folio
            THEN 0
            WHEN f2.strap IS NOT NULL
             AND bp2.strap = f2.strap
            THEN 1
            WHEN f2.folio IS NOT NULL
             AND bp2.folio = f2.folio
            THEN 2
            ELSE 3
        END,
        bp2.source_file_id DESC
    LIMIT 1
) bp ON TRUE
-- Coordinates fallback
LEFT JOIN hcpa_latlon ll ON COALESCE(f2.folio, bp.folio) = ll.folio
-- Clerk case metadata (join on normalized case number)
LEFT JOIN clerk_civil_cases cc
    ON normalize_case_number_fn(f2.case_number_raw) = cc.case_number
-- Tax data (latest tax year per strap)
LEFT JOIN LATERAL (
    SELECT dn2.homestead_exempt, dn2.estimated_annual_tax
    FROM dor_nal_parcels dn2
    WHERE dn2.strap = COALESCE(f2.strap, bp.strap)
    ORDER BY dn2.tax_year DESC
    LIMIT 1
) dn ON COALESCE(f2.strap, bp.strap) IS NOT NULL
-- Market snapshot
LEFT JOIN property_market pm
    ON COALESCE(f2.strap, bp.strap) = pm.strap
WHERE f.foreclosure_id = f2.foreclosure_id;
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
    SELECT f2.foreclosure_id,
           COUNT(*)                              AS total,
           COUNT(*) FILTER (
               WHERE NOT oe.is_satisfied
                 AND COALESCE(fes.survival_status, oe.survival_status, 'UNKNOWN') NOT IN (
                     'SATISFIED', 'EXPIRED', 'HISTORICAL', 'EXTINGUISHED'
                 )
           )
                                                 AS unsatisfied
    FROM foreclosures f2
    JOIN ori_encumbrances oe
      ON oe.strap = f2.strap
    LEFT JOIN foreclosure_encumbrance_survival fes
      ON fes.foreclosure_id = f2.foreclosure_id
     AND fes.encumbrance_id = oe.id
    WHERE f2.strap IS NOT NULL
      AND oe.encumbrance_type != 'noc'
    GROUP BY f2.foreclosure_id
) sub
WHERE sub.foreclosure_id = f.foreclosure_id;
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
        ON p.party_role = 'debtor'
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
# Step 6 — Load judgment extractions from disk JSON files
# ---------------------------------------------------------------------------


def _load_judgment_data(conn: object) -> int:
    """Scan data/Foreclosure/*/documents/*_extracted.json and push into PG.

    Uses PgJudgmentService.select_best_judgment to pick the single best
    final-judgment JSON per case directory, then delegates the actual PG
    write to PgJudgmentService.persist_judgment — the single canonical
    persistence path.  This eliminates the previous divergence where the
    refresh path used different write semantics (missing step_pdf_downloaded,
    no best-judgment selection, arbitrary PDF matching).
    """
    from src.services.pg_judgment_service import PgJudgmentService

    if not FORECLOSURE_DATA_DIR.exists():
        return 0

    # Build lookup maps — prefer active rows; within active, prefer newest auction_date
    rows = conn.execute(  # type: ignore[union-attr]
        text(
            "SELECT DISTINCT ON (case_number_raw)"
            "  foreclosure_id, case_number_raw, strap"
            " FROM foreclosures"
            " ORDER BY case_number_raw, archived_at NULLS FIRST, auction_date DESC"
        )
    ).fetchall()
    case_map: dict[str, int] = {r[1]: r[0] for r in rows}
    strap_map: dict[str, int] = {r[2]: r[0] for r in rows if r[2]}

    # Group extracted JSONs by case directory so we process each case
    # exactly once, choosing the best candidate.
    case_jsons: dict[str, list[Path]] = {}
    for json_path in FORECLOSURE_DATA_DIR.rglob("*_extracted.json"):
        if json_path.parent.name != "documents":
            continue
        case_number = json_path.parent.parent.name
        case_jsons.setdefault(case_number, []).append(json_path)

    updated = 0
    unmatched = 0
    for case_number, json_paths in case_jsons.items():
        best = PgJudgmentService.select_best_judgment(json_paths)
        if best is None:
            continue
        chosen_json_path, jd = best

        # Match: case_number first, then strap from JSON parcel_id
        fid = case_map.get(case_number)
        if not fid:
            parcel_id = jd.get("parcel_id", "")
            if parcel_id:
                fid = strap_map.get(parcel_id)
        if not fid:
            unmatched += 1
            continue

        # Derive PDF path from chosen JSON stem (not arbitrary first PDF)
        matching_pdf = chosen_json_path.parent / (
            f"{chosen_json_path.stem.removesuffix('_extracted')}.pdf"
        )
        pdf_path = str(matching_pdf) if matching_pdf.exists() else None

        normalized_jd, _, _ = PgJudgmentService.normalize_judgment_payload(jd)
        validation = PgJudgmentService.validate_judgment_payload(normalized_jd)
        if not validation.get("is_valid"):
            logger.warning(
                "Skipping canonical judgment persistence for case {} during refresh because chosen cache is invalid: {}",
                case_number,
                "; ".join(validation.get("failures") or ["unknown validation failure"]),
            )
            continue

        if PgJudgmentService.persist_judgment(
            conn,
            foreclosure_id=fid,
            judgment_data=normalized_jd,
            pdf_path=pdf_path,
        ):
            updated += 1

    if unmatched:
        logger.info(
            "Judgment extraction ingest had {} files that did not match foreclosures",
            unmatched,
        )

    return updated


# ---------------------------------------------------------------------------
# Step 7 — Copy enrichment data from archived rows to rescheduled auctions
# ---------------------------------------------------------------------------

RESCHEDULED_REUSE_SQL = """
UPDATE foreclosures new_f SET
    strap = COALESCE(new_f.strap, donor.strap),
    folio = COALESCE(new_f.folio, donor.folio),
    property_address = COALESCE(new_f.property_address, donor.property_address),
    judgment_data = COALESCE(new_f.judgment_data, donor.judgment_data),
    step_judgment_extracted = COALESCE(new_f.step_judgment_extracted, donor.step_judgment_extracted),
    step_identifier_recovery = COALESCE(
        new_f.step_identifier_recovery,
        donor.step_identifier_recovery
    ),
    step_ori_searched = CASE
        WHEN COALESCE(new_f.strap, donor.strap) = donor.strap
         AND EXISTS (SELECT 1 FROM ori_encumbrances WHERE strap = donor.strap)
        THEN COALESCE(new_f.step_ori_searched, donor.step_ori_searched)
        ELSE new_f.step_ori_searched
    END,
    step_survival_analyzed = CASE
        WHEN COALESCE(new_f.strap, donor.strap) = donor.strap
         AND NOT EXISTS (
             SELECT 1
             FROM ori_encumbrances oe
             WHERE oe.strap = donor.strap
               AND oe.encumbrance_type != 'noc'
               AND NOT EXISTS (
                   SELECT 1
                   FROM foreclosure_encumbrance_survival fes
                   WHERE fes.foreclosure_id = donor.foreclosure_id
                     AND fes.encumbrance_id = oe.id
               )
         )
        THEN COALESCE(new_f.step_survival_analyzed, donor.step_survival_analyzed)
        ELSE new_f.step_survival_analyzed
    END
FROM (
    SELECT DISTINCT ON (case_number_raw)
        foreclosure_id, case_number_raw, strap, folio, property_address,
        judgment_data, step_judgment_extracted, step_identifier_recovery,
        step_ori_searched, step_survival_analyzed
    FROM foreclosures
    WHERE archived_at IS NOT NULL
    ORDER BY case_number_raw, archived_at DESC NULLS LAST
) donor
WHERE new_f.case_number_raw = donor.case_number_raw
  AND new_f.archived_at IS NULL
  AND new_f.foreclosure_id > donor.foreclosure_id
  AND (new_f.judgment_data IS NULL
       OR new_f.strap IS NULL
       OR new_f.folio IS NULL
       OR new_f.property_address IS NULL)
RETURNING
    new_f.foreclosure_id AS new_foreclosure_id,
    donor.foreclosure_id AS donor_foreclosure_id,
    CASE
        WHEN COALESCE(new_f.strap, donor.strap) = donor.strap
         AND NOT EXISTS (
             SELECT 1
             FROM ori_encumbrances oe
             WHERE oe.strap = donor.strap
               AND oe.encumbrance_type != 'noc'
               AND NOT EXISTS (
                   SELECT 1
                   FROM foreclosure_encumbrance_survival fes
                   WHERE fes.foreclosure_id = donor.foreclosure_id
                     AND fes.encumbrance_id = oe.id
               )
         )
        THEN TRUE
        ELSE FALSE
    END AS copy_survival;
"""

RESCHEDULED_COPY_SURVIVAL_SQL = """
INSERT INTO foreclosure_encumbrance_survival (
    foreclosure_id,
    encumbrance_id,
    survival_status,
    survival_reason,
    survival_case_number,
    analyzed_at,
    created_at,
    updated_at
)
SELECT
    :new_foreclosure_id,
    fes.encumbrance_id,
    fes.survival_status,
    fes.survival_reason,
    fes.survival_case_number,
    fes.analyzed_at,
    now(),
    now()
FROM foreclosure_encumbrance_survival fes
WHERE fes.foreclosure_id = :donor_foreclosure_id
ON CONFLICT (foreclosure_id, encumbrance_id) DO NOTHING;
"""


def _reuse_rescheduled_enrichment(conn: Any) -> dict[str, int]:
    """Copy archived-row enrichment and any matching per-foreclosure survival rows."""
    result = conn.execute(text(RESCHEDULED_REUSE_SQL))
    rows = result.mappings().fetchall()
    updated_foreclosures = len(rows)
    copied_survival_rows = 0

    survival_pairs = [
        {
            "new_foreclosure_id": row["new_foreclosure_id"],
            "donor_foreclosure_id": row["donor_foreclosure_id"],
        }
        for row in rows
        if row["copy_survival"]
    ]
    if survival_pairs:
        copy_result = conn.execute(text(RESCHEDULED_COPY_SURVIVAL_SQL), survival_pairs)
        copied_survival_rows = max(copy_result.rowcount or 0, 0)

    return {
        "updated_foreclosures": updated_foreclosures,
        "copied_survival_rows": copied_survival_rows,
    }


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
    logger.warning(
        f"Missing Map Coordinates ({len(rows)} upcoming properties):\\n"
        f"These properties do not have lat/lon coordinates yet because their addresses "
        f"could not be definitively mapped by the bulk data load."
    )
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
        # Step 1: Enrich foreclosure rows from bulk reference data
        r = conn.execute(text(ENRICH_BASE_SQL))
        counts["enriched"] = r.rowcount
        logger.info(f"Step 1: enriched {r.rowcount} foreclosures from bulk data")

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

        # Step 6: Judgment data from disk
        n = _load_judgment_data(conn)
        counts["judgments"] = n
        logger.info(f"Step 6: loaded {n} judgment extractions from disk")

        # Step 7: Reuse enrichment data for rescheduled auctions
        reuse_counts = _reuse_rescheduled_enrichment(conn)
        counts["rescheduled_reused"] = reuse_counts["updated_foreclosures"]
        counts["rescheduled_survival_rows"] = reuse_counts["copied_survival_rows"]
        logger.info(
            "Step 7: copied enrichment data to {} rescheduled auction rows",
            reuse_counts["updated_foreclosures"],
        )
        if reuse_counts["copied_survival_rows"]:
            logger.info(
                "Step 7: copied {} per-foreclosure survival rows to rescheduled auctions",
                reuse_counts["copied_survival_rows"],
            )

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
            "Summary: {} upcoming auctions currently present in foreclosures",
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
