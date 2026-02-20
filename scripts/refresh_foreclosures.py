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
  - SQLite auctions        (upcoming auction ingestion)
  - SQLite encumbrances    (ORI encumbrance sync)
  - sunbiz_flr_*           (UCC exposure)

Run:  uv run python scripts/refresh_foreclosures.py
      uv run python scripts/refresh_foreclosures.py --migrate   # create tables first
      uv run python scripts/refresh_foreclosures.py --sync-encumbrances  # one-time SQLite→PG
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path

from loguru import logger
from sqlalchemy import text

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
    for json_path in FORECLOSURE_DATA_DIR.rglob("*_extracted.json"):
        case_number = json_path.parent.parent.name

        try:
            jd = json.loads(json_path.read_text())
        except (json.JSONDecodeError, OSError):
            continue

        # Match: case_number first, then strap from JSON parcel_id
        fid = case_map.get(case_number)
        if not fid:
            parcel_id = jd.get("parcel_id", "")
            if parcel_id:
                fid = strap_map.get(parcel_id)
        if not fid:
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

    return updated


# ---------------------------------------------------------------------------
# Step 7 — Ingest upcoming auctions from SQLite pipeline
# ---------------------------------------------------------------------------

def _resolve_sqlite_path() -> Path | None:
    """Find the SQLite database path from .env or default location."""
    import os

    from dotenv import load_dotenv

    load_dotenv()
    db_path = os.environ.get("HILLS_SQLITE_DB")
    if db_path:
        p = Path(db_path)
        if p.exists():
            return p
    # Fallback
    for candidate in [
        Path("/home/user/hills_data/property_master_sqlite.db"),
        Path("data/property_master_sqlite.db"),
    ]:
        if candidate.exists() and candidate.stat().st_size > 0:
            return candidate
    return None


def _ingest_upcoming_auctions(conn: object) -> int:
    """Read upcoming auctions from SQLite and upsert into PG foreclosures."""
    sqlite_path = _resolve_sqlite_path()
    if not sqlite_path:
        logger.warning("SQLite database not found — skipping upcoming auction ingestion")
        return 0

    sconn = sqlite3.connect(str(sqlite_path))
    sconn.row_factory = sqlite3.Row
    try:
        rows = sconn.execute("""
            SELECT case_number, auction_date, parcel_id, property_address,
                   final_judgment_amount, auction_type, plaintiff, defendant,
                   judgment_date, assessed_value
            FROM auctions
            WHERE auction_date >= date('now')
              AND case_number IS NOT NULL
        """).fetchall()
    finally:
        sconn.close()

    if not rows:
        return 0

    inserted = 0
    for row in rows:
        r = dict(row)
        conn.execute(  # type: ignore[union-attr]
            text("""
                INSERT INTO foreclosures (
                    case_number_raw, auction_date, auction_type,
                    strap, property_address,
                    final_judgment_amount, assessed_value,
                    judgment_date
                ) VALUES (
                    :case_number, CAST(:auction_date AS DATE),
                    COALESCE(:auction_type, 'foreclosure'),
                    NULLIF(:parcel_id, ''),
                    :property_address,
                    :final_judgment_amount, :assessed_value,
                    CAST(NULLIF(:judgment_date, '') AS DATE)
                )
                ON CONFLICT (case_number_raw, auction_date) DO UPDATE SET
                    property_address = COALESCE(EXCLUDED.property_address, foreclosures.property_address),
                    strap = COALESCE(EXCLUDED.strap, foreclosures.strap),
                    final_judgment_amount = COALESCE(EXCLUDED.final_judgment_amount, foreclosures.final_judgment_amount),
                    assessed_value = COALESCE(EXCLUDED.assessed_value, foreclosures.assessed_value)
            """),
            {
                "case_number": r["case_number"],
                "auction_date": r["auction_date"],
                "auction_type": (r.get("auction_type") or "foreclosure").lower(),
                "parcel_id": r.get("parcel_id") or "",
                "property_address": r.get("property_address"),
                "final_judgment_amount": r.get("final_judgment_amount"),
                "assessed_value": r.get("assessed_value"),
                "judgment_date": r.get("judgment_date") or "",
            },
        )
        inserted += 1

    return inserted


# ---------------------------------------------------------------------------
# ORI Encumbrance sync: SQLite → PG (one-time or periodic)
# ---------------------------------------------------------------------------

def _sync_ori_encumbrances(conn: object) -> int:
    """Migrate encumbrances from SQLite to PG ori_encumbrances.

    Maps SQLite encumbrances columns to the PG ori_encumbrances schema.
    Idempotent: unique constraint on (folio, instrument_number, book, page, book_type).
    """
    sqlite_path = _resolve_sqlite_path()
    if not sqlite_path:
        logger.warning("SQLite database not found — skipping ORI encumbrance sync")
        return 0

    sconn = sqlite3.connect(str(sqlite_path))
    sconn.row_factory = sqlite3.Row
    try:
        rows = sconn.execute("""
            SELECT id, folio, encumbrance_type, creditor, debtor,
                   amount, amount_confidence, recording_date, instrument,
                   book, page, is_satisfied, satisfaction_instrument,
                   satisfaction_date, survival_status, survival_reason,
                   is_inferred, chain_period_id
            FROM encumbrances
        """).fetchall()
    finally:
        sconn.close()

    if not rows:
        logger.info("No encumbrances found in SQLite")
        return 0

    logger.info(f"Syncing {len(rows)} encumbrances from SQLite to PG ori_encumbrances")

    # Build strap→folio lookup from PG
    folio_map: dict[str, str] = {}
    pg_rows = conn.execute(  # type: ignore[union-attr]
        text("SELECT DISTINCT strap, folio FROM hcpa_bulk_parcels WHERE strap IS NOT NULL AND folio IS NOT NULL")
    ).fetchall()
    for pr in pg_rows:
        folio_map[pr[0]] = pr[1]

    # Build case_number→strap lookup for judgment-inferred encumbrances
    # (these have case_number as folio instead of real strap)
    case_strap_map: dict[str, str] = {}
    # From SQLite auctions
    try:
        sconn2 = sqlite3.connect(str(sqlite_path))
        sconn2.row_factory = sqlite3.Row
        case_rows = sconn2.execute(
            "SELECT case_number, parcel_id FROM auctions WHERE parcel_id IS NOT NULL AND parcel_id != ''"
        ).fetchall()
        for cr in case_rows:
            case_strap_map[cr["case_number"]] = cr["parcel_id"]
        sconn2.close()
    except Exception:
        pass
    # From PG foreclosures (covers address-resolved cases)
    fc_rows = conn.execute(  # type: ignore[union-attr]
        text("SELECT case_number_raw, strap FROM foreclosures WHERE strap IS NOT NULL")
    ).fetchall()
    for fr in fc_rows:
        if fr[0] not in case_strap_map:
            case_strap_map[fr[0]] = fr[1]

    # Valid PG enum values
    valid_types = {
        "mortgage", "judgment", "lis_pendens", "lien",
        "easement", "satisfaction", "release", "assignment", "other",
    }

    inserted = 0
    skipped = 0
    for row in rows:
        r = dict(row)
        raw_folio = r["folio"]  # SQLite folio = HCPA strap format OR case_number

        # If the "folio" is actually a case number, look up the real strap
        if raw_folio and (raw_folio.startswith("29") and ("CA" in raw_folio or "CC" in raw_folio)):
            strap = case_strap_map.get(raw_folio)
            if not strap:
                skipped += 1
                continue  # Can't resolve — skip
        else:
            strap = raw_folio

        pg_folio = folio_map.get(strap) if strap else None

        enc_type = r["encumbrance_type"] or "other"
        if enc_type not in valid_types:
            enc_type = "other"

        instrument = r.get("instrument") or ""
        # Synthetic key for rows without instrument numbers (unique constraint needs non-empty)
        if not instrument:
            instrument = f"SQ{r['id']}"

        book = r.get("book") or ""
        page = r.get("page") or ""

        # Use savepoint so individual row errors don't kill the whole transaction
        conn.execute(text("SAVEPOINT enc_row"))  # type: ignore[union-attr]
        try:
            conn.execute(  # type: ignore[union-attr]
                text("""
                    INSERT INTO ori_encumbrances (
                        folio, strap, instrument_number, book, page, book_type,
                        encumbrance_type, party1, party2,
                        amount, amount_confidence,
                        recording_date, is_satisfied,
                        satisfaction_instrument, satisfaction_date,
                        survival_status, survival_reason,
                        discovered_at, updated_at
                    ) VALUES (
                        :folio, :strap, :instrument, NULLIF(:book, ''), NULLIF(:page, ''), 'OR',
                        CAST(:enc_type AS encumbrance_type_enum), :party1, :party2,
                        :amount, :amount_confidence,
                        CAST(NULLIF(:recording_date, '') AS DATE),
                        :is_satisfied,
                        NULLIF(:satisfaction_instrument, ''),
                        CAST(NULLIF(:satisfaction_date, '') AS DATE),
                        :survival_status, :survival_reason,
                        now(), now()
                    )
                    ON CONFLICT (folio, COALESCE(instrument_number, ''),
                                 COALESCE(book, ''), COALESCE(page, ''),
                                 COALESCE(book_type, 'OR'))
                    DO UPDATE SET
                        survival_status = COALESCE(EXCLUDED.survival_status, ori_encumbrances.survival_status),
                        survival_reason = COALESCE(EXCLUDED.survival_reason, ori_encumbrances.survival_reason),
                        amount = COALESCE(EXCLUDED.amount, ori_encumbrances.amount),
                        updated_at = now()
                """),
                {
                    "folio": pg_folio,
                    "strap": strap,
                    "instrument": instrument,
                    "book": book,
                    "page": page,
                    "enc_type": enc_type,
                    "party1": r.get("creditor"),
                    "party2": r.get("debtor"),
                    "amount": r.get("amount"),
                    "amount_confidence": r.get("amount_confidence"),
                    "recording_date": r.get("recording_date") or "",
                    "is_satisfied": bool(r.get("is_satisfied")),
                    "satisfaction_instrument": r.get("satisfaction_instrument") or "",
                    "satisfaction_date": r.get("satisfaction_date") or "",
                    "survival_status": r.get("survival_status"),
                    "survival_reason": r.get("survival_reason"),
                },
            )
            conn.execute(text("RELEASE SAVEPOINT enc_row"))  # type: ignore[union-attr]
            inserted += 1
        except Exception as exc:
            conn.execute(text("ROLLBACK TO SAVEPOINT enc_row"))  # type: ignore[union-attr]
            skipped += 1
            if skipped <= 3:
                logger.warning(f"Skipped encumbrance SQLite id={r['id']}: {exc}")

    if skipped:
        logger.warning(f"Skipped {skipped} encumbrances due to errors")
    return inserted


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
    sample = [dict(r._mapping) for r in rows]  # noqa: SLF001
    logger.warning(f"Unresolved coordinate sample (up to {limit}): {sample}")


def refresh(dsn: str | None = None, sync_encumbrances: bool = False) -> dict[str, int]:
    """Run all refresh steps. Returns rowcounts per step."""
    engine = get_engine(resolve_pg_dsn(dsn))
    counts: dict[str, int] = {}

    t0 = time.monotonic()
    with engine.begin() as conn:
        # Step 0: Sync ORI encumbrances from SQLite (if requested)
        if sync_encumbrances:
            n = _sync_ori_encumbrances(conn)
            counts["ori_encumbrances_synced"] = n
            logger.info(f"Step 0: synced {n} ORI encumbrances from SQLite")

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

        # Step 7: Ingest upcoming auctions from SQLite
        n = _ingest_upcoming_auctions(conn)
        counts["upcoming_auctions"] = n
        logger.info(f"Step 7: ingested {n} upcoming auctions from SQLite")

        # Step 7.5: Re-enrich newly ingested auctions
        # (repeat strap resolve + encumbrance counts + lat/lon + property data for new rows)
        if n > 0:
            r = conn.execute(text(RESOLVE_STRAP_SQL))
            counts["strap_resolved_2"] = r.rowcount

            missing_before_2 = _count_missing_coords(conn)
            r = conn.execute(text(ENRICH_COORDS_PROPERTY_SQL))
            counts["enriched_coords"] = r.rowcount
            counts["coords_missing_after_2"] = _count_missing_coords(conn)
            logger.info(
                "Step 7.5: enriched coords/property for {} rows; missing_coords {} -> {}",
                r.rowcount,
                missing_before_2,
                counts["coords_missing_after_2"],
            )
            if counts["coords_missing_after_2"] > 0:
                _log_unresolved_coord_sample(conn, limit=10)

            # Tax data
            r = conn.execute(text("""
                UPDATE foreclosures f SET
                    homestead_exempt = dn.homestead_exempt,
                    estimated_annual_tax = dn.estimated_annual_tax
                FROM (
                    SELECT DISTINCT ON (dn2.strap) dn2.strap, dn2.homestead_exempt, dn2.estimated_annual_tax
                    FROM dor_nal_parcels dn2
                    ORDER BY dn2.strap, dn2.tax_year DESC
                ) dn
                WHERE f.strap = dn.strap
                  AND f.homestead_exempt IS NULL
            """))
            counts["enriched_tax"] = r.rowcount

            r = conn.execute(text(ENCUMBRANCE_SQL))
            counts["encumbrances_2"] = r.rowcount
            logger.info(f"Step 7.5: re-enriched {r.rowcount} new auction rows")

    elapsed = time.monotonic() - t0
    logger.info(f"Refresh complete in {elapsed:.1f}s — {counts}")
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh foreclosures table from PG reference data"
    )
    parser.add_argument("--dsn", help="PostgreSQL DSN (default from env / sunbiz.db)")
    parser.add_argument(
        "--migrate", action="store_true",
        help="Run DDL migration before refreshing",
    )
    parser.add_argument(
        "--sync-encumbrances", action="store_true",
        help="Sync ORI encumbrances from SQLite to PG (one-time migration)",
    )
    args = parser.parse_args()

    if args.migrate:
        from src.db.migrations.create_foreclosures import migrate

        migrate(dsn=args.dsn)

    refresh(dsn=args.dsn, sync_encumbrances=args.sync_encumbrances)


if __name__ == "__main__":
    main()
