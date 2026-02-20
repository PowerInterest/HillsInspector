"""
Step 2.5 — Resolve missing parcel IDs for auctions.

65 of 186 auctions (35%) have empty parcel_id because the clerk's auction page
had no parcel link. Without a parcel_id, Steps 3-12 all skip. This module uses
judgment data and bulk_parcels to resolve them via four strategies in order of
reliability: strap conversion, exact address match, legal description
disambiguation, and defendant name matching.
"""

from __future__ import annotations

import json
import re
from typing import Any

from loguru import logger

from src.scrapers.hcpa_gis_scraper import convert_bulk_parcel_to_url_format

TAG = "[RESOLVE]"


def resolve_missing_parcel_ids(db, pg_service=None) -> dict:
    """Resolve missing parcel_ids on auctions using judgment data and bulk_parcels.

    Args:
        db: PropertyDB instance.
        pg_service: Optional PgSalesService for fuzzy name resolution.

    Returns:
        Stats dict with counts by strategy and outcome.
    """
    conn = db.connect()

    # Find auctions that might need resolution — include those with non-empty
    # parcel_id but empty folio so we can explicitly skip "already resolved"
    candidates = conn.execute(
        """
        SELECT case_number, folio, parcel_id, property_address,
               extracted_judgment_data, has_valid_parcel_id
        FROM auctions
        WHERE (folio IS NULL OR folio = '' OR folio = 'MULTIPLE PARCEL')
        """
    ).fetchall()
    candidates = [dict(r) for r in candidates]

    stats = {
        "total_candidates": len(candidates),
        "resolved": 0,
        "skipped_already_resolved": 0,
        "skipped_no_data": 0,
        "skipped_multiple_parcel": 0,
        "skipped_ambiguous": 0,
        "by_strategy": {
            "pg_fuzzy": 0,
            "strap_conversion": 0,
            "strap_conversion_unverified": 0,
            "address_exact_unique": 0,
            "legal_desc_match": 0,
            "name_match": 0,
        },
    }

    logger.info(f"{TAG} Found {len(candidates)} auctions with missing/invalid folio")
    # Warn if bulk_parcels is empty (all resolution strategies depend on bulk lookups)
    try:
        bulk_count = conn.execute("SELECT COUNT(*) FROM bulk_parcels").fetchone()[0]
        if bulk_count == 0:
            logger.warning(
                f"{TAG} bulk_parcels is empty — parcel resolution will fail for all strategies. "
                "Load bulk data via: python -m src.ingest.bulk_parcel_ingest --download "
                "and then python -m src.ingest.bulk_parcel_ingest --enrich"
            )
    except Exception as e:
        logger.exception(f"{TAG} Could not verify bulk_parcels row count: {e}")
        raise RuntimeError(
            f"{TAG} bulk_parcels verification failed. Ensure bulk table exists and is readable."
        ) from e

    for row in candidates:
        case = row["case_number"]
        logger.info(f"{TAG} ═══ {case} ═══")

        # Parse judgment data if available
        jdata = _parse_judgment(row.get("extracted_judgment_data"))
        auction_addr = (row.get("property_address") or "").strip()
        has_judgment = jdata is not None

        # Log data sources
        logger.info(
            f"{TAG}   Data sources: judgment={'YES' if has_judgment else 'NO'}, "
            f'auction_address="{auction_addr or "NONE"}"'
        )

        # --- Immediate skips ---
        # Already has a valid parcel_id (just no folio yet — will be set by enrichment)
        pid = (row.get("parcel_id") or "").strip()
        if pid and row.get("has_valid_parcel_id"):
            logger.info(f"{TAG}   SKIP: already has valid parcel_id={pid}")
            stats["skipped_already_resolved"] += 1
            continue

        if row.get("folio") == "MULTIPLE PARCEL":
            logger.info(f"{TAG}   SKIP: MULTIPLE PARCEL — cannot resolve to single parcel")
            stats["skipped_multiple_parcel"] += 1
            continue

        if not has_judgment and not auction_addr:
            logger.info(f"{TAG}   SKIP: no judgment data and no auction address")
            stats["skipped_no_data"] += 1
            continue

        # --- Strategy 0: PG fuzzy name resolution ---
        if pg_service and pg_service.available:
            defendant = None
            if jdata:
                defs = jdata.get("defendants") or []
                if defs:
                    d = defs[0]
                    defendant = d.get("name", "") if isinstance(d, dict) else str(d)
            if not defendant:
                defendant = row.get("defendant") or ""
            plaintiff_hint = None
            if jdata:
                plaintiff_hint = jdata.get("plaintiff")

            if defendant and defendant.strip():
                matches = pg_service.resolve_property_by_name(
                    defendant_name=defendant.strip(),
                    plaintiff_hint=plaintiff_hint,
                )
                if matches and matches[0]["match_score"] >= 0.5:
                    resolved_strap = matches[0]["strap"]
                    # Verify strap exists in bulk_parcels
                    found, _ = _lookup_strap(conn, resolved_strap)
                    if found:
                        logger.info(
                            f"{TAG}   Strategy 0 (PG fuzzy): score={matches[0]['match_score']:.2f}, "
                            f"method={matches[0]['match_method']}, strap={resolved_strap}"
                        )
                        _apply_resolution(conn, case, resolved_strap, "pg_fuzzy", stats)
                        continue
                    logger.debug(f"{TAG}   Strategy 0: PG strap {resolved_strap} not in bulk_parcels")

        # --- Strategy 1: Judgment parcel_id → strap conversion ---
        result, verified = _strategy_strap_conversion(conn, jdata)
        if result:
            key = "strap_conversion" if verified else "strap_conversion_unverified"
            _apply_resolution(conn, case, result, key, stats)
            continue

        # --- Strategy 2: Exact address match ---
        address, address_source = _best_address(jdata, auction_addr)
        if address:
            logger.info(f"{TAG}   Strategy 2 (address match): using {address_source} address")
        address_candidates = _strategy_address_match(conn, address)
        if address_candidates is not None:
            if len(address_candidates) == 1:
                cand = address_candidates[0]
                logger.info(
                    f'{TAG}     candidate: strap={cand["strap"]}, owner="{cand["owner_name"]}"'
                )
                _apply_resolution(conn, case, cand["strap"], "address_exact_unique", stats)
                continue
            if len(address_candidates) == 0:
                logger.info(f"{TAG}   Strategy 2 (address match): 0 results — no match")
                # Fall through — no candidates to disambiguate
                address_candidates = None

        # --- Strategy 3: Legal description disambiguation ---
        if address_candidates and len(address_candidates) > 1 and not jdata:
            logger.info(f"{TAG}   Strategy 3 (legal description): SKIP — no judgment data")
        if address_candidates and len(address_candidates) > 1 and jdata:
            result = _strategy_legal_desc(address_candidates, jdata)
            if result:
                _apply_resolution(conn, case, result, "legal_desc_match", stats)
                continue

        # --- Strategy 4: Defendant name matching ---
        if address_candidates and len(address_candidates) > 1 and not jdata:
            logger.info(f"{TAG}   Strategy 4 (name match): SKIP — no judgment data")
        if address_candidates and len(address_candidates) > 1 and jdata:
            result = _strategy_name_match(address_candidates, jdata)
            if result:
                _apply_resolution(conn, case, result, "name_match", stats)
                continue

        # All strategies exhausted
        n = len(address_candidates) if address_candidates else 0
        logger.info(
            f"{TAG}   ✗ SKIP: all strategies exhausted"
            + (f", {n} ambiguous candidates for \"{address}\"" if n > 1 else "")
        )
        stats["skipped_ambiguous"] += 1

    # Batch commit
    conn.commit()

    logger.info(
        f"{TAG} ═══ Summary: {stats['resolved']} resolved, "
        f"{stats['skipped_already_resolved']} already resolved, "
        f"{stats['skipped_no_data']} no data, "
        f"{stats['skipped_multiple_parcel']} multi-parcel, "
        f"{stats['skipped_ambiguous']} ambiguous ═══"
    )
    return stats


# ---------------------------------------------------------------------------
# Strategy implementations
# ---------------------------------------------------------------------------


def _strategy_strap_conversion(conn, jdata: dict | None) -> tuple[str | None, bool]:
    """Strategy 1: Convert judgment parcel_id to strap via format conversion.

    Returns (strap_or_none, was_verified_in_bulk_parcels).
    """
    if not jdata:
        logger.info(f"{TAG}   Strategy 1 (strap conversion): SKIP — no judgment data")
        return None, False

    pid = (jdata.get("parcel_id") or "").strip()
    if not pid:
        logger.info(f"{TAG}   Strategy 1 (strap conversion): SKIP — no parcel_id in judgment")
        return None, False

    # Detect structured format (has dashes → bulk format like A-08-29-19-4NU-B00000-00004.0)
    if "-" in pid and len(pid.split("-")) >= 6:
        converted = convert_bulk_parcel_to_url_format(pid)
        logger.info(f'{TAG}   Strategy 1 (strap conversion): parcel_id="{pid}" → converted to "{converted}"')

        # Check bulk_parcels for the converted strap
        found, owner = _lookup_strap(conn, converted)
        if found:
            logger.info(f'{TAG}     bulk_parcels lookup: FOUND, owner="{owner or "UNKNOWN"}"')
            return converted, True

        logger.info(f"{TAG}     bulk_parcels lookup: NOT FOUND")

        # Try U suffix instead of A (condos)
        if converted.endswith("A"):
            u_variant = converted[:-1] + "U"
            found, owner = _lookup_strap(conn, u_variant)
            if found:
                logger.info(
                    f'{TAG}     trying U suffix: "{u_variant}" → FOUND, owner="{owner or "UNKNOWN"}"'
                )
                return u_variant, True
            logger.info(f'{TAG}     trying U suffix: "{u_variant}" → NOT FOUND')

        # Deterministic conversion is valid even without bulk_parcels confirmation
        # (bulk data may not be loaded yet). Accept as unverified.
        logger.info(f"{TAG}     accepting unverified strap (deterministic conversion)")
        return converted, False

    # Numeric folio format (e.g. A0039920000 or 0039920000)
    cleaned = pid.lstrip("A").strip()
    if cleaned.isdigit():
        padded = cleaned.zfill(10)
        logger.info(f'{TAG}   Strategy 1 (strap conversion): numeric folio="{padded}"')
        row = conn.execute(
            "SELECT strap FROM bulk_parcels WHERE folio = ? LIMIT 1", [padded]
        ).fetchone()
        if row:
            strap = row[0]
            logger.info(f'{TAG}     bulk_parcels folio lookup: FOUND strap="{strap}"')
            return strap, True
        logger.info(f"{TAG}     bulk_parcels folio lookup: NOT FOUND")
        return None, False

    logger.info(f'{TAG}   Strategy 1 (strap conversion): unrecognized format "{pid}" — SKIP')
    return None, False


def _strategy_address_match(conn, address: str | None) -> list[dict[str, Any]] | None:
    """Strategy 2: Exact address match in bulk_parcels.

    Returns list of candidate dicts, or None if no address available.
    """
    if not address:
        logger.info(f"{TAG}   Strategy 2 (address match): SKIP — no address available")
        return None

    normalized = _normalize_address(address)
    if not normalized:
        logger.info(f"{TAG}   Strategy 2 (address match): SKIP — address normalized to empty")
        return None

    rows = conn.execute(
        """
        SELECT folio, strap, owner_name,
               COALESCE(raw_legal1,'') || ' ' || COALESCE(raw_legal2,'') || ' ' ||
               COALESCE(raw_legal3,'') || ' ' || COALESCE(raw_legal4,'') AS legal_concat
        FROM bulk_parcels
        WHERE UPPER(property_address) = ?
        """,
        [normalized],
    ).fetchall()
    results = [dict(r) for r in rows]
    logger.info(f'{TAG}   Strategy 2 (address match): exact query="{normalized}" → {len(results)} result(s)')

    if not results:
        # Fallback: LIKE-based search using street number + first significant word
        # Handles cases like "815 10TH ST SW" vs "815 SW 10TH ST" ordering differences
        match = re.match(r"^(\d+)\s+(.+)$", normalized)
        if match:
            street_num = match.group(1)
            rest = match.group(2)
            # Use first significant word (3+ chars, not a directional)
            sig_words = [w for w in rest.split()
                         if len(w) >= 3 and w not in {"N", "S", "E", "W", "NE", "NW", "SE", "SW"}]
            if sig_words:
                like_pattern = f"{street_num}%{sig_words[0]}%"
                rows = conn.execute(
                    """
                    SELECT folio, strap, owner_name,
                           COALESCE(raw_legal1,'') || ' ' || COALESCE(raw_legal2,'') || ' ' ||
                           COALESCE(raw_legal3,'') || ' ' || COALESCE(raw_legal4,'') AS legal_concat
                    FROM bulk_parcels
                    WHERE UPPER(property_address) LIKE ?
                    """,
                    [like_pattern],
                ).fetchall()
                results = [dict(r) for r in rows]
                logger.info(f'{TAG}   Strategy 2 (address LIKE): pattern="{like_pattern}" → {len(results)} result(s)')

    return results


def _strategy_legal_desc(candidates: list[dict], jdata: dict) -> str | None:
    """Strategy 3: Disambiguate multiple address matches using legal description fields."""
    unit = (jdata.get("unit") or "").strip()
    lot = (jdata.get("lot") or "").strip()
    block = (jdata.get("block") or "").strip()
    subdivision = (jdata.get("subdivision") or "").strip()
    is_condo = jdata.get("is_condo", False)

    fields = []
    if unit:
        fields.append(f"unit={unit}")
    if lot:
        fields.append(f"lot={lot}")
    if block:
        fields.append(f"block={block}")
    if subdivision:
        fields.append(f"subdivision={subdivision}")
    if is_condo:
        fields.append("is_condo=True")

    logger.info(
        f"{TAG}   Strategy 3 (legal description): judgment fields: "
        + (", ".join(fields) if fields else "NONE")
    )

    if not any([unit, lot, block, subdivision]):
        logger.info(f"{TAG}     no structured fields available for disambiguation — FAIL")
        return None

    best_score = 0
    best_strap = None
    best_count = 0
    best_matches: list[str] = []
    best_owner = ""
    match_counts = {"unit": 0, "lot": 0, "block": 0, "subdivision": 0}
    scored: list[tuple[str, int, list[str], str]] = []

    for cand in candidates:
        legal = (cand.get("legal_concat") or "").upper()
        score = 0
        matches: list[str] = []

        if unit:
            pattern = rf"\bUNIT\s+{re.escape(unit)}\b"
            if re.search(pattern, legal, re.IGNORECASE):
                score += 2  # Unit match is strong signal
                matches.append("unit")
                match_counts["unit"] += 1

        if lot:
            pattern = rf"\bLOT\s+{re.escape(lot)}\b"
            if re.search(pattern, legal, re.IGNORECASE):
                score += 1
                matches.append("lot")
                match_counts["lot"] += 1

        if block:
            pattern = rf"\bBLOCK\s+{re.escape(block)}\b"
            if re.search(pattern, legal, re.IGNORECASE):
                score += 1
                matches.append("block")
                match_counts["block"] += 1

        if subdivision:
            # Use first significant word (4+ chars) from subdivision
            sig_words = [w for w in subdivision.split() if len(w) >= 4]
            if sig_words and sig_words[0].upper() in legal:
                score += 1
                matches.append("subdivision")
                match_counts["subdivision"] += 1

        scored.append((cand["strap"], score, matches, cand.get("owner_name") or ""))

        if score > best_score:
            best_score = score
            best_strap = cand["strap"]
            best_count = 1
            best_matches = matches
            best_owner = cand.get("owner_name") or ""
        elif score == best_score and score > 0:
            best_count += 1

    if len(scored) <= 5:
        for strap, score, matches, owner in scored:
            logger.info(
                f"{TAG}     candidate score: strap={strap}, score={score}, "
                f"matches={matches or ['none']}, owner=\"{owner}\""
            )
    else:
        logger.info(
            f"{TAG}     candidates={len(scored)}; match_counts={match_counts}, "
            f"best_score={best_score}, best_count={best_count}"
        )

    if best_score >= 1 and best_count == 1:
        logger.info(
            f"{TAG}     1 candidate matched (score={best_score}, matches={best_matches or ['none']}): "
            f'strap={best_strap}, owner="{best_owner}"'
        )
        return best_strap

    if best_count > 1:
        logger.info(f"{TAG}     {best_count} candidates tied at score={best_score} — FAIL")
    elif best_score == 0:
        logger.info(f"{TAG}     no candidates scored > 0 — FAIL")

    return None


def _strategy_name_match(candidates: list[dict], jdata: dict) -> str | None:
    """Strategy 4: Match defendant names to owner_name as last resort."""
    defendants = jdata.get("defendants") or []
    if not defendants:
        logger.info(f"{TAG}   Strategy 4 (name match): SKIP — no defendants in judgment")
        return None

    # Extract and normalize defendant names
    def_names = []
    for d in defendants:
        name = d.get("name", "") if isinstance(d, dict) else str(d)
        name = name.strip()
        if name:
            def_names.append(name)

    if not def_names:
        logger.info(f"{TAG}   Strategy 4 (name match): SKIP — no defendant names")
        return None

    logger.info(f'{TAG}   Strategy 4 (name match): defendants={def_names}')

    normalized_defs = [_normalize_name(n) for n in def_names]
    normalized_display = [sorted(s) for s in normalized_defs]
    logger.info(f"{TAG}     normalized_defendants={normalized_display}")
    matching_straps = []
    matched_details: list[tuple[str, str, set[str]]] = []

    for cand in candidates:
        owner_raw = cand.get("owner_name") or ""
        owner = _normalize_name(owner_raw)
        if not owner:
            continue
        for def_words_set in normalized_defs:
            if def_words_set and def_words_set.issubset(owner):
                matching_straps.append(cand["strap"])
                matched_details.append((cand["strap"], owner_raw, def_words_set))
                break

    if len(matching_straps) == 1:
        strap, owner_raw, def_words = matched_details[0]
        logger.info(
            f'{TAG}     1 owner matched: strap={strap}, owner="{owner_raw}", '
            f"matched_words={sorted(def_words)}"
        )
        return matching_straps[0]

    if len(matching_straps) > 1:
        sample = ", ".join(
            [f'{s}:"{o}"' for s, o, _ in matched_details[:3]]
        )
        logger.info(
            f"{TAG}     {len(matching_straps)}/{len(candidates)} candidates matched "
            f"— no disambiguation possible; sample={sample}"
        )
    else:
        logger.info(f"{TAG}     0 candidates matched defendants")

    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_judgment(raw: str | None) -> dict | None:
    """Parse extracted_judgment_data JSON, returning None on failure."""
    if not raw:
        return None
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except (json.JSONDecodeError, TypeError) as e:
        logger.debug(f"Could not parse judgment JSON ({e}): {raw[:100] if raw else 'None'}")
        return None


def _best_address(jdata: dict | None, auction_addr: str) -> tuple[str | None, str]:
    """Pick the best available address, preferring judgment over auction page."""
    if jdata:
        jaddr = (jdata.get("property_address") or "").strip()
        if jaddr:
            return jaddr, "judgment"
    return auction_addr or None, "auction"


_STREET_SUFFIX_MAP = {
    "STREET": "ST", "AVENUE": "AVE", "DRIVE": "DR", "BOULEVARD": "BLVD",
    "ROAD": "RD", "LANE": "LN", "COURT": "CT", "CIRCLE": "CIR",
    "PLACE": "PL", "TRAIL": "TRL", "TERRACE": "TER", "PARKWAY": "PKWY",
    "HIGHWAY": "HWY", "EXPRESSWAY": "EXPY", "LOOP": "LOOP", "WAY": "WAY",
}
_DIRECTIONAL_MAP = {
    "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
    "NORTHEAST": "NE", "NORTHWEST": "NW", "SOUTHEAST": "SE", "SOUTHWEST": "SW",
}


def _normalize_address(addr: str) -> str:
    """Normalize address: extract street portion, strip city/state/zip, uppercase.

    Handles both comma-delimited ("123 MAIN ST, TAMPA, FL 33601") and
    single-line ("123 MAIN ST TAMPA FL 33601") formats.
    Also normalizes street suffixes (STREET→ST) and directionals (WEST→W)
    to match bulk_parcels conventions.
    """
    if not addr:
        return ""
    street = addr.strip().upper()
    # Remove extra whitespace
    street = re.sub(r"\s+", " ", street)

    # Strip zip/state first (before comma split, handles "VALRICO FLORIDA 33594")
    street = re.sub(r"\s+FLORIDA\s*\d{5}(?:-\d{4})?\s*$", "", street)

    # If comma present, take first part (most reliable delimiter)
    if "," in street:
        street = street.split(",")[0].strip()
    else:
        # No comma — strip trailing state/zip patterns:
        #   "123 MAIN ST TAMPA FL 33601"  → "123 MAIN ST TAMPA"
        #   "123 MAIN ST TAMPA FL- 33625" → "123 MAIN ST TAMPA"
        #   "123 MAIN ST FL 33601"        → "123 MAIN ST"
        street = re.sub(r"\s+FL[-\s]*\d{5}(?:-\d{4})?\s*$", "", street)
        street = re.sub(r"\s+FL\s*$", "", street)
        # Strip known Hillsborough County city names from end
        for city in ["TAMPA", "PLANT CITY", "TEMPLE TERRACE", "BRANDON",
                      "RIVERVIEW", "VALRICO", "LUTZ", "SEFFNER", "RUSKIN",
                      "APOLLO BEACH", "LITHIA", "THONOTOSASSA", "DOVER",
                      "GIBSONTON", "WIMAUMA", "SUN CITY CENTER", "ODESSA"]:
            if street.endswith(f" {city}"):
                street = street[: -len(f" {city}")]
                break

    # Remove trailing unit/apt markers
    street = re.sub(r"\s*#\s*\d+\s*$", "", street)

    # Normalize street suffixes (STREET → ST, AVENUE → AVE, etc.)
    words = street.split()
    normalized = []
    for word in words:
        normalized.append(_STREET_SUFFIX_MAP.get(word, _DIRECTIONAL_MAP.get(word, word)))
    street = " ".join(normalized)

    return street.strip()


def _normalize_name(name: str) -> set[str]:
    """Normalize a person/entity name to a set of significant words."""
    name = name.upper()
    # Strip common suffixes
    for suffix in [
        "/TRUSTEE", "/TR", " ET AL", " ET UX", " ET VIR",
        " A/K/A", " F/K/A", " N/K/A", " D/B/A",
        " AS TRUSTEE", " INDIVIDUALLY", " AS PERSONAL REPRESENTATIVE",
        " LLC", " INC", " CORP", " CORPORATION", " LP", " LLP",
        " TRUST", " REVOCABLE", " IRREVOCABLE", " LIVING",
    ]:
        name = name.replace(suffix, "")
    # Split and keep significant words (3+ chars, not common filler)
    filler = {"AND", "THE", "FOR", "HIS", "HER", "ITS", "ANY", "ALL", "WHO", "ARE"}
    return {w for w in re.split(r"[^A-Z0-9]+", name) if len(w) >= 3 and w not in filler}


def _lookup_strap(conn, strap: str) -> tuple[bool, str | None]:
    """Look up a strap in bulk_parcels. Returns (found, owner_name)."""
    row = conn.execute(
        "SELECT owner_name FROM bulk_parcels WHERE strap = ? LIMIT 1", [strap]
    ).fetchone()
    if not row:
        return False, None
    return True, row[0]


def _apply_resolution(conn, case_number: str, strap: str, strategy: str, stats: dict):
    """Apply a resolved strap to the auction record and reset status for re-processing."""
    conn.execute(
        """
        UPDATE auctions
        SET folio = ?, parcel_id = ?, has_valid_parcel_id = 1,
            updated_at = CURRENT_TIMESTAMP
        WHERE case_number = ?
        """,
        [strap, strap, case_number],
    )
    # Also update the status table so enrichment can proceed:
    # - Set parcel_id so _enrich_property finds it
    # - Reset pipeline_status from 'skipped' to 'pending' so it gets picked up
    conn.execute(
        """
        UPDATE status
        SET parcel_id = ?,
            pipeline_status = CASE
                WHEN pipeline_status = 'skipped' THEN 'pending'
                ELSE pipeline_status
            END,
            last_error = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE case_number = ?
        """,
        [strap, case_number],
    )
    stats["resolved"] += 1
    stats["by_strategy"][strategy] += 1
    logger.info(f"{TAG}   ✓ RESOLVED via {strategy} → strap={strap}")
