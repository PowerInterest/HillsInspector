"""Phase B Step 2.5: Recover missing foreclosure strap/folio identifiers.

This service fills missing ``foreclosures.strap`` / ``foreclosures.folio`` using
already-extracted final-judgment JSON data:
1. Parcel-ID match (exact + digit-normalized) against ``hcpa_bulk_parcels``.
2. Legal-description match against HCPA legal text (lot/block/subdivision aware).
3. Address + legal cross-check (address alone is never trusted).

The service is intentionally conservative:
- It only updates unresolved foreclosures (strap or folio missing).
- It updates null fields only (``COALESCE``).
- It logs ambiguous/unresolved cases explicitly (no silent failures).
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import Any

from loguru import logger
import requests
from sqlalchemy import text

from src.utils.legal_description import legal_descriptions_match
from src.utils.legal_description import parse_legal_description
from sunbiz.db import get_engine, resolve_pg_dsn

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

_LEGAL_EXPR = (
    "UPPER(COALESCE(raw_legal1, '') || ' ' || COALESCE(raw_legal2, '') || "
    "' ' || COALESCE(raw_legal3, '') || ' ' || COALESCE(raw_legal4, ''))"
)

_GENERIC_SUBDIVISION_TERMS = frozenset(
    {
        "THE",
        "OF",
        "IN",
        "AT",
        "A",
        "AN",
        "AND",
        "OR",
        "LOT",
        "BLOCK",
        "UNIT",
        "PHASE",
        "SECTION",
        "SUBDIVISION",
        "SUBDIV",
        "PLAT",
        "BOOK",
        "PAGE",
    }
)

_MAX_CANDIDATES_LEGAL = 300
_MAX_CANDIDATES_ADDRESS = 60
_UNRESOLVED_SAMPLE_LIMIT = 12
_MAX_OWNER_NAMES = 6
_MAX_OWNER_MATCHES = 12
_OWNER_MATCH_THRESHOLD = 0.50
_OWNER_LEGAL_CONFIRM_THRESHOLD = 0.85

_PAV_KEYWORD_URL = (
    "https://publicaccess.hillsclerk.com"
    "/PAVDirectSearch/api/CustomQuery/KeywordSearch"
)
_ORI_DOC_SEARCH_URL = (
    "https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search"
)
_ORI_DOC_HEADERS = {
    "Content-Type": "application/json; charset=UTF-8",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Origin": "https://publicaccess.hillsclerk.com",
    "Referer": "https://publicaccess.hillsclerk.com/oripublicaccess/",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0",
}
_PAV_HEADERS = {
    "Content-Type": "application/json",
    "Origin": "https://publicaccess.hillsclerk.com",
    "Referer": "https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html",
}

_CASE_NUM_RE = re.compile(r"^\d{0,4}\s*(CA|CC)\s*\d+$", re.IGNORECASE)
_INSTRUMENT_FROM_STEM_RE = re.compile(r"(\d{8,12})$")
_PARCEL_SEGMENT_RE = re.compile(
    r"^(?:U-)?(\d{2})-(\d{2})-(\d{2})-([A-Z0-9]+)-(\d+)-(\d+)\.(\d)$"
)
_PLACEHOLDER_PARCEL_IDS = frozenset(
    {
        "STRING OR NULL",
        "N/A",
        "NONE",
        "UNKNOWN",
        "MULTIPLE PARCEL",
    }
)
_ENTITY_KEYWORDS = frozenset(
    {
        "LLC",
        "INC",
        "CORP",
        "ASSOCIATION",
        "ASSOC",
        "CITY OF",
        "TRUSTEE",
        "TRUST",
        "BANK",
        "MORTGAGE",
        "SERVICING",
        "FINANCIAL",
        "FUNDING",
        "CAPITAL",
        "LOAN",
        "UNKNOWN",
        "TENANT",
        "POSSESSION",
        "INTERNAL REVENUE SERVICE",
        "UNITED STATES",
        "STATE OF",
        "DEPARTMENT OF",
    }
)

_SCOPE_SQL = """
SELECT
    f.foreclosure_id,
    f.case_number_raw,
    f.strap,
    f.folio,
    f.pdf_path,
    f.property_address,
    NULLIF(TRIM(f.judgment_data->>'parcel_id'), '')          AS jd_parcel_id,
    NULLIF(TRIM(f.judgment_data->>'property_address'), '')   AS jd_property_address,
    NULLIF(TRIM(f.judgment_data->>'legal_description'), '')  AS jd_legal_description,
    NULLIF(TRIM(f.judgment_data->>'subdivision'), '')        AS jd_subdivision,
    NULLIF(TRIM(f.judgment_data->>'lot'), '')                AS jd_lot,
    NULLIF(TRIM(f.judgment_data->>'block'), '')              AS jd_block,
    NULLIF(TRIM(f.judgment_data->>'unit'), '')               AS jd_unit,
    NULLIF(TRIM(f.judgment_data->>'plat_book'), '')          AS jd_plat_book,
    NULLIF(TRIM(f.judgment_data->>'plat_page'), '')          AS jd_plat_page
FROM foreclosures f
WHERE f.archived_at IS NULL
  AND f.judgment_data IS NOT NULL
  AND (f.strap IS NULL OR f.folio IS NULL)
ORDER BY f.auction_date NULLS LAST, f.foreclosure_id
"""

_PARCEL_LOOKUP_SQL = text(
    """
    SELECT
        folio, strap, property_address,
        raw_legal1, raw_legal2, raw_legal3, raw_legal4,
        source_file_id
    FROM hcpa_bulk_parcels
    WHERE strap = :value OR folio = :value
    ORDER BY source_file_id DESC NULLS LAST
    LIMIT 10
    """
)

_PARCEL_LOOKUP_DIGITS_SQL = text(
    r"""
    SELECT
        folio, strap, property_address,
        raw_legal1, raw_legal2, raw_legal3, raw_legal4,
        source_file_id
    FROM hcpa_bulk_parcels
    WHERE regexp_replace(COALESCE(strap, ''), '\D', '', 'g') = :value
       OR regexp_replace(COALESCE(folio, ''), '\D', '', 'g') = :value
    ORDER BY source_file_id DESC NULLS LAST
    LIMIT 10
    """
)

_ADDRESS_LOOKUP_SQL = text(
    """
    SELECT
        folio, strap, property_address,
        raw_legal1, raw_legal2, raw_legal3, raw_legal4,
        source_file_id
    FROM hcpa_bulk_parcels
    WHERE property_address = :address
    ORDER BY source_file_id DESC NULLS LAST
    LIMIT :limit
    """
)

_ORI_LOT_BLOCK_SQL = text(
    """
    SELECT
        folio, strap, property_address,
        raw_legal1, raw_legal2, raw_legal3, raw_legal4,
        source_file_id
    FROM hcpa_bulk_parcels
    WHERE raw_legal2 ILIKE :lot_block
      AND raw_legal1 ILIKE :sub_seed
    ORDER BY source_file_id DESC NULLS LAST
    LIMIT 25
    """
)

_ORI_CONDO_SQL = text(
    """
    SELECT
        folio, strap, property_address,
        raw_legal1, raw_legal2, raw_legal3, raw_legal4,
        source_file_id
    FROM hcpa_bulk_parcels
    WHERE raw_legal2 ILIKE :unit_pattern
      AND raw_legal1 ILIKE :sub_seed
    ORDER BY source_file_id DESC NULLS LAST
    LIMIT 25
    """
)

_OWNER_RESOLVE_SQL = text(
    """
    SELECT folio, strap, match_score
    FROM resolve_property_by_name(
        CAST(:name AS text),
        CAST(NULL AS text),
        CAST(:threshold AS real)
    )
    ORDER BY match_score DESC
    LIMIT :limit
    """
)

_PARCEL_LEGAL_SQL = text(
    """
    SELECT
        folio, strap, property_address,
        raw_legal1, raw_legal2, raw_legal3, raw_legal4,
        source_file_id
    FROM hcpa_bulk_parcels
    WHERE (:folio IS NOT NULL AND folio = :folio)
       OR (:strap IS NOT NULL AND strap = :strap)
    ORDER BY source_file_id DESC NULLS LAST
    LIMIT 1
    """
)

_UPDATE_FORECLOSURE_SQL = text(
    """
    UPDATE foreclosures f
    SET
        strap = COALESCE(f.strap, :strap),
        folio = COALESCE(f.folio, :folio),
        property_address = COALESCE(f.property_address, :property_address)
    WHERE f.foreclosure_id = :foreclosure_id
    RETURNING f.strap, f.folio
    """
)


@dataclass(slots=True)
class _ParcelCandidate:
    folio: str | None
    strap: str | None
    property_address: str | None
    legal_description: str
    source_file_id: int | None


@dataclass(slots=True)
class _ResolutionDecision:
    candidate: _ParcelCandidate | None
    method: str | None
    ambiguous: bool
    reason: str


class PgForeclosureIdentifierRecoveryService:
    """Recover missing strap/folio values from final-judgment extraction data."""

    def __init__(self, dsn: str | None = None) -> None:
        self._available = False
        self._run_stats: dict[str, int] = {}
        self._dsn = resolve_pg_dsn(dsn)
        try:
            self._engine = get_engine(self._dsn)
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1 FROM foreclosures LIMIT 0"))
                conn.execute(text("SELECT 1 FROM hcpa_bulk_parcels LIMIT 0"))
                conn.execute(text("SELECT 1 FROM resolve_property_by_name('x', NULL, 0.3) LIMIT 0"))
            self._ori_session = requests.Session()
            self._ori_session.headers.update(_ORI_DOC_HEADERS)
            try:
                # Seed ORI session cookies for DocumentSearch endpoint.
                bootstrap_response = self._ori_session.get(
                    "https://publicaccess.hillsclerk.com/oripublicaccess/",
                    timeout=20,
                )
                if bootstrap_response.status_code != 200:
                    logger.warning(
                        "ORI session bootstrap HTTP {} (continuing)",
                        bootstrap_response.status_code,
                    )
            except requests.RequestException as exc:
                logger.warning("ORI session bootstrap failed: {}", exc)
            self._available = True
            logger.info("PgForeclosureIdentifierRecoveryService connected")
        except Exception as exc:
            self._engine = None  # type: ignore[assignment]
            self._ori_session = None
            logger.warning(
                "PgForeclosureIdentifierRecoveryService unavailable: {}",
                exc,
            )

    @property
    def available(self) -> bool:
        return self._available

    def run(self, *, limit: int | None = None) -> dict[str, Any]:
        if not self._available:
            return {"skipped": True, "reason": "service_unavailable"}

        stats: dict[str, Any] = {
            "rows_scanned": 0,
            "rows_updated": 0,
            "rows_still_missing_after_update": 0,
            "resolved_parcel_id": 0,
            "resolved_parcel_id_legal_disambiguated": 0,
            "resolved_ori_instrument_legal": 0,
            "resolved_ori_case_legal": 0,
            "resolved_ori_owner_cross_party": 0,
            "resolved_legal_description": 0,
            "resolved_address_plus_legal": 0,
            "ambiguous": 0,
            "unresolved": 0,
            "errors": 0,
            "unresolved_samples": [],
        }
        self._run_stats = {
            "ori_instrument_queries": 0,
            "ori_case_queries": 0,
            "ori_http_errors": 0,
            "ori_payload_errors": 0,
        }

        with self._engine.begin() as conn:
            scope_rows = self._load_scope_rows(conn, limit=limit)
            if not scope_rows:
                return {"skipped": True, "reason": "no_missing_identifiers"}

            logger.info(
                "Identifier recovery: evaluating {} unresolved foreclosures",
                len(scope_rows),
            )

            for row in scope_rows:
                stats["rows_scanned"] += 1
                case_number = row.get("case_number_raw") or "<unknown>"
                try:
                    decision = self._resolve_one(conn, row)
                except Exception as exc:
                    stats["errors"] += 1
                    logger.opt(exception=True).error(
                        "Identifier recovery failed for case {}: {}",
                        case_number,
                        exc,
                    )
                    self._append_unresolved_sample(
                        stats=stats,
                        case_number=case_number,
                        reason=f"error: {exc}",
                    )
                    continue

                if not decision.candidate or not decision.method:
                    if decision.ambiguous:
                        stats["ambiguous"] += 1
                    else:
                        stats["unresolved"] += 1
                    self._append_unresolved_sample(
                        stats=stats,
                        case_number=case_number,
                        reason=decision.reason,
                    )
                    continue

                updated = conn.execute(
                    _UPDATE_FORECLOSURE_SQL,
                    {
                        "foreclosure_id": row["foreclosure_id"],
                        "strap": decision.candidate.strap,
                        "folio": decision.candidate.folio,
                        "property_address": decision.candidate.property_address,
                    },
                ).mappings().fetchone()

                if not updated:
                    stats["unresolved"] += 1
                    self._append_unresolved_sample(
                        stats=stats,
                        case_number=case_number,
                        reason="update_returned_no_row",
                    )
                    continue

                stats["rows_updated"] += 1
                stats[decision.method] = (stats.get(decision.method, 0) or 0) + 1

                if not updated.get("strap") or not updated.get("folio"):
                    stats["rows_still_missing_after_update"] += 1
                    self._append_unresolved_sample(
                        stats=stats,
                        case_number=case_number,
                        reason="updated_but_missing_identifier",
                    )

        if stats["ambiguous"] or stats["unresolved"]:
            logger.warning(
                "Identifier recovery unresolved cases: ambiguous={}, unresolved={}",
                stats["ambiguous"],
                stats["unresolved"],
            )
        if stats["errors"]:
            logger.error(
                "Identifier recovery encountered {} processing errors",
                stats["errors"],
            )

        logger.info(
            "Identifier recovery summary: scanned={}, updated={}, ambiguous={}, unresolved={}, "
            "errors={}, ori_case_queries={}, ori_instrument_queries={}, "
            "ori_http_errors={}, ori_payload_errors={}",
            stats["rows_scanned"],
            stats["rows_updated"],
            stats["ambiguous"],
            stats["unresolved"],
            stats["errors"],
            self._run_stats.get("ori_case_queries", 0),
            self._run_stats.get("ori_instrument_queries", 0),
            self._run_stats.get("ori_http_errors", 0),
            self._run_stats.get("ori_payload_errors", 0),
        )

        stats["success"] = stats["errors"] == 0
        if stats["errors"] > 0:
            stats["error"] = (
                f"{stats['errors']} case-level errors during identifier recovery"
            )
        stats.update(self._run_stats)
        return stats

    def _load_scope_rows(
        self,
        conn: Connection,
        *,
        limit: int | None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        sql = _SCOPE_SQL
        if limit is not None and limit > 0:
            sql += "\nLIMIT :limit"
            params["limit"] = limit
        rows = conn.execute(text(sql), params).mappings().fetchall()
        return [dict(row) for row in rows]

    def _resolve_one(
        self,
        conn: Connection,
        row: dict[str, Any],
    ) -> _ResolutionDecision:
        judgment_legal = _clean_text(row.get("jd_legal_description"))
        case_number = _clean_text(row.get("case_number_raw")) or ""
        parcel_tokens = _parcel_tokens(row.get("jd_parcel_id"))
        saw_ambiguous = False
        ambiguous_reason: str | None = None

        if parcel_tokens:
            parcel_candidates = self._lookup_by_parcel_tokens(conn, parcel_tokens)
            if parcel_candidates:
                if len(parcel_candidates) == 1:
                    return _ResolutionDecision(
                        candidate=parcel_candidates[0],
                        method="resolved_parcel_id",
                        ambiguous=False,
                        reason="parcel_id_exact",
                    )
                picked = self._pick_single_legal_match(
                    judgment_legal=judgment_legal,
                    candidates=parcel_candidates,
                    threshold=0.78,
                )
                if picked.candidate:
                    return _ResolutionDecision(
                        candidate=picked.candidate,
                        method="resolved_parcel_id_legal_disambiguated",
                        ambiguous=False,
                        reason=picked.reason,
                    )
                if picked.ambiguous:
                    return _ResolutionDecision(
                        candidate=None,
                        method=None,
                        ambiguous=True,
                        reason="parcel_id_candidates_ambiguous",
                    )

        instrument = _extract_instrument_from_pdf_path(row.get("pdf_path"))
        if instrument:
            ori_inst_docs = self._ori_search_instrument(instrument)
            instrument_legal = _first_useful_legal(ori_inst_docs)
            if instrument_legal:
                via_instrument = self._resolve_by_ori_legal(
                    conn,
                    legal_text=instrument_legal,
                    method="resolved_ori_instrument_legal",
                )
                if via_instrument.candidate and via_instrument.method:
                    return via_instrument
                if via_instrument.ambiguous:
                    saw_ambiguous = True
                    ambiguous_reason = ambiguous_reason or via_instrument.reason

        case_docs = self._ori_search_case(case_number) if case_number else []
        if case_docs:
            for doc in case_docs:
                legal_text = _clean_text(doc.get("Legal") or doc.get("legal"))
                if not _is_useful_case_legal(legal_text):
                    continue
                if legal_text is None:
                    continue

                via_case_legal = self._resolve_by_ori_legal(
                    conn,
                    legal_text=legal_text,
                    method="resolved_ori_case_legal",
                )
                if via_case_legal.candidate and via_case_legal.method:
                    return via_case_legal
                if via_case_legal.ambiguous:
                    saw_ambiguous = True
                    ambiguous_reason = ambiguous_reason or via_case_legal.reason

            via_owner = self._resolve_by_ori_owner_cross_party(
                conn,
                case_docs=case_docs,
                judgment_legal=judgment_legal,
            )
            if via_owner.candidate and via_owner.method:
                return via_owner
            if via_owner.ambiguous:
                saw_ambiguous = True
                ambiguous_reason = ambiguous_reason or via_owner.reason

        legal_candidates = self._lookup_by_legal_description(conn, row=row)
        if legal_candidates and judgment_legal:
            picked = self._pick_single_legal_match(
                judgment_legal=judgment_legal,
                candidates=legal_candidates,
                threshold=0.78,
            )
            if picked.candidate:
                return _ResolutionDecision(
                    candidate=picked.candidate,
                    method="resolved_legal_description",
                    ambiguous=False,
                    reason=picked.reason,
                )
            if picked.ambiguous:
                return _ResolutionDecision(
                    candidate=None,
                    method=None,
                    ambiguous=True,
                    reason="legal_candidates_ambiguous",
                )

        if judgment_legal:
            for address_source in ("property_address", "jd_property_address"):
                address = _address_head(row.get(address_source))
                if not address:
                    continue
                address_candidates = self._lookup_by_address(conn, address=address)
                if not address_candidates:
                    continue
                picked = self._pick_single_legal_match(
                    judgment_legal=judgment_legal,
                    candidates=address_candidates,
                    threshold=0.80,
                )
                if picked.candidate:
                    return _ResolutionDecision(
                        candidate=picked.candidate,
                        method="resolved_address_plus_legal",
                        ambiguous=False,
                        reason=f"{address_source}_plus_legal",
                    )
                if picked.ambiguous:
                    return _ResolutionDecision(
                        candidate=None,
                        method=None,
                        ambiguous=True,
                        reason=f"{address_source}_ambiguous",
                    )

        if saw_ambiguous:
            return _ResolutionDecision(
                candidate=None,
                method=None,
                ambiguous=True,
                reason=ambiguous_reason or "ori_ambiguous",
            )

        return _ResolutionDecision(
            candidate=None,
            method=None,
            ambiguous=False,
            reason="no_match",
        )

    def _resolve_by_ori_legal(
        self,
        conn: Connection,
        *,
        legal_text: str,
        method: str,
    ) -> _ResolutionDecision:
        lot, block = _parse_lot_block(legal_text)
        if lot and block:
            sub_seed = _extract_subdivision_seed(legal_text)
            lot_block_candidates = self._lookup_by_ori_lot_block(
                conn,
                lot=lot,
                block=block,
                sub_seed=sub_seed,
            )
            if lot_block_candidates:
                picked = self._pick_single_legal_match(
                    judgment_legal=legal_text,
                    candidates=lot_block_candidates,
                    threshold=0.75,
                )
                if picked.candidate:
                    return _ResolutionDecision(
                        candidate=picked.candidate,
                        method=method,
                        ambiguous=False,
                        reason=f"{method}_lot_block:{picked.reason}",
                    )
                if picked.ambiguous:
                    return _ResolutionDecision(
                        candidate=None,
                        method=None,
                        ambiguous=True,
                        reason=f"{method}_lot_block_ambiguous",
                    )

        if "UNIT" in legal_text.upper():
            condo_candidates = self._lookup_by_ori_condo(conn, legal_text=legal_text)
            if condo_candidates:
                picked = self._pick_single_legal_match(
                    judgment_legal=legal_text,
                    candidates=condo_candidates,
                    threshold=0.75,
                )
                if picked.candidate:
                    return _ResolutionDecision(
                        candidate=picked.candidate,
                        method=method,
                        ambiguous=False,
                        reason=f"{method}_condo:{picked.reason}",
                    )
                if picked.ambiguous:
                    return _ResolutionDecision(
                        candidate=None,
                        method=None,
                        ambiguous=True,
                        reason=f"{method}_condo_ambiguous",
                    )

        return _ResolutionDecision(
            candidate=None,
            method=None,
            ambiguous=False,
            reason=f"{method}_no_match",
        )

    def _resolve_by_ori_owner_cross_party(
        self,
        conn: Connection,
        *,
        case_docs: list[dict[str, Any]],
        judgment_legal: str | None,
    ) -> _ResolutionDecision:
        party_names = _extract_party_two_individuals(case_docs)
        if len(party_names) < 2:
            return _ResolutionDecision(
                candidate=None,
                method=None,
                ambiguous=False,
                reason="ori_owner_not_enough_individuals",
            )

        evidence: dict[tuple[str | None, str | None], dict[str, Any]] = {}
        for party_name in party_names[:_MAX_OWNER_NAMES]:
            for variant in _owner_name_variants(party_name):
                rows = conn.execute(
                    _OWNER_RESOLVE_SQL,
                    {
                        "name": variant,
                        "threshold": _OWNER_MATCH_THRESHOLD,
                        "limit": _MAX_OWNER_MATCHES,
                    },
                ).mappings().fetchall()
                for row in rows:
                    key = (_clean_text(row.get("folio")), _clean_text(row.get("strap")))
                    if not key[0] and not key[1]:
                        continue
                    if key not in evidence:
                        evidence[key] = {
                            "folio": key[0],
                            "strap": key[1],
                            "names": set(),
                            "best_party": 0.0,
                            "candidate": None,
                            "legal_confidence": 0.0,
                        }
                    evidence[key]["names"].add(party_name)
                    score = float(row.get("match_score") or 0.0)
                    evidence[key]["best_party"] = max(
                        evidence[key]["best_party"],
                        score,
                    )
                    if evidence[key]["candidate"] is None:
                        evidence[key]["candidate"] = self._load_parcel_candidate(
                            conn,
                            folio=key[0],
                            strap=key[1],
                        )

        overlaps = [
            v for v in evidence.values()
            if len(v["names"]) >= 2 and v["candidate"] is not None
        ]
        if not overlaps:
            return _ResolutionDecision(
                candidate=None,
                method=None,
                ambiguous=False,
                reason="ori_owner_no_overlap",
            )

        qualified: list[tuple[float, dict[str, Any]]] = []
        for item in overlaps:
            candidate = item["candidate"]
            if not isinstance(candidate, _ParcelCandidate):
                continue

            legal_confidence = 0.0
            if judgment_legal and candidate.legal_description:
                matched, confidence, _ = legal_descriptions_match(
                    judgment_legal,
                    candidate.legal_description,
                    threshold=0.78,
                )
                if matched:
                    legal_confidence = confidence
            item["legal_confidence"] = legal_confidence

            if (
                legal_confidence >= _OWNER_LEGAL_CONFIRM_THRESHOLD
                and item["best_party"] >= _OWNER_MATCH_THRESHOLD
            ):
                score = (
                    legal_confidence,
                    float(len(item["names"])),
                    item["best_party"],
                )
                qualified.append((score[0] + score[2], item))

        if len(qualified) == 1:
            chosen = qualified[0][1]
            return _ResolutionDecision(
                candidate=chosen["candidate"],
                method="resolved_ori_owner_cross_party",
                ambiguous=False,
                reason=(
                    "ori_owner_cross_party:"
                    f"names={len(chosen['names'])},"
                    f"party={chosen['best_party']:.2f},"
                    f"legal={chosen['legal_confidence']:.2f}"
                ),
            )
        if len(qualified) > 1:
            return _ResolutionDecision(
                candidate=None,
                method=None,
                ambiguous=True,
                reason="ori_owner_cross_party_ambiguous",
            )

        return _ResolutionDecision(
            candidate=None,
            method=None,
            ambiguous=False,
            reason="ori_owner_cross_party_no_qualified_match",
        )

    def _lookup_by_ori_lot_block(
        self,
        conn: Connection,
        *,
        lot: str,
        block: str,
        sub_seed: str | None,
    ) -> list[_ParcelCandidate]:
        if not sub_seed:
            return []
        rows = conn.execute(
            _ORI_LOT_BLOCK_SQL,
            {
                "lot_block": f"LOT {lot} BLOCK {block}%",
                "sub_seed": f"%{sub_seed}%",
            },
        ).mappings().fetchall()
        return _unique_candidates(list(rows))

    def _lookup_by_ori_condo(
        self,
        conn: Connection,
        *,
        legal_text: str,
    ) -> list[_ParcelCandidate]:
        unit_match = re.search(r"\bUNIT\s+([\w/-]+)\b", legal_text.upper())
        if not unit_match:
            return []
        unit = unit_match.group(1)
        building_match = re.search(r"\bBLDG\s+([\w/-]+)\b", legal_text.upper())
        building = building_match.group(1) if building_match else None
        seed = _condo_seed(legal_text)
        if not seed:
            return []

        unit_pattern = (
            f"UNIT {unit}%" if not building
            else f"UNIT {unit}%BLDG {building}%"
        )
        rows = conn.execute(
            _ORI_CONDO_SQL,
            {"unit_pattern": unit_pattern, "sub_seed": f"%{seed}%"},
        ).mappings().fetchall()
        return _unique_candidates(list(rows))

    def _load_parcel_candidate(
        self,
        conn: Connection,
        *,
        folio: str | None,
        strap: str | None,
    ) -> _ParcelCandidate | None:
        row = conn.execute(
            _PARCEL_LEGAL_SQL,
            {"folio": folio, "strap": strap},
        ).mappings().fetchone()
        if not row:
            return None
        return _row_to_candidate(row)

    def _ori_search_instrument(
        self,
        instrument: str,
    ) -> list[dict[str, Any]]:
        if self._ori_session is None:
            return []
        self._run_stats["ori_instrument_queries"] = (
            self._run_stats.get("ori_instrument_queries", 0) + 1
        )

        payload = {
            "QueryID": 320,
            "Keywords": [{"Id": 1006, "Value": instrument}],
            "QueryLimit": 500,
        }
        try:
            response = self._ori_session.post(
                _PAV_KEYWORD_URL,
                json=payload,
                headers=_PAV_HEADERS,
                timeout=30,
            )
        except requests.RequestException as exc:
            self._run_stats["ori_http_errors"] = (
                self._run_stats.get("ori_http_errors", 0) + 1
            )
            logger.warning("ORI instrument query failed {}: {}", instrument, exc)
            return []

        if response.status_code != 200:
            self._run_stats["ori_http_errors"] = (
                self._run_stats.get("ori_http_errors", 0) + 1
            )
            logger.warning(
                "ORI instrument query HTTP {} for {}",
                response.status_code,
                instrument,
            )
            return []
        try:
            data = response.json()
        except ValueError as exc:
            self._run_stats["ori_http_errors"] = (
                self._run_stats.get("ori_http_errors", 0) + 1
            )
            logger.warning(
                "ORI instrument JSON decode failed for {}: {}",
                instrument,
                exc,
            )
            return []

        results: list[dict[str, Any]] = []
        data_rows = data.get("Data")
        if not isinstance(data_rows, list):
            self._run_stats["ori_payload_errors"] = (
                self._run_stats.get("ori_payload_errors", 0) + 1
            )
            logger.warning(
                "ORI instrument payload missing Data list for {}",
                instrument,
            )
            return []
        for item in data_rows:
            if not isinstance(item, dict):
                self._run_stats["ori_payload_errors"] = (
                    self._run_stats.get("ori_payload_errors", 0) + 1
                )
                continue
            cols = item.get("DisplayColumnValues") or []
            values = [str((col or {}).get("Value") or "").strip() for col in cols[:9]]
            values.extend([""] * (9 - len(values)))
            results.append(
                {
                    "person_type": values[0],
                    "name": html.unescape(values[1]),
                    "record_date": values[2],
                    "doc_type": values[3],
                    "book_type": values[4],
                    "book_num": values[5],
                    "page_num": values[6],
                    "legal": values[7],
                    "instrument": values[8],
                }
            )
        return results

    def _ori_search_case(
        self,
        case_number: str,
    ) -> list[dict[str, Any]]:
        if self._ori_session is None or not case_number:
            return []
        self._run_stats["ori_case_queries"] = (
            self._run_stats.get("ori_case_queries", 0) + 1
        )
        payload = {"CaseNum": case_number.strip()}
        try:
            response = self._ori_session.post(
                _ORI_DOC_SEARCH_URL,
                json=payload,
                headers=_ORI_DOC_HEADERS,
                timeout=45,
            )
        except requests.RequestException as exc:
            self._run_stats["ori_http_errors"] = (
                self._run_stats.get("ori_http_errors", 0) + 1
            )
            logger.warning("ORI case query failed {}: {}", case_number, exc)
            return []

        if response.status_code != 200:
            self._run_stats["ori_http_errors"] = (
                self._run_stats.get("ori_http_errors", 0) + 1
            )
            logger.warning(
                "ORI case query HTTP {} for {}",
                response.status_code,
                case_number,
            )
            return []

        try:
            data = response.json()
        except ValueError as exc:
            self._run_stats["ori_http_errors"] = (
                self._run_stats.get("ori_http_errors", 0) + 1
            )
            logger.warning(
                "ORI case JSON decode failed for {}: {}",
                case_number,
                exc,
            )
            return []
        results: list[dict[str, Any]] = []
        result_rows = data.get("ResultList")
        if not isinstance(result_rows, list):
            self._run_stats["ori_payload_errors"] = (
                self._run_stats.get("ori_payload_errors", 0) + 1
            )
            logger.warning(
                "ORI case payload missing ResultList for {}",
                case_number,
            )
            return []
        for row in result_rows:
            if not isinstance(row, dict):
                self._run_stats["ori_payload_errors"] = (
                    self._run_stats.get("ori_payload_errors", 0) + 1
                )
                continue
            parties_one_raw = row.get("PartiesOne")
            parties_two_raw = row.get("PartiesTwo")
            parties_one = parties_one_raw if isinstance(parties_one_raw, list) else []
            parties_two = parties_two_raw if isinstance(parties_two_raw, list) else []
            if not isinstance(parties_one_raw, list) or not isinstance(parties_two_raw, list):
                self._run_stats["ori_payload_errors"] = (
                    self._run_stats.get("ori_payload_errors", 0) + 1
                )
            results.append(
                {
                    "Instrument": str(row.get("Instrument") or "").strip(),
                    "PartiesOne": [html.unescape(str(v)) for v in parties_one if v],
                    "PartiesTwo": [html.unescape(str(v)) for v in parties_two if v],
                    "RecordDate": row.get("RecordDate"),
                    "DocType": str(row.get("DocType") or "").strip(),
                    "BookType": str(row.get("BookType") or "").strip(),
                    "BookNum": str(row.get("BookNum") or "").strip(),
                    "PageNum": str(row.get("PageNum") or "").strip(),
                    "Legal": str(row.get("Legal") or "").strip(),
                }
            )
        return results

    def _lookup_by_parcel_tokens(
        self,
        conn: Connection,
        tokens: list[str],
    ) -> list[_ParcelCandidate]:
        candidates: dict[tuple[str | None, str | None], _ParcelCandidate] = {}

        for token in tokens:
            rows = conn.execute(_PARCEL_LOOKUP_SQL, {"value": token}).mappings().fetchall()
            for row in rows:
                candidate = _row_to_candidate(row)
                candidates[(candidate.folio, candidate.strap)] = candidate

            digits = re.sub(r"\D", "", token)
            if not digits:
                continue
            rows = conn.execute(
                _PARCEL_LOOKUP_DIGITS_SQL,
                {"value": digits},
            ).mappings().fetchall()
            for row in rows:
                candidate = _row_to_candidate(row)
                candidates[(candidate.folio, candidate.strap)] = candidate

        return list(candidates.values())

    def _lookup_by_address(
        self,
        conn: Connection,
        *,
        address: str,
    ) -> list[_ParcelCandidate]:
        rows = conn.execute(
            _ADDRESS_LOOKUP_SQL,
            {"address": address, "limit": _MAX_CANDIDATES_ADDRESS},
        ).mappings().fetchall()
        return [_row_to_candidate(row) for row in rows]

    def _lookup_by_legal_description(
        self,
        conn: Connection,
        *,
        row: dict[str, Any],
    ) -> list[_ParcelCandidate]:
        judgment_legal = _clean_text(row.get("jd_legal_description"))
        if not judgment_legal:
            return []

        parsed = parse_legal_description(judgment_legal)
        subdivision = _clean_text(row.get("jd_subdivision")) or _clean_text(
            parsed.subdivision
        )
        lot = _clean_text(row.get("jd_lot")) or _clean_text(parsed.lot)
        block = _clean_text(row.get("jd_block")) or _clean_text(parsed.block)
        unit = _clean_text(row.get("jd_unit")) or _clean_text(parsed.unit)
        plat_book = _clean_text(row.get("jd_plat_book")) or _clean_text(parsed.plat_book)
        plat_page = _clean_text(row.get("jd_plat_page")) or _clean_text(parsed.plat_page)

        clauses: list[str] = []
        params: dict[str, Any] = {"limit": _MAX_CANDIDATES_LEGAL}

        subdiv_terms = _subdivision_terms(subdivision)
        if subdiv_terms:
            for index, term in enumerate(subdiv_terms):
                key = f"sub_term_{index}"
                clauses.append(f"{_LEGAL_EXPR} LIKE :{key}")
                params[key] = f"%{term}%"

        if lot:
            clauses.append(f"{_LEGAL_EXPR} ~* :lot_regex")
            params["lot_regex"] = (
                rf"(^|[^A-Z0-9])(LOT|L|LT)\s*{re.escape(lot.upper())}([^A-Z0-9]|$)"
            )

        if block:
            clauses.append(f"{_LEGAL_EXPR} ~* :block_regex")
            params["block_regex"] = (
                rf"(^|[^A-Z0-9])(BLOCK|BLK|B)\s*{re.escape(block.upper())}"
                r"([^A-Z0-9]|$)"
            )

        if unit:
            clauses.append(f"{_LEGAL_EXPR} ~* :unit_regex")
            params["unit_regex"] = (
                rf"(^|[^A-Z0-9])(UNIT|U|UN)\s*{re.escape(unit.upper())}"
                r"([^A-Z0-9]|$)"
            )

        if plat_book and plat_page:
            clauses.append(f"{_LEGAL_EXPR} ~* :plat_regex")
            params["plat_regex"] = (
                r"PLAT\s*(BOOK|BK)?\s*0*"
                + re.escape(plat_book.upper())
                + r"\s*(PAGE|PG|P)?\s*0*"
                + re.escape(plat_page.upper())
            )

        if not clauses:
            return []

        sql = text(
            f"""
            SELECT
                folio, strap, property_address,
                raw_legal1, raw_legal2, raw_legal3, raw_legal4,
                source_file_id
            FROM hcpa_bulk_parcels
            WHERE {' AND '.join(clauses)}
            ORDER BY source_file_id DESC NULLS LAST
            LIMIT :limit
            """
        )

        rows = conn.execute(sql, params).mappings().fetchall()
        return [_row_to_candidate(row) for row in rows]

    @staticmethod
    def _pick_single_legal_match(
        *,
        judgment_legal: str | None,
        candidates: list[_ParcelCandidate],
        threshold: float,
    ) -> _ResolutionDecision:
        if not judgment_legal:
            return _ResolutionDecision(
                candidate=None,
                method=None,
                ambiguous=False,
                reason="missing_judgment_legal_description",
            )

        scored: list[tuple[float, _ParcelCandidate, str]] = []
        for candidate in candidates:
            if not candidate.legal_description:
                continue
            matched, confidence, reason = legal_descriptions_match(
                judgment_legal,
                candidate.legal_description,
                threshold=threshold,
            )
            if matched:
                scored.append((confidence, candidate, reason))

        if not scored:
            return _ResolutionDecision(
                candidate=None,
                method=None,
                ambiguous=False,
                reason="no_legal_match",
            )

        scored.sort(key=lambda item: item[0], reverse=True)
        if len(scored) == 1:
            return _ResolutionDecision(
                candidate=scored[0][1],
                method=None,
                ambiguous=False,
                reason=scored[0][2],
            )

        top_score, top_candidate, top_reason = scored[0]
        second_score = scored[1][0]
        if top_score >= 0.92 and (top_score - second_score) >= 0.08:
            return _ResolutionDecision(
                candidate=top_candidate,
                method=None,
                ambiguous=False,
                reason=top_reason,
            )

        return _ResolutionDecision(
            candidate=None,
            method=None,
            ambiguous=True,
            reason="multiple_legal_matches",
        )

    @staticmethod
    def _append_unresolved_sample(
        *,
        stats: dict[str, Any],
        case_number: str,
        reason: str,
    ) -> None:
        samples = stats.get("unresolved_samples")
        if not isinstance(samples, list):
            return
        if len(samples) >= _UNRESOLVED_SAMPLE_LIMIT:
            return
        samples.append({"case_number": case_number, "reason": reason})


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value if text_value else None


def _address_head(value: Any) -> str | None:
    cleaned = _clean_text(value)
    if not cleaned:
        return None
    first = cleaned.replace("\t", " ").split(",", 1)[0].strip()
    return first.upper() if first else None


def _parcel_tokens(value: Any) -> list[str]:
    cleaned = _clean_text(value)
    if not cleaned:
        return []
    upper = cleaned.upper().strip()
    if upper in _PLACEHOLDER_PARCEL_IDS:
        return []
    candidates = [upper, re.sub(r"\s+", "", upper)]
    for part in re.split(r"\bAND\b|,|;", upper):
        token = part.strip()
        if not token or token in _PLACEHOLDER_PARCEL_IDS:
            continue
        candidates.append(token)
        candidates.append(re.sub(r"[^A-Z0-9]", "", token))

        converted = _hcpa_strap_from_segmented_parcel(token)
        if converted:
            candidates.append(converted)

    digits = re.sub(r"\D", "", upper)
    if len(digits) >= 8:
        candidates.append(digits)

    result: list[str] = []
    seen: set[str] = set()
    for token in candidates:
        token = token.strip()
        if not token or token in seen:
            continue
        seen.add(token)
        result.append(token)
    return result


def _extract_instrument_from_pdf_path(pdf_path: Any) -> str | None:
    path_value = _clean_text(pdf_path)
    if not path_value:
        return None
    stem = re.sub(r"\.pdf$", "", path_value, flags=re.IGNORECASE).split("/")[-1]
    match = _INSTRUMENT_FROM_STEM_RE.search(stem)
    if not match:
        return None
    return match.group(1)


def _first_useful_legal(rows: list[dict[str, Any]]) -> str | None:
    for row in rows:
        legal = _clean_text(row.get("legal") or row.get("Legal"))
        if _is_useful_case_legal(legal):
            return legal
    return None


def _looks_like_case_number(value: str | None) -> bool:
    text_value = _clean_text(value)
    if not text_value:
        return False
    normalized = re.sub(r"[^A-Z0-9]", " ", text_value.upper())
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return bool(_CASE_NUM_RE.match(normalized))


def _is_useful_case_legal(value: str | None) -> bool:
    legal = _clean_text(value)
    if not legal:
        return False
    if _looks_like_case_number(legal):
        return False
    return len(legal) >= 12


def _parse_lot_block(legal: str) -> tuple[str | None, str | None]:
    source = legal.upper()
    lot_match = re.search(r"\b(?:LOT|L)\s+([A-Z]?\d+[A-Z]?)\b", source)
    block_match = re.search(r"\b(?:BLOCK|BLK|B)\s+([A-Z]?\d+[A-Z]?)\b", source)
    return (
        lot_match.group(1) if lot_match else None,
        block_match.group(1) if block_match else None,
    )


def _extract_subdivision_seed(legal: str) -> str | None:
    source = legal.upper()
    source = re.sub(r"\b(?:LOT|L)\s+[A-Z]?\d+[A-Z]?\b", " ", source)
    source = re.sub(r"\b(?:BLOCK|BLK|B)\s+[A-Z]?\d+[A-Z]?\b", " ", source)
    source = re.sub(r"\bACCORDING\b.*", " ", source)
    source = re.sub(r"\bOF\b", " ", source)
    tokens = [
        token for token in source.split()
        if len(token) > 2 and token not in {",", "AND", "THE", "A", "AS"}
    ]
    if not tokens:
        return None
    return " ".join(tokens[:3])


def _condo_seed(legal: str) -> str | None:
    source = legal.upper()
    source = re.sub(r"\bUNIT\s+[\w/-]+\b", " ", source)
    source = re.sub(r"\bBLDG\s+[\w/-]+\b", " ", source)
    tokens = [
        token for token in source.split()
        if len(token) > 2 and token not in {",", "AND", "THE", "OF", "A"}
    ]
    if not tokens:
        return None
    return " ".join(tokens[:3])


def _is_entity_name(name: str) -> bool:
    upper_name = name.upper()
    return any(keyword in upper_name for keyword in _ENTITY_KEYWORDS)


def _extract_party_two_individuals(case_docs: list[dict[str, Any]]) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for doc in case_docs:
        for raw_name in doc.get("PartiesTwo") or []:
            clean_name = _clean_text(raw_name)
            if not clean_name:
                continue
            if _is_entity_name(clean_name):
                continue
            if clean_name in seen:
                continue
            seen.add(clean_name)
            names.append(clean_name)
    return names


def _owner_name_variants(name: str) -> list[str]:
    base = _clean_text(name)
    if not base:
        return []
    parts = base.split()
    variants = [base]
    if len(parts) >= 2:
        variants.append(f"{parts[-1]} {' '.join(parts[:-1])}")
        variants.append(f"{' '.join(parts[1:])} {parts[0]}")
    deduped: list[str] = []
    seen: set[str] = set()
    for variant in variants:
        v = variant.strip()
        if not v or v in seen:
            continue
        seen.add(v)
        deduped.append(v)
    return deduped


def _unique_candidates(rows: list[Any]) -> list[_ParcelCandidate]:
    deduped: dict[tuple[str | None, str | None], _ParcelCandidate] = {}
    for row in rows:
        candidate = _row_to_candidate(row)
        deduped[(candidate.folio, candidate.strap)] = candidate
    return list(deduped.values())


def _hcpa_strap_from_segmented_parcel(parcel_id: str) -> str | None:
    token = parcel_id.strip().upper()
    match = _PARCEL_SEGMENT_RE.match(token)
    if not match:
        return None
    seg1, seg2, seg3, seg4, seg5, seg6, decimal = match.groups()
    return f"{seg3}{seg2}{seg1}{seg4}{seg5}{seg6}{decimal}U"


def _subdivision_terms(subdivision: str | None) -> list[str]:
    if not subdivision:
        return []
    words = re.findall(r"[A-Z0-9]+", subdivision.upper())
    terms: list[str] = []
    for word in words:
        if len(word) < 4 or word in _GENERIC_SUBDIVISION_TERMS:
            continue
        if word not in terms:
            terms.append(word)
        if len(terms) >= 3:
            break
    return terms


def _row_to_candidate(row: Any) -> _ParcelCandidate:
    legal_description = " ".join(
        part.strip()
        for part in (
            row.get("raw_legal1"),
            row.get("raw_legal2"),
            row.get("raw_legal3"),
            row.get("raw_legal4"),
        )
        if part and str(part).strip()
    )
    return _ParcelCandidate(
        folio=_clean_text(row.get("folio")),
        strap=_clean_text(row.get("strap")),
        property_address=_clean_text(row.get("property_address")),
        legal_description=legal_description,
        source_file_id=row.get("source_file_id"),
    )
