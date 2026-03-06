"""Municipal utility-lien Phase 0 detector (read/write findings only).

Architectural purpose:
    This service closes the first municipal-lien gap by transforming existing
    ORI encumbrance records into normalized provider findings. It does not
    submit provider requests and does not require any external portal/API calls.

How it fits in the pipeline:
    PgPipelineController step ``municipal_liens_phase0`` executes after ORI
    search. The step scans in-scope foreclosures, detects recorded utility-lien
    evidence for Hillsborough Water Resources and Tampa Conduits, and upserts
    rows into ``municipal_lien_findings``.

Scope (Phase 0 only):
    - Provider statuses for:
      - hillsborough_water_resources
      - tampa_conduits
      - teco (policy classification only)
    - Evidence source:
      - ori_detector (recorded ORI evidence)
      - policy (TECO default classification)
    - No provider request queueing, submission, or payoff-letter lifecycle.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

from loguru import logger
from sqlalchemy import text

from sunbiz.db import get_engine, resolve_pg_dsn

PROVIDER_HILLSBOROUGH = "hillsborough_water_resources"
PROVIDER_TAMPA = "tampa_conduits"
PROVIDER_TECO = "teco"

SOURCE_ORI_DETECTOR = "ori_detector"
SOURCE_POLICY = "policy"

STATUS_UNKNOWN = "unknown"
STATUS_LIEN_RECORDED = "lien_recorded"
STATUS_NOT_APPLICABLE = "not_applicable"

_HILLSBOROUGH_DIRECT_TOKENS = (
    "HILLSBOROUGH COUNTY PUBLIC UTILITIES",
    "HILLSBOROUGH COUNTY WATER RESOURCES",
    "HILLSBOROUGH COUNTY UTILITIES",
    "HILLSBOROUGH COUNTY WATER",
)

_TAMPA_DIRECT_TOKENS = (
    "CITY OF TAMPA UTILITIES",
    "CITY OF TAMPA WATER",
    "CITY OF TAMPA WASTEWATER",
    "CITY OF TAMPA STORMWATER",
)

_UTILITY_HINT_TOKENS = (
    "UTILITY",
    "UTILITIES",
    "WATER",
    "WASTEWATER",
    "SEWER",
    "STORMWATER",
    "LIEN",
)

_CONFIDENCE_SCORE = {"low": 1, "medium": 2, "high": 3}


class PgMunicipalLienService:
    """Derive provider-level municipal-lien findings from existing ORI records."""

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = resolve_pg_dsn(dsn)
        self.engine = get_engine(self.dsn)

    def run_phase0(
        self,
        *,
        limit: int | None = None,
        foreclosure_id: int | None = None,
        case_number: str | None = None,
        active_only: bool = True,
    ) -> dict[str, Any]:
        targets = self._load_targets(
            limit=limit,
            foreclosure_id=foreclosure_id,
            case_number=case_number,
            active_only=active_only,
        )
        if not targets:
            return {
                "skipped": True,
                "reason": "no_foreclosures_in_scope",
                "targets": 0,
                "evidence_rows_scanned": 0,
                "findings_written": 0,
                "provider_status_counts": {},
            }

        evidence_rows = self._load_candidate_evidence(targets)
        findings = self._build_findings(targets, evidence_rows)
        findings_written = self._upsert_findings(findings)

        provider_status_counts: dict[str, dict[str, int]] = {}
        lien_recorded_by_provider: dict[str, int] = {
            PROVIDER_HILLSBOROUGH: 0,
            PROVIDER_TAMPA: 0,
            PROVIDER_TECO: 0,
        }
        for row in findings:
            provider = str(row["provider"])
            status = str(row["status"])
            provider_status_counts.setdefault(provider, {})
            provider_status_counts[provider][status] = (
                provider_status_counts[provider].get(status, 0) + 1
            )
            if status == STATUS_LIEN_RECORDED:
                lien_recorded_by_provider[provider] = (
                    lien_recorded_by_provider.get(provider, 0) + 1
                )

        result = {
            "targets": len(targets),
            "evidence_rows_scanned": len(evidence_rows),
            "findings_written": findings_written,
            "provider_status_counts": provider_status_counts,
            "lien_recorded_by_provider": lien_recorded_by_provider,
        }
        logger.info("Municipal lien Phase 0 complete: {}", result)
        return result

    def _load_targets(
        self,
        *,
        limit: int | None,
        foreclosure_id: int | None,
        case_number: str | None,
        active_only: bool,
    ) -> list[dict[str, Any]]:
        where_clauses = ["f.judgment_data IS NOT NULL"]
        params: dict[str, Any] = {}

        if active_only:
            where_clauses.append("f.archived_at IS NULL")
        if foreclosure_id is not None:
            where_clauses.append("f.foreclosure_id = :foreclosure_id")
            params["foreclosure_id"] = int(foreclosure_id)
        if case_number:
            where_clauses.append("f.case_number_raw = :case_number")
            params["case_number"] = str(case_number).strip()

        limit_sql = ""
        if limit is not None and int(limit) > 0:
            limit_sql = "LIMIT :limit"
            params["limit"] = int(limit)

        sql = text(
            f"""
            SELECT f.foreclosure_id,
                   NULLIF(btrim(f.strap), '') AS strap,
                   NULLIF(btrim(f.folio), '') AS folio,
                   f.case_number_raw,
                   f.property_address
            FROM foreclosures f
            WHERE {" AND ".join(where_clauses)}
            ORDER BY f.auction_date DESC NULLS LAST, f.foreclosure_id DESC
            {limit_sql}
            """
        )

        with self.engine.connect() as conn:
            return [dict(r) for r in conn.execute(sql, params).mappings().all()]

    def _load_candidate_evidence(
        self,
        targets: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        straps = sorted({str(t["strap"]).strip() for t in targets if t.get("strap")})
        folios = sorted({str(t["folio"]).strip() for t in targets if t.get("folio")})

        where_parts: list[str] = []
        params: dict[str, Any] = {}
        if straps:
            where_parts.append("oe.strap = ANY(:straps)")
            params["straps"] = straps
        if folios:
            where_parts.append("oe.folio = ANY(:folios)")
            params["folios"] = folios

        if not where_parts:
            return []

        sql = text(
            f"""
            SELECT oe.id,
                   NULLIF(btrim(oe.strap), '') AS strap,
                   NULLIF(btrim(oe.folio), '') AS folio,
                   oe.instrument_number,
                   oe.recording_date,
                   oe.amount,
                   oe.party1,
                   oe.party2,
                   oe.current_holder,
                   oe.legal_description,
                   oe.raw_document_type,
                   oe.encumbrance_type
            FROM ori_encumbrances oe
            WHERE oe.encumbrance_type IN ('lien', 'other', 'judgment')
              AND ({' OR '.join(where_parts)})
            ORDER BY oe.recording_date DESC NULLS LAST, oe.id DESC
            """
        )
        with self.engine.connect() as conn:
            return [dict(r) for r in conn.execute(sql, params).mappings().all()]

    def _build_findings(
        self,
        targets: list[dict[str, Any]],
        evidence_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        by_strap: dict[str, set[int]] = {}
        by_folio: dict[str, set[int]] = {}
        for t in targets:
            foreclosure_id = int(t["foreclosure_id"])
            strap = str(t["strap"]).strip() if t.get("strap") else ""
            folio = str(t["folio"]).strip() if t.get("folio") else ""
            if strap:
                by_strap.setdefault(strap, set()).add(foreclosure_id)
            if folio:
                by_folio.setdefault(folio, set()).add(foreclosure_id)

        best_evidence: dict[tuple[int, str], dict[str, Any]] = {}

        for row in evidence_rows:
            matched_foreclosure_ids: set[int] = set()
            strap = str(row["strap"]).strip() if row.get("strap") else ""
            folio = str(row["folio"]).strip() if row.get("folio") else ""
            if strap and strap in by_strap:
                matched_foreclosure_ids.update(by_strap[strap])
            if folio and folio in by_folio:
                matched_foreclosure_ids.update(by_folio[folio])
            if not matched_foreclosure_ids:
                continue

            haystack = " ".join(
                str(v).strip()
                for v in (
                    row.get("party1"),
                    row.get("party2"),
                    row.get("current_holder"),
                    row.get("raw_document_type"),
                    row.get("legal_description"),
                )
                if v not in (None, "")
            ).upper()

            provider_hits = _classify_provider_hits(haystack)
            if not provider_hits:
                continue

            for foreclosure_id in matched_foreclosure_ids:
                for provider, confidence, reason in provider_hits:
                    candidate = {
                        "provider": provider,
                        "confidence": confidence,
                        "reason": reason,
                        "instrument_number": (
                            str(row.get("instrument_number")).strip()
                            if row.get("instrument_number")
                            else None
                        ),
                        "recording_date": row.get("recording_date"),
                        "amount": row.get("amount"),
                        "raw_json": {
                            "encumbrance_id": row.get("id"),
                            "encumbrance_type": row.get("encumbrance_type"),
                            "matched_on": reason,
                        },
                    }
                    key = (foreclosure_id, provider)
                    current = best_evidence.get(key)
                    if current is None or _candidate_rank(candidate) > _candidate_rank(current):
                        best_evidence[key] = candidate

        findings: list[dict[str, Any]] = []
        for target in targets:
            foreclosure_id = int(target["foreclosure_id"])
            has_property_key = bool(target.get("strap") or target.get("folio"))

            for provider in (PROVIDER_HILLSBOROUGH, PROVIDER_TAMPA):
                evidence = best_evidence.get((foreclosure_id, provider))
                if evidence:
                    findings.append(
                        {
                            "foreclosure_id": foreclosure_id,
                            "provider": provider,
                            "status": STATUS_LIEN_RECORDED,
                            "source": SOURCE_ORI_DETECTOR,
                            "instrument_number": evidence.get("instrument_number"),
                            "amount": evidence.get("amount"),
                            "as_of_date": evidence.get("recording_date"),
                            "confidence": evidence.get("confidence"),
                            "reason": evidence.get("reason"),
                            "raw_json": evidence.get("raw_json"),
                        }
                    )
                else:
                    findings.append(
                        {
                            "foreclosure_id": foreclosure_id,
                            "provider": provider,
                            "status": STATUS_UNKNOWN,
                            "source": SOURCE_ORI_DETECTOR,
                            "instrument_number": None,
                            "amount": None,
                            "as_of_date": None,
                            "confidence": "low",
                            "reason": (
                                "missing_property_identifier_for_ori_lookup"
                                if not has_property_key
                                else "no_recorded_utility_lien_detected"
                            ),
                            "raw_json": None,
                        }
                    )

            findings.append(
                {
                    "foreclosure_id": foreclosure_id,
                    "provider": PROVIDER_TECO,
                    "status": STATUS_NOT_APPLICABLE,
                    "source": SOURCE_POLICY,
                    "instrument_number": None,
                    "amount": None,
                    "as_of_date": None,
                    "confidence": "high",
                    "reason": "teco_electric_default_customer_level",
                    "raw_json": {"policy": "phase0_default_not_property_lien"},
                }
            )
        return findings

    def _upsert_findings(self, findings: list[dict[str, Any]]) -> int:
        if not findings:
            return 0

        # Serialize raw_json dicts to JSON strings for psycopg v3
        serialized = []
        for row in findings:
            row_copy = dict(row)
            rj = row_copy.get("raw_json")
            row_copy["raw_json"] = json.dumps(rj) if rj is not None else None
            serialized.append(row_copy)

        sql = text(
            """
            INSERT INTO municipal_lien_findings (
                foreclosure_id,
                provider,
                status,
                source,
                instrument_number,
                amount,
                as_of_date,
                confidence,
                reason,
                raw_json
            ) VALUES (
                :foreclosure_id,
                :provider,
                :status,
                :source,
                :instrument_number,
                :amount,
                :as_of_date,
                :confidence,
                :reason,
                CAST(:raw_json AS jsonb)
            )
            ON CONFLICT (foreclosure_id, provider, source)
            DO UPDATE SET
                status = EXCLUDED.status,
                instrument_number = EXCLUDED.instrument_number,
                amount = EXCLUDED.amount,
                as_of_date = EXCLUDED.as_of_date,
                confidence = EXCLUDED.confidence,
                reason = EXCLUDED.reason,
                raw_json = EXCLUDED.raw_json,
                updated_at = now()
            """
        )

        with self.engine.begin() as conn:
            result = conn.execute(sql, serialized)
        rowcount = int(result.rowcount or 0)
        return rowcount if rowcount > 0 else len(findings)


def _classify_provider_hits(haystack_upper: str) -> list[tuple[str, str, str]]:
    if not haystack_upper:
        return []

    matches: list[tuple[str, str, str]] = []

    hills_direct = any(token in haystack_upper for token in _HILLSBOROUGH_DIRECT_TOKENS)
    tampa_direct = any(token in haystack_upper for token in _TAMPA_DIRECT_TOKENS)
    has_utility_hint = any(token in haystack_upper for token in _UTILITY_HINT_TOKENS)

    if hills_direct:
        matches.append(
            (
                PROVIDER_HILLSBOROUGH,
                "high",
                "direct_hillsborough_utilities_party_match",
            )
        )
    elif "HILLSBOROUGH COUNTY" in haystack_upper and has_utility_hint:
        matches.append(
            (
                PROVIDER_HILLSBOROUGH,
                "medium",
                "hillsborough_county_plus_utility_terms",
            )
        )

    if tampa_direct:
        matches.append(
            (
                PROVIDER_TAMPA,
                "high",
                "direct_city_tampa_utilities_party_match",
            )
        )
    elif "CITY OF TAMPA" in haystack_upper and has_utility_hint:
        matches.append(
            (
                PROVIDER_TAMPA,
                "medium",
                "city_tampa_plus_utility_terms",
            )
        )

    return matches


def _candidate_rank(candidate: dict[str, Any]) -> tuple[int, dt.date]:
    confidence = str(candidate.get("confidence") or "low").lower()
    score = _CONFIDENCE_SCORE.get(confidence, 1)
    recording_date = candidate.get("recording_date")
    if isinstance(recording_date, dt.datetime):
        date_value = recording_date.date()
    elif isinstance(recording_date, dt.date):
        date_value = recording_date
    else:
        date_value = dt.date.min
    return score, date_value
