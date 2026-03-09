"""PG-only active-foreclosure encumbrance audit report.

Architectural purpose
---------------------
This tool categorises every active foreclosure into diagnostic "buckets" based
on what encumbrance coverage is missing. It combines deterministic SQL buckets
with a read-only LP-to-judgment signal layer that is surfaced only as operator
audit output. No new schema is created and no inferred signals are persisted as
facts.

The bucket taxonomy comes from ``docs/domain/ENCUMBRANCE_AUDIT_BUCKETS.md``.
Each bucket maps to a deterministic SQL query that tests a specific gap
condition against the joined foreclosure/encumbrance/title/clerk data.

Bucket output is consumed by:
  - pipeline operators (console summary, ``--json``, ``--csv``),
  - downstream recovery tools that target specific gap categories,
  - the web audit-intel view (future).

Usage::

    uv run python -m src.services.audit.pg_audit_encumbrance
    uv run python -m src.services.audit.pg_audit_encumbrance --json
    uv run python -m src.tools.pg_encumbrance_audit --csv audit_out.csv
    uv run python -m src.tools.pg_encumbrance_audit --dsn postgresql://...

How it fits into the broader system
------------------------------------
This is a *read-only diagnostic*. It never writes rows. The implementation now
lives under ``src/services/audit`` so the audit logic can be reused by the CLI,
tests, and future web views from one import path.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import json
import sys
from dataclasses import asdict, dataclass, field
from io import StringIO
from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import text

project_root = Path(__file__).resolve().parents[3]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.services.audit.encumbrance_audit_signals import AuditSignal, AuditSignalExtractor  # noqa: E402
from sunbiz.db import get_engine, resolve_pg_dsn  # noqa: E402


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class BucketHit:
    """One foreclosure's membership in one audit bucket."""

    bucket: str
    foreclosure_id: int
    case_number: str
    strap: str | None
    property_address: str | None
    reason: str


@dataclass
class BucketSummary:
    """Aggregate summary for one bucket."""

    bucket: str
    description: str
    count: int
    deferred: bool = False
    deferred_reason: str | None = None


@dataclass
class AuditReport:
    """Full audit result across all buckets."""

    active_count: int
    judged_count: int
    with_strap_count: int
    with_encumbrances_count: int
    with_survival_count: int = 0
    summaries: list[BucketSummary] = field(default_factory=list)
    hits: list[BucketHit] = field(default_factory=list)


# ---------------------------------------------------------------------------
# SQL helpers (same pattern as db_audit.py)
# ---------------------------------------------------------------------------


def _rows(conn: Any, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    return [dict(r) for r in conn.execute(text(query), params or {}).mappings().all()]


def _val(conn: Any, query: str, params: dict[str, Any] | None = None, default: int = 0) -> int:
    try:
        result = conn.execute(text(query), params or {}).scalar()
        return result if result is not None else default
    except Exception:
        conn.rollback()
        logger.exception("Audit scope query failed")
        raise


def _has_table(conn: Any, name: str) -> bool:
    try:
        return bool(
            conn.execute(
                text("SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=:n"),
                {"n": name},
            ).scalar()
        )
    except Exception:
        conn.rollback()
        logger.exception("Audit table-presence check failed for {}", name)
        raise


SIGNAL_BUCKET_DESCRIPTIONS: dict[str, str] = {
    "judgment_joined_party_gap": "Judgment parties missing from encumbrance discovery",
    "judgment_instrument_gap": "Judgment instrument detail missing from encumbrance set",
    "lp_to_judgment_plaintiff_change": "LP plaintiff differs from judgment plaintiff",
    "lp_to_judgment_party_expansion": "Judgment adds parties not present in LP",
    "lp_to_judgment_property_change": "Property description changed between LP filing and judgment",
    "long_case_interim_risk": "Long LP-to-judgment gap without lifecycle evidence",
}


def _load_foreclosure_lookup(
    conn: Any,
    foreclosure_ids: set[int],
) -> dict[int, dict[str, Any]]:
    """Load minimal foreclosure metadata for signal-backed bucket hits."""
    if not foreclosure_ids:
        return {}

    rows = _rows(
        conn,
        """
        SELECT foreclosure_id,
               case_number_raw AS case_number,
               strap,
               property_address
        FROM foreclosures
        WHERE foreclosure_id = ANY(:ids)
        """,
        {"ids": sorted(foreclosure_ids)},
    )
    return {int(row["foreclosure_id"]): row for row in rows}


def _signal_reason(signal: AuditSignal) -> str:
    """Collapse structured signal evidence into the existing reason string."""
    detail = signal.detail

    if signal.signal_type == "judgment_joined_party_gap":
        party_name = detail.get("party_name") or "unknown"
        party_type = detail.get("party_type") or "unknown"
        return f"Judgment names party '{party_name}' (type={party_type}) not reflected in encumbrance discovery"

    if signal.signal_type == "judgment_instrument_gap":
        source = detail.get("reference_source") or "reference"
        parts = [
            f"instrument={detail['instrument_number']}" for key in ("instrument_number",)
            if detail.get(key)
        ]
        if detail.get("book") or detail.get("page"):
            parts.append(
                f"book/page={detail.get('book') or '?'}:{detail.get('page') or '?'}"
            )
        suffix = f" ({', '.join(parts)})" if parts else ""
        return f"Judgment {source} reference missing from encumbrance set{suffix}"

    if signal.signal_type == "lp_to_judgment_plaintiff_change":
        lp_plaintiff = detail.get("lp_plaintiff") or "unknown"
        judgment_plaintiff = detail.get("judgment_plaintiff") or "unknown"
        return f"LP plaintiff '{lp_plaintiff}' differs from judgment plaintiff '{judgment_plaintiff}'"

    if signal.signal_type == "lp_to_judgment_party_expansion":
        new_parties = detail.get("new_parties") or []
        names = ", ".join(
            str(p.get("name") or "").strip()
            for p in new_parties
            if isinstance(p, dict) and str(p.get("name") or "").strip()
        )
        count = detail.get("new_party_count") or len(new_parties)
        suffix = f": {names}" if names else ""
        return f"Judgment adds {count} party(s) not present in LP{suffix}"

    if signal.signal_type == "lp_to_judgment_property_change":
        changes = detail.get("changes") or []
        parts: list[str] = []
        for change in changes:
            if not isinstance(change, dict):
                continue
            field = change.get("field") or "field"
            if field == "legal_description":
                parts.append(
                    f"legal_description similarity={change.get('jaccard_similarity', 'n/a')}"
                )
            elif field == "property_address":
                parts.append(
                    f"address '{change.get('lp_address', '?')}' -> '{change.get('judgment_address', '?')}'"
                )
            else:
                parts.append(str(field))
        suffix = f" ({'; '.join(parts)})" if parts else ""
        return f"LP/judgment property description differs{suffix}"

    if signal.signal_type == "long_case_interim_risk":
        gap_years = detail.get("gap_years") or "unknown"
        return f"Long LP-to-judgment gap ({gap_years} years) with no lifecycle evidence"

    return f"Audit signal triggered: {signal.signal_type}"


def _collect_signal_bucket_hits(conn: Any) -> dict[str, list[BucketHit]]:
    """Run the LP-to-judgment signal extractor once and map to bucket hits."""
    extractor = AuditSignalExtractor()
    signals = extractor.extract_all_signals(conn=conn)

    hits_by_bucket = {name: [] for name in SIGNAL_BUCKET_DESCRIPTIONS}
    if not signals:
        return hits_by_bucket

    foreclosure_lookup = _load_foreclosure_lookup(
        conn,
        {int(signal.foreclosure_id) for signal in signals},
    )

    for signal in signals:
        if signal.signal_type not in hits_by_bucket:
            continue
        foreclosure = foreclosure_lookup.get(int(signal.foreclosure_id))
        if not foreclosure:
            continue
        hits_by_bucket[signal.signal_type].append(
            BucketHit(
                bucket=signal.signal_type,
                foreclosure_id=int(signal.foreclosure_id),
                case_number=str(foreclosure.get("case_number") or ""),
                strap=foreclosure.get("strap"),
                property_address=foreclosure.get("property_address"),
                reason=_signal_reason(signal),
            )
        )

    return hits_by_bucket


def _lp_required_case_sql(alias: str) -> str:
    """SQL predicate for cases where lis pendens coverage is expected."""
    return (
        "("
        f"COALESCE({alias}.is_foreclosure, FALSE) = TRUE "
        f"OR COALESCE({alias}.clerk_case_type, '') ILIKE 'Mortgage Foreclosure%' "
        f"OR COALESCE({alias}.clerk_case_type, '') ILIKE 'CC Real Property/Mortgage Foreclosure%' "
        f"OR ({alias}.case_number_norm LIKE '%-CA-%' "
        f"AND NULLIF(TRIM(COALESCE({alias}.clerk_case_type, '')), '') IS NULL)"
        ")"
    )


# ---------------------------------------------------------------------------
# Bucket queries
# ---------------------------------------------------------------------------
# Each function returns a list of BucketHit for that bucket.  They all
# operate on a single SQLAlchemy connection.


def _bucket_lp_missing(conn: Any) -> list[BucketHit]:
    """Foreclosures missing lis pendens in ori_encumbrances AND title events."""
    sql = """
    SELECT f.foreclosure_id,
           f.case_number_raw  AS case_number,
           f.strap,
           f.property_address
    FROM   foreclosures f
    WHERE  f.archived_at IS NULL
      AND  f.judgment_data IS NOT NULL
      AND  {lp_required_case_sql}
      AND  NOT EXISTS (
               SELECT 1
               FROM   ori_encumbrances oe
               WHERE  oe.strap = f.strap
                 AND  oe.encumbrance_type = 'lis_pendens'
           )
      AND  NOT EXISTS (
               SELECT 1
               FROM   foreclosure_title_events fte
               WHERE  fte.foreclosure_id = f.foreclosure_id
                 AND  fte.event_subtype IN ('LP', 'LPR')
           )
    ORDER  BY f.foreclosure_id
    """.format(lp_required_case_sql=_lp_required_case_sql("f"))
    hits: list[BucketHit] = []
    for r in _rows(conn, sql):
        hits.append(
            BucketHit(
                bucket="lp_missing",
                foreclosure_id=r["foreclosure_id"],
                case_number=r["case_number"],
                strap=r.get("strap"),
                property_address=r.get("property_address"),
                reason="No lis pendens found in ori_encumbrances or title events",
            )
        )
    return hits


def _bucket_foreclosing_lien_missing(conn: Any) -> list[BucketHit]:
    """LP-required foreclosures with no mortgage/lien base encumbrance row."""
    sql = """
    SELECT f.foreclosure_id,
           f.case_number_raw  AS case_number,
           f.strap,
           f.property_address,
           f.clerk_case_type,
           f.judgment_data->>'plaintiff' AS plaintiff
    FROM   foreclosures f
    WHERE  f.archived_at IS NULL
      AND  f.judgment_data IS NOT NULL
      AND  f.strap IS NOT NULL
      AND  f.strap != ''
      AND  {lp_required_case_sql}
      AND  NOT EXISTS (
               SELECT 1
               FROM   ori_encumbrances oe
               WHERE  oe.strap = f.strap
                 AND  oe.encumbrance_type IN ('mortgage', 'lien')
           )
    ORDER  BY f.foreclosure_id
    """.format(lp_required_case_sql=_lp_required_case_sql("f"))
    hits: list[BucketHit] = []
    for r in _rows(conn, sql):
        ct = r.get("clerk_case_type") or "unknown"
        plaintiff = r.get("plaintiff") or "unknown"
        hits.append(
            BucketHit(
                bucket="foreclosing_lien_missing",
                foreclosure_id=r["foreclosure_id"],
                case_number=r["case_number"],
                strap=r.get("strap"),
                property_address=r.get("property_address"),
                reason=f"No mortgage/lien base encumbrance found (case_type={ct}, plaintiff={plaintiff})",
            )
        )
    return hits


def _bucket_plaintiff_chain_gap(conn: Any) -> list[BucketHit]:
    """Plaintiff from judgment is not reflected in any encumbrance party.

    We check whether the judgment plaintiff name appears (case-insensitive
    substring) in party1 or party2 of *any* encumbrance for this strap.
    """
    sql = """
    SELECT f.foreclosure_id,
           f.case_number_raw  AS case_number,
           f.strap,
           f.property_address,
           f.judgment_data->>'plaintiff' AS plaintiff
    FROM   foreclosures f
    WHERE  f.archived_at IS NULL
      AND  f.judgment_data IS NOT NULL
      AND  f.strap IS NOT NULL
      AND  f.strap != ''
      AND  NULLIF(TRIM(f.judgment_data->>'plaintiff'), '') IS NOT NULL
      AND  NOT EXISTS (
               SELECT 1
               FROM   ori_encumbrances oe
               WHERE  oe.strap = f.strap
                 AND  (
                     UPPER(oe.party1) LIKE '%%' || UPPER(TRIM(f.judgment_data->>'plaintiff')) || '%%'
                  OR UPPER(oe.party2) LIKE '%%' || UPPER(TRIM(f.judgment_data->>'plaintiff')) || '%%'
                 )
           )
    ORDER  BY f.foreclosure_id
    """
    hits: list[BucketHit] = []
    for r in _rows(conn, sql):
        plaintiff = r.get("plaintiff") or "unknown"
        hits.append(
            BucketHit(
                bucket="plaintiff_chain_gap",
                foreclosure_id=r["foreclosure_id"],
                case_number=r["case_number"],
                strap=r.get("strap"),
                property_address=r.get("property_address"),
                reason=f"Plaintiff '{plaintiff}' not found in any encumbrance party1/party2",
            )
        )
    return hits


def _bucket_cc_lien_gap(conn: Any) -> list[BucketHit]:
    """Lien-style county-civil cases with no lien encumbrance."""
    sql = """
    SELECT f.foreclosure_id,
           f.case_number_raw  AS case_number,
           f.strap,
           f.property_address,
           f.clerk_case_type
    FROM   foreclosures f
    WHERE  f.archived_at IS NULL
      AND  f.judgment_data IS NOT NULL
      AND  f.strap IS NOT NULL
      AND  f.strap != ''
      AND  COALESCE(f.clerk_case_type, '') ILIKE 'CC Enforce Lien%'
      AND  NOT EXISTS (
               SELECT 1
               FROM   ori_encumbrances oe
               WHERE  oe.strap = f.strap
                 AND  oe.encumbrance_type = 'lien'
           )
    ORDER  BY f.foreclosure_id
    """
    hits: list[BucketHit] = []
    for r in _rows(conn, sql):
        hits.append(
            BucketHit(
                bucket="cc_lien_gap",
                foreclosure_id=r["foreclosure_id"],
                case_number=r["case_number"],
                strap=r.get("strap"),
                property_address=r.get("property_address"),
                reason="CC case has no lien-type encumbrance in ori_encumbrances",
            )
        )
    return hits


def _bucket_construction_lien_risk(conn: Any) -> list[BucketHit]:
    """Properties with recent NOC or permit activity but no construction lien.

    We look for:
      1. NOC in ori_encumbrances (encumbrance_type='noc'), OR
      2. Recent permits (county_permits within 4 years)
    but *no* mechanic's / construction lien evidence.  The lien risk is that
    a contractor was not paid and may file (or has filed) a construction lien.
    """
    sql = """
    SELECT f.foreclosure_id,
           f.case_number_raw  AS case_number,
           f.strap,
           f.property_address,
           CASE
             WHEN noc.id IS NOT NULL THEN 'NOC recorded'
             ELSE 'Recent permit'
           END AS signal_source
    FROM   foreclosures f
    LEFT   JOIN LATERAL (
               SELECT oe.id
               FROM   ori_encumbrances oe
               WHERE  oe.strap = f.strap
                 AND  oe.encumbrance_type = 'noc'
               LIMIT  1
           ) noc ON TRUE
    LEFT   JOIN LATERAL (
               SELECT 1 AS has_permit
               FROM   hcpa_bulk_parcels bp
               JOIN   county_permits cp
                 ON   cp.folio_clean = bp.folio
               WHERE  bp.strap = f.strap
                 AND  cp.issue_date >= (CURRENT_DATE - INTERVAL '4 years')
               LIMIT  1
           ) perm ON TRUE
    WHERE  f.archived_at IS NULL
      AND  f.judgment_data IS NOT NULL
      AND  f.strap IS NOT NULL
      AND  f.strap != ''
      AND  (noc.id IS NOT NULL OR perm.has_permit IS NOT NULL)
      AND  NOT EXISTS (
               SELECT 1
               FROM   ori_encumbrances oe
               WHERE  oe.strap = f.strap
                 AND  oe.encumbrance_type = 'lien'
                 AND  UPPER(COALESCE(oe.raw_document_type, '')) LIKE '%%MECH%%'
           )
      AND  NOT EXISTS (
               SELECT 1
               FROM   ori_encumbrances oe
               WHERE  oe.strap = f.strap
                 AND  oe.encumbrance_type = 'lien'
                 AND  UPPER(COALESCE(oe.raw_document_type, '')) LIKE '%%CONSTRUCTION%%'
           )
    ORDER  BY f.foreclosure_id
    """
    hits: list[BucketHit] = []
    for r in _rows(conn, sql):
        hits.append(
            BucketHit(
                bucket="construction_lien_risk",
                foreclosure_id=r["foreclosure_id"],
                case_number=r["case_number"],
                strap=r.get("strap"),
                property_address=r.get("property_address"),
                reason=f"{r['signal_source']}: NOC or recent permit but no construction/mechanic lien found",
            )
        )
    return hits


def _bucket_sat_parent_gap(conn: Any) -> list[BucketHit]:
    """Satisfaction or release exists but the parent encumbrance is missing.

    A satisfaction or release document should reference a parent mortgage or
    lien.  We flag cases where a satisfaction/release exists for the strap
    but ``satisfies_encumbrance_id`` is NULL (no linked parent).
    """
    sql = """
    SELECT f.foreclosure_id,
           f.case_number_raw  AS case_number,
           f.strap,
           f.property_address,
           COUNT(*) AS sat_count,
           STRING_AGG(
               oe.encumbrance_type || ' (instrument=' || COALESCE(NULLIF(oe.instrument_number, ''), 'unknown') || ')',
               '; ' ORDER BY oe.recording_date NULLS LAST, oe.id
           ) AS sat_refs
    FROM   foreclosures f
    JOIN   ori_encumbrances oe
      ON   oe.strap = f.strap
    WHERE  f.archived_at IS NULL
      AND  f.judgment_data IS NOT NULL
      AND  f.strap IS NOT NULL
      AND  oe.encumbrance_type IN ('satisfaction', 'release')
      AND  oe.satisfies_encumbrance_id IS NULL
    GROUP  BY f.foreclosure_id, f.case_number_raw, f.strap, f.property_address
    ORDER  BY f.foreclosure_id
    """
    hits: list[BucketHit] = []
    for r in _rows(conn, sql):
        sat_count = int(r.get("sat_count") or 1)
        sat_refs = str(r.get("sat_refs") or "").strip()
        if sat_refs:
            reason = (
                f"{sat_count} unresolved SAT/REL doc(s): {sat_refs}"
                if sat_count > 1
                else f"Unresolved SAT/REL doc: {sat_refs}"
            )
        else:
            inst = r.get("sat_instrument") or "unknown"
            st = r.get("sat_type") or "unknown"
            reason = f"{st} (instrument={inst}) has no linked parent encumbrance"
        hits.append(
            BucketHit(
                bucket="sat_parent_gap",
                foreclosure_id=r["foreclosure_id"],
                case_number=r["case_number"],
                strap=r.get("strap"),
                property_address=r.get("property_address"),
                reason=reason,
            )
        )
    return hits


def _bucket_superpriority_non_ori_risk(conn: Any) -> list[BucketHit]:
    """Superpriority risk signals that are unlikely to appear in ORI.

    Superpriority liens (code enforcement, utility, PACE, CDD, tax) often do
    not appear in official records.  We flag foreclosures that have *none* of:
      - code enforcement or utility liens in ori_encumbrances,
      - Tampa Accela violation records for the address,
      - CDD special-district membership for the strap.

    Any property *could* have superpriority risk, but we only flag those where
    we have positive signals from adjacent data (e.g. Accela violations or
    CDD membership) but no corresponding ORI encumbrance.
    """
    sql = """
    SELECT f.foreclosure_id,
           f.case_number_raw  AS case_number,
           f.strap,
           f.property_address,
           CASE
             WHEN accela.has_violation THEN 'Tampa Accela violation/open record'
             WHEN cdd.has_cdd THEN 'CDD special district membership'
             ELSE 'signal'
           END AS risk_signal
    FROM   foreclosures f
    LEFT   JOIN LATERAL (
               SELECT TRUE AS has_violation
               FROM   tampa_accela_records tar
               WHERE  tar.address_normalized IS NOT NULL
                 AND  f.property_address IS NOT NULL
                 AND  tar.is_violation = TRUE
                 AND  tar.address_normalized ILIKE
                      '%%' || SPLIT_PART(f.property_address, ',', 1) || '%%'
               LIMIT  1
           ) accela ON TRUE
    LEFT   JOIN LATERAL (
               SELECT TRUE AS has_cdd
               FROM   hcpa_bulk_parcels bp
               JOIN   hcpa_special_district_cdds cdd
                 ON   NULLIF(LTRIM(COALESCE(bp.raw_sub, ''), '0'), '')
                      = NULLIF(LTRIM(COALESCE(cdd.cdd_code, ''), '0'), '')
               WHERE  bp.strap = f.strap
               LIMIT  1
           ) cdd ON TRUE
    WHERE  f.archived_at IS NULL
      AND  f.judgment_data IS NOT NULL
      AND  f.strap IS NOT NULL
      AND  f.strap != ''
      AND  (accela.has_violation IS TRUE OR cdd.has_cdd IS TRUE)
      AND  NOT EXISTS (
               SELECT 1
               FROM   ori_encumbrances oe
               WHERE  oe.strap = f.strap
                 AND  (
                     UPPER(COALESCE(oe.raw_document_type, '')) LIKE '%%CODE%%'
                  OR UPPER(COALESCE(oe.raw_document_type, '')) LIKE '%%UTILITY%%'
                  OR UPPER(COALESCE(oe.raw_document_type, '')) LIKE '%%PACE%%'
                  OR UPPER(COALESCE(oe.raw_document_type, '')) LIKE '%%MUNICIPAL%%'
                 )
           )
    ORDER  BY f.foreclosure_id
    """
    hits: list[BucketHit] = []
    for r in _rows(conn, sql):
        hits.append(
            BucketHit(
                bucket="superpriority_non_ori_risk",
                foreclosure_id=r["foreclosure_id"],
                case_number=r["case_number"],
                strap=r.get("strap"),
                property_address=r.get("property_address"),
                reason=r.get("risk_signal", "Superpriority risk signal with no matching ORI lien"),
            )
        )
    return hits


def _bucket_historical_window_gap(conn: Any) -> list[BucketHit]:
    """Encumbrances exist but all are HISTORICAL — no current-owner coverage.

    If *every* encumbrance for the strap has survival_status='HISTORICAL',
    the current ownership period has no encumbrance coverage.  This is a
    meaningful gap: the current owner's liens/mortgages are missing.
    """
    sql = """
    SELECT f.foreclosure_id,
           f.case_number_raw  AS case_number,
           f.strap,
           f.property_address,
           COUNT(*)                                     AS total_enc,
           COUNT(*) FILTER (
               WHERE COALESCE(fes.survival_status, oe.survival_status) = 'HISTORICAL'
           )                                            AS historical_enc
    FROM   foreclosures f
    JOIN   ori_encumbrances oe
      ON   oe.strap = f.strap
    LEFT JOIN foreclosure_encumbrance_survival fes
      ON   fes.foreclosure_id = f.foreclosure_id
     AND   fes.encumbrance_id = oe.id
    WHERE  f.archived_at IS NULL
      AND  f.judgment_data IS NOT NULL
      AND  f.strap IS NOT NULL
      AND  oe.encumbrance_type NOT IN ('noc', 'satisfaction', 'release', 'assignment')
    GROUP  BY f.foreclosure_id, f.case_number_raw, f.strap, f.property_address
    HAVING COUNT(*) = COUNT(*) FILTER (
               WHERE COALESCE(fes.survival_status, oe.survival_status) = 'HISTORICAL'
           )
      AND  COUNT(*) > 0
    ORDER  BY f.foreclosure_id
    """
    hits: list[BucketHit] = []
    for r in _rows(conn, sql):
        total = r.get("total_enc", 0)
        hits.append(
            BucketHit(
                bucket="historical_window_gap",
                foreclosure_id=r["foreclosure_id"],
                case_number=r["case_number"],
                strap=r.get("strap"),
                property_address=r.get("property_address"),
                reason=f"All {total} encumbrances are HISTORICAL — no current-owner coverage",
            )
        )
    return hits


def _bucket_lifecycle_base_gap(conn: Any) -> list[BucketHit]:
    """Lifecycle documents (assignments, modifications) exist without a base.

    If an assignment or modification encumbrance exists for a strap but there
    is no corresponding mortgage or lien base encumbrance, the base was missed
    during ORI discovery.
    """
    sql = """
    SELECT DISTINCT
           f.foreclosure_id,
           f.case_number_raw  AS case_number,
           f.strap,
           f.property_address,
           COALESCE(NULLIF(oe.raw_document_type, ''), oe.encumbrance_type::text) AS lifecycle_type,
           oe.instrument_number AS lifecycle_instrument
    FROM   foreclosures f
    JOIN   ori_encumbrances oe
      ON   oe.strap = f.strap
    WHERE  f.archived_at IS NULL
      AND  f.judgment_data IS NOT NULL
      AND  f.strap IS NOT NULL
      AND  (
          oe.encumbrance_type = 'assignment'
          OR (
              oe.encumbrance_type = 'other'
              AND (
                  UPPER(COALESCE(oe.raw_document_type, '')) LIKE '%MOD%'
                  OR UPPER(COALESCE(oe.raw_document_type, '')) LIKE '%SUB%'
                  OR UPPER(COALESCE(oe.raw_document_type, '')) LIKE '%NCL%'
              )
          )
      )
      AND  NOT EXISTS (
               SELECT 1
               FROM   ori_encumbrances base
               WHERE  base.strap = f.strap
                 AND  base.encumbrance_type IN ('mortgage', 'lien')
           )
    ORDER  BY f.foreclosure_id
    """
    hits: list[BucketHit] = []
    for r in _rows(conn, sql):
        lt = r.get("lifecycle_type") or "assignment"
        inst = r.get("lifecycle_instrument") or "unknown"
        hits.append(
            BucketHit(
                bucket="lifecycle_base_gap",
                foreclosure_id=r["foreclosure_id"],
                case_number=r["case_number"],
                strap=r.get("strap"),
                property_address=r.get("property_address"),
                reason=f"{lt} (instrument={inst}) exists but no base mortgage/lien found",
            )
        )
    return hits


# ---------------------------------------------------------------------------
# Bucket registry
# ---------------------------------------------------------------------------

BUCKET_DEFINITIONS: list[dict[str, Any]] = [
    {
        "name": "lp_missing",
        "description": "Active foreclosures missing lis pendens",
        "handler": _bucket_lp_missing,
        "deferred": False,
    },
    {
        "name": "foreclosing_lien_missing",
        "description": "Foreclosing lien/mortgage not flagged in encumbrances",
        "handler": _bucket_foreclosing_lien_missing,
        "deferred": False,
    },
    {
        "name": "plaintiff_chain_gap",
        "description": "Judgment plaintiff not found in any encumbrance party",
        "handler": _bucket_plaintiff_chain_gap,
        "deferred": False,
    },
    {
        "name": "cc_lien_gap",
        "description": "CC cases missing lien encumbrance",
        "handler": _bucket_cc_lien_gap,
        "deferred": False,
    },
    {
        "name": "construction_lien_risk",
        "description": "NOC or recent permit with no construction/mechanic lien",
        "handler": _bucket_construction_lien_risk,
        "deferred": False,
    },
    {
        "name": "sat_parent_gap",
        "description": "Satisfaction/release without linked parent mortgage",
        "handler": _bucket_sat_parent_gap,
        "deferred": False,
    },
    {
        "name": "superpriority_non_ori_risk",
        "description": "Superpriority risk signals (violations/CDD) without ORI lien",
        "handler": _bucket_superpriority_non_ori_risk,
        "deferred": False,
    },
    {
        "name": "historical_window_gap",
        "description": "All encumbrances are HISTORICAL — no current-owner coverage",
        "handler": _bucket_historical_window_gap,
        "deferred": False,
    },
    {
        "name": "lifecycle_base_gap",
        "description": "Assignment exists without base mortgage/lien",
        "handler": _bucket_lifecycle_base_gap,
        "deferred": False,
    },
    {
        "name": "judgment_joined_party_gap",
        "description": SIGNAL_BUCKET_DESCRIPTIONS["judgment_joined_party_gap"],
        "source": "signal",
        "deferred": False,
    },
    {
        "name": "judgment_instrument_gap",
        "description": SIGNAL_BUCKET_DESCRIPTIONS["judgment_instrument_gap"],
        "source": "signal",
        "deferred": False,
    },
    {
        "name": "lp_to_judgment_plaintiff_change",
        "description": SIGNAL_BUCKET_DESCRIPTIONS["lp_to_judgment_plaintiff_change"],
        "source": "signal",
        "deferred": False,
    },
    {
        "name": "lp_to_judgment_party_expansion",
        "description": SIGNAL_BUCKET_DESCRIPTIONS["lp_to_judgment_party_expansion"],
        "source": "signal",
        "deferred": False,
    },
    {
        "name": "lp_to_judgment_property_change",
        "description": SIGNAL_BUCKET_DESCRIPTIONS["lp_to_judgment_property_change"],
        "source": "signal",
        "deferred": False,
    },
    {
        "name": "long_case_interim_risk",
        "description": SIGNAL_BUCKET_DESCRIPTIONS["long_case_interim_risk"],
        "source": "signal",
        "deferred": False,
    },
]


# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------


def run_audit(dsn: str | None = None, *, conn: Any | None = None) -> AuditReport:
    """Run the encumbrance audit and return a structured report.

    Parameters
    ----------
    dsn : str | None
        PostgreSQL DSN override.  Ignored when *conn* is provided.
    conn : Any | None
        Pre-existing SQLAlchemy connection (used by tests and callers that
        already hold a transaction).  When provided, the caller owns the
        connection lifecycle.
    """

    def _run(c: Any) -> AuditReport:
        # --- Scope metrics ---
        active_count = _val(c, "SELECT COUNT(*) FROM foreclosures WHERE archived_at IS NULL")
        judged_count = _val(
            c,
            "SELECT COUNT(*) FROM foreclosures WHERE archived_at IS NULL AND judgment_data IS NOT NULL",
        )
        with_strap_count = _val(
            c,
            "SELECT COUNT(*) FROM foreclosures "
            "WHERE archived_at IS NULL AND judgment_data IS NOT NULL "
            "AND strap IS NOT NULL AND strap != ''",
        )

        has_enc = _has_table(c, "ori_encumbrances")
        if has_enc:
            with_enc_count = _val(
                c,
                "SELECT COUNT(DISTINCT f.foreclosure_id) "
                "FROM foreclosures f "
                "JOIN ori_encumbrances oe ON oe.strap = f.strap "
                "WHERE f.archived_at IS NULL AND f.judgment_data IS NOT NULL "
                "AND f.strap IS NOT NULL",
            )
            with_survival_count = _val(
                c,
                "SELECT COUNT(DISTINCT f.foreclosure_id) "
                "FROM foreclosures f "
                "JOIN ori_encumbrances oe ON oe.strap = f.strap "
                "LEFT JOIN foreclosure_encumbrance_survival fes "
                "ON fes.foreclosure_id = f.foreclosure_id AND fes.encumbrance_id = oe.id "
                "WHERE f.archived_at IS NULL AND f.judgment_data IS NOT NULL "
                "AND f.strap IS NOT NULL "
                "AND COALESCE(fes.survival_status, oe.survival_status) IS NOT NULL",
            )
        else:
            with_enc_count = 0
            with_survival_count = 0

        report = AuditReport(
            active_count=active_count,
            judged_count=judged_count,
            with_strap_count=with_strap_count,
            with_encumbrances_count=with_enc_count,
            with_survival_count=with_survival_count,
        )
        signal_hits_by_bucket: dict[str, list[BucketHit]] | None = None
        signal_bucket_error: str | None = None

        # --- Run each bucket ---
        for bdef in BUCKET_DEFINITIONS:
            bucket_name: str = bdef["name"]
            if bdef.get("deferred"):
                report.summaries.append(
                    BucketSummary(
                        bucket=bucket_name,
                        description=bdef["description"],
                        count=0,
                        deferred=True,
                        deferred_reason=bdef.get("deferred_reason", "Not yet implementable"),
                    )
                )
                continue

            if bdef.get("source") == "signal":
                if not has_enc:
                    report.summaries.append(
                        BucketSummary(
                            bucket=bucket_name,
                            description=bdef["description"],
                            count=0,
                            deferred=True,
                            deferred_reason="ori_encumbrances table not found",
                        )
                    )
                    continue

                if signal_hits_by_bucket is None and signal_bucket_error is None:
                    try:
                        signal_hits_by_bucket = _collect_signal_bucket_hits(c)
                    except Exception as exc:
                        with contextlib.suppress(Exception):
                            c.rollback()
                        logger.exception("Signal bucket extraction failed")
                        signal_bucket_error = f"Signal extraction error: {exc}"

                if signal_bucket_error is not None:
                    report.summaries.append(
                        BucketSummary(
                            bucket=bucket_name,
                            description=bdef["description"],
                            count=0,
                            deferred=True,
                            deferred_reason=signal_bucket_error,
                        )
                    )
                    continue

                hits = (signal_hits_by_bucket or {}).get(bucket_name, [])
                report.hits.extend(hits)
                report.summaries.append(
                    BucketSummary(
                        bucket=bucket_name,
                        description=bdef["description"],
                        count=len(hits),
                    )
                )
                continue

            # Skip buckets that need ori_encumbrances if the table is missing
            needs_enc = bucket_name not in {"lp_missing"}
            if needs_enc and not has_enc:
                report.summaries.append(
                    BucketSummary(
                        bucket=bucket_name,
                        description=bdef["description"],
                        count=0,
                        deferred=True,
                        deferred_reason="ori_encumbrances table not found",
                    )
                )
                continue

            handler = bdef["handler"]
            try:
                hits = handler(c)
            except Exception as exc:
                with contextlib.suppress(Exception):
                    c.rollback()
                logger.exception("Bucket {} failed", bucket_name)
                report.summaries.append(
                    BucketSummary(
                        bucket=bucket_name,
                        description=bdef["description"],
                        count=0,
                        deferred=True,
                        deferred_reason=f"Bucket error: {exc}",
                    )
                )
                continue

            report.hits.extend(hits)
            report.summaries.append(
                BucketSummary(
                    bucket=bucket_name,
                    description=bdef["description"],
                    count=len(hits),
                )
            )

        return report

    if conn is not None:
        return _run(conn)

    engine = get_engine(resolve_pg_dsn(dsn))
    with engine.connect() as c:
        return _run(c)


# ---------------------------------------------------------------------------
# Output formatters
# ---------------------------------------------------------------------------


def _pct(num: int, den: int) -> str:
    if den == 0:
        return "N/A"
    return f"{100.0 * num / den:.1f}%"


def format_console(report: AuditReport) -> str:
    """Human-readable console summary."""
    lines: list[str] = []
    lines.append("=" * 65)
    lines.append("  ENCUMBRANCE AUDIT — ACTIVE FORECLOSURES")
    lines.append("=" * 65)
    lines.append("")
    lines.append(f"  Active foreclosures:     {report.active_count}")
    lines.append(f"  With judgment data:      {report.judged_count}")
    lines.append(f"  With strap:              {report.with_strap_count}")
    lines.append(f"  With encumbrances:       {report.with_encumbrances_count}")
    lines.append(f"  With survival status:    {report.with_survival_count}")
    lines.append("")
    lines.append("-" * 65)
    lines.append("  BUCKET SUMMARY")
    lines.append("-" * 65)
    lines.append("")

    for s in report.summaries:
        if s.deferred:
            lines.append(f"  {s.bucket:35s}  DEFERRED  ({s.deferred_reason})")
        else:
            pct = _pct(s.count, report.judged_count)
            lines.append(f"  {s.bucket:35s}  {s.count:5d}  ({pct} of judged)")

    lines.append("")
    lines.append("-" * 65)
    lines.append("  PER-CASE DETAIL")
    lines.append("-" * 65)

    # Group hits by bucket
    buckets_seen: dict[str, list[BucketHit]] = {}
    for h in report.hits:
        buckets_seen.setdefault(h.bucket, []).append(h)

    for bucket_name, bucket_hits in buckets_seen.items():
        lines.append("")
        lines.append(f"  [{bucket_name}] ({len(bucket_hits)} cases)")
        for h in bucket_hits:
            addr = h.property_address or "no address"
            lines.append(f"    {h.case_number:16s}  {addr:40s}  {h.reason}")

    lines.append("")
    lines.append("=" * 65)
    return "\n".join(lines)


def format_json(report: AuditReport) -> str:
    """JSON output."""
    payload: dict[str, Any] = {
        "scope": {
            "active_count": report.active_count,
            "judged_count": report.judged_count,
            "with_strap_count": report.with_strap_count,
            "with_encumbrances_count": report.with_encumbrances_count,
            "with_survival_count": report.with_survival_count,
        },
        "summaries": [asdict(s) for s in report.summaries],
        "hits": [asdict(h) for h in report.hits],
    }
    return json.dumps(payload, indent=2, default=str)


def format_csv(report: AuditReport) -> str:
    """CSV output of per-case hits."""
    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow(["bucket", "foreclosure_id", "case_number", "strap", "property_address", "reason"])
    for h in report.hits:
        writer.writerow([h.bucket, h.foreclosure_id, h.case_number, h.strap or "", h.property_address or "", h.reason])
    return buf.getvalue()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PG-only active-foreclosure encumbrance audit",
    )
    parser.add_argument("--dsn", help="PostgreSQL DSN override")
    parser.add_argument(
        "--json",
        dest="output_json",
        action="store_true",
        help="Emit JSON instead of console summary",
    )
    parser.add_argument(
        "--csv",
        dest="csv_path",
        metavar="PATH",
        help="Write CSV of per-case hits to this file",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    try:
        report = run_audit(dsn=args.dsn)
    except Exception as exc:
        logger.exception("Encumbrance audit failed: {}", exc)
        sys.exit(1)

    # Output
    if args.output_json:
        print(format_json(report))
    else:
        print(format_console(report))

    if args.csv_path:
        csv_text = format_csv(report)
        Path(args.csv_path).write_text(csv_text, encoding="utf-8")
        logger.info("CSV written to {}", args.csv_path)


if __name__ == "__main__":
    main()
