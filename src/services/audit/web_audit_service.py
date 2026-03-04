"""Read-only audit adapter for web presentation.

This module provides a thin read-only layer that bridges the encumbrance audit
engine (``pg_audit_encumbrance`` and ``encumbrance_audit_signals``) to the web
UI.  It does **not** persist audit results anywhere — every call recomputes
from live PostgreSQL data.

Two entry points:

* ``get_property_audit_snapshot`` — resolves audit issues for a single
  foreclosure, suitable for the property detail page.  Runs per-property
  signal extraction (not the full global audit) so it's fast.

* ``get_encumbrance_audit_inbox`` — runs the full global audit across all
  active foreclosures and reshapes the output for the operator inbox page.

Bucket metadata (labels, families, why-it-matters blurbs) is maintained in
``BUCKET_META`` so the UI never duplicates presentation logic.
"""

from __future__ import annotations

from typing import Any

from loguru import logger

from src.services.audit.encumbrance_audit_signals import AuditSignalExtractor
from src.services.audit.pg_audit_encumbrance import (
    BUCKET_DEFINITIONS,
    run_audit,
)

# ---------------------------------------------------------------------------
# Bucket presentation metadata
# ---------------------------------------------------------------------------

BUCKET_META: dict[str, dict[str, str]] = {
    # --- Data Coverage ---
    "lp_missing": {
        "label": "Missing Lis Pendens",
        "family": "Data Coverage",
        "why_it_matters": "Without the LP filing, we cannot trace the foreclosure's origin document or verify the chain.",
        "badge_class": "badge-warning",
    },
    "foreclosing_lien_missing": {
        "label": "Foreclosing Lien Missing",
        "family": "Data Coverage",
        "why_it_matters": "The base mortgage or lien being foreclosed is not in the encumbrance set — survival analysis is incomplete.",
        "badge_class": "badge-warning",
    },
    "sat_parent_gap": {
        "label": "Orphan Satisfaction",
        "family": "Data Coverage",
        "why_it_matters": "A satisfaction or release exists but its parent mortgage was never discovered — possible hidden encumbrance.",
        "badge_class": "badge-warning",
    },
    "lifecycle_base_gap": {
        "label": "Assignment Without Base",
        "family": "Data Coverage",
        "why_it_matters": "An assignment record exists without the original mortgage — the base lien may be missing from discovery.",
        "badge_class": "badge-warning",
    },
    "cc_lien_gap": {
        "label": "CC Lien Gap",
        "family": "Data Coverage",
        "why_it_matters": "County-civil lien case found but no lien encumbrance in ORI — lien may survive and affect equity.",
        "badge_class": "badge-warning",
    },
    "historical_window_gap": {
        "label": "Historical-Only Coverage",
        "family": "Data Coverage",
        "why_it_matters": "All encumbrances are HISTORICAL — no coverage for the current ownership period.",
        "badge_class": "badge-info",
    },
    # --- Identity / Parties ---
    "plaintiff_chain_gap": {
        "label": "Plaintiff Not In Encumbrances",
        "family": "Identity / Parties",
        "why_it_matters": "The judgment plaintiff is not reflected in any encumbrance party — possible entity mismatch or missing assignment.",
        "badge_class": "badge-warning",
    },
    "judgment_joined_party_gap": {
        "label": "Judgment Party Gap",
        "family": "Identity / Parties",
        "why_it_matters": "Parties named in the judgment are absent from encumbrance records — possible undiscovered liens.",
        "badge_class": "badge-warning",
    },
    "lp_to_judgment_plaintiff_change": {
        "label": "Plaintiff Changed",
        "family": "Identity / Parties",
        "why_it_matters": "The foreclosing plaintiff changed between LP and judgment — likely an assignment or merger not yet captured.",
        "badge_class": "badge-info",
    },
    "lp_to_judgment_party_expansion": {
        "label": "New Parties At Judgment",
        "family": "Identity / Parties",
        "why_it_matters": "Judgment introduces parties not present in the original LP — may indicate additional lienholders.",
        "badge_class": "badge-info",
    },
    # --- Property Mismatch ---
    "lp_to_judgment_property_change": {
        "label": "Property Description Changed",
        "family": "Property Mismatch",
        "why_it_matters": "Legal description or address changed between LP and judgment — verify correct parcel is being foreclosed.",
        "badge_class": "badge-danger",
    },
    "judgment_instrument_gap": {
        "label": "Instrument Detail Missing",
        "family": "Property Mismatch",
        "why_it_matters": "Judgment references instruments (book/page or number) not found in encumbrance set.",
        "badge_class": "badge-warning",
    },
    # --- Risk Signals ---
    "construction_lien_risk": {
        "label": "Construction Lien Risk",
        "family": "Risk Signals",
        "why_it_matters": "Active NOC or recent permit found without a matching construction lien — mechanic's lien may survive.",
        "badge_class": "badge-danger",
    },
    "superpriority_non_ori_risk": {
        "label": "Superpriority Risk",
        "family": "Risk Signals",
        "why_it_matters": "Violation or CDD signal present without a matching ORI lien — superpriority claim may survive.",
        "badge_class": "badge-danger",
    },
    "long_case_interim_risk": {
        "label": "Long Case Gap",
        "family": "Risk Signals",
        "why_it_matters": "Significant time gap between LP and judgment with no lifecycle evidence — intervening liens may exist.",
        "badge_class": "badge-info",
    },
}

_FALLBACK_META: dict[str, str] = {
    "label": "Unknown Bucket",
    "family": "Other",
    "why_it_matters": "Unclassified audit signal.",
    "badge_class": "badge-secondary",
}

FAMILY_ORDER = [
    "Data Coverage",
    "Identity / Parties",
    "Property Mismatch",
    "Risk Signals",
    "Other",
]


def get_bucket_meta(bucket: str) -> dict[str, str]:
    """Return presentation metadata for a bucket, with fallback."""
    return BUCKET_META.get(bucket, {**_FALLBACK_META, "label": bucket})


# ---------------------------------------------------------------------------
# Property-level snapshot
# ---------------------------------------------------------------------------


def get_property_audit_snapshot(
    *,
    foreclosure_id: int | None = None,
    folio: str | None = None,
    strap: str | None = None,
    case_number: str | None = None,
    conn: Any | None = None,
) -> dict[str, Any]:
    """Compute audit issues for a single foreclosure.

    Returns a dict with:
    - ``total_open_issues``: int
    - ``family_counts``: dict[str, int]
    - ``bucket_counts``: dict[str, int]
    - ``issues``: list[dict] (each has bucket, label, family, reason, why_it_matters, badge_class)
    - ``has_issues``: bool
    - ``top_buckets``: list[str] (top 3 bucket labels)
    """
    empty: dict[str, Any] = {
        "total_open_issues": 0,
        "family_counts": {},
        "bucket_counts": {},
        "issues": [],
        "has_issues": False,
        "top_buckets": [],
    }

    if not foreclosure_id:
        return empty

    try:
        issues: list[dict[str, Any]] = []

        # --- SQL-backed buckets: run per-foreclosure queries ---
        if conn is not None:
            for bdef in BUCKET_DEFINITIONS:
                if bdef.get("source") == "signal" or bdef.get("deferred"):
                    continue
                handler = bdef["handler"]
                try:
                    all_hits = handler(conn)
                    for hit in all_hits:
                        if hit.foreclosure_id == foreclosure_id:
                            meta = get_bucket_meta(hit.bucket)
                            issues.append({
                                "bucket": hit.bucket,
                                "label": meta["label"],
                                "family": meta["family"],
                                "reason": hit.reason,
                                "why_it_matters": meta["why_it_matters"],
                                "badge_class": meta["badge_class"],
                            })
                except Exception:
                    logger.debug("Bucket {} failed for foreclosure {}", bdef["name"], foreclosure_id, exc_info=True)

        # --- Signal-backed buckets: use per-foreclosure extractor ---
        try:
            extractor = AuditSignalExtractor()
            signals = extractor.extract_signals_for(foreclosure_id, conn=conn)
            for signal in signals:
                meta = get_bucket_meta(signal.signal_type)
                detail = signal.detail or {}
                reason_parts = [str(v) for v in detail.values() if v]
                reason = "; ".join(reason_parts[:3]) if reason_parts else signal.signal_type
                issues.append({
                    "bucket": signal.signal_type,
                    "label": meta["label"],
                    "family": meta["family"],
                    "reason": reason,
                    "why_it_matters": meta["why_it_matters"],
                    "badge_class": meta["badge_class"],
                })
        except Exception:
            logger.debug("Signal extraction failed for foreclosure {}", foreclosure_id, exc_info=True)

        # --- Aggregate ---
        family_counts: dict[str, int] = {}
        bucket_counts: dict[str, int] = {}
        for iss in issues:
            family_counts[iss["family"]] = family_counts.get(iss["family"], 0) + 1
            bucket_counts[iss["bucket"]] = bucket_counts.get(iss["bucket"], 0) + 1

        top_buckets = sorted(bucket_counts, key=lambda b: bucket_counts[b], reverse=True)[:3]
        top_bucket_labels = [get_bucket_meta(b)["label"] for b in top_buckets]

        return {
            "total_open_issues": len(issues),
            "family_counts": family_counts,
            "bucket_counts": bucket_counts,
            "issues": issues,
            "has_issues": len(issues) > 0,
            "top_buckets": top_bucket_labels,
        }

    except Exception:
        logger.exception("get_property_audit_snapshot failed for foreclosure_id={}", foreclosure_id)
        return empty


# ---------------------------------------------------------------------------
# Global inbox
# ---------------------------------------------------------------------------


def get_encumbrance_audit_inbox(
    *,
    conn: Any | None = None,
) -> dict[str, Any]:
    """Run the full global audit and reshape for the inbox page.

    Returns a dict with:
    - ``summary_cards``: dict (open_issues, affected_foreclosures, top_bucket, data_coverage_count)
    - ``bucket_summaries``: list[dict] with label, family, count, description
    - ``rows``: list[dict] with property_address, case_number, strap, bucket, label, family, reason, badge_class
    """
    empty: dict[str, Any] = {
        "summary_cards": {
            "open_issues": 0,
            "affected_foreclosures": 0,
            "top_bucket": None,
            "data_coverage_count": 0,
        },
        "bucket_summaries": [],
        "rows": [],
    }

    try:
        report = run_audit(conn=conn)
    except Exception:
        logger.exception("get_encumbrance_audit_inbox: run_audit failed")
        return empty

    # Build rows from hits
    rows: list[dict[str, Any]] = []
    for hit in report.hits:
        meta = get_bucket_meta(hit.bucket)
        rows.append({
            "foreclosure_id": hit.foreclosure_id,
            "property_address": hit.property_address or "",
            "case_number": hit.case_number or "",
            "strap": hit.strap or "",
            "bucket": hit.bucket,
            "label": meta["label"],
            "family": meta["family"],
            "reason": hit.reason or "",
            "badge_class": meta["badge_class"],
            "why_it_matters": meta["why_it_matters"],
        })

    # Bucket summaries
    bucket_summaries: list[dict[str, Any]] = []
    for s in report.summaries:
        if s.count == 0 and not s.deferred:
            continue
        meta = get_bucket_meta(s.bucket)
        bucket_summaries.append({
            "bucket": s.bucket,
            "label": meta["label"],
            "family": meta["family"],
            "count": s.count,
            "description": s.description,
            "deferred": s.deferred,
            "deferred_reason": s.deferred_reason,
            "badge_class": meta["badge_class"],
        })

    # Summary cards
    affected = len({r["foreclosure_id"] for r in rows})
    top_bucket = max(bucket_summaries, key=lambda b: b["count"])["label"] if bucket_summaries else None
    data_coverage_count = sum(
        r["count"] for r in bucket_summaries if r["family"] == "Data Coverage"
    )

    return {
        "summary_cards": {
            "open_issues": len(rows),
            "affected_foreclosures": affected,
            "top_bucket": top_bucket,
            "data_coverage_count": data_coverage_count,
        },
        "bucket_summaries": sorted(
            bucket_summaries,
            key=lambda b: (FAMILY_ORDER.index(b["family"]) if b["family"] in FAMILY_ORDER else 99, -b["count"]),
        ),
        "rows": rows,
    }


def group_issues_by_family(issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group a flat issues list by family for template rendering.

    Returns a list of dicts, each with ``family`` and ``issues`` keys,
    sorted by FAMILY_ORDER.
    """
    families: dict[str, list[dict[str, Any]]] = {}
    for iss in issues:
        fam = iss.get("family", "Other")
        families.setdefault(fam, []).append(iss)

    result = []
    for fam in FAMILY_ORDER:
        if fam in families:
            result.append({"family": fam, "issues": families[fam]})
    # Catch any families not in FAMILY_ORDER
    for fam, iss_list in families.items():
        if fam not in FAMILY_ORDER:
            result.append({"family": fam, "issues": iss_list})
    return result
