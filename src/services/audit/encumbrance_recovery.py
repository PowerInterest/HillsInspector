"""Audit-driven encumbrance recovery orchestration.

Architectural purpose
---------------------
The encumbrance audit identifies coverage gaps and suspicious deltas, but the
audit itself must remain read-only. This module closes that loop by mapping a
subset of audit buckets to the *existing* enrichment services that know how to
load real source data into PostgreSQL:

- ``PgOriService`` for targeted ORI rediscovery and gap backfills,
- ``PgEncumbranceExtractionService`` for encumbrance PDF extraction on newly-touched
  straps,
- ``PgEncumbranceRelationshipService`` for exact-reference discovery and holder
  propagation from extracted JSON,
- ``PgSurvivalService`` for targeted re-analysis after encumbrance changes.

This service does not invent new facts and it does not require audit-specific
tables. It only reuses the current pipeline writers against a narrower,
audit-selected scope, then reruns the read-only audit so operators can see
whether the recovery pass reduced the open issue set.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from src.services.audit.pg_audit_encumbrance import AuditReport, run_audit
from src.services.pg_encumbrance_extraction_service import PgEncumbranceExtractionService
from src.services.pg_encumbrance_relationship_service import PgEncumbranceRelationshipService
from src.services.pg_ori_service import PgOriService
from src.services.pg_survival_service import PgSurvivalService

LP_BACKFILL_BUCKETS = frozenset({
    "lp_missing",
})

ORI_RETRY_BUCKETS = frozenset({
    "foreclosing_lien_missing",
    "plaintiff_chain_gap",
    "cc_lien_gap",
    "construction_lien_risk",
    "sat_parent_gap",
    "lifecycle_base_gap",
    "judgment_joined_party_gap",
    "judgment_instrument_gap",
    "lp_to_judgment_plaintiff_change",
    "lp_to_judgment_party_expansion",
})

RECENT_PERMIT_NOC_BUCKETS = frozenset({
    "construction_lien_risk",
})

REVIEW_ONLY_BUCKETS = frozenset({
    "superpriority_non_ori_risk",
    "historical_window_gap",
    "lp_to_judgment_property_change",
    "long_case_interim_risk",
})

RECOVERABLE_BUCKETS = (
    LP_BACKFILL_BUCKETS
    | ORI_RETRY_BUCKETS
    | RECENT_PERMIT_NOC_BUCKETS
)


def _bucket_counts(report: AuditReport) -> dict[str, int]:
    return {summary.bucket: int(summary.count) for summary in report.summaries}


def _recoverable_bucket_counts(report: AuditReport) -> dict[str, int]:
    counts = _bucket_counts(report)
    return {bucket: counts.get(bucket, 0) for bucket in sorted(RECOVERABLE_BUCKETS)}


def _bucket_foreclosure_ids(report: AuditReport) -> dict[str, set[int]]:
    ids_by_bucket: dict[str, set[int]] = defaultdict(set)
    for hit in report.hits:
        ids_by_bucket[hit.bucket].add(int(hit.foreclosure_id))
    return ids_by_bucket


def _retry_reasons_by_foreclosure_id(
    report: AuditReport,
    *,
    buckets: frozenset[str],
) -> dict[int, list[str]]:
    reasons: dict[int, set[str]] = defaultdict(set)
    for hit in report.hits:
        if hit.bucket in buckets:
            reasons[int(hit.foreclosure_id)].add(hit.bucket)
    return {
        foreclosure_id: sorted(bucket_names)
        for foreclosure_id, bucket_names in reasons.items()
    }


def _changed_target_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    changed: list[dict[str, Any]] = []
    for row in result.get("per_target", []) or []:
        saved = int(row.get("saved") or 0)
        inferred = int(row.get("inferred") or 0)
        linked = int(row.get("satisfactions_linked") or 0)
        if saved > 0 or inferred > 0 or linked > 0:
            changed.append(row)
    return changed


def _changed_foreclosure_ids(result: dict[str, Any]) -> set[int]:
    ids: set[int] = set()
    for row in _changed_target_rows(result):
        foreclosure_id = row.get("foreclosure_id")
        if foreclosure_id:
            ids.add(int(foreclosure_id))
    return ids


def _changed_straps(result: dict[str, Any]) -> set[str]:
    straps: set[str] = set()
    for row in _changed_target_rows(result):
        strap = str(row.get("strap") or "").strip()
        if strap:
            straps.add(strap)
    return straps


def _pct_value(num: int, den: int) -> float | None:
    if den <= 0:
        return None
    return round((100.0 * num) / den, 2)


def _open_issue_counts(report: AuditReport) -> tuple[int, int]:
    recoverable = sum(1 for hit in report.hits if hit.bucket in RECOVERABLE_BUCKETS)
    review_only = sum(1 for hit in report.hits if hit.bucket in REVIEW_ONLY_BUCKETS)
    return recoverable, review_only


def _persistence_checks(report: AuditReport) -> dict[str, Any]:
    recoverable_open, review_only_open = _open_issue_counts(report)
    with_strap = int(report.with_strap_count)
    with_encumbrances = int(report.with_encumbrances_count)
    with_survival = int(report.with_survival_count)
    enc_cov = _pct_value(with_encumbrances, with_strap)
    survival_cov = _pct_value(with_survival, with_strap)
    return {
        "judged_with_strap": with_strap,
        "with_encumbrances": with_encumbrances,
        "with_survival": with_survival,
        "encumbrance_coverage_pct": enc_cov,
        "survival_coverage_pct": survival_cov,
        "encumbrance_coverage_target_met": bool(enc_cov is not None and enc_cov >= 80.0),
        "survival_coverage_target_met": bool(survival_cov is not None and survival_cov >= 80.0),
        "open_issues": len(report.hits),
        "recoverable_open_issues": recoverable_open,
        "review_only_open_issues": review_only_open,
        "affected_foreclosures": len({int(hit.foreclosure_id) for hit in report.hits}),
    }


def _persistence_delta(
    before: dict[str, Any],
    after: dict[str, Any],
) -> dict[str, int | float | None]:
    def _diff(key: str) -> int:
        return int(after.get(key) or 0) - int(before.get(key) or 0)

    def _pct_diff(key: str) -> float | None:
        before_v = before.get(key)
        after_v = after.get(key)
        if before_v is None or after_v is None:
            return None
        return round(float(after_v) - float(before_v), 2)

    return {
        "with_encumbrances_delta": _diff("with_encumbrances"),
        "with_survival_delta": _diff("with_survival"),
        "open_issues_delta": _diff("open_issues"),
        "recoverable_open_issues_delta": _diff("recoverable_open_issues"),
        "encumbrance_coverage_pct_delta": _pct_diff("encumbrance_coverage_pct"),
        "survival_coverage_pct_delta": _pct_diff("survival_coverage_pct"),
    }


class EncumbranceRecoveryService:
    """Drive targeted recovery actions from a read-only encumbrance audit report."""

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn

    def run(self, *, report: AuditReport | None = None) -> dict[str, Any]:
        """Execute the audit-driven recovery loop and return before/after stats."""
        pre_report = report or run_audit(dsn=self.dsn)
        pre_counts = _bucket_counts(pre_report)
        pre_checks = _persistence_checks(pre_report)
        ids_by_bucket = _bucket_foreclosure_ids(pre_report)

        lp_targets = sorted(
            {
                fid
                for bucket in LP_BACKFILL_BUCKETS
                for fid in ids_by_bucket.get(bucket, set())
            }
        )
        ori_retry_targets = sorted(
            {
                fid
                for bucket in ORI_RETRY_BUCKETS
                for fid in ids_by_bucket.get(bucket, set())
            }
            - set(lp_targets)
        )
        noc_targets = sorted(
            {
                fid
                for bucket in RECENT_PERMIT_NOC_BUCKETS
                for fid in ids_by_bucket.get(bucket, set())
            }
        )

        if not lp_targets and not ori_retry_targets and not noc_targets:
            return {
                "skipped": True,
                "reason": "no_recoverable_audit_targets",
                "open_issues_before": len(pre_report.hits),
                "recoverable_bucket_counts_before": _recoverable_bucket_counts(pre_report),
                "all_bucket_counts_before": pre_counts,
                "persistence_checks_before": pre_checks,
                "review_only_bucket_counts_before": {
                    bucket: pre_counts.get(bucket, 0)
                    for bucket in sorted(REVIEW_ONLY_BUCKETS)
                },
            }

        actions: dict[str, dict[str, Any]] = {}
        changed_foreclosure_ids: set[int] = set()
        changed_straps: set[str] = set()
        action_errors = 0

        ori_service = PgOriService(dsn=self.dsn)
        ori_retry_reasons = _retry_reasons_by_foreclosure_id(
            pre_report,
            buckets=ORI_RETRY_BUCKETS,
        )

        if lp_targets:
            lp_result = ori_service.run_lis_pendens_backfill(
                foreclosure_ids=lp_targets,
                require_ori_searched=True,
            )
            actions["lis_pendens_backfill"] = lp_result
            action_errors += int(lp_result.get("errors") or 0)
            changed_foreclosure_ids.update(_changed_foreclosure_ids(lp_result))
            changed_straps.update(_changed_straps(lp_result))

        if ori_retry_targets:
            ori_result = ori_service.run_targeted_recovery(
                foreclosure_ids=ori_retry_targets,
                force_satisfaction_relink=True,
                retry_reasons=ori_retry_reasons,
            )
            actions["ori_targeted_recovery"] = ori_result
            action_errors += int(ori_result.get("errors") or 0)
            changed_foreclosure_ids.update(_changed_foreclosure_ids(ori_result))
            changed_straps.update(_changed_straps(ori_result))

        if noc_targets:
            noc_result = ori_service.run_recent_permit_noc_backfill(
                foreclosure_ids=noc_targets,
                require_ori_searched=True,
            )
            actions["recent_permit_noc_backfill"] = noc_result
            action_errors += int(noc_result.get("errors") or 0)
            changed_foreclosure_ids.update(_changed_foreclosure_ids(noc_result))
            changed_straps.update(_changed_straps(noc_result))

        mortgage_result: dict[str, Any]
        if changed_straps:
            mortgage_result = PgEncumbranceExtractionService(dsn=self.dsn).run(
                straps=sorted(changed_straps),
            )
        else:
            mortgage_result = {"skipped": True, "reason": "no_recovered_straps"}
        actions["mortgage_extract"] = mortgage_result
        action_errors += int(mortgage_result.get("errors") or 0)

        relationship_result: dict[str, Any]
        if changed_straps or changed_foreclosure_ids:
            relationship_result = PgEncumbranceRelationshipService(dsn=self.dsn).run(
                straps=sorted(changed_straps) or None,
                foreclosure_ids=sorted(changed_foreclosure_ids) or None,
            )
        else:
            relationship_result = {
                "skipped": True,
                "reason": "no_recovered_relationship_targets",
            }
        actions["encumbrance_relationships"] = relationship_result
        action_errors += int(relationship_result.get("errors") or 0)

        survival_result: dict[str, Any]
        if changed_foreclosure_ids:
            survival_result = PgSurvivalService(dsn=self.dsn).run(
                foreclosure_ids=sorted(changed_foreclosure_ids),
                force_reanalysis=True,
            )
        else:
            survival_result = {"skipped": True, "reason": "no_recovered_foreclosures"}
        actions["survival_analysis"] = survival_result
        action_errors += int(survival_result.get("errors") or 0)

        post_report = run_audit(dsn=self.dsn)
        post_counts = _bucket_counts(post_report)
        post_checks = _persistence_checks(post_report)
        return {
            "recovery_targets": {
                "lis_pendens": lp_targets,
                "ori_retry": ori_retry_targets,
                "recent_permit_noc": noc_targets,
            },
            "recovered_foreclosure_ids": sorted(changed_foreclosure_ids),
            "recovered_straps": sorted(changed_straps),
            "open_issues_before": len(pre_report.hits),
            "open_issues_after": len(post_report.hits),
            "recoverable_bucket_counts_before": _recoverable_bucket_counts(pre_report),
            "recoverable_bucket_counts_after": _recoverable_bucket_counts(post_report),
            "all_bucket_counts_before": pre_counts,
            "all_bucket_counts_after": post_counts,
            "persistence_checks_before": pre_checks,
            "persistence_checks_after": post_checks,
            "persistence_delta": _persistence_delta(pre_checks, post_checks),
            "review_only_bucket_counts_before": {
                bucket: pre_counts.get(bucket, 0)
                for bucket in sorted(REVIEW_ONLY_BUCKETS)
            },
            "review_only_bucket_counts_after": {
                bucket: post_counts.get(bucket, 0)
                for bucket in sorted(REVIEW_ONLY_BUCKETS)
            },
            "actions": actions,
            "degraded": action_errors > 0,
            "errors": action_errors,
        }
