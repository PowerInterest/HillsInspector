from __future__ import annotations

from typing import Any

from src.services.audit.encumbrance_recovery import EncumbranceRecoveryService
from src.services.audit.pg_audit_encumbrance import AuditReport, BucketHit, BucketSummary


def _report(
    bucket_counts: dict[str, int],
    hits: list[BucketHit],
    *,
    with_encumbrances_count: int = 8,
    with_survival_count: int = 0,
) -> AuditReport:
    return AuditReport(
        active_count=10,
        judged_count=9,
        with_strap_count=9,
        with_encumbrances_count=with_encumbrances_count,
        with_survival_count=with_survival_count,
        summaries=[
            BucketSummary(
                bucket=bucket,
                description=bucket,
                count=count,
            )
            for bucket, count in bucket_counts.items()
        ],
        hits=hits,
    )


def _hit(bucket: str, foreclosure_id: int, strap: str) -> BucketHit:
    return BucketHit(
        bucket=bucket,
        foreclosure_id=foreclosure_id,
        case_number=f"24-CA-{foreclosure_id:06d}",
        strap=strap,
        property_address=f"{foreclosure_id} Main St",
        reason=bucket,
    )


def test_recovery_skips_when_audit_has_only_review_buckets() -> None:
    report = _report(
        {
            "lp_to_judgment_property_change": 1,
            "long_case_interim_risk": 1,
        },
        [
            _hit("lp_to_judgment_property_change", 4, "S4"),
            _hit("long_case_interim_risk", 5, "S5"),
        ],
    )

    result = EncumbranceRecoveryService(dsn="postgresql://db").run(report=report)

    assert result["skipped"] is True
    assert result["reason"] == "no_recoverable_audit_targets"
    assert result["review_only_bucket_counts_before"]["lp_to_judgment_property_change"] == 1
    assert result["recoverable_bucket_counts_before"]["lp_missing"] == 0
    assert result["persistence_checks_before"]["open_issues"] == 2
    assert result["persistence_checks_before"]["recoverable_open_issues"] == 0


def test_recovery_maps_buckets_to_targeted_services(monkeypatch: Any) -> None:
    pre_report = _report(
        {
            "lp_missing": 1,
            "construction_lien_risk": 1,
            "sat_parent_gap": 1,
            "lp_to_judgment_property_change": 1,
        },
        [
            _hit("lp_missing", 1, "S1"),
            _hit("construction_lien_risk", 2, "S2"),
            _hit("sat_parent_gap", 3, "S3"),
            _hit("lp_to_judgment_property_change", 4, "S4"),
        ],
        with_encumbrances_count=6,
        with_survival_count=3,
    )
    post_report = _report(
        {
            "lp_missing": 0,
            "construction_lien_risk": 0,
            "sat_parent_gap": 0,
            "lp_to_judgment_property_change": 1,
        },
        [
            _hit("lp_to_judgment_property_change", 4, "S4"),
        ],
        with_encumbrances_count=8,
        with_survival_count=7,
    )
    reports = iter([pre_report, post_report])
    calls: dict[str, Any] = {}

    class _FakeOriService:
        def __init__(self, dsn: str | None = None) -> None:
            calls["ori_dsn"] = dsn

        def run_lis_pendens_backfill(self, **kwargs: Any) -> dict[str, Any]:
            calls["lp_kwargs"] = kwargs
            return {
                "errors": 0,
                "per_target": [
                    {"foreclosure_id": 1, "strap": "S1", "saved": 1},
                ],
            }

        def run_targeted_recovery(self, **kwargs: Any) -> dict[str, Any]:
            calls["ori_kwargs"] = kwargs
            return {
                "errors": 0,
                "per_target": [
                    {"foreclosure_id": 2, "strap": "S2", "saved": 1},
                    {"foreclosure_id": 3, "strap": "S3", "saved": 0, "satisfactions_linked": 1},
                ],
            }

        def run_recent_permit_noc_backfill(self, **kwargs: Any) -> dict[str, Any]:
            calls["noc_kwargs"] = kwargs
            return {
                "errors": 0,
                "per_target": [
                    {"foreclosure_id": 2, "strap": "S2", "saved": 1},
                ],
            }

    class _FakeMortgageService:
        def __init__(self, dsn: str | None = None) -> None:
            calls["mortgage_dsn"] = dsn

        def run(self, *, straps: list[str] | None = None, limit: int | None = None) -> dict[str, Any]:
            calls["mortgage_straps"] = straps
            calls["mortgage_limit"] = limit
            return {"mortgages_found": 2, "mortgages_extracted": 2, "errors": 0}

    class _FakeSurvivalService:
        def __init__(self, dsn: str | None = None) -> None:
            calls["survival_dsn"] = dsn

        def run(
            self,
            *,
            foreclosure_ids: list[int] | None = None,
            force_reanalysis: bool = False,
            limit: int | None = None,
        ) -> dict[str, Any]:
            calls["survival_ids"] = foreclosure_ids
            calls["survival_force"] = force_reanalysis
            calls["survival_limit"] = limit
            return {"targets": 3, "analyzed": 3, "errors": 0}

    def _fake_run_audit(*, dsn: str | None = None) -> AuditReport:
        _ = dsn
        return next(reports)

    monkeypatch.setattr(
        "src.services.audit.encumbrance_recovery.run_audit",
        _fake_run_audit,
    )
    monkeypatch.setattr(
        "src.services.audit.encumbrance_recovery.PgOriService",
        _FakeOriService,
    )
    monkeypatch.setattr(
        "src.services.audit.encumbrance_recovery.PgMortgageExtractionService",
        _FakeMortgageService,
    )
    monkeypatch.setattr(
        "src.services.audit.encumbrance_recovery.PgSurvivalService",
        _FakeSurvivalService,
    )

    result = EncumbranceRecoveryService(dsn="postgresql://db").run()

    assert calls["lp_kwargs"]["foreclosure_ids"] == [1]
    assert calls["ori_kwargs"]["foreclosure_ids"] == [2, 3]
    assert calls["ori_kwargs"]["force_satisfaction_relink"] is True
    assert calls["noc_kwargs"]["foreclosure_ids"] == [2]
    assert calls["mortgage_straps"] == ["S1", "S2", "S3"]
    assert calls["survival_ids"] == [1, 2, 3]
    assert calls["survival_force"] is True
    assert result["recoverable_bucket_counts_before"]["lp_missing"] == 1
    assert result["recoverable_bucket_counts_after"]["lp_missing"] == 0
    assert result["review_only_bucket_counts_after"]["lp_to_judgment_property_change"] == 1
    assert result["open_issues_before"] == 4
    assert result["open_issues_after"] == 1
    assert result["persistence_checks_before"]["with_encumbrances"] == 6
    assert result["persistence_checks_after"]["with_encumbrances"] == 8
    assert result["persistence_delta"]["with_encumbrances_delta"] == 2
    assert result["persistence_delta"]["with_survival_delta"] == 4
    assert result["persistence_delta"]["open_issues_delta"] == -3
    assert result["degraded"] is False


def test_recovery_marks_step_degraded_when_actions_error(monkeypatch: Any) -> None:
    report = _report(
        {"sat_parent_gap": 1},
        [_hit("sat_parent_gap", 9, "S9")],
    )

    class _FakeOriService:
        def __init__(self, dsn: str | None = None) -> None:
            pass

        def run_targeted_recovery(self, **_kwargs: Any) -> dict[str, Any]:
            return {"errors": 1, "per_target": []}

    def _fake_run_audit(*, dsn: str | None = None) -> AuditReport:
        _ = dsn
        return report

    monkeypatch.setattr(
        "src.services.audit.encumbrance_recovery.run_audit",
        _fake_run_audit,
    )
    monkeypatch.setattr(
        "src.services.audit.encumbrance_recovery.PgOriService",
        _FakeOriService,
    )
    result = EncumbranceRecoveryService(dsn="postgresql://db").run(report=report)

    assert result["degraded"] is True
    assert result["errors"] == 1
    assert result["actions"]["mortgage_extract"]["reason"] == "no_recovered_straps"
    assert result["actions"]["survival_analysis"]["reason"] == "no_recovered_foreclosures"
