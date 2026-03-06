from __future__ import annotations

from typing import Any, Self

from src.services import pg_pipeline_controller
from src.services.audit.pg_audit_encumbrance import AuditReport, BucketHit, BucketSummary


class _DummyConnection:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> bool:
        return False

    def execute(self, _statement: object, _params: dict[str, object] | None = None) -> None:
        return None

    def commit(self) -> None:
        return None


class _DummyEngine:
    def connect(self) -> _DummyConnection:
        return _DummyConnection()


def _build_controller(
    monkeypatch: Any,
    *,
    audit_only: bool = False,
) -> pg_pipeline_controller.PgPipelineController:
    monkeypatch.setattr(
        pg_pipeline_controller,
        "resolve_pg_dsn",
        lambda _dsn: "postgresql://user:pw@host:5432/db",
    )
    monkeypatch.setattr(
        pg_pipeline_controller,
        "get_engine",
        lambda _dsn: _DummyEngine(),
    )
    settings = pg_pipeline_controller.ControllerSettings()
    if audit_only:
        settings.skip_hcpa = True
        settings.skip_clerk_bulk = True
        settings.skip_clerk_criminal = True
        settings.skip_clerk_civil_alpha = True
        settings.skip_nal = True
        settings.skip_flr = True
        settings.skip_sunbiz_entity = True
        settings.skip_county_permits = True
        settings.skip_tampa_permits = True
        settings.skip_single_pin_permits = True
        settings.skip_foreclosure_refresh = True
        settings.skip_trust_accounts = True
        settings.skip_title_chain = True
        settings.skip_title_breaks = True
        settings.skip_auction_scrape = True
        settings.skip_judgment_extract = True
        settings.skip_identifier_recovery = True
        settings.skip_ori_search = True
        settings.skip_municipal_liens = True
        settings.skip_mortgage_extract = True
        settings.skip_survival = True
        settings.skip_final_refresh = True
        settings.skip_market_data = True
    return pg_pipeline_controller.PgPipelineController(settings)


def _report() -> AuditReport:
    return AuditReport(
        active_count=3,
        judged_count=3,
        with_strap_count=3,
        with_encumbrances_count=2,
        with_survival_count=1,
        summaries=[
            BucketSummary(
                bucket="lp_missing",
                description="Missing Lis Pendens",
                count=2,
            ),
        ],
        hits=[
            BucketHit(
                bucket="lp_missing",
                foreclosure_id=1,
                case_number="24-CA-000001",
                strap="S1",
                property_address="1 Main St",
                reason="missing lp",
            ),
            BucketHit(
                bucket="lp_missing",
                foreclosure_id=2,
                case_number="24-CA-000002",
                strap="S2",
                property_address="2 Main St",
                reason="missing lp",
            ),
        ],
    )


def test_run_encumbrance_audit_caches_report(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)
    report = _report()

    def _fake_run_audit(*, dsn: str | None = None) -> AuditReport:
        _ = dsn
        return report

    monkeypatch.setattr(
        "src.services.audit.pg_audit_encumbrance.run_audit",
        _fake_run_audit,
    )

    result = controller._run_encumbrance_audit()  # noqa: SLF001

    assert result["open_issues"] == 2
    assert result["affected_foreclosures"] == 2
    assert result["with_survival_count"] == 1
    assert result["encumbrance_coverage_pct"] == 66.67
    assert result["survival_coverage_pct"] == 33.33
    assert result["encumbrance_coverage_target_met"] is False
    assert result["bucket_counts"] == {"lp_missing": 2}
    assert controller._encumbrance_audit_report is report  # noqa: SLF001


def test_run_encumbrance_recovery_passes_cached_report_and_clears_state(
    monkeypatch: Any,
) -> None:
    controller = _build_controller(monkeypatch)
    cached_report = _report()
    controller._encumbrance_audit_report = cached_report  # noqa: SLF001
    captured: dict[str, Any] = {}

    class _FakeRecoveryService:
        def __init__(self, dsn: str | None = None) -> None:
            captured["dsn"] = dsn

        def run(self, *, report: Any | None = None) -> dict[str, Any]:
            captured["report"] = report
            return {"recovered_foreclosure_ids": [1, 2]}

    monkeypatch.setattr(
        "src.services.audit.encumbrance_recovery.EncumbranceRecoveryService",
        _FakeRecoveryService,
    )

    result = controller._run_encumbrance_recovery()  # noqa: SLF001

    assert captured["dsn"] == controller.dsn
    assert captured["report"] is cached_report
    assert result["recovered_foreclosure_ids"] == [1, 2]
    assert controller._encumbrance_audit_report is None  # noqa: SLF001


def test_run_executes_recovery_after_audit_with_shared_state(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch, audit_only=True)
    shared_report = {"targets": [101, 202]}
    call_order: list[str] = []

    def fake_audit() -> dict[str, Any]:
        call_order.append("encumbrance_audit")
        controller._encumbrance_audit_report = shared_report  # noqa: SLF001
        return {"update": {"target_count": 2}}

    def fake_recovery() -> dict[str, Any]:
        call_order.append("encumbrance_recovery")
        return {
            "update": {
                "saw_same_state": controller._encumbrance_audit_report is shared_report,  # noqa: SLF001
                "targets_seen": list(shared_report["targets"]),
            }
        }

    monkeypatch.setattr(controller, "_run_encumbrance_audit", fake_audit)
    monkeypatch.setattr(controller, "_run_encumbrance_recovery", fake_recovery)

    result = controller.run()
    steps = {step["name"]: step for step in result["steps"]}

    assert call_order == ["encumbrance_audit", "encumbrance_recovery"]
    assert steps["encumbrance_audit"]["status"] == "ok"
    assert steps["encumbrance_recovery"]["payload"]["update"]["saw_same_state"] is True
    assert steps["encumbrance_recovery"]["payload"]["update"]["targets_seen"] == [101, 202]
