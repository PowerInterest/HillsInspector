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
        settings.skip_encumbrance_extraction = True
        settings.skip_encumbrance_relationships = True
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

    assert result.details["open_issues"] == 2
    assert result.details["affected_foreclosures"] == 2
    assert result.details["with_survival_count"] == 1
    assert result.details["encumbrance_coverage_pct"] == 66.67
    assert result.details["survival_coverage_pct"] == 33.33
    assert result.details["encumbrance_coverage_target_met"] is False
    assert result.details["bucket_counts"] == {"lp_missing": 2}
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
    assert result.details["recovered_foreclosure_ids"] == [1, 2]
    assert controller._encumbrance_audit_report is None  # noqa: SLF001


def test_run_encumbrance_recovery_marks_partial_errors_degraded(
    monkeypatch: Any,
) -> None:
    controller = _build_controller(monkeypatch)
    controller._encumbrance_audit_report = _report()  # noqa: SLF001

    class _FakeRecoveryService:
        def __init__(self, dsn: str | None = None) -> None:
            _ = dsn

        def run(self, *, report: Any | None = None) -> dict[str, Any]:
            assert report is not None
            return {
                "recovered_foreclosure_ids": [1],
                "degraded": True,
                "errors": 1,
            }

    monkeypatch.setattr(
        "src.services.audit.encumbrance_recovery.EncumbranceRecoveryService",
        _FakeRecoveryService,
    )

    result = controller._run_encumbrance_recovery()  # noqa: SLF001

    assert result.status == "degraded"
    assert result.errors == 1
    assert result.updated == 1


def test_run_title_chain_counts_chain_summary_and_event_rows(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)

    class _FakeTitleChainController:
        def __init__(self, _config: Any) -> None:
            pass

        def run(self) -> dict[str, int]:
            return {
                "chain_rows": 7,
                "summary_rows": 2,
                "events_inserted": 11,
            }

    monkeypatch.setattr(
        pg_pipeline_controller,
        "TitleChainController",
        _FakeTitleChainController,
    )

    result = controller._run_title_chain()  # noqa: SLF001

    assert result.status == "success"
    assert result.inserted == 20
    assert result.details["update"]["chain_rows"] == 7
    assert result.details["update"]["summary_rows"] == 2


def test_run_title_breaks_rebuilds_title_chain_after_repairs(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)
    rebuild_calls: list[str] = []
    run_calls: list[int] = []

    class _FakeTitleBreakService:
        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == controller.dsn

        def run(
            self,
            *,
            limit: int | None = None,
            foreclosure_id: int | None = None,
            case_number: str | None = None,
        ) -> dict[str, Any]:
            run_calls.append(len(run_calls) + 1)
            assert limit is None
            assert foreclosure_id is None
            assert case_number is None
            if len(run_calls) == 1:
                return {
                    "targets": 2,
                    "gaps_found": 4,
                    "deeds_inserted": 1,
                    "backfilled": 2,
                    "errors": 0,
                }
            return {
                "targets": 2,
                "gaps_found": 2,
                "deeds_inserted": 0,
                "backfilled": 0,
                "errors": 0,
            }

    class _FakeTitleChainController:
        def __init__(self, _config: Any) -> None:
            rebuild_calls.append("init")

        def run(self) -> dict[str, int]:
            rebuild_calls.append("run")
            return {
                "chain_rows": 8,
                "summary_rows": 2,
                "events_inserted": 13,
            }

    monkeypatch.setattr(
        "src.services.pg_title_break_service.PgTitleBreakService",
        _FakeTitleBreakService,
    )
    monkeypatch.setattr(
        pg_pipeline_controller,
        "TitleChainController",
        _FakeTitleChainController,
    )

    result = controller._run_title_breaks()  # noqa: SLF001

    assert result.status == "success"
    assert result.updated == 3
    assert rebuild_calls == ["init", "run"]
    assert run_calls == [1, 2]
    assert result.details["pass_count"] == 2
    assert result.details["passes"][0]["repairs"] == 3
    assert result.details["passes"][1]["repairs"] == 0
    assert result.details["title_chain_rebuild"]["chain_rows"] == 8


def test_run_title_breaks_runs_two_passes_before_noop_stop(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)
    run_calls: list[int] = []

    class _FakeTitleBreakService:
        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == controller.dsn

        def run(
            self,
            *,
            limit: int | None = None,
            foreclosure_id: int | None = None,
            case_number: str | None = None,
        ) -> dict[str, Any]:
            run_calls.append(len(run_calls) + 1)
            assert limit is None
            assert foreclosure_id is None
            assert case_number is None
            return {
                "targets": 2,
                "gaps_found": 4,
                "deeds_inserted": 0,
                "backfilled": 0,
                "errors": 0,
            }

    class _ExplodingTitleChainController:
        def __init__(self, _config: Any) -> None:
            raise AssertionError("title_chain rebuild should not run")

    monkeypatch.setattr(
        "src.services.pg_title_break_service.PgTitleBreakService",
        _FakeTitleBreakService,
    )
    monkeypatch.setattr(
        pg_pipeline_controller,
        "TitleChainController",
        _ExplodingTitleChainController,
    )

    result = controller._run_title_breaks()  # noqa: SLF001

    assert result.status == "noop"
    assert result.updated == 0
    assert run_calls == [1, 2]
    assert result.details["pass_count"] == 2
    assert "title_chain_rebuild" not in result.details


def test_run_title_breaks_passes_scope_to_service(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)
    controller.settings.foreclosure_id = 21007
    controller.settings.case_number = "292024CA003727A001HC"
    captured: dict[str, Any] = {}

    class _FakeTitleBreakService:
        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == controller.dsn

        def run(
            self,
            *,
            limit: int | None = None,
            foreclosure_id: int | None = None,
            case_number: str | None = None,
        ) -> dict[str, Any]:
            captured["limit"] = limit
            captured["foreclosure_id"] = foreclosure_id
            captured["case_number"] = case_number
            return {
                "targets": 0,
                "gaps_found": 0,
                "deeds_inserted": 0,
                "backfilled": 0,
                "errors": 0,
            }

    monkeypatch.setattr(
        "src.services.pg_title_break_service.PgTitleBreakService",
        _FakeTitleBreakService,
    )

    result = controller._run_title_breaks()  # noqa: SLF001

    assert result.status == "noop"
    assert captured == {
        "limit": None,
        "foreclosure_id": 21007,
        "case_number": "292024CA003727A001HC",
    }


def test_run_title_breaks_marks_sentinel_only_passes_degraded(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)
    run_calls: list[int] = []

    class _FakeTitleBreakService:
        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == controller.dsn

        def run(
            self,
            *,
            limit: int | None = None,
            foreclosure_id: int | None = None,
            case_number: str | None = None,
        ) -> dict[str, Any]:
            run_calls.append(len(run_calls) + 1)
            assert limit is None
            assert foreclosure_id is None
            assert case_number is None
            if len(run_calls) == 1:
                return {
                    "targets": 1,
                    "gaps_found": 1,
                    "deeds_inserted": 0,
                    "backfilled": 0,
                    "sentinels_inserted": 1,
                    "errors": 0,
                }
            return {
                "targets": 0,
                "gaps_found": 0,
                "deeds_inserted": 0,
                "backfilled": 0,
                "sentinels_inserted": 0,
                "errors": 0,
            }

    class _ExplodingTitleChainController:
        def __init__(self, _config: Any) -> None:
            raise AssertionError("title_chain rebuild should not run for sentinel-only passes")

    monkeypatch.setattr(
        "src.services.pg_title_break_service.PgTitleBreakService",
        _FakeTitleBreakService,
    )
    monkeypatch.setattr(
        pg_pipeline_controller,
        "TitleChainController",
        _ExplodingTitleChainController,
    )

    result = controller._run_title_breaks()  # noqa: SLF001

    assert result.status == "degraded"
    assert result.updated == 1
    assert result.errors == 0
    assert run_calls == [1, 2]
    assert result.details["total_sentinels_inserted"] == 1


def test_run_title_breaks_continues_after_second_pass_until_stop_condition(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)
    run_calls: list[int] = []
    rebuild_calls: list[str] = []

    class _FakeTitleBreakService:
        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == controller.dsn

        def run(
            self,
            *,
            limit: int | None = None,
            foreclosure_id: int | None = None,
            case_number: str | None = None,
        ) -> dict[str, Any]:
            run_calls.append(len(run_calls) + 1)
            if len(run_calls) == 1:
                return {"targets": 1, "gaps_found": 3, "deeds_inserted": 1, "backfilled": 0, "errors": 0}
            if len(run_calls) == 2:
                return {"targets": 1, "gaps_found": 2, "deeds_inserted": 0, "backfilled": 1, "errors": 0}
            return {"targets": 1, "gaps_found": 1, "deeds_inserted": 0, "backfilled": 0, "errors": 0}

    class _FakeTitleChainController:
        def __init__(self, _config: Any) -> None:
            rebuild_calls.append("init")

        def run(self) -> dict[str, int]:
            rebuild_calls.append("run")
            return {"chain_rows": 3, "summary_rows": 1, "events_inserted": 5}

    monkeypatch.setattr(
        "src.services.pg_title_break_service.PgTitleBreakService",
        _FakeTitleBreakService,
    )
    monkeypatch.setattr(
        pg_pipeline_controller,
        "TitleChainController",
        _FakeTitleChainController,
    )

    result = controller._run_title_breaks()  # noqa: SLF001

    assert result.status == "success"
    assert result.updated == 2
    assert run_calls == [1, 2, 3]
    assert rebuild_calls == ["init", "run", "init", "run"]
    assert result.details["pass_count"] == 3
    assert result.details["rebuild_count"] == 2


def test_run_title_breaks_hard_caps_after_five_extra_cycles(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)
    run_calls: list[int] = []
    rebuild_calls: list[str] = []

    class _FakeTitleBreakService:
        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == controller.dsn

        def run(
            self,
            *,
            limit: int | None = None,
            foreclosure_id: int | None = None,
            case_number: str | None = None,
        ) -> dict[str, Any]:
            run_calls.append(len(run_calls) + 1)
            return {"targets": 1, "gaps_found": 1, "deeds_inserted": 1, "backfilled": 0, "errors": 0}

    class _FakeTitleChainController:
        def __init__(self, _config: Any) -> None:
            rebuild_calls.append("init")

        def run(self) -> dict[str, int]:
            rebuild_calls.append("run")
            return {"chain_rows": 1, "summary_rows": 1, "events_inserted": 1}

    monkeypatch.setattr(
        "src.services.pg_title_break_service.PgTitleBreakService",
        _FakeTitleBreakService,
    )
    monkeypatch.setattr(
        pg_pipeline_controller,
        "TitleChainController",
        _FakeTitleChainController,
    )

    result = controller._run_title_breaks()  # noqa: SLF001

    assert result.status == "success"
    assert result.updated == 7
    assert run_calls == [1, 2, 3, 4, 5, 6, 7]
    assert rebuild_calls == ["init", "run"] * 7
    assert result.details["pass_count"] == 7
    assert result.details["rebuild_count"] == 7


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
    assert steps["encumbrance_audit"]["status"] == "success"
    assert steps["encumbrance_recovery"]["details"]["update"]["saw_same_state"] is True
    assert steps["encumbrance_recovery"]["details"]["update"]["targets_seen"] == [101, 202]
