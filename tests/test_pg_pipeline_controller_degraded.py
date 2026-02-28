from __future__ import annotations

from typing import Any

from src.services import pg_pipeline_controller


class _DummyEngine:
    pass


def _build_controller(monkeypatch: Any) -> pg_pipeline_controller.PgPipelineController:
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
    return pg_pipeline_controller.PgPipelineController(
        pg_pipeline_controller.ControllerSettings(),
    )


def test_single_pin_permits_partial_failure_is_degraded(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)
    monkeypatch.setattr(controller, "_missing_tables", lambda _tables: [])
    monkeypatch.setattr(
        controller,
        "_select_single_pin_permit_candidates",
        lambda limit: [{"pin": "123"}, {"pin": "456"}][:limit],
    )

    class _FakePermitService:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def sync_pins_to_postgres(self, _pins: list[str], **_kwargs: Any) -> dict[str, Any]:
            return {
                "pins_targeted": 2,
                "pins_failed": 1,
                "errors": [{"pin": "456"}],
                "permits_observed_total": 0,
                "total_writes": 0,
            }

    monkeypatch.setattr(
        pg_pipeline_controller,
        "PgPermitSinglePinService",
        _FakePermitService,
    )

    result = controller._execute_step(  # noqa: SLF001
        name="single_pin_permits",
        skip=False,
        fn=controller._run_single_pin_permits,  # noqa: SLF001
    )

    assert result["status"] == "degraded"
    assert result["reason"] == "partial_pin_failures"
    assert result["payload"]["failed_pins"] == ["456"]


def test_single_pin_permits_all_failures_mark_step_failed(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)
    monkeypatch.setattr(controller, "_missing_tables", lambda _tables: [])
    monkeypatch.setattr(
        controller,
        "_select_single_pin_permit_candidates",
        lambda limit: [{"pin": "123"}, {"pin": "456"}][:limit],
    )

    class _FakePermitService:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def sync_pins_to_postgres(self, _pins: list[str], **_kwargs: Any) -> dict[str, Any]:
            return {
                "pins_targeted": 2,
                "pins_failed": 2,
                "errors": [{"pin": "123"}, {"pin": "456"}],
                "permits_observed_total": 0,
                "total_writes": 0,
            }

    monkeypatch.setattr(
        pg_pipeline_controller,
        "PgPermitSinglePinService",
        _FakePermitService,
    )

    result = controller._execute_step(  # noqa: SLF001
        name="single_pin_permits",
        skip=False,
        fn=controller._run_single_pin_permits,  # noqa: SLF001
    )

    assert result["status"] == "failed"
    assert result["reason"] == "success_false"
    assert "All 2 targeted pins failed" in result["payload"]["error"]
