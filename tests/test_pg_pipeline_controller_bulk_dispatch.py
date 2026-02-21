from __future__ import annotations

from typing import Any

from src.services import controller_step_dispatcher
from src.services import pg_pipeline_controller


class _DummyEngine:
    pass


def _build_controller(
    monkeypatch: Any,
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
    settings = pg_pipeline_controller.ControllerSettings(
    )
    return pg_pipeline_controller.PgPipelineController(settings)


def test_execute_step_dispatches_bulk_steps_by_default(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)
    monkeypatch.setattr(
        controller_step_dispatcher,
        "dispatch_controller_step",
        lambda step_name, dsn, force_all=False: {
            "skipped": True,
            "reason": "step_worker_dispatched_background",
            "dispatched": True,
            "step_name": step_name,
            "dsn": dsn,
            "force_all": force_all,
        },
    )

    def _unexpected_inline() -> dict[str, Any]:
        raise AssertionError("bulk step should be background-dispatched")

    result = controller._execute_step(  # noqa: SLF001
        name="hcpa_suite",
        skip=False,
        fn=_unexpected_inline,
    )

    assert result["status"] == "skipped"
    assert result["reason"] == "step_worker_dispatched_background"
    assert result["payload"]["dispatched"] is True
    assert result["payload"]["step_name"] == "hcpa_suite"


def test_execute_step_runs_inline_for_non_bulk_steps(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)

    def _raise_dispatch(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("non-bulk steps should run inline")

    monkeypatch.setattr(controller_step_dispatcher, "dispatch_controller_step", _raise_dispatch)

    result = controller._execute_step(  # noqa: SLF001
        name="foreclosure_refresh",
        skip=False,
        fn=lambda: {"update": {"ran_inline": True}},
    )

    assert result["status"] == "ok"
    assert result["payload"]["update"]["ran_inline"] is True
