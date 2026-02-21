from __future__ import annotations

from typing import Any

from src.services import market_data_dispatcher
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
    settings = pg_pipeline_controller.ControllerSettings()
    return pg_pipeline_controller.PgPipelineController(settings)


def test_run_market_data_uses_background_dispatch_by_default(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)
    monkeypatch.setattr(
        market_data_dispatcher,
        "dispatch_market_data_worker",
        lambda dsn: {"dispatched": True, "mode": "background", "dsn": dsn},
    )

    result = controller._run_market_data()  # noqa: SLF001

    assert result["dispatched"] is True
    assert result["mode"] == "background"
    assert result["dsn"] == controller.dsn
