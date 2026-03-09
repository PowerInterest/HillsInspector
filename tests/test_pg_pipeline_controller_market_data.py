from __future__ import annotations

from typing import Any

from src.services import market_data_dispatcher
from src.services import market_data_worker
from src.services import pg_market_data_scrapling
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


def _disable_scrapling(monkeypatch: Any) -> None:
    monkeypatch.setattr(
        pg_market_data_scrapling,
        "_query_properties_needing_market",
        lambda **_kwargs: [],
    )


def test_run_market_data_runs_inline_by_default(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)
    _disable_scrapling(monkeypatch)
    monkeypatch.setattr(
        market_data_worker,
        "run_market_data_update",
        lambda dsn=None, **_kwargs: {
            "mode": "inline",
            "dsn": dsn,
            "update": {"realtor": 1, "photos": 2},
        },
    )

    result = controller._run_market_data()  # noqa: SLF001

    assert result.status == "success"
    assert result.updated == 3
    assert result.details["worker"]["mode"] == "inline"
    assert result.details["worker"]["dsn"] == controller.dsn
    assert "scrapling" in result.details


def test_run_market_data_uses_background_dispatch_when_enabled(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)
    controller.settings.background_market_data = True
    _disable_scrapling(monkeypatch)
    monkeypatch.setattr(
        market_data_dispatcher,
        "dispatch_market_data_worker",
        lambda dsn, **_kwargs: {"dispatched": True, "mode": "background", "dsn": dsn},
    )

    result = controller._run_market_data()  # noqa: SLF001

    assert result.status == "skipped"
    assert result.details["worker"]["dispatched"] is True
    assert result.details["worker"]["mode"] == "background"
    assert result.details["worker"]["dsn"] == controller.dsn
    assert "scrapling" in result.details


def test_run_market_data_propagates_force_all(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)
    controller.settings.force_all = True
    _disable_scrapling(monkeypatch)

    captured: dict[str, Any] = {}

    def _fake_run_market_data_update(dsn=None, **kwargs: Any) -> dict[str, Any]:
        captured["dsn"] = dsn
        captured.update(kwargs)
        return {
            "mode": "inline",
            "dsn": dsn,
            "force": kwargs.get("force"),
            "update": {"homeharvest": 1},
        }

    monkeypatch.setattr(
        market_data_worker,
        "run_market_data_update",
        _fake_run_market_data_update,
    )

    result = controller._run_market_data()  # noqa: SLF001

    assert captured["dsn"] == controller.dsn
    assert captured["force"] is True
    assert result.status == "success"
    assert result.details["worker"]["force"] is True


def test_run_market_data_marks_degraded_when_worker_payload_has_error(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)
    _disable_scrapling(monkeypatch)
    monkeypatch.setattr(
        market_data_worker,
        "run_market_data_update",
        lambda dsn=None, **_kwargs: {
            "properties_queried": 1,
            "error": "browser_phase_failed:timeout",
            "update": {"realtor": 1, "photos": 0, "error": "browser_phase_failed:timeout"},
            "dsn": dsn,
        },
    )

    result = controller._run_market_data()  # noqa: SLF001

    assert result.status == "degraded"
    assert result.errors == 1
    assert result.updated == 1
