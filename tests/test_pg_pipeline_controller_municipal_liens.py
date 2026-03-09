from __future__ import annotations

from typing import Any

from src.services import pg_pipeline_controller


class _DummyEngine:
    pass


def _build_controller(
    monkeypatch: Any,
    settings: pg_pipeline_controller.ControllerSettings,
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
    return pg_pipeline_controller.PgPipelineController(settings)


def test_parse_args_defaults_municipal_liens_enabled(monkeypatch: Any) -> None:
    monkeypatch.setattr("sys.argv", ["Controller.py"])
    settings = pg_pipeline_controller.parse_args()
    assert settings.skip_municipal_liens is False


def test_run_executes_municipal_step_between_ori_and_mortgage(monkeypatch: Any) -> None:
    settings = pg_pipeline_controller.ControllerSettings()
    skip_attrs = [
        "skip_hcpa",
        "skip_clerk_bulk",
        "skip_clerk_criminal",
        "skip_clerk_civil_alpha",
        "skip_nal",
        "skip_flr",
        "skip_sunbiz_entity",
        "skip_county_permits",
        "skip_tampa_permits",
        "skip_single_pin_permits",
        "skip_foreclosure_refresh",
        "skip_trust_accounts",
        "skip_title_chain",
        "skip_title_breaks",
        "skip_auction_scrape",
        "skip_judgment_extract",
        "skip_identifier_recovery",
        "skip_survival",
        "skip_encumbrance_audit",
        "skip_encumbrance_recovery",
        "skip_final_refresh",
        "skip_market_data",
    ]
    for attr in skip_attrs:
        setattr(settings, attr, True)

    controller = _build_controller(monkeypatch, settings)
    call_order: list[str] = []

    monkeypatch.setattr(
        controller,
        "_run_ori_search",
        lambda: call_order.append("ori_search") or {"update": {"ok": True}},
    )
    monkeypatch.setattr(
        controller,
        "_run_municipal_liens_phase0",
        lambda: call_order.append("municipal_liens_phase0") or {"update": {"ok": True}},
    )
    monkeypatch.setattr(
        controller,
        "_run_mortgage_extract",
        lambda: call_order.append("mortgage_extract") or {"update": {"ok": True}},
    )

    result = controller.run()
    step_status = {step["name"]: step["status"] for step in result["steps"]}

    assert call_order == ["ori_search", "municipal_liens_phase0", "mortgage_extract"]
    assert step_status["ori_search"] == "success"
    assert step_status["municipal_liens_phase0"] == "success"
    assert step_status["mortgage_extract"] == "success"

