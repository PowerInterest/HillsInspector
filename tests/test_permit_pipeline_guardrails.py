from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from src.services import pg_pipeline_controller
from src.services.pg_title_chain_controller import TitleChainController


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
    settings = pg_pipeline_controller.ControllerSettings()
    return pg_pipeline_controller.PgPipelineController(settings)


def test_permit_steps_run_inline_not_background(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)

    assert controller._should_dispatch_bulk_step("county_permits") is False  # noqa: SLF001
    assert controller._should_dispatch_bulk_step("tampa_permits") is False  # noqa: SLF001
    assert controller._should_dispatch_bulk_step("hcpa_suite") is False  # noqa: SLF001


def test_run_tampa_permits_raises_on_zero_rows_for_large_window(
    monkeypatch: Any,
) -> None:
    controller = _build_controller(monkeypatch)
    monkeypatch.setattr(
        controller,
        "_get_table_state",
        lambda *_args, **_kwargs: {"row_count": 1000, "latest_at": date(2026, 1, 15)},
    )
    monkeypatch.setattr(controller, "_should_run", lambda **_kwargs: True)
    monkeypatch.setattr(
        controller,
        "_resolve_tampa_window",
        lambda: (date(2026, 1, 1), date(2026, 1, 31)),
    )

    class _StubTampaPermitService:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def sync_date_range(self, **_kwargs: Any) -> dict[str, int]:
            return {
                "windows_processed": 1,
                "windows_split": 0,
                "csv_rows_total": 0,
                "parsed_total": 0,
                "written_total": 0,
            }

        def enrich_missing_details(self, **_kwargs: Any) -> dict[str, int]:
            return {"selected": 0, "updated": 0, "errors": 0}

    monkeypatch.setattr(
        pg_pipeline_controller,
        "TampaPermitService",
        _StubTampaPermitService,
    )

    with pytest.raises(RuntimeError, match="zero rows"):
        controller._run_tampa_permits()  # noqa: SLF001


def test_permit_event_sql_ignores_blank_addresses() -> None:
    county_sql = TitleChainController._insert_county_permit_events_sql()  # noqa: SLF001
    tampa_sql = TitleChainController._insert_tampa_permit_events_sql()  # noqa: SLF001

    assert "btrim(sc.property_address) <> ''" in county_sql
    assert "btrim(cp.address) <> ''" in county_sql
    assert "btrim(sc.property_address) <> ''" in tampa_sql
    assert "btrim(coalesce(tr.address_normalized, tr.address_raw, '')) <> ''" in tampa_sql
