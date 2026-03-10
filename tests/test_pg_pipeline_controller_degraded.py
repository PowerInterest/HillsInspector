from __future__ import annotations

from typing import Any
from typing import Self

from src.services import market_data_worker
from src.services import pg_market_data_scrapling
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

    assert result.status == "degraded"
    assert result.details["reason"] == "partial_pin_failures"
    assert result.details["failed_pins"] == ["456"]


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

    assert result.status == "failed"
    assert "All 2 targeted pins failed" in result.details["error"]


def test_single_pin_candidate_sql_excludes_tampa_violation_rows(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    class _FakeMappings:
        def all(self) -> list[dict[str, Any]]:
            return []

    class _FakeResult:
        def mappings(self) -> _FakeMappings:
            return _FakeMappings()

    class _FakeConnection:
        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, sql: Any, params: dict[str, Any]) -> _FakeResult:
            captured["sql"] = str(sql)
            captured["params"] = params
            return _FakeResult()

    class _FakeEngine:
        def connect(self) -> _FakeConnection:
            return _FakeConnection()

    monkeypatch.setattr(
        pg_pipeline_controller,
        "resolve_pg_dsn",
        lambda _dsn: "postgresql://user:pw@host:5432/db",
    )
    monkeypatch.setattr(
        pg_pipeline_controller,
        "get_engine",
        lambda _dsn: _FakeEngine(),
    )

    controller = pg_pipeline_controller.PgPipelineController(
        pg_pipeline_controller.ControllerSettings(),
    )

    assert controller._select_single_pin_permit_candidates(limit=25) == []  # noqa: SLF001
    assert captured["params"]["pin_limit"] == 25
    sql_text = captured["sql"].lower()
    assert "coalesce(tr.is_violation, false) = false" in sql_text
    assert "coalesce(tr.module, '') <> 'business'" in sql_text
    assert "coalesce(tr.record_number, '') not like 'btx-%'" in sql_text
    assert "coalesce(tr.record_type, '') not ilike 'tax receipt%'" in sql_text


def test_market_data_nested_degraded_marks_step_degraded(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)

    monkeypatch.setattr(
        pg_market_data_scrapling,
        "_query_properties_needing_market",
        lambda **_kwargs: [{"strap": "A", "property_address": "1 Main St"}],
    )

    class _FakeScraplingService:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        async def run_batch(
            self,
            properties: list[dict[str, Any]],
            sources: list[str] | None = None,
        ) -> dict[str, Any]:
            assert properties == [{"strap": "A", "property_address": "1 Main St"}]
            assert sources == ["realtor"]
            return {"realtor": 1}

    monkeypatch.setattr(
        pg_market_data_scrapling,
        "PgMarketDataScraplingService",
        _FakeScraplingService,
    )
    monkeypatch.setattr(
        market_data_worker,
        "run_market_data_update",
        lambda **_kwargs: {
            "properties_queried": 1,
            "status": "degraded",
            "update": {"redfin": 1, "degraded": True, "photo_errors": 2},
        },
    )

    result = controller._run_market_data()  # noqa: SLF001

    assert result.status == "degraded"
    assert result.details["worker"]["update"]["photo_errors"] == 2
