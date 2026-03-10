from __future__ import annotations

from typing import Any
from typing import Self

import pytest

from src.services import market_data_worker
from src.utils.step_result import is_failed_payload


class _FakeResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[Any]:
        return [type("_Row", (), {"_mapping": row})() for row in self._rows]


class _FakeConnection:
    def __init__(self, captured: dict[str, Any], rows: list[dict[str, Any]]) -> None:
        self._captured = captured
        self._rows = rows

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: Any, params: dict[str, Any]) -> _FakeResult:
        self._captured["sql"] = str(sql)
        self._captured["params"] = params
        return _FakeResult(self._rows)


class _FakeEngine:
    def __init__(self, captured: dict[str, Any], rows: list[dict[str, Any]]) -> None:
        self._captured = captured
        self._rows = rows

    def connect(self) -> _FakeConnection:
        return _FakeConnection(self._captured, self._rows)


def test_run_market_data_update_skips_when_no_properties(monkeypatch: Any) -> None:
    monkeypatch.setattr(market_data_worker, "resolve_pg_dsn", lambda _dsn: "postgresql://x")

    def _fake_query(dsn: str, limit: int | None = None) -> list[dict[str, Any]]:
        assert dsn == "postgresql://x"
        assert limit is None
        return []

    monkeypatch.setattr(
        market_data_worker,
        "_query_properties_needing_market",
        _fake_query,
    )

    result = market_data_worker.run_market_data_update()

    assert result["skipped"] is True
    assert result["reason"] == "no_properties_need_market_data"


def test_run_market_data_update_runs_batch_and_refresh(monkeypatch: Any) -> None:
    props = [
        {"strap": "A", "folio": "F-A", "case_number": "C-A", "property_address": "1 Main St"},
        {"strap": "B", "folio": "F-B", "case_number": "C-B", "property_address": "2 Main St"},
    ]

    class _FakeService:
        def __init__(self, dsn: str, **_kwargs: Any) -> None:
            self.dsn = dsn

        async def run_batch(
            self,
            properties: list[dict[str, Any]],
        ) -> dict[str, int]:
                assert properties == props
                return {"redfin": 1, "zillow": 1, "homeharvest": 1, "photos": 2}

    monkeypatch.setattr(market_data_worker, "resolve_pg_dsn", lambda _dsn: "postgresql://x")

    def _fake_query(dsn: str, limit: int | None = None) -> list[dict[str, Any]]:
        assert dsn == "postgresql://x"
        assert limit is None
        return props

    monkeypatch.setattr(
        market_data_worker,
        "_query_properties_needing_market",
        _fake_query,
    )
    monkeypatch.setattr(market_data_worker, "MarketDataService", _FakeService)

    def _fake_refresh(dsn: str) -> dict[str, int]:
        assert dsn == "postgresql://x"
        return {"foreclosures_updated": 2}

    monkeypatch.setattr(
        market_data_worker,
        "refresh_foreclosures",
        _fake_refresh,
    )

    result = market_data_worker.run_market_data_update()

    assert result["properties_queried"] == 2
    assert result["update"]["redfin"] == 1
    assert result["update"]["homeharvest"] == 1
    assert result["update"]["foreclosure_refresh"]["foreclosures_updated"] == 2


def test_run_market_data_update_tolerates_refresh_failure(monkeypatch: Any) -> None:
    props = [
        {"strap": "A", "folio": "F-A", "case_number": "C-A", "property_address": "1 Main St"},
    ]

    class _FakeService:
        def __init__(self, dsn: str, **_kwargs: Any) -> None:
            self.dsn = dsn

        async def run_batch(
            self,
            properties: list[dict[str, Any]],
        ) -> dict[str, int]:
            assert properties == props
            return {"redfin": 0, "zillow": 0, "homeharvest": 1, "photos": 0}

    def _raise_refresh(dsn: str) -> dict[str, int]:
        raise RuntimeError(f"refresh fail {dsn}")

    monkeypatch.setattr(market_data_worker, "resolve_pg_dsn", lambda _dsn: "postgresql://x")

    def _fake_query(dsn: str, limit: int | None = None) -> list[dict[str, Any]]:
        assert dsn == "postgresql://x"
        assert limit is None
        return props

    monkeypatch.setattr(
        market_data_worker,
        "_query_properties_needing_market",
        _fake_query,
    )
    monkeypatch.setattr(market_data_worker, "MarketDataService", _FakeService)
    monkeypatch.setattr(market_data_worker, "refresh_foreclosures", _raise_refresh)

    result = market_data_worker.run_market_data_update()

    assert result["properties_queried"] == 1
    assert result["status"] == "degraded"
    assert result["degraded"] is True
    assert "refresh fail postgresql://x" in result["refresh_error"]
    assert result["update"]["homeharvest"] == 1
    assert result["update"]["foreclosure_refresh_error"] == "refresh fail postgresql://x"


def test_run_market_data_update_propagates_batch_error(monkeypatch: Any) -> None:
    props = [
        {"strap": "A", "folio": "F-A", "case_number": "C-A", "property_address": "1 Main St"},
    ]

    class _FakeService:
        def __init__(self, dsn: str, **_kwargs: Any) -> None:
            self.dsn = dsn

        async def run_batch(
            self,
            properties: list[dict[str, Any]],
        ) -> dict[str, Any]:
            assert properties == props
            return {"redfin": 0, "zillow": 0, "homeharvest": 0, "photos": 0, "error": "browser_phase_failed:timeout"}

    monkeypatch.setattr(market_data_worker, "resolve_pg_dsn", lambda _dsn: "postgresql://x")

    def _fake_query(dsn: str, limit: int | None = None) -> list[dict[str, Any]]:
        assert dsn == "postgresql://x"
        assert limit is None
        return props

    monkeypatch.setattr(
        market_data_worker,
        "_query_properties_needing_market",
        _fake_query,
    )
    monkeypatch.setattr(market_data_worker, "MarketDataService", _FakeService)

    result = market_data_worker.run_market_data_update()

    assert result["properties_queried"] == 1
    assert result["error"] == "browser_phase_failed:timeout"


def test_run_market_data_update_passes_force_to_query(monkeypatch: Any) -> None:
    monkeypatch.setattr(market_data_worker, "resolve_pg_dsn", lambda _dsn: "postgresql://x")
    captured: dict[str, Any] = {}

    def _fake_query(
        dsn: str,
        limit: int | None = None,
        *,
        force: bool = False,
    ) -> list[dict[str, Any]]:
        captured["dsn"] = dsn
        captured["limit"] = limit
        captured["force"] = force
        return []

    monkeypatch.setattr(
        market_data_worker,
        "_query_properties_needing_market",
        _fake_query,
    )

    result = market_data_worker.run_market_data_update(force=True)

    assert result["skipped"] is True
    assert captured == {
        "dsn": "postgresql://x",
        "limit": None,
        "force": True,
    }


def test_query_properties_needing_market_reprocesses_thin_payloads(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}
    expected = [
        {"strap": "A", "folio": "F-A", "case_number": "C-A", "property_address": "1 Main St"},
    ]
    monkeypatch.setattr(
        market_data_worker,
        "get_engine",
        lambda _dsn: _FakeEngine(captured, expected),
    )

    result = market_data_worker._query_properties_needing_market("postgresql://x")  # noqa: SLF001

    assert result == expected
    sql_text = captured["sql"]
    assert "pm.redfin_json->>'_found'" in sql_text
    assert "pm.redfin_json->>'list_price'" in sql_text
    assert "pm.zillow_json->>'zestimate'" in sql_text
    assert "pm.homeharvest_json->>'estimated_value'" in sql_text


def test_run_market_data_update_marks_degraded_when_batch_degraded(monkeypatch: Any) -> None:
    props = [
        {"strap": "A", "folio": "F-A", "case_number": "C-A", "property_address": "1 Main St"},
    ]

    class _FakeService:
        def __init__(self, dsn: str, **_kwargs: Any) -> None:
            self.dsn = dsn

        async def run_batch(
            self,
            properties: list[dict[str, Any]],
        ) -> dict[str, Any]:
            assert properties == props
            return {"redfin": 1, "zillow": 0, "homeharvest": 0, "photos": 0, "degraded": True}

    monkeypatch.setattr(market_data_worker, "resolve_pg_dsn", lambda _dsn: "postgresql://x")
    monkeypatch.setattr(
        market_data_worker,
        "_query_properties_needing_market",
        lambda **_kwargs: props,
    )
    monkeypatch.setattr(market_data_worker, "MarketDataService", _FakeService)
    monkeypatch.setattr(market_data_worker, "refresh_foreclosures", lambda _dsn: {"foreclosures_updated": 1})

    result = market_data_worker.run_market_data_update()

    assert result["status"] == "degraded"
    assert result["update"]["degraded"] is True


def test_payload_failed_detects_nested_update_error() -> None:
    assert not is_failed_payload({"update": {"rows": 1}})
    assert is_failed_payload({"update": {"error": "boom"}})
    assert is_failed_payload({"update": {"success": False}})


def test_main_exits_nonzero_when_nested_update_fails(monkeypatch: Any) -> None:
    monkeypatch.setattr("sys.argv", ["market_data_worker"])
    monkeypatch.setattr(
        market_data_worker,
        "run_market_data_update",
        lambda **_kw: {"properties_queried": 1, "update": {"success": False}},
    )

    with pytest.raises(SystemExit) as exc:
        market_data_worker.main()

    assert exc.value.code == 1
