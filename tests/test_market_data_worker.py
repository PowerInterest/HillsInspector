from __future__ import annotations

from typing import Any

import pytest

from src.services import market_data_worker


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
        def __init__(self, dsn: str) -> None:
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
        def __init__(self, dsn: str) -> None:
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
    assert result["update"]["homeharvest"] == 1
    assert "foreclosure_refresh" not in result["update"]


def test_run_market_data_update_propagates_batch_error(monkeypatch: Any) -> None:
    props = [
        {"strap": "A", "folio": "F-A", "case_number": "C-A", "property_address": "1 Main St"},
    ]

    class _FakeService:
        def __init__(self, dsn: str) -> None:
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


def test_payload_failed_detects_nested_update_error() -> None:
    assert not market_data_worker._payload_failed({"update": {"rows": 1}})  # noqa: SLF001
    assert market_data_worker._payload_failed({"update": {"error": "boom"}})  # noqa: SLF001
    assert market_data_worker._payload_failed({"update": {"success": False}})  # noqa: SLF001


def test_main_exits_nonzero_when_nested_update_fails(monkeypatch: Any) -> None:
    monkeypatch.setattr(
        market_data_worker,
        "run_market_data_update",
        lambda: {"properties_queried": 1, "update": {"success": False}},
    )

    with pytest.raises(SystemExit) as exc:
        market_data_worker.main()

    assert exc.value.code == 1
