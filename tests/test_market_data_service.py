from __future__ import annotations

import asyncio
import sys
import types
from typing import Any, Self

from src.services import market_data_service


class _FakeRow:
    def __init__(self, mapping: dict[str, Any]) -> None:
        self._mapping = mapping

    def scalar_value(self) -> Any:
        if len(self._mapping) == 1:
            return next(iter(self._mapping.values()))
        return self._mapping


class _FakeResult:
    def __init__(self, rows: list[_FakeRow]) -> None:
        self._rows = rows

    def fetchall(self) -> list[_FakeRow]:
        return self._rows

    def fetchone(self) -> Any:
        if not self._rows:
            return None
        first = self._rows[0]
        if isinstance(first, _FakeRow):
            return types.SimpleNamespace(**first.scalar_value())
        return first

    def scalar(self) -> Any:
        if not self._rows:
            return None
        first = self._rows[0]
        if isinstance(first, _FakeRow):
            return first.scalar_value()
        return first


class _FakeConnection:
    def __init__(self, captured: dict[str, Any], rows: list[_FakeRow]) -> None:
        self._captured = captured
        self._rows = rows

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: Any, params: Any = None) -> _FakeResult:
        self._captured["sql"] = str(sql)
        if params is not None:
            self._captured["params"] = params
        return _FakeResult(self._rows)


class _FakeTxnConnection(_FakeConnection):
    def execute(self, sql: Any, params: Any = None) -> _FakeResult:
        self._captured["sql"] = str(sql)
        self._captured["params"] = params
        return _FakeResult(self._rows)


class _FakeEngine:
    def __init__(self, captured: dict[str, Any], rows: list[_FakeRow]) -> None:
        self._captured = captured
        self._rows = rows

    def connect(self) -> _FakeConnection:
        return _FakeConnection(self._captured, self._rows)


class _FakeBeginEngine(_FakeEngine):
    def begin(self) -> _FakeTxnConnection:
        return _FakeTxnConnection(self._captured, self._rows)


class _RaisingConnection:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, _sql: Any, _params: Any = None) -> Any:
        raise RuntimeError("db boom")


class _RaisingEngine:
    def connect(self) -> _RaisingConnection:
        return _RaisingConnection()


def test_query_properties_needing_market_includes_photo_backfill_clause(
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {}
    expected = {
        "strap": "A",
        "folio": "F-A",
        "case_number": "C-A",
        "property_address": "1 Main St",
    }
    rows = [_FakeRow(expected)]

    monkeypatch.setattr(
        market_data_service,
        "resolve_pg_dsn",
        lambda _dsn: "postgresql://user:pw@host:5432/db",
    )
    monkeypatch.setattr(
        market_data_service,
        "get_engine",
        lambda _dsn: _FakeEngine(captured, rows),
    )

    result = market_data_service._query_properties_needing_market()  # noqa: SLF001

    assert result == [expected]
    sql_text = captured["sql"].lower()
    assert "photo_cdn_urls" in sql_text
    assert "photo_local_paths" in sql_text
    assert "jsonb_array_length(pm.photo_local_paths) < 15" in sql_text
    assert "redfin-logo" in sql_text


def test_filter_photos_drops_placeholder_urls_and_preserves_real_ones() -> None:
    photos = [
        "https://ssl.cdn-redfin.com/logos/redfin-logo-square-red-1200.png",
        "https://example.com/no_image.jpg",
        "https://photos.zillowstatic.com/fp/real-house-1.webp",
        "https://example.com/property/front.jpg",
    ]

    assert market_data_service._filter_photos(photos) == [  # noqa: SLF001
        "https://photos.zillowstatic.com/fp/real-house-1.webp",
        "https://example.com/property/front.jpg",
    ]


def test_market_result_matches_query_accepts_matching_listing_address() -> None:
    assert market_data_service._market_result_matches_query(  # noqa: SLF001
        "zillow",
        "2535 MIDDLETON GROVE DR 2001",
        listing_address="2535 Middleton Grove Dr, Brandon, FL 33511",
        detail_url=None,
    )


def test_market_result_matches_query_rejects_mismatched_redfin_url() -> None:
    assert not market_data_service._market_result_matches_query(  # noqa: SLF001
        "redfin",
        "2535 MIDDLETON GROVE DR",
        listing_address=None,
        detail_url="https://www.redfin.com/FL/Brandon/604-Julie-Ln-33511/home/47212003",
    )


def test_market_result_matches_query_accepts_matching_realtor_url() -> None:
    assert market_data_service._market_result_matches_query(  # noqa: SLF001
        "realtor",
        "2535 MIDDLETON GROVE DR",
        listing_address=None,
        detail_url=(
            "https://www.realtor.com/realestateandhomes-detail/"
            "2535-Middleton-Grove-Dr_Brandon_FL_33511_M57452-66305"
        ),
    )


def test_addresses_match_canonicalizes_directionals_and_street_types() -> None:
    assert market_data_service._addresses_match(  # noqa: SLF001
        "123 W Oak Ave",
        "123 West Oak Avenue",
    )
    assert market_data_service._addresses_match(  # noqa: SLF001
        "456 Main St",
        "456 Main Street",
    )


def test_repair_stale_detail_urls_updates_rows_with_verified_fallbacks() -> None:
    captured: dict[str, Any] = {}
    svc = object.__new__(market_data_service.MarketDataService)
    svc.__dict__["_engine"] = _FakeBeginEngine(captured, [_FakeRow({"rowcount": 1})])

    repaired = svc._repair_stale_detail_urls()  # noqa: SLF001

    assert repaired == 1
    sql_text = captured["sql"]
    assert "UPDATE property_market" in sql_text
    assert "detail_url LIKE 'https://www.redfin.com/%'" in sql_text
    assert "zillow_json->>'detail_url'" in sql_text
    assert "realtor_json->>'detail_url'" in sql_text


def test_detail_url_upsert_sql_validates_zillow_fallback_for_realtor_and_homeharvest() -> None:
    realtor_sql = market_data_service._detail_url_upsert_sql("realtor")  # noqa: SLF001
    homeharvest_sql = market_data_service._detail_url_upsert_sql("homeharvest")  # noqa: SLF001

    assert "property_market.zillow_json->>'detail_url'" in realtor_sql
    assert "LIKE 'https://www.zillow.com/%'" in realtor_sql
    assert "property_market.zillow_json->>'detail_url'" in homeharvest_sql
    assert "LIKE 'https://www.zillow.com/%'" in homeharvest_sql


def test_payload_has_market_content_rejects_tombstones_and_thin_payloads() -> None:
    assert not market_data_service._payload_has_market_content(  # noqa: SLF001
        "redfin",
        {"_attempted": True, "_found": False},
    )
    assert not market_data_service._payload_has_market_content(  # noqa: SLF001
        "redfin",
        {"address": "1 Main St"},
    )
    assert market_data_service._payload_has_market_content(  # noqa: SLF001
        "redfin",
        {"list_price": 325000},
    )


def test_specs_priority_sql_allows_same_source_refresh_and_higher_priority_upgrade() -> None:
    redfin_sql = market_data_service._specs_priority_sql("beds", source="redfin")  # noqa: SLF001
    realtor_sql = market_data_service._specs_priority_sql("beds", source="realtor")  # noqa: SLF001

    assert "property_market.specs_source = 'redfin'" in redfin_sql
    assert "WHEN 30 > (" in redfin_sql
    assert "property_market.beds IS NULL THEN EXCLUDED.beds" in redfin_sql
    assert "WHEN 10 > (" in realtor_sql


def test_specs_source_upsert_sql_does_not_downgrade_existing_higher_priority_source() -> None:
    sql = market_data_service._specs_source_upsert_sql("realtor")  # noqa: SLF001

    assert "property_market.specs_source IS NULL THEN 'realtor'" in sql
    assert "property_market.specs_source = 'realtor' THEN 'realtor'" in sql
    assert "WHEN 10 > (" in sql


def test_specs_updated_at_upsert_sql_advances_for_null_gap_fills() -> None:
    sql = market_data_service._specs_updated_at_upsert_sql("realtor")  # noqa: SLF001

    assert "property_market.beds IS NULL AND EXCLUDED.beds IS NOT NULL" in sql
    assert "THEN NOW()" in sql


def test_specs_seed_values_only_sets_source_when_spec_data_present() -> None:
    assert market_data_service._specs_seed_values("zillow", {"beds": None, "sqft": None}) == {  # noqa: SLF001
        "specs_source": None,
        "specs_updated_at": None,
    }

    seeded = market_data_service._specs_seed_values("zillow", {"beds": 3, "sqft": None})  # noqa: SLF001
    assert seeded["specs_source"] == "zillow"
    assert seeded["specs_updated_at"] is not None


def test_get_market_state_treats_thin_payloads_as_incomplete(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}
    svc = object.__new__(market_data_service.MarketDataService)
    svc.__dict__["_has_realtor_column"] = True
    svc.__dict__["_engine"] = _FakeEngine(
        captured,
        [
            _FakeRow(
                {
                    "redfin_json": {"address": "1 Main St"},
                    "zillow_json": {"zestimate": 250000},
                    "realtor_json": {"_attempted": True, "_found": False},
                    "homeharvest_json": {"estimated_value": 260000},
                }
            )
        ],
    )

    state = svc._get_market_state("STRAP1")  # noqa: SLF001

    assert state == {
        "has_redfin": False,
        "has_zillow": True,
        "has_hh": True,
        "has_realtor": False,
    }


def test_get_market_state_assumes_complete_on_query_failure() -> None:
    svc = object.__new__(market_data_service.MarketDataService)
    svc.__dict__["_has_realtor_column"] = True
    svc.__dict__["_engine"] = _RaisingEngine()
    svc.__dict__["_market_state_failures"] = 0
    svc.__dict__["_market_state_failure_straps"] = []

    state = svc._get_market_state("STRAP1")  # noqa: SLF001

    assert state == {
        "has_redfin": True,
        "has_zillow": True,
        "has_hh": True,
        "has_realtor": True,
    }
    assert svc.__dict__["_market_state_failures"] == 1
    assert svc.__dict__["_market_state_failure_straps"] == ["STRAP1"]


def test_run_batch_marks_degraded_when_photo_download_has_errors(monkeypatch: Any) -> None:
    svc = object.__new__(market_data_service.MarketDataService)
    svc.__dict__["_has_realtor_column"] = False
    svc.__dict__["_engine"] = None
    monkeypatch.setattr(svc, "_repair_stale_detail_urls", lambda: 0)
    monkeypatch.setattr(
        svc,
        "_get_market_state",
        lambda _strap: {"has_redfin": True, "has_zillow": True, "has_hh": True, "has_realtor": False},
    )
    monkeypatch.setattr(
        svc,
        "_download_all_photos_with_stats",
        lambda _properties: {"downloaded": 2, "errors": 3},
    )

    result = asyncio.run(
        svc.run_batch(
            [{"strap": "STRAP1", "property_address": "1 Main St"}],
            sources=["redfin", "zillow", "homeharvest"],
        )
    )

    assert result["photos"] == 2
    assert result["photo_errors"] == 3
    assert result["degraded"] is True
    assert result["status"] == "degraded"


def test_run_batch_marks_degraded_when_market_state_check_fails(monkeypatch: Any) -> None:
    svc = object.__new__(market_data_service.MarketDataService)
    svc.__dict__["_has_realtor_column"] = False
    svc.__dict__["_engine"] = _RaisingEngine()
    monkeypatch.setattr(svc, "_repair_stale_detail_urls", lambda: 0)
    monkeypatch.setattr(
        svc,
        "_download_all_photos_with_stats",
        lambda _properties: {"downloaded": 0, "errors": 0},
    )

    result = asyncio.run(
        svc.run_batch(
            [{"strap": "STRAP1", "property_address": "1 Main St"}],
            sources=["redfin", "zillow", "homeharvest"],
        )
    )

    assert result["state_check_failures"] == 1
    assert result["state_check_failure_straps"] == ["STRAP1"]
    assert result["degraded"] is True
    assert result["status"] == "degraded"


def test_run_homeharvest_does_not_count_failed_upsert(monkeypatch: Any) -> None:
    svc = object.__new__(market_data_service.MarketDataService)

    class _FakeDataFrame:
        empty = False

        class _ILoc:
            @staticmethod
            def __getitem__(_index: int) -> Any:
                return types.SimpleNamespace(
                    to_dict=lambda: {
                        "estimated_value": 250000,
                        "list_price": 245000,
                        "beds": 3,
                        "full_baths": 2,
                        "sqft": 1800,
                        "year_built": 2004,
                        "property_url": "https://example.com/home",
                    }
                )

        iloc = _ILoc()

    monkeypatch.setitem(sys.modules, "homeharvest", types.SimpleNamespace(scrape_property=lambda **_kwargs: _FakeDataFrame()))
    monkeypatch.setattr(market_data_service, "_build_homeharvest_payload", lambda *_args, **_kwargs: {"list_price": 245000})
    monkeypatch.setattr(svc, "_upsert_homeharvest", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(svc, "_mark_source_attempted", lambda *_args, **_kwargs: None)

    matched, errors = asyncio.run(
        svc._run_homeharvest(  # noqa: SLF001
            [{"strap": "STRAP1", "folio": "FOLIO1", "case_number": "CASE1", "property_address": "1 Main St"}]
        )
    )

    assert matched == 0
    assert errors == 1
