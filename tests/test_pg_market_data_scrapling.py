# ruff: noqa: SLF001
from __future__ import annotations

from typing import Any, Self

import pytest

from src.services import pg_market_data_scrapling
from src.services.pg_market_data_scrapling import PgMarketDataScraplingService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> _FakeResult:
        return self

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class _FakeConnection:
    def __init__(self, captured: dict[str, Any], rows: list[dict[str, Any]]) -> None:
        self._captured = captured
        self._rows = rows

    def execute(self, sql: Any, params: dict[str, Any] | None = None) -> _FakeResult:
        self._captured["sql"] = str(sql)
        self._captured["params"] = params or {}
        return _FakeResult(self._rows)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class _FakeEngine:
    def __init__(self, captured: dict[str, Any], rows: list[dict[str, Any]]) -> None:
        self._captured = captured
        self._rows = rows

    def connect(self) -> _FakeConnection:
        return _FakeConnection(self._captured, self._rows)


# ---------------------------------------------------------------------------
# Query tests
# ---------------------------------------------------------------------------


def test_query_properties_needing_market_includes_limit(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}
    expected = [
        {"strap": "S1", "folio": "F1", "case_number": "C1", "property_address": "1 Main St"},
    ]

    monkeypatch.setattr(pg_market_data_scrapling, "get_engine", lambda _dsn: _FakeEngine(captured, expected))

    result = pg_market_data_scrapling._query_properties_needing_market(
        "postgresql://user:pw@localhost:5432/db",
        limit=7,
    )

    assert result == expected
    assert captured["params"]["limit"] == 7
    sql = captured["sql"].lower()
    assert "pm.redfin_json" in sql
    assert "pm.zillow_json" in sql
    assert "pm.homeharvest_json" in sql


# ---------------------------------------------------------------------------
# Normalization tests
# ---------------------------------------------------------------------------


class TestNormalizeFloat:
    def test_dollar_string(self) -> None:
        assert PgMarketDataScraplingService._normalize_float("$325,000") == 325000.0

    def test_negative_returns_none(self) -> None:
        assert PgMarketDataScraplingService._normalize_float(-1) is None

    def test_none_returns_none(self) -> None:
        assert PgMarketDataScraplingService._normalize_float(None) is None

    def test_empty_returns_none(self) -> None:
        assert PgMarketDataScraplingService._normalize_float("") is None

    def test_numeric(self) -> None:
        assert PgMarketDataScraplingService._normalize_float(42) == 42.0

    def test_zero(self) -> None:
        assert PgMarketDataScraplingService._normalize_float(0) == 0.0

    def test_garbage_returns_none(self) -> None:
        assert PgMarketDataScraplingService._normalize_float("not-a-number") is None


class TestNormalizeInt:
    def test_comma_string(self) -> None:
        assert PgMarketDataScraplingService._normalize_int("1,500") == 1500

    def test_none_returns_none(self) -> None:
        assert PgMarketDataScraplingService._normalize_int(None) is None

    def test_int_passthrough(self) -> None:
        assert PgMarketDataScraplingService._normalize_int(42) == 42


class TestNormalizeIntOrFloat:
    def test_integer_value(self) -> None:
        result = PgMarketDataScraplingService._normalize_int_or_float("3")
        assert result == 3
        assert isinstance(result, int)

    def test_fractional_value(self) -> None:
        assert PgMarketDataScraplingService._normalize_int_or_float("2.5") == 2.5


# ---------------------------------------------------------------------------
# URL builder tests
# ---------------------------------------------------------------------------


class TestToRealtorUrl:
    def test_basic_address(self) -> None:
        url = PgMarketDataScraplingService._to_realtor_url("123 Main St Tampa FL 33602")
        assert url == "https://www.realtor.com/realestateandhomes-search/123-main-st-tampa-fl-33602"

    def test_special_chars_replaced(self) -> None:
        url = PgMarketDataScraplingService._to_realtor_url("123 Main St #4, Tampa")
        assert "#" not in url
        assert "," not in url

    def test_empty_raises(self) -> None:
        with pytest.raises(ValueError, match="empty address"):
            PgMarketDataScraplingService._to_realtor_url("   ")


# ---------------------------------------------------------------------------
# JSON extraction tests
# ---------------------------------------------------------------------------


class TestExtractJsonFromScript:
    def test_extracts_simple_object(self) -> None:
        script = 'window.__NEXT_DATA__ = {"key": "value"};'
        result = PgMarketDataScraplingService._extract_json_from_script(script, "__NEXT_DATA__")
        assert result == {"key": "value"}

    def test_returns_none_for_missing_marker(self) -> None:
        result = PgMarketDataScraplingService._extract_json_from_script("no marker here", "__NEXT_DATA__")
        assert result is None

    def test_returns_none_for_invalid_json(self) -> None:
        result = PgMarketDataScraplingService._extract_json_from_script("__NEXT_DATA__ = {broken", "__NEXT_DATA__")
        assert result is None

    def test_nested_objects(self) -> None:
        script = '__NEXT_DATA__ = {"a": {"b": 1}}'
        result = PgMarketDataScraplingService._extract_json_from_script(script, "__NEXT_DATA__")
        assert result == {"a": {"b": 1}}


# ---------------------------------------------------------------------------
# Safe list tests
# ---------------------------------------------------------------------------


class TestSafeList:
    def test_string_input(self) -> None:
        assert PgMarketDataScraplingService._safe_list("hello") == ["hello"]

    def test_list_input(self) -> None:
        assert PgMarketDataScraplingService._safe_list(["a", "b"]) == ["a", "b"]

    def test_dict_with_href(self) -> None:
        result = PgMarketDataScraplingService._safe_list([{"href": "http://x.com/photo.jpg"}])
        assert result == ["http://x.com/photo.jpg"]

    def test_empty_strings_filtered(self) -> None:
        assert PgMarketDataScraplingService._safe_list(["", "  ", "valid"]) == ["valid"]

    def test_none_returns_empty(self) -> None:
        assert PgMarketDataScraplingService._safe_list(None) == []

    def test_integer_returns_empty(self) -> None:
        assert PgMarketDataScraplingService._safe_list(42) == []


# ---------------------------------------------------------------------------
# Payload node finder tests
# ---------------------------------------------------------------------------


class TestFindRealtorPayloadNode:
    def test_finds_direct_match(self) -> None:
        payload = {"list_price": 300000, "beds": 3}
        assert PgMarketDataScraplingService._find_realtor_payload_node(payload) is payload

    def test_finds_nested_match(self) -> None:
        inner = {"list_price": 300000}
        payload = {"data": {"props": {"pageProps": inner}}}
        assert PgMarketDataScraplingService._find_realtor_payload_node(payload) is inner

    def test_returns_none_for_empty(self) -> None:
        assert PgMarketDataScraplingService._find_realtor_payload_node({}) is None

    def test_handles_list_payload(self) -> None:
        inner = {"list_price": 200000}
        payload = [{"unrelated": True}, inner]
        assert PgMarketDataScraplingService._find_realtor_payload_node(payload) is inner


# ---------------------------------------------------------------------------
# Payload extraction tests
# ---------------------------------------------------------------------------


class TestExtractRealtorPayloadFromNode:
    def test_basic_extraction(self) -> None:
        node = {
            "list_price": 250000,
            "beds": 3,
            "baths": 2,
            "sqft": 1500,
            "year_built": 1990,
            "homeStatus": "FOR_SALE",
        }
        result = PgMarketDataScraplingService._extract_realtor_payload_from_node(
            node, "123 Main St", "http://realtor.com/detail"
        )
        assert result["list_price"] == 250000
        assert result["beds"] == 3
        assert result["listing_status"] == "FOR_SALE"
        assert result["_source_address"] == "123 Main St"

    def test_price_fallback_from_offers(self) -> None:
        node = {"offers": {"price": 180000}}
        result = PgMarketDataScraplingService._extract_realtor_payload_from_node(
            node, "addr", "http://url"
        )
        assert result["list_price"] == 180000

    def test_estimate_dict_value(self) -> None:
        node = {"estimate": {"value": 300000}}
        result = PgMarketDataScraplingService._extract_realtor_payload_from_node(
            node, "addr", "http://url"
        )
        assert result["zestimate"] == 300000

    def test_is_for_sale_fallback(self) -> None:
        node = {"is_for_sale": True}
        result = PgMarketDataScraplingService._extract_realtor_payload_from_node(
            node, "addr", "http://url"
        )
        assert result["listing_status"] == "FOR_SALE"


# ---------------------------------------------------------------------------
# JSON-LD extraction tests
# ---------------------------------------------------------------------------


class TestExtractRealtorPayloadFromJsonld:
    def test_extracts_from_listing_type(self) -> None:
        nodes = [{"@type": "RealEstateListing", "url": "http://listing"}]
        result = PgMarketDataScraplingService._extract_realtor_payload_from_jsonld(
            nodes, "addr", "http://fallback"
        )
        assert result is not None
        assert result["detail_url"] == "http://listing"

    def test_extracts_price_from_offers(self) -> None:
        nodes = [{"@type": "House", "offers": {"price": 250000}}]
        result = PgMarketDataScraplingService._extract_realtor_payload_from_jsonld(
            nodes, "addr", "http://fallback"
        )
        assert result is not None
        assert result["list_price"] == 250000

    def test_returns_none_for_unrecognized_types(self) -> None:
        nodes = [{"@type": "Organization"}]
        result = PgMarketDataScraplingService._extract_realtor_payload_from_jsonld(
            nodes, "addr", "http://fallback"
        )
        assert result is None


# ---------------------------------------------------------------------------
# Usefulness check tests
# ---------------------------------------------------------------------------


class TestIsUsefulRealtorPayload:
    def test_empty_is_not_useful(self) -> None:
        svc = object.__new__(PgMarketDataScraplingService)
        assert svc._is_useful_realtor_payload({}) is False

    def test_with_price_is_useful(self) -> None:
        svc = object.__new__(PgMarketDataScraplingService)
        assert svc._is_useful_realtor_payload({"list_price": 300000}) is True

    def test_with_photos_only_is_useful(self) -> None:
        svc = object.__new__(PgMarketDataScraplingService)
        assert svc._is_useful_realtor_payload({"photos": ["http://photo.jpg"]}) is True

    def test_none_values_not_useful(self) -> None:
        svc = object.__new__(PgMarketDataScraplingService)
        assert svc._is_useful_realtor_payload({"list_price": None, "beds": None}) is False


# ---------------------------------------------------------------------------
# run_market_data_update tests
# ---------------------------------------------------------------------------


def test_run_market_data_update_runs_batch_and_refresh(monkeypatch: Any) -> None:
    props = [
        {"strap": "A", "folio": "F-A", "case_number": "C-A", "property_address": "1 Main St"},
        {"strap": "B", "folio": "F-B", "case_number": "C-B", "property_address": "2 Main St"},
    ]

    class _FakeService:
        def __init__(self, dsn: str | None = None, **_: Any) -> None:
            assert dsn == "postgresql://x"

        async def run_batch(self, properties: list[dict[str, Any]]) -> dict[str, Any]:
            assert properties == props
            return {
                "redfin": 1,
                "zillow": 1,
                "realtor": 1,
                "homeharvest": 1,
                "photos": 2,
            }

    monkeypatch.setattr(pg_market_data_scrapling, "resolve_pg_dsn", lambda _dsn: "postgresql://x")
    monkeypatch.setattr(pg_market_data_scrapling, "_query_properties_needing_market", lambda **_kwargs: props)
    monkeypatch.setattr(pg_market_data_scrapling, "PgMarketDataScraplingService", _FakeService)
    monkeypatch.setattr(pg_market_data_scrapling, "refresh_foreclosures", lambda **_kwargs: {"foreclosures_updated": 2})

    result = pg_market_data_scrapling.run_market_data_update()

    assert result["properties_queried"] == 2
    assert result["update"]["realtor"] == 1
    assert result["update"]["foreclosure_refresh"]["foreclosures_updated"] == 2


def test_run_market_data_update_skips_when_no_properties(monkeypatch: Any) -> None:
    monkeypatch.setattr(pg_market_data_scrapling, "resolve_pg_dsn", lambda _dsn: "postgresql://x")
    monkeypatch.setattr(pg_market_data_scrapling, "_query_properties_needing_market", lambda **_kwargs: [])

    result = pg_market_data_scrapling.run_market_data_update()

    assert result["skipped"] is True
    assert result["reason"] == "no_properties_need_market_data"


def test_run_market_data_update_propagates_batch_error(monkeypatch: Any) -> None:
    props = [
        {"strap": "A", "folio": "F-A", "case_number": "C-A", "property_address": "1 Main St"},
    ]

    class _FakeService:
        def __init__(self, dsn: str | None = None, **_: Any) -> None:
            assert dsn == "postgresql://x"

        async def run_batch(self, properties: list[dict[str, Any]]) -> dict[str, Any]:
            assert properties == props
            return {"redfin": 0, "zillow": 0, "realtor": 0, "homeharvest": 0, "photos": 0, "error": "browser_phase_failed:timeout"}

    monkeypatch.setattr(pg_market_data_scrapling, "resolve_pg_dsn", lambda _dsn: "postgresql://x")
    monkeypatch.setattr(pg_market_data_scrapling, "_query_properties_needing_market", lambda **_kwargs: props)
    monkeypatch.setattr(pg_market_data_scrapling, "PgMarketDataScraplingService", _FakeService)

    result = pg_market_data_scrapling.run_market_data_update()

    assert result["error"] == "browser_phase_failed:timeout"


def test_run_market_data_update_tolerates_refresh_failure(monkeypatch: Any) -> None:
    props = [
        {"strap": "A", "folio": "F-A", "case_number": "C-A", "property_address": "1 Main St"},
    ]

    class _FakeService:
        def __init__(self, dsn: str | None = None, **_: Any) -> None:
            pass

        async def run_batch(self, properties: list[dict[str, Any]]) -> dict[str, Any]:
            return {"redfin": 0, "zillow": 0, "realtor": 1, "homeharvest": 0, "photos": 0}

    monkeypatch.setattr(pg_market_data_scrapling, "resolve_pg_dsn", lambda _dsn: "postgresql://x")
    monkeypatch.setattr(pg_market_data_scrapling, "_query_properties_needing_market", lambda **_kwargs: props)
    monkeypatch.setattr(pg_market_data_scrapling, "PgMarketDataScraplingService", _FakeService)
    monkeypatch.setattr(pg_market_data_scrapling, "refresh_foreclosures", lambda **_kw: (_ for _ in ()).throw(RuntimeError("boom")))

    result = pg_market_data_scrapling.run_market_data_update()

    assert result["properties_queried"] == 1
    assert result["update"]["realtor"] == 1
    assert "foreclosure_refresh" not in result["update"]


# ---------------------------------------------------------------------------
# _payload_failed tests
# ---------------------------------------------------------------------------


def test_payload_failed_detects_nested_update_error() -> None:
    assert not pg_market_data_scrapling._payload_failed({"update": {"rows": 1}})
    assert pg_market_data_scrapling._payload_failed({"update": {"error": "boom"}})
    assert pg_market_data_scrapling._payload_failed({"update": {"success": False}})


def test_payload_failed_top_level_error() -> None:
    assert pg_market_data_scrapling._payload_failed({"error": "top-level"})


def test_payload_failed_top_level_success_false() -> None:
    assert pg_market_data_scrapling._payload_failed({"success": False})


# ---------------------------------------------------------------------------
# main exit code test
# ---------------------------------------------------------------------------


def test_main_exits_nonzero_when_update_fails(monkeypatch: Any) -> None:
    monkeypatch.setattr("sys.argv", ["pg_market_data_scrapling"])
    monkeypatch.setattr(
        pg_market_data_scrapling,
        "run_market_data_update",
        lambda **_kw: {"properties_queried": 1, "update": {"success": False}},
    )

    with pytest.raises(SystemExit) as exc:
        pg_market_data_scrapling.main()

    assert exc.value.code == 1
