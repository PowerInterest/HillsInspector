from __future__ import annotations

from typing import Any, Self

from src.services import market_data_service


class _FakeRow:
    def __init__(self, mapping: dict[str, Any]) -> None:
        self._mapping = mapping


class _FakeResult:
    def __init__(self, rows: list[_FakeRow]) -> None:
        self._rows = rows

    def fetchall(self) -> list[_FakeRow]:
        return self._rows


class _FakeConnection:
    def __init__(self, captured: dict[str, Any], rows: list[_FakeRow]) -> None:
        self._captured = captured
        self._rows = rows

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: Any) -> _FakeResult:
        self._captured["sql"] = str(sql)
        return _FakeResult(self._rows)


class _FakeEngine:
    def __init__(self, captured: dict[str, Any], rows: list[_FakeRow]) -> None:
        self._captured = captured
        self._rows = rows

    def connect(self) -> _FakeConnection:
        return _FakeConnection(self._captured, self._rows)


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
