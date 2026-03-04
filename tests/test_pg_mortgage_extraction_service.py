from __future__ import annotations

from typing import Any
from typing import Self

from src.services import pg_mortgage_extraction_service


class _DummyStorage:
    pass


class _DummyVision:
    pass


class _CaptureMappings:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[dict[str, Any]]:
        return self._rows


class _CaptureResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> _CaptureMappings:
        return _CaptureMappings(self._rows)


class _CaptureConnection:
    def __init__(self, captured: dict[str, Any], rows: list[dict[str, Any]]) -> None:
        self._captured = captured
        self._rows = rows

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: Any, params: dict[str, Any] | None = None) -> _CaptureResult:
        self._captured["sql"] = str(sql)
        self._captured["params"] = params or {}
        return _CaptureResult(self._rows)


class _CaptureEngine:
    def __init__(self, captured: dict[str, Any], rows: list[dict[str, Any]]) -> None:
        self._captured = captured
        self._rows = rows

    def connect(self) -> _CaptureConnection:
        return _CaptureConnection(self._captured, self._rows)


def _build_service(monkeypatch: Any) -> pg_mortgage_extraction_service.PgMortgageExtractionService:
    monkeypatch.setattr(
        pg_mortgage_extraction_service,
        "resolve_pg_dsn",
        lambda _dsn: "postgresql://user:pw@host:5432/db",
    )
    monkeypatch.setattr(
        pg_mortgage_extraction_service,
        "get_engine",
        lambda _dsn: object(),
    )
    monkeypatch.setattr(
        pg_mortgage_extraction_service,
        "ScraperStorage",
        _DummyStorage,
    )
    monkeypatch.setattr(
        pg_mortgage_extraction_service,
        "VisionService",
        _DummyVision,
    )
    return pg_mortgage_extraction_service.PgMortgageExtractionService()


def test_find_unextracted_mortgages_scopes_to_target_straps(monkeypatch: Any) -> None:
    service = _build_service(monkeypatch)
    captured: dict[str, Any] = {}
    service.engine = _CaptureEngine(
        captured,
        rows=[
            {
                "id": 1,
                "ori_id": "abc",
                "instrument_number": "2025000001",
                "case_number": "24-CA-000001",
                "folio": "F1",
                "strap": "S1",
            }
        ],
    )

    rows = service._find_unextracted_mortgages(5, straps=["S1", "S2"])  # noqa: SLF001

    assert rows[0]["strap"] == "S1"
    assert captured["params"]["straps"] == ["S1", "S2"]
    assert captured["params"]["limit"] == 5
    sql_text = captured["sql"].lower()
    assert "strap = any(:straps)" in sql_text
    assert "limit :limit" in sql_text
