from __future__ import annotations

from datetime import date
from typing import Any, Self

from src.services import pg_foreclosure_service


class _FakeResult:
    def __init__(self, rowcount: int = 1) -> None:
        self.rowcount = rowcount


class _FakeConnection:
    def __init__(self, captured: dict[str, Any]) -> None:
        self._captured = captured

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: Any, params: dict[str, Any] | None = None) -> _FakeResult:
        sql_text = str(sql)
        if "SELECT 1 FROM foreclosures LIMIT 0" in sql_text:
            return _FakeResult()
        self._captured["sql"] = sql_text
        self._captured["params"] = params or {}
        return _FakeResult()


class _FakeEngine:
    def __init__(self, captured: dict[str, Any]) -> None:
        self._captured = captured

    def connect(self) -> _FakeConnection:
        return _FakeConnection(self._captured)

    def begin(self) -> _FakeConnection:
        return _FakeConnection(self._captured)


def test_update_judgment_data_sets_pdf_download_step(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        pg_foreclosure_service,
        "resolve_pg_dsn",
        lambda _dsn: "postgresql://user:pw@host:5432/db",
    )
    monkeypatch.setattr(
        pg_foreclosure_service,
        "get_engine",
        lambda _dsn: _FakeEngine(captured),
    )

    service = pg_foreclosure_service.PgForeclosureService()

    assert service.update_judgment_data(
        "25-CA-123456",
        date(2026, 3, 1),
        {"plaintiff": "Bank"},
        pdf_path="data/Foreclosure/25-CA-123456/documents/final_judgment.pdf",
    )

    sql_text = captured["sql"].lower()
    assert "step_pdf_downloaded" in sql_text
    assert "coalesce(:pp, pdf_path, '')" in sql_text
