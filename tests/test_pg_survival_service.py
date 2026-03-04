from __future__ import annotations

from typing import Any
from typing import Self

from src.services import pg_survival_service


class _CaptureResult:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows


class _CaptureConnection:
    def __init__(self, captured: dict[str, Any], rows: list[tuple[Any, ...]]) -> None:
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
    def __init__(self, captured: dict[str, Any], rows: list[tuple[Any, ...]]) -> None:
        self._captured = captured
        self._rows = rows

    def connect(self) -> _CaptureConnection:
        return _CaptureConnection(self._captured, self._rows)


def _build_service(monkeypatch: Any) -> pg_survival_service.PgSurvivalService:
    monkeypatch.setattr(
        pg_survival_service,
        "resolve_pg_dsn",
        lambda _dsn: "postgresql://user:pw@host:5432/db",
    )
    monkeypatch.setattr(
        pg_survival_service,
        "get_engine",
        lambda _dsn: object(),
    )
    return pg_survival_service.PgSurvivalService()


def test_find_targets_force_reanalysis_scopes_to_selected_foreclosures(
    monkeypatch: Any,
) -> None:
    service = _build_service(monkeypatch)
    captured: dict[str, Any] = {}
    service.engine = _CaptureEngine(
        captured,
        rows=[
            (7, "24-CA-000007", "S7", {"plaintiff": "BANK"}, True),
        ],
    )

    targets = service._find_targets(25, foreclosure_ids=[7], force_reanalysis=True)  # noqa: SLF001

    assert targets[0]["foreclosure_id"] == 7
    assert captured["params"]["foreclosure_ids"] == [7]
    assert captured["params"]["limit"] == 25
    sql_text = captured["sql"].lower()
    assert "f.foreclosure_id = any(:foreclosure_ids)" in sql_text
    assert "f.step_survival_analyzed is null" not in sql_text
    assert "oe.survival_status is null" not in sql_text
    assert "oe.encumbrance_type != 'noc'" in sql_text
