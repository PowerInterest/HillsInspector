from __future__ import annotations

from datetime import date
from typing import Self

from src.services import pg_auction_service


class _FakeResult:
    def __init__(self, rows: list[tuple[date]]) -> None:
        self.rows = rows

    def fetchall(self) -> list[tuple[date]]:
        return self.rows


class _FakeConnection:
    def __init__(self, rows: list[tuple[date]]) -> None:
        self.rows = rows
        self.sql = ""

    def __enter__(self) -> Self:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> bool:
        return False

    def execute(
        self,
        statement: object,
        _params: dict[str, object] | None = None,
    ) -> _FakeResult:
        self.sql = " ".join(str(statement).split())
        return _FakeResult(self.rows)


class _FakeEngine:
    def __init__(self, conn: _FakeConnection) -> None:
        self.conn = conn

    def connect(self) -> _FakeConnection:
        return self.conn


def test_dates_with_auctions_keeps_current_date_out_of_covered_dates_query(
    monkeypatch: object,
) -> None:
    conn = _FakeConnection(rows=[(date(2026, 2, 27),)])
    monkeypatch.setattr(
        pg_auction_service,
        "resolve_pg_dsn",
        lambda _dsn: "postgresql://user:pw@host:5432/db",
    )
    monkeypatch.setattr(
        pg_auction_service,
        "get_engine",
        lambda _dsn: _FakeEngine(conn),
    )

    service = pg_auction_service.PgAuctionService()
    dates = service._dates_with_auctions(date(2026, 2, 27), date(2026, 3, 31))  # noqa: SLF001

    assert dates == {date(2026, 2, 27)}
    assert "auction_date > CURRENT_DATE" in conn.sql
    assert "archived_at IS NULL" in conn.sql
    assert "auction_date = CURRENT_DATE" not in conn.sql
