from __future__ import annotations

import re
from typing import TYPE_CHECKING
from typing import Any
from typing import cast
from sqlalchemy.exc import SQLAlchemyError

from src.services import pg_trust_accounts

if TYPE_CHECKING:
    from types import TracebackType


class _FakeResult:
    def __init__(
        self,
        *,
        rows: list[dict[str, Any]] | None = None,
        first_row: Any = None,
    ) -> None:
        self._rows = rows or []
        self._first_row = first_row

    def mappings(self) -> _FakeResult:
        return self

    def all(self) -> list[dict[str, Any]]:
        return self._rows

    def first(self) -> Any:
        return self._first_row


class _FakeConnection:
    def __init__(self, execute_fn: Any | None = None) -> None:
        self.execute_calls: list[tuple[str, dict[str, Any] | None]] = []
        self._execute_fn = execute_fn or (
            lambda _statement, _params: _FakeResult(first_row=(1,))
        )

    def execute(self, statement: Any, params: dict[str, Any] | None = None) -> _FakeResult:
        sql = str(statement)
        self.execute_calls.append((sql, params))
        return self._execute_fn(statement, params)


class _ConnectionContext:
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn

    def __enter__(self) -> _FakeConnection:
        return self._conn

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        return False


class _FakeEngine:
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn

    def connect(self) -> _ConnectionContext:
        return _ConnectionContext(self._conn)


class _BrokenEngine:
    def connect(self) -> _BrokenContext:
        return _BrokenContext()


class _BrokenContext:
    def __enter__(self) -> None:
        raise SQLAlchemyError("db down")

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        return False


class _TestPgTrustAccountsService(pg_trust_accounts.PgTrustAccountsService):
    def load_upcoming_auction_context_for_test(
        self,
        conn: _FakeConnection,
    ) -> dict[str, dict[str, str]]:
        return self._load_upcoming_auction_context(cast("Any", conn))

    def load_history_winning_bids_for_test(self) -> dict[str, dict[float, int]]:
        return self._load_history_winning_bids()

    def load_known_third_party_bidders_for_test(self) -> tuple[set[str], set[str]]:
        return self._load_known_third_party_bidders()


def _build_service(monkeypatch: Any, conn: _FakeConnection) -> _TestPgTrustAccountsService:
    monkeypatch.setattr(pg_trust_accounts, "resolve_pg_dsn", lambda _: "postgresql://x")
    monkeypatch.setattr(pg_trust_accounts, "get_engine", lambda _: _FakeEngine(conn))
    svc = _TestPgTrustAccountsService()
    conn.execute_calls.clear()
    return svc


def test_init_uses_connectivity_probe_not_table_check(monkeypatch: Any) -> None:
    conn = _FakeConnection()
    monkeypatch.setattr(pg_trust_accounts, "resolve_pg_dsn", lambda _: "postgresql://x")
    monkeypatch.setattr(pg_trust_accounts, "get_engine", lambda _: _FakeEngine(conn))

    svc = pg_trust_accounts.PgTrustAccountsService()

    assert svc.available
    assert svc.unavailable_reason is None
    sqls = [sql for sql, _ in conn.execute_calls]
    assert any("SELECT 1" in sql for sql in sqls)
    assert all('FROM "TrustAccount"' not in sql for sql in sqls)


def test_init_failure_exposes_unavailable_reason(monkeypatch: Any) -> None:
    monkeypatch.setattr(pg_trust_accounts, "resolve_pg_dsn", lambda _: "postgresql://x")
    monkeypatch.setattr(pg_trust_accounts, "get_engine", lambda _: _BrokenEngine())

    svc = pg_trust_accounts.PgTrustAccountsService()
    result = svc.run()

    assert not svc.available
    assert "db down" in (svc.unavailable_reason or "")
    assert result["skipped"] is True
    assert result["reason"] == "service_unavailable"
    assert "db down" in str(result.get("details"))


def test_upcoming_context_returns_empty_when_foreclosures_missing(monkeypatch: Any) -> None:
    conn = _FakeConnection()
    svc = _build_service(monkeypatch, conn)
    monkeypatch.setattr(svc, "_table_exists", lambda _conn, _table: False)

    result = svc.load_upcoming_auction_context_for_test(conn)

    assert result == {}
    assert conn.execute_calls == []


def test_upcoming_context_returns_empty_when_required_columns_missing(
    monkeypatch: Any,
) -> None:
    conn = _FakeConnection()
    svc = _build_service(monkeypatch, conn)
    monkeypatch.setattr(svc, "_table_exists", lambda _conn, _table: True)
    monkeypatch.setattr(svc, "_table_columns", lambda _conn, _table: {"case_number_raw"})

    result = svc.load_upcoming_auction_context_for_test(conn)

    assert result == {}
    assert conn.execute_calls == []


def test_upcoming_context_keeps_earliest_date_per_case(monkeypatch: Any) -> None:
    rows = [
        {
            "case_number": "292026CA000001A001HC",
            "auction_date": "2026-03-01",
            "plaintiff": "LATER BANK",
        },
        {
            "case_number": "292026CA000001A001HC",
            "auction_date": "2026-02-20",
            "plaintiff": "EARLY BANK",
        },
        {
            "case_number": "26-CA-000002",
            "auction_date": "2026-02-22",
            "plaintiff": None,
        },
    ]

    conn = _FakeConnection(
        execute_fn=lambda _statement, _params: _FakeResult(rows=rows, first_row=(1,))
    )
    svc = _build_service(monkeypatch, conn)
    monkeypatch.setattr(svc, "_table_exists", lambda _conn, _table: True)
    monkeypatch.setattr(
        svc,
        "_table_columns",
        lambda _conn, _table: {
            "case_number_raw",
            "auction_date",
            "judgment_data",
            "archived_at",
        },
    )

    result = svc.load_upcoming_auction_context_for_test(conn)

    assert result["20-CA-000001"]["auction_date"] == "2026-02-20"
    assert result["20-CA-000001"]["plaintiff"] == "EARLY BANK"
    assert result["26-CA-000002"]["plaintiff"] == ""
    sql = conn.execute_calls[0][0]
    assert "judgment_data->>'plaintiff_name'" in sql
    assert "archived_at IS NULL" in sql


def test_upcoming_context_query_failure_raises(monkeypatch: Any) -> None:
    def _raise_sqlalchemy_error(statement: Any, params: Any) -> _FakeResult:
        raise SQLAlchemyError("query failed")

    conn = _FakeConnection(execute_fn=_raise_sqlalchemy_error)
    svc = _build_service(monkeypatch, conn)
    monkeypatch.setattr(svc, "_table_exists", lambda _conn, _table: True)
    monkeypatch.setattr(
        svc,
        "_table_columns",
        lambda _conn, _table: {"case_number_raw", "auction_date"},
    )

    try:
        svc.load_upcoming_auction_context_for_test(conn)
    except SQLAlchemyError:
        pass
    else:
        raise AssertionError("Expected SQLAlchemyError for upcoming-context query failure")


def test_winning_bids_aggregates_and_skips_invalid_source_tables(
    monkeypatch: Any,
) -> None:
    rows = [
        {"auction_date": "2026-02-19", "winning_bid": 1000},
        {"auction_date": "2026-02-19", "winning_bid": 1000},
        {"auction_date": "2026-02-19", "winning_bid": 2000.123},
    ]
    conn = _FakeConnection(
        execute_fn=lambda _statement, _params: _FakeResult(rows=rows, first_row=(1,))
    )
    svc = _build_service(monkeypatch, conn)
    monkeypatch.setattr(svc, "_table_exists", lambda _conn, _table: True)
    monkeypatch.setattr(
        svc,
        "_table_has_columns",
        lambda _conn, table, _required: table != "foreclosures_history",
    )

    result = svc.load_history_winning_bids_for_test()

    assert result["2026-02-19"][1000.0] == 2
    assert result["2026-02-19"][2000.12] == 1
    sql = conn.execute_calls[0][0]
    assert "FROM historical_auctions" in sql
    assert "FROM foreclosures" in sql
    assert "FROM foreclosures_history" not in sql


def test_winning_bids_query_failure_raises(monkeypatch: Any) -> None:
    def _raise_sqlalchemy_error(statement: Any, params: Any) -> _FakeResult:
        raise SQLAlchemyError("query failed")

    conn = _FakeConnection(execute_fn=_raise_sqlalchemy_error)
    svc = _build_service(monkeypatch, conn)
    monkeypatch.setattr(svc, "_table_exists", lambda _conn, _table: True)
    monkeypatch.setattr(svc, "_table_has_columns", lambda _conn, _table, _required: True)

    try:
        svc.load_history_winning_bids_for_test()
    except SQLAlchemyError:
        pass
    else:
        raise AssertionError("Expected SQLAlchemyError for winning-bids query failure")


def test_known_bidders_normalizes_names_and_skips_invalid_source_tables(
    monkeypatch: Any,
) -> None:
    rows = [
        {"sold_to": "Acme LLC"},
        {"sold_to": "ACME LLC"},
        {"sold_to": " River Ventures, Inc. "},
        {"sold_to": " "},
    ]
    conn = _FakeConnection(
        execute_fn=lambda _statement, _params: _FakeResult(rows=rows, first_row=(1,))
    )
    svc = _build_service(monkeypatch, conn)
    monkeypatch.setattr(svc, "_table_exists", lambda _conn, _table: True)
    monkeypatch.setattr(
        svc,
        "_table_has_columns",
        lambda _conn, table, _required: table != "foreclosures",
    )

    exact, core = svc.load_known_third_party_bidders_for_test()

    assert "ACME LLC" in exact
    assert "RIVER VENTURES INC" in exact
    assert "ACME" in core
    assert "RIVER VENTURES" in core
    sql = conn.execute_calls[0][0]
    assert "FROM historical_auctions" in sql
    assert "FROM foreclosures_history" in sql
    assert re.search(r"\bFROM foreclosures\b", sql) is None


def test_known_bidders_query_failure_raises(monkeypatch: Any) -> None:
    def _raise_sqlalchemy_error(statement: Any, params: Any) -> _FakeResult:
        raise SQLAlchemyError("query failed")

    conn = _FakeConnection(execute_fn=_raise_sqlalchemy_error)
    svc = _build_service(monkeypatch, conn)
    monkeypatch.setattr(svc, "_table_exists", lambda _conn, _table: True)
    monkeypatch.setattr(svc, "_table_has_columns", lambda _conn, _table, _required: True)

    try:
        svc.load_known_third_party_bidders_for_test()
    except SQLAlchemyError:
        pass
    else:
        raise AssertionError("Expected SQLAlchemyError for bidder-context query failure")
