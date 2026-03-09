from __future__ import annotations

from typing import Any
from typing import Self

from src.services import pg_foreclosure_identifier_recovery_service as identifier_recovery


class _FakeResult:
    def __init__(
        self,
        *,
        row: dict[str, Any] | None = None,
    ) -> None:
        self._row = row

    def mappings(self) -> _FakeResult:
        return self

    def fetchone(self) -> dict[str, Any] | None:
        return self._row


class _ParcelLookupConnection:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, dict[str, Any] | None]] = []

    def execute(
        self,
        statement: Any,
        params: dict[str, Any] | None = None,
    ) -> _FakeResult:
        self.execute_calls.append((str(statement), params))
        return _FakeResult(
            row={
                "folio": "F-1",
                "strap": "S-1",
                "property_address": "1 MAIN ST",
                "raw_legal1": "LEGAL",
                "raw_legal2": "",
                "raw_legal3": "",
                "raw_legal4": "",
                "source_file_id": 123,
            }
        )


class _NestedTransaction:
    def __init__(self, conn: _RunConnection) -> None:
        self._conn = conn

    def __enter__(self) -> Self:
        self._conn.begin_nested_calls += 1
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> bool:
        if exc_type is not None:
            self._conn.aborted = False
        return False


class _RunConnection:
    def __init__(self) -> None:
        self.aborted = False
        self.begin_nested_calls = 0
        self.update_calls: list[dict[str, Any]] = []

    def begin_nested(self) -> _NestedTransaction:
        return _NestedTransaction(self)

    def execute(
        self,
        statement: Any,
        params: dict[str, Any] | None = None,
    ) -> _FakeResult:
        if self.aborted:
            raise RuntimeError("transaction remained aborted")
        sql = str(statement)
        if "UPDATE foreclosures" not in sql:
            raise AssertionError(sql)
        self.update_calls.append(params or {})
        return _FakeResult(
            row={
                "strap": (params or {}).get("strap"),
                "folio": (params or {}).get("folio"),
            }
        )


class _BeginContext:
    def __init__(self, conn: _RunConnection) -> None:
        self._conn = conn

    def __enter__(self) -> _RunConnection:
        return self._conn

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> bool:
        return False


class _FakeEngine:
    def __init__(self, conn: _RunConnection) -> None:
        self._conn = conn

    def begin(self) -> _BeginContext:
        return _BeginContext(self._conn)


def _build_service() -> identifier_recovery.PgForeclosureIdentifierRecoveryService:
    service = object.__new__(
        identifier_recovery.PgForeclosureIdentifierRecoveryService,
    )
    service._available = True  # noqa: SLF001
    service._dsn = "postgresql://x"  # noqa: SLF001
    service._ori_session = None  # noqa: SLF001
    service._run_stats = {}  # noqa: SLF001
    return service


def test_load_parcel_candidate_casts_lookup_binds_to_text() -> None:
    service = _build_service()
    conn = _ParcelLookupConnection()

    candidate = service._load_parcel_candidate(  # noqa: SLF001
        conn,  # type: ignore[arg-type]
        folio="F-1",
        strap="S-1",
    )

    assert candidate is not None
    sql, params = conn.execute_calls[0]
    assert "folio = CAST(:folio AS text)" in sql
    assert "strap = CAST(:strap AS text)" in sql
    assert "WHEN folio = CAST(:folio AS text) AND strap = CAST(:strap AS text)" in sql
    assert params == {"folio": "F-1", "strap": "S-1"}


def test_run_uses_nested_transactions_to_isolate_case_failures() -> None:
    service = _build_service()
    conn = _RunConnection()
    service._engine = _FakeEngine(conn)  # noqa: SLF001

    rows = [
        {"foreclosure_id": 1, "case_number_raw": "bad"},
        {"foreclosure_id": 2, "case_number_raw": "good"},
    ]

    def _fake_load_scope_rows(_conn: Any, *, limit: int | None) -> list[dict[str, Any]]:
        _ = limit
        return rows

    service._load_scope_rows = _fake_load_scope_rows  # type: ignore[method-assign]  # noqa: SLF001

    def _fake_resolve(
        _conn: _RunConnection,
        row: dict[str, Any],
    ) -> identifier_recovery._ResolutionDecision:
        if row["case_number_raw"] == "bad":
            _conn.aborted = True
            raise RuntimeError("boom")
        return identifier_recovery._ResolutionDecision(  # noqa: SLF001
            candidate=identifier_recovery._ParcelCandidate(  # noqa: SLF001
                folio="F-2",
                strap="S-2",
                property_address="2 MAIN ST",
                legal_description="LEGAL",
                source_file_id=1,
            ),
            method="resolved_parcel_id",
            ambiguous=False,
            reason="resolved",
        )

    service._resolve_one = _fake_resolve  # type: ignore[method-assign]  # noqa: SLF001

    result = service.run()

    assert result["errors"] == 1
    assert result["rows_updated"] == 1
    assert result["resolved_parcel_id"] == 1
    assert conn.begin_nested_calls == 2
    assert conn.update_calls == [
        {
            "foreclosure_id": 2,
            "strap": "S-2",
            "folio": "F-2",
            "property_address": "2 MAIN ST",
        }
    ]
