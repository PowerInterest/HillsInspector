from __future__ import annotations

from typing import Any, Self

from src.services.audit import pg_audit_encumbrance
from src.services.audit import web_audit_service


class _FakeResult:
    def __init__(
        self,
        *,
        rows: list[dict[str, Any]] | None = None,
        scalar_value: Any = None,
        fetchone_value: Any = None,
    ) -> None:
        self._rows = rows or []
        self._scalar_value = scalar_value
        self._fetchone_value = fetchone_value

    def mappings(self) -> Self:
        return self

    def all(self) -> list[dict[str, Any]]:
        return self._rows

    def scalar(self) -> Any:
        return self._scalar_value

    def fetchone(self) -> Any:
        return self._fetchone_value

    def fetchall(self) -> list[Any]:
        return []


class _CaptureConnection:
    def __init__(self) -> None:
        self.sql = ""
        self.params: dict[str, Any] = {}

    def execute(self, statement: Any, params: dict[str, Any] | None = None) -> _FakeResult:
        self.sql = str(statement)
        self.params = params or {}
        return _FakeResult(rows=[])

    def rollback(self) -> None:
        return None


class _ScopeConnection:
    def execute(self, statement: Any, params: dict[str, Any] | None = None) -> _FakeResult:
        sql = str(statement).upper()
        if "INFORMATION_SCHEMA" in sql:
            return _FakeResult(scalar_value=1)
        if "COUNT(DISTINCT" in sql:
            return _FakeResult(scalar_value=0)
        if "SELECT COUNT(*) FROM FORECLOSURES" in sql and "JUDGMENT_DATA IS NOT NULL" in sql and "STRAP IS NOT NULL" in sql:
            return _FakeResult(scalar_value=0)
        if "SELECT COUNT(*) FROM FORECLOSURES" in sql and "JUDGMENT_DATA IS NOT NULL" in sql:
            return _FakeResult(scalar_value=1)
        if "SELECT COUNT(*) FROM FORECLOSURES" in sql:
            return _FakeResult(scalar_value=1)
        return _FakeResult(rows=[])

    def rollback(self) -> None:
        return None


def test_lp_missing_bucket_checks_case_numbers_as_well_as_strap() -> None:
    conn = _CaptureConnection()

    pg_audit_encumbrance._bucket_lp_missing(conn)  # noqa: SLF001

    sql_text = conn.sql
    assert "oe.case_number = f.case_number_raw" in sql_text
    assert "oe.case_number = f.case_number_norm" in sql_text


def test_run_audit_records_bucket_query_errors_separately() -> None:
    conn = _ScopeConnection()
    original_definitions = pg_audit_encumbrance.BUCKET_DEFINITIONS
    try:
        pg_audit_encumbrance.BUCKET_DEFINITIONS = [
            {
                "name": "lp_missing",
                "description": "LP",
                "handler": lambda _conn: (_ for _ in ()).throw(RuntimeError("boom")),
                "deferred": False,
            }
        ]
        report = pg_audit_encumbrance.run_audit(conn=conn)
    finally:
        pg_audit_encumbrance.BUCKET_DEFINITIONS = original_definitions

    assert report.summaries[0].count == 0
    assert report.summaries[0].error_count == 1
    assert report.summaries[0].deferred is True


def test_property_snapshot_scopes_bucket_queries_to_one_foreclosure(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}
    original_definitions = web_audit_service.BUCKET_DEFINITIONS
    def _fake_handler(_conn: Any, foreclosure_ids: list[int] | None = None) -> list[Any]:
        captured["foreclosure_ids"] = foreclosure_ids
        return []

    try:
        web_audit_service.BUCKET_DEFINITIONS = [
            {
                "name": "lp_missing",
                "handler": _fake_handler,
            }
        ]
        snapshot = web_audit_service.get_property_audit_snapshot(
            foreclosure_id=42,
            conn=object(),
        )
    finally:
        web_audit_service.BUCKET_DEFINITIONS = original_definitions

    assert captured["foreclosure_ids"] == [42]
    assert snapshot["total_open_issues"] == 0


def test_encumbrance_audit_inbox_exposes_error_count(monkeypatch: Any) -> None:
    monkeypatch.setattr(
        web_audit_service,
        "run_audit",
        lambda **_kwargs: pg_audit_encumbrance.AuditReport(
            active_count=1,
            judged_count=1,
            with_strap_count=1,
            with_encumbrances_count=0,
            summaries=[
                pg_audit_encumbrance.BucketSummary(
                    bucket="lp_missing",
                    description="LP",
                    count=0,
                    error_count=1,
                    deferred=True,
                    deferred_reason="boom",
                )
            ],
            hits=[],
        ),
    )

    inbox = web_audit_service.get_encumbrance_audit_inbox(conn=object())

    assert inbox["bucket_summaries"][0]["error_count"] == 1
