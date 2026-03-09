"""Tests for judgment data persistence via PgJudgmentService.persist_judgment.

The old PgForeclosureService.update_judgment_data was removed as dead code.
These tests validate the canonical shared persistence helper that both
PgJudgmentService._load_judgment_data_to_pg and refresh_foreclosures
._load_judgment_data now use.
"""

from __future__ import annotations

from typing import Any, Self

from src.services.pg_judgment_service import PgJudgmentService


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
        self._captured["sql"] = str(sql)
        self._captured["params"] = params or {}
        return _FakeResult()


def test_persist_judgment_sets_all_step_flags() -> None:
    """persist_judgment must set step_pdf_downloaded and step_judgment_extracted."""
    captured: dict[str, Any] = {}
    conn = _FakeConnection(captured)

    result = PgJudgmentService.persist_judgment(
        conn,
        foreclosure_id=42,
        judgment_data={"plaintiff": "Bank", "total_judgment_amount": 150000},
        pdf_path="data/Foreclosure/25-CA-123456/documents/final_judgment.pdf",
    )

    assert result is True
    sql_text = captured["sql"].lower()
    assert "step_pdf_downloaded" in sql_text, "persist_judgment must set step_pdf_downloaded"
    assert "step_judgment_extracted" in sql_text, "persist_judgment must set step_judgment_extracted"
    assert "final_judgment_amount" in sql_text, "persist_judgment must set final_judgment_amount"
    assert "coalesce(:pp, pdf_path)" in sql_text


def test_persist_judgment_uses_coalesce_preserve_first() -> None:
    """Step flags should use COALESCE to preserve the first-set timestamp."""
    captured: dict[str, Any] = {}
    conn = _FakeConnection(captured)

    PgJudgmentService.persist_judgment(
        conn,
        foreclosure_id=99,
        judgment_data={"plaintiff": "Lender"},
        pdf_path=None,
    )

    sql_text = captured["sql"].lower()
    # step_judgment_extracted should COALESCE (preserve first)
    assert "coalesce(step_judgment_extracted, now())" in sql_text


def test_persist_judgment_extracts_final_judgment_amount() -> None:
    """total_judgment_amount from JSON should populate final_judgment_amount."""
    captured: dict[str, Any] = {}
    conn = _FakeConnection(captured)

    PgJudgmentService.persist_judgment(
        conn,
        foreclosure_id=7,
        judgment_data={"total_judgment_amount": 250000.50},
        pdf_path=None,
    )

    assert captured["params"]["fja"] == 250000.50


def test_persist_judgment_returns_false_on_no_match() -> None:
    """If no row matches the foreclosure_id, persist_judgment returns False."""

    class _NoMatchConn:
        def execute(self, _sql: Any, _params: Any = None) -> _FakeResult:
            return _FakeResult(rowcount=0)

    result = PgJudgmentService.persist_judgment(
        _NoMatchConn(),
        foreclosure_id=999,
        judgment_data={},
        pdf_path=None,
    )
    assert result is False


def test_load_judgment_data_to_pg_only_counts_actual_updates(
    tmp_path: Any,
    monkeypatch: Any,
) -> None:
    """_load_judgment_data_to_pg should only count rows where persist_judgment returned True."""
    import json

    from src.services import pg_judgment_service

    # Create two case directories with judgment JSONs
    for case in ["25-CA-000001", "25-CA-000002"]:
        doc_dir = tmp_path / case / "documents"
        doc_dir.mkdir(parents=True)
        (doc_dir / "final_judgment_2025001111_extracted.json").write_text(
            json.dumps({"recording_date": "2025-01-01", "instrument_number": "2025001111"}),
            encoding="utf-8",
        )

    monkeypatch.setattr(pg_judgment_service, "FORECLOSURE_DATA_DIR", tmp_path)

    # Fake engine that returns case_map rows and tracks persist calls
    class _FakeConn:
        def execute(self, sql: Any, params: Any = None) -> _FakeResult:
            # The lookup query for case_map
            return _FakeResult(rowcount=0)

        def fetchall(self) -> list[tuple[int, str, str]]:
            return [
                (1, "25-CA-000001", "STRAP001"),
                (2, "25-CA-000002", "STRAP002"),
            ]

    class _FakeConnCtx:
        """Context manager that returns a conn with dynamic persist behavior."""

        def __init__(self) -> None:
            self._persist_rowcounts = {1: 1, 2: 0}  # fid 1 matches, fid 2 doesn't

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, sql: Any, params: Any = None) -> Any:
            sql_str = str(sql)
            if "DISTINCT ON" in sql_str:
                # Lookup query
                class _Rows:
                    @staticmethod
                    def fetchall() -> list[tuple[int, str, str]]:
                        return [
                            (1, "25-CA-000001", "STRAP001"),
                            (2, "25-CA-000002", "STRAP002"),
                        ]

                return _Rows()
            # persist_judgment UPDATE
            fid = params.get("fid") if params else None
            rc = self._persist_rowcounts.get(fid, 0)
            return _FakeResult(rowcount=rc)

    class _FakeEngine:
        def begin(self) -> _FakeConnCtx:
            return _FakeConnCtx()

    monkeypatch.setattr(
        pg_judgment_service,
        "resolve_pg_dsn",
        lambda _dsn: "postgresql://user:pw@host:5432/db",
    )
    monkeypatch.setattr(pg_judgment_service, "get_engine", lambda _dsn: _FakeEngine())

    svc = pg_judgment_service.PgJudgmentService()
    count = svc._load_judgment_data_to_pg()  # noqa: SLF001

    # Only fid=1 actually updated a row; fid=2 returned rowcount=0
    assert count == 1, f"Expected 1 actual update, got {count}"
