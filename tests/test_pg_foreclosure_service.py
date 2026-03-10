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


def _valid_judgment_cache(
    instrument_number: str,
    *,
    include_validation: bool = True,
) -> dict[str, Any]:
    payload = {
        "instrument_number": instrument_number,
        "recording_book": None,
        "recording_page": None,
        "recording_date": "2025-01-01",
        "execution_date": None,
        "property_address": "10217 GRANT CREEK DR TAMPA, FL 33647",
        "legal_description": (
            "LOT 6, BLOCK 3, CROSS CREEK PARCEL K PHASE 1D, "
            "PLAT BOOK 89, PAGE 51, OF THE PUBLIC RECORDS OF "
            "HILLSBOROUGH COUNTY, FLORIDA."
        ),
        "parcel_id": "172834985C00000000010U",
        "confidence_score": 0.72,
        "unclear_sections": [],
        "case_number": "292024CA000333A001HC",
        "court_circuit": "13th",
        "county": "Hillsborough",
        "judge_name": "CHERYL THOMAS",
        "judgment_date": "2026-01-26",
        "plaintiff": "NAVY FEDERAL CREDIT UNION",
        "plaintiff_type": "bank",
        "defendants": [
            {
                "name": "JOHN DOE",
                "party_type": "borrower",
                "is_federal_entity": False,
                "is_deceased": False,
                "lien_recording_reference": None,
            }
        ],
        "subdivision": "CROSS CREEK PARCEL K PHASE 1D",
        "lot": "6",
        "block": "3",
        "unit": None,
        "plat_book": "89",
        "plat_page": "51",
        "is_condo": False,
        "foreclosed_mortgage": {
            "original_date": "2020-01-01",
            "original_amount": 267080.05,
            "recording_date": "2020-01-07",
            "recording_book": "12345",
            "recording_page": "678",
            "instrument_number": "2020000001",
            "original_lender": "NAVY FEDERAL CREDIT UNION",
            "current_holder": "NAVY FEDERAL CREDIT UNION",
        },
        "lis_pendens": {
            "recording_date": "2024-03-01",
            "recording_book": "22345",
            "recording_page": "100",
            "instrument_number": "2024000001",
        },
        "principal_amount": 267080.05,
        "interest_amount": 22784.32,
        "interest_through_date": "2025-12-31",
        "per_diem_rate": 27.93,
        "per_diem_interest": None,
        "late_charges": 53.66,
        "escrow_advances": 18148.47,
        "title_search_costs": 325.00,
        "court_costs": 2608.50,
        "attorney_fees": 8345.00,
        "other_costs": 2337.80,
        "total_judgment_amount": 321682.80,
        "foreclosure_sale_date": "2026-04-01",
        "sale_location": "https://www.hillsborough.realforeclose.com",
        "is_online_sale": True,
        "foreclosure_type": "FIRST MORTGAGE",
        "hoa_safe_harbor_mentioned": False,
        "superiority_language": None,
        "plaintiff_maximum_bid": None,
        "monthly_payment": None,
        "default_date": "2024-01-01",
        "service_by_publication": False,
        "red_flags": [],
    }
    if include_validation:
        payload["_validation"] = {"is_valid": True, "failures": [], "warnings": []}
    return payload


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
            json.dumps(_valid_judgment_cache("2025001111")),
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


def test_load_judgment_data_to_pg_revalidates_legacy_cache_without_metadata(
    tmp_path: Any,
    monkeypatch: Any,
) -> None:
    """Legacy v1 caches without _validation should still persist when valid."""
    import json

    from src.services import pg_judgment_service

    doc_dir = tmp_path / "25-CA-000001" / "documents"
    doc_dir.mkdir(parents=True)
    (doc_dir / "final_judgment_2025001111_extracted.json").write_text(
        json.dumps(
            _valid_judgment_cache(
                "2025001111",
                include_validation=False,
            )
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(pg_judgment_service, "FORECLOSURE_DATA_DIR", tmp_path)

    class _FakeConnCtx:
        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, sql: Any, params: Any = None) -> Any:
            sql_str = str(sql)
            if "DISTINCT ON" in sql_str:
                class _Rows:
                    @staticmethod
                    def fetchall() -> list[tuple[int, str, str]]:
                        return [(1, "25-CA-000001", "STRAP001")]

                return _Rows()
            return _FakeResult(rowcount=1)

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

    assert svc._load_judgment_data_to_pg() == 1  # noqa: SLF001


def test_load_judgment_data_to_pg_ignores_stale_embedded_validation(
    tmp_path: Any,
    monkeypatch: Any,
) -> None:
    """Embedded _validation must not override the current validation contract."""
    import json

    from src.services import pg_judgment_service

    doc_dir = tmp_path / "25-CA-000001" / "documents"
    doc_dir.mkdir(parents=True)
    payload = _valid_judgment_cache("2025001111")
    payload["_validation"] = {
        "is_valid": False,
        "failures": ["stale invalidation"],
        "warnings": [],
    }
    (doc_dir / "final_judgment_2025001111_extracted.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )

    monkeypatch.setattr(pg_judgment_service, "FORECLOSURE_DATA_DIR", tmp_path)

    class _FakeConnCtx:
        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, sql: Any, params: Any = None) -> Any:
            sql_str = str(sql)
            if "DISTINCT ON" in sql_str:
                class _Rows:
                    @staticmethod
                    def fetchall() -> list[tuple[int, str, str]]:
                        return [(1, "25-CA-000001", "STRAP001")]

                return _Rows()
            return _FakeResult(rowcount=1)

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

    assert svc._load_judgment_data_to_pg() == 1  # noqa: SLF001
