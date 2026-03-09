from __future__ import annotations

from typing import Any
from typing import Self

from src.services import pg_survival_service


class _CaptureResult:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def fetchall(self) -> list[Any]:
        return self._rows

    def mappings(self) -> Self:
        return self


class _CaptureConnection:
    def __init__(self, captured: dict[str, Any], rows: list[Any]) -> None:
        self._captured = captured
        self._rows = rows

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: Any, params: dict[str, Any] | None = None) -> _CaptureResult:
        self._captured["sql"] = str(sql)
        self._captured["params"] = params or {}
        self._captured.setdefault("executed", []).append((str(sql), params or {}))
        return _CaptureResult(self._rows)


class _CaptureEngine:
    def __init__(self, captured: dict[str, Any], rows: list[Any]) -> None:
        self._captured = captured
        self._rows = rows

    def connect(self) -> _CaptureConnection:
        return _CaptureConnection(self._captured, self._rows)

    def begin(self) -> _CaptureConnection:
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


def test_find_targets_builds_foreclosing_refs_from_foreclosed_mortgage(
    monkeypatch: Any,
) -> None:
    service = _build_service(monkeypatch)
    captured: dict[str, Any] = {}
    service.engine = _CaptureEngine(
        captured,
        rows=[
            (
                7,
                "24-CA-000007",
                "S7",
                {
                    "plaintiff": "BANK",
                    "foreclosed_mortgage": {
                        "instrument_number": "2024000123",
                        "recording_book": "12345",
                        "recording_page": "678",
                    },
                },
                True,
            ),
        ],
    )

    targets = service._find_targets(25)  # noqa: SLF001

    assert targets[0]["judgment_data"]["foreclosing_refs"] == {
        "instrument": "2024000123",
        "book": "12345",
        "page": "678",
    }
    assert targets[0]["judgment_data"]["case_number"] == "24-CA-000007"


def test_find_targets_checks_per_foreclosure_survival_rows_when_not_forced(
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

    service._find_targets(25)  # noqa: SLF001

    sql_text = captured["sql"].lower()
    assert "foreclosure_encumbrance_survival" in sql_text
    assert "oe.survival_status is null" not in sql_text


def test_save_survival_results_upserts_per_foreclosure_rows(
    monkeypatch: Any,
) -> None:
    service = _build_service(monkeypatch)
    captured: dict[str, Any] = {}
    service.engine = _CaptureEngine(captured, rows=[])

    service._save_survival_results(  # noqa: SLF001
        7,
        "292025CA000007A001HC",
        "STRAP-7",
        {
            "results": {
                "historical": [
                    {
                        "id": 101,
                        "case_number": "292025CA000007A001HC",
                        "survival_status": "HISTORICAL",
                        "survival_reason": "Recorded in the current foreclosure case",
                    }
                ]
            }
        },
    )

    executed = captured["executed"]
    assert len(executed) == 2

    insert_sql, insert_params = executed[0]
    assert "insert into foreclosure_encumbrance_survival" in insert_sql.lower()
    assert insert_params["foreclosure_id"] == 7
    assert insert_params["encumbrance_id"] == 101
    assert insert_params["case_number"] == "292025CA000007A001HC"

    update_sql, update_params = executed[1]
    assert "update ori_encumbrances set" in update_sql.lower()
    assert update_params["id"] == 101
    assert update_params["case_number"] == "292025CA000007A001HC"


def test_load_encumbrances_maps_mortgage_holder_to_creditor(
    monkeypatch: Any,
) -> None:
    service = _build_service(monkeypatch)
    captured: dict[str, Any] = {}
    service.engine = _CaptureEngine(
        captured,
        rows=[
            {
                "id": 1,
                "encumbrance_type": "mortgage",
                "party1": "BORROWER ONE, BORROWER TWO",
                "party2": "WELLS FARGO BANK NA",
                "amount": 250000.0,
                "recording_date": "2024-01-15",
                "instrument_number": "2024011501",
                "book": "",
                "page": "",
                "is_satisfied": False,
                "satisfaction_instrument": "",
                "satisfaction_date": None,
                "survival_status": None,
                "case_number": "24-CA-000123",
                "current_holder": "",
            },
        ],
    )

    encumbrances = service._load_encumbrances("STRAP-1")  # noqa: SLF001

    assert encumbrances == [
        {
            "id": 1,
            "encumbrance_type": "mortgage",
            "creditor": "WELLS FARGO BANK NA",
            "debtor": "BORROWER ONE, BORROWER TWO",
            "amount": 250000.0,
            "recording_date": "2024-01-15",
            "instrument": "2024011501",
            "book": "",
            "page": "",
            "is_satisfied": False,
            "satisfaction_instrument": "",
            "satisfaction_date": None,
            "survival_status": None,
            "case_number": "24-CA-000123",
        },
    ]
