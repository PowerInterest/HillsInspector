from __future__ import annotations

from typing import Any
from typing import Self

from src.services.pg_encumbrance_relationship_service import (
    PgEncumbranceRelationshipService,
)


class _Result:
    def __init__(self, rowcount: int = 0) -> None:
        self.rowcount = rowcount


class _Conn:
    def __init__(self, captured: list[tuple[str, dict[str, Any]]]) -> None:
        self._captured = captured

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: Any, params: dict[str, Any] | None = None) -> _Result:
        payload = params or {}
        self._captured.append((str(sql), payload))
        if "UPDATE ori_encumbrances" in str(sql):
            return _Result(rowcount=1)
        return _Result(rowcount=0)


class _Engine:
    def __init__(self, captured: list[tuple[str, dict[str, Any]]]) -> None:
        self._captured = captured

    def begin(self) -> _Conn:
        return _Conn(self._captured)

    def connect(self) -> _Conn:
        return _Conn(self._captured)


def test_find_targets_orders_without_select_distinct(monkeypatch: Any) -> None:
    captured_sql: list[tuple[str, dict[str, Any]]] = []
    service = PgEncumbranceRelationshipService.__new__(PgEncumbranceRelationshipService)
    service.engine = _Engine(captured_sql)

    rows = [
        {
            "foreclosure_id": 7,
            "case_number": "CASE-7",
            "strap": "STRAP-7",
            "folio": "FOLIO-7",
            "judgment_data": {"plaintiff": "BANK"},
        }
    ]

    class _Rows:
        def mappings(self) -> _Rows:
            return self

        def all(self) -> list[dict[str, Any]]:
            return rows

    def _execute(
        self: _Conn,
        sql: Any,
        params: dict[str, Any] | None = None,
    ) -> _Rows:
        captured_sql.append((str(sql), params or {}))
        return _Rows()

    monkeypatch.setattr(_Conn, "execute", _execute, raising=False)

    result = service._find_targets(limit=25, straps=None, foreclosure_ids=None)  # noqa: SLF001

    assert result == rows
    sql_text, params = captured_sql[0]
    assert "SELECT DISTINCT" not in sql_text
    assert "ORDER BY f.auction_date NULLS LAST, f.foreclosure_id" in sql_text
    assert params == {"limit": 25}


def test_process_target_chases_missing_refs_and_updates_holder(monkeypatch: Any) -> None:
    captured_sql: list[tuple[str, dict[str, Any]]] = []
    service = PgEncumbranceRelationshipService.__new__(PgEncumbranceRelationshipService)
    service.engine = _Engine(captured_sql)
    service.extraction_service = None

    rows = [
        {
            "id": 1,
            "encumbrance_type": "mortgage",
            "instrument_number": "2024000001",
            "book": "100",
            "page": "200",
            "current_holder": "OLD BANK",
            "extracted_data": None,
            "recording_date": "2024-01-10",
        },
        {
            "id": 2,
            "encumbrance_type": "assignment",
            "instrument_number": "2024000002",
            "book": "",
            "page": "",
            "current_holder": None,
            "extracted_data": {
                "assignee": "NEW BANK",
                "parent_instrument": {"instrument_number": "2024000001"},
            },
            "recording_date": "2024-02-10",
        },
        {
            "id": 3,
            "encumbrance_type": "satisfaction",
            "instrument_number": "2024000003",
            "book": "",
            "page": "",
            "current_holder": None,
            "extracted_data": {
                "parent_instrument": {"instrument_number": "2024000999"},
            },
            "recording_date": "2024-03-10",
        },
        {
            "id": 4,
            "encumbrance_type": "lien",
            "instrument_number": "2024000004",
            "book": "",
            "page": "",
            "current_holder": None,
            "extracted_data": {
                "referenced_noc": {"recording_book": "300", "recording_page": "400"},
            },
            "recording_date": "2024-04-10",
        },
        {
            "id": 5,
            "encumbrance_type": "lis_pendens",
            "instrument_number": "2024000005",
            "book": "",
            "page": "",
            "current_holder": None,
            "extracted_data": {
                "foreclosed_instrument": {"instrument_number": "2024000001"},
            },
            "recording_date": "2024-05-10",
        },
    ]
    monkeypatch.setattr(service, "_load_rows_for_strap", lambda _strap: rows)

    class _FakeOriService:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def discover_exact_references(self, **kwargs: Any) -> dict[str, Any]:
            self.calls.append(kwargs)
            return {
                "searched_instruments": 2,
                "searched_book_pages": 1,
                "docs_found": 3,
                "saved": 2,
                "linked_satisfactions": 1,
                "linked_modifications": 0,
            }

    service.ori_service = _FakeOriService()

    result = service._process_target({  # noqa: SLF001
        "foreclosure_id": 77,
        "strap": "STRAP-77",
        "folio": "FOLIO-77",
        "judgment_data": {
            "foreclosed_mortgage": {"instrument_number": "2024000998"},
        },
    })

    assert result["leads_total"] == 5
    assert result["local_matches"] == 2
    assert result["saved"] == 2
    assert result["linked_satisfactions"] == 1
    assert result["holder_updates"] == 1
    assert result["changed"] is True

    assert service.ori_service.calls == [{
        "strap": "STRAP-77",
        "folio": "FOLIO-77",
        "instruments": ["2024000998", "2024000999"],
        "book_pages": [("300", "400")],
    }]
    update_params = next(params for sql, params in captured_sql if "current_holder" in sql)
    assert update_params["id"] == 1
    assert update_params["holder"] == "NEW BANK"


def test_assignment_holder_overrides_judgment_holder(monkeypatch: Any) -> None:
    """Only assignments recorded after judgment should override the judgment holder."""
    captured_sql: list[tuple[str, dict[str, Any]]] = []
    service = PgEncumbranceRelationshipService.__new__(PgEncumbranceRelationshipService)
    service.engine = _Engine(captured_sql)
    service.extraction_service = None

    rows = [
        {
            "id": 10,
            "encumbrance_type": "mortgage",
            "instrument_number": "2024000001",
            "book": "",
            "page": "",
            "current_holder": "ORIGINAL LENDER",
            "extracted_data": None,
            "recording_date": "2024-01-01",
        },
        {
            "id": 20,
            "encumbrance_type": "assignment",
            "instrument_number": "2024000050",
            "book": "",
            "page": "",
            "current_holder": None,
            "extracted_data": {
                "assignee": "LATEST SERVICER LLC",
                "parent_instrument": {"instrument_number": "2024000001"},
            },
            "recording_date": "2024-06-01",
        },
    ]
    monkeypatch.setattr(service, "_load_rows_for_strap", lambda _strap: rows)

    # Judgment also references the same mortgage with an older holder name.
    result = service._apply_holder_updates(  # noqa: SLF001
        strap="STRAP-X",
        judgment_data={
            "judgment_date": "2024-05-15",
            "foreclosed_mortgage": {
                "instrument_number": "2024000001",
                "current_holder": "JUDGMENT ERA BANK",
            },
        },
    )

    assert result == 1
    holder_updates = [
        params for sql, params in captured_sql if "current_holder" in sql
    ]
    assert holder_updates == [{"id": 10, "holder": "LATEST SERVICER LLC"}]


def test_judgment_holder_beats_prejudgment_assignment(monkeypatch: Any) -> None:
    """Assignments recorded before judgment should not replace the judgment holder."""
    captured_sql: list[tuple[str, dict[str, Any]]] = []
    service = PgEncumbranceRelationshipService.__new__(PgEncumbranceRelationshipService)
    service.engine = _Engine(captured_sql)
    service.extraction_service = None

    rows = [
        {
            "id": 30,
            "encumbrance_type": "mortgage",
            "instrument_number": "2024000100",
            "book": "",
            "page": "",
            "current_holder": "ORIGINAL LENDER",
            "extracted_data": None,
            "recording_date": "2024-01-01",
        },
        {
            "id": 31,
            "encumbrance_type": "assignment",
            "instrument_number": "2024000101",
            "book": "",
            "page": "",
            "current_holder": None,
            "extracted_data": {
                "assignee": "OLDER ASSIGNEE LLC",
                "parent_instrument": {"instrument_number": "2024000100"},
            },
            "recording_date": "2024-06-01",
        },
    ]
    monkeypatch.setattr(service, "_load_rows_for_strap", lambda _strap: rows)

    result = service._apply_holder_updates(  # noqa: SLF001
        strap="STRAP-Y",
        judgment_data={
            "judgment_date": "2024-07-01",
            "foreclosed_mortgage": {
                "instrument_number": "2024000100",
                "current_holder": "FORECLOSING PLAINTIFF BANK",
            },
        },
    )

    assert result == 1
    holder_updates = [
        params for sql, params in captured_sql if "current_holder" in sql
    ]
    assert holder_updates == [{"id": 30, "holder": "FORECLOSING PLAINTIFF BANK"}]


def test_run_counts_reextract_errors_as_relationship_errors(monkeypatch: Any) -> None:
    service = PgEncumbranceRelationshipService.__new__(PgEncumbranceRelationshipService)
    service.engine = None
    service.ori_service = None

    target = {
        "foreclosure_id": 11,
        "strap": "S11",
        "folio": "F11",
        "judgment_data": {},
    }
    find_calls = {"n": 0}

    def _fake_find_targets(**_kwargs: Any) -> list[dict[str, Any]]:
        find_calls["n"] += 1
        if find_calls["n"] == 1:
            return [target]
        return []

    monkeypatch.setattr(service, "_find_targets", _fake_find_targets)
    monkeypatch.setattr(
        service,
        "_process_target",
        lambda _target: {
            "foreclosure_id": 11,
            "strap": "S11",
            "leads_total": 1,
            "local_matches": 0,
            "searched_instruments": 1,
            "searched_book_pages": 0,
            "docs_found": 1,
            "saved": 1,
            "linked_satisfactions": 0,
            "linked_modifications": 0,
            "holder_updates": 0,
            "changed": True,
        },
    )

    class _FakeExtractionService:
        def run(self, **_kwargs: Any) -> dict[str, Any]:
            return {
                "extracted": 0,
                "cached": 0,
                "errors": 2,
                "skipped": 0,
                "ori_id_backfilled": 0,
            }

    service.extraction_service = _FakeExtractionService()

    result = service.run(max_passes=2)

    assert result["reextract_errors"] == 2
    assert result["errors"] == 2


def test_run_reextracts_changed_straps_between_passes(monkeypatch: Any) -> None:
    service = PgEncumbranceRelationshipService.__new__(PgEncumbranceRelationshipService)
    service.engine = None
    service.ori_service = None

    target = {
        "foreclosure_id": 11,
        "strap": "S11",
        "folio": "F11",
        "judgment_data": {},
    }
    find_calls = {"n": 0}

    def _fake_find_targets(**_kwargs: Any) -> list[dict[str, Any]]:
        find_calls["n"] += 1
        if find_calls["n"] == 1:
            return [target]
        if find_calls["n"] == 2:
            return [target]
        return []

    process_results = iter([
        {
            "foreclosure_id": 11,
            "strap": "S11",
            "leads_total": 2,
            "local_matches": 0,
            "searched_instruments": 1,
            "searched_book_pages": 0,
            "docs_found": 1,
            "saved": 1,
            "linked_satisfactions": 0,
            "linked_modifications": 0,
            "holder_updates": 0,
            "changed": True,
        },
        {
            "foreclosure_id": 11,
            "strap": "S11",
            "leads_total": 2,
            "local_matches": 1,
            "searched_instruments": 0,
            "searched_book_pages": 0,
            "docs_found": 0,
            "saved": 0,
            "linked_satisfactions": 0,
            "linked_modifications": 0,
            "holder_updates": 1,
            "changed": True,
        },
    ])

    class _FakeExtractionService:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def run(self, **kwargs: Any) -> dict[str, Any]:
            self.calls.append(kwargs)
            return {
                "extracted": 1,
                "cached": 0,
                "errors": 0,
                "skipped": 0,
                "ori_id_backfilled": 1,
            }

    service.extraction_service = _FakeExtractionService()
    monkeypatch.setattr(service, "_find_targets", _fake_find_targets)
    monkeypatch.setattr(service, "_process_target", lambda _target: next(process_results))

    result = service.run(max_passes=2)

    assert result["passes"] == 2
    assert result["saved"] == 1
    assert result["holder_updates"] == 1
    assert result["reextract_extracted"] == 1
    assert result["reextract_ori_id_backfilled"] == 1
    assert service.extraction_service.calls == [{"straps": ["S11"]}]
