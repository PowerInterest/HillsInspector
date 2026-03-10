from __future__ import annotations

from typing import Any, Self, cast

from src.services import pg_loader_clerk


class _FakeSession:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def execute(self, _statement: object) -> None:
        return None


def test_load_civil_alpha_index_preserves_full_ucn(
    monkeypatch: Any,
    tmp_path,
) -> None:
    alpha_file = tmp_path / "CircuitCivilNameIndex_A.txt"
    alpha_file.write_text("ignored\n", encoding="utf-8")

    captured: dict[str, list[dict[str, Any]]] = {"cases": [], "parties": []}
    sample_row = {
        "Uniform Case Number": "292008CA009351A001HC",
        "Court Type": "Circuit",
        "Case Type": "Mortgage Foreclosure",
        "Division": "N",
        "Judge Name": "Judge Example",
        "Current Status": "Open",
        "Date Filed": "01/02/2008",
        "Current Status Date": "02/03/2008",
        "Party Connection Type": "DEFENDANT",
        "LastName": "DOE",
        "FirstName": "JANE",
        "MiddleName": "",
        "Suffix": "",
        "BusinessName": "",
        "Party Address Line 1": "123 MAIN ST",
        "Party Address Line 2": "",
        "Party Address City": "TAMPA",
        "Party Address State": "FL",
        "Party Address Zip Code": "33602",
        "Disposition Code": "",
        "Disposition Description": "",
        "Disposition Date": "",
        "Amount Paid": "",
        "Date Paid": "",
        "AKAs": "",
    }

    monkeypatch.setattr(pg_loader_clerk, "get_session_factory", lambda _dsn: lambda: _FakeSession())
    monkeypatch.setattr(pg_loader_clerk, "_compute_sha256", lambda _path: "sha")
    monkeypatch.setattr(pg_loader_clerk, "_get_existing_ingest_file", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pg_loader_clerk, "_upsert_ingest_file", lambda **_kwargs: 1)
    monkeypatch.setattr(pg_loader_clerk, "_mark_ingest_file", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pg_loader_clerk, "_iter_pipe_delimited", lambda _path: [sample_row])
    monkeypatch.setattr(
        pg_loader_clerk,
        "_upsert_alpha_cases_batch",
        lambda _session, rows: captured["cases"].extend(rows),
    )
    monkeypatch.setattr(
        pg_loader_clerk,
        "_upsert_alpha_parties_batch",
        lambda _session, rows: captured["parties"].extend(rows),
    )

    stats = pg_loader_clerk.load_civil_alpha_index(
        dsn="postgresql://user:pw@host:5432/db",
        root=tmp_path,
    )

    assert stats["files_loaded"] == 1
    assert captured["cases"][0]["case_number"] == "08-CA-009351"
    assert captured["cases"][0]["ucn"] == "292008CA009351A001HC"


def test_insert_criminal_name_index_batch_normalizes_null_conflict_keys(
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {}

    class _FakeInsert:
        def values(self, rows: list[dict[str, Any]]) -> Self:
            captured["rows"] = rows
            return self

        def on_conflict_do_nothing(self, **kwargs: Any) -> Self:
            captured["conflict_kwargs"] = kwargs
            return self

    session = _FakeSession()
    monkeypatch.setattr(pg_loader_clerk, "pg_insert", lambda _model: _FakeInsert())

    pg_loader_clerk._insert_criminal_name_index_batch(  # noqa: SLF001
        cast("Any", session),
        [
            {
                "ucn": "2025CF000001",
                "count_number": None,
                "disposition_code": None,
            }
        ],
    )

    assert captured["rows"] == [
        {
            "ucn": "2025CF000001",
            "count_number": "",
            "disposition_code": "",
        }
    ]
    assert captured["conflict_kwargs"]["constraint"] == "uq_clerk_crim_ni_ucn_count_disp"
