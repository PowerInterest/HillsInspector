from __future__ import annotations

import datetime as dt
from typing import Any, Self

from src.services import pg_municipal_lien_service


class _FakeResult:
    def __init__(
        self,
        *,
        rows: list[dict[str, Any]] | None = None,
        rowcount: int = 0,
    ) -> None:
        self._rows = rows or []
        self.rowcount = rowcount

    def mappings(self) -> _FakeResult:
        return self

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class _FakeConnection:
    def __init__(
        self,
        *,
        targets: list[dict[str, Any]],
        evidence: list[dict[str, Any]],
    ) -> None:
        self.targets = targets
        self.evidence = evidence
        self.upserts: list[dict[str, Any]] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> bool:
        return False

    def execute(
        self,
        statement: object,
        params: dict[str, Any] | list[dict[str, Any]] | None = None,
    ) -> _FakeResult:
        sql = " ".join(str(statement).split())
        if "FROM foreclosures f" in sql:
            return _FakeResult(rows=self.targets)
        if "FROM ori_encumbrances oe" in sql:
            return _FakeResult(rows=self.evidence)
        if "INSERT INTO municipal_lien_findings" in sql:
            assert isinstance(params, list)
            self.upserts.extend(params)
            return _FakeResult(rowcount=len(params))
        raise AssertionError(f"Unexpected SQL in test: {sql}")


class _FakeEngine:
    def __init__(self, conn: _FakeConnection) -> None:
        self.conn = conn

    def connect(self) -> _FakeConnection:
        return self.conn

    def begin(self) -> _FakeConnection:
        return self.conn


def test_run_phase0_upserts_provider_findings(monkeypatch: Any) -> None:
    targets = [
        {
            "foreclosure_id": 101,
            "strap": "S101",
            "folio": "F101",
            "case_number_raw": "24-CA-000101",
            "property_address": "101 Main St",
        },
        {
            "foreclosure_id": 202,
            "strap": "S202",
            "folio": "F202",
            "case_number_raw": "24-CA-000202",
            "property_address": "202 Main St",
        },
    ]
    evidence = [
        {
            "id": 1,
            "strap": "S101",
            "folio": None,
            "instrument_number": "INST-101",
            "recording_date": dt.date(2025, 1, 15),
            "amount": 1200,
            "party1": "Hillsborough County Public Utilities",
            "party2": "",
            "current_holder": None,
            "legal_description": "",
            "raw_document_type": "LN",
            "encumbrance_type": "lien",
        },
        {
            "id": 2,
            "strap": None,
            "folio": "F202",
            "instrument_number": "INST-202",
            "recording_date": dt.date(2025, 2, 1),
            "amount": 900,
            "party1": "City of Tampa Utilities",
            "party2": "",
            "current_holder": None,
            "legal_description": "",
            "raw_document_type": "LN",
            "encumbrance_type": "lien",
        },
    ]

    conn = _FakeConnection(targets=targets, evidence=evidence)
    monkeypatch.setattr(
        pg_municipal_lien_service,
        "resolve_pg_dsn",
        lambda _dsn: "postgresql://user:pw@host:5432/db",
    )
    monkeypatch.setattr(
        pg_municipal_lien_service,
        "get_engine",
        lambda _dsn: _FakeEngine(conn),
    )

    service = pg_municipal_lien_service.PgMunicipalLienService()
    result = service.run_phase0()

    assert result["targets"] == 2
    assert result["findings_written"] == 6
    assert result["lien_recorded_by_provider"]["hillsborough_water_resources"] == 1
    assert result["lien_recorded_by_provider"]["tampa_conduits"] == 1
    assert result["lien_recorded_by_provider"]["teco"] == 0
    assert len(conn.upserts) == 6

    row_hills = next(
        r for r in conn.upserts
        if r["foreclosure_id"] == 101 and r["provider"] == "hillsborough_water_resources"
    )
    assert row_hills["status"] == "lien_recorded"
    assert row_hills["source"] == "ori_detector"
    assert row_hills["instrument_number"] == "INST-101"

    row_tampa = next(
        r for r in conn.upserts
        if r["foreclosure_id"] == 202 and r["provider"] == "tampa_conduits"
    )
    assert row_tampa["status"] == "lien_recorded"
    assert row_tampa["source"] == "ori_detector"
    assert row_tampa["instrument_number"] == "INST-202"

    row_teco = next(
        r for r in conn.upserts
        if r["foreclosure_id"] == 101 and r["provider"] == "teco"
    )
    assert row_teco["status"] == "not_applicable"
    assert row_teco["source"] == "policy"


def test_run_phase0_skips_when_no_targets(monkeypatch: Any) -> None:
    conn = _FakeConnection(targets=[], evidence=[])
    monkeypatch.setattr(
        pg_municipal_lien_service,
        "resolve_pg_dsn",
        lambda _dsn: "postgresql://user:pw@host:5432/db",
    )
    monkeypatch.setattr(
        pg_municipal_lien_service,
        "get_engine",
        lambda _dsn: _FakeEngine(conn),
    )

    service = pg_municipal_lien_service.PgMunicipalLienService()
    result = service.run_phase0()

    assert result["skipped"] is True
    assert result["reason"] == "no_foreclosures_in_scope"
    assert result["findings_written"] == 0
    assert conn.upserts == []
