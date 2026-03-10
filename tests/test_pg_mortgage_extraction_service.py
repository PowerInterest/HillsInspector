from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any
from typing import cast
from typing import Self

import fitz

from src.services import pg_mortgage_extraction_service


class _DummyStorage:
    pass


class _DummyVision:
    pass


class _CaptureMappings:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[dict[str, Any]]:
        return self._rows


class _CaptureResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> _CaptureMappings:
        return _CaptureMappings(self._rows)


class _CaptureConnection:
    def __init__(self, captured: dict[str, Any], rows: list[dict[str, Any]]) -> None:
        self._captured = captured
        self._rows = rows

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: Any, params: dict[str, Any] | None = None) -> _CaptureResult:
        self._captured["sql"] = str(sql)
        self._captured["params"] = params or {}
        return _CaptureResult(self._rows)


class _CaptureEngine:
    def __init__(self, captured: dict[str, Any], rows: list[dict[str, Any]]) -> None:
        self._captured = captured
        self._rows = rows

    def connect(self) -> _CaptureConnection:
        return _CaptureConnection(self._captured, self._rows)


def _build_service(monkeypatch: Any) -> pg_mortgage_extraction_service.PgMortgageExtractionService:
    monkeypatch.setattr(
        pg_mortgage_extraction_service,
        "resolve_pg_dsn",
        lambda _dsn: "postgresql://user:pw@host:5432/db",
    )
    monkeypatch.setattr(
        pg_mortgage_extraction_service,
        "get_engine",
        lambda _dsn: object(),
    )
    monkeypatch.setattr(
        pg_mortgage_extraction_service,
        "ScraperStorage",
        _DummyStorage,
    )
    monkeypatch.setattr(
        pg_mortgage_extraction_service,
        "VisionService",
        _DummyVision,
    )
    return pg_mortgage_extraction_service.PgMortgageExtractionService()


def test_find_unextracted_mortgages_scopes_to_target_straps(monkeypatch: Any) -> None:
    service = _build_service(monkeypatch)
    captured: dict[str, Any] = {}
    service.engine = _CaptureEngine(
        captured,
        rows=[
            {
                "id": 1,
                "ori_id": "abc",
                "instrument_number": "2025000001",
                "case_number": "24-CA-000001",
                "folio": "F1",
                "strap": "S1",
            }
        ],
    )

    rows = service._find_unextracted_mortgages(5, straps=["S1", "S2"])  # noqa: SLF001

    assert rows[0]["strap"] == "S1"
    assert captured["params"]["straps"] == ["S1", "S2"]
    assert captured["params"]["limit"] == 5
    sql_text = captured["sql"].lower()
    assert "strap = any(:straps)" in sql_text
    assert "limit :limit" in sql_text


def test_process_single_skips_partial_extract_db_write(
    monkeypatch: Any,
    tmp_path: Any,
) -> None:
    service = _build_service(monkeypatch)
    pdf_path = tmp_path / "mortgage.pdf"
    doc = fitz.open()
    doc.new_page()
    doc.save(pdf_path)
    doc.close()

    service.storage = SimpleNamespace(
        document_exists=lambda **_kwargs: pdf_path,
    )
    service.vision = SimpleNamespace(
        extract_json=lambda *_args, **_kwargs: {
            "borrower": "Borrower Name",
        },
    )
    monkeypatch.setattr(
        service,
        "_save_to_pg",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("_save_to_pg should not be called for partial extracts")
        ),
    )

    success = asyncio.run(
        service._process_single(  # noqa: SLF001
            cast("Any", object()),
            {
                "instrument_number": "2025000001",
                "case_number": "24-CA-000001",
                "id": 7,
                "ori_id": "abc123",
            },
        )
    )

    assert success is False
    assert not (tmp_path / "mortgage_extracted.json").exists()
