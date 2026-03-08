from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from src.services import pg_judgment_service

if TYPE_CHECKING:
    from pathlib import Path


def _build_service(monkeypatch: Any) -> pg_judgment_service.PgJudgmentService:
    monkeypatch.setattr(
        pg_judgment_service,
        "resolve_pg_dsn",
        lambda _dsn: "postgresql://user:pw@host:5432/db",
    )
    monkeypatch.setattr(pg_judgment_service, "get_engine", lambda _dsn: object())
    return pg_judgment_service.PgJudgmentService()


def test_find_unextracted_pdfs_ignores_non_judgment_documents(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    doc_dir = tmp_path / "26-CA-000001" / "documents"
    doc_dir.mkdir(parents=True)
    judgment_pdf = doc_dir / "final_judgment_2026001111.pdf"
    judgment_pdf.write_bytes(b"%PDF-judgment")
    mortgage_pdf = doc_dir / "mortgage_2024002222.pdf"
    mortgage_pdf.write_bytes(b"%PDF-mortgage")

    monkeypatch.setattr(pg_judgment_service, "FORECLOSURE_DATA_DIR", tmp_path)
    svc = _build_service(monkeypatch)

    assert svc._find_unextracted_pdfs(None) == [  # noqa: SLF001
        {
            "case_number": "26-CA-000001",
            "pdf_path": str(judgment_pdf),
        }
    ]


def test_select_best_judgment_prefers_extracted_metadata_over_filename_stem(
    tmp_path: Path,
) -> None:
    older_case_stem = tmp_path / "final_judgment_26-CA-000001_extracted.json"
    older_case_stem.write_text(
        json.dumps(
            {
                "recording_date": "2025-01-01",
                "instrument_number": "2025000001",
                "legal_description": "LOT 1",
            }
        ),
        encoding="utf-8",
    )
    newer_instrument_stem = tmp_path / "final_judgment_2026009000_extracted.json"
    newer_instrument_stem.write_text(
        json.dumps(
            {
                "recording_date": "2026-01-01",
                "instrument_number": "2026009000",
                "legal_description": "LOT 2",
            }
        ),
        encoding="utf-8",
    )

    best = pg_judgment_service.PgJudgmentService._select_best_judgment(  # noqa: SLF001
        [older_case_stem, newer_instrument_stem]
    )

    assert best is not None
    assert best[0] == newer_instrument_stem
