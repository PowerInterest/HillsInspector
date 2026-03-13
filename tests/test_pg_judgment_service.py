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


def test_find_unextracted_pdfs_reextracts_stale_judgment_cache(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    doc_dir = tmp_path / "26-CA-000001" / "documents"
    doc_dir.mkdir(parents=True)
    judgment_pdf = doc_dir / "final_judgment_2026001111.pdf"
    judgment_pdf.write_bytes(b"%PDF-judgment")
    stale_cache = doc_dir / "final_judgment_2026001111_extracted.json"
    stale_cache.write_text(json.dumps({"plaintiff": "BANK"}), encoding="utf-8")

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

    best = pg_judgment_service.PgJudgmentService.select_best_judgment(
        [older_case_stem, newer_instrument_stem]
    )

    assert best is not None
    assert best[0] == newer_instrument_stem


def test_normalize_judgment_payload_repairs_legacy_enum_drift(
    monkeypatch: Any,
) -> None:
    svc = _build_service(monkeypatch)
    payload = {
        "instrument_number": None,
        "recording_book": None,
        "recording_page": None,
        "recording_date": None,
        "execution_date": None,
        "property_address": "3061 SUTTON WOODS DR PLANT CITY, FL 33566",
        "legal_description": "LOT 2, BLOCK 2, WALDEN LAKE UNIT 23",
        "parcel_id": None,
        "confidence_score": 0.8,
        "unclear_sections": [],
        "case_number": "292024CA005094A001HC",
        "court_circuit": "13th",
        "county": "Hillsborough",
        "judge_name": None,
        "judgment_date": "2025-08-28",
        "plaintiff": "SUTTON WOODS CONDOMINIUM ASSOCIATION, INC.",
        "plaintiff_type": "condo_association",
        "defendants": [
            {
                "name": "UNKNOWN TENANTS/OWNERS 1",
                "party_type": "borrower|co_borrower|spouse|hoa|condo_association|tenant|unknown",
                "is_federal_entity": False,
                "is_deceased": False,
                "lien_recording_reference": None,
            }
        ],
        "subdivision": "WALDEN LAKE UNIT 23",
        "lot": "2",
        "block": "2",
        "unit": None,
        "plat_book": None,
        "plat_page": None,
        "is_condo": False,
        "foreclosed_mortgage": {
            "original_date": None,
            "original_amount": 0.0,
            "recording_date": None,
            "recording_book": None,
            "recording_page": None,
            "instrument_number": None,
            "original_lender": None,
            "current_holder": None,
        },
        "lis_pendens": {
            "recording_date": None,
            "recording_book": None,
            "recording_page": None,
            "instrument_number": None,
        },
        "principal_amount": 400000.0,
        "interest_amount": 991.83,
        "interest_through_date": None,
        "per_diem_rate": None,
        "per_diem_interest": None,
        "late_charges": None,
        "escrow_advances": None,
        "title_search_costs": None,
        "court_costs": None,
        "attorney_fees": None,
        "other_costs": 0.0,
        "total_judgment_amount": 400991.83,
        "foreclosure_sale_date": "2025-10-27",
        "sale_location": "https://www.hillsborough.realforeclose.com",
        "is_online_sale": True,
        "foreclosure_type": "FIRST MORTGAGE",
        "hoa_safe_harbor_mentioned": False,
        "superiority_language": None,
        "plaintiff_maximum_bid": None,
        "monthly_payment": None,
        "default_date": None,
        "service_by_publication": False,
        "red_flags": [
            {
                "flag_type": "missing_document_pages",
                "severity": "critical",
                "description": "Pages 1-5 are missing from the cache export.",
            },
            {
                "flag_type": "federal_defendant|lost_note|deceased_borrower|service_issue|missing_hoa_defendant",
                "severity": "critical|high|medium",
                "description": "United States of America is a named junior lienholder.",
            },
        ],
    }

    normalized, _, _ = svc.normalize_judgment_payload(payload)
    validation = svc.validate_judgment_payload(normalized)

    assert normalized["plaintiff_type"] == "hoa"
    assert normalized["defendants"][0]["party_type"] == "tenant"
    assert normalized["red_flags"] == [
        {
            "flag_type": "federal_defendant",
            "severity": "critical",
            "description": "United States of America is a named junior lienholder.",
        }
    ]
    assert "Pages 1-5 are missing from the cache export." in normalized["unclear_sections"]
    assert validation["is_valid"] is True


def test_normalize_judgment_payload_preserves_raw_text_for_credit_reconciliation(
    monkeypatch: Any,
) -> None:
    svc = _build_service(monkeypatch)
    payload = {
        "instrument_number": "2025001111",
        "recording_book": None,
        "recording_page": None,
        "recording_date": "2025-01-01",
        "execution_date": None,
        "property_address": "123 MAIN ST TAMPA, FL 33602",
        "legal_description": "LOT 1, BLOCK 1, TEST SUBDIVISION",
        "parcel_id": None,
        "confidence_score": 0.9,
        "unclear_sections": [],
        "case_number": "25-CA-000111",
        "court_circuit": "13th",
        "county": "Hillsborough",
        "judge_name": "JANE DOE",
        "judgment_date": "2025-01-01",
        "plaintiff": "TEST BANK",
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
        "subdivision": None,
        "lot": None,
        "block": None,
        "unit": None,
        "plat_book": None,
        "plat_page": None,
        "is_condo": False,
        "foreclosed_mortgage": {
            "original_date": None,
            "original_amount": None,
            "recording_date": None,
            "recording_book": None,
            "recording_page": None,
            "instrument_number": None,
            "original_lender": None,
            "current_holder": None,
        },
        "lis_pendens": {
            "recording_date": None,
            "recording_book": None,
            "recording_page": None,
            "instrument_number": None,
        },
        "principal_amount": 100.0,
        "interest_amount": 50.0,
        "interest_through_date": None,
        "per_diem_rate": None,
        "per_diem_interest": None,
        "late_charges": None,
        "escrow_advances": None,
        "title_search_costs": 20.0,
        "court_costs": 10.0,
        "attorney_fees": 30.0,
        "other_costs": 0.0,
        "total_judgment_amount": 185.0,
        "foreclosure_sale_date": "2025-02-01",
        "sale_location": "http://www.hillsborough.realforeclose.com",
        "is_online_sale": True,
        "foreclosure_type": "FIRST MORTGAGE",
        "hoa_safe_harbor_mentioned": False,
        "superiority_language": None,
        "plaintiff_maximum_bid": None,
        "monthly_payment": None,
        "default_date": None,
        "service_by_publication": False,
        "red_flags": [],
        "raw_text": "Amounts Due\\nLess: Suspense Balance ($25.00)\\nTOTAL SUM $185.00",
    }

    normalized, _, _ = svc.normalize_judgment_payload(payload)

    assert normalized["raw_text"] == payload["raw_text"]
    assert svc.validate_judgment_payload(normalized)["is_valid"] is True

    without_raw = dict(normalized)
    without_raw.pop("raw_text")
    assert svc.validate_judgment_payload(without_raw)["is_valid"] is False
