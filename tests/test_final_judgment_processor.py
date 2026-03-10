from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

from src.services.final_judgment_processor import FinalJudgmentProcessor


def _valid_candidate() -> dict[str, Any]:
    return {
        "instrument_number": None,
        "recording_book": None,
        "recording_page": None,
        "recording_date": None,
        "execution_date": None,
        "property_address": "10217 GRANT CREEK DR TAMPA, FL 33647",
        "legal_description": (
            "LOT 6, BLOCK 3, CROSS CREEK PARCEL K PHASE 1D, ACCORDING TO THE "
            "PLAT THEREOF AS RECORDED IN PLAT BOOK 89, PAGE 51, OF THE PUBLIC "
            "RECORDS OF HILLSBOROUGH COUNTY, FLORIDA."
        ),
        "parcel_id": None,
        "confidence_score": 0.82,
        "unclear_sections": [],
        "case_number": "24-CA-000333",
        "court_circuit": "13th",
        "county": "Hillsborough",
        "judge_name": "VICTOR D. CRIST",
        "judgment_date": "2026-01-26",
        "plaintiff": "NAVY FEDERAL CREDIT UNION",
        "plaintiff_type": "bank",
        "defendants": [
            {
                "name": "DEBRA D RILEY",
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
        "principal_amount": 267080.05,
        "interest_amount": 22784.32,
        "interest_through_date": "2026-01-07",
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
        "sale_location": "http://www.hillsborough.realforeclose.com",
        "is_online_sale": True,
        "foreclosure_type": "FIRST MORTGAGE",
        "hoa_safe_harbor_mentioned": False,
        "superiority_language": None,
        "plaintiff_maximum_bid": None,
        "monthly_payment": None,
        "default_date": "2024-12-01",
        "service_by_publication": False,
        "red_flags": [],
    }


def test_validation_summary_rejects_missing_sale_and_property_identity() -> None:
    candidate = _valid_candidate()
    candidate["property_address"] = None
    candidate["legal_description"] = None
    candidate["foreclosure_sale_date"] = None

    summary = FinalJudgmentProcessor.validation_summary(candidate)

    assert summary["is_valid"] is False
    assert any("foreclosure_sale_date" in failure for failure in summary["failures"])
    assert any(
        "legal_description or property_address" in failure
        for failure in summary["failures"]
    )


def test_merge_page_data_prefers_richer_property_and_financial_sections() -> None:
    processor = FinalJudgmentProcessor()
    early_candidate = _valid_candidate()
    early_candidate["legal_description"] = "LOT 6, BLOCK 3"
    early_candidate["subdivision"] = None
    early_candidate["plat_book"] = None
    early_candidate["plat_page"] = None
    early_candidate["property_address"] = None
    early_candidate["other_costs"] = None
    early_candidate["unclear_sections"] = ["First pass missed cost carry"]
    early_candidate["defendants"].append(
        {
            "name": "DEBRA D RILEY",
            "party_type": "unknown",
            "is_federal_entity": False,
            "is_deceased": False,
            "lien_recording_reference": None,
        }
    )

    repaired_candidate = _valid_candidate()
    repaired_candidate["defendants"].append(
        {
            "name": "AQUA FINANCE, INC.",
            "party_type": "judgment_creditor",
            "is_federal_entity": False,
            "is_deceased": False,
            "lien_recording_reference": None,
        }
    )
    repaired_candidate["unclear_sections"] = ["Judge signature is light but legible"]

    merged = processor._merge_page_data([early_candidate, repaired_candidate])  # noqa: SLF001
    summary = FinalJudgmentProcessor.validation_summary(merged)

    assert merged["legal_description"] == repaired_candidate["legal_description"]
    assert merged["property_address"] == repaired_candidate["property_address"]
    assert merged["other_costs"] == repaired_candidate["other_costs"]
    assert len(merged["defendants"]) == 2
    assert "First pass missed cost carry" in merged["unclear_sections"]
    assert "Judge signature is light but legible" in merged["unclear_sections"]
    assert summary["is_valid"] is True


def test_canonicalize_candidate_dedupes_duplicate_defendants() -> None:
    candidate = _valid_candidate()
    candidate["defendants"].append(deepcopy(candidate["defendants"][0]))

    canonical = FinalJudgmentProcessor.canonicalize_candidate(candidate)

    assert len(canonical["defendants"]) == 1


def test_extract_candidate_from_text_uses_text_endpoint_and_keeps_raw_text(
    monkeypatch: Any,
) -> None:
    processor = FinalJudgmentProcessor()
    captured: dict[str, Any] = {}

    def _fake_analyze_text(
        prompt: str,
        *,
        max_tokens: int,
        response_format: dict[str, Any],
    ) -> str:
        captured["prompt"] = prompt
        captured["max_tokens"] = max_tokens
        captured["response_format"] = response_format
        return json.dumps(_valid_candidate())

    monkeypatch.setattr(processor.vision_service, "analyze_text", _fake_analyze_text)

    ocr_text = "--- PAGE 1 ---\nFINAL JUDGMENT OF FORECLOSURE"
    result = processor._extract_candidate_from_text(ocr_text)  # noqa: SLF001

    assert result is not None
    assert result["raw_text"] == ocr_text
    assert captured["max_tokens"] == 6000
    assert captured["response_format"]["type"] == "json_schema"
    assert "OCR Text:" in captured["prompt"]
    assert ocr_text in captured["prompt"]
