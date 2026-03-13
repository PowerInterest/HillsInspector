from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

from PIL import Image

import src.services.final_judgment_processor as final_judgment_processor_module
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


def test_canonicalize_candidate_applies_amount_normalizations() -> None:
    candidate = _valid_candidate()
    candidate["principal_amount"] = 53608.00
    candidate["interest_amount"] = 35706.83
    candidate["per_diem_rate"] = 418.05
    candidate["per_diem_interest"] = None
    candidate["late_charges"] = None
    candidate["escrow_advances"] = 24516.51
    candidate["title_search_costs"] = None
    candidate["court_costs"] = 5259.46
    candidate["attorney_fees"] = 2620.00
    candidate["other_costs"] = 473.54
    candidate["total_judgment_amount"] = 120548.26
    candidate["raw_text"] = (
        "Principal due on the note secured by the mortgage foreclosed: $53,608.00\n"
        "Interest on the note and mortgage to 12/31/2025: $35,706.83\n"
        "Intra Month Per Diem Interest good through 1/21/2026: $418.05\n"
        "MIP: $180.31\n"
        "Servicing Fees: $3,453.56\n"
        "Corporate Advances: $24,516.51\n"
        "Probate Review: $250.00\n"
        "Death Certificate: $13.50\n"
        "Skip Trace: $8.04\n"
        "Lis Pendens: $10.00\n"
        "Complaint Filing Fee: $938.78\n"
        "Clerk Summons: $135.39\n"
        "Publication: $203.02\n"
        "Service of Process: $3,536.25\n"
        "Attendance at Court: $975.00\n"
        "Document Preparation: $100.00\n"
        "Motions for Amended Complaint: $925.00\n"
        "Flat Fee Already Paid Out: $3,480.00\n"
        "Remaining Corporate Advances: $13,941.53\n"
        "Additional Costs:\n"
        "Death Certificate $45.00\n"
        "Outstanding Attorneys' Fee Total: $2,620.00\n"
        "TOTAL SUM $120,548.26\n"
    )

    canonical = FinalJudgmentProcessor.canonicalize_candidate(candidate)

    assert canonical["per_diem_rate"] is None
    assert canonical["per_diem_interest"] == 418.05
    assert canonical["court_costs"] is None
    assert canonical["other_costs"] == 3678.87


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


def test_repair_prompt_targets_amount_reconciliation_failures() -> None:
    processor = FinalJudgmentProcessor()

    prompt = processor._build_text_extraction_prompt(  # noqa: SLF001
        "--- PAGE 1 ---\nAmounts Due",
        current_candidate=_valid_candidate(),
        validation_failures=[
            "FAIL: Known line items $203,950.86 exceed total $191,872.15 by $12,078.71 (>19 threshold)"
        ],
    )

    assert "Repair focus:" in prompt
    assert "authoritative 'Amounts Due' table" in prompt
    assert "Use only the final awarded attorney fee total" in prompt
    assert "Funds Held in Suspense" in prompt
    assert "Recoverable Balance" in prompt
    assert "store that amount only once" in prompt
    assert "Do not double-count SUBTOTAL lines" in prompt
    assert "force-placed insurance" in prompt


def test_repair_prompt_targets_sale_and_property_failures() -> None:
    processor = FinalJudgmentProcessor()
    current = _valid_candidate()
    current["foreclosure_sale_date"] = None
    current["sale_location"] = "http://www.hillsborough.realforeclose.com"
    current["is_online_sale"] = True
    current["property_address"] = None
    current["legal_description"] = None

    prompt = processor._build_text_extraction_prompt(  # noqa: SLF001
        "--- PAGE 1 ---\nFinal Foreclosure Judgment",
        current_candidate=current,
        validation_failures=[
            "REQUIRED: foreclosure_sale_date is missing",
            "REQUIRED: need at least one of legal_description or property_address to identify the property",
            "FAIL: is_online_sale is true but foreclosure_sale_date is missing",
        ],
    )

    assert "Repair focus:" in prompt
    assert "Re-read the 'Lien on Property' paragraph" in prompt
    assert "Re-read the 'Sale of Property' paragraph" in prompt
    assert "Ignore unrelated URLs in headers" in prompt
    assert "clear sale_location and set is_online_sale=false" in prompt


def test_ocr_text_covers_all_pages_rejects_gaps() -> None:
    assert FinalJudgmentProcessor._ocr_text_covers_all_pages(  # noqa: SLF001
        "--- PAGE 1 ---\nA\n--- PAGE 2 ---\nB\n--- PAGE 3 ---\nC",
        3,
    )
    assert not FinalJudgmentProcessor._ocr_text_covers_all_pages(  # noqa: SLF001
        "--- PAGE 1 ---\nA\n--- PAGE 3 ---\nC",
        3,
    )


def test_cache_is_current_rejects_incomplete_ocr_cache() -> None:
    cached = _valid_candidate()
    cached["raw_text"] = "--- PAGE 1 ---\nA\n--- PAGE 3 ---\nC"
    cached["_metadata"] = {
        "cache_format_version": FinalJudgmentProcessor._CACHE_FORMAT_VERSION,  # noqa: SLF001
        "total_pages": 3,
    }
    cached["_validation"] = {"is_valid": True, "failures": [], "warnings": []}

    assert not FinalJudgmentProcessor._cache_is_current(cached)  # noqa: SLF001


def test_ocr_images_to_page_texts_uses_rescue_config(tmp_path, monkeypatch: Any) -> None:
    processor = FinalJudgmentProcessor()
    image_path = tmp_path / "page.png"
    Image.new("RGB", (8, 8), color="white").save(image_path)
    captured: dict[str, Any] = {}

    def _fake_image_to_string(image: Image.Image, *, config: str) -> str:
        captured["config"] = config
        captured["mode"] = image.mode
        return "TEXT"

    monkeypatch.setattr(
        final_judgment_processor_module.pytesseract,
        "image_to_string",
        _fake_image_to_string,
    )

    page_texts = processor._ocr_images_to_page_texts(  # noqa: SLF001
        [str(image_path)],
        preprocess=True,
        user_defined_dpi=300,
    )

    assert page_texts == ["--- PAGE 1 ---\nTEXT"]
    assert "--oem 1" in captured["config"]
    assert "--psm 6" in captured["config"]
    assert "user_defined_dpi=300" in captured["config"]
    assert captured["mode"] in {"1", "L"}


def test_process_pdf_uses_high_res_ocr_rescue_for_invalid_initial_extraction(
    tmp_path,
    monkeypatch: Any,
) -> None:
    processor = FinalJudgmentProcessor()
    processor.vision_service._active_endpoint = {  # noqa: SLF001
        "model": "test-model",
        "url": "http://local.test",
    }
    pdf_path = tmp_path / "final_judgment_2026001111.pdf"
    pdf_path.write_bytes(b"%PDF-1.4")
    base_candidate = _valid_candidate()
    base_candidate["foreclosure_sale_date"] = None
    rescue_candidate = _valid_candidate()
    calls: dict[str, list[Any]] = {"render": [], "ocr": []}

    def _fake_render(
        pdf_path_arg: str,
        case_number_arg: str,
        *,
        dpi: int,
        suffix: str = "",
    ) -> tuple[list[str], int]:
        calls["render"].append((pdf_path_arg, case_number_arg, dpi, suffix))
        prefix = "rescue" if suffix else "base"
        return ([f"{prefix}-page-1.png"], 1)

    def _fake_ocr(
        image_paths: list[str],
        *,
        preprocess: bool = False,
        user_defined_dpi: int | None = None,
    ) -> list[str]:
        calls["ocr"].append((tuple(image_paths), preprocess, user_defined_dpi))
        if preprocess:
            return ["--- PAGE 1 ---\nRESCUE OCR"]
        return ["--- PAGE 1 ---\nBASE OCR"]

    def _fake_extract(
        ocr_text: str,
        current_candidate: dict[str, Any] | None = None,
        validation_failures: list[str] | None = None,
    ) -> dict[str, Any]:
        candidate = deepcopy(rescue_candidate if "RESCUE OCR" in ocr_text else base_candidate)
        candidate["raw_text"] = ocr_text
        return candidate

    def _never_needs_full_pass(_extracted_data: dict[str, Any] | None) -> bool:
        return False

    def _noop_batches(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(processor, "_render_pdf_to_images", _fake_render)
    monkeypatch.setattr(processor, "_ocr_images_to_page_texts", _fake_ocr)
    monkeypatch.setattr(processor, "_select_priority_pages", lambda page_texts: page_texts)
    monkeypatch.setattr(processor, "_extract_candidate_from_text", _fake_extract)
    monkeypatch.setattr(processor, "_needs_full_pass", _never_needs_full_pass)
    monkeypatch.setattr(processor, "_extract_in_batches", _noop_batches)
    monkeypatch.setattr(processor, "_extract_candidate_from_images", _noop_batches)
    monkeypatch.setattr(processor, "_repair_candidate", _noop_batches)

    result = processor.process_pdf(str(pdf_path), "24-CA-TEST", force=True)

    assert result is not None
    assert result["_validation"]["is_valid"] is True
    assert result["raw_text"] == "--- PAGE 1 ---\nRESCUE OCR"
    assert "ocr_text_rescue" in result["_metadata"]["extraction_strategies"]
    assert calls["render"] == [
        (str(pdf_path), "24-CA-TEST", 150, ""),
        (str(pdf_path), "24-CA-TEST", 300, "ocr_rescue"),
    ]
    assert calls["ocr"] == [
        (("base-page-1.png",), False, 150),
        (("rescue-page-1.png",), True, 300),
    ]
