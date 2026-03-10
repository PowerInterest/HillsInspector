from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from src.models.judgment_extraction import JudgmentExtraction
from src.models.lis_pendens_extraction import LisPendensExtraction
from src.models.mortgage_extraction import MortgageExtraction


def _judgment_payload() -> dict[str, Any]:
    return {
        "instrument_number": None,
        "recording_book": None,
        "recording_page": None,
        "recording_date": None,
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


def _mortgage_payload() -> dict[str, Any]:
    return {
        "instrument_number": "2020000001",
        "recording_book": "12345",
        "recording_page": "678",
        "recording_date": "2020-01-07",
        "execution_date": "2020-01-01",
        "property_address": "10217 GRANT CREEK DR TAMPA, FL 33647",
        "legal_description": "LOT 6, BLOCK 3, CROSS CREEK PARCEL K PHASE 1D",
        "parcel_id": "172834985C00000000010U",
        "confidence_score": 0.81,
        "unclear_sections": [],
        "mortgage_type": "MTG",
        "mortgagor": "JOHN DOE",
        "mortgagee": "NAVY FEDERAL CREDIT UNION",
        "principal_amount": 267080.05,
        "interest_rate": 6.25,
        "maturity_date": "2050-01-01",
        "is_adjustable_rate": False,
        "mers_min": None,
        "is_mers_nominee": False,
        "association_name": None,
        "has_pud_rider": False,
        "has_condo_rider": False,
    }


def test_judgment_schema_is_strict_and_exposes_required_keys() -> None:
    schema = JudgmentExtraction.model_json_schema()

    assert "raw_text" not in schema.get("properties", {})
    assert schema.get("additionalProperties") is False
    assert "foreclosure_sale_date" in schema.get("required", [])
    assert "plaintiff_type" in schema.get("required", [])

    defendant_schema = schema["$defs"]["Defendant"]
    assert defendant_schema["additionalProperties"] is False
    assert "party_type" in defendant_schema["required"]


def test_other_document_schemas_require_business_fields() -> None:
    mortgage_schema = MortgageExtraction.model_json_schema()

    assert "mortgage_type" in mortgage_schema.get("required", [])
    assert "mortgagor" in mortgage_schema.get("required", [])
    assert "mortgagee" in mortgage_schema.get("required", [])


def test_judgment_rejects_unknown_extra_keys() -> None:
    payload = _judgment_payload()
    payload["bogus_field"] = "should fail"

    with pytest.raises(
        ValidationError,
        match=r"bogus_field|Extra inputs are not permitted",
    ):
        JudgmentExtraction.model_validate(payload)


def test_judgment_requires_explicit_top_level_keys() -> None:
    payload = _judgment_payload()
    payload.pop("foreclosure_sale_date")

    with pytest.raises(ValidationError, match="Missing required field key\\(s\\): foreclosure_sale_date"):
        JudgmentExtraction.model_validate(payload)


def test_judgment_requires_explicit_nested_keys() -> None:
    payload = _judgment_payload()
    payload["defendants"][0].pop("party_type")

    with pytest.raises(ValidationError, match="party_type"):
        JudgmentExtraction.model_validate(payload)


def test_judgment_rejects_invalid_enum_values() -> None:
    payload = _judgment_payload()
    payload["plaintiff_type"] = "bankish"

    with pytest.raises(ValidationError, match="plaintiff_type"):
        JudgmentExtraction.model_validate(payload)


def test_mortgage_rejects_invalid_enum_values() -> None:
    payload = _mortgage_payload()
    payload["mortgage_type"] = "totally custom mortgage"

    with pytest.raises(ValidationError, match="mortgage_type"):
        MortgageExtraction.model_validate(payload)


def test_judgment_requires_non_null_sale_date() -> None:
    payload = _judgment_payload()
    payload["foreclosure_sale_date"] = None

    with pytest.raises(ValidationError, match="REQUIRED: foreclosure_sale_date is missing"):
        JudgmentExtraction.model_validate(payload)


def test_judgment_amount_rollup_recomputes_other_costs_and_warns() -> None:
    payload = _judgment_payload()
    payload["total_judgment_amount"] = 350000.00

    model = JudgmentExtraction.model_validate(payload)
    failures, warnings = model.validate_extraction()

    assert failures == []
    assert model.other_costs == 30655.00
    assert any("major line item may be missing" in warning for warning in warnings)


def test_judgment_negative_rollup_gap_fails_validation() -> None:
    payload = _judgment_payload()
    payload["total_judgment_amount"] = 310000.00

    with pytest.raises(ValidationError, match="exceed total"):
        JudgmentExtraction.model_validate(payload)


def test_judgment_credit_lines_from_raw_text_keep_amount_gate_strict() -> None:
    payload = _judgment_payload()
    payload["principal_amount"] = 35394.64
    payload["interest_amount"] = 3596.82
    payload["late_charges"] = None
    payload["escrow_advances"] = None
    payload["title_search_costs"] = None
    payload["court_costs"] = 1207.17
    payload["attorney_fees"] = 4593.50
    payload["total_judgment_amount"] = 20401.98
    payload["raw_text"] = (
        "The value of Plaintiff's lien is the sum of $35,394.64 in principal; "
        "$3,596.82 in interest; $1,207.17 in costs; and $4,593.50 in attorneys' "
        "fees, all less payments received totaling ($24,390.15), for a total of "
        "$20,401.98."
    )

    model = JudgmentExtraction.model_validate(payload)
    failures, warnings = model.validate_extraction()

    assert failures == []
    assert model.other_costs == 0.0
    assert warnings == []


def test_judgment_subtotal_credit_lines_reconcile_total() -> None:
    payload = _judgment_payload()
    payload["principal_amount"] = 200000.00
    payload["interest_amount"] = 48920.48
    payload["late_charges"] = None
    payload["escrow_advances"] = None
    payload["title_search_costs"] = 275.00
    payload["court_costs"] = 1514.00
    payload["attorney_fees"] = 3240.00
    payload["total_judgment_amount"] = 242674.08
    payload["raw_text"] = (
        "Sub-Total. $254,054.48\n"
        "$ (11,380.40)\n"
        "$242,674.08\n"
    )

    model = JudgmentExtraction.model_validate(payload)
    failures, warnings = model.validate_extraction()

    assert failures == []
    assert model.other_costs == 105.00
    assert warnings == []


def test_judgment_multiline_credit_and_duplicate_late_fee_are_normalized() -> None:
    payload = _judgment_payload()
    payload["principal_amount"] = 5881.43
    payload["interest_amount"] = 437.08
    payload["late_charges"] = 647.49
    payload["escrow_advances"] = None
    payload["title_search_costs"] = None
    payload["court_costs"] = 647.49
    payload["attorney_fees"] = 4593.50
    payload["total_judgment_amount"] = 10719.50
    payload["per_diem_rate"] = None
    payload["raw_text"] = (
        "$647.49 in costs; and $4,593.50 in attorneys' fees, all less payments "
        "received totaling\n"
        "($840.00), for a total of $10,719.50.\n"
    )

    model = JudgmentExtraction.model_validate(payload)
    failures, warnings = model.validate_extraction()

    assert failures == []
    assert model.late_charges is None
    assert model.other_costs == 0.0
    assert warnings == []


def test_judgment_attorney_fee_breakout_uses_authoritative_total() -> None:
    payload = _judgment_payload()
    payload["principal_amount"] = 98932.06
    payload["interest_amount"] = 32358.22
    payload["per_diem_interest"] = 670.95
    payload["late_charges"] = 85.20
    payload["escrow_advances"] = 25388.00
    payload["court_costs"] = 6044.93
    payload["attorney_fees"] = 40471.50
    payload["other_costs"] = 200.00
    payload["total_judgment_amount"] = 191872.15
    payload["raw_text"] = (
        "Attorney’s Fees $20,146.50\n"
        "Attorney's fees $5,400.00\n"
        "Additional Attorney’s fees $14,746.50\n"
        "Property Preservations $200.00\n"
        "GRAND TOTAL $191,872.15\n"
    )

    model = JudgmentExtraction.model_validate(payload)
    failures, warnings = model.validate_extraction()

    assert failures == []
    assert model.attorney_fees == 20146.50
    assert model.other_costs == 7921.29
    assert warnings == []


def test_judgment_confidence_warning_needs_multiple_unclear_sections() -> None:
    payload = _judgment_payload()
    payload["confidence_score"] = 0.98
    payload["unclear_sections"] = ["minor OCR noise"]

    model = JudgmentExtraction.model_validate(payload)
    failures, warnings = model.validate_extraction()

    assert failures == []
    assert not any("confidence_score is 0.98" in warning for warning in warnings)


def test_judgment_corporate_advances_subtotal_does_not_double_count_embedded_costs() -> None:
    payload = _judgment_payload()
    payload["principal_amount"] = 53608.00
    payload["interest_amount"] = 35706.83
    payload["per_diem_rate"] = 418.05
    payload["per_diem_interest"] = None
    payload["late_charges"] = None
    payload["escrow_advances"] = 24516.51
    payload["title_search_costs"] = None
    payload["court_costs"] = 5259.46
    payload["attorney_fees"] = 2620.00
    payload["other_costs"] = 473.54
    payload["total_judgment_amount"] = 120548.26
    payload["raw_text"] = (
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

    model = JudgmentExtraction.model_validate(payload)
    failures, warnings = model.validate_extraction()

    assert failures == []
    assert model.per_diem_rate is None
    assert model.per_diem_interest == 418.05
    assert model.court_costs is None
    assert model.other_costs == 3678.87
    assert warnings == []


def test_judgment_online_sale_mismatch_fails_validation() -> None:
    payload = _judgment_payload()
    payload["is_online_sale"] = False

    with pytest.raises(ValidationError, match="sale_location indicates an online sale"):
        JudgmentExtraction.model_validate(payload)


def test_validate_extraction_keeps_sale_terms_hard_gate_after_mutation() -> None:
    model = JudgmentExtraction.model_validate(_judgment_payload())
    model.is_online_sale = False

    failures, warnings = model.validate_extraction()

    assert warnings == []
    assert any("sale_location indicates an online sale" in failure for failure in failures)


def test_lis_pendens_prompt_keeps_real_clerk_parties() -> None:
    schema = LisPendensExtraction.model_json_schema()
    description = schema["properties"]["defendants"]["description"]

    assert "Do NOT include the presiding judge." in description
    assert "If the Clerk of Court is actually named as a party" in description
    assert "Do NOT include the presiding judge or the Clerk of Court." not in description
