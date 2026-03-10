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


def test_judgment_amount_rollup_mismatch_fails_validation() -> None:
    payload = _judgment_payload()
    payload["total_judgment_amount"] = 350000.00

    with pytest.raises(ValidationError, match="Itemized sum"):
        JudgmentExtraction.model_validate(payload)


def test_judgment_online_sale_mismatch_fails_validation() -> None:
    payload = _judgment_payload()
    payload["is_online_sale"] = False

    with pytest.raises(ValidationError, match="sale_location indicates an online sale"):
        JudgmentExtraction.model_validate(payload)


def test_lis_pendens_prompt_keeps_real_clerk_parties() -> None:
    schema = LisPendensExtraction.model_json_schema()
    description = schema["properties"]["defendants"]["description"]

    assert "Do NOT include the presiding judge." in description
    assert "If the Clerk of Court is actually named as a party" in description
    assert "Do NOT include the presiding judge or the Clerk of Court." not in description
