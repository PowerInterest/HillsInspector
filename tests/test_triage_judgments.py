from __future__ import annotations

import json

from scripts import triage_judgments


def test_case_numbers_match_accepts_spaced_format() -> None:
    assert triage_judgments.case_numbers_match(
        "2025 CA 008465",
        "292025CA008465A001HC",
        None,
    )


def test_entity_similarity_handles_contained_party_names() -> None:
    similarity = triage_judgments.entity_similarity(
        "CHRISTIANA TRUST, A DIVISION OF WILMINGTON SAVINGS FUND SOCIETY, FSB, "
        "AS TRUSTEE ON BEHALF OF AGATE BAY MORTGAGE TRUST 2015-5",
        "CHRISTIANA TRUST",
    )

    assert similarity >= 0.5


def test_entity_similarity_handles_punctuation_only_differences() -> None:
    similarity = triage_judgments.entity_similarity("L.W.T., INC.", "LWT INC")

    assert similarity == 1.0


def test_addresses_equivalent_handles_spacing_and_suffix_variants() -> None:
    assert triage_judgments.addresses_equivalent(
        "5105 CRESTHILL DRIVE, TAMPA, FL 33615",
        "5105 CREST HILL DR",
    )
    assert triage_judgments.addresses_equivalent(
        "6412N QUEENSWAY DR, TEMPLE TERRACE, FL 33617",
        "6412 N QUEENSWAY DR",
    )


def test_patch_v1_schema_fills_nested_required_keys() -> None:
    patched = triage_judgments.patch_v1_schema(
        {
            "case_number": "25-CA-000001",
            "plaintiff": "BANK",
            "plaintiff_type": "bank",
            "property_address": "1 MAIN ST",
            "legal_description": None,
            "defendants": [{"name": "JOHN DOE"}],
            "foreclosed_mortgage": {},
        }
    )

    assert patched["defendants"][0]["party_type"] == "unknown"
    assert patched["foreclosed_mortgage"]["original_amount"] is None
    assert patched["foreclosed_mortgage"]["original_date"] is None


def test_property_identity_downgrades_when_document_matches_alternate_parcel() -> None:
    case_dir = "292024CA000333A001HC"
    raw = {
        "case_number": "24-CA-000333",
        "property_address": "10217 GRANT CREEK DR TAMPA, FL 33647",
        "subdivision": "CROSS CREEK PARCEL K PHASE 1D",
        "lot": "6",
        "block": "3",
        "raw_text": (
            "Case No. 24-CA-000333\n"
            "Property Address: 10217 GRANT CREEK DR TAMPA, FL 33647\n"
            "LOT 6, BLOCK 3, CROSS CREEK PARCEL K PHASE 1D, ACCORDING TO THE "
            "PLAT THEREOF.\n"
        ),
    }
    fc_index = {
        case_dir: {
            "strap": "WRONG_STRAP",
            "folio": None,
            "address": "5150 BISHOP RD",
            "auction_date": "2026-04-01",
            "archived": False,
        }
    }
    hcpa = {
        "WRONG_STRAP": {
            "strap": "WRONG_STRAP",
            "folio": None,
            "address": "5150 BISHOP RD",
            "address_key": "5150BISHOPRD",
            "raw_sub": "",
            "legal": "W 220 FT OF E 245 OF S 297 FT OF SE 1/4 OF SE 1/4 LESS RD",
            "owner": "",
        },
        "RIGHT_STRAP": {
            "strap": "RIGHT_STRAP",
            "folio": None,
            "address": "10217 GRANT CREEK DR TAMPA, FL 33647",
            "address_key": "10217GRANTCREEKDRTAMPAFL33647",
            "raw_sub": "",
            "legal": "LOT 6 BLOCK 3 CROSS CREEK PARCEL K PHASE 1D",
            "owner": "",
        },
    }

    failures, warnings = triage_judgments.check_property_identity(
        raw,
        case_dir,
        fc_index,
        hcpa,
    )

    assert failures == []
    assert any("alternate HCPA parcel RIGHT_STRAP" in warning for warning in warnings)


def test_property_identity_downgrades_when_pdf_is_grounded_even_without_alt_hcpa() -> None:
    case_dir = "292025CA006142A001HC"
    raw = {
        "case_number": "25-CA-006142",
        "property_address": "4624 W. Bay To Bay Boulevard, Tampa, FL 33629",
        "subdivision": "SUNSET PARK",
        "lot": "10 and the West 5.30 feet of Lot 9",
        "block": "13",
        "raw_text": (
            "Case No. 25-CA-006142\n"
            "Property Address: 4624 W. Bay To Bay Boulevard, Tampa, FL 33629\n"
            "Lot 10 and the West 5.30 feet of Lot 9, Block 13, SUNSET PARK.\n"
        ),
    }
    fc_index = {
        case_dir: {
            "strap": "WRONG_STRAP",
            "folio": None,
            "address": "9610 IVORY DR",
            "auction_date": "2026-04-01",
            "archived": False,
        }
    }
    hcpa = {
        "WRONG_STRAP": {
            "strap": "WRONG_STRAP",
            "folio": None,
            "address": "9610 IVORY DR",
            "address_key": "9610IVORYDR",
            "raw_sub": "",
            "legal": "BELMONT NORTH PHASE 2C LOT 4 BLOCK 41",
            "owner": "",
        }
    }

    failures, warnings = triage_judgments.check_property_identity(
        raw,
        case_dir,
        fc_index,
        hcpa,
    )

    assert failures == []
    assert any("Treat this as a linkage-data problem" in warning for warning in warnings)


def test_property_identity_accepts_normalized_address_variants() -> None:
    case_dir = "292025CA007403A001HC"
    raw = {
        "case_number": "25-CA-007403",
        "property_address": "5105 CRESTHILL DRIVE, TAMPA, FL 33615",
        "subdivision": "SOMETHING",
        "lot": None,
        "block": None,
        "raw_text": "Case No. 25-CA-007403\n5105 CRESTHILL DRIVE, TAMPA, FL 33615",
    }
    fc_index = {
        case_dir: {
            "strap": "STRAP1",
            "folio": None,
            "address": "5105 CREST HILL DR",
            "auction_date": "2026-04-01",
            "archived": False,
        }
    }
    hcpa = {
        "STRAP1": {
            "strap": "STRAP1",
            "folio": None,
            "address": "5105 CREST HILL DR",
            "address_key": triage_judgments.normalize_address_key("5105 CREST HILL DR"),
            "raw_sub": "",
            "legal": "",
            "owner": "",
        }
    }

    failures, warnings = triage_judgments.check_property_identity(
        raw,
        case_dir,
        fc_index,
        hcpa,
    )

    assert failures == []
    assert warnings == []


def test_triage_one_uses_review_status_for_archived_and_orphaned_cases(
    tmp_path,
) -> None:
    payload = {
        "case_number": "25-CA-000001",
        "plaintiff": "BANK",
        "plaintiff_type": "bank",
        "property_address": "1 MAIN ST",
        "legal_description": "LOT 1",
        "judgment_date": "2026-01-01",
        "total_judgment_amount": 100.0,
        "foreclosure_sale_date": "2026-02-01",
        "is_online_sale": False,
        "defendants": [
            {
                "name": "JOHN DOE",
                "party_type": "borrower",
                "is_federal_entity": False,
                "is_deceased": False,
                "lien_recording_reference": None,
            }
        ],
        "unclear_sections": [],
        "confidence_score": 0.5,
        "red_flags": [],
    }
    json_path = tmp_path / "292025CA000001A001HC" / "documents" / "final_judgment_1_extracted.json"
    json_path.parent.mkdir(parents=True)
    json_path.write_text(json.dumps(payload), encoding="utf-8")

    archived_result = triage_judgments.triage_one(
        json_path,
        {
            "292025CA000001A001HC": {
                "strap": "STRAP1",
                "folio": None,
                "address": "1 MAIN ST",
                "auction_date": "2026-02-01",
                "archived": True,
            }
        },
        {},
        {},
    )
    orphaned_result = triage_judgments.triage_one(
        json_path,
        {},
        {},
        {},
    )

    assert archived_result["status"] == "ARCHIVED_REVIEW"
    assert orphaned_result["status"] == "ORPHANED_REVIEW"


def test_triage_one_uses_linkage_review_for_non_extraction_conflicts(
    tmp_path,
) -> None:
    payload = {
        "case_number": "25-CA-000002",
        "plaintiff": "BANK",
        "plaintiff_type": "bank",
        "property_address": "999 WRONG PINE STREET",
        "legal_description": "LOT 1 BLOCK 1 SOME SUBDIVISION",
        "judgment_date": "2026-01-01",
        "total_judgment_amount": 100.0,
        "foreclosure_sale_date": "2026-02-01",
        "is_online_sale": False,
        "defendants": [
            {
                "name": "JOHN DOE",
                "party_type": "borrower",
                "is_federal_entity": False,
                "is_deceased": False,
                "lien_recording_reference": None,
            }
        ],
        "unclear_sections": [],
        "confidence_score": 0.5,
        "red_flags": [],
    }
    json_path = tmp_path / "292025CA000002A001HC" / "documents" / "final_judgment_1_extracted.json"
    json_path.parent.mkdir(parents=True)
    json_path.write_text(json.dumps(payload), encoding="utf-8")

    result = triage_judgments.triage_one(
        json_path,
        {
            "292025CA000002A001HC": {
                "strap": "STRAP1",
                "folio": None,
                "address": "1 RIGHT PINE STREET",
                "auction_date": "2026-02-01",
                "archived": False,
            }
        },
        {
            "STRAP1": {
                "strap": "STRAP1",
                "folio": None,
                "address": "1 RIGHT PINE STREET",
                "address_key": triage_judgments.normalize_address_key(
                    "1 RIGHT PINE STREET"
                ),
                "raw_sub": "",
                "legal": "LOT 9 BLOCK 9 OTHER SUBDIVISION",
                "owner": "",
            }
        },
        {},
    )

    assert result["status"] == "LINKAGE_REVIEW"


def test_triage_one_uses_model_normalization_for_embedded_corporate_advances(
    tmp_path,
) -> None:
    payload = {
        "case_number": "22-CA-010278",
        "plaintiff": "MORTGAGE ASSETS MANAGEMENT, LLC",
        "plaintiff_type": "bank",
        "property_address": "2002 E CRENSHAW ST, TAMPA, FL 33610",
        "legal_description": "LOT 10, BLOCK 6, SEMINOLE CREST ADDITION",
        "subdivision": "SEMINOLE CREST ADDITION",
        "lot": "10",
        "block": "6",
        "judgment_date": "2026-01-21",
        "total_judgment_amount": 120548.26,
        "foreclosure_sale_date": "2026-04-22",
        "is_online_sale": True,
        "sale_location": "https://www.hillsborough.realforeclose.com",
        "principal_amount": 53608.00,
        "interest_amount": 35706.83,
        "per_diem_rate": 418.05,
        "per_diem_interest": None,
        "escrow_advances": 24516.51,
        "court_costs": 5259.46,
        "attorney_fees": 2620.00,
        "other_costs": 473.54,
        "late_charges": None,
        "title_search_costs": None,
        "defendants": [
            {
                "name": "JOHN DOE",
                "party_type": "borrower",
                "is_federal_entity": False,
                "is_deceased": False,
                "lien_recording_reference": None,
            }
        ],
        "unclear_sections": [],
        "confidence_score": 0.9,
        "red_flags": [],
        "raw_text": (
            "Case No. 22-CA-010278\n"
            "Property Address: 2002 E CRENSHAW ST, TAMPA, FL 33610\n"
            "LOT 10, BLOCK 6, SEMINOLE CREST ADDITION\n"
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
        ),
    }
    json_path = tmp_path / "292022CA010278A001HC" / "documents" / "final_judgment_1_extracted.json"
    json_path.parent.mkdir(parents=True)
    json_path.write_text(json.dumps(payload), encoding="utf-8")

    result = triage_judgments.triage_one(
        json_path,
        {
            "292022CA010278A001HC": {
                "strap": "STRAP1",
                "folio": None,
                "address": "2002 E CRENSHAW ST, TAMPA, FL 33610",
                "auction_date": "2026-04-22",
                "archived": False,
            }
        },
        {
            "STRAP1": {
                "strap": "STRAP1",
                "folio": None,
                "address": "2002 E CRENSHAW ST, TAMPA, FL 33610",
                "address_key": triage_judgments.normalize_address_key(
                    "2002 E CRENSHAW ST, TAMPA, FL 33610"
                ),
                "raw_sub": "",
                "legal": "LOT 10 BLOCK 6 SEMINOLE CREST ADDITION",
                "owner": "",
            }
        },
        {},
    )

    assert result["status"] == "GOOD"
    assert not any("AMOUNTS:" in issue for issue in result["issues"])
