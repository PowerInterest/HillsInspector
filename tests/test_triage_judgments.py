from __future__ import annotations

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
