from __future__ import annotations

from src.services.lien_survival.survival_service import SurvivalService


def test_association_foreclosure_prefers_association_lis_pendens_over_old_mortgage() -> None:
    service = SurvivalService("STRAP-1")
    encumbrances = [
        {
            "id": 1,
            "encumbrance_type": "mortgage",
            "creditor": "GOLD BANK",
            "recording_date": "2004-02-03",
            "instrument": "2004041140",
            "is_satisfied": False,
        },
        {
            "id": 2,
            "encumbrance_type": "lis_pendens",
            "creditor": "TOWERS OF CHANNELSIDE CONDOMINIUM ASSOCIATION INC",
            "recording_date": "2025-02-05",
            "instrument": "2025020501",
            "is_satisfied": False,
        },
        {
            "id": 3,
            "encumbrance_type": "judgment",
            "creditor": "THE TOWERS OF CHANNELSIDE CONDOMINIUM ASSOCIATION, INC.",
            "recording_date": "2026-02-05",
            "instrument": "2026020501",
            "is_satisfied": False,
        },
    ]
    judgment_data = {
        "plaintiff": "The Towers of Channelside Condominium Association, Inc.",
        "foreclosure_type": "CONDO",
        "defendants": [],
        "foreclosing_refs": {"instrument": "2004041140"},
    }

    result = service.analyze(encumbrances, judgment_data, [], None)

    foreclosing = result["results"]["foreclosing"]
    assert len(foreclosing) == 1
    assert foreclosing[0]["id"] == 2
    assert foreclosing[0]["survival_status"] == "FORECLOSING"
    assert "Association foreclosing lis pendens" in foreclosing[0]["survival_reason"]

    mortgage = next(enc for enc in result["results"]["survived"] if enc["id"] == 1)
    assert mortgage["survival_status"] == "SURVIVED"
    assert mortgage["survival_reason"] == "Senior to foreclosing lien"


def test_association_plaintiff_infers_association_foreclosure_without_type() -> None:
    service = SurvivalService("STRAP-2")
    encumbrances = [
        {
            "id": 10,
            "encumbrance_type": "mortgage",
            "creditor": "LEGACY BANK",
            "recording_date": "2012-06-01",
            "instrument": "2012060101",
            "is_satisfied": False,
        },
        {
            "id": 11,
            "encumbrance_type": "lis_pendens",
            "creditor": "SEASIDE HOMEOWNERS ASSOCIATION INC",
            "recording_date": "2025-08-11",
            "instrument": "2025081101",
            "is_satisfied": False,
        },
    ]
    judgment_data = {
        "plaintiff": "Seaside Homeowners Association, Inc.",
        "foreclosure_type": "",
        "defendants": [],
    }

    result = service.analyze(encumbrances, judgment_data, [], None)

    foreclosing = result["results"]["foreclosing"]
    assert len(foreclosing) == 1
    assert foreclosing[0]["id"] == 11
    assert foreclosing[0]["survival_status"] == "FORECLOSING"


def test_mortgage_lender_with_homeowner_in_name_not_misclassified_as_hoa() -> None:
    """Regression: 'HOMEOWNERS FINANCIAL GROUP USA LLC' is a mortgage lender,
    NOT an HOA.  When foreclosure_type is 'MORTGAGE FORECLOSURE', the plaintiff
    name heuristic must NOT override it to HOA, which would skip exact
    foreclosing_refs matching and pick a lis pendens instead of the mortgage."""
    service = SurvivalService("STRAP-3")
    encumbrances = [
        {
            "id": 20,
            "encumbrance_type": "mortgage",
            "creditor": "HOMEOWNERS FINANCIAL GROUP USA LLC",
            "recording_date": "2018-03-15",
            "instrument": "2018031501",
            "is_satisfied": False,
        },
        {
            "id": 21,
            "encumbrance_type": "lis_pendens",
            "creditor": "HOMEOWNERS FINANCIAL GROUP USA LLC",
            "recording_date": "2024-11-01",
            "instrument": "2024110101",
            "is_satisfied": False,
        },
    ]
    judgment_data = {
        "plaintiff": "HOMEOWNERS FINANCIAL GROUP USA LLC",
        "foreclosure_type": "MORTGAGE FORECLOSURE",
        "defendants": ["JOHN DOE", "JANE DOE"],
        "foreclosing_refs": {"instrument": "2018031501"},
    }

    result = service.analyze(encumbrances, judgment_data, [], None)

    # The mortgage (id=20) must be identified as the foreclosing lien
    # via exact instrument match (Step 3a), NOT the lis pendens.
    foreclosing = result["results"]["foreclosing"]
    assert len(foreclosing) == 1
    assert foreclosing[0]["id"] == 20
    assert foreclosing[0]["survival_status"] == "FORECLOSING"
    assert "INSTRUMENT_MATCH" in foreclosing[0]["survival_reason"].upper() or \
           "foreclosing lien" in foreclosing[0]["survival_reason"]

    # The LP should NOT be marked as foreclosing
    lp = next(
        enc for enc in encumbrances if enc["id"] == 21
    )
    assert lp["survival_status"] != "FORECLOSING"


def test_homeowner_in_plaintiff_no_fc_type_not_hoa_without_association() -> None:
    """A plaintiff name containing 'HOMEOWNER' but NOT 'ASSOCIATION' or 'ASSN'
    should NOT trigger HOA inference, even when foreclosure_type is blank."""
    service = SurvivalService("STRAP-4")
    encumbrances = [
        {
            "id": 30,
            "encumbrance_type": "mortgage",
            "creditor": "HOMEOWNERS FINANCIAL GROUP USA LLC",
            "recording_date": "2019-01-10",
            "instrument": "2019011001",
            "is_satisfied": False,
        },
        {
            "id": 31,
            "encumbrance_type": "lis_pendens",
            "creditor": "HOMEOWNERS FINANCIAL GROUP USA LLC",
            "recording_date": "2025-05-01",
            "instrument": "2025050101",
            "is_satisfied": False,
        },
    ]
    judgment_data = {
        "plaintiff": "HOMEOWNERS FINANCIAL GROUP USA LLC",
        "foreclosure_type": "",  # blank — heuristic could apply
        "defendants": ["ALICE SMITH"],
        "foreclosing_refs": {"instrument": "2019011001"},
    }

    result = service.analyze(encumbrances, judgment_data, [], None)

    # Should use Step 3a exact instrument match on the mortgage, not HOA path
    foreclosing = result["results"]["foreclosing"]
    assert len(foreclosing) == 1
    assert foreclosing[0]["id"] == 30
    assert foreclosing[0]["survival_status"] == "FORECLOSING"


def test_mortgage_case_prefers_underlying_mortgage_over_matching_lis_pendens() -> None:
    service = SurvivalService("STRAP-5")
    encumbrances = [
        {
            "id": 40,
            "encumbrance_type": "mortgage",
            "creditor": "NAVY FEDERAL CREDIT UNION",
            "debtor": "MAIDA AMANDA, MAIDA CHRISTOPHER S",
            "recording_date": "2014-09-26",
            "instrument": "2014318871",
            "is_satisfied": False,
        },
        {
            "id": 41,
            "encumbrance_type": "lis_pendens",
            "creditor": "NAVY FEDERAL CREDIT UNION",
            "recording_date": "2022-09-08",
            "instrument": "2022438726",
            "is_satisfied": False,
        },
    ]
    judgment_data = {
        "plaintiff": "NAVY FEDERAL CREDIT UNION",
        "foreclosure_type": "FIRST MORTGAGE FORECLOSURE",
        "defendants": [],
    }

    result = service.analyze(encumbrances, judgment_data, [], None)

    foreclosing = result["results"]["foreclosing"]
    assert len(foreclosing) == 1
    assert foreclosing[0]["id"] == 40
    assert foreclosing[0]["survival_status"] == "FORECLOSING"

    lp = next(enc for enc in result["results"]["historical"] if enc["id"] == 41)
    assert lp["survival_reason"] == "Lis pendens is procedural notice, not an independent encumbrance"


def test_mortgage_case_matches_plaintiff_against_debtor_side_when_needed() -> None:
    service = SurvivalService("STRAP-6")
    encumbrances = [
        {
            "id": 50,
            "encumbrance_type": "mortgage",
            "creditor": "BORROWER NAME",
            "debtor": "ROCKET MORTGAGE LLC, QUICKEN LOANS LLC",
            "recording_date": "2021-12-07",
            "instrument": "2021630628",
            "is_satisfied": False,
        },
        {
            "id": 51,
            "encumbrance_type": "assignment",
            "creditor": "MORTGAGE ELECTRONIC REGISTRATION SYSTEMS INC NOM, QUICKEN LOANS LLC, ROCKET MORTGAGE LLC",
            "debtor": "BORROWER NAME",
            "recording_date": "2024-10-15",
            "instrument": "2024419943",
            "is_satisfied": False,
        },
    ]
    judgment_data = {
        "plaintiff": "ROCKET MORTGAGE, LLC F/K/A QUICKEN LOANS, LLC",
        "foreclosure_type": "MORTGAGE FORECLOSURE",
        "defendants": [],
    }

    result = service.analyze(encumbrances, judgment_data, [], None)

    foreclosing = result["results"]["foreclosing"]
    assert len(foreclosing) == 1
    assert foreclosing[0]["id"] == 50
    assert foreclosing[0]["survival_status"] == "FORECLOSING"

    assignment = next(enc for enc in result["results"]["historical"] if enc["id"] == 51)
    assert assignment["survival_reason"] == (
        "Assignment transfers lien ownership; not an independent encumbrance"
    )


def test_same_case_judgment_is_not_treated_as_surviving_independent_lien() -> None:
    service = SurvivalService("STRAP-7")
    encumbrances = [
        {
            "id": 60,
            "encumbrance_type": "mortgage",
            "creditor": "WELLS FARGO BANK NA",
            "debtor": "KERRI BROWNING",
            "recording_date": "2011-06-17",
            "instrument": "2011199740",
            "is_satisfied": False,
            "case_number": "",
        },
        {
            "id": 61,
            "encumbrance_type": "judgment",
            "creditor": "WELLS FARGO BANK NA",
            "debtor": "KERRI BROWNING",
            "recording_date": "2026-01-21",
            "instrument": "2026024338",
            "is_satisfied": False,
            "case_number": "292025CA000651A001HC",
        },
    ]
    judgment_data = {
        "plaintiff": "Wells Fargo Bank, N.A.",
        "foreclosure_type": "MORTGAGE FORECLOSURE",
        "case_number": "2025 CA 000651",
        "defendants": [],
    }

    result = service.analyze(encumbrances, judgment_data, [], None)

    foreclosing = result["results"]["foreclosing"]
    assert len(foreclosing) == 1
    assert foreclosing[0]["id"] == 60

    same_case_judgment = next(enc for enc in result["results"]["historical"] if enc["id"] == 61)
    assert same_case_judgment["survival_reason"] == (
        "Recorded in the current foreclosure case; not an independent encumbrance"
    )


def test_same_case_judgment_matches_pipeline_and_clerk_case_formats() -> None:
    service = SurvivalService("STRAP-8")
    encumbrances = [
        {
            "id": 70,
            "encumbrance_type": "mortgage",
            "creditor": "TRUIST BANK",
            "debtor": "JANE DOE",
            "recording_date": "2020-01-15",
            "instrument": "2020012345",
            "is_satisfied": False,
            "case_number": "",
        },
        {
            "id": 71,
            "encumbrance_type": "judgment",
            "creditor": "TRUIST BANK",
            "debtor": "JANE DOE",
            "recording_date": "2024-07-01",
            "instrument": "2024123456",
            "is_satisfied": False,
            "case_number": "292024CA003253A001HC",
        },
    ]
    judgment_data = {
        "plaintiff": "Truist Bank",
        "foreclosure_type": "MORTGAGE FORECLOSURE",
        "case_number": "24-CA-3253",
        "defendants": [],
    }

    result = service.analyze(encumbrances, judgment_data, [], None)

    same_case_judgment = next(enc for enc in result["results"]["historical"] if enc["id"] == 71)
    assert same_case_judgment["survival_reason"] == (
        "Recorded in the current foreclosure case; not an independent encumbrance"
    )


def test_same_case_judgment_matches_five_digit_clerk_sequence() -> None:
    service = SurvivalService("STRAP-9")
    encumbrances = [
        {
            "id": 80,
            "encumbrance_type": "lis_pendens",
            "creditor": "PALMETTO COVE COMMUNITY ASSOCIATION INC",
            "debtor": "ALEX PAJIL",
            "recording_date": "2025-05-01",
            "instrument": "2025012345",
            "is_satisfied": False,
            "case_number": "292025CC019119A001HC",
        },
        {
            "id": 81,
            "encumbrance_type": "judgment",
            "creditor": "PALMETTO COVE COMMUNITY ASSOCIATION INC",
            "debtor": "ALEX PAJIL",
            "recording_date": "2026-01-10",
            "instrument": "2026044534",
            "is_satisfied": False,
            "case_number": "292025CC019119A001HC",
        },
    ]
    judgment_data = {
        "plaintiff": "Palmetto Cove Community Association, Inc.",
        "foreclosure_type": "HOA",
        "case_number": "25-CC-19119",
        "defendants": [],
    }

    result = service.analyze(encumbrances, judgment_data, [], None)

    same_case_judgment = next(enc for enc in result["results"]["historical"] if enc["id"] == 81)
    assert same_case_judgment["survival_reason"] == (
        "Recorded in the current foreclosure case; not an independent encumbrance"
    )
