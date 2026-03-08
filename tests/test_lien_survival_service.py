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
