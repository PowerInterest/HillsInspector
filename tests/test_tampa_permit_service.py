from src.services.TampaPermit import TampaPermitService


def test_normalize_address_t_shorthand() -> None:
    parsed = TampaPermitService.normalize_address("618 Seascape Way, T, 33602")
    assert parsed["address_normalized"] == "618 Seascape Way, TAMPA, FL 33602"
    assert parsed["city"] == "TAMPA"
    assert parsed["state"] == "FL"
    assert parsed["zip_code"] == "33602"


def test_normalize_address_full_city_state_zip() -> None:
    parsed = TampaPermitService.normalize_address(
        "401 E JACKSON St, SUITE 1700, TAMPA, FL 33602"
    )
    assert parsed["city"] == "TAMPA"
    assert parsed["state"] == "FL"
    assert parsed["zip_code"] == "33602"


def test_violation_and_fix_flags() -> None:
    assert TampaPermitService.is_violation_record("Enforcement", "Code Case")
    assert TampaPermitService.is_fix_record("Building Plan Revision", None)
    assert not TampaPermitService.is_fix_record("Commercial Mechanical Trade Permit", None)


def test_open_status_logic() -> None:
    assert TampaPermitService.is_open_status("In Process")
    assert TampaPermitService.is_open_status("Issued")
    assert not TampaPermitService.is_open_status("Final")
    assert not TampaPermitService.is_open_status("Closed")


def test_estimate_cost_from_export_text() -> None:
    amount, source = TampaPermitService.estimate_cost_from_export(
        "HVAC Replacement - Job Value $8,500",
        None,
        "Building",
    )
    assert amount == 8500.0
    assert source == "export_text"
