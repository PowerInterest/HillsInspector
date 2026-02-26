from __future__ import annotations

from sunbiz.pg_loader import HILLSBOROUGH_MILLAGE_2025
from sunbiz.pg_loader import _parse_nal_csv_row


def _base_row() -> dict[str, str]:
    return {
        "co_no": "39",
        "parcel_id": "R123456789000000000",
        "tax_auth_cd": "TA",
        "tot_mill": "",
        "co_mill": "",
        "schl_mill": "",
        "muni_mill": "",
    }


def test_nal_row_uses_2025_fallback_rates_for_2025_only() -> None:
    row = _base_row()
    parsed = _parse_nal_csv_row(
        row=row,
        file_id=1,
        tax_year=2025,
        source_file="nal_2025.csv",
        folio_lookup={},
    )
    assert parsed is not None
    assert parsed["total_millage"] == HILLSBOROUGH_MILLAGE_2025["TA"]["total_millage"]
    assert parsed["county_millage"] == HILLSBOROUGH_MILLAGE_2025["TA"]["county_millage"]


def test_nal_row_does_not_apply_2025_fallback_to_non_2025_years() -> None:
    row = _base_row()
    parsed = _parse_nal_csv_row(
        row=row,
        file_id=1,
        tax_year=2026,
        source_file="nal_2026.csv",
        folio_lookup={},
    )
    assert parsed is not None
    assert parsed["total_millage"] is None
    assert parsed["county_millage"] is None
    assert parsed["school_millage"] is None
    assert parsed["city_millage"] is None
