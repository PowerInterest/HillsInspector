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
    assert TampaPermitService.is_business_tax_record(
        "BTX-R-26-1012368",
        "Business",
        "Tax Receipt Non-Merchant Renewal",
    )
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


def test_business_tax_rows_do_not_need_closeout() -> None:
    service = TampaPermitService.__new__(TampaPermitService)
    parsed = service.normalize_csv_row(
        {
            "Record Number": "BTX-R-26-1012368",
            "Record Type": "Tax Receipt Non-Merchant Renewal",
            "Module": "Business",
            "Status": "Paid",
            "Address": "3023 W GREEN ST, T, 33609",
        }
    )

    assert parsed is not None
    assert parsed["is_violation"] is False
    assert parsed["is_open"] is True
    assert parsed["needs_closeout"] is False


def test_extract_detail_fields_reads_job_value_from_adjacent_field() -> None:
    parsed = TampaPermitService._extract_detail_fields(  # noqa: SLF001
        """
        <div>
          <span>Record Status:</span><span>Awaiting Client Reply</span>
          <span>Expiration Date:</span><span>03/31/2026</span>
          <div><span>Job Value:</span></div>
          <div><span>109000</span></div>
        </div>
        """
    )

    assert parsed["status"] == "Awaiting Client Reply"
    assert str(parsed["expiration_date"]) == "2026-03-31"
    assert parsed["estimated_work_cost"] == 109000.0


def test_extract_detail_fields_reads_total_project_value_label() -> None:
    parsed = TampaPermitService._extract_detail_fields(  # noqa: SLF001
        """
        <div>
          <span>Record Status:</span><span>Complete</span>
          <div><span>Total Project Value:</span></div>
          <div><span>$45,500</span></div>
        </div>
        """
    )

    assert parsed["status"] == "Complete"
    assert parsed["estimated_work_cost"] == 45500.0


def test_extract_postback_target_from_search_results_link() -> None:
    target = TampaPermitService._extract_postback_target(  # noqa: SLF001
        """
        <table>
          <tr>
            <td>
              <a href="javascript:__doPostBack('ctl00$PlaceHolderMain$CapView$gdvPermitList$ctl03$lnkPermitNumber','')">
                UTL-26-0000588
              </a>
            </td>
          </tr>
        </table>
        """,
        "UTL-26-0000588",
    )

    assert target == "ctl00$PlaceHolderMain$CapView$gdvPermitList$ctl03$lnkPermitNumber"


def test_extract_async_redirect_url_reads_cap_detail_redirect() -> None:
    redirect_url = TampaPermitService._extract_async_redirect_url(  # noqa: SLF001
        (
            "1|#||4|144|pageRedirect||"
            "%2fTAMPA%2fCap%2fCapDetail.aspx%3fModule%3dBuilding%26TabName%3dBuilding"
            "%26capID1%3d26CAP%26capID2%3d00000%26capID3%3d003RL%26agencyCode%3dTAMPA|"
        ),
        "https://aca-prod.accela.com/TAMPA/Cap/GlobalSearchResults.aspx?QueryText=UTL-26-0000588",
    )

    assert redirect_url == (
        "https://aca-prod.accela.com/TAMPA/Cap/CapDetail.aspx"
        "?Module=Building&TabName=Building&capID1=26CAP&capID2=00000"
        "&capID3=003RL&agencyCode=TAMPA"
    )
