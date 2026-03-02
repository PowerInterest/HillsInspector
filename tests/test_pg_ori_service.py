from __future__ import annotations

from typing import Any

from src.services import pg_ori_service


class _DummyEngine:
    pass


def _build_service(monkeypatch: Any) -> pg_ori_service.PgOriService:
    monkeypatch.setattr(
        pg_ori_service,
        "resolve_pg_dsn",
        lambda _dsn: "postgresql://user:pw@host:5432/db",
    )
    monkeypatch.setattr(
        pg_ori_service,
        "get_engine",
        lambda _dsn: _DummyEngine(),
    )
    return pg_ori_service.PgOriService()


def test_matches_property_rejects_owner_only_noc() -> None:
    tokens = {
        "legal_tokens": {"QUEENSWAY", "DRIVE"},
        "owner_names": ["MOHAMED WALID KHAFFED BEN"],
        "street_tokens": {"6412", "QUEENSWAY"},
        "case_number": "292025CA002884A001HC",
    }
    doc = {
        "DocType": "NOC",
        "Legal": "10907 THERESA ARBOR DR NOTICE OF COMMENCEMENT",
        "party1": "MOHAMED WALID; KHAFFED BEN",
        "party2": "",
        "PartiesOne": [],
        "PartiesTwo": [],
    }

    assert pg_ori_service.PgOriService.matches_property(doc, tokens) is False


def test_matches_property_rejects_noc_with_other_street_address() -> None:
    tokens = {
        "legal_tokens": {"PATIO", "TEMPLE", "CONDOMINIUM", "TERRACE"},
        "owner_names": ["LISA CHAMBERS"],
        "street_tokens": {"5264", "TENNIS", "COURT"},
        "case_number": "292024CC001609A001HC",
    }
    doc = {
        "DocType": "NOC",
        "Legal": "9221 N 56TH ST TEMPLE TERRACE FL 33617 NOTICE OF COMMENCEMENT",
        "party1": "TEMPLE TERRACE ASSOCIATES LLC",
        "party2": "",
        "PartiesOne": [],
        "PartiesTwo": [],
    }

    assert pg_ori_service.PgOriService.matches_property(doc, tokens) is False


def test_matches_property_rejects_noc_with_same_house_number_other_street() -> None:
    tokens = {
        "legal_tokens": {"TAMPA", "PALMS", "LOT", "BLOCK"},
        "owner_names": ["DANIELLE THOMAS"],
        "street_number": "6710",
        "street_name_tokens": {"YARDLEY", "WAY"},
        "street_tokens": {"6710", "YARDLEY", "WAY"},
        "case_number": "292023CA015562A001HC",
    }
    doc = {
        "DocType": "Notice of Commencement - ORI",
        "Legal": "6710 JOSIE DRIVE SEFFNER FL 33584 NOTICE OF COMMENCEMENT",
        "party1": "DILWORTH MARY",
        "party2": "QB AND ASSOCIATES OF FL INC",
        "PartiesOne": [],
        "PartiesTwo": [],
    }

    assert pg_ori_service.PgOriService.matches_property(doc, tokens) is False


def test_matches_property_rejects_noc_with_same_house_number_and_generic_suffix_only() -> None:
    tokens = {
        "legal_tokens": {"IDLE", "GROVE", "PARK", "LOT", "BLOCK"},
        "owner_names": ["JAMES H TRICE"],
        "street_number": "6003",
        "street_name_tokens": {"HIMES"},
        "street_tokens": {"6003", "HIMES", "AVE"},
        "case_number": "292023CA012693A001HC",
    }
    doc = {
        "DocType": "Notice of Commencement - ORI",
        "Legal": "6003 N MANHATTAN AVE TAMPA FL 33614 NOTICE OF COMMENCEMENT",
        "party1": "SUAREZ EMILSA",
        "party2": "TAMAYO GILBERTO",
        "PartiesOne": [],
        "PartiesTwo": [],
    }

    assert pg_ori_service.PgOriService.matches_property(doc, tokens) is False


def test_matches_property_rejects_full_text_noc_with_other_street_address() -> None:
    tokens = {
        "legal_tokens": {"PANTHER", "TRACE", "PHASE"},
        "owner_names": ["THERESA BING"],
        "street_number": "10731",
        "street_tokens": {"10731", "BANFIELD"},
        "case_number": "292016CA007158A001HC",
    }
    doc = {
        "DocType": "Notice of Commencement - ORI",
        "Legal": "PANTHER TRACE PHASE 1A LOT 29 BLOCK 1 10745 BANFIELD DR RIVERVIEW FL 33579",
        "party1": "SIMMONS JULIE",
        "party2": "SOUTH SHORE ROOFING",
        "PartiesOne": [],
        "PartiesTwo": [],
    }

    assert pg_ori_service.PgOriService.matches_property(doc, tokens) is False


def test_matches_property_rejects_noc_with_subdivision_only_without_unit_match() -> None:
    tokens = {
        "legal_tokens": {"TEMPLE", "TERRACE", "PATIO", "HOMES", "CONDOMINIUM"},
        "legal_locators": [("UNIT", "5")],
        "owner_names": ["LISA CHAMBERS"],
        "street_number": "5264",
        "street_tokens": {"5264", "TENNIS", "COURT"},
        "case_number": "292024CC001609A001HC",
    }
    doc = {
        "DocType": "Notice of Commencement - ORI",
        "Legal": "TEMPLE TERRACE PATIO HOMES CONDO ASSN INC SMART CHOICE ROOFING LLC",
        "party1": "TEMPLE TERRACE PATIO HOMES CONDO ASSN INC",
        "party2": "SMART CHOICE ROOFING LLC",
        "PartiesOne": [],
        "PartiesTwo": [],
    }

    assert pg_ori_service.PgOriService.matches_property(doc, tokens) is False


def test_matches_property_accepts_noc_with_unit_locator_match() -> None:
    tokens = {
        "legal_tokens": {"TEMPLE", "TERRACE", "PATIO", "HOMES", "CONDOMINIUM"},
        "legal_locators": [("UNIT", "5")],
        "owner_names": ["LISA CHAMBERS"],
        "street_number": "5264",
        "street_tokens": {"5264", "TENNIS", "COURT"},
        "case_number": "292024CC001609A001HC",
    }
    doc = {
        "DocType": "Notice of Commencement - ORI",
        "Legal": "TEMPLE TERRACE PATIO HOMES A CONDOMINIUM UNIT NO 5 NOTICE OF COMMENCEMENT",
        "party1": "LISA CHAMBERS",
        "party2": "",
        "PartiesOne": [],
        "PartiesTwo": [],
    }

    assert pg_ori_service.PgOriService.matches_property(doc, tokens) is True


def test_matches_property_rejects_noc_with_wrong_lot_same_block() -> None:
    tokens = {
        "legal_tokens": {"CORY", "LAKE", "ISLES", "PHASE", "UNIT"},
        "legal_locators": [("LOT", "4"), ("BLOCK", "1")],
        "owner_names": ["FRANKLIN VELEZ"],
        "street_number": "17937",
        "street_name_tokens": {"BAHAMA", "ISLE"},
        "street_tokens": {"17937", "BAHAMA", "ISLE", "CIR"},
        "case_number": "292025CC030885A001HC",
    }
    doc = {
        "DocType": "Notice of Commencement - ORI",
        "Legal": "CORY LAKE ISLES PHASE 3 UNIT 1 LOT 1 BLOCK 1 NOTICE OF COMMENCEMENT",
        "party1": "DORCHAK SHARIN",
        "party2": "FHIA LLC",
        "PartiesOne": [],
        "PartiesTwo": [],
    }

    assert pg_ori_service.PgOriService.matches_property(doc, tokens) is False


def test_matches_property_rejects_noc_when_unit_matches_but_lot_does_not() -> None:
    tokens = {
        "legal_tokens": {"CORY", "LAKE", "ISLES", "PHASE", "UNIT"},
        "legal_locators": [("UNIT", "1"), ("LOT", "4"), ("BLOCK", "1")],
        "owner_names": ["FRANKLIN VELEZ"],
        "street_number": "17937",
        "street_name_tokens": {"BAHAMA", "ISLE"},
        "street_tokens": {"17937", "BAHAMA", "ISLE", "CIR"},
        "case_number": "292025CC030885A001HC",
    }
    doc = {
        "DocType": "Notice of Commencement - ORI",
        "Legal": "CORY LAKE ISLES PHASE 3 UNIT 1 LOT 1 BLOCK 1 NOTICE OF COMMENCEMENT",
        "party1": "DORCHAK SHARIN",
        "party2": "FHIA LLC",
        "PartiesOne": [],
        "PartiesTwo": [],
    }

    assert pg_ori_service.PgOriService.matches_property(doc, tokens) is False


def test_matches_property_accepts_noc_with_legal_text() -> None:
    tokens = {
        "legal_tokens": {"PROGRESS", "VILLAGE", "UNIT", "B27"},
        "owner_names": ["DIAZ CELESTINO JIMENEZ"],
        "street_number": "4922",
        "street_tokens": {"4922", "82ND"},
        "case_number": "292025CA007991A001HC",
    }
    doc = {
        "DocType": "NOC",
        "Legal": "PROGRESS VILLAGE UNIT 2 L 13 B 27 NOTICE OF COMMENCEMENT",
        "party1": "DIAZ CELESTINO JIMENEZ",
        "party2": "",
        "PartiesOne": [],
        "PartiesTwo": [],
    }

    assert pg_ori_service.PgOriService.matches_property(doc, tokens) is True


def test_official_match_score_zero_for_noc_without_property_evidence(
    monkeypatch: Any,
) -> None:
    service = _build_service(monkeypatch)
    doc = {
        "DocType": "NOC",
        "Legal": "10907 THERESA ARBOR DR NOTICE OF COMMENCEMENT",
        "party1": "MOHAMED WALID; KHAFFED BEN",
        "party2": "",
        "PartiesOne": [],
        "PartiesTwo": [],
    }
    property_tokens = {
        "legal_tokens": {"QUEENSWAY", "DRIVE"},
        "owner_names": ["MOHAMED WALID KHAFFED BEN"],
        "street_number": "6412",
        "street_tokens": {"6412", "QUEENSWAY"},
        "case_number": "292025CA002884A001HC",
    }

    score = service._official_match_score(  # noqa: SLF001
        doc=doc,
        case_variants_upper=[],
        legal_terms_upper=[],
        party_tokens_upper=["MOHAMED", "KHAFFED"],
        property_tokens=property_tokens,
    )

    assert score == 0


def test_matches_property_rejects_noc_with_same_street_name_wrong_house_number() -> None:
    tokens = {
        "legal_tokens": {"TEMPLE", "TERRACE", "PATIO", "HOMES", "CONDOMINIUM"},
        "legal_locators": [("UNIT", "5")],
        "owner_names": ["LISA CHAMBERS"],
        "street_number": "5264",
        "street_tokens": {"5264", "TENNIS", "COURT", "CIR"},
        "case_number": "292024CC001609A001HC",
    }
    doc = {
        "DocType": "Notice of Commencement - ORI",
        "Legal": "TEMPLE TERRACE PATIO HOMES A CONDOMINIUM UNIT NO 9 5256 TENNIS COURT CIR TAMPA FL 33617",
        "party1": "ACHAT CATHERINE",
        "party2": "W F SEXTON INC",
        "PartiesOne": [],
        "PartiesTwo": [],
    }

    assert pg_ori_service.PgOriService.matches_property(doc, tokens) is False


def test_discover_property_keeps_noc_docs(monkeypatch: Any) -> None:
    service = _build_service(monkeypatch)
    noc_doc = {
        "Instrument": "2026065622",
        "DocType": "NOC",
        "RecordDate": "2026-02-20",
        "BookType": "OR",
        "Book": "12345",
        "Page": "678",
        "Legal": "PROGRESS VILLAGE UNIT 2 L 13 B 27 NOTICE OF COMMENCEMENT",
        "party1": "DIAZ CELESTINO JIMENEZ",
        "party2": "",
        "PartiesOne": [],
        "PartiesTwo": [],
    }

    monkeypatch.setattr(service, "_get_ownership_chain", lambda _strap: [])
    monkeypatch.setattr(
        service,
        "_seed_from_official_records",
        lambda **_kwargs: [noc_doc],
    )
    monkeypatch.setattr(service, "_case_variants", lambda _case: [])
    monkeypatch.setattr(service, "_search_case_pav", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        service,
        "_search_instrument_pav",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(service, "_search_legal_pav", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(service, "_search_book_page_pav", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(service, "_search_party_pav", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(service, "_get_clerk_case_seeds", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(service, "_build_search_terms", lambda _target: [])
    monkeypatch.setattr(service, "_extract_primary_legal_line", lambda _target: "")

    documents, stats = service._discover_property({  # noqa: SLF001
        "case_number": "292025CA007991A001HC",
        "strap": "1930011QP000022000110U",
        "judgment_data": {},
        "filing_date": None,
        "owner_name": "DIAZ CELESTINO JIMENEZ",
        "property_address": "4922 S 82ND ST",
        "legal1": "PROGRESS VILLAGE UNIT 2 L 13 B 27",
        "legal2": "",
        "legal3": "",
        "legal4": "",
    })

    assert [doc["Instrument"] for doc in documents] == ["2026065622"]
    assert stats["official_seed_docs"] == 1


def test_discover_property_runs_live_noc_fallback_for_recent_permit_signal(
    monkeypatch: Any,
) -> None:
    service = _build_service(monkeypatch)
    noc_doc = {
        "Instrument": "2024339003",
        "DocType": "NOC",
        "RecordDate": "2024-11-20",
        "BookType": "OR",
        "Book": "12345",
        "Page": "678",
        "Legal": "10731 BANFIELD DR RIVERVIEW FL 33579 NOTICE OF COMMENCEMENT",
        "party1": "THERESA BING",
        "party2": "",
        "PartiesOne": [],
        "PartiesTwo": [],
    }

    monkeypatch.setattr(service, "_get_ownership_chain", lambda _strap: [])
    monkeypatch.setattr(service, "_seed_from_official_records", lambda **_kwargs: [])
    monkeypatch.setattr(service, "_case_variants", lambda _case: [])
    monkeypatch.setattr(service, "_search_case_pav", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(service, "_search_instrument_pav", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(service, "_search_legal_pav", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(service, "_search_book_page_pav", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(service, "_search_party_pav", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(service, "_get_clerk_case_seeds", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(service, "_build_search_terms", lambda _target: ["BANFIELD PARK"])
    monkeypatch.setattr(service, "_extract_primary_legal_line", lambda _target: "")
    monkeypatch.setattr(service, "_target_has_recent_permit_signal", lambda _target: True)
    monkeypatch.setattr(
        service,
        "_official_noc_coverage_start",
        lambda: pg_ori_service.date(2021, 11, 4),
    )
    monkeypatch.setattr(
        service,
        "_search_noc_legal_pav",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        service,
        "_search_noc_party_pav",
        lambda *_args, **_kwargs: [noc_doc],
    )
    monkeypatch.setattr(
        service,
        "_search_noc_full_text_pav",
        lambda *_args, **_kwargs: [],
    )

    documents, stats = service._discover_property(  # noqa: SLF001
        {
            "case_number": "292016CA007158A001HC",
            "strap": "20310561L000001000360U",
            "folio": "0774524272",
            "judgment_data": {},
            "filing_date": None,
            "owner_name": "THERESA BING",
            "property_address": "10731 BANFIELD DR",
            "legal1": "",
            "legal2": "",
            "legal3": "",
            "legal4": "",
        }
    )

    assert [doc["Instrument"] for doc in documents] == ["2024339003"]
    assert stats["live_noc_docs"] == 1


def test_run_live_noc_fallback_uses_party_then_full_text(monkeypatch: Any) -> None:
    service = _build_service(monkeypatch)
    calls: list[str] = []
    noc_doc = {
        "Instrument": "2024339003",
        "DocType": "NOC",
        "RecordDate": "2024-11-20",
        "BookType": "OR",
        "Book": "12345",
        "Page": "678",
        "Legal": "10731 BANFIELD DR RIVERVIEW FL 33579 NOTICE OF COMMENCEMENT",
        "party1": "THERESA BING",
        "party2": "",
        "PartiesOne": [],
        "PartiesTwo": [],
    }

    monkeypatch.setattr(service, "_target_has_recent_permit_signal", lambda _target: True)
    monkeypatch.setattr(service, "_build_search_terms", lambda _target: ["BANFIELD PARK"])
    monkeypatch.setattr(service, "_extract_primary_legal_line", lambda _target: "")
    monkeypatch.setattr(
        service,
        "_official_noc_coverage_start",
        lambda: pg_ori_service.date(2021, 11, 4),
    )

    def fake_noc_legal(term: str, *_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        calls.append(f"legal:{term}")
        return []

    def fake_noc_party(name: str, *_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        calls.append(f"party:{name}")
        return []

    def fake_noc_full_text(term: str, *_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        calls.append(f"full_text:{term}")
        return [noc_doc]

    monkeypatch.setattr(service, "_search_noc_legal_pav", fake_noc_legal)
    monkeypatch.setattr(service, "_search_noc_party_pav", fake_noc_party)
    monkeypatch.setattr(service, "_search_noc_full_text_pav", fake_noc_full_text)

    docs = service._run_live_noc_fallback(  # noqa: SLF001
        target={
            "case_number": "292016CA007158A001HC",
            "strap": "20310561L000001000360U",
            "judgment_data": {},
            "owner_name": "THERESA BING",
            "property_address": "10731 BANFIELD DR",
            "legal1": "",
            "legal2": "",
            "legal3": "",
            "legal4": "",
        },
        ownership_chain=[],
        property_tokens={
            "legal_tokens": set(),
            "owner_names": ["THERESA BING"],
            "street_number": "10731",
            "street_tokens": {"10731", "BANFIELD"},
            "case_number": "292016CA007158A001HC",
        },
        earliest_date=pg_ori_service.date(2021, 11, 4),
        latest_date=pg_ori_service.date(2026, 3, 1),
        stats={"api_calls": 0, "retries": 0, "truncated": 0, "unresolved_truncations": 0},
    )

    assert [doc["Instrument"] for doc in docs] == ["2024339003"]
    assert calls == [
        "legal:BANFIELD PARK",
        "party:THERESA BING",
        "full_text:10731 BANFIELD DR",
    ]


def test_run_live_noc_fallback_skips_without_recent_permit_signal(
    monkeypatch: Any,
) -> None:
    service = _build_service(monkeypatch)

    monkeypatch.setattr(service, "_target_has_recent_permit_signal", lambda _target: False)
    monkeypatch.setattr(
        service,
        "_search_noc_legal_pav",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not search")),
    )

    docs = service._run_live_noc_fallback(  # noqa: SLF001
        target={
            "case_number": "292016CA007158A001HC",
            "strap": "20310561L000001000360U",
            "judgment_data": {},
            "owner_name": "THERESA BING",
            "property_address": "10731 BANFIELD DR",
            "legal1": "",
            "legal2": "",
            "legal3": "",
            "legal4": "",
        },
        ownership_chain=[],
        property_tokens={
            "legal_tokens": set(),
            "owner_names": ["THERESA BING"],
            "street_tokens": {"10731", "BANFIELD"},
            "case_number": "292016CA007158A001HC",
        },
        earliest_date=pg_ori_service.date(2021, 11, 4),
        latest_date=pg_ori_service.date(2026, 3, 1),
        stats={"api_calls": 0, "retries": 0, "truncated": 0, "unresolved_truncations": 0},
    )

    assert docs == []


def test_parse_pav_full_text_rows_extracts_noc_fields(monkeypatch: Any) -> None:
    service = _build_service(monkeypatch)

    docs = service._parse_pav_full_text_rows(  # noqa: SLF001
        [
            {
                "ID": "abc123",
                "Name": (
                    "Notice of Commencement - ORI Record Date -   8/12/2024 12:55:25 PM  "
                    "Name -  BING THERESA - CP DANNER CONSTRUCTION INC,  "
                    "Inst. #:  2024339003   Case # -  Recpt # -  6674151 "
                ),
                "Summary": (
                    "COMMENCEMENT I LEGAL DESCRIPTION OF PROPERTY STREET ADDRESS REQUIRED "
                    "10731 BANFIELD DR RIVERVIEW FL 33579"
                ),
            }
        ]
    )

    assert docs == [
        {
            "Instrument": "2024339003",
            "DocType": "Notice of Commencement - ORI",
            "RecordDate": "8/12/2024 12:55:25 PM",
            "BookType": "OR",
            "Book": "",
            "Page": "",
            "Legal": (
                "COMMENCEMENT I LEGAL DESCRIPTION OF PROPERTY STREET ADDRESS REQUIRED "
                "10731 BANFIELD DR RIVERVIEW FL 33579"
            ),
            "PartiesOne": ["BING THERESA"],
            "PartiesTwo": ["CP DANNER CONSTRUCTION INC"],
            "ID": "abc123",
        }
    ]


def test_run_recent_permit_noc_backfill_summarizes_results(monkeypatch: Any) -> None:
    service = _build_service(monkeypatch)
    target = {
        "foreclosure_id": 15332,
        "case_number": "292016CA007158A001HC",
        "strap": "20310561L000001000360U",
        "folio": "0774524272",
        "judgment_data": {},
        "auction_date": None,
        "filing_date": None,
        "legal1": "",
        "legal2": "",
        "legal3": "",
        "legal4": "",
        "owner_name": "THERESA BING",
        "property_address": "10731 BANFIELD DR",
    }
    noc_doc = {
        "Instrument": "2024339003",
        "DocType": "Notice of Commencement - ORI",
        "RecordDate": "8/12/2024 12:55:25 PM",
        "BookType": "OR",
        "Book": "",
        "Page": "",
        "Legal": "10731 BANFIELD DR RIVERVIEW FL 33579",
        "PartiesOne": ["BING THERESA"],
        "PartiesTwo": ["CP DANNER CONSTRUCTION INC"],
    }

    states = iter([[target], []])
    monkeypatch.setattr(
        service,
        "_find_recent_permit_no_noc_targets",
        lambda **_kwargs: next(states),
    )
    monkeypatch.setattr(service, "_get_ownership_chain", lambda _strap: [])
    monkeypatch.setattr(service, "_build_property_tokens", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        service,
        "_earliest_relevant_date",
        lambda *_args, **_kwargs: pg_ori_service.date(2021, 11, 4),
    )
    monkeypatch.setattr(
        service,
        "_run_live_noc_fallback",
        lambda **_kwargs: [noc_doc],
    )
    monkeypatch.setattr(service, "_save_documents", lambda *_args, **_kwargs: 1)

    result = service.run_recent_permit_noc_backfill()

    assert result["targets"] == 1
    assert result["targets_with_live_noc"] == 1
    assert result["total_noc_docs_found"] == 1
    assert result["total_saved"] == 1
    assert result["remaining_recent_permit_no_noc_before"] == 1
    assert result["remaining_recent_permit_no_noc_after"] == 0
    assert result["per_target"][0]["instruments"] == ["2024339003"]
