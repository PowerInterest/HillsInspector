from __future__ import annotations

import json
from datetime import date
from typing import Any
from typing import Self

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


class _CaptureResult:
    def __init__(
        self,
        rowcount: int = 0,
        rows: list[tuple[Any, ...]] | None = None,
        mapping_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.rowcount = rowcount
        self._rows = rows or []
        self._mapping_rows = mapping_rows or []

    def fetchall(self) -> list[Any]:
        if self._mapping_rows:
            return self._mapping_rows
        return self._rows

    def fetchone(self) -> Any:
        if self._mapping_rows:
            return self._mapping_rows[0] if self._mapping_rows else None
        return self._rows[0] if self._rows else None

    def first(self) -> dict[str, Any] | None:
        if self._mapping_rows:
            return self._mapping_rows[0]
        return None

    def mappings(self) -> Self:
        return self

    def all(self) -> list[Any]:
        if self._mapping_rows:
            return self._mapping_rows
        return self._rows


class _CaptureConnection:
    def __init__(self, captured: dict[str, Any], rows: list[tuple[Any, ...]] | None = None) -> None:
        self._captured = captured
        self._rows = rows or []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: Any, params: dict[str, Any] | None = None) -> _CaptureResult:
        self._captured["sql"] = str(sql)
        self._captured["params"] = params or {}
        return _CaptureResult(rows=self._rows)


class _CaptureEngine:
    def __init__(self, captured: dict[str, Any], rows: list[tuple[Any, ...]] | None = None) -> None:
        self._captured = captured
        self._rows = rows or []

    def connect(self) -> _CaptureConnection:
        return _CaptureConnection(self._captured, self._rows)


class _ExecuteFnConnection:
    def __init__(self, execute_fn: Any, captured: list[tuple[str, dict[str, Any]]]) -> None:
        self._execute_fn = execute_fn
        self._captured = captured

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: Any, params: dict[str, Any] | None = None) -> _CaptureResult:
        sql_text = str(sql)
        payload = params or {}
        self._captured.append((sql_text, payload))
        return self._execute_fn(sql_text, payload)


class _ExecuteFnEngine:
    def __init__(self, execute_fn: Any, captured: list[tuple[str, dict[str, Any]]]) -> None:
        self._execute_fn = execute_fn
        self._captured = captured

    def begin(self) -> _ExecuteFnConnection:
        return _ExecuteFnConnection(self._execute_fn, self._captured)

    def connect(self) -> _ExecuteFnConnection:
        return _ExecuteFnConnection(self._execute_fn, self._captured)


def _passthrough_prepare_target_identity(
    target: dict[str, Any],
    *,
    persist_update: bool,
) -> tuple[dict[str, Any], None]:
    assert isinstance(persist_update, bool)
    return dict(target), None


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


def test_is_generic_name_flags_navy_federal_credit_union(monkeypatch: Any) -> None:
    monkeypatch.setattr(pg_ori_service, "_GENERIC_NAMES", None)

    assert pg_ori_service._is_generic_name("NAVY FEDERAL CREDIT UNION") is True  # noqa: SLF001


def test_matches_property_rejects_owner_only_foreclosure_judgment_with_other_case() -> None:
    tokens = {
        "legal_tokens": {"LAKEWOOD", "ESTATES", "UNIT"},
        "owner_names": ["SECRETARY OF HOUSING AND URBAN DEVELOPMENT"],
        "street_tokens": {"2902", "147TH"},
        "case_number": "292025CA008465A001HC",
    }
    doc = {
        "DocType": "(JUD) JUDGMENT",
        "Legal": "JUDGMENT",
        "CaseNum": "292022CA010278A001HC",
        "party1": "MORTGAGE ASSETS MANAGEMENT LLC",
        "party2": "UNITED STATES OF AMERICA, ACTING ON BEHALF OF THE SECRETARY OF HOUSING AND URBAN DEVELOPMENT",
        "PartiesOne": [],
        "PartiesTwo": [],
    }

    assert pg_ori_service.PgOriService.matches_property(doc, tokens) is False


def test_matches_property_rejects_owner_only_lis_pendens_without_case_or_property_text() -> None:
    tokens = {
        "legal_tokens": {"LAKEWOOD", "ESTATES", "UNIT"},
        "owner_names": ["SECRETARY OF HOUSING AND URBAN DEVELOPMENT"],
        "street_tokens": {"2902", "147TH"},
        "case_number": "292025CA008465A001HC",
    }
    doc = {
        "DocType": "(LIS) LIS PENDENS",
        "Legal": "FORECLOSURE ACTION",
        "party1": "TRUSTEE NAME",
        "party2": "UNITED STATES OF AMERICA, ACTING ON BEHALF OF THE SECRETARY OF HOUSING AND URBAN DEVELOPMENT",
        "PartiesOne": [],
        "PartiesTwo": [],
    }

    assert pg_ori_service.PgOriService.matches_property(doc, tokens) is False


def test_matches_property_rejects_owner_only_mortgage_without_property_text() -> None:
    tokens = {
        "legal_tokens": {"LAKEWOOD", "ESTATES", "UNIT"},
        "owner_names": ["SECRETARY OF HOUSING AND URBAN DEVELOPMENT"],
        "street_tokens": {"2902", "147TH"},
        "case_number": "292025CA008465A001HC",
    }
    doc = {
        "DocType": "(MTG) MORTGAGE",
        "Legal": "MORTGAGE",
        "party1": "OTHER BORROWER",
        "party2": "SECRETARY OF HOUSING AND URBAN DEVELOPMENT",
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


def test_matches_property_or_reference_accepts_reference_only_lifecycle_doc(
    monkeypatch: Any,
) -> None:
    service = _build_service(monkeypatch)

    doc = {
        "DocType": "(MOD) MODIFICATION",
        "Legal": "THIS MODIFICATION RELATES TO CLK #2024000123",
        "party1": "",
        "party2": "",
        "PartiesOne": [],
        "PartiesTwo": [],
    }

    assert service._matches_property_or_reference(  # noqa: SLF001
        doc,
        property_tokens={
            "legal_tokens": set(),
            "owner_names": [],
            "street_tokens": set(),
            "case_number": "292025CA000123A001HC",
        },
        anchor_instruments={"2024000123"},
        anchor_book_pages=set(),
    ) is True


def test_discover_property_keeps_adjacent_reference_only_lifecycle_doc(
    monkeypatch: Any,
) -> None:
    service = _build_service(monkeypatch)
    mortgage_doc = {
        "Instrument": "2024000123",
        "DocType": "(MTG) MORTGAGE",
        "RecordDate": "2024-01-10",
        "BookType": "OR",
        "Book": "12345",
        "Page": "678",
        "Legal": "123 MAIN ST TAMPA FL",
        "party1": "BANK",
        "party2": "OWNER",
        "PartiesOne": [],
        "PartiesTwo": [],
    }
    mod_doc = {
        "Instrument": "2024000124",
        "DocType": "(MOD) MODIFICATION",
        "RecordDate": "2024-02-10",
        "BookType": "OR",
        "Book": "12345",
        "Page": "679",
        "Legal": "MODIFICATION OF CLK #2024000123",
        "party1": "",
        "party2": "",
        "PartiesOne": [],
        "PartiesTwo": [],
    }

    monkeypatch.setattr(service, "_get_ownership_chain", lambda _strap: [])
    monkeypatch.setattr(service, "_seed_from_official_records", lambda **_kwargs: [mortgage_doc])
    monkeypatch.setattr(service, "_case_variants", lambda _case: [])
    monkeypatch.setattr(service, "_search_case_pav", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        service,
        "_search_instrument_pav",
        lambda instrument, *_args, **_kwargs: [mod_doc] if str(instrument) == "2024000124" else [],
    )
    monkeypatch.setattr(service, "_search_legal_pav", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(service, "_search_book_page_pav", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(service, "_search_party_pav", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(service, "_get_clerk_case_seeds", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(service, "_build_search_terms", lambda _target: [])
    monkeypatch.setattr(service, "_extract_primary_legal_line", lambda _target: "")

    documents, _stats = service._discover_property(  # noqa: SLF001
        {
            "case_number": "292025CA000123A001HC",
            "strap": "123",
            "judgment_data": {},
            "filing_date": None,
            "owner_name": "OWNER",
            "property_address": "123 MAIN ST",
            "legal1": "",
            "legal2": "",
            "legal3": "",
            "legal4": "",
            "skip_live_noc_fallback": True,
        }
    )

    assert [doc["Instrument"] for doc in documents] == ["2024000123", "2024000124"]


def test_save_documents_does_not_reset_is_satisfied_for_non_satisfaction_docs(
    monkeypatch: Any,
) -> None:
    service = _build_service(monkeypatch)
    captured: list[tuple[str, dict[str, Any]]] = []

    def _execute(sql_text: str, _params: dict[str, Any]) -> _CaptureResult:
        if "UPDATE ori_encumbrances" in sql_text and "WHERE instrument_number = :instrument" in sql_text:
            return _CaptureResult(rowcount=1)
        return _CaptureResult()

    service.engine = _ExecuteFnEngine(_execute, captured)  # type: ignore[assignment]

    saved = service._save_documents(  # noqa: SLF001
        "strap",
        "folio",
        [
            {
                "Instrument": "2024000123",
                "DocType": "(MTG) MORTGAGE",
                "RecordDate": "2024-01-10",
                "BookType": "OR",
                "Book": "12345",
                "Page": "678",
                "Legal": "123 MAIN ST TAMPA FL",
            }
        ],
    )

    assert saved == 1
    update_calls = [
        params
        for sql_text, params in captured
        if "UPDATE ori_encumbrances" in sql_text and "WHERE instrument_number = :instrument" in sql_text
    ]
    assert update_calls[0]["is_sat_insert"] is False
    assert update_calls[0]["is_sat_update"] is None


def test_save_documents_matches_existing_instrument_by_strap_when_folio_changes(
    monkeypatch: Any,
) -> None:
    service = _build_service(monkeypatch)
    captured: list[tuple[str, dict[str, Any]]] = []

    def _execute(sql_text: str, _params: dict[str, Any]) -> _CaptureResult:
        if "UPDATE ori_encumbrances" in sql_text and "WHERE instrument_number = :instrument" in sql_text:
            return _CaptureResult(rowcount=1)
        return _CaptureResult()

    service.engine = _ExecuteFnEngine(_execute, captured)  # type: ignore[assignment]

    saved = service._save_documents(  # noqa: SLF001
        "strap-123",
        "folio-456",
        [
            {
                "Instrument": "2024000456",
                "DocType": "(MTG) MORTGAGE",
                "RecordDate": "2024-01-10",
                "BookType": "OR",
                "Book": "12345",
                "Page": "678",
                "Legal": "123 MAIN ST TAMPA FL",
            }
        ],
    )

    assert saved == 1
    update_sql = next(
        sql_text
        for sql_text, _params in captured
        if "UPDATE ori_encumbrances" in sql_text and "WHERE instrument_number = :instrument" in sql_text
    )
    assert "folio IS NOT DISTINCT FROM :folio" in update_sql
    assert "OR strap = :strap" in update_sql


def test_save_documents_casts_change_detection_params_for_pg_type_inference(
    monkeypatch: Any,
) -> None:
    service = _build_service(monkeypatch)
    captured: list[tuple[str, dict[str, Any]]] = []

    def _execute(sql_text: str, _params: dict[str, Any]) -> _CaptureResult:
        if "UPDATE ori_encumbrances" in sql_text and "WHERE instrument_number = :instrument" in sql_text:
            return _CaptureResult(rowcount=1)
        return _CaptureResult()

    service.engine = _ExecuteFnEngine(_execute, captured)  # type: ignore[assignment]

    saved = service._save_documents(  # noqa: SLF001
        "strap-123",
        "folio-456",
        [
            {
                "Instrument": "2024000456",
                "DocType": "(JUD) JUDGMENT",
                "RecordDate": "2024-01-10",
                "BookType": "OR",
                "Book": "12345",
                "Page": "678",
                "Legal": "123 MAIN ST TAMPA FL",
                "PartiesOne": ["BANK"],
                "PartiesTwo": ["OWNER"],
            }
        ],
    )

    assert saved == 1
    update_sql = next(
        sql_text
        for sql_text, _params in captured
        if "UPDATE ori_encumbrances" in sql_text and "WHERE instrument_number = :instrument" in sql_text
    )
    assert "CAST(:book AS TEXT) IS NOT NULL" in update_sql
    assert "CAST(:enc_type AS TEXT) IS NOT NULL" in update_sql
    assert "CAST(:p1_json AS TEXT) IS NOT NULL" in update_sql
    assert "CAST(:amount AS NUMERIC) IS NOT NULL" in update_sql
    assert "CAST(:is_sat_update AS BOOLEAN) IS TRUE" in update_sql


def test_save_documents_does_not_count_noop_upserts_as_saved(
    monkeypatch: Any,
) -> None:
    service = _build_service(monkeypatch)
    captured: list[tuple[str, dict[str, Any]]] = []

    def _execute(sql_text: str, _params: dict[str, Any]) -> _CaptureResult:
        if "UPDATE ori_encumbrances" in sql_text and "WHERE instrument_number = :instrument" in sql_text:
            return _CaptureResult(rowcount=0)
        if "INSERT INTO ori_encumbrances" in sql_text:
            return _CaptureResult(rowcount=0)
        return _CaptureResult()

    service.engine = _ExecuteFnEngine(_execute, captured)  # type: ignore[assignment]

    saved = service._save_documents(  # noqa: SLF001
        "strap-123",
        "folio-456",
        [
            {
                "Instrument": "2024000456",
                "DocType": "(MTG) MORTGAGE",
                "RecordDate": "2024-01-10",
                "BookType": "OR",
                "Book": "12345",
                "Page": "678",
                "Legal": "123 MAIN ST TAMPA FL",
            }
        ],
    )

    assert saved == 0


def test_link_satisfactions_updates_parent_without_self_reference(
    monkeypatch: Any,
) -> None:
    service = _build_service(monkeypatch)
    captured: list[tuple[str, dict[str, Any]]] = []

    def _execute(sql_text: str, _params: dict[str, Any]) -> _CaptureResult:
        if "FROM information_schema.columns" in sql_text:
            return _CaptureResult(
                rows=[
                    ("satisfies_encumbrance_id",),
                    ("satisfaction_method",),
                    ("satisfaction_date",),
                    ("satisfaction_instrument",),
                ]
            )
        if "encumbrance_type IN ('satisfaction', 'release')" in sql_text:
            # Shape: id, instrument_number, legal_description, party1,
            #        party2, recording_date, case_number
            return _CaptureResult(
                rows=[
                    (
                        11,
                        "2024000999",
                        "SATISFACTION OF CLK #2024000123",
                        "",
                        "",
                        date(2025, 1, 5),
                        "25-CA-000123",
                    )
                ]
            )
        if "encumbrance_type IN ('mortgage', 'lien', 'judgment')" in sql_text:
            # Shape: id, instrument_number, book, page, case_number,
            #        party1, party2, amount, recording_date
            return _CaptureResult(
                rows=[
                    (
                        22,
                        "2024000123",
                        "12345",
                        "678",
                        "25-CA-000123",
                        "",
                        "",
                        100000.0,
                        date(2024, 1, 10),
                    )
                ]
            )
        return _CaptureResult(rowcount=1)

    service.engine = _ExecuteFnEngine(_execute, captured)  # type: ignore[assignment]

    linked = service._link_satisfactions("strap")  # noqa: SLF001

    assert linked == 1
    sat_select_sql = next(
        sql_text
        for sql_text, _params in captured
        if "encumbrance_type IN ('satisfaction', 'release')" in sql_text
    )
    assert "satisfies_encumbrance_id IS NULL" not in sat_select_sql
    parent_update_sql = next(
        sql_text
        for sql_text, _params in captured
        if "WHERE id = :enc_id" in sql_text
    )
    assert "satisfies_encumbrance_id" not in parent_update_sql
    sat_update_params = next(
        params
        for sql_text, params in captured
        if "WHERE id = :sat_id" in sql_text
    )
    assert sat_update_params["enc_id"] == 22


def test_link_satisfactions_skips_when_link_columns_missing(
    monkeypatch: Any,
) -> None:
    service = _build_service(monkeypatch)
    captured: list[tuple[str, dict[str, Any]]] = []

    def _execute(sql_text: str, _params: dict[str, Any]) -> _CaptureResult:
        if "FROM information_schema.columns" in sql_text:
            return _CaptureResult(rows=[("satisfaction_date",), ("satisfaction_instrument",)])
        raise AssertionError(f"unexpected SQL after column check: {sql_text}")

    service.engine = _ExecuteFnEngine(_execute, captured)  # type: ignore[assignment]

    linked = service._link_satisfactions("strap")  # noqa: SLF001

    assert linked == 0


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


def test_find_targets_merges_standard_and_lp_gap_targets(monkeypatch: Any) -> None:
    service = _build_service(monkeypatch)
    standard = [
        {"foreclosure_id": 1, "case_number": "292025CA000001A001HC"},
        {"foreclosure_id": 2, "case_number": "292025CA000002A001HC"},
    ]
    lp_gap = [
        {"foreclosure_id": 2, "case_number": "292025CA000002A001HC", "lp_recovery_mode": True},
        {"foreclosure_id": 3, "case_number": "292025CA000003A001HC", "lp_recovery_mode": True},
    ]

    def _fake_standard_targets(limit: int | None = None) -> list[dict[str, Any]]:
        assert limit is None
        return standard

    monkeypatch.setattr(service, "_find_standard_targets", _fake_standard_targets)
    monkeypatch.setattr(
        service,
        "_find_lis_pendens_gap_targets",
        lambda **_kwargs: lp_gap,
    )

    targets = service._find_targets(limit=None)  # noqa: SLF001

    assert [target["foreclosure_id"] for target in targets] == [1, 2, 3]
    assert targets[2]["lp_recovery_mode"] is True


def test_find_lis_pendens_gap_targets_sql_does_not_require_strap(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}
    sample_row = (
        21007,
        "292024CA003727A001HC",
        None,
        None,
        {},
        None,
        None,
        "",
        "",
        "",
        "",
        "",
        "",
    )

    monkeypatch.setattr(
        pg_ori_service,
        "resolve_pg_dsn",
        lambda _dsn: "postgresql://user:pw@host:5432/db",
    )
    monkeypatch.setattr(
        pg_ori_service,
        "get_engine",
        lambda _dsn: _CaptureEngine(captured, rows=[sample_row]),
    )

    service = pg_ori_service.PgOriService()
    targets = service._find_lis_pendens_gap_targets(  # noqa: SLF001
        limit=25,
        require_ori_searched=None,
    )

    sql_text = captured["sql"].lower()
    where_sql = sql_text.split("where", 1)[1].split("order by", 1)[0]
    assert "and f.strap is not null" not in where_sql
    assert "oe.case_number = f.case_number_raw" in sql_text
    assert "oe.case_number = f.case_number_norm" in sql_text
    assert targets[0]["foreclosure_id"] == 21007
    assert targets[0]["lp_recovery_mode"] is True
    assert targets[0]["mark_ori_searched"] is False


def test_post_pav_bypass_cache_skips_cache_io(monkeypatch: Any) -> None:
    service = _build_service(monkeypatch)
    calls = {"get": 0, "put": 0}

    class _FakeResponse:
        status_code = 200

        @staticmethod
        def json() -> dict[str, Any]:
            return {"Data": []}

    class _FakeSession:
        @staticmethod
        def post(*_args: Any, **_kwargs: Any) -> _FakeResponse:
            return _FakeResponse()

    monkeypatch.setattr(pg_ori_service, "pav_cache_get", lambda _payload: calls.__setitem__("get", calls["get"] + 1))
    monkeypatch.setattr(
        pg_ori_service,
        "pav_cache_put",
        lambda _payload, _data: calls.__setitem__("put", calls["put"] + 1),
    )
    service._pav_session = _FakeSession()  # noqa: SLF001

    stats = {"api_calls": 0, "retries": 0}
    result = service._post_pav(  # noqa: SLF001
        {"QueryID": 350, "Keywords": [], "QueryLimit": 100},
        "case:292024CA003727A001HC",
        stats,
        bypass_cache=True,
    )

    assert result == {"Data": []}
    assert calls == {"get": 0, "put": 0}


def test_search_case_pav_tags_docs_with_canonical_case_number(monkeypatch: Any) -> None:
    service = _build_service(monkeypatch)

    monkeypatch.setattr(
        service,
        "_pav_search",
        lambda **_kwargs: [{"Instrument": "2024000123", "DocType": "LIS PENDENS"}],
    )

    docs = service._search_case_pav(  # noqa: SLF001
        "24CA003727",
        {"api_calls": 0, "retries": 0},
        persist_case_number="292024CA003727A001HC",
        bypass_cache=True,
    )

    assert docs == [
        {
            "Instrument": "2024000123",
            "DocType": "LIS PENDENS",
            "CaseNum": "292024CA003727A001HC",
        }
    ]


def test_process_target_skips_inferred_fallback_for_lp_gap(monkeypatch: Any) -> None:
    service = _build_service(monkeypatch)
    target = {
        "foreclosure_id": 21007,
        "case_number": "292024CA003727A001HC",
        "strap": None,
        "folio": None,
        "skip_inferred_fallback": True,
        "mark_ori_searched": False,
    }
    monkeypatch.setattr(
        service,
        "_prepare_target_identity",
        _passthrough_prepare_target_identity,
    )

    monkeypatch.setattr(
        service,
        "_discover_property",
        lambda _target: (
            [],
            {
                "api_calls": 1,
                "retries": 0,
                "truncated": 0,
                "unresolved_truncations": 0,
                "deed_count": 0,
                "clerk_case_count": 0,
                "official_seed_docs": 0,
            },
        ),
    )

    def _unexpected_infer(*_args: Any, **_kwargs: Any) -> int:
        raise AssertionError("inferred fallback should not run for LP gap targets")

    monkeypatch.setattr(service, "_save_documents", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(service, "_infer_from_judgment", _unexpected_infer)

    result = service._process_target(target, persist=True)  # noqa: SLF001

    assert result["saved"] == 0
    assert result["inferred"] == 0


def test_prepare_target_identity_recovers_from_judgment_legal(
    monkeypatch: Any,
) -> None:
    service = _build_service(monkeypatch)
    captured: list[tuple[str, dict[str, Any]]] = []

    def _execute(sql_text: str, params: dict[str, Any]) -> _CaptureResult:
        if "FROM hcpa_bulk_parcels" in sql_text and "LIMIT :limit" in sql_text:
            return _CaptureResult(
                mapping_rows=[
                    {
                        "strap": "19283348Y000000000310A",
                        "folio": "1534060000",
                        "property_address": "4102 E HANNA AVE",
                        "raw_legal1": "HIGH POINT SUBDIVISION",
                        "raw_legal2": "LOT 31",
                        "raw_legal3": "",
                        "raw_legal4": "",
                        "owner_name": "EXAMPLE OWNER",
                    }
                ],
            )
        return _CaptureResult()

    service.engine = _ExecuteFnEngine(_execute, captured)  # type: ignore[assignment]

    target = {
        "foreclosure_id": 21007,
        "case_number": "292024CA003727A001HC",
        "strap": None,
        "folio": None,
        "judgment_data": {
            "legal_description": "LOT 31, HIGH POINT SUBDIVISION",
            "property_address": "314 S. Franklin Street, Titusville, Pennsylvania 16354",
        },
        "property_address": "",
        "legal1": "",
        "legal2": "",
        "legal3": "",
        "legal4": "",
        "owner_name": "",
    }

    prepared, recovered = service._prepare_target_identity(  # noqa: SLF001
        target,
        persist_update=True,
    )

    assert recovered is not None
    assert recovered.reason == "judgment_legal_match"
    assert prepared["strap"] == "19283348Y000000000310A"
    assert prepared["folio"] == "1534060000"
    assert prepared["property_address"] == "4102 E HANNA AVE"
    update_params = next(
        params for sql_text, params in captured if "UPDATE foreclosures" in sql_text
    )
    assert update_params["foreclosure_id"] == 21007
    assert update_params["strap"] == "19283348Y000000000310A"
    assert update_params["folio"] == "1534060000"


def test_process_target_stages_case_only_docs_when_identity_missing(
    monkeypatch: Any,
    tmp_path: Any,
) -> None:
    service = _build_service(monkeypatch)
    marks: list[int] = []
    target = {
        "foreclosure_id": 21007,
        "case_number": "292024CA003727A001HC",
        "strap": None,
        "folio": None,
        "judgment_data": {"legal_description": "LOT 31 HIGH POINT SUBDIVISION"},
        "property_address": "",
    }

    monkeypatch.setattr(pg_ori_service, "FORECLOSURE_DATA_DIR", tmp_path)
    monkeypatch.setattr(
        service,
        "_prepare_target_identity",
        _passthrough_prepare_target_identity,
    )
    monkeypatch.setattr(
        service,
        "_discover_property",
        lambda _target: (
            [{"Instrument": "2024000123", "DocType": "MORTGAGE"}],
            {
                "api_calls": 1,
                "retries": 0,
                "truncated": 0,
                "unresolved_truncations": 0,
                "deed_count": 0,
                "clerk_case_count": 0,
                "official_seed_docs": 0,
            },
        ),
    )

    def _unexpected_save(*_args: Any, **_kwargs: Any) -> int:
        raise AssertionError("_save_documents should not run without recovered identity")

    monkeypatch.setattr(service, "_save_documents", _unexpected_save)
    monkeypatch.setattr(service, "_mark_searched", lambda fid: marks.append(fid))

    result = service._process_target(target, persist=True)  # noqa: SLF001

    assert result["saved"] == 0
    assert result["case_only_stage_path"] is not None
    assert marks == []
    staged_payload = json.loads(
        (tmp_path / "292024CA003727A001HC" / "ori" / "case_only_unresolved_documents.json").read_text(
            encoding="utf-8"
        )
    )
    assert staged_payload["reason"] == "missing_property_identity"
    assert staged_payload["documents"][0]["Instrument"] == "2024000123"


def test_process_target_does_not_mark_searched_on_zero_persistence(monkeypatch: Any) -> None:
    service = _build_service(monkeypatch)
    marks: list[int] = []
    target = {
        "foreclosure_id": 21007,
        "case_number": "292024CA003727A001HC",
        "strap": "19283348Y000000000310A",
        "folio": "1534060000",
        "judgment_data": {},
        "property_address": "",
        "skip_inferred_fallback": True,
    }

    monkeypatch.setattr(
        service,
        "_prepare_target_identity",
        _passthrough_prepare_target_identity,
    )
    monkeypatch.setattr(
        service,
        "_discover_property",
        lambda _target: (
            [],
            {
                "api_calls": 0,
                "retries": 0,
                "truncated": 0,
                "unresolved_truncations": 0,
                "deed_count": 0,
                "clerk_case_count": 0,
                "official_seed_docs": 0,
            },
        ),
    )

    def _save_zero(_strap: str | None, _folio: str | None, _docs: list[dict[str, Any]]) -> int:
        service._last_save_documents_stats = {  # noqa: SLF001
            "saved": 0,
            "skipped": 0,
            "eligible": 0,
        }
        return 0

    monkeypatch.setattr(service, "_save_documents", _save_zero)
    monkeypatch.setattr(service, "_mark_searched", lambda fid: marks.append(fid))

    result = service._process_target(target, persist=True)  # noqa: SLF001

    assert result["saved"] == 0
    assert result["marked_ori_searched"] is False
    assert marks == []


def test_process_target_does_not_mark_searched_after_save_skips(monkeypatch: Any) -> None:
    service = _build_service(monkeypatch)
    marks: list[int] = []
    target = {
        "foreclosure_id": 21007,
        "case_number": "292024CA003727A001HC",
        "strap": "19283348Y000000000310A",
        "folio": "1534060000",
        "judgment_data": {},
        "property_address": "",
        "skip_inferred_fallback": True,
    }

    monkeypatch.setattr(
        service,
        "_prepare_target_identity",
        _passthrough_prepare_target_identity,
    )
    monkeypatch.setattr(
        service,
        "_discover_property",
        lambda _target: (
            [{"Instrument": "2024000123", "DocType": "MORTGAGE"}],
            {
                "api_calls": 1,
                "retries": 0,
                "truncated": 0,
                "unresolved_truncations": 0,
                "deed_count": 0,
                "clerk_case_count": 0,
                "official_seed_docs": 0,
            },
        ),
    )

    def _save_with_skip(_strap: str | None, _folio: str | None, _docs: list[dict[str, Any]]) -> int:
        service._last_save_documents_stats = {  # noqa: SLF001
            "saved": 0,
            "skipped": 1,
            "eligible": 1,
        }
        return 0

    monkeypatch.setattr(service, "_save_documents", _save_with_skip)
    monkeypatch.setattr(service, "_mark_searched", lambda fid: marks.append(fid))

    result = service._process_target(target, persist=True)  # noqa: SLF001

    assert result["save_skips"] == 1
    assert result["eligible_documents"] == 1
    assert result["marked_ori_searched"] is False
    assert marks == []


def test_process_target_logs_zero_persistence_context(monkeypatch: Any) -> None:
    service = _build_service(monkeypatch)
    warnings: list[str] = []
    target = {
        "foreclosure_id": 15289,
        "case_number": "292025CA001913A001HC",
        "strap": "172834985C00000000010U",
        "folio": "0066170422",
        "judgment_data": {"plaintiff": "Newrez LLC", "defendant": "Example Borrower"},
        "property_address": "10243 VILLA PALAZZO CT",
        "ori_run_context": "targeted_recovery",
        "ori_retry_reasons": ["construction_lien_risk"],
    }

    monkeypatch.setattr(
        service,
        "_prepare_target_identity",
        _passthrough_prepare_target_identity,
    )
    monkeypatch.setattr(
        service,
        "_discover_property",
        lambda _target: (
            [
                {"Instrument": "2025208942", "DocType": "LIS PENDENS"},
                {"Instrument": "2026041591", "DocType": "JUDGMENT"},
            ],
            {
                "api_calls": 0,
                "retries": 0,
                "truncated": 1,
                "unresolved_truncations": 0,
                "deed_count": 5,
                "clerk_case_count": 3,
                "official_seed_docs": 0,
            },
        ),
    )

    def _save_zero(_strap: str | None, _folio: str | None, _docs: list[dict[str, Any]]) -> int:
        service._last_save_documents_stats = {  # noqa: SLF001
            "saved": 0,
            "skipped": 0,
            "eligible": 2,
        }
        return 0

    def _infer_zero(_strap: str | None, _folio: str | None, _target: dict[str, Any]) -> int:
        service._last_infer_from_judgment_stats = {  # noqa: SLF001
            "saved": 0,
            "reason": "existing_inferred_encumbrance",
        }
        return 0

    monkeypatch.setattr(service, "_save_documents", _save_zero)
    monkeypatch.setattr(service, "_infer_from_judgment", _infer_zero)
    monkeypatch.setattr(
        service,
        "_existing_encumbrance_snapshot",
        lambda _strap, _case: {
            "total": 9,
            "core_liens": 3,
            "foreclosing": 1,
            "lis_pendens": 1,
            "noc": 0,
        },
    )
    monkeypatch.setattr(
        pg_ori_service.logger,
        "warning",
        lambda message, *args: warnings.append(message.format(*args)),
    )

    result = service._process_target(target, persist=True)  # noqa: SLF001

    assert result["saved"] == 0
    assert result["run_context"] == "targeted_recovery"
    assert result["retry_reasons"] == ["construction_lien_risk"]
    assert warnings
    assert (
        "No new encumbrances persisted for case=292025CA001913A001HC"
        in warnings[-1]
    )
    assert "retry_reasons=construction_lien_risk" in warnings[-1]
    assert "why=eligible documents were already persisted unchanged" in warnings[-1]
    assert "inferred_why=judgment fallback was already persisted" in warnings[-1]
    assert "existing_total=9" in warnings[-1]
    assert "existing_foreclosing=1" in warnings[-1]


def test_infer_from_judgment_tracks_existing_inferred_reason(monkeypatch: Any) -> None:
    service = _build_service(monkeypatch)
    captured: list[tuple[str, dict[str, Any]]] = []

    def _execute(sql_text: str, _params: dict[str, Any]) -> _CaptureResult:
        if "SELECT id FROM ori_encumbrances WHERE strap = :strap AND instrument_number = :inst" in sql_text:
            return _CaptureResult(rows=[(1,)])
        return _CaptureResult()

    service.engine = _ExecuteFnEngine(_execute, captured)  # type: ignore[assignment]

    result = service._infer_from_judgment(  # noqa: SLF001
        "172834985C00000000010U",
        "0066170422",
        {
            "case_number": "292025CA001913A001HC",
            "judgment_data": {
                "plaintiff": "Newrez LLC D/B/A Shellpoint Mortgage Servicing",
                "defendant": "Example Borrower",
            },
        },
    )

    assert result == 0
    assert service._last_infer_from_judgment_stats == {  # noqa: SLF001
        "saved": 0,
        "reason": "existing_inferred_encumbrance",
    }


def test_discover_property_bypasses_case_cache_and_skips_noc_fallback_for_lp_gap(
    monkeypatch: Any,
) -> None:
    service = _build_service(monkeypatch)
    captured: list[tuple[str, str | None, bool]] = []
    target = {
        "foreclosure_id": 21007,
        "case_number": "292024CA003727A001HC",
        "strap": None,
        "folio": None,
        "judgment_data": {},
        "auction_date": None,
        "filing_date": date(2024, 3, 1),
        "legal1": "",
        "legal2": "",
        "legal3": "",
        "legal4": "",
        "owner_name": "",
        "property_address": "",
        "lp_recovery_mode": True,
        "skip_live_noc_fallback": True,
    }

    monkeypatch.setattr(service, "_get_ownership_chain", lambda _strap: [])
    monkeypatch.setattr(service, "_seed_from_official_records", lambda **_kwargs: [])
    monkeypatch.setattr(service, "_get_clerk_case_seeds", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(service, "_build_search_terms", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(service, "_search_legal_pav", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(service, "_search_party_pav", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(service, "_search_book_page_pav", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        service,
        "_search_case_pav",
        lambda case_number, _stats, *, persist_case_number=None, bypass_cache=False: (
            captured.append((case_number, persist_case_number, bypass_cache)) or []
        ),
    )

    def _unexpected_noc(**_kwargs: Any) -> list[dict[str, Any]]:
        raise AssertionError("NOC fallback should not run for LP recovery mode")

    monkeypatch.setattr(service, "_run_live_noc_fallback", _unexpected_noc)

    docs, _stats = service._discover_property(target)  # noqa: SLF001

    assert docs == []
    assert captured == [
        ("292024CA003727A001HC", "292024CA003727A001HC", True),
        ("24-CA-003727", "292024CA003727A001HC", True),
        ("24CA003727", "292024CA003727A001HC", True),
    ]


def test_run_lis_pendens_backfill_saves_only_lis_pendens_docs(monkeypatch: Any) -> None:
    service = _build_service(monkeypatch)
    target = {
        "foreclosure_id": 21007,
        "case_number": "292024CA003727A001HC",
        "strap": "19283348Y000000000310A",
        "folio": "1534060000",
        "judgment_data": {},
        "auction_date": None,
        "filing_date": None,
        "legal1": "",
        "legal2": "",
        "legal3": "",
        "legal4": "",
        "owner_name": "",
        "property_address": "",
        "lp_recovery_mode": True,
        "skip_inferred_fallback": True,
        "skip_live_noc_fallback": True,
    }
    discovered_docs = [
        {"Instrument": "2024000123", "DocType": "LIS PENDENS"},
        {"Instrument": "2024000456", "DocType": "MORTGAGE"},
    ]
    saved_docs: list[list[dict[str, Any]]] = []

    states = iter([[target], []])
    monkeypatch.setattr(
        service,
        "_find_lis_pendens_gap_targets",
        lambda **_kwargs: next(states),
    )
    monkeypatch.setattr(
        service,
        "_prepare_target_identity",
        _passthrough_prepare_target_identity,
    )
    monkeypatch.setattr(
        service,
        "_discover_property",
        lambda _target: (
            discovered_docs,
            {
                "api_calls": 2,
                "retries": 0,
                "truncated": 0,
                "unresolved_truncations": 0,
                "deed_count": 0,
                "clerk_case_count": 0,
                "official_seed_docs": 0,
            },
        ),
    )
    monkeypatch.setattr(
        service,
        "_save_documents",
        lambda _strap, _folio, docs: saved_docs.append(docs) or len(docs),
    )
    monkeypatch.setattr(service, "_has_persisted_lis_pendens", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(service, "_mark_searched", lambda _fid: None)

    result = service.run_lis_pendens_backfill()

    assert result["targets"] == 1
    assert result["targets_with_lp_docs"] == 1
    assert result["total_lp_docs_found"] == 1
    assert result["total_saved"] == 1
    assert result["per_target"][0]["instruments"] == ["2024000123"]
    assert saved_docs == [[{"Instrument": "2024000123", "DocType": "LIS PENDENS"}]]


def test_run_lis_pendens_backfill_does_not_mark_searched_without_persisted_lp(
    monkeypatch: Any,
) -> None:
    service = _build_service(monkeypatch)
    target = {
        "foreclosure_id": 21007,
        "case_number": "292024CA003727A001HC",
        "strap": "19283348Y000000000310A",
        "folio": "1534060000",
        "judgment_data": {},
        "auction_date": None,
        "filing_date": None,
        "legal1": "",
        "legal2": "",
        "legal3": "",
        "legal4": "",
        "owner_name": "",
        "property_address": "",
        "lp_recovery_mode": True,
        "skip_inferred_fallback": True,
        "skip_live_noc_fallback": True,
    }
    states = iter([[target], [target]])
    marks: list[int] = []

    monkeypatch.setattr(
        service,
        "_find_lis_pendens_gap_targets",
        lambda **_kwargs: next(states),
    )
    monkeypatch.setattr(
        service,
        "_prepare_target_identity",
        _passthrough_prepare_target_identity,
    )
    monkeypatch.setattr(
        service,
        "_discover_property",
        lambda _target: (
            [{"Instrument": "2024000123", "DocType": "LIS PENDENS"}],
            {
                "api_calls": 2,
                "retries": 0,
                "truncated": 0,
                "unresolved_truncations": 0,
                "deed_count": 0,
                "clerk_case_count": 0,
                "official_seed_docs": 0,
            },
        ),
    )

    monkeypatch.setattr(service, "_save_documents", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(service, "_has_persisted_lis_pendens", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(service, "_mark_searched", lambda fid: marks.append(fid))

    result = service.run_lis_pendens_backfill(require_ori_searched=False)

    assert result["total_lp_docs_found"] == 1
    assert result["total_saved"] == 0
    assert result["remaining_lp_gaps_after"] == 1
    assert result["per_target"][0]["persisted_lp"] is False
    assert marks == []


def test_run_lis_pendens_backfill_stages_unresolved_docs_when_identity_missing(
    monkeypatch: Any,
    tmp_path: Any,
) -> None:
    service = _build_service(monkeypatch)
    target = {
        "foreclosure_id": 21007,
        "case_number": "292024CA003727A001HC",
        "strap": None,
        "folio": None,
        "judgment_data": {},
        "auction_date": None,
        "filing_date": None,
        "legal1": "",
        "legal2": "",
        "legal3": "",
        "legal4": "",
        "owner_name": "",
        "property_address": "",
        "lp_recovery_mode": True,
        "skip_inferred_fallback": True,
        "skip_live_noc_fallback": True,
    }
    states = iter([[target], [target]])
    marks: list[int] = []

    monkeypatch.setattr(pg_ori_service, "FORECLOSURE_DATA_DIR", tmp_path)
    monkeypatch.setattr(
        service,
        "_find_lis_pendens_gap_targets",
        lambda **_kwargs: next(states),
    )
    monkeypatch.setattr(
        service,
        "_prepare_target_identity",
        _passthrough_prepare_target_identity,
    )
    monkeypatch.setattr(
        service,
        "_discover_property",
        lambda _target: (
            [{"Instrument": "2024000123", "DocType": "LIS PENDENS"}],
            {
                "api_calls": 2,
                "retries": 0,
                "truncated": 0,
                "unresolved_truncations": 0,
                "deed_count": 0,
                "clerk_case_count": 0,
                "official_seed_docs": 0,
            },
        ),
    )
    monkeypatch.setattr(service, "_save_documents", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(service, "_has_persisted_lis_pendens", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(service, "_mark_searched", lambda fid: marks.append(fid))

    result = service.run_lis_pendens_backfill(require_ori_searched=False)

    assert result["total_lp_docs_found"] == 1
    assert result["total_saved"] == 0
    assert result["per_target"][0]["case_only_stage_path"] is not None
    assert result["per_target"][0]["persisted_lp"] is False
    assert marks == []
    staged_payload = json.loads(
        (
            tmp_path
            / "292024CA003727A001HC"
            / "ori"
            / "case_only_unresolved_lis_pendens_docs.json"
        ).read_text(encoding="utf-8")
    )
    assert staged_payload["lp_only"] is True
    assert staged_payload["documents"][0]["Instrument"] == "2024000123"


# ===================================================================
#  backfill_missing_ori_ids
# ===================================================================


def test_backfill_missing_ori_ids_noop_when_no_rows(monkeypatch: Any) -> None:
    """When every encumbrance already has ori_id, the method should skip."""
    service = _build_service(monkeypatch)
    captured: list[tuple[str, dict[str, Any]]] = []

    def _execute_fn(sql: str, params: dict[str, Any]) -> _CaptureResult:
        # The SELECT for rows with NULL ori_id returns nothing
        return _CaptureResult(mapping_rows=[])

    service.engine = _ExecuteFnEngine(_execute_fn, captured)
    result = service.backfill_missing_ori_ids()
    assert result["skipped"] is True
    assert result["reason"] == "no_rows_need_ori_id"


def test_backfill_missing_ori_ids_resolves_single_match(monkeypatch: Any) -> None:
    """When PAV returns exactly one matching document, ori_id is written back."""
    service = _build_service(monkeypatch)
    captured: list[tuple[str, dict[str, Any]]] = []
    call_seq = {"idx": 0}

    rows_needing_backfill = [
        {"id": 42, "instrument_number": "2026038531"},
    ]

    def _execute_fn(sql: str, params: dict[str, Any]) -> _CaptureResult:
        sql_upper = sql.upper()
        idx = call_seq["idx"]
        call_seq["idx"] += 1

        # First call is the SELECT for rows with NULL ori_id
        if idx == 0 and "ORI_ID IS NULL" in sql_upper:
            return _CaptureResult(mapping_rows=rows_needing_backfill)

        # Subsequent calls are UPDATE statements
        if "UPDATE ORI_ENCUMBRANCES" in sql_upper:
            assert params.get("ori_id") == "PAV-DOC-999"
            assert params.get("enc_id") == 42
            return _CaptureResult(rowcount=1)

        return _CaptureResult()

    service.engine = _ExecuteFnEngine(_execute_fn, captured)

    # Mock _search_instrument_pav to return a single matching document
    monkeypatch.setattr(
        service,
        "_search_instrument_pav",
        lambda _instrument, _stats: [
            {"Instrument": "2026038531", "ID": "PAV-DOC-999", "DocType": "LN"},
        ],
    )

    result = service.backfill_missing_ori_ids()
    assert result["resolved"] == 1
    assert result["not_found"] == 0
    assert result["ambiguous"] == 0
    assert result["errors"] == 0
    assert result["targets"] == 1


def test_backfill_missing_ori_ids_skips_ambiguous(monkeypatch: Any) -> None:
    """When PAV returns multiple docs for the same instrument, skip it."""
    service = _build_service(monkeypatch)
    captured: list[tuple[str, dict[str, Any]]] = []
    call_seq = {"idx": 0}

    rows_needing_backfill = [
        {"id": 10, "instrument_number": "2026042141"},
    ]

    def _execute_fn(sql: str, params: dict[str, Any]) -> _CaptureResult:
        idx = call_seq["idx"]
        call_seq["idx"] += 1
        if idx == 0:
            return _CaptureResult(mapping_rows=rows_needing_backfill)
        return _CaptureResult()

    service.engine = _ExecuteFnEngine(_execute_fn, captured)

    monkeypatch.setattr(
        service,
        "_search_instrument_pav",
        lambda _instrument, _stats: [
            {"Instrument": "2026042141", "ID": "ID-A", "DocType": "LN"},
            {"Instrument": "2026042141", "ID": "ID-B", "DocType": "LN"},
        ],
    )

    result = service.backfill_missing_ori_ids()
    assert result["resolved"] == 0
    assert result["ambiguous"] == 1
    assert result["targets"] == 1


def test_backfill_missing_ori_ids_handles_not_found(monkeypatch: Any) -> None:
    """When PAV returns no docs, record as not_found."""
    service = _build_service(monkeypatch)
    captured: list[tuple[str, dict[str, Any]]] = []
    call_seq = {"idx": 0}

    rows_needing_backfill = [
        {"id": 7, "instrument_number": "2026056127"},
    ]

    def _execute_fn(sql: str, params: dict[str, Any]) -> _CaptureResult:
        idx = call_seq["idx"]
        call_seq["idx"] += 1
        if idx == 0:
            return _CaptureResult(mapping_rows=rows_needing_backfill)
        return _CaptureResult()

    service.engine = _ExecuteFnEngine(_execute_fn, captured)

    monkeypatch.setattr(
        service,
        "_search_instrument_pav",
        lambda _instrument, _stats: [],
    )

    result = service.backfill_missing_ori_ids()
    assert result["resolved"] == 0
    assert result["not_found"] == 1
    assert result["targets"] == 1


def test_backfill_missing_ori_ids_respects_limit(monkeypatch: Any) -> None:
    """The limit parameter should be forwarded to the SQL query."""
    service = _build_service(monkeypatch)
    captured: list[tuple[str, dict[str, Any]]] = []
    call_seq = {"idx": 0}

    rows_needing_backfill = [
        {"id": 1, "instrument_number": "2026000001"},
    ]

    def _execute_fn(sql: str, params: dict[str, Any]) -> _CaptureResult:
        idx = call_seq["idx"]
        call_seq["idx"] += 1
        if idx == 0:
            assert params.get("lim") == 5
            return _CaptureResult(mapping_rows=rows_needing_backfill)
        return _CaptureResult(rowcount=1)

    service.engine = _ExecuteFnEngine(_execute_fn, captured)

    monkeypatch.setattr(
        service,
        "_search_instrument_pav",
        lambda _instrument, _stats: [
            {"Instrument": "2026000001", "ID": "ID-X", "DocType": "MTG"},
        ],
    )

    result = service.backfill_missing_ori_ids(limit=5)
    assert result["resolved"] == 1


def test_backfill_missing_ori_ids_handles_search_exception(monkeypatch: Any) -> None:
    """If PAV search throws, count it as an error and continue."""
    service = _build_service(monkeypatch)
    captured: list[tuple[str, dict[str, Any]]] = []
    call_seq = {"idx": 0}

    rows_needing_backfill = [
        {"id": 1, "instrument_number": "2026000001"},
        {"id": 2, "instrument_number": "2026000002"},
    ]

    def _execute_fn(sql: str, params: dict[str, Any]) -> _CaptureResult:
        idx = call_seq["idx"]
        call_seq["idx"] += 1
        if idx == 0:
            return _CaptureResult(mapping_rows=rows_needing_backfill)
        return _CaptureResult(rowcount=1)

    service.engine = _ExecuteFnEngine(_execute_fn, captured)

    call_count = {"n": 0}

    def _mock_search(instrument: str, stats: dict[str, int]) -> list[dict[str, Any]]:
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("network timeout")
        return [{"Instrument": instrument, "ID": "ID-OK", "DocType": "SAT"}]

    monkeypatch.setattr(service, "_search_instrument_pav", _mock_search)

    result = service.backfill_missing_ori_ids()
    assert result["errors"] == 1
    assert result["resolved"] == 1
    assert result["targets"] == 2


def test_backfill_missing_ori_ids_filters_non_matching_instruments(
    monkeypatch: Any,
) -> None:
    """PAV may return docs for other instruments; only exact matches count."""
    service = _build_service(monkeypatch)
    captured: list[tuple[str, dict[str, Any]]] = []
    call_seq = {"idx": 0}

    rows_needing_backfill = [
        {"id": 50, "instrument_number": "2026040896"},
    ]

    def _execute_fn(sql: str, params: dict[str, Any]) -> _CaptureResult:
        idx = call_seq["idx"]
        call_seq["idx"] += 1
        if idx == 0:
            return _CaptureResult(mapping_rows=rows_needing_backfill)
        return _CaptureResult(rowcount=1)

    service.engine = _ExecuteFnEngine(_execute_fn, captured)

    monkeypatch.setattr(
        service,
        "_search_instrument_pav",
        lambda _instrument, _stats: [
            # PAV returned a different instrument in the batch
            {"Instrument": "2026040897", "ID": "ID-WRONG", "DocType": "ASG"},
            # And the one we want
            {"Instrument": "2026040896", "ID": "ID-RIGHT", "DocType": "ASG"},
        ],
    )

    result = service.backfill_missing_ori_ids()
    assert result["resolved"] == 1

    # Verify the correct ID was written
    update_calls = [
        (sql, params)
        for sql, params in captured
        if "UPDATE" in sql.upper() and "ori_id" in sql
    ]
    assert len(update_calls) == 1
    assert update_calls[0][1]["ori_id"] == "ID-RIGHT"


# ---------------------------------------------------------------------------
# resolve_inferred_encumbrances tests
# ---------------------------------------------------------------------------


def test_resolve_inferred_noop_when_no_rows(monkeypatch: Any) -> None:
    """When there are no inferred rows, the method should skip."""
    service = _build_service(monkeypatch)
    captured: list[tuple[str, dict[str, Any]]] = []

    def _execute_fn(sql: str, params: dict[str, Any]) -> _CaptureResult:
        return _CaptureResult(mapping_rows=[])

    service.engine = _ExecuteFnEngine(_execute_fn, captured)
    result = service.resolve_inferred_encumbrances()
    assert result["skipped"] is True


def test_resolve_inferred_pass1_deletes_matching(monkeypatch: Any) -> None:
    """Pass 1 deletes inferred row when real encumbrance matches plaintiff."""
    service = _build_service(monkeypatch)
    captured: list[tuple[str, dict[str, Any]]] = []
    call_seq = {"idx": 0}

    inferred_rows = [
        {
            "id": 100,
            "strap": "ABC123",
            "folio": "0001",
            "instrument_number": "INFERRED-292025CA001",
            "case_number": "292025CA001",
            "encumbrance_type": "mortgage",
            "party1": "BANK OF AMERICA",
        },
    ]

    # entity_match_score match result (real encumbrance)
    match_row = (42, "2020123456", "mortgage", 0.85, 0.10)

    def _execute_fn(sql: str, params: dict[str, Any]) -> _CaptureResult:
        sql_upper = sql.upper()
        idx = call_seq["idx"]
        call_seq["idx"] += 1

        # First call: SELECT inferred rows
        if idx == 0 and "INFERRED" in sql_upper and "LIKE" in sql_upper:
            return _CaptureResult(mapping_rows=inferred_rows)

        # Second call: entity_match_score query
        if "ENTITY_MATCH_SCORE" in sql_upper:
            return _CaptureResult(rows=[match_row])

        # Third call: DELETE
        if "DELETE" in sql_upper:
            assert params.get("id") == 100
            return _CaptureResult(rowcount=1)

        return _CaptureResult()

    service.engine = _ExecuteFnEngine(_execute_fn, captured)
    result = service.resolve_inferred_encumbrances()
    assert result["pass1_deleted"] == 1
    assert result["total_deleted"] == 1
    assert result["kept"] == 0


def test_resolve_inferred_pass1_keeps_unmatched(monkeypatch: Any) -> None:
    """Pass 1 keeps inferred row when no real encumbrance matches."""
    service = _build_service(monkeypatch)
    captured: list[tuple[str, dict[str, Any]]] = []
    call_seq = {"idx": 0}

    inferred_rows = [
        {
            "id": 200,
            "strap": "XYZ789",
            "folio": "0002",
            "instrument_number": "INFERRED-292025CA002",
            "case_number": "292025CA002",
            "encumbrance_type": "lien",
            "party1": "UNKNOWN HOA INC",
        },
    ]

    def _execute_fn(sql: str, params: dict[str, Any]) -> _CaptureResult:
        sql_upper = sql.upper()
        idx = call_seq["idx"]
        call_seq["idx"] += 1

        if idx == 0 and "INFERRED" in sql_upper and "LIKE" in sql_upper:
            return _CaptureResult(mapping_rows=inferred_rows)

        # No match from entity_match_score
        if "ENTITY_MATCH_SCORE" in sql_upper:
            return _CaptureResult(rows=[])

        return _CaptureResult()

    service.engine = _ExecuteFnEngine(_execute_fn, captured)

    # Mock _search_case_pav to return nothing
    monkeypatch.setattr(service, "_search_case_pav", lambda *_a, **_kw: [])

    result = service.resolve_inferred_encumbrances()
    assert result["pass1_deleted"] == 0
    assert result["kept"] == 1
    assert result["total_deleted"] == 0


def test_resolve_inferred_pass2_searches_case_and_deletes(monkeypatch: Any) -> None:
    """Pass 2 searches ORI by case, saves docs, and deletes inferred."""
    service = _build_service(monkeypatch)
    captured: list[tuple[str, dict[str, Any]]] = []
    call_seq = {"idx": 0}

    inferred_rows = [
        {
            "id": 300,
            "strap": "DEF456",
            "folio": "0003",
            "instrument_number": "INFERRED-292025CA003",
            "case_number": "292025CA003",
            "encumbrance_type": "mortgage",
            "party1": "RARE LENDER LLC",
        },
    ]

    def _execute_fn(sql: str, params: dict[str, Any]) -> _CaptureResult:
        sql_upper = sql.upper()
        idx = call_seq["idx"]
        call_seq["idx"] += 1

        if idx == 0 and "INFERRED" in sql_upper and "LIKE" in sql_upper:
            return _CaptureResult(mapping_rows=inferred_rows)

        # No entity_match_score match in pass 1
        if "ENTITY_MATCH_SCORE" in sql_upper:
            return _CaptureResult(rows=[])

        # DELETE in pass 2
        if "DELETE" in sql_upper:
            assert params.get("id") == 300
            return _CaptureResult(rowcount=1)

        return _CaptureResult()

    service.engine = _ExecuteFnEngine(_execute_fn, captured)

    # Mock _search_case_pav to return docs
    monkeypatch.setattr(
        service,
        "_search_case_pav",
        lambda _case, _stats: [{"DocType": "MTG", "Instrument": "2020999999"}],
    )

    # Mock _save_documents to return 1 saved
    monkeypatch.setattr(service, "_save_documents", lambda _strap, _folio, _docs: 1)

    result = service.resolve_inferred_encumbrances()
    assert result["pass1_deleted"] == 0
    assert result["pass2_deleted"] == 1
    assert result["pass2_new_docs"] == 1
    assert result["total_deleted"] == 1


def test_resolve_inferred_pass2_keeps_when_no_new_docs(monkeypatch: Any) -> None:
    """Pass 2 keeps inferred when case search finds docs but none are new."""
    service = _build_service(monkeypatch)
    captured: list[tuple[str, dict[str, Any]]] = []
    call_seq = {"idx": 0}

    inferred_rows = [
        {
            "id": 400,
            "strap": "GHI012",
            "folio": "0004",
            "instrument_number": "INFERRED-292025CA004",
            "case_number": "292025CA004",
            "encumbrance_type": "mortgage",
            "party1": "GHOST BANK NA",
        },
    ]

    def _execute_fn(sql: str, params: dict[str, Any]) -> _CaptureResult:
        sql_upper = sql.upper()
        idx = call_seq["idx"]
        call_seq["idx"] += 1

        if idx == 0 and "INFERRED" in sql_upper and "LIKE" in sql_upper:
            return _CaptureResult(mapping_rows=inferred_rows)

        if "ENTITY_MATCH_SCORE" in sql_upper:
            return _CaptureResult(rows=[])

        return _CaptureResult()

    service.engine = _ExecuteFnEngine(_execute_fn, captured)

    monkeypatch.setattr(
        service,
        "_search_case_pav",
        lambda _case, _stats: [{"DocType": "LP", "Instrument": "2020111111"}],
    )

    # _save_documents returns 0 (all docs already existed)
    monkeypatch.setattr(service, "_save_documents", lambda _strap, _folio, _docs: 0)

    result = service.resolve_inferred_encumbrances()
    assert result["pass2_deleted"] == 0
    assert result["kept"] == 1
