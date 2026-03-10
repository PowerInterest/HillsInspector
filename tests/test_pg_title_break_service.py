from src.services.pg_title_break_service import PgTitleBreakService


class _FakeOri:
    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.calls: list[tuple[dict, str, dict, bool]] = []

    def _post_pav(
        self,
        payload: dict,
        query_label: str,
        stats: dict,
        *,
        bypass_cache: bool = False,
    ) -> dict:
        self.calls.append((payload, query_label, stats, bypass_cache))
        return self.payload


class _GapOri:
    def __init__(self) -> None:
        self.party_calls: list[tuple[str, dict[str, int]]] = []
        self.legal_calls: list[tuple[str, dict[str, int]]] = []
        self.party_result: list[dict] = []
        self.legal_result: list[dict] = []

    def _build_search_terms(self, target: dict) -> list[str]:
        legal1 = (target.get("legal1") or "").strip()
        return [legal1] if legal1 else []

    def search_party_pav(
        self,
        name: str,
        stats: dict[str, int],
        *,
        from_date: object,
        to_date: object,
        split_on_truncated: bool,
        depth: int = 0,
    ) -> list[dict]:
        self.party_calls.append((name, dict(stats)))
        return list(self.party_result)

    def search_legal_pav(
        self,
        text_value: str,
        stats: dict[str, int],
        *,
        from_date: object,
        to_date: object,
        split_on_truncated: bool,
        depth: int = 0,
    ) -> list[dict]:
        self.legal_calls.append((text_value, dict(stats)))
        return list(self.legal_result)


def test_lookup_instrument_parties_uses_keyword_search_payload() -> None:
    service = PgTitleBreakService.__new__(PgTitleBreakService)
    fake_ori = _FakeOri(
        {
            "Data": [
                {
                    "DisplayColumnValues": [
                        {"Value": "PARTY 1"},
                        {"Value": "EAGLE CREEK DEVELOPERS INC"},
                        {"Value": "8/20/1999 12:00:00 AM"},
                        {"Value": "(D) DEED"},
                        {"Value": "O"},
                        {"Value": "9787"},
                        {"Value": "1841"},
                        {"Value": "L 21 B 6 BRENTWOOD HILLS TR F #1"},
                        {"Value": "99258994"},
                    ]
                },
                {
                    "DisplayColumnValues": [
                        {"Value": "PARTY 2"},
                        {"Value": "LOEFFLER KATHLEEN H"},
                        {"Value": "8/20/1999 12:00:00 AM"},
                        {"Value": "(D) DEED"},
                        {"Value": "O"},
                        {"Value": "9787"},
                        {"Value": "1841"},
                        {"Value": "L 21 B 6 BRENTWOOD HILLS TR F #1"},
                        {"Value": "99258994"},
                    ]
                },
            ]
        }
    )
    service._ori = fake_ori  # noqa: SLF001

    parsed = service._lookup_instrument_parties("99258994")  # noqa: SLF001

    assert parsed == {
        "doc_type": "(D) DEED",
        "record_date": "8/20/1999 12:00:00 AM",
        "from_text": "EAGLE CREEK DEVELOPERS INC",
        "to_text": "LOEFFLER KATHLEEN H",
    }
    payload, query_label, stats, bypass_cache = fake_ori.calls[0]
    assert payload == {
        "QueryID": 320,
        "Keywords": [{"Id": 1006, "Value": "99258994"}],
        "QueryLimit": 5,
    }
    assert query_label == "title_break_instrument:99258994"
    assert stats == {"api_calls": 0, "retries": 0}
    assert bypass_cache is True


def test_search_gap_deeds_uses_legal_first_for_high_volume_builder(monkeypatch) -> None:
    service = PgTitleBreakService.__new__(PgTitleBreakService)
    fake_ori = _GapOri()
    fake_ori.legal_result = [
        {
            "Instrument": "12345678",
            "DocType": "(D) DEED",
            "PartiesOne": ["LENNAR HOMES LLC"],
            "PartiesTwo": ["BUYER ONE"],
        }
    ]
    service._ori = fake_ori  # noqa: SLF001
    monkeypatch.setattr(
        service,
        "_search_gap_in_local_ori",
        lambda target, gap, *, party, from_date, to_date: [],  # noqa: ARG005
    )

    docs = service._search_gap_deeds(  # noqa: SLF001
        {"folio": "0701451488", "legal1": "BRENTWOOD HILLS TRACT F UNIT 1"},
        {
            "expected_from_party": "LENNAR HOMES LLC",
            "observed_to_party": "BUYER ONE",
        },
        from_date=__import__("datetime").date(2008, 1, 1),
        to_date=__import__("datetime").date(2008, 12, 31),
    )

    assert [doc["Instrument"] for doc in docs] == ["12345678"]
    assert fake_ori.legal_calls == [("BRENTWOOD HILLS TRACT F UNIT 1", {"api_calls": 0, "retries": 0, "truncated": 0, "unresolved_truncations": 0})]
    assert fake_ori.party_calls == []


def test_search_gap_deeds_falls_back_to_legal_after_party_truncation(monkeypatch) -> None:
    service = PgTitleBreakService.__new__(PgTitleBreakService)
    fake_ori = _GapOri()

    def _party_search(
        name: str,
        stats: dict[str, int],
        *,
        from_date: object,
        to_date: object,
        split_on_truncated: bool,
        depth: int = 0,
    ) -> list[dict]:
        stats["unresolved_truncations"] = 1
        fake_ori.party_calls.append((name, dict(stats)))
        return []

    fake_ori.search_party_pav = _party_search  # type: ignore[method-assign]
    fake_ori.legal_result = [
        {
            "Instrument": "87654321",
            "DocType": "(D) DEED",
            "PartiesOne": ["SMALL BUILDER LLC"],
            "PartiesTwo": ["BUYER TWO"],
        }
    ]
    service._ori = fake_ori  # noqa: SLF001
    monkeypatch.setattr(
        service,
        "_search_gap_in_local_ori",
        lambda target, gap, *, party, from_date, to_date: [],  # noqa: ARG005
    )

    docs = service._search_gap_deeds(  # noqa: SLF001
        {"folio": "0123456789", "legal1": "OAK GROVE LOT 7 BLOCK 2"},
        {
            "expected_from_party": "SMALL BUILDER LLC",
            "observed_to_party": "BUYER TWO",
        },
        from_date=__import__("datetime").date(2018, 1, 1),
        to_date=__import__("datetime").date(2018, 12, 31),
    )

    assert [doc["Instrument"] for doc in docs] == ["87654321"]
    assert fake_ori.party_calls[0][0] == "SMALL BUILDER LLC"
    assert fake_ori.legal_calls == [("OAK GROVE LOT 7 BLOCK 2", {"api_calls": 0, "retries": 0, "truncated": 0, "unresolved_truncations": 0})]


def test_search_gap_deeds_prefers_local_ori_for_high_volume_builder(monkeypatch) -> None:
    service = PgTitleBreakService.__new__(PgTitleBreakService)
    fake_ori = _GapOri()
    service._ori = fake_ori  # noqa: SLF001

    monkeypatch.setattr(
        service,
        "_search_gap_in_local_ori",
        lambda target, gap, *, party, from_date, to_date: [  # noqa: ARG005
            {
                "Instrument": "96115832",
                "DocType": "(D) DEED",
                "PartiesOne": ["WESTCHASE ASSOCIATES", "WESTCHASE DEVELOPMENT CORP"],
                "PartiesTwo": ["PULTE HOME CORP"],
            }
        ],
    )
    monkeypatch.setattr(
        service,
        "_search_gap_by_legal",
        lambda target, gap, *, from_date, to_date: [],  # noqa: ARG005
    )

    docs = service._search_gap_deeds(  # noqa: SLF001
        {"folio": "0040415108", "legal1": "BERKELEY SQUARE"},
        {
            "expected_from_party": "",
            "observed_to_party": "PULTE HOME CORP",
        },
        from_date=__import__("datetime").date(1996, 5, 1),
        to_date=__import__("datetime").date(1997, 6, 1),
    )

    assert [doc["Instrument"] for doc in docs] == ["96115832"]
    assert fake_ori.party_calls == []
    assert fake_ori.legal_calls == []


def test_local_ori_doc_score_prefers_directional_party_match() -> None:
    better = PgTitleBreakService._local_ori_doc_score(  # noqa: SLF001
        {
            "PartiesOne": ["PULTE HOME CORP"],
            "PartiesTwo": ["PULTE HOME CORP", "TRIPP H DOUGLAS ATTY"],
        },
        expected=PgTitleBreakService._normalize_party_text("PULTE HOME CORP"),  # noqa: SLF001
        observed=PgTitleBreakService._normalize_party_text("PULTE HOME CORP; TRIPP H DOUGLAS ATTY"),  # noqa: SLF001
    )
    weaker = PgTitleBreakService._local_ori_doc_score(  # noqa: SLF001
        {
            "PartiesOne": ["WESTCHASE ASSOCIATES"],
            "PartiesTwo": ["PULTE HOME CORP"],
        },
        expected=PgTitleBreakService._normalize_party_text("PULTE HOME CORP"),  # noqa: SLF001
        observed=PgTitleBreakService._normalize_party_text("PULTE HOME CORP; TRIPP H DOUGLAS ATTY"),  # noqa: SLF001
    )

    assert better > weaker
