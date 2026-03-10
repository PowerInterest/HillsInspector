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
