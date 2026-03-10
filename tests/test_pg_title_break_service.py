from typing import Any

from src.services import pg_title_break_service
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


class _SentinelConn:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.executed: list[tuple[str, dict[str, Any]]] = []

    def execute(self, sql: Any, params: dict[str, Any]) -> Any:
        sql_text = str(sql)
        self.executed.append((sql_text, params))
        if "FROM fn_title_chain_gaps" in sql_text:
            return _GapResult(self.rows)
        return type("_RowcountResult", (), {"rowcount": 1})()

    def __enter__(self) -> "_SentinelConn":
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class _SentinelEngine:
    def __init__(self, conn: _SentinelConn) -> None:
        self.conn = conn

    def connect(self) -> _SentinelConn:
        return self.conn

    def begin(self) -> _SentinelConn:
        return self.conn


class _GapMappings:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[dict[str, Any]]:
        return self._rows


class _GapResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> _GapMappings:
        return _GapMappings(self._rows)


class _FindTargetsConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(self, sql: Any, params: dict[str, Any]) -> _GapResult:
        self.calls.append((str(sql), params))
        return _GapResult([])

    def __enter__(self) -> "_FindTargetsConn":
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class _FindTargetsEngine:
    def __init__(self, conn: _FindTargetsConn) -> None:
        self.conn = conn

    def connect(self) -> _FindTargetsConn:
        return self.conn


class _SentinelSkipConn:
    def __init__(
        self,
        *,
        target_rows: list[dict[str, Any]] | None = None,
        sentinel_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.target_rows = target_rows or []
        self.sentinel_rows = sentinel_rows or []
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(self, sql: Any, params: dict[str, Any]) -> _GapResult:
        sql_text = str(sql)
        self.calls.append((sql_text, params))
        if "retry_eligible_on" in sql_text:
            return _GapResult(self.sentinel_rows)
        return _GapResult(self.target_rows)

    def __enter__(self) -> "_SentinelSkipConn":
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class _CaptureLogger:
    def __init__(self) -> None:
        self.info_messages: list[str] = []
        self.error_messages: list[str] = []

    def info(self, message: str, *args: object) -> None:
        self.info_messages.append(message.format(*args) if args else message)

    def error(self, message: str, *args: object) -> None:
        self.error_messages.append(message.format(*args) if args else message)


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


def test_process_one_inserts_search_sentinel_when_no_deeds_found(monkeypatch) -> None:
    conn = _SentinelConn(
        [
            {
                "gap_type": "missing_party",
                "expected_from_party": "SELLER LLC",
                "observed_to_party": "BUYER LLC",
                "missing_from_date": "2024-01-01",
                "missing_to_date": "2024-12-31",
            }
        ]
    )
    service = PgTitleBreakService.__new__(PgTitleBreakService)
    service.engine = _SentinelEngine(conn)  # type: ignore[assignment]
    monkeypatch.setattr(
        service,
        "_search_gap_deeds",
        lambda *_args, **_kwargs: [],
    )

    gaps_found, deeds_inserted, sentinels_inserted = service._process_one(  # noqa: SLF001
        {
            "foreclosure_id": 11,
            "case_number_raw": "24-CA-000011",
            "case_number_norm": "24-CA-000011",
            "folio": "F11",
            "strap": "S11",
        }
    )

    assert gaps_found == 1
    assert deeds_inserted == 0
    assert sentinels_inserted == 1
    sentinel_sql, sentinel_params = conn.executed[-1]
    assert "event_source = 'ORI_DEED_SEARCH'" in sentinel_sql
    assert sentinel_params["foreclosure_id"] == 11
    assert sentinel_params["description"] == "ORI deed search completed with no matching deeds"


def test_find_targets_filters_complete_chains_and_recent_sentinels() -> None:
    conn = _FindTargetsConn()
    service = PgTitleBreakService.__new__(PgTitleBreakService)
    service.engine = _FindTargetsEngine(conn)  # type: ignore[assignment]

    assert service._find_targets(limit=25) == []  # noqa: SLF001

    sql_text, params = conn.calls[0]
    sql_lower = sql_text.lower()
    assert "left join foreclosure_title_summary ts" in sql_lower
    assert "coalesce(ts.gap_count, 0) > 0" in sql_lower
    assert "coalesce(ts.chain_status, '') <> 'complete'" in sql_lower
    assert "coalesce(e2.event_subtype, '') <> 'search_no_result'" in sql_lower
    assert "event_date >= current_date - cast(:retry_ttl_days as integer)" in sql_lower
    assert params["retry_ttl_days"] == 14


def test_insert_search_sentinel_respects_retry_ttl() -> None:
    conn = _SentinelConn([])
    service = PgTitleBreakService.__new__(PgTitleBreakService)
    service.engine = _SentinelEngine(conn)  # type: ignore[assignment]

    inserted = service._insert_search_sentinel(  # noqa: SLF001
        {
            "foreclosure_id": 42,
            "case_number_raw": "24-CA-000042",
            "case_number_norm": "24-CA-000042",
            "folio": "F42",
            "strap": "S42",
        }
    )

    assert inserted == 1
    sql_text, params = conn.executed[-1]
    sql_lower = sql_text.lower()
    assert "coalesce(event_subtype, '') <> 'search_no_result'" in sql_lower
    assert "event_subtype = 'search_no_result'" in sql_lower
    assert "event_date >= current_date - cast(:retry_ttl_days as integer)" in sql_lower
    assert params["retry_ttl_days"] == 14


def test_run_logs_recent_sentinel_skips(monkeypatch: Any) -> None:
    conn = _SentinelSkipConn(
        target_rows=[],
        sentinel_rows=[
            {
                "foreclosure_id": 42,
                "case_number_raw": "24-CA-000042",
                "folio": "F42",
                "strap": "S42",
                "chain_status": "BROKEN",
                "gap_count": 2,
                "sentinel_date": "2026-03-09",
                "retry_eligible_on": "2026-03-23",
                "retry_days_remaining": 13,
            }
        ],
    )
    service = PgTitleBreakService.__new__(PgTitleBreakService)
    service.engine = _FindTargetsEngine(conn)  # type: ignore[assignment]
    capture_logger = _CaptureLogger()
    monkeypatch.setattr(pg_title_break_service, "logger", capture_logger)

    result = service.run()

    assert result == {
        "skipped": True,
        "reason": "recent_search_no_result_sentinels",
        "recent_sentinel_skip_count": 1,
    }
    assert capture_logger.error_messages == []
    assert capture_logger.info_messages[0] == (
        "title_breaks: skipping 1 foreclosures due to recent SEARCH_NO_RESULT sentinels"
    )
    assert (
        "title_breaks: skipping foreclosure_id=42 case=24-CA-000042 folio=F42 strap=S42 "
        "chain_status=BROKEN gap_count=2 due to SEARCH_NO_RESULT sentinel dated 2026-03-09 "
        "(retry eligible 2026-03-23, 13 day(s) remaining)"
    ) in capture_logger.info_messages
