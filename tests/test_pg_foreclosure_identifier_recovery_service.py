from __future__ import annotations

from typing import Any
from typing import Self

from src.services import pg_foreclosure_identifier_recovery_service as identifier_recovery


class _FakeResult:
    def __init__(
        self,
        *,
        row: dict[str, Any] | None = None,
        rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self._row = row
        self._rows = rows or ([] if row is None else [row])

    def mappings(self) -> _FakeResult:
        return self

    def fetchone(self) -> dict[str, Any] | None:
        return self._row

    def fetchall(self) -> list[dict[str, Any]]:
        return list(self._rows)


class _ParcelLookupConnection:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, dict[str, Any] | None]] = []

    def execute(
        self,
        statement: Any,
        params: dict[str, Any] | None = None,
    ) -> _FakeResult:
        self.execute_calls.append((str(statement), params))
        return _FakeResult(
            row={
                "folio": "F-1",
                "strap": "S-1",
                "property_address": "1 MAIN ST",
                "raw_legal1": "LEGAL",
                "raw_legal2": "",
                "raw_legal3": "",
                "raw_legal4": "",
                "source_file_id": 123,
            }
        )


class _NestedTransaction:
    def __init__(self, conn: _RunConnection) -> None:
        self._conn = conn

    def __enter__(self) -> Self:
        self._conn.begin_nested_calls += 1
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> bool:
        if exc_type is not None:
            self._conn.aborted = False
        return False


class _RunConnection:
    def __init__(self) -> None:
        self.aborted = False
        self.begin_nested_calls = 0
        self.update_calls: list[dict[str, Any]] = []
        self.mark_calls: list[dict[str, Any]] = []

    def begin_nested(self) -> _NestedTransaction:
        return _NestedTransaction(self)

    def execute(
        self,
        statement: Any,
        params: dict[str, Any] | None = None,
    ) -> _FakeResult:
        if self.aborted:
            raise RuntimeError("transaction remained aborted")
        sql = str(statement)
        if "UPDATE foreclosures" not in sql:
            raise AssertionError(sql)
        if "step_identifier_recovery = now()" in sql:
            self.mark_calls.append(params or {})
            return _FakeResult()
        self.update_calls.append(params or {})
        return _FakeResult(
            row={
                "strap": (params or {}).get("strap"),
                "folio": (params or {}).get("folio"),
            }
        )


class _AddressLookupConnection:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, dict[str, Any] | None]] = []

    def execute(
        self,
        statement: Any,
        params: dict[str, Any] | None = None,
    ) -> _FakeResult:
        sql = str(statement)
        self.execute_calls.append((sql, params))
        if "WHERE property_address = :address" in sql:
            return _FakeResult(rows=[])
        return _FakeResult(
            rows=[
                {
                    "folio": "F-2",
                    "strap": "S-2",
                    "property_address": "123 CRESTHILL DR",
                    "raw_legal1": "CREST HILL",
                    "raw_legal2": "LOT 4",
                    "raw_legal3": "",
                    "raw_legal4": "",
                    "source_file_id": 77,
                }
            ]
        )


class _BlaineAddressLookupConnection:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, dict[str, Any] | None]] = []

    def execute(
        self,
        statement: Any,
        params: dict[str, Any] | None = None,
    ) -> _FakeResult:
        sql = str(statement)
        self.execute_calls.append((sql, params))
        if "WHERE property_address = :address" in sql:
            return _FakeResult(rows=[])
        return _FakeResult(
            rows=[
                {
                    "folio": "F-3",
                    "strap": "S-3",
                    "property_address": "11112 BLAINE TOP PL",
                    "raw_legal1": "WESTCHASE",
                    "raw_legal2": "LOT 8 BLOCK 2",
                    "raw_legal3": "",
                    "raw_legal4": "",
                    "source_file_id": 88,
                }
            ]
        )


class _BeginContext:
    def __init__(self, conn: _RunConnection) -> None:
        self._conn = conn

    def __enter__(self) -> _RunConnection:
        return self._conn

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> bool:
        return False


class _FakeEngine:
    def __init__(self, conn: _RunConnection) -> None:
        self._conn = conn

    def begin(self) -> _BeginContext:
        return _BeginContext(self._conn)


def _build_service() -> identifier_recovery.PgForeclosureIdentifierRecoveryService:
    service = object.__new__(
        identifier_recovery.PgForeclosureIdentifierRecoveryService,
    )
    service._available = True  # noqa: SLF001
    service._dsn = "postgresql://x"  # noqa: SLF001
    service._ori_session = None  # noqa: SLF001
    service._run_stats = {}  # noqa: SLF001
    return service


def test_load_parcel_candidate_casts_lookup_binds_to_text() -> None:
    service = _build_service()
    conn = _ParcelLookupConnection()

    candidate = service._load_parcel_candidate(  # noqa: SLF001
        conn,  # type: ignore[arg-type]
        folio="F-1",
        strap="S-1",
    )

    assert candidate is not None
    sql, params = conn.execute_calls[0]
    assert "folio = CAST(:folio AS text)" in sql
    assert "strap = CAST(:strap AS text)" in sql
    assert "WHEN folio = CAST(:folio AS text) AND strap = CAST(:strap AS text)" in sql
    assert params == {"folio": "F-1", "strap": "S-1"}


def test_run_uses_nested_transactions_to_isolate_case_failures() -> None:
    service = _build_service()
    conn = _RunConnection()
    service._engine = _FakeEngine(conn)  # noqa: SLF001

    rows = [
        {"foreclosure_id": 1, "case_number_raw": "bad"},
        {"foreclosure_id": 2, "case_number_raw": "good"},
    ]

    def _fake_load_scope_rows(_conn: Any, *, limit: int | None) -> list[dict[str, Any]]:
        _ = limit
        return rows

    service._load_scope_rows = _fake_load_scope_rows  # type: ignore[method-assign]  # noqa: SLF001

    def _fake_resolve(
        _conn: _RunConnection,
        row: dict[str, Any],
    ) -> identifier_recovery._ResolutionDecision:
        if row["case_number_raw"] == "bad":
            _conn.aborted = True
            raise RuntimeError("boom")
        return identifier_recovery._ResolutionDecision(  # noqa: SLF001
            candidate=identifier_recovery._ParcelCandidate(  # noqa: SLF001
                folio="F-2",
                strap="S-2",
                property_address="2 MAIN ST",
                legal_description="LEGAL",
                source_file_id=1,
            ),
            method="resolved_parcel_id",
            ambiguous=False,
            reason="resolved",
        )

    service._resolve_one = _fake_resolve  # type: ignore[method-assign]  # noqa: SLF001

    result = service.run()

    assert result["errors"] == 1
    assert result["rows_updated"] == 1
    assert result["resolved_parcel_id"] == 1
    assert conn.begin_nested_calls == 2
    assert conn.mark_calls == [{"foreclosure_id": 2}]
    assert conn.update_calls == [
        {
            "foreclosure_id": 2,
            "strap": "S-2",
            "folio": "F-2",
            "property_address": "2 MAIN ST",
        }
    ]


def test_run_marks_recovery_attempt_for_unresolved_rows() -> None:
    service = _build_service()
    conn = _RunConnection()
    service._engine = _FakeEngine(conn)  # noqa: SLF001

    def _fake_load_scope_rows(
        _conn: Any,
        *,
        limit: int | None,
    ) -> list[dict[str, Any]]:
        _ = limit
        return [{"foreclosure_id": 9, "case_number_raw": "unresolved"}]

    service._load_scope_rows = _fake_load_scope_rows  # type: ignore[method-assign]  # noqa: SLF001
    service._resolve_one = lambda _conn, _row: identifier_recovery._ResolutionDecision(  # type: ignore[method-assign]  # noqa: SLF001
        candidate=None,
        method=None,
        ambiguous=False,
        reason="no_match",
    )

    result = service.run()

    assert result["rows_updated"] == 0
    assert result["unresolved"] == 1
    assert conn.update_calls == []
    assert conn.mark_calls == [{"foreclosure_id": 9}]


def test_scope_sql_uses_identifier_recovery_cooldown() -> None:
    sql = identifier_recovery._SCOPE_SQL  # noqa: SLF001

    assert "step_identifier_recovery IS NULL" in sql
    assert "step_identifier_recovery < now() - INTERVAL '14 days'" in sql


def test_address_lookup_terms_strip_directionals_and_suffixes() -> None:
    house_number, street_tokens = identifier_recovery._address_lookup_terms(  # noqa: SLF001
        "123 W Crest Hill Drive, Tampa FL 33602"
    )

    assert house_number == "123"
    assert street_tokens == ["CREST", "HILL"]


def test_lookup_by_address_falls_back_to_normalized_terms() -> None:
    service = _build_service()
    conn = _AddressLookupConnection()

    candidates = service._lookup_by_address(  # noqa: SLF001
        conn,  # type: ignore[arg-type]
        address="123 W Crest Hill Drive",
    )

    assert len(candidates) == 1
    assert candidates[0].strap == "S-2"
    assert len(conn.execute_calls) == 2
    fallback_sql, fallback_params = conn.execute_calls[1]
    assert "regexp_replace(UPPER(COALESCE(property_address, '')), '[^A-Z0-9]', '', 'g')" in fallback_sql
    assert fallback_params == {
        "house_prefix": "123%",
        "limit": 60,
        "street_token_0": "%CREST%",
        "street_token_1": "%HILL%",
    }


def test_address_lookup_terms_drop_suffix_for_blaine_place_variant() -> None:
    house_number, street_tokens = identifier_recovery._address_lookup_terms(  # noqa: SLF001
        "11112 Blaine Place, Tampa, FL 33626"
    )

    assert house_number == "11112"
    assert street_tokens == ["BLAINE"]


def test_lookup_by_address_matches_blaine_place_to_blaine_top_pl() -> None:
    service = _build_service()
    conn = _BlaineAddressLookupConnection()

    candidates = service._lookup_by_address(  # noqa: SLF001
        conn,  # type: ignore[arg-type]
        address="11112 Blaine Place, Tampa, FL 33626",
    )

    assert len(candidates) == 1
    assert candidates[0].strap == "S-3"
    fallback_sql, fallback_params = conn.execute_calls[1]
    assert "regexp_replace(UPPER(COALESCE(property_address, '')), '[^A-Z0-9]', '', 'g')" in fallback_sql
    assert fallback_params == {
        "house_prefix": "11112%",
        "limit": 60,
        "street_token_0": "%BLAINE%",
    }


def test_address_head_normalizes_avenue_to_ave() -> None:
    assert identifier_recovery._address_head("3127 W SLIGH AVENUE") == "3127 W SLIGH AVE"


def test_address_head_normalizes_drive_to_dr() -> None:
    assert identifier_recovery._address_head("2303 Briana Drive, Brandon, FL 33511") == "2303 BRIANA DR"


def test_address_head_normalizes_court_to_ct() -> None:
    assert identifier_recovery._address_head("821 Luent Sands Court, Brandon, FL 33511") == "821 LUENT SANDS CT"


def test_address_head_normalizes_street_to_st() -> None:
    assert identifier_recovery._address_head("123 Main Street, Tampa") == "123 MAIN ST"


def test_address_head_normalizes_boulevard_to_blvd() -> None:
    assert identifier_recovery._address_head("456 N Dale Mabry Boulevard") == "456 N DALE MABRY BLVD"


def test_address_head_does_not_double_abbreviate() -> None:
    assert identifier_recovery._address_head("1202 E 15TH AVE, TAMPA, FL 33605") == "1202 E 15TH AVE"


def test_address_head_strips_city_state_zip_when_no_comma() -> None:
    """Address without commas should still strip city/state/zip."""
    assert identifier_recovery._address_head("1202 DESERT HILLS DR SUN CITY CENTER FL 33573") == "1202 DESERT HILLS DR"


def test_address_head_returns_none_for_empty() -> None:
    assert identifier_recovery._address_head("") is None
    assert identifier_recovery._address_head(None) is None


def test_address_with_unit_from_hash() -> None:
    """Unit numbers after # should be appended when comma splits them off."""
    assert (
        identifier_recovery._address_with_unit(
            "3127 W. Sligh Avenue, #203B, Tampa, FL 33614"
        )
        == "3127 W SLIGH AVE 203B"
    )
    assert identifier_recovery._address_with_unit("100 Main St, Tampa, FL") is None
    assert (
        identifier_recovery._address_with_unit("100 Main St #5, Tampa")
        == "100 MAIN ST 5"
    )


class _ExactAddressConnection:
    """Fake connection that returns one parcel for exact address match."""

    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, dict[str, Any] | None]] = []

    def execute(
        self,
        statement: Any,
        params: dict[str, Any] | None = None,
    ) -> _FakeResult:
        sql = str(statement)
        self.execute_calls.append((sql, params))
        if "WHERE property_address = :address" in sql:
            return _FakeResult(
                rows=[
                    {
                        "folio": "F-EXACT",
                        "strap": "S-EXACT",
                        "property_address": "2303 BRIANA DR",
                        "raw_legal1": "CUSCADEN A W",
                        "raw_legal2": "LOT 4",
                        "raw_legal3": "",
                        "raw_legal4": "",
                        "source_file_id": 99,
                    }
                ]
            )
        return _FakeResult(rows=[])


def test_lookup_by_exact_address_returns_candidate() -> None:
    service = _build_service()
    conn = _ExactAddressConnection()

    candidates = service._lookup_by_exact_address(  # noqa: SLF001
        conn,  # type: ignore[arg-type]
        address="2303 BRIANA DR",
    )

    assert len(candidates) == 1
    assert candidates[0].strap == "S-EXACT"


def test_hcpa_strap_from_a_prefix_parcel() -> None:
    result = identifier_recovery._hcpa_strap_from_segmented_parcel(
        "A-13-28-18-3C7-000004-00012.4"
    )
    assert result == "1828133C7000004000124A"


def test_hcpa_strap_from_u_prefix_parcel_unchanged() -> None:
    """Existing U-prefix behavior still works."""
    result = identifier_recovery._hcpa_strap_from_segmented_parcel(
        "U-13-28-18-3C7-000004-00012.4"
    )
    assert result == "1828133C7000004000124U"
