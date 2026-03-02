from __future__ import annotations

from types import TracebackType
from types import SimpleNamespace
from typing import Any

from src.services.pg_permit_single_pin_service import HCPA_SINGLE_PIN_LAYER_ID
from src.services.pg_permit_single_pin_service import PgPermitSinglePinService


class _FakeConn:
    def __init__(self, rowcounts: list[int]) -> None:
        self._rowcounts = list(rowcounts)
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(self, sql: Any, params: dict[str, Any]) -> Any:
        self.calls.append((str(sql), params))
        rowcount = self._rowcounts.pop(0) if self._rowcounts else 0
        return SimpleNamespace(rowcount=rowcount)


class _FakeBegin:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def __enter__(self) -> _FakeConn:
        return self._conn

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None


class _FakeEngine:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def begin(self) -> _FakeBegin:
        return _FakeBegin(self._conn)


def _build_service(payload: dict[str, Any], rowcounts: list[int]) -> tuple[PgPermitSinglePinService, _FakeConn]:
    conn = _FakeConn(rowcounts)
    service = object.__new__(PgPermitSinglePinService)
    object.__setattr__(service, "_engine", _FakeEngine(conn))
    object.__setattr__(
        service,
        "fetcher",
        SimpleNamespace(
            fetch_pin_permits=lambda *_args, **_kwargs: payload,
        ),
    )
    return service, conn


def test_sync_pin_to_postgres_inserts_hcpa_county_row_when_unmatched() -> None:
    payload = {
        "parcel_context": {
            "folio": "0152190100",
            "site_address": "17064 COMUNIDAD DE AVILA, LUTZ",
        },
        "permits": [
            {
                "permit_number": "HC-BTR-22-0088338",
                "description": "4.0 TON HEAT PUMP SYSTEM",
                "issue_date": "2022-01-05",
                "estimated_value": 4200.0,
                "permit_url": (
                    "https://aca-prod.accela.com/HCFL/Cap/GlobalSearchResults.aspx"
                    "?isNewQuery=yes&QueryText=HC-BTR-22-0088338"
                ),
                "permit_type_code": "Z",
                "property_type_code": "R",
                "source_guess": "hcfl",
                "source_row_id": 1059083,
                "arcgis": {"matches": [], "error": None},
                "accela": {
                    "detail_url": None,
                    "detail_extract": None,
                    "search_extract": None,
                    "error": None,
                },
            }
        ],
    }
    service, conn = _build_service(payload, rowcounts=[0, 1])

    stats = service.sync_pin_to_postgres("1827230KS000000000194U")

    assert stats["permit_count"] == 1
    assert stats["county_backfill_updates"] == 0
    assert stats["county_hcpa_upserts"] == 1
    assert stats["tampa_upserts"] == 0
    assert stats["total_writes"] == 1
    assert stats["permits_with_any_write"] == 1
    assert len(conn.calls) == 2

    insert_sql, insert_params = conn.calls[1]
    assert "INSERT INTO county_permits" in insert_sql
    assert insert_params["permit_number"] == "HC-BTR-22-0088338"
    assert insert_params["source_layer_id"] == HCPA_SINGLE_PIN_LAYER_ID
    assert insert_params["source_object_id"] == 1059083
    assert insert_params["folio_clean"] == "0152190100"
    assert insert_params["address"] == "17064 COMUNIDAD DE AVILA, LUTZ"
    assert insert_params["city"] == "LUTZ"
    assert insert_params["category"] == "HCPA_SINGLE_PIN"


def test_sync_pin_to_postgres_keeps_tampa_records_out_of_county_fallback() -> None:
    payload = {
        "parcel_context": {
            "folio": "0123456789",
            "site_address": "123 MAIN ST, TAMPA",
        },
        "permits": [
            {
                "permit_number": "BLD-25-0513202",
                "description": "BUILDING PERMIT",
                "issue_date": "2025-01-05",
                "estimated_value": 10000.0,
                "permit_url": (
                    "https://aca-prod.accela.com/TAMPA/Cap/GlobalSearchResults.aspx"
                    "?isNewQuery=yes&QueryText=BLD-25-0513202"
                ),
                "permit_type_code": "BLD",
                "property_type_code": "R",
                "source_guess": "tampa",
                "source_row_id": 555,
                "arcgis": {"matches": [], "error": None},
                "accela": {
                    "detail_url": "https://aca-prod.accela.com/TAMPA/Cap/CapDetail.aspx?foo=bar",
                    "detail_extract": {"status": "Issued"},
                    "search_extract": {"status": "Issued"},
                    "error": None,
                },
            }
        ],
    }
    service, conn = _build_service(payload, rowcounts=[0, 1])

    stats = service.sync_pin_to_postgres("1234567890")

    assert stats["county_backfill_updates"] == 0
    assert stats["county_hcpa_upserts"] == 0
    assert stats["tampa_upserts"] == 1
    assert stats["total_writes"] == 1
    assert len(conn.calls) == 2

    _, second_params = conn.calls[1]
    assert second_params["record_number"] == "BLD-25-0513202"
