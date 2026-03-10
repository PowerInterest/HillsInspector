from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from src.services.TempleTerracePermit import TempleTerracePermitService


def test_sync_address_to_postgres_keeps_search_row_when_detail_fetch_fails(
    monkeypatch: Any,
) -> None:
    service = object.__new__(TempleTerracePermitService)
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        service,
        "_request",
        lambda *_args, **_kwargs: SimpleNamespace(
            text="<html></html>",
            url="https://example.com/search",
        ),
    )
    monkeypatch.setattr(service, "_extract_owasp_token", lambda *_args, **_kwargs: "token")
    monkeypatch.setattr(service, "_build_search_payload", lambda *_args, **_kwargs: {"query": "value"})
    monkeypatch.setattr(
        service,
        "_extract_search_rows",
        lambda *_args, **_kwargs: [
            {
                "application_number": "TT-25-0001",
                "address": "8301 N 56TH ST, Temple Terrace, FL 33617",
                "parcel_id": "P-1",
                "detail_url": "https://example.com/detail/TT-25-0001",
            }
        ],
    )
    monkeypatch.setattr(
        service,
        "_fetch_detail_fields",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("detail failed")),
    )
    def _capture_rows(rows: list[dict[str, Any]], **_kwargs: Any) -> int:
        captured["rows"] = rows
        return len(rows)

    monkeypatch.setattr(
        service,
        "_upsert_rows",
        _capture_rows,
    )

    stats = service.sync_address_to_postgres("8301 N 56TH ST", max_rows=5)

    assert stats["records_observed"] == 1
    assert stats["records_normalized"] == 1
    assert stats["written"] == 1
    assert stats["detail_errors"] == 1
    assert captured["rows"][0]["record_number"] == "TEMPLETERRACE:TT-25-0001"
    assert captured["rows"][0]["detail_url"] == "https://example.com/detail/TT-25-0001"
