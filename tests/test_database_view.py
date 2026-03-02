from __future__ import annotations

import asyncio
import sys
import types
from typing import Any
from typing import Self

from app.web.routers import database_view


class _FakeConnection:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class _FakeEngine:
    def connect(self) -> _FakeConnection:
        return _FakeConnection()


class _FakeTemplates:
    def template_response(
        self,
        template: str,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        return {"template": template, "context": context}

    def __getattr__(self, name: str) -> Any:
        if name == "TemplateResponse":
            return self.template_response
        raise AttributeError(name)


def _install_fake_templates(monkeypatch: Any) -> None:
    fake_main = types.ModuleType("app.web.main")
    fake_main.templates = _FakeTemplates()
    monkeypatch.setitem(sys.modules, "app.web.main", fake_main)


def test_person_search_uses_live_schema_columns(monkeypatch: Any) -> None:
    captured: dict[str, str] = {}

    monkeypatch.setattr(database_view, "_pg_engine", lambda: _FakeEngine())
    _install_fake_templates(monkeypatch)

    def fake_safe_query(
        _conn: object,
        label: str,
        sql: str,
        _params: dict[str, object],
    ) -> list[dict[str, object]]:
        captured[label] = sql
        return []

    monkeypatch.setattr(database_view, "_safe_query", fake_safe_query)

    response = asyncio.run(database_view.person_search(object(), name="Jane Doe"))

    assert response["template"] == "partials/person_search_results.html"
    corporate_sql = captured["Corporate Roles"].lower()
    assert "sef.status as entity_status" in corporate_sql
    assert "sef.entity_status" not in corporate_sql

    trust_sql = captured["Trust Accounts"].lower()
    assert "plaintiff_name as party_name" in trust_sql
    assert "movement_type as transaction_type" in trust_sql
    assert "party_name ilike" not in trust_sql

    sales_sql = captured["Sales History"].lower()
    assert "left join hcpa_bulk_parcels" in sales_sql
    assert "strap" in sales_sql


def test_property_search_uses_live_schema_columns(monkeypatch: Any) -> None:
    captured: dict[str, str] = {}

    monkeypatch.setattr(database_view, "_pg_engine", lambda: _FakeEngine())
    monkeypatch.setattr(
        database_view,
        "_resolve_property_targets",
        lambda _conn, _query: {
            "folios": ["1234567890"],
            "folio_clean": ["1234567890"],
            "straps": ["1828133CU000000000440A"],
            "addresses": ["307 E ALTHEA AVE"],
            "case_numbers": ["2026-CA-000001"],
        },
    )
    _install_fake_templates(monkeypatch)

    def fake_safe_query(
        _conn: object,
        label: str,
        sql: str,
        _params: dict[str, object],
    ) -> list[dict[str, object]]:
        captured[label] = sql
        return []

    monkeypatch.setattr(database_view, "_safe_query", fake_safe_query)

    response = asyncio.run(
        database_view.property_search(object(), identifier="2026-CA-000001")
    )

    assert response["template"] == "partials/property_search_results.html"

    parcel_sql = captured["Parcel Info"].lower()
    assert "nal.homestead_exempt" in parcel_sql
    assert "homestead_flag" in parcel_sql

    tax_sql = captured["Tax Info"].lower()
    assert "assessed_value_school" in tax_sql
    assert "taxable_value_nonschool" in tax_sql
    assert "land_value" not in tax_sql

    market_sql = captured["Market Data"].lower()
    assert "rent_zestimate as rental_zestimate" in market_sql
    assert "sqft as living_area" in market_sql
    assert "updated_at as fetched_at" in market_sql
    assert "coalesce(hp.property_address, f.property_address) as address" in market_sql

    permits_county_sql = captured["Permits (County)"].lower()
    assert "cp.status" in permits_county_sql
    assert "cp.complete_date as end_date" in permits_county_sql
    assert "permit_status" not in permits_county_sql
    assert "job_value" not in permits_county_sql

    permits_tampa_sql = captured["Permits (Tampa)"].lower()
    assert "tr.record_number" in permits_tampa_sql
    assert "tr.address_raw as address" in permits_tampa_sql
    assert "record_id" not in permits_tampa_sql
    assert "opened_date" not in permits_tampa_sql

    title_sql = captured["Title Chain"].lower()
    assert "acquired_date" in title_sql
    assert "link_status" in title_sql
    assert "recording_date" not in title_sql

    events_sql = captured["Clerk Events"].lower()
    assert "event_source" in events_sql
    assert "event_subtype" in events_sql
    assert "event_type" not in events_sql
