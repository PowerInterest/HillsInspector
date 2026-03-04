"""Tests for the web audit UI layer.

Validates:
1. Bucket metadata correctly classifies buckets into families.
2. Property-detail context includes audit_summary.
3. Property audit tab route returns expected template and grouped context.
4. Review inbox route returns expected template/context.
5. Property template contains the Audit tab and HTMX partial target.

No live PG connection required — all DB access is monkeypatched.
"""

from __future__ import annotations

import asyncio
import sys
import types
from typing import Any, Self
from pathlib import Path

from src.services.audit.web_audit_service import (
    BUCKET_META,
    FAMILY_ORDER,
    get_bucket_meta,
    group_issues_by_family,
)


# ---------------------------------------------------------------------------
# 1. Bucket metadata classification
# ---------------------------------------------------------------------------


def test_all_known_buckets_have_metadata() -> None:
    """Every bucket in BUCKET_META has a label, family, why_it_matters, badge_class."""
    for bucket, meta in BUCKET_META.items():
        assert "label" in meta, f"{bucket} missing label"
        assert "family" in meta, f"{bucket} missing family"
        assert "why_it_matters" in meta, f"{bucket} missing why_it_matters"
        assert "badge_class" in meta, f"{bucket} missing badge_class"
        assert meta["family"] in FAMILY_ORDER, f"{bucket} family '{meta['family']}' not in FAMILY_ORDER"


def test_get_bucket_meta_fallback() -> None:
    """Unknown bucket gets fallback metadata."""
    meta = get_bucket_meta("totally_unknown_bucket_xyz")
    assert meta["family"] == "Other"
    assert meta["label"] == "totally_unknown_bucket_xyz"


def test_group_issues_by_family_ordering() -> None:
    """Issues are grouped by family in FAMILY_ORDER order."""
    issues = [
        {"bucket": "a", "family": "Risk Signals"},
        {"bucket": "b", "family": "Data Coverage"},
        {"bucket": "c", "family": "Identity / Parties"},
        {"bucket": "d", "family": "Data Coverage"},
    ]
    grouped = group_issues_by_family(issues)
    families_in_order = [g["family"] for g in grouped]
    assert families_in_order == ["Data Coverage", "Identity / Parties", "Risk Signals"]
    assert len(grouped[0]["issues"]) == 2  # Data Coverage has 2


def test_group_issues_by_family_empty() -> None:
    """Empty issues list returns empty groups."""
    assert group_issues_by_family([]) == []


# ---------------------------------------------------------------------------
# Helpers for route testing
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# 2. Property-detail context includes audit_summary
# ---------------------------------------------------------------------------


def test_property_detail_includes_audit_summary(monkeypatch: Any) -> None:
    """property_detail() injects audit_summary into template context."""
    from app.web.routers import properties

    _install_fake_templates(monkeypatch)
    monkeypatch.setattr(properties, "templates", _FakeTemplates())
    monkeypatch.setattr(properties, "_pg_engine", lambda: _FakeEngine())

    fake_prop = {
        "folio": "TEST123",
        "auction": {},
        "parcel": {},
        "encumbrances": [],
        "net_equity": 0,
        "market_value": 0,
        "market": {},
        "enrichments": {},
        "_foreclosure_id": None,
        "_strap": None,
        "_folio_raw": None,
        "_case_number_raw": None,
    }
    monkeypatch.setattr(properties, "_pg_property_detail", lambda _folio: fake_prop)

    class _FakePgQueries:
        available = False
        def get_subdivision_info(self, _f: str) -> None: return None
        def is_multi_unit(self, _f: str) -> None: return None

    monkeypatch.setattr(properties, "get_pg_queries", lambda: _FakePgQueries())

    response = asyncio.run(properties.property_detail(object(), folio="TEST123"))

    ctx = response["context"]
    assert "audit_summary" in ctx
    assert ctx["audit_summary"]["has_issues"] is False


# ---------------------------------------------------------------------------
# 3. Audit tab route
# ---------------------------------------------------------------------------


def test_property_audit_route_returns_partial(monkeypatch: Any) -> None:
    """GET /property/{folio}/audit returns partials/audit.html with grouped context."""
    from app.web.routers import properties

    _install_fake_templates(monkeypatch)
    monkeypatch.setattr(properties, "templates", _FakeTemplates())
    monkeypatch.setattr(properties, "_pg_engine", lambda: _FakeEngine())

    fake_prop = {
        "_foreclosure_id": 42,
        "_strap": "TEST_STRAP",
        "_folio_raw": "1234567890",
        "_case_number_raw": "2026-CA-000001",
    }
    monkeypatch.setattr(properties, "_pg_property_detail", lambda _folio: fake_prop)
    monkeypatch.setattr(
        properties,
        "get_property_audit_snapshot",
        lambda **_kw: {
            "total_open_issues": 1,
            "has_issues": True,
            "issues": [{"bucket": "lp_missing", "family": "Data Coverage", "reason": "test"}],
            "family_counts": {"Data Coverage": 1},
            "bucket_counts": {"lp_missing": 1},
            "top_buckets": ["Missing Lis Pendens"],
        },
    )

    response = asyncio.run(properties.property_audit(object(), folio="TEST123"))

    assert response["template"] == "partials/audit.html"
    ctx = response["context"]
    assert ctx["error"] is None
    assert len(ctx["grouped_issues"]) == 1
    assert ctx["grouped_issues"][0]["family"] == "Data Coverage"
    assert ctx["snapshot"]["has_issues"] is True


# ---------------------------------------------------------------------------
# 4. Review inbox route
# ---------------------------------------------------------------------------


def test_encumbrance_audit_inbox_route(monkeypatch: Any) -> None:
    """GET /review/encumbrance-audit returns template with summary_cards and rows."""
    from app.web.routers import review

    _install_fake_templates(monkeypatch)
    monkeypatch.setattr(review, "templates", _FakeTemplates())
    monkeypatch.setattr(review, "_engine", lambda: _FakeEngine())

    fake_inbox = {
        "summary_cards": {
            "open_issues": 5,
            "affected_foreclosures": 3,
            "top_bucket": "Missing Lis Pendens",
            "data_coverage_count": 4,
        },
        "bucket_summaries": [
            {"bucket": "lp_missing", "label": "Missing LP", "family": "Data Coverage", "count": 4,
             "description": "test", "deferred": False, "deferred_reason": None, "badge_class": "badge-warning"},
        ],
        "rows": [
            {"foreclosure_id": 1, "property_address": "123 Main St", "case_number": "2026-CA-001",
             "strap": "STRAP1", "bucket": "lp_missing", "label": "Missing LP",
             "family": "Data Coverage", "reason": "No LP found", "badge_class": "badge-warning",
             "why_it_matters": "test"},
        ],
    }
    monkeypatch.setattr(review, "get_encumbrance_audit_inbox", lambda **_kw: fake_inbox)

    response = asyncio.run(review.encumbrance_audit(object()))

    assert response["template"] == "review/encumbrance_audit.html"
    ctx = response["context"]
    assert ctx["summary_cards"]["open_issues"] == 5
    assert len(ctx["rows"]) == 1
    assert ctx["error"] is None


def test_encumbrance_audit_inbox_filters(monkeypatch: Any) -> None:
    """Inbox server-side filters narrow rows."""
    from app.web.routers import review

    _install_fake_templates(monkeypatch)
    monkeypatch.setattr(review, "templates", _FakeTemplates())
    monkeypatch.setattr(review, "_engine", lambda: _FakeEngine())

    fake_inbox = {
        "summary_cards": {"open_issues": 2, "affected_foreclosures": 2, "top_bucket": "X", "data_coverage_count": 1},
        "bucket_summaries": [],
        "rows": [
            {"foreclosure_id": 1, "property_address": "123 Main", "case_number": "CA-001",
             "strap": "S1", "bucket": "lp_missing", "label": "LP", "family": "Data Coverage",
             "reason": "No LP", "badge_class": "badge-warning", "why_it_matters": "t"},
            {"foreclosure_id": 2, "property_address": "456 Oak", "case_number": "CA-002",
             "strap": "S2", "bucket": "sat_parent_gap", "label": "Sat", "family": "Data Coverage",
             "reason": "Orphan sat", "badge_class": "badge-warning", "why_it_matters": "t"},
        ],
    }
    monkeypatch.setattr(review, "get_encumbrance_audit_inbox", lambda **_kw: fake_inbox)

    response = asyncio.run(review.encumbrance_audit(object(), bucket="lp_missing"))
    assert len(response["context"]["rows"]) == 1
    assert response["context"]["rows"][0]["bucket"] == "lp_missing"


# ---------------------------------------------------------------------------
# 5. Property template contains Audit tab
# ---------------------------------------------------------------------------


def test_property_template_has_audit_tab() -> None:
    """property.html contains the Audit tab button and HTMX target div."""
    template_path = Path("app/web/templates/property.html")
    content = template_path.read_text()
    assert "openTab(event, 'audit')" in content
    assert 'id="audit"' in content
    assert "/audit" in content
    assert "hx-trigger" in content
