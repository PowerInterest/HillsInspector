"""Tests for the PG encumbrance audit tool.

These tests use in-memory fakes instead of a live database, following the
same pattern as ``test_pg_trust_accounts.py``.  Each test injects a fake
connection whose ``execute()`` returns canned result sets that exercise the
bucket logic.
"""

from __future__ import annotations

import json
from typing import Any

from src.tools.pg_encumbrance_audit import (
    AuditReport,
    BucketHit,
    BucketSummary,
    format_console,
    format_csv,
    format_json,
    run_audit,
)


# ---------------------------------------------------------------------------
# Fake DB helpers (mirrors test_pg_trust_accounts.py pattern)
# ---------------------------------------------------------------------------

class _FakeMapping(dict):
    """dict subclass returned by .mappings()."""


class _FakeResult:
    def __init__(
        self,
        *,
        rows: list[dict[str, Any]] | None = None,
        scalar_value: Any = None,
    ) -> None:
        self._rows = rows or []
        self._scalar_value = scalar_value

    def mappings(self) -> _FakeResult:
        return self

    def all(self) -> list[_FakeMapping]:
        return [_FakeMapping(r) for r in self._rows]

    def scalar(self) -> Any:
        return self._scalar_value


class _FakeConnection:
    """Programmable fake SQLAlchemy connection.

    *dispatch* is a callable ``(sql_text, params) -> _FakeResult``.  If not
    given, the default returns scalar=0 and empty row lists.
    """

    def __init__(self, dispatch: Any | None = None) -> None:
        self.calls: list[tuple[str, dict[str, Any] | None]] = []
        self._dispatch = dispatch or self._default_dispatch

    @staticmethod
    def _default_dispatch(_sql: str, _params: dict[str, Any] | None) -> _FakeResult:
        return _FakeResult(scalar_value=0)

    def execute(self, statement: Any, params: dict[str, Any] | None = None) -> _FakeResult:
        sql = str(statement)
        self.calls.append((sql, params))
        return self._dispatch(sql, params)

    def rollback(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _scope_dispatch(
    active: int = 10,
    judged: int = 8,
    with_strap: int = 7,
    with_enc: int = 5,
    bucket_rows: dict[str, list[dict[str, Any]]] | None = None,
) -> Any:
    """Build a dispatch function that returns scope counts and bucket rows.

    Bucket identification uses a serial order: the bucket queries are always
    dispatched in the same deterministic order (BUCKET_DEFINITIONS), so we
    assign a counter to non-scope queries and map each ordinal to the
    bucket name.
    """
    bucket_rows = bucket_rows or {}

    # The bucket handler order mirrors BUCKET_DEFINITIONS in the tool
    _BUCKET_ORDER = [
        "lp_missing",
        "foreclosing_lien_missing",
        "plaintiff_chain_gap",
        "cc_lien_gap",
        "construction_lien_risk",
        "sat_parent_gap",
        "superpriority_non_ori_risk",
        "historical_window_gap",
        "lifecycle_base_gap",
    ]
    counter = {"n": 0}

    def dispatch(sql: str, _params: dict[str, Any] | None) -> _FakeResult:
        sql_upper = sql.upper().strip()

        # Scope queries are simple one-table SELECTs (no GROUP BY).
        # The with_enc_count query does have a JOIN but has COUNT(DISTINCT
        # and no GROUP BY.
        has_group_by = "GROUP BY" in sql_upper

        # --- Scope counts (ORDER MATTERS — most specific first) ---
        if "INFORMATION_SCHEMA" in sql_upper:
            return _FakeResult(scalar_value=1)  # table exists
        if "COUNT(DISTINCT" in sql_upper and not has_group_by:
            return _FakeResult(scalar_value=with_enc)
        if not has_group_by and "STRAP" in sql_upper and "JUDGMENT_DATA" in sql_upper and sql_upper.startswith("SELECT COUNT(*)"):
            return _FakeResult(scalar_value=with_strap)
        if not has_group_by and "JUDGMENT_DATA IS NOT NULL" in sql_upper and sql_upper.startswith("SELECT COUNT(*)"):
            return _FakeResult(scalar_value=judged)
        if not has_group_by and "ARCHIVED_AT IS NULL" in sql_upper and sql_upper.startswith("SELECT COUNT(*)"):
            return _FakeResult(scalar_value=active)

        # --- Bucket queries: dispatched in order ---
        idx = counter["n"]
        counter["n"] += 1
        if idx < len(_BUCKET_ORDER):
            bucket_name = _BUCKET_ORDER[idx]
            return _FakeResult(rows=bucket_rows.get(bucket_name, []))

        return _FakeResult(scalar_value=0)

    return dispatch


def _hit(bucket: str, fid: int = 1, case: str = "24-CA-000001") -> dict[str, Any]:
    """Convenience: a single fake row matching the SELECT shape of bucket queries."""
    return {
        "foreclosure_id": fid,
        "case_number": case,
        "strap": "STRAP001",
        "property_address": "123 Test St",
        # Extra columns expected by specific buckets
        "clerk_case_type": "CA",
        "plaintiff": "BANK OF TEST",
        "signal_source": "NOC recorded",
        "sat_instrument": "2024000111",
        "sat_type": "satisfaction",
        "total_enc": 3,
        "historical_enc": 3,
        "lifecycle_type": "assignment",
        "lifecycle_instrument": "2024000222",
        "risk_signal": "Tampa Accela violation/open record",
    }


# ---------------------------------------------------------------------------
# Tests — scope metrics
# ---------------------------------------------------------------------------

class TestScopeMetrics:
    def test_empty_database(self) -> None:
        conn = _FakeConnection(_scope_dispatch(active=0, judged=0, with_strap=0, with_enc=0))
        report = run_audit(conn=conn)
        assert report.active_count == 0
        assert report.judged_count == 0

    def test_scope_counts_populated(self) -> None:
        conn = _FakeConnection(_scope_dispatch(active=50, judged=40, with_strap=35, with_enc=30))
        report = run_audit(conn=conn)
        assert report.active_count == 50
        assert report.judged_count == 40
        assert report.with_strap_count == 35
        assert report.with_encumbrances_count == 30


# ---------------------------------------------------------------------------
# Tests — individual buckets
# ---------------------------------------------------------------------------

class TestBucketLpMissing:
    def test_no_hits(self) -> None:
        conn = _FakeConnection(_scope_dispatch())
        report = run_audit(conn=conn)
        s = next(s for s in report.summaries if s.bucket == "lp_missing")
        assert s.count == 0

    def test_with_hits(self) -> None:
        conn = _FakeConnection(_scope_dispatch(
            bucket_rows={"lp_missing": [_hit("lp_missing", fid=1), _hit("lp_missing", fid=2)]},
        ))
        report = run_audit(conn=conn)
        s = next(s for s in report.summaries if s.bucket == "lp_missing")
        assert s.count == 2
        lp_hits = [h for h in report.hits if h.bucket == "lp_missing"]
        assert len(lp_hits) == 2
        assert lp_hits[0].reason == "No lis pendens found in ori_encumbrances or title events"


class TestBucketForeclosingLienMissing:
    def test_with_hit(self) -> None:
        row = _hit("foreclosing_lien_missing")
        conn = _FakeConnection(_scope_dispatch(
            bucket_rows={"foreclosing_lien_missing": [row]},
        ))
        report = run_audit(conn=conn)
        s = next(s for s in report.summaries if s.bucket == "foreclosing_lien_missing")
        assert s.count == 1
        h = report.hits[0]
        assert h.bucket == "foreclosing_lien_missing"
        assert "base encumbrance" in h.reason


class TestBucketPlaintiffChainGap:
    def test_with_hit(self) -> None:
        row = _hit("plaintiff_chain_gap")
        conn = _FakeConnection(_scope_dispatch(
            bucket_rows={"plaintiff_chain_gap": [row]},
        ))
        report = run_audit(conn=conn)
        s = next(s for s in report.summaries if s.bucket == "plaintiff_chain_gap")
        assert s.count == 1
        h = next(h for h in report.hits if h.bucket == "plaintiff_chain_gap")
        assert "BANK OF TEST" in h.reason


class TestBucketCcLienGap:
    def test_with_hit(self) -> None:
        row = _hit("cc_lien_gap")
        conn = _FakeConnection(_scope_dispatch(
            bucket_rows={"cc_lien_gap": [row]},
        ))
        report = run_audit(conn=conn)
        s = next(s for s in report.summaries if s.bucket == "cc_lien_gap")
        assert s.count == 1


class TestBucketConstructionLienRisk:
    def test_with_hit(self) -> None:
        row = _hit("construction_lien_risk")
        conn = _FakeConnection(_scope_dispatch(
            bucket_rows={"construction_lien_risk": [row]},
        ))
        report = run_audit(conn=conn)
        s = next(s for s in report.summaries if s.bucket == "construction_lien_risk")
        assert s.count == 1
        h = next(h for h in report.hits if h.bucket == "construction_lien_risk")
        assert "NOC" in h.reason or "permit" in h.reason.lower()


class TestBucketSatParentGap:
    def test_with_hit(self) -> None:
        row = _hit("sat_parent_gap")
        conn = _FakeConnection(_scope_dispatch(
            bucket_rows={"sat_parent_gap": [row]},
        ))
        report = run_audit(conn=conn)
        s = next(s for s in report.summaries if s.bucket == "sat_parent_gap")
        assert s.count == 1
        h = next(h for h in report.hits if h.bucket == "sat_parent_gap")
        assert "satisfaction" in h.reason


class TestBucketSuperpriority:
    def test_with_hit(self) -> None:
        row = _hit("superpriority_non_ori_risk")
        conn = _FakeConnection(_scope_dispatch(
            bucket_rows={"superpriority_non_ori_risk": [row]},
        ))
        report = run_audit(conn=conn)
        s = next(s for s in report.summaries if s.bucket == "superpriority_non_ori_risk")
        assert s.count == 1


class TestBucketHistoricalWindowGap:
    def test_with_hit(self) -> None:
        row = _hit("historical_window_gap")
        conn = _FakeConnection(_scope_dispatch(
            bucket_rows={"historical_window_gap": [row]},
        ))
        report = run_audit(conn=conn)
        s = next(s for s in report.summaries if s.bucket == "historical_window_gap")
        assert s.count == 1
        h = next(h for h in report.hits if h.bucket == "historical_window_gap")
        assert "HISTORICAL" in h.reason


class TestBucketLifecycleBaseGap:
    def test_with_hit(self) -> None:
        row = _hit("lifecycle_base_gap")
        conn = _FakeConnection(_scope_dispatch(
            bucket_rows={"lifecycle_base_gap": [row]},
        ))
        report = run_audit(conn=conn)
        s = next(s for s in report.summaries if s.bucket == "lifecycle_base_gap")
        assert s.count == 1
        h = next(h for h in report.hits if h.bucket == "lifecycle_base_gap")
        assert "assignment" in h.reason


# ---------------------------------------------------------------------------
# Tests — multi-bucket interaction
# ---------------------------------------------------------------------------

class TestMultiBucket:
    def test_multiple_buckets_fire(self) -> None:
        conn = _FakeConnection(_scope_dispatch(
            active=20,
            judged=15,
            bucket_rows={
                "lp_missing": [_hit("lp_missing", fid=1)],
                "cc_lien_gap": [_hit("cc_lien_gap", fid=2)],
                "sat_parent_gap": [_hit("sat_parent_gap", fid=3)],
            },
        ))
        report = run_audit(conn=conn)
        assert len(report.hits) == 3
        bucket_names = {h.bucket for h in report.hits}
        assert bucket_names == {"lp_missing", "cc_lien_gap", "sat_parent_gap"}

    def test_same_foreclosure_in_multiple_buckets(self) -> None:
        """A single foreclosure can appear in more than one bucket."""
        shared = _hit("shared", fid=99, case="24-CA-099099")
        conn = _FakeConnection(_scope_dispatch(
            bucket_rows={
                "lp_missing": [shared],
                "foreclosing_lien_missing": [shared],
            },
        ))
        report = run_audit(conn=conn)
        fid99_buckets = {h.bucket for h in report.hits if h.foreclosure_id == 99}
        assert len(fid99_buckets) == 2


# ---------------------------------------------------------------------------
# Tests — output formatters
# ---------------------------------------------------------------------------

class TestFormatConsole:
    def test_contains_header(self) -> None:
        report = AuditReport(active_count=10, judged_count=8, with_strap_count=7, with_encumbrances_count=5)
        text = format_console(report)
        assert "ENCUMBRANCE AUDIT" in text
        assert "10" in text

    def test_deferred_bucket_shown(self) -> None:
        report = AuditReport(
            active_count=1, judged_count=1, with_strap_count=1, with_encumbrances_count=1,
            summaries=[
                BucketSummary(bucket="test_bucket", description="Test", count=0, deferred=True, deferred_reason="No data"),
            ],
        )
        text = format_console(report)
        assert "DEFERRED" in text
        assert "No data" in text


class TestFormatJson:
    def test_valid_json(self) -> None:
        report = AuditReport(
            active_count=5, judged_count=4, with_strap_count=3, with_encumbrances_count=2,
            summaries=[BucketSummary(bucket="b1", description="d1", count=1)],
            hits=[BucketHit(bucket="b1", foreclosure_id=1, case_number="24-CA-000001", strap="S1", property_address="Addr", reason="r")],
        )
        text = format_json(report)
        parsed = json.loads(text)
        assert parsed["scope"]["active_count"] == 5
        assert len(parsed["summaries"]) == 1
        assert len(parsed["hits"]) == 1

    def test_empty_report(self) -> None:
        report = AuditReport(active_count=0, judged_count=0, with_strap_count=0, with_encumbrances_count=0)
        parsed = json.loads(format_json(report))
        assert parsed["hits"] == []


class TestFormatCsv:
    def test_csv_headers(self) -> None:
        report = AuditReport(active_count=1, judged_count=1, with_strap_count=1, with_encumbrances_count=1)
        csv_text = format_csv(report)
        assert "bucket,foreclosure_id,case_number,strap,property_address,reason" in csv_text

    def test_csv_with_hits(self) -> None:
        report = AuditReport(
            active_count=1, judged_count=1, with_strap_count=1, with_encumbrances_count=1,
            hits=[BucketHit(bucket="lp_missing", foreclosure_id=7, case_number="24-CA-000007", strap="S7", property_address="7 St", reason="test reason")],
        )
        csv_text = format_csv(report)
        assert "lp_missing" in csv_text
        assert "24-CA-000007" in csv_text
        assert "test reason" in csv_text


# ---------------------------------------------------------------------------
# Tests — error handling
# ---------------------------------------------------------------------------

class TestErrorHandling:
    def test_query_error_marks_bucket_deferred(self) -> None:
        """If a bucket SQL query fails, the bucket is deferred."""
        call_count = 0

        def exploding_dispatch(sql: str, _params: Any) -> _FakeResult:
            nonlocal call_count
            sql_upper = sql.upper().strip()
            if "INFORMATION_SCHEMA" in sql_upper:
                return _FakeResult(scalar_value=1)
            if "GROUP BY" not in sql_upper and sql_upper.startswith("SELECT COUNT"):
                return _FakeResult(scalar_value=5)
            if "COUNT(DISTINCT" in sql_upper:
                return _FakeResult(scalar_value=5)
            # Make the first bucket query blow up
            call_count += 1
            if call_count == 1:
                raise RuntimeError("simulated DB failure")
            return _FakeResult(rows=[])

        conn = _FakeConnection(exploding_dispatch)
        report = run_audit(conn=conn)
        s = next(s for s in report.summaries if s.bucket == "lp_missing")
        assert s.count == 0
        assert s.deferred is True
        assert "Bucket error" in (s.deferred_reason or "")

    def test_handler_exception_marks_deferred(self) -> None:
        """If the bucket handler itself raises (not the query helper),
        the bucket is marked deferred with the error message."""
        import src.tools.pg_encumbrance_audit as audit_mod

        # Temporarily replace one bucket handler with one that raises
        original_defs = audit_mod.BUCKET_DEFINITIONS[:]
        try:
            def _bad_handler(_conn: Any) -> list[Any]:
                raise ValueError("deliberate handler failure")

            audit_mod.BUCKET_DEFINITIONS[0] = {
                **audit_mod.BUCKET_DEFINITIONS[0],
                "handler": _bad_handler,
            }
            conn = _FakeConnection(_scope_dispatch())
            report = run_audit(conn=conn)
            s = next(s for s in report.summaries if s.bucket == "lp_missing")
            assert s.deferred is True
            assert "Bucket error" in (s.deferred_reason or "")
        finally:
            audit_mod.BUCKET_DEFINITIONS[:] = original_defs


# ---------------------------------------------------------------------------
# Tests — data model
# ---------------------------------------------------------------------------

class TestDataModel:
    def test_bucket_hit_fields(self) -> None:
        h = BucketHit(
            bucket="test",
            foreclosure_id=42,
            case_number="24-CA-042042",
            strap="STRAP42",
            property_address="42 Main St",
            reason="Testing",
        )
        assert h.bucket == "test"
        assert h.foreclosure_id == 42

    def test_bucket_summary_defaults(self) -> None:
        s = BucketSummary(bucket="b", description="d", count=0)
        assert s.deferred is False
        assert s.deferred_reason is None

    def test_audit_report_defaults(self) -> None:
        r = AuditReport(active_count=0, judged_count=0, with_strap_count=0, with_encumbrances_count=0)
        assert r.summaries == []
        assert r.hits == []
