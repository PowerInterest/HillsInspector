from __future__ import annotations

from typing import Any
from typing import Self

from src.services.audit.pg_audit_encumbrance import AuditReport, BucketHit, BucketSummary
from src.tools import db_audit
from src.tools.db_audit import _count_existing_paths


def test_count_existing_paths_counts_only_files_present(tmp_path) -> None:
    present = tmp_path / "judgment.pdf"
    present.write_text("pdf", encoding="utf-8")

    missing = tmp_path / "missing.pdf"

    assert _count_existing_paths([str(present), str(missing), None, ""]) == 1


def test_market_photo_metrics_uses_canonical_photo_status_query(monkeypatch: Any) -> None:
    captured: dict[str, str] = {}

    def fake_row(_conn: object, query: str, params: dict | None = None) -> dict[str, int]:
        del params
        captured["query"] = query
        return {
            "has_remote": 11,
            "has_local": 9,
            "fully_missing": 4,
            "needs_backfill": 2,
            "total_market_rows": 15,
        }

    monkeypatch.setattr(db_audit, "_row", fake_row)

    metrics = db_audit._market_photo_metrics(object())  # noqa: SLF001

    assert metrics == {
        "has_remote": 11,
        "has_local": 9,
        "fully_missing": 4,
        "needs_backfill": 2,
        "total_market_rows": 15,
    }
    query = captured["query"].lower()
    assert "photo_cdn_urls" in query
    assert "photo_local_paths" in query
    assert "needs_backfill" in query


class _FakeConnection:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class _FakeEngine:
    def connect(self) -> _FakeConnection:
        return _FakeConnection()


def test_audit_database_logs_photo_metrics(monkeypatch: Any) -> None:
    messages: list[str] = []

    monkeypatch.setattr(db_audit, "resolve_pg_dsn", lambda _dsn: "postgresql://db")
    monkeypatch.setattr(db_audit, "get_engine", lambda _dsn: _FakeEngine())
    monkeypatch.setattr(
        db_audit,
        "_has_table",
        lambda _conn, name: name == "property_market",
    )
    monkeypatch.setattr(
        db_audit,
        "_market_photo_metrics",
        lambda _conn: {
            "has_remote": 11,
            "has_local": 9,
            "fully_missing": 4,
            "needs_backfill": 2,
            "total_market_rows": 15,
        },
    )

    def fake_val(_conn: object, query: str, params: dict | None = None, default: int = 0) -> int:
        del params
        q = query.lower()
        if "count(*) from property_market where zestimate is not null" in q:
            return 7
        if "count(*) from foreclosures where archived_at is null" in q:
            return 0
        if "count(*) from foreclosures" in q:
            return 0
        return default

    monkeypatch.setattr(db_audit, "_val", fake_val)
    monkeypatch.setattr(db_audit, "_rows", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(db_audit, "_row", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(db_audit.logger, "info", lambda message: messages.append(str(message)))
    monkeypatch.setattr(db_audit.logger, "warning", lambda message: messages.append(str(message)))

    db_audit.audit_database()

    joined = "\n".join(messages)
    assert "Remote photos available: 11" in joined
    assert "Local photos cached:    9" in joined
    assert "Fully missing photos:   4" in joined
    assert "Needs photo backfill:   2" in joined


def test_encumbrance_audit_reporting_metrics(monkeypatch: Any) -> None:
    report = AuditReport(
        active_count=20,
        judged_count=18,
        with_strap_count=15,
        with_encumbrances_count=12,
        with_survival_count=9,
        summaries=[
            BucketSummary(bucket="lp_missing", description="x", count=2),
            BucketSummary(bucket="lp_to_judgment_property_change", description="x", count=1),
        ],
        hits=[
            BucketHit(
                bucket="lp_missing",
                foreclosure_id=10,
                case_number="24-CA-000010",
                strap="S10",
                property_address="10 Main St",
                reason="missing lp",
            ),
            BucketHit(
                bucket="lp_to_judgment_property_change",
                foreclosure_id=11,
                case_number="24-CA-000011",
                strap="S11",
                property_address="11 Main St",
                reason="changed property",
            ),
        ],
    )

    monkeypatch.setattr(db_audit, "run_audit", lambda **_kwargs: report)
    metrics = db_audit._encumbrance_audit_reporting(object())  # noqa: SLF001

    assert metrics["open_issues"] == 2
    assert metrics["recoverable_open_issues"] == 1
    assert metrics["review_only_open_issues"] == 1
    assert metrics["with_strap_count"] == 15
    assert metrics["with_encumbrances_count"] == 12
    assert metrics["with_survival_count"] == 9
    assert metrics["encumbrance_coverage_pct"] == 80.0
    assert metrics["survival_coverage_pct"] == 60.0
    assert metrics["encumbrance_target_met"] is True
    assert metrics["survival_target_met"] is False
