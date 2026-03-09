"""Regression tests for the title-chain gap metric (Issue #1).

These tests validate the Python semantics used to derive gap classifications
and summary status, then assert the generated SQL references the same shared
gap predicate. That gives us behavioral coverage without needing a live
PostgreSQL fixture for the full title-chain pipeline.
"""

from __future__ import annotations

from src.services.pg_title_chain_controller import TitleChainController


def test_is_gap_status_flags_missing_party_and_chained_by_folio() -> None:
    assert TitleChainController.is_gap_status("MISSING_PARTY") is True
    assert TitleChainController.is_gap_status("CHAINED_BY_FOLIO") is True


def test_is_gap_status_does_not_flag_linked_or_root_statuses() -> None:
    assert TitleChainController.is_gap_status("ROOT") is False
    assert TitleChainController.is_gap_status("LINKED_EXACT") is False
    assert TitleChainController.is_gap_status("LINKED_FUZZY") is False
    assert TitleChainController.is_gap_status(None) is False


def test_summarize_chain_status_prefers_missing_folio() -> None:
    status = TitleChainController.summarize_chain_status(
        folio=None,
        sale_events_count=10,
        gap_count=3,
    )
    assert status == "MISSING_FOLIO"


def test_summarize_chain_status_marks_no_sales_before_gap_logic() -> None:
    status = TitleChainController.summarize_chain_status(
        folio="123456",
        sale_events_count=0,
        gap_count=4,
    )
    assert status == "NO_SALES"


def test_summarize_chain_status_marks_complete_when_gap_count_zero() -> None:
    status = TitleChainController.summarize_chain_status(
        folio="123456",
        sale_events_count=5,
        gap_count=0,
    )
    assert status == "COMPLETE"


def test_summarize_chain_status_marks_broken_when_gap_count_positive() -> None:
    status = TitleChainController.summarize_chain_status(
        folio="123456",
        sale_events_count=5,
        gap_count=2,
    )
    assert status == "BROKEN"


def test_gap_status_sql_contains_shared_gap_statuses() -> None:
    sql = TitleChainController._gap_status_sql("e.link_status")  # noqa: SLF001
    assert sql == "e.link_status IN ('MISSING_PARTY', 'CHAINED_BY_FOLIO')"


def test_build_chain_sql_uses_shared_gap_predicate() -> None:
    sql = TitleChainController._build_chain_sql()  # noqa: SLF001
    assert "(s.link_status IN ('MISSING_PARTY', 'CHAINED_BY_FOLIO')) AS is_gap" in sql
    assert "= 'GAP'" not in sql


def test_build_summary_sql_uses_shared_gap_predicate() -> None:
    sql = TitleChainController._build_summary_sql()  # noqa: SLF001
    assert "COUNT(*) FILTER (WHERE e.link_status IN ('MISSING_PARTY', 'CHAINED_BY_FOLIO')) AS gap_count" in sql
    assert "= 'GAP'" not in sql
