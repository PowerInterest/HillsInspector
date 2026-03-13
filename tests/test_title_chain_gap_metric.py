"""Regression tests for the title-chain gap metric (Issue #1).

These tests validate the Python semantics used to derive gap classifications
and summary status, then assert the generated SQL references the same shared
gap predicate. That gives us behavioral coverage without needing a live
PostgreSQL fixture for the full title-chain pipeline.
"""

from __future__ import annotations

from src.db.migrations.create_foreclosures import DDL
from src.services.pg_title_chain_controller import ControllerConfig, TitleChainController


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


class _CaptureConn:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: object) -> None:
        self.statements.append(str(statement))


def test_reset_outputs_preserves_overlay_rows_for_partial_runs() -> None:
    controller = TitleChainController.__new__(TitleChainController)
    controller._config = ControllerConfig(active_only=True)  # noqa: SLF001
    conn = _CaptureConn()

    controller._reset_outputs(conn)  # noqa: SLF001

    assert "DELETE FROM foreclosure_title_events" in conn.statements[2]
    assert (
        "event_source IN ('ORI_DEED_SEARCH', 'ORI_DEED_BACKFILL')"
        in conn.statements[2]
    )
    assert "NOT (" in conn.statements[2]


def test_reset_outputs_preserves_overlay_rows_for_full_runs() -> None:
    controller = TitleChainController.__new__(TitleChainController)
    controller._config = ControllerConfig(active_only=False)  # noqa: SLF001
    conn = _CaptureConn()

    controller._reset_outputs(conn)  # noqa: SLF001

    assert conn.statements[0] == (
        "TRUNCATE TABLE foreclosure_title_chain, foreclosure_title_summary "
        "RESTART IDENTITY"
    )
    assert "DELETE FROM foreclosure_title_events" in conn.statements[1]
    assert (
        "event_source IN ('ORI_DEED_SEARCH', 'ORI_DEED_BACKFILL')"
        in conn.statements[1]
    )


def test_insert_sales_events_sql_uses_recovery_overlay_rows() -> None:
    sql = TitleChainController._insert_sales_events_sql()  # noqa: SLF001

    assert "NULLIF(btrim(backfill.grantor), '')" in sql
    assert "NULLIF(btrim(ori.parties_from_text), '')" in sql
    assert "NULLIF(btrim(s.grantor), '')" in sql
    assert "NULLIF(btrim(backfill.grantee), '')" in sql
    assert "NULLIF(btrim(ori.parties_to_text), '')" in sql
    assert "NULLIF(btrim(s.grantee), '')" in sql
    assert "LEFT JOIN LATERAL (" in sql
    assert "e.event_source IN ('ORI_DEED_SEARCH', 'ORI_DEED_BACKFILL')" in sql
    assert "JOIN foreclosure_title_events e" in sql
    assert "e.event_source = 'ORI_DEED_SEARCH'" in sql
    assert "UNION ALL" in sql
    assert "AND (s.grantor IS NULL OR s.grantee IS NULL)" not in sql


def test_insert_sales_events_sql_excludes_search_no_result_sentinels() -> None:
    sql = TitleChainController._insert_sales_events_sql()  # noqa: SLF001

    assert "COALESCE(e.event_subtype, '') <> 'SEARCH_NO_RESULT'" in sql
    assert "NULLIF(btrim(COALESCE(e.instrument_number, '')), '') IS NOT NULL" in sql
    assert "NULLIF(btrim(COALESCE(e.grantor, '')), '') IS NOT NULL" in sql
    assert "NULLIF(btrim(COALESCE(e.grantee, '')), '') IS NOT NULL" in sql


def test_bootstrap_fn_title_chain_excludes_search_no_result_sentinels() -> None:
    ddl = next(stmt for stmt in DDL if "CREATE OR REPLACE FUNCTION fn_title_chain(" in stmt)

    assert "COALESCE(e.event_subtype, '') <> 'SEARCH_NO_RESULT'" in ddl
    assert "NULLIF(btrim(COALESCE(e.instrument_number, '')), '') IS NOT NULL" in ddl
    assert "NULLIF(btrim(COALESCE(e.grantor, '')), '') IS NOT NULL" in ddl
    assert "NULLIF(btrim(COALESCE(e.grantee, '')), '') IS NOT NULL" in ddl


def test_normalize_party_name_py_strips_representative_tokens() -> None:
    observed = TitleChainController._normalize_party_name_py("Silva Matthew Tru")  # noqa: SLF001

    assert observed == "MATTHEW SILVA"


def test_entity_match_score_py_uses_best_party_from_semicolon_list() -> None:
    observed = TitleChainController._entity_match_score_py(  # noqa: SLF001
        "AMADOR NANCY C; TRUJILLO ERNESTO",
        "TRUJILLO NANCY C",
    )

    assert observed is not None
    assert observed >= 0.68


def test_classify_sale_link_accepts_same_owner_continuation() -> None:
    status, score = TitleChainController.classify_sale_link(
        seq=2,
        grantor="SILVA MATTHEW TRU",
        grantee="TRUJILLO ERNESTO",
        prev_grantee="TRUJILLO ERNESTO",
    )

    assert status == "LINKED_EXACT"
    assert score == 1.0


def test_score_sales_links_sql_checks_current_grantee_for_same_owner_continuity() -> None:
    sql = TitleChainController._score_sales_links_sql()  # noqa: SLF001

    assert "entity_match_score(os.grantee, os.prev_grantee)" in sql
    assert "entity_match_score(os.grantor, os.prev_grantee)" in sql


def test_bootstrap_fn_title_chain_uses_current_grantee_as_same_owner_guard() -> None:
    ddl = next(stmt for stmt in DDL if "CREATE OR REPLACE FUNCTION fn_title_chain(" in stmt)

    assert "entity_match_score(a.prev_grantee, a.grantee)" in ddl
    assert "is_same_entity(a.prev_grantee, a.grantee, p_match_threshold)" in ddl
