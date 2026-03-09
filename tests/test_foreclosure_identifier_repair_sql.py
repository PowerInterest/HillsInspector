from __future__ import annotations

from src.scripts.refresh_foreclosures import ENRICH_BASE_SQL
from src.db.migrations.create_foreclosures import DDL


def test_refresh_sql_prefers_hcpa_match_over_stored_bad_strap() -> None:
    assert "strap                 = COALESCE(bp.strap, f.strap)" in ENRICH_BASE_SQL
    assert "AND bp2.strap = f2.strap" in ENRICH_BASE_SQL
    assert "AND bp2.folio = f2.folio" in ENRICH_BASE_SQL
    assert "THEN 0" in ENRICH_BASE_SQL
    assert "THEN 1" in ENRICH_BASE_SQL
    assert "THEN 2" in ENRICH_BASE_SQL


def test_normalize_trigger_repairs_non_null_invalid_strap_from_folio() -> None:
    trigger_sql = next(stmt for stmt in DDL if "CREATE OR REPLACE FUNCTION normalize_foreclosure()" in stmt)

    assert "Repair non-null straps that do not resolve to the same folio." in trigger_sql
    assert "WHERE bp.strap = NEW.strap" in trigger_sql
    assert "AND bp.folio = NEW.folio" in trigger_sql
    assert "WHERE bp.folio = NEW.folio" in trigger_sql


def test_bootstrap_dashboard_auctions_prefers_per_foreclosure_survival_rows() -> None:
    sql = next(stmt for stmt in DDL if "CREATE OR REPLACE FUNCTION get_dashboard_auctions(" in stmt)

    assert "LEFT JOIN foreclosure_encumbrance_survival fes" in sql
    assert "fes.foreclosure_id = f.foreclosure_id" in sql
    assert "COALESCE(fes.survival_status, oe.survival_status)" in sql


def test_bootstrap_property_encumbrances_accepts_foreclosure_context() -> None:
    drop_sql = next(stmt for stmt in DDL if "DROP FUNCTION IF EXISTS get_property_encumbrances(TEXT);" in stmt)
    sql = next(stmt for stmt in DDL if "CREATE OR REPLACE FUNCTION get_property_encumbrances(" in stmt)

    assert "DROP FUNCTION IF EXISTS get_property_encumbrances(TEXT);" in drop_sql
    assert "p_foreclosure_id BIGINT DEFAULT NULL" in sql
    assert "COALESCE(fes.survival_status, oe.survival_status)" in sql
    assert "fes.foreclosure_id = v_foreclosure_id" in sql


def test_bootstrap_compute_net_equity_accepts_foreclosure_context() -> None:
    drop_sql = next(stmt for stmt in DDL if "DROP FUNCTION IF EXISTS compute_net_equity(TEXT);" in stmt)
    sql = next(stmt for stmt in DDL if "CREATE OR REPLACE FUNCTION compute_net_equity(" in stmt)

    assert "DROP FUNCTION IF EXISTS compute_net_equity(TEXT);" in drop_sql
    assert "p_foreclosure_id BIGINT DEFAULT NULL" in sql
    assert "COALESCE(fes.survival_status, oe.survival_status)" in sql
    assert "WHERE f.foreclosure_id = COALESCE(v_foreclosure_id, f.foreclosure_id)" in sql
