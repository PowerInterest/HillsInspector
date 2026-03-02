from __future__ import annotations

from scripts.refresh_foreclosures import ENRICH_BASE_SQL
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
