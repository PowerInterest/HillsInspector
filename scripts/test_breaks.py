from sunbiz.db import get_engine, resolve_pg_dsn
from sqlalchemy import text

dsn = resolve_pg_dsn(None)
engine = get_engine(dsn)

sql = """
            SELECT DISTINCT f.foreclosure_id, f.case_number_raw,
                   f.case_number_norm, f.strap, f.folio
            FROM foreclosures f
            JOIN foreclosure_title_events fte
              ON fte.foreclosure_id = f.foreclosure_id
            WHERE f.archived_at IS NULL
              AND f.folio IS NOT NULL
              AND btrim(f.folio) <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM foreclosure_title_events e2
                  WHERE e2.foreclosure_id = f.foreclosure_id
                    AND e2.event_source = 'ORI_DEED_SEARCH'
              )
            ORDER BY f.foreclosure_id
            LIMIT 5
"""

with engine.connect() as conn:
    targets = conn.execute(text(sql)).fetchall()
    print("Targets:")
    for t in targets:
        print(f"Target: {t}")
        gaps = conn.execute(text("SELECT * FROM fn_title_chain_gaps(:folio)"), {"folio": t.folio}).fetchall()
        print(f"  Gaps for folio {t.folio}:")
        for gap in gaps:
            print(f"    {gap}")
