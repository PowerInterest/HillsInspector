from sunbiz.db import get_engine, resolve_pg_dsn
from sqlalchemy import text
from src.services.pg_ori_service import PgOriService
from src.services.pg_mortgage_extraction_service import PgMortgageExtractionService

svc = PgOriService()
resp = svc._search_case_pav("292023CA012294A001HC", {"api_calls": 0})
target_id = None
target_inst = None
for row in resp:
    cols = row.get("DisplayColumnValues", [])
    if len(cols) >= 9 and "MORTGAGE" in str(cols[3].get("Value")).upper():
        target_id = row.get("ID")
        target_inst = str(cols[8].get("Value")).strip()
        print("Found mortgage:", target_inst, "ID:", target_id)
        break

if target_id and target_inst:
    engine = get_engine(resolve_pg_dsn(None))
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE ori_encumbrances SET ori_id = :oid WHERE instrument_number = :inst"),
            {"oid": target_id, "inst": target_inst},
        )
    msvc = PgMortgageExtractionService()
    res = msvc.run(limit=1)
    print("Extraction Result:", res)
else:
    print("Could not find mortgage for the case.")
