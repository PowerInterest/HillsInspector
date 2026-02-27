import requests
from sqlalchemy import text
from sunbiz.db import get_engine, resolve_pg_dsn

dsn = resolve_pg_dsn(None)
engine = get_engine(dsn)

with engine.connect() as conn:
    rows = conn.execute(
        text("""
            SELECT 
                s.doc_num, 
                MAX(s.sale_date) as sale_date,
                MAX(s.folio) as folio
            FROM hcpa_allsales s
            WHERE (s.grantor IS NULL OR s.grantee IS NULL)
              AND s.doc_num IS NOT NULL
              AND trim(s.doc_num) <> ''
            GROUP BY s.doc_num
            LIMIT 5
        """)
    ).fetchall()

pav_url = "https://publicaccess.hillsclerk.com/api/OfficialRecordsDirectSearch/AdvancedSearch"
pav_headers = {"Content-Type": "application/json"}
sess = requests.Session()

print("Testing PAV Backfill...")
for row in rows:
    doc_num = row.doc_num.strip()
    print(f"Doc Num: '{doc_num}', Folio: {row.folio}, Sale Date: {row.sale_date}")
    payload = {
        "MatchAnySearchWord": True,
        "InstrumentNumberSearchValue": doc_num,
        "IsExactNameSearchMode": False,
    }
    resp = sess.post(pav_url, json=payload, headers=pav_headers)
    data = resp.json()
    docs = data.get("Documents", [])
    print(f"  PAV Docs returned: {len(docs)}")
    if docs:
        print(f"  First Doc cols: {[c.get('Value') for c in docs[0].get('DisplayColumnValues', [])][:5]}")
