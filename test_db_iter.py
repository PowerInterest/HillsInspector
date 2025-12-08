from src.db.operations import PropertyDB

def test_db():
    db = PropertyDB()
    rows = db.connect().execute("""
        SELECT DISTINCT folio, case_number 
        FROM documents 
        WHERE folio IS NOT NULL
    """).fetchall()
    
    print(f"Rows found: {len(rows)}")
    for r in rows:
        print(f"Row: {r}")

if __name__ == "__main__":
    test_db()
