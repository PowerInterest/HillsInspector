from src.db.operations import PropertyDB

def inspect_doc_types():
    db = PropertyDB()
    types = db.connect().execute("""
        SELECT document_type, COUNT(*) 
        FROM documents 
        GROUP BY document_type
        ORDER BY COUNT(*) DESC
    """).fetchall()
    
    print("Document Types in DB:")
    for t, c in types:
        print(f"  {t}: {c}")

if __name__ == "__main__":
    inspect_doc_types()
