import sys
import os
import json
from datetime import date, datetime
from pathlib import Path

# Add project root to path
sys.path.append(str(Path.cwd()))

from src.db.operations import PropertyDB
from src.services.title_chain_service import TitleChainService

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def run_test():
    import shutil
    import time
    
    db_path = "data/property_master_sqlite.db"
    temp_db_path = f"data/property_master_copy_{int(time.time())}.db"
    
    print(f"Copying DB to {temp_db_path} to avoid locks...")
    try:
        shutil.copy(db_path, temp_db_path)
    except Exception as e:
        print(f"Could not copy DB: {e}")
        # If copy fails, maybe we try original, but likely fails too.
        # Check if file exists first
        if not os.path.exists(db_path):
            print(f"DB file {db_path} does not exist!")
            return

    print("Connecting to DB...")
    db = PropertyDB(temp_db_path)
    conn = db.connect()
    
    # Get 5 random folios that have documents
    print("Selecting 5 random properties with documents...")
    try:
        # Try simplified sample syntax if USING SAMPLE is brittle or version dependent
        folios = conn.execute("""
            SELECT DISTINCT folio 
            FROM documents 
            ORDER BY random() 
            LIMIT 5
        """).fetchall()
    except Exception as e:
        print(f"Error fetching random folios: {e}")
        return
    
    if not folios:
        print("No documents found in DB. Cannot test.")
        return

    service = TitleChainService()
    
    for row in folios:
        folio = row[0]
        print(f"\n{'#'*60}")
        print(f"Analyzing Folio: {folio}")
        print(f"{'#'*60}")
        
        # Fetch docs
        docs_rows = conn.execute("""
            SELECT * FROM documents WHERE folio = ?
        """, [folio]).fetchall()
        
        docs = [dict(r) for r in docs_rows]
        print(f"Found {len(docs)} documents.")
        
        try:
            analysis = service.build_chain_and_analyze(docs)
            
            # Print Summary
            print("\n--- Summary ---")
            print(json.dumps(analysis['summary'], indent=2))
            
            # Print Chain
            print("\n--- Chain of Title ---")
            for deed in analysis['chain']:
                print(f"  {deed['date']} | {deed['grantor']} -> {deed['grantee']}")
                print(f"    Type: {deed['doc_type']} | Ref: {deed['book_page']}")
                if deed['notes']:
                    for note in deed['notes']:
                        print(f"    [!] {note}")
                print()
                    
            # Print Encumbrances
            print("\n--- Encumbrances ---")
            for enc in analysis['encumbrances']:
                status = enc['status']
                marker = "[OPEN]     " if status == 'OPEN' else "[SATISFIED]"
                print(f"  {marker} {enc['date']} | {enc['type']:<15} | ${enc['amount']} | {enc['creditor']}")
                if status == 'SATISFIED':
                    print(f"      Satisfied by: {enc['satisfaction_ref']} (Method: {enc['match_method']})")
            
            if not analysis['encumbrances']:
                print("  No encumbrances found.")

        except Exception as e:
            print(f"ERROR analyzing {folio}: {e}")
            import traceback
            traceback.print_exc()
        
        print("\n")

if __name__ == "__main__":
    run_test()
