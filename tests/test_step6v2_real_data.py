"""
Integration test for Step 6 v2 using real data from property_master_v2.db.
"""
import duckdb
import json
import contextlib
from datetime import date
from loguru import logger
from src.services.lien_survival.survival_service import SurvivalService
from src.services.step4v2.chain_builder import ChainBuilder
from src.db.operations import PropertyDB
from config.step4v2 import V2_DB_PATH

def test_real_data():
    # 1. Setup connections
    v2_conn = duckdb.connect(V2_DB_PATH)
    legacy_db = PropertyDB()
    
    # 2. Get properties that have encumbrances in v2
    folios = v2_conn.execute("SELECT DISTINCT folio FROM encumbrances LIMIT 5").fetchall()
    folios = [f[0] for f in folios]
    
    if not folios:
        print("No folios with encumbrances found in v2 DB.")
        return

    print(f"Testing Step 6 v2 on {len(folios)} real properties...\n")
    
    builder = ChainBuilder(v2_conn)
    
    for folio in folios:
        print(f"--- Property: {folio} ---")
        
        # A. Load data from v2
        periods = builder.get_chain(folio)
        encumbrances = builder.get_encumbrances(folio)
        
        # Map to dicts
        period_dicts = [p.__dict__.copy() for p in periods]
        enc_dicts = [e.__dict__.copy() for e in encumbrances]
        
        # B. Load judgment data from legacy (using case number if possible)
        # For test, we'll try to find a case for this folio in the status table
        status_row = legacy_db.execute_query(
            "SELECT case_number FROM status WHERE parcel_id = ? LIMIT 1", [folio]
        )
        if not status_row:
            print(f"Skipping {folio}: No case found in status table.")
            continue
            
        case_number = status_row[0]['case_number']
        auction = legacy_db.get_auction_by_case(case_number)
        
        judgment_data = {}
        if auction and auction.get('extracted_judgment_data'):
            raw = auction['extracted_judgment_data']
            with contextlib.suppress(json.JSONDecodeError):
                judgment_data = json.loads(raw) if isinstance(raw, str) else raw
        
        # Ensure plaintiff and defendants for service
        if not judgment_data.get('plaintiff'):
             judgment_data['plaintiff'] = auction.get('plaintiff', 'UNKNOWN')
        if not judgment_data.get('defendants'):
             def_name = auction.get('defendant')
             if def_name:
                 judgment_data['defendants'] = [def_name]
        
        # C. Run Survival Service
        service = SurvivalService(folio)
        
        # Identify current period
        current_period_id = None
        if periods:
            latest = sorted(periods, key=lambda p: p.acquisition_date or date.min, reverse=True)[0]
            current_period_id = latest.id
            
        analysis = service.analyze(enc_dicts, judgment_data, period_dicts, current_period_id)
        
        # D. Print Results
        results = analysis['results']
        print(f"Summary: {analysis['summary']}")
        if analysis['uncertainty_flags']:
            print(f"Uncertainty Flags: {analysis['uncertainty_flags']}")
            
        for category, list_of_enc in results.items():
            if list_of_enc:
                print(f"  [{category.upper()}]:")
                for enc in list_of_enc:
                    creditor = enc.get('creditor') or 'Unknown'
                    reason = enc.get('survival_reason') or 'No reason given'
                    print(f"    - {creditor}: {reason}")
        print("\n")

    v2_conn.close()

if __name__ == "__main__":
    test_real_data()
