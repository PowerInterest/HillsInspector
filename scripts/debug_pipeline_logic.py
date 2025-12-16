from src.pipeline import PipelineDB, is_valid_folio
from src.db.operations import PropertyDB

def debug_pipeline():
    db = PipelineDB()
    rows = db.execute_query("SELECT * FROM auctions WHERE needs_ori_ingestion = TRUE AND parcel_id IS NOT NULL")
    print(f"Found {len(rows)} pending auctions")
    
    target_folio = '193206C07000000005160U'
    
    for r in rows:
        folio = r['parcel_id']
        if folio != target_folio: continue
        
        print(f"  {folio} (Valid: {is_valid_folio(folio)})")
        
        # Check skip logic
        last_case = db.get_last_analyzed_case(folio)
        has_chain = db.folio_has_chain_of_title(folio)
        print(f"    Chain: {has_chain}, Last Case: {last_case}, Curr Case: {r['case_number']}")
        
        if has_chain and last_case == r['case_number']:
            print("    -> WOULD SKIP")
        else:
            print("    -> WOULD PROCESS")

if __name__ == "__main__":
    debug_pipeline()
