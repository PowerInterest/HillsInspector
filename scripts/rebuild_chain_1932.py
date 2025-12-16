from src.services.ingestion_service import IngestionService
from src.models.property import Property
from src.db.operations import PropertyDB
from loguru import logger

def rebuild_chain():
    folio = '193206C07000000005160U'
    case_number = '292023CA015282A001HC' # From previous debug
    
    logger.info(f"Rebuilding chain for {folio}...")
    
    # 1. Clear existing chain/encumbrances
    db = PropertyDB()
    conn = db.connect()
    conn.execute("DELETE FROM chain_of_title WHERE folio = ?", [folio])
    conn.execute("DELETE FROM encumbrances WHERE folio = ?", [folio])
    logger.info("Cleared existing chain data.")
    
    # 2. Run IngestionService
    # This will:
    # - Find existing documents in DB (we didn't delete them)
    # - Re-run build_chain_and_analyze (using the FIXED code)
    # - Save new chain/encumbrances (with amounts!)
    
    service = IngestionService()
    
    # Need to verify if we need to pass raw_docs or if it fetches from DB
    # ingestion_service.ingest_property fetches from ORI if not provided.
    # But it also groups docs. 
    
    # Let's fetch the docs from the DB first to pass them in, 
    # effectively simulating "pre-fetched" docs so it doesn't scrape ORI again.
    
    docs = conn.execute("SELECT extracted_data FROM documents WHERE folio = ?", [folio]).fetchall()
    import json
    raw_docs = []
    for d in docs:
        if d[0]:
            try:
                # We stored the *grouped* doc in extracted_data? 
                # Or the raw ORI doc? 
                # IngestionService._map_grouped_ori_doc stores `grouped_doc` in `extracted_data`.
                # We need to reconstruct the "raw" list for ingest_property.
                # Actually, ingest_property takes `raw_docs`.
                
                # It's easier to just let it search ORI again? No, that's slow.
                # Let's trust that if we provide NO docs, it searches.
                pass
            except:
                pass
                
    # Actually, simpler: ingest_property(prop) will search ORI.
    # Since we have the docs in DB, we could just load them.
    # But IngestionService doesn't have a "rebuild from DB" method easily exposed.
    
    # Let's just let it search ORI again. It's one property.
    # It will find the existing docs by instrument number and update them.
    
    prop = Property(
        case_number=case_number,
        parcel_id=folio,
        legal_description="Lot 516, Shell Cove Phase 1", # Simplified for search
        address="1308 OCEAN SPRAY DR"
    )
    
    # We need valid legal search terms
    # From debug output: "Lot 516, Shell Cove Phase 1"
    prop.legal_search_terms = ["L 516 SHELL COVE*"] 
    
    service.ingest_property(prop)
    logger.success("Rebuild complete.")

if __name__ == "__main__":
    rebuild_chain()
