import asyncio
import traceback
from src.db.operations import PropertyDB
from src.services.ingestion_service import IngestionService
from src.services.title_chain_service import TitleChainService
from src.services.lien_survival_analyzer import LienSurvivalAnalyzer
from src.models.property import Lien
from datetime import datetime, date

async def run_analysis_only():
    print("STARTING ANALYSIS-ONLY PIPELINE", flush=True)
    
    db = PropertyDB()
    db.create_chain_tables()
    
    print("[1/2] Building Chain of Title...", flush=True)
    
    # Fetch docs
    rows = db.connect().execute("""
        SELECT DISTINCT folio 
        FROM documents 
        WHERE folio IS NOT NULL
    """).fetchall()
    
    print(f"Found {len(rows)} properties.", flush=True)
    
    chain_service = TitleChainService()
    ingestion_service = IngestionService()
    
    count = 0
    for i, (folio,) in enumerate(rows):
        print(f"Processing {repr(folio)} ({i+1}/{len(rows)})...", flush=True)
        try:
            # Get docs
            inner_db = PropertyDB()
            # Try exact match first
            docs = inner_db.connect().execute("SELECT * FROM documents WHERE folio = ?", [folio]).fetchall()
            
            if not docs:
                # Try trimmed match
                print(f"  No docs found for exact match. Trying trimmed...", flush=True)
                docs = inner_db.connect().execute("SELECT * FROM documents WHERE TRIM(folio) = TRIM(?)", [folio]).fetchall()
                
            columns = [desc[0] for desc in inner_db.connect().description]
            doc_dicts = [dict(zip(columns, row)) for row in docs]
            inner_db.close()
            
            # Map
            mapped_docs = []
            for d in doc_dicts:
                extracted = d.get('extracted_data')
                if isinstance(extracted, str):
                    import json
                    try:
                        extracted = json.loads(extracted)
                    except:
                        extracted = {}
                
                mapped_docs.append({
                    'doc_type': d.get('document_type'),
                    'recording_date': d.get('recording_date'),
                    'book': d.get('book'),
                    'page': d.get('page'),
                    'instrument_number': d.get('instrument_number'),
                    'party1': d.get('party1'),
                    'party2': d.get('party2'),
                    'legal_description': d.get('legal_description'),
                    'notes': d.get('ocr_text')
                })

            # Build Chain
            print(f"  Building chain for {folio} with {len(mapped_docs)} docs...", flush=True)
            analysis = chain_service.build_chain_and_analyze(mapped_docs)
            print(f"  Found {len(analysis.get('encumbrances', []))} encumbrances.", flush=True)
            
            # Transform for DB (Custom logic to ensure no encumbrances are lost)
            chain = analysis.get('chain', [])
            encumbrances = analysis.get('encumbrances', [])
            
            timeline = []
            
            for i, deed in enumerate(chain):
                start_date = ingestion_service._parse_date(deed.get('date'))
                end_date = None
                if i < len(chain) - 1:
                    end_date = ingestion_service._parse_date(chain[i+1].get('date'))
                
                # Find encumbrances in this period
                period_encs = []
                for enc in encumbrances:
                    enc_date = ingestion_service._parse_date(enc.get('date'))
                    
                    if enc_date and start_date:
                        if enc_date >= start_date:
                            if end_date is None or enc_date < end_date:
                                period_encs.append(ingestion_service._map_encumbrance(enc))
                
                timeline.append({
                    "owner": deed.get('grantee'),
                    "acquired_from": deed.get('grantor'),
                    "acquisition_date": deed.get('date'),
                    "disposition_date": chain[i+1].get('date') if i < len(chain) - 1 else None,
                    "acquisition_instrument": None, 
                    "acquisition_doc_type": deed.get('doc_type'),
                    "acquisition_price": None,
                    "encumbrances": period_encs
                })
            
            # If no chain, or if we want to catch everything else
            # Create a "catch-all" period if there are encumbrances but no chain
            # Or if there are encumbrances that didn't fit into any period (TODO: check unassigned)
            # For now, if chain is empty, save encumbrances in a catch-all
            if not chain and encumbrances:
                print(f"  No chain but {len(encumbrances)} encumbrances found. Creating catch-all period.", flush=True)
                timeline.append({
                    "owner": "Unknown (No Deed Found)",
                    "acquired_from": None,
                    "acquisition_date": None,
                    "disposition_date": None,
                    "acquisition_instrument": None,
                    "acquisition_doc_type": "UNKNOWN",
                    "acquisition_price": None,
                    "encumbrances": [ingestion_service._map_encumbrance(e) for e in encumbrances]
                })
            
            db_data = {"ownership_timeline": timeline}
            
            # Save
            db.save_chain_of_title(folio, db_data)
            count += 1
            print(f"  Saved chain for {folio}", flush=True)
            
        except Exception:
            traceback.print_exc()

    print(f"Built chain for {count} properties.", flush=True)

    # --- Step 2: Analyze Lien Survival ---
    print("\n[2/2] Analyzing Lien Survival...", flush=True)
    survival_analyzer = LienSurvivalAnalyzer()
    
    auctions = db.connect().execute("SELECT * FROM auctions").fetchall()
    columns = [desc[0] for desc in db.connect().description]
    auction_dicts = [dict(zip(columns, row)) for row in auctions]
    
    analyzed_count = 0
    for auction in auction_dicts:
        folio = auction.get('parcel_id')
        case_number = auction.get('case_number')
        
        if not folio: continue
        
        # Get Encumbrances
        encs_rows = db.connect().execute(f"""
            SELECT id, encumbrance_type, recording_date, book, page, amount 
            FROM encumbrances 
            WHERE folio = '{folio}' AND is_satisfied = FALSE
        """).fetchall()
        
        if not encs_rows:
            continue
            
        enc_cols = [desc[0] for desc in db.connect().description]
        encs = [dict(zip(enc_cols, row)) for row in encs_rows]
        
        liens_for_analysis = []
        lien_id_map = {}
        
        for row in encs:
            rec_date = None
            if row['recording_date']:
                try:
                    if isinstance(row['recording_date'], str):
                        rec_date = datetime.strptime(row['recording_date'], "%Y-%m-%d").date()
                    else:
                        rec_date = row['recording_date']
                except:
                    pass
            
            l = Lien(
                document_type=row['encumbrance_type'],
                recording_date=rec_date or date.min,
                amount=row['amount'],
                book=row['book'],
                page=row['page']
            )
            liens_for_analysis.append(l)
            lien_id_map[id(l)] = row['id']
            
        # Judgment Data
        judgment_data = {}
        if auction.get('extracted_judgment_data'):
            import json
            try:
                judgment_data = json.loads(auction['extracted_judgment_data'])
            except:
                pass
                
        lis_pendens_date = None
        lp_str = judgment_data.get('lis_pendens_date')
        if lp_str:
            try:
                lis_pendens_date = datetime.strptime(lp_str, "%Y-%m-%d").date()
            except:
                pass

        # Analyze
        survival_result = survival_analyzer.analyze(
            liens=liens_for_analysis,
            foreclosure_type=auction.get('foreclosure_type') or judgment_data.get('foreclosure_type'),
            lis_pendens_date=lis_pendens_date,
            original_mortgage_amount=auction.get('original_mortgage_amount')
        )
        
        # Update DB
        surviving_ids = []
        expired_ids = []
        
        for l in survival_result['surviving_liens']:
            lid = lien_id_map.get(id(l))
            if lid:
                db.update_encumbrance_survival(lid, "SURVIVED")
                surviving_ids.append(lid)
                
        for l in survival_result.get('expired_liens', []):
            lid = lien_id_map.get(id(l))
            if lid:
                db.update_encumbrance_survival(lid, "EXPIRED")
                expired_ids.append(lid)
                
        for row in encs:
            lid = row['id']
            if lid not in surviving_ids and lid not in expired_ids:
                db.update_encumbrance_survival(lid, "WIPED_OUT")
                
        db.mark_as_analyzed(case_number)
        analyzed_count += 1

    print(f"Analyzed liens for {analyzed_count} properties", flush=True)

if __name__ == "__main__":
    asyncio.run(run_analysis_only())
