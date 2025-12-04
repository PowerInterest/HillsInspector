from typing import List, Optional, Dict, Any
from pathlib import Path
from datetime import datetime
from loguru import logger
import json

from src.models.property import Property
from src.db.operations import PropertyDB
from src.scrapers.ori_api_scraper import ORIApiScraper
from src.services.title_chain_service import TitleChainService

class IngestionService:
    def __init__(self):
        self.db = PropertyDB()
        self.ori_scraper = ORIApiScraper()
        self.chain_service = TitleChainService()
        self.pdf_dir = Path("data/documents/ori_docs")
        self.pdf_dir.mkdir(parents=True, exist_ok=True)

    def ingest_property(self, prop: Property):
        """
        Full ingestion pipeline for a single property.
        1. Fetch docs from ORI (by Legal Description)
        2. Save docs to DB
        3. Build Chain of Title
        4. Save analysis
        """
        logger.info(f"Ingesting property {prop.case_number} (Folio: {prop.parcel_id})")
        
        # 1. Search ORI
        search_term = self._clean_legal_description(prop.legal_description)
        if not search_term:
            logger.warning(f"No valid legal description for {prop.case_number}")
            return

        logger.info(f"Searching ORI for: {search_term}")
        docs = self.ori_scraper.search_by_legal(search_term)
        logger.info(f"Found {len(docs)} documents")
        
        if not docs:
            logger.warning("No documents found.")
            return

        processed_docs = []
        
        for doc in docs:
            # Map ORI doc to our schema
            mapped_doc = self._map_ori_doc(doc, prop)
            
            # Save to DB
            doc_id = self.db.save_document(prop.parcel_id, mapped_doc)
            mapped_doc['id'] = doc_id
            processed_docs.append(mapped_doc)
            
            # TODO: Download PDF if needed (e.g. for OCR of specific docs)
            # For now, we rely on metadata for chain building
            
        # 2. Build Chain
        logger.info("Building Chain of Title...")
        analysis = self.chain_service.build_chain_and_analyze(processed_docs)
        
        # Transform for DB
        db_data = self._transform_analysis_for_db(analysis)
        
        # 3. Save Analysis
        self.db.save_chain_of_title(prop.parcel_id, db_data)
        logger.success(f"Ingestion complete for {prop.case_number}")

    def _transform_analysis_for_db(self, analysis: dict) -> dict:
        chain = analysis.get('chain', [])
        encumbrances = analysis.get('encumbrances', [])
        
        timeline = []
        
        for i, deed in enumerate(chain):
            start_date = self._parse_date(deed.get('date'))
            end_date = None
            if i < len(chain) - 1:
                end_date = self._parse_date(chain[i+1].get('date'))
            
            # Find encumbrances in this period
            period_encs = []
            for enc in encumbrances:
                enc_date = self._parse_date(enc.get('date'))
                # If enc_date is None, we can't place it. Maybe put in last period?
                if enc_date and start_date:
                    # Check if enc_date is >= start_date
                    # And if end_date exists, enc_date < end_date
                    if enc_date >= start_date:
                        if end_date is None or enc_date < end_date:
                            period_encs.append(self._map_encumbrance(enc))
            
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
            
        return {"ownership_timeline": timeline}

    def _map_encumbrance(self, enc: dict) -> dict:
        """Map analysis encumbrance to DB format."""
        bk, pg = None, None
        if enc.get('book_page'):
            parts = enc['book_page'].split('/')
            if len(parts) == 2:
                bk, pg = parts
                
        return {
            "type": enc.get('type'),
            "creditor": enc.get('creditor'),
            "amount": self._parse_amount(enc.get('amount')),
            "recording_date": enc.get('date'),
            "instrument": None, # Not in analysis dict?
            "book": bk,
            "page": pg,
            "is_satisfied": enc.get('status') == 'SATISFIED',
            "satisfaction_instrument": enc.get('satisfaction_ref'),
            "satisfaction_date": None, # Not in analysis dict?
            "survival_status": "UNKNOWN"
        }

    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        if not date_str: return None
        try:
            return datetime.strptime(date_str, "%m/%d/%Y")
        except:
            try:
                return datetime.strptime(date_str, "%Y-%m-%d")
            except:
                return None

    def _parse_amount(self, amt: Any) -> float:
        if not amt or amt == 'Unknown': return 0.0
        try:
            return float(str(amt).replace('$', '').replace(',', ''))
        except:
            return 0.0

    def _clean_legal_description(self, legal: str) -> Optional[str]:
        if not legal:
            return None
        # Remove common prefixes/suffixes or noise
        # Example: "LOT 5 BLOCK 3 SUBDIVISION NAME" -> "SUBDIVISION NAME" is hard without NLP
        # But usually searching for the whole string works if it's exact, or we use CONTAINS.
        # The API uses CONTAINS.
        # So we should try to pick the most unique part.
        # For now, return the first 60 chars as a heuristic to avoid super long strings
        return legal[:60]

    def _map_ori_doc(self, ori_doc: dict, prop: Property) -> dict:
        try:
            # Timestamp seems to be in milliseconds in some systems, but test script used it directly.
            # Let's assume seconds for now, or check length.
            ts = ori_doc.get("RecordDate", 0)
            if ts > 100000000000: # It's ms
                ts = ts / 1000
            rec_date = datetime.fromtimestamp(ts).strftime("%m/%d/%Y")
        except:
            rec_date = None
            
        return {
            "folio": prop.parcel_id,
            "case_number": prop.case_number,
            "document_type": ori_doc.get("DocType", "UNKNOWN"),
            "recording_date": rec_date,
            "book": ori_doc.get("Book"),
            "page": ori_doc.get("Page"),
            "instrument_number": ori_doc.get("Instrument"),
            "party1": ", ".join(ori_doc.get("PartiesOne", [])),
            "party2": ", ".join(ori_doc.get("PartiesTwo", [])),
            "legal_description": str(ori_doc.get("Legal", "")),
            "extracted_data": ori_doc # Store raw data too
        }
