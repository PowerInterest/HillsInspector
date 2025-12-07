from typing import Optional, Any
from pathlib import Path
from datetime import datetime
from loguru import logger

from src.models.property import Property
from src.db.operations import PropertyDB
from src.scrapers.ori_api_scraper import ORIApiScraper
from src.services.title_chain_service import TitleChainService
from src.services.party2_resolution_service import Party2ResolutionService

class IngestionService:
    def __init__(self):
        self.db = PropertyDB()
        self.ori_scraper = ORIApiScraper()
        self.chain_service = TitleChainService()
        self.party2_service = Party2ResolutionService()
        self.pdf_dir = Path("data/documents/ori_docs")
        self.pdf_dir.mkdir(parents=True, exist_ok=True)

    def ingest_property(self, prop: Property, raw_docs: list = None):
        """
        Full ingestion pipeline for a single property.
        1. Fetch docs from ORI (by Legal Description with permutation fallback) - SKIPPED if raw_docs provided
        2. Save docs to DB
        3. Build Chain of Title
        4. Save analysis

        Args:
            prop: Property object with case_number, parcel_id, legal_description
            raw_docs: Optional pre-fetched ORI documents. If provided, skips ORI search.
                      This avoids duplicate browser sessions and speeds up batch processing.
        """
        logger.info(f"Ingesting property {prop.case_number} (Folio: {prop.parcel_id})")

        # 1. Use pre-fetched docs if provided, otherwise search ORI
        docs = raw_docs or []

        if not docs:
            # Get search terms from Property (set by pipeline) or fall back to legal description
            search_terms = getattr(prop, 'legal_search_terms', None) or []

            # If no pre-built search terms, try to build from legal_description
            if not search_terms:
                search_term = self._clean_legal_description(prop.legal_description)
                if search_term:
                    search_terms = [search_term]

            if not search_terms:
                logger.warning(f"No valid legal description for {prop.case_number}")
                return

            # Try each search term until we get results
            successful_term = None
            for search_term in search_terms:
                logger.info(f"Searching ORI for: {search_term}")
                try:
                    # Use browser-based search to avoid API blocking
                    docs = self.ori_scraper.search_by_legal_sync(search_term, headless=True)
                    if docs:
                        successful_term = search_term
                        logger.info(f"Found {len(docs)} documents with term: {search_term}")
                        break
                    logger.debug(f"No results for: {search_term}")
                except Exception as e:
                    logger.warning(f"Search failed for '{search_term}': {e}")
                    continue

            if not docs:
                logger.warning(f"No documents found after trying {len(search_terms)} search terms.")
                return

            # Log successful search term for future reference
            if successful_term:
                logger.info(f"Successful search term: {successful_term}")
        else:
            logger.info(f"Using {len(docs)} pre-fetched ORI records")

        # Group raw ORI records by instrument number (ORI returns one row per party)
        grouped_docs = self._group_ori_records_by_instrument(docs)
        logger.info(f"Grouped {len(docs)} raw records into {len(grouped_docs)} unique documents")

        # Resolve missing Party 2 for deeds
        grouped_docs = self._resolve_missing_party2(grouped_docs)

        processed_docs = []

        for doc in grouped_docs:
            # Map grouped ORI doc to our schema
            mapped_doc = self._map_grouped_ori_doc(doc, prop)

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

    def _group_ori_records_by_instrument(self, docs: list) -> list:
        """
        Group ORI records by instrument number.
        ORI returns one row per party per document, so we need to combine them.

        Args:
            docs: Raw ORI records with person_type, name, instrument, etc.

        Returns:
            List of grouped documents with party1_names and party2_names lists
        """
        by_instrument = {}

        for doc in docs:
            # Get instrument number (key for grouping)
            instrument = doc.get("instrument") or doc.get("Instrument", "")
            if not instrument:
                continue

            # Initialize group if new
            if instrument not in by_instrument:
                by_instrument[instrument] = {
                    "instrument": instrument,
                    "doc_type": doc.get("doc_type") or doc.get("DocType", ""),
                    "record_date": doc.get("record_date") or doc.get("RecordDate", ""),
                    "book_num": doc.get("book_num") or doc.get("Book", ""),
                    "page_num": doc.get("page_num") or doc.get("Page", ""),
                    "legal": doc.get("legal") or doc.get("Legal", ""),
                    "party1_names": [],  # PARTY 1 = Grantor/Mortgagor/Debtor
                    "party2_names": [],  # PARTY 2 = Grantee/Mortgagee/Creditor
                }

            # Add party to appropriate list
            person_type = doc.get("person_type", "").upper()
            name = doc.get("name", "").strip()

            if name:
                if "PARTY 1" in person_type or "GRANTOR" in person_type:
                    if name not in by_instrument[instrument]["party1_names"]:
                        by_instrument[instrument]["party1_names"].append(name)
                elif "PARTY 2" in person_type or "GRANTEE" in person_type:
                    if name not in by_instrument[instrument]["party2_names"]:
                        by_instrument[instrument]["party2_names"].append(name)

        return list(by_instrument.values())

    def _resolve_missing_party2(self, grouped_docs: list) -> list:
        """
        Resolve missing Party 2 (grantee) for deed documents.

        For deeds with Party 1 but no Party 2, attempts resolution via:
        1. CQID 326 party name search
        2. vLLM OCR extraction from PDF

        Also detects self-transfers (grantor == grantee).

        Args:
            grouped_docs: List of grouped documents with party1_names/party2_names

        Returns:
            Updated list with resolved Party 2 data
        """
        # Deed types that need both parties
        DEED_TYPES = {"(D) DEED", "(WD) WARRANTY DEED", "(QC) QUIT CLAIM",
                      "(CD) CORRECTIVE DEED", "(TD) TRUSTEE DEED", "(TAXDEED) TAX DEED"}

        resolved_count = 0
        self_transfer_count = 0

        for doc in grouped_docs:
            doc_type = (doc.get("doc_type") or "").upper()

            # Only process deeds
            if not any(dt in doc_type for dt in DEED_TYPES):
                continue

            # Skip if already has Party 2
            if doc.get("party2_names"):
                continue

            # Need Party 1 to search
            party1_names = doc.get("party1_names", [])
            if not party1_names:
                continue

            instrument = doc.get("instrument")
            legal = doc.get("legal")

            # Build doc dict for resolution service
            resolution_doc = {
                "instrument": instrument,
                "party1": party1_names[0],  # Use first party1
                "party2": None,
                "doc_type": doc_type,
                "legal": legal,
            }

            logger.info(f"Resolving Party 2 for deed {instrument}...")

            try:
                result = self.party2_service.resolve_party2(resolution_doc, self.pdf_dir)

                if result.party2:
                    doc["party2_names"] = [result.party2]
                    doc["party2_resolution_method"] = result.method
                    doc["is_self_transfer"] = result.is_self_transfer
                    doc["self_transfer_type"] = result.self_transfer_type
                    resolved_count += 1

                    if result.is_self_transfer:
                        self_transfer_count += 1
                        logger.info(f"  Self-transfer detected: {party1_names[0]} -> {result.party2}")
                    else:
                        logger.info(f"  Resolved: {result.party2} ({result.method})")
                else:
                    logger.warning(f"  Could not resolve Party 2 for {instrument}")

            except Exception as e:
                logger.error(f"  Party 2 resolution failed for {instrument}: {e}")

        if resolved_count > 0:
            logger.success(f"Resolved Party 2 for {resolved_count} deeds ({self_transfer_count} self-transfers)")

        return grouped_docs

    def _map_grouped_ori_doc(self, grouped_doc: dict, prop: Property) -> dict:
        """
        Map a grouped ORI document (with combined parties) to our schema.

        Args:
            grouped_doc: Document with party1_names and party2_names lists
            prop: Property object for folio/case_number

        Returns:
            Mapped document dict
        """
        # Parse recording date
        rec_date = None
        date_str = grouped_doc.get("record_date", "")
        if date_str:
            for fmt in ["%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"]:
                try:
                    from datetime import datetime as dt
                    parsed = dt.strptime(date_str, fmt)
                    rec_date = parsed.strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue

        # Combine party names
        party1 = ", ".join(grouped_doc.get("party1_names", []))
        party2 = ", ".join(grouped_doc.get("party2_names", []))

        return {
            "folio": prop.parcel_id,
            "case_number": prop.case_number,
            "document_type": grouped_doc.get("doc_type", "UNKNOWN"),
            "recording_date": rec_date,
            "book": grouped_doc.get("book_num"),
            "page": grouped_doc.get("page_num"),
            "instrument_number": grouped_doc.get("instrument"),
            "party1": party1,
            "party2": party2,
            "legal_description": grouped_doc.get("legal", ""),
            "extracted_data": grouped_doc,  # Store raw grouped data
            # Party 2 resolution fields
            "party2_resolution_method": grouped_doc.get("party2_resolution_method"),
            "is_self_transfer": grouped_doc.get("is_self_transfer", False),
            "self_transfer_type": grouped_doc.get("self_transfer_type"),
        }

    def _map_ori_doc(self, ori_doc: dict, prop: Property) -> dict:
        """Map ORI document to our schema. Handles both API and browser scraper formats."""

        # Handle date - browser format: "11/19/2024 11:22:50 AM", API format: timestamp
        rec_date = None
        if "record_date" in ori_doc:
            # Browser format: "MM/DD/YYYY HH:MM:SS AM/PM"
            date_str = ori_doc.get("record_date", "")
            if date_str:
                try:
                    # Parse the datetime and convert to ISO format for DuckDB
                    from datetime import datetime as dt
                    # Try parsing with time
                    for fmt in ["%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"]:
                        try:
                            parsed = dt.strptime(date_str, fmt)
                            rec_date = parsed.strftime("%Y-%m-%d")  # ISO format for DuckDB
                            break
                        except ValueError:
                            continue
                except Exception:
                    pass
        elif "RecordDate" in ori_doc:
            # API format: timestamp
            try:
                ts = ori_doc.get("RecordDate", 0)
                if ts > 100000000000:  # It's ms
                    ts = ts / 1000
                rec_date = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
            except Exception:
                pass

        # Handle document type - browser: "doc_type", API: "DocType"
        doc_type = ori_doc.get("doc_type") or ori_doc.get("DocType", "UNKNOWN")

        # Handle book/page - browser: "book_num"/"page_num", API: "Book"/"Page"
        book = ori_doc.get("book_num") or ori_doc.get("Book")
        page = ori_doc.get("page_num") or ori_doc.get("Page")

        # Handle instrument - browser: "instrument", API: "Instrument"
        instrument = ori_doc.get("instrument") or ori_doc.get("Instrument")

        # Handle parties - browser: "name"/"person_type", API: "PartiesOne"/"PartiesTwo"
        if "name" in ori_doc:
            # Browser format: single row per party
            party_type = ori_doc.get("person_type", "")
            name = ori_doc.get("name", "")
            party1 = name if "PARTY 1" in party_type else ""
            party2 = name if "PARTY 2" in party_type else ""
        else:
            # API format: arrays
            party1 = ", ".join(ori_doc.get("PartiesOne", []))
            party2 = ", ".join(ori_doc.get("PartiesTwo", []))

        # Handle legal - browser: "legal", API: "Legal"
        legal = ori_doc.get("legal") or ori_doc.get("Legal", "")

        return {
            "folio": prop.parcel_id,
            "case_number": prop.case_number,
            "document_type": doc_type,
            "recording_date": rec_date,
            "book": book,
            "page": page,
            "instrument_number": instrument,
            "party1": party1,
            "party2": party2,
            "legal_description": str(legal),
            "extracted_data": ori_doc  # Store raw data too
        }
