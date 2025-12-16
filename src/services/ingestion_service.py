from typing import Optional, Any, List, Dict
from pathlib import Path
from datetime import datetime, timezone
import json
from loguru import logger

from src.models.property import Property
from src.db.operations import PropertyDB
from src.scrapers.ori_api_scraper import ORIApiScraper
from src.services.title_chain_service import TitleChainService
from src.services.party2_resolution_service import Party2ResolutionService
from src.services.document_analyzer import DocumentAnalyzer

from src.services.scraper_storage import ScraperStorage
from src.services.institutional_names import is_institutional_name, get_searchable_party_name

# Maximum party name searches for gap-filling per property
MAX_PARTY_SEARCHES = 5

# Document types worth analyzing (download PDF and extract data)
ANALYZABLE_DOC_TYPES = {
    'D', 'WD', 'QC', 'SWD', 'TD', 'CD', 'PRD', 'CT',  # Deeds
    'MTG', 'MTGNT', 'MTGNIT', 'DOT', 'HELOC',  # Mortgages
    'LN', 'LIEN', 'JUD', 'TL', 'ML', 'HOA', 'COD', 'MECH',  # Liens
    'SAT', 'REL', 'SATMTG', 'RELMTG',  # Satisfactions
    'ASG', 'ASGN', 'ASGNMTG', 'ASSIGN', 'ASINT',  # Assignments
    'LP', 'LISPEN',  # Lis Pendens
    'NOC',  # Notice of Commencement
    'AFF', 'AFFD',  # Affidavits
}


class IngestionService:
    def __init__(self, ori_scraper: ORIApiScraper = None, analyze_pdfs: bool = True):
        self.db = PropertyDB()
        # Share ORI scraper across services to avoid multiple browser sessions
        self.ori_scraper = ori_scraper or ORIApiScraper()
        self.chain_service = TitleChainService()
        # Pass shared ORI scraper to Party2ResolutionService
        self.party2_service = Party2ResolutionService(ori_scraper=self.ori_scraper)
        self.storage = ScraperStorage()
        # Document analyzer for PDF extraction
        self.analyze_pdfs = analyze_pdfs
        self.doc_analyzer = DocumentAnalyzer() if analyze_pdfs else None

    def ingest_property(self, prop: Property, raw_docs: list | None = None):
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
        # Track whether docs came from browser (affects PDF download strategy)
        docs_from_browser = False

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

            # Extract filter info and search terms
            filter_info = None
            actual_search_terms = []
            for term in search_terms:
                if isinstance(term, tuple) and term[0] == "__filter__":
                    filter_info = term[1]
                else:
                    actual_search_terms.append(term)

            successful_term = None

            # STRATEGY: Prefer browser search for lot/block specific terms (contain wildcards)
            # because browser search supports wildcards and returns unlimited results.
            # The API doesn't support wildcards and is limited to 25 results.

            # Check if first term has wildcard (our ORI-optimized lot/block format)
            first_term = actual_search_terms[0] if actual_search_terms else ""
            use_browser_first = "*" in first_term

            if use_browser_first:
                # Browser search with wildcard terms (e.g., "L 44 B 2 SYMPHONY*")
                logger.info("Using browser search for wildcard terms (ORI-optimized format)")
                for term in actual_search_terms:
                    logger.info(f"Searching ORI browser for: {term}")
                    try:
                        docs = self.ori_scraper.search_by_legal_sync(term, headless=True)
                        if docs:
                            successful_term = term
                            docs_from_browser = True
                            logger.info(f"Found {len(docs)} documents via browser with term: {term}")
                            break
                    except Exception as e:
                        logger.warning(f"Browser search failed for '{term}': {e}")

                # Fall back to API if browser failed (strip wildcards for API)
                if not docs:
                    logger.info("Browser returned no results, trying API...")
                    for term in actual_search_terms:
                        api_term = term.rstrip("*")  # API doesn't use wildcards
                        logger.info(f"Searching ORI API for: {api_term}")
                        try:
                            docs = self.ori_scraper.search_by_legal(api_term)
                            if docs:
                                successful_term = term
                                docs_from_browser = False
                                logger.info(f"Found {len(docs)} documents via API with term: {api_term}")
                                break
                        except Exception as e:
                            logger.warning(f"API search failed for '{api_term}': {e}")
            else:
                # Non-wildcard terms: try API first (original behavior)
                for term in actual_search_terms:
                    logger.info(f"Searching ORI API for: {term}")
                    try:
                        docs = self.ori_scraper.search_by_legal(term)
                        if docs:
                            successful_term = term
                            docs_from_browser = False
                            logger.info(f"Found {len(docs)} documents via API with term: {term}")
                            break
                    except Exception as e:
                        logger.warning(f"API search failed for '{term}': {e}")

                # Fall back to browser search if API returned nothing
                if not docs:
                    logger.info("API returned no results, trying browser search...")
                    for term in actual_search_terms:
                        try:
                            docs = self.ori_scraper.search_by_legal_sync(term, headless=True)
                            if docs:
                                successful_term = term
                                docs_from_browser = True
                                logger.info(f"Found {len(docs)} documents via browser with term: {term}")
                                break
                        except Exception as e:
                            logger.warning(f"Browser search failed for '{term}': {e}")

            # Filter results by lot/block if filter info provided
            if docs and filter_info:
                docs = self._filter_docs_by_lot_block(docs, filter_info)
                logger.info(f"After lot/block filtering: {len(docs)} documents")

            if not docs:
                logger.warning(f"No documents found after trying {len(search_terms)} search terms.")
                return

            # Log successful search term for future reference
            if successful_term:
                logger.info(f"Successful search term: {successful_term}")
        else:
            logger.info(f"Using {len(docs)} pre-fetched ORI records")
            # Pre-fetched docs typically come from browser in pipeline
            docs_from_browser = True

        # Group raw ORI records by instrument number (ORI returns one row per party)
        grouped_docs = self._group_ori_records_by_instrument(docs)
        logger.info(f"Grouped {len(docs)} raw records into {len(grouped_docs)} unique documents")

        # NOTE: We no longer call _resolve_missing_party2 here - instead we extract
        # parties from the PDF using vLLM which is more reliable

        processed_docs = []

        for doc in grouped_docs:
            # Map grouped ORI doc to our schema
            mapped_doc = self._map_grouped_ori_doc(doc, prop)

            # Download and analyze PDF for ALL document types (not just "analyzable")
            # This extracts party data from the PDF which is more reliable than ORI indexing
            if self.analyze_pdfs:
                download_result = self._download_and_analyze_document(doc, prop.parcel_id, prefer_browser=docs_from_browser)
                if download_result:
                    # Always set file_path if download succeeded
                    mapped_doc['file_path'] = download_result.get('file_path')
                    # Set extracted data if vision analysis succeeded
                    if download_result.get('extracted_data'):
                        mapped_doc['vision_extracted_data'] = download_result['extracted_data']
                        # Update party1/party2 from vLLM extraction if missing
                        mapped_doc = self._update_parties_from_extraction(mapped_doc, download_result['extracted_data'])

            # Save to DB (after party updates)
            doc_id = self.db.save_document(prop.parcel_id, mapped_doc)
            mapped_doc['id'] = doc_id

            # Update extracted data in DB if we have it
            if mapped_doc.get('vision_extracted_data'):
                self._update_document_with_extracted_data(doc_id, mapped_doc['vision_extracted_data'])

            processed_docs.append(mapped_doc)

        # Track instruments we've found so far
        existing_instruments = {
            str(doc.get('instrument_number', '')) for doc in processed_docs
            if doc.get('instrument_number')
        }

        # 2. Build Chain
        logger.info("Building Chain of Title...")
        analysis = self.chain_service.build_chain_and_analyze(processed_docs)

        # 3. Gap-filling: If chain has gaps, search by party name
        gaps = analysis.get('gaps', [])
        if gaps:
            logger.info(f"Chain has {len(gaps)} gaps, attempting gap-fill...")
            gap_docs = self._fill_chain_gaps(gaps, existing_instruments, filter_info, prop)

            if gap_docs:
                # Process new documents
                new_grouped = self._group_ori_records_by_instrument(gap_docs)
                for doc in new_grouped:
                    mapped_doc = self._map_grouped_ori_doc(doc, prop)

                    if self.analyze_pdfs:
                        download_result = self._download_and_analyze_document(doc, prop.parcel_id, prefer_browser=docs_from_browser)
                        if download_result:
                            mapped_doc['file_path'] = download_result.get('file_path')
                            if download_result.get('extracted_data'):
                                mapped_doc['vision_extracted_data'] = download_result['extracted_data']
                                mapped_doc = self._update_parties_from_extraction(mapped_doc, download_result['extracted_data'])

                    doc_id = self.db.save_document(prop.parcel_id, mapped_doc)
                    mapped_doc['id'] = doc_id

                    if mapped_doc.get('vision_extracted_data'):
                        self._update_document_with_extracted_data(doc_id, mapped_doc['vision_extracted_data'])

                    processed_docs.append(mapped_doc)

                # Rebuild chain with new documents
                logger.info("Rebuilding chain with gap-fill documents...")
                analysis = self.chain_service.build_chain_and_analyze(processed_docs)

        # 4. Verify current owner against HCPA
        chain_owner = analysis.get('summary', {}).get('current_owner', '')
        hcpa_owner = getattr(prop, 'owner_name', None)

        if chain_owner and chain_owner != "Unknown":
            owner_docs = self._verify_current_owner(chain_owner, hcpa_owner, filter_info, existing_instruments)

            if owner_docs:
                # Process new owner documents
                new_grouped = self._group_ori_records_by_instrument(owner_docs)
                for doc in new_grouped:
                    mapped_doc = self._map_grouped_ori_doc(doc, prop)

                    if self.analyze_pdfs:
                        download_result = self._download_and_analyze_document(doc, prop.parcel_id, prefer_browser=docs_from_browser)
                        if download_result:
                            mapped_doc['file_path'] = download_result.get('file_path')
                            if download_result.get('extracted_data'):
                                mapped_doc['vision_extracted_data'] = download_result['extracted_data']
                                mapped_doc = self._update_parties_from_extraction(mapped_doc, download_result['extracted_data'])

                    doc_id = self.db.save_document(prop.parcel_id, mapped_doc)
                    mapped_doc['id'] = doc_id

                    if mapped_doc.get('vision_extracted_data'):
                        self._update_document_with_extracted_data(doc_id, mapped_doc['vision_extracted_data'])

                    processed_docs.append(mapped_doc)

                # Rebuild chain with owner verification documents
                logger.info("Rebuilding chain with owner verification documents...")
                analysis = self.chain_service.build_chain_and_analyze(processed_docs)

        # Transform for DB
        db_data = self._transform_analysis_for_db(analysis)

        # 5. Save Analysis
        self.db.save_chain_of_title(prop.parcel_id, db_data)
        logger.success(f"Ingestion complete for {prop.case_number} ({len(processed_docs)} total docs)")

    def ingest_property_by_party(
        self,
        prop: Property,
        plaintiff: Optional[str] = None,
        defendant: Optional[str] = None
    ):
        """
        Ingest ORI documents for a property by searching by party name.

        This is used as a fallback when the folio/parcel_id is invalid (e.g., mobile home
        foreclosures where the borrower doesn't own the land).

        Searches ORI for documents where the plaintiff or defendant appears as a party,
        then filters to relevant document types.

        Args:
            prop: Property object (parcel_id may be invalid, use case_number as identifier)
            plaintiff: Plaintiff name (Party 1 / foreclosing party)
            defendant: Defendant name (Party 2 / borrower)
        """
        if not plaintiff and not defendant:
            logger.warning(f"No party names provided for {prop.case_number}, cannot search ORI")
            return

        logger.info(f"Ingesting ORI by party for case {prop.case_number}")
        if plaintiff:
            logger.info(f"  Plaintiff: {plaintiff}")
        if defendant:
            logger.info(f"  Defendant: {defendant}")

        all_docs: List[dict] = []

        # Search by defendant first (borrower - more likely to have title documents)
        if defendant:
            search_name = self._normalize_party_name_for_search(defendant)
            if search_name:
                logger.info(f"Searching ORI by defendant: {search_name}")
                try:
                    docs = self.ori_scraper.search_by_party(search_name)
                    if docs:
                        logger.info(f"Found {len(docs)} documents for defendant")
                        all_docs.extend(docs)
                except Exception as e:
                    logger.warning(f"Party search failed for defendant '{search_name}': {e}")

        # Also search by plaintiff (may find lis pendens, mortgages)
        if plaintiff:
            search_name = self._normalize_party_name_for_search(plaintiff)
            if search_name:
                logger.info(f"Searching ORI by plaintiff: {search_name}")
                try:
                    docs = self.ori_scraper.search_by_party(search_name)
                    if docs:
                        logger.info(f"Found {len(docs)} documents for plaintiff")
                        all_docs.extend(docs)
                except Exception as e:
                    logger.warning(f"Party search failed for plaintiff '{search_name}': {e}")

        if not all_docs:
            logger.warning(f"No ORI documents found by party search for {prop.case_number}")
            return

        # Group by instrument to dedupe
        grouped_docs = self._group_ori_records_by_instrument(all_docs)
        logger.info(f"Grouped {len(all_docs)} records into {len(grouped_docs)} unique documents")

        # Process documents (same as standard ingestion)
        processed_docs = []

        for doc in grouped_docs:
            mapped_doc = self._map_grouped_ori_doc(doc, prop)

            if self.analyze_pdfs:
                download_result = self._download_and_analyze_document(doc, prop.parcel_id or prop.case_number)
                if download_result:
                    mapped_doc['file_path'] = download_result.get('file_path')
                    if download_result.get('extracted_data'):
                        mapped_doc['vision_extracted_data'] = download_result['extracted_data']
                        mapped_doc = self._update_parties_from_extraction(mapped_doc, download_result['extracted_data'])

            doc_id = self.db.save_document(prop.parcel_id or prop.case_number, mapped_doc)
            mapped_doc['id'] = doc_id

            if mapped_doc.get('vision_extracted_data'):
                self._update_document_with_extracted_data(doc_id, mapped_doc['vision_extracted_data'])

            processed_docs.append(mapped_doc)

        # Build Chain of Title
        logger.info("Building Chain of Title from party search results...")
        analysis = self.chain_service.build_chain_and_analyze(processed_docs)
        db_data = self._transform_analysis_for_db(analysis)
        self.db.save_chain_of_title(prop.parcel_id or prop.case_number, db_data)

        logger.success(f"Party-based ingestion complete for {prop.case_number} ({len(processed_docs)} docs)")

    def _normalize_party_name_for_search(self, name: str) -> Optional[str]:
        """
        Normalize a party name for ORI search.

        Handles:
        - Converting "LASTNAME, FIRSTNAME" to "LASTNAME FIRSTNAME"
        - Removing common suffixes like "et al", "a/k/a", etc.
        - Truncating at A/K/A, F/K/A patterns (take only first name)
        - Adding wildcard for partial matches

        Args:
            name: Raw party name from auction data

        Returns:
            Normalized search string or None if name is unusable
        """
        if not name or len(name.strip()) < 3:
            return None

        import re

        # Clean up the name
        search = name.strip().upper()

        # Truncate at A/K/A, F/K/A, D/B/A patterns (keep only the primary name)
        # These indicate aliases - we want the primary name before the alias
        for pattern in [' A/K/A ', ' AKA ', ' F/K/A ', ' FKA ', ' D/B/A ', ' DBA ']:
            if pattern in search:
                search = search.split(pattern)[0].strip()
                break

        # Remove common legal suffixes at the end
        suffixes_to_remove = [
            " ET AL", " ET AL.", " ET UX", " ET VIR",
            " A/K/A", " AKA", " F/K/A", " FKA",
            " D/B/A", " DBA", " AS TRUSTEE",
            " INDIVIDUALLY", " AND ALL", " AND",
        ]
        for suffix in suffixes_to_remove:
            if search.endswith(suffix):
                search = search[:-len(suffix)].strip()

        # Remove any content in parentheses
        search = re.sub(r'\([^)]*\)', '', search).strip()

        # If name has comma (LAST, FIRST format), convert to LAST FIRST
        if ',' in search:
            parts = search.split(',', 1)
            search = f"{parts[0].strip()} {parts[1].strip()}"

        # Clean up multiple spaces
        search = ' '.join(search.split())

        if len(search) < 3:
            return None

        # Add wildcard for partial matching
        if not search.endswith('*'):
            search += '*'

        return search

    async def ingest_property_async(self, prop: Property, raw_docs: list | None = None):
        """
        Async version of ingest_property for use in async batch processing.

        Avoids event loop conflicts by using async Party 2 resolution.

        Args:
            prop: Property object with case_number, parcel_id, legal_description
            raw_docs: Optional pre-fetched ORI documents. If provided, skips ORI search.
        """
        logger.info(f"Ingesting property {prop.case_number} (Folio: {prop.parcel_id})")

        # 1. Use pre-fetched docs if provided, otherwise search ORI
        docs = raw_docs or []
        # Async version always uses browser search, so prefer_browser is always True
        docs_from_browser = True

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
                    # Use browser-based search (async) to avoid API blocking
                    docs = await self.ori_scraper.search_by_legal_browser(search_term, headless=True)
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

        # NOTE: We no longer call _resolve_missing_party2_async here - instead we extract
        # parties from the PDF using vLLM which is more reliable

        processed_docs = []

        for doc in grouped_docs:
            # Map grouped ORI doc to our schema
            mapped_doc = self._map_grouped_ori_doc(doc, prop)

            # Download and analyze PDF for ALL document types
            # This extracts party data from the PDF which is more reliable than ORI indexing
            if self.analyze_pdfs:
                download_result = self._download_and_analyze_document(doc, prop.parcel_id, prefer_browser=docs_from_browser)
                if download_result:
                    # Always set file_path if download succeeded
                    mapped_doc['file_path'] = download_result.get('file_path')
                    # Set extracted data if vision analysis succeeded
                    if download_result.get('extracted_data'):
                        mapped_doc['vision_extracted_data'] = download_result['extracted_data']
                        # Update party1/party2 from vLLM extraction if missing
                        mapped_doc = self._update_parties_from_extraction(mapped_doc, download_result['extracted_data'])

            # Save to DB (after party updates)
            doc_id = self.db.save_document(prop.parcel_id, mapped_doc)
            mapped_doc['id'] = doc_id

            # Update extracted data in DB if we have it
            if mapped_doc.get('vision_extracted_data'):
                self._update_document_with_extracted_data(doc_id, mapped_doc['vision_extracted_data'])

            processed_docs.append(mapped_doc)

        # 2. Build Chain
        logger.info("Building Chain of Title...")
        analysis = self.chain_service.build_chain_and_analyze(processed_docs)

        # Transform for DB
        db_data = self._transform_analysis_for_db(analysis)

        # 3. Save Analysis
        self.db.save_chain_of_title(prop.parcel_id, db_data)
        logger.success(f"Ingestion complete for {prop.case_number}")

    def _should_analyze_document(self, doc: dict) -> bool:
        """Check if document type is worth downloading and analyzing."""
        doc_type = doc.get('DocType', '')
        # Extract just the code: "(MTG) MORTGAGE" -> "MTG"
        code = doc_type.replace('(', '').replace(')', '').split()[0].upper() if doc_type else ''
        return code in ANALYZABLE_DOC_TYPES

    def _download_and_analyze_document(self, doc: dict, folio: str, prefer_browser: bool = False) -> Optional[dict]:
        """Download PDF and extract structured data using vision analysis.

        Args:
            doc: Document dict with instrument number and optional ID
            folio: Property folio for storage path
            prefer_browser: If True, use browser-based PDF download instead of API.
                           Set when docs came from browser search to avoid API rate limits.

        Returns:
            Dict with 'file_path' and optionally 'extracted_data' if vision analysis succeeded.
            Returns None if PDF download failed.
        """
        # Support both grouped doc format (lowercase) and raw ORI format (uppercase)
        instrument = doc.get('instrument') or doc.get('Instrument', '')
        doc_type = doc.get('doc_type') or doc.get('DocType', 'UNKNOWN')

        if not instrument:
            return None

        try:
            # Create output directory
            output_dir = Path(f"data/properties/{folio}/documents")
            output_dir.mkdir(parents=True, exist_ok=True)

            # Build ORI doc format for download_pdf (needs ID or instrument)
            ori_doc = {
                'Instrument': instrument,
                'DocType': doc_type,
                'ID': doc.get('ID') or doc.get('id'),  # May not have ID yet
            }

            # Download PDF - use browser if docs came from browser search
            pdf_path = self.ori_scraper.download_pdf(ori_doc, output_dir, prefer_browser=prefer_browser)
            if not pdf_path or not pdf_path.exists():
                logger.debug(f"Could not download PDF for {instrument}")
                return None

            logger.info(f"Analyzing PDF: {pdf_path.name}")

            # Start result with file path (always set if download succeeded)
            result = {'file_path': str(pdf_path)}

            # Analyze with vision service
            extracted_data = self.doc_analyzer.analyze_document(str(pdf_path), doc_type, instrument)
            if extracted_data:
                logger.success(f"Extracted data from {doc_type}: {instrument}")
                result['extracted_data'] = extracted_data

            return result

        except Exception as e:
            logger.warning(f"Error analyzing {instrument}: {e}")
            return None

    def _update_parties_from_extraction(self, mapped_doc: dict, extracted_data: dict) -> dict:
        """
        Update party1/party2 fields from vLLM extraction if they are missing.

        For deeds: grantor -> party1, grantee -> party2
        For mortgages: borrower -> party1, lender -> party2
        For liens: debtor -> party1, creditor -> party2
        """
        doc_type = (mapped_doc.get('document_type') or '').upper()

        # Determine which extraction fields to use based on document type
        if 'DEED' in doc_type or doc_type in ['D', 'WD', 'QC', 'TD', 'CD']:
            party1_field = 'grantor'
            party2_field = 'grantee'
        elif 'MORTGAGE' in doc_type or 'MTG' in doc_type:
            party1_field = 'borrower'
            party2_field = 'lender'
        elif 'LIEN' in doc_type or 'JUDGMENT' in doc_type:
            party1_field = 'debtor'
            party2_field = 'creditor'
        elif 'SATISFACTION' in doc_type or 'SAT' in doc_type:
            party1_field = 'releasing_party'
            party2_field = 'released_party'
        elif 'ASSIGNMENT' in doc_type or 'ASG' in doc_type:
            party1_field = 'assignor'
            party2_field = 'assignee'
        else:
            # Generic fallback
            party1_field = 'grantor'
            party2_field = 'grantee'

        # Update party1 if missing
        if not mapped_doc.get('party1') or mapped_doc['party1'].strip() == '':
            extracted_party1 = extracted_data.get(party1_field) or extracted_data.get('party1')
            if extracted_party1:
                mapped_doc['party1'] = extracted_party1
                logger.info(f"  Updated party1 from vLLM: {extracted_party1[:50]}")

        # Update party2 if missing
        if not mapped_doc.get('party2') or mapped_doc['party2'].strip() == '':
            extracted_party2 = extracted_data.get(party2_field) or extracted_data.get('party2')
            if extracted_party2:
                mapped_doc['party2'] = extracted_party2
                logger.info(f"  Updated party2 from vLLM: {extracted_party2[:50]}")

        return mapped_doc

    def _update_document_with_extracted_data(self, doc_id: int, extracted_data: dict):
        """Update document record with vision-extracted data."""
        try:
            # Store as JSON in extracted_data field
            self.db.conn.execute("""
                UPDATE documents
                SET extracted_data = ?
                WHERE id = ?
            """, [json.dumps(extracted_data), doc_id])
            self.db.conn.commit()
        except Exception as e:
            logger.warning(f"Failed to update document {doc_id} with extracted data: {e}")

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
                if enc_date and start_date and enc_date >= start_date and (end_date is None or enc_date < end_date):
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

        # Handle case where there are encumbrances but no chain (no deeds found)
        # Create a catch-all period so encumbrances aren't lost
        if not chain and encumbrances:
            logger.warning(f"No chain but {len(encumbrances)} encumbrances found - creating catch-all period")
            timeline.append({
                "owner": "Unknown (No Deed Found)",
                "acquired_from": None,
                "acquisition_date": None,
                "disposition_date": None,
                "acquisition_instrument": None,
                "acquisition_doc_type": "UNKNOWN",
                "acquisition_price": None,
                "encumbrances": [self._map_encumbrance(e) for e in encumbrances]
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
        except ValueError:
            try:
                return datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                return None

    def _parse_amount(self, amt: Any) -> float:
        if not amt or amt == 'Unknown': return 0.0
        try:
            return float(str(amt).replace('$', '').replace(',', ''))
        except ValueError:
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

    def _filter_docs_by_lot_block(self, docs: List[Dict], filter_info: Dict) -> List[Dict]:
        """
        Filter ORI documents to only those matching the specified lot/block.

        ORI legal descriptions use various formats:
        - "L 1 B 1 SUBDIVISION NAME"
        - "L  1  B  1  SUBDIVISION NAME"
        - "L1 B1 SUBDIVISION NAME"
        - "LOT 1 BLOCK 1 SUBDIVISION NAME"

        Args:
            docs: List of ORI documents with 'Legal' field
            filter_info: Dict with 'lot', 'block', 'subdivision' keys

        Returns:
            Filtered list of documents matching the lot/block
        """
        import re

        lot = filter_info.get("lot")
        block = filter_info.get("block")

        if not lot and not block:
            return docs

        filtered = []
        for doc in docs:
            # Handle both API format (Legal) and browser format (legal)
            legal = doc.get("Legal") or doc.get("legal") or ""
            legal_upper = legal.upper()

            # Check lot match - various formats: "L 1", "L1", "LOT 1"
            lot_match = True
            if lot:
                # Pattern matches L/LOT followed by the lot number
                lot_pattern = rf'\bL(?:OT)?\s*{re.escape(lot)}\b'
                lot_match = bool(re.search(lot_pattern, legal_upper))

            # Check block match - various formats: "B 1", "B1", "BLOCK 1", "BLK 1"
            block_match = True
            if block:
                # Pattern matches B/BLK/BLOCK followed by the block number
                block_pattern = rf'\bB(?:LK|LOCK)?\s*{re.escape(block)}\b'
                block_match = bool(re.search(block_pattern, legal_upper))

            if lot_match and block_match:
                filtered.append(doc)

        logger.info(f"Filtered {len(docs)} -> {len(filtered)} docs (lot={lot}, block={block})")
        return filtered

    def _fill_chain_gaps(
        self,
        gaps: List,
        existing_instruments: set,
        filter_info: Optional[dict],
        prop: "Property"
    ) -> List[Dict]:
        """
        Attempt to fill gaps in the chain of title by searching ORI by party name.

        This searches for documents where gap parties appear, filters by lot/block,
        and returns new documents that weren't already found.

        Args:
            gaps: List of ChainGap objects from TitleChainService
            existing_instruments: Set of instrument numbers already retrieved
            filter_info: Dict with lot/block/subdivision for filtering results
            prop: Property object for logging

        Returns:
            List of new ORI documents that may fill gaps
        """
        if not gaps:
            return []

        new_docs = []
        searches_performed = 0
        searched_names = set()  # Avoid duplicate searches

        logger.info(f"Attempting to fill {len(gaps)} chain gaps for {prop.case_number}")

        for gap in gaps:
            if searches_performed >= MAX_PARTY_SEARCHES:
                logger.warning(f"Reached max party searches ({MAX_PARTY_SEARCHES}), stopping gap-fill")
                break

            for party_name in gap.searchable_names:
                if searches_performed >= MAX_PARTY_SEARCHES:
                    break

                # Skip if we already searched this name
                name_key = party_name.upper().strip()
                if name_key in searched_names:
                    continue

                # Double-check it's not institutional (defensive)
                if is_institutional_name(party_name):
                    logger.debug(f"Skipping institutional name: {party_name}")
                    continue

                searched_names.add(name_key)
                searches_performed += 1

                logger.info(f"Gap-fill search #{searches_performed}: {party_name}")

                try:
                    # Search ORI by party name
                    results = self.ori_scraper.search_by_party(party_name)

                    if not results:
                        logger.debug(f"No results for party: {party_name}")
                        continue

                    logger.info(f"Found {len(results)} docs for party: {party_name}")

                    # Filter by lot/block if we have filter info
                    if filter_info:
                        results = self._filter_docs_by_lot_block(results, filter_info)
                        logger.info(f"After lot/block filter: {len(results)} docs")

                    # Filter out documents we already have
                    for doc in results:
                        instrument = str(doc.get("Instrument", ""))
                        if instrument and instrument not in existing_instruments:
                            new_docs.append(doc)
                            existing_instruments.add(instrument)

                except Exception as e:
                    logger.warning(f"Gap-fill search failed for '{party_name}': {e}")

        if new_docs:
            logger.success(f"Gap-fill found {len(new_docs)} new documents after {searches_performed} searches")
        else:
            logger.info(f"Gap-fill completed {searches_performed} searches, no new documents found")

        return new_docs

    def _verify_current_owner(
        self,
        chain_owner: str,
        hcpa_owner: Optional[str],
        filter_info: Optional[dict],
        existing_instruments: set,
    ) -> List[Dict]:
        """
        Verify the chain endpoint matches HCPA owner, search by owner name if not.

        When the chain's final grantee doesn't match the HCPA owner, there may be
        a recent deed not yet found. Search by the HCPA owner name to find it.

        Args:
            chain_owner: Current owner according to chain of title
            hcpa_owner: Current owner according to HCPA records
            filter_info: Dict with lot/block for filtering results
            existing_instruments: Set of already-found instruments

        Returns:
            List of new ORI documents that may update ownership
        """
        if not hcpa_owner or not chain_owner:
            return []

        # Normalize names for comparison
        chain_clean = chain_owner.upper().strip()
        hcpa_clean = hcpa_owner.upper().strip()

        # Check if names match (simple check - could be more sophisticated)
        if chain_clean == hcpa_clean:
            logger.debug(f"Chain owner matches HCPA owner: {chain_owner}")
            return []

        # Check if significant words match
        chain_words = set(chain_clean.replace(",", "").split())
        hcpa_words = set(hcpa_clean.replace(",", "").split())
        common = chain_words & hcpa_words
        stopwords = {'LLC', 'INC', 'CORP', 'THE', 'OF', 'AND', 'TRUST', 'TRUSTEE'}
        significant = common - stopwords

        if len(significant) >= 1:
            logger.debug(f"Chain owner likely matches HCPA owner (shared: {significant})")
            return []

        # Names don't match - search by HCPA owner
        logger.info(f"Chain owner '{chain_owner}' != HCPA owner '{hcpa_owner}', searching for recent deed")

        # Get searchable name (skip if institutional)
        search_name = get_searchable_party_name(hcpa_owner)
        if not search_name:
            logger.debug(f"HCPA owner '{hcpa_owner}' is institutional or invalid, skipping search")
            return []

        try:
            results = self.ori_scraper.search_by_party(search_name)
            if not results:
                logger.debug(f"No results for HCPA owner: {search_name}")
                return []

            logger.info(f"Found {len(results)} docs for HCPA owner")

            # Filter by lot/block
            if filter_info:
                results = self._filter_docs_by_lot_block(results, filter_info)
                logger.info(f"After lot/block filter: {len(results)} docs")

            # Filter to new documents only
            new_docs = []
            for doc in results:
                instrument = str(doc.get("Instrument", ""))
                if instrument and instrument not in existing_instruments:
                    new_docs.append(doc)
                    existing_instruments.add(instrument)

            if new_docs:
                logger.success(f"Found {len(new_docs)} new docs for HCPA owner verification")

            return new_docs

        except Exception as e:
            logger.warning(f"HCPA owner search failed: {e}")
            return []

    def _group_ori_records_by_instrument(self, docs: list) -> list:
        """
        Group ORI records by instrument number.

        Handles two formats:
        1. Browser format: one row per party with person_type, name, instrument, etc.
        2. API format: one row per document with PartiesOne/PartiesTwo lists, ID, etc.

        Args:
            docs: Raw ORI records from browser or API search

        Returns:
            List of grouped documents with party1_names, party2_names, and ID
        """
        by_instrument = {}

        for doc in docs:
            # Get instrument number (key for grouping)
            instrument = str(doc.get("instrument") or doc.get("Instrument", ""))
            if not instrument:
                continue

            # Check if this is API format (has PartiesOne/PartiesTwo) or browser format
            is_api_format = "PartiesOne" in doc or "PartiesTwo" in doc

            # Initialize or update group
            if instrument not in by_instrument:
                by_instrument[instrument] = {
                    "instrument": instrument,
                    "doc_type": doc.get("doc_type") or doc.get("DocType", ""),
                    "record_date": doc.get("record_date") or doc.get("RecordDate", ""),
                    "book_num": doc.get("book_num") or doc.get("BookNum", ""),
                    "page_num": doc.get("page_num") or doc.get("PageNum", ""),
                    "legal": doc.get("legal") or doc.get("Legal", ""),
                    "party1_names": [],  # PARTY 1 = Grantor/Mortgagor/Debtor
                    "party2_names": [],  # PARTY 2 = Grantee/Mortgagee/Creditor
                    "ID": doc.get("ID"),  # API returns document ID for PDF download
                    # Additional ORI API fields
                    "sales_price": doc.get("SalesPrice"),  # Sale price or loan amount
                    "page_count": doc.get("PageCount"),    # Number of pages in document
                    "uuid": doc.get("UUID"),               # Unique document identifier
                    "book_type": doc.get("BookType"),      # Book type (OR, etc.)
                }

            if is_api_format:
                # API format: PartiesOne and PartiesTwo are lists
                parties_one = doc.get("PartiesOne", []) or []
                parties_two = doc.get("PartiesTwo", []) or []

                for name in parties_one:
                    if name and name not in by_instrument[instrument]["party1_names"]:
                        by_instrument[instrument]["party1_names"].append(name)

                for name in parties_two:
                    if name and name not in by_instrument[instrument]["party2_names"]:
                        by_instrument[instrument]["party2_names"].append(name)

                # Preserve the ID from API
                if doc.get("ID") and not by_instrument[instrument].get("ID"):
                    by_instrument[instrument]["ID"] = doc.get("ID")

            else:
                # Browser format: one row per party
                person_type = doc.get("person_type", "").upper()
                name = doc.get("name", "").strip()

                if name and ("PARTY 1" in person_type or "GRANTOR" in person_type):
                    if name not in by_instrument[instrument]["party1_names"]:
                        by_instrument[instrument]["party1_names"].append(name)
                elif name and ("PARTY 2" in person_type or "GRANTEE" in person_type) and name not in by_instrument[instrument]["party2_names"]:
                    by_instrument[instrument]["party2_names"].append(name)

        return list(by_instrument.values())

    def _resolve_missing_party2(self, grouped_docs: list, prop: Property) -> list:
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
                # Use property-specific documents folder
                doc_dir = self.storage.get_full_path(prop.parcel_id, "documents")
                doc_dir.mkdir(parents=True, exist_ok=True)
                
                result = self.party2_service.resolve_party2(resolution_doc, doc_dir)

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

    async def _resolve_missing_party2_async(self, grouped_docs: list, prop: Property) -> list:
        """
        Async version of _resolve_missing_party2 for batch processing.

        Uses async Party 2 resolution to avoid event loop conflicts when called
        from an async context (e.g., batch processing with browser automation).

        Args:
            grouped_docs: List of grouped documents with party1_names/party2_names
            prop: Property object for storage paths

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
                # Use property-specific documents folder
                doc_dir = self.storage.get_full_path(prop.parcel_id, "documents")
                doc_dir.mkdir(parents=True, exist_ok=True)

                # Use async version to avoid event loop conflicts
                result = await self.party2_service.resolve_party2_async(resolution_doc, doc_dir)

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
        # Parse recording date - handle both timestamp (API) and string (browser) formats
        rec_date = None
        date_val = grouped_doc.get("record_date", "")
        if date_val:
            from datetime import datetime as dt
            # Check if it's a Unix timestamp (int or float)
            if isinstance(date_val, (int, float)):
                try:
                    parsed = dt.fromtimestamp(date_val, tz=timezone.utc)
                    rec_date = parsed.strftime("%Y-%m-%d")
                except (ValueError, OSError):
                    pass
            elif isinstance(date_val, str):
                # Try string date formats
                for fmt in ["%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"]:
                    try:
                        parsed = dt.strptime(date_val, fmt)
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
            # ORI API additional fields
            "sales_price": grouped_doc.get("sales_price"),
            "page_count": grouped_doc.get("page_count"),
            "ori_uuid": grouped_doc.get("uuid"),
            "ori_id": grouped_doc.get("ID"),
            "book_type": grouped_doc.get("book_type"),
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
                except Exception as exc:
                    logger.debug("Could not parse record_date for %s: %s", prop.case_number, exc)
        elif "RecordDate" in ori_doc:
            # API format: timestamp
            try:
                ts = ori_doc.get("RecordDate", 0)
                if ts > 100000000000:  # It's ms
                    ts = ts / 1000
                rec_date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
            except Exception as exc:
                logger.debug("Could not parse timestamp %s for %s: %s", ts, prop.case_number, exc)

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
