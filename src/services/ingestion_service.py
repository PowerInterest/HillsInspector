from typing import Optional, Any, List, Dict
from pathlib import Path
from datetime import UTC, date, datetime
import json
import re
from loguru import logger

from src.models.property import Property
from src.db.operations import PropertyDB
from src.scrapers.ori_api_scraper import ORIApiScraper
from src.services.title_chain_service import TitleChainService
from src.services.party2_resolution_service import Party2ResolutionService
from src.services.document_analyzer import DocumentAnalyzer
from src.services.vision_service import VisionService
import asyncio
import functools

from src.services.scraper_storage import ScraperStorage
from src.services.institutional_names import is_institutional_name, get_searchable_party_name
from src.utils.relevance_checker import verify_document_relevance

# Maximum party name searches for gap-filling per property
MAX_PARTY_SEARCHES = 5

# Document types worth downloading (all recordable docs relevant to title)
ANALYZABLE_DOC_TYPES = {
    "D",
    "WD",
    "QC",
    "SWD",
    "TD",
    "CD",
    "PRD",
    "CT",  # Deeds
    "MTG",
    "MTGNT",
    "MTGNIT",
    "DOT",
    "HELOC",  # Mortgages
    "LN",
    "LIEN",
    "JUD",
    "TL",
    "ML",
    "HOA",
    "COD",
    "MECH",  # Liens
    "SAT",
    "REL",
    "SATMTG",
    "RELMTG",  # Satisfactions
    "ASG",
    "ASGN",
    "ASGNMTG",
    "ASSIGN",
    "ASINT",  # Assignments
    "LP",
    "LISPEN",  # Lis Pendens
    "NOC",  # Notice of Commencement
    "AFF",
    "AFFD",  # Affidavits
}

# Document types where vision extraction adds financial data (amounts, rates, etc.)
# Other types in ANALYZABLE_DOC_TYPES are downloaded but skip vision — ORI metadata
# already provides parties, dates, and instrument numbers which is sufficient.
VISION_EXTRACT_DOC_TYPES = {
    "D",
    "WD",
    "QC",
    "SWD",
    "TD",
    "CD",
    "PRD",
    "CT",  # Deeds (sale price)
    "MTG",
    "MTGNT",
    "MTGNIT",
    "DOT",
    "HELOC",  # Mortgages (principal, rate, MERS)
    "LN",
    "LIEN",
    "JUD",
    "TL",
    "ML",
    "HOA",
    "COD",
    "MECH",  # Liens (amounts)
    "SAT",
    "REL",
    "SATMTG",
    "RELMTG",  # Satisfactions (which instrument satisfied)
    "LP",
    "LISPEN",  # Lis Pendens (case details)
}


class IngestionService:
    def __init__(
        self,
        ori_scraper: ORIApiScraper | None = None,
        analyze_pdfs: bool = True,
        db_writer: Any = None,
        db: PropertyDB | None = None,
        storage: ScraperStorage | None = None,
    ):
        self.db = db or PropertyDB()
        self.db_writer = db_writer
        # Share ORI scraper across services to avoid multiple browser sessions
        self.ori_scraper = ori_scraper or ORIApiScraper()
        self.chain_service = TitleChainService()
        # Pass shared ORI scraper to Party2ResolutionService
        self.party2_service = Party2ResolutionService(ori_scraper=self.ori_scraper)
        self.storage = storage or ScraperStorage(db_path=self.db.db_path, db=self.db)
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
        filter_info = None

        if not docs:
            # Get search terms from Property (set by pipeline) or fall back to legal description
            search_terms = getattr(prop, "legal_search_terms", None) or []

            # If no pre-built search terms, try to build from legal_description
            if not search_terms:
                search_term = self._clean_legal_description(prop.legal_description)
                if search_term:
                    search_terms = [search_term]

            if not search_terms:
                error_msg = (
                    f"No valid legal description/search terms for ORI ingestion "
                    f"(case={prop.case_number}, folio={prop.parcel_id})"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)

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
                        candidate_docs = self.ori_scraper.search_by_legal_sync(term, headless=True)
                        if not candidate_docs:
                            continue

                        # Apply filtering immediately; if everything filters out, try next term.
                        if filter_info:
                            candidate_docs = self._filter_docs_by_lot_block(candidate_docs, filter_info)
                        elif prop.legal_description:
                            candidate_docs = self._filter_docs_by_relevance(candidate_docs, prop)

                        if not candidate_docs:
                            logger.info(f"All results filtered out for term: {term}, trying next term")
                            continue

                        docs = candidate_docs
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
                            candidate_docs = self.ori_scraper.search_by_legal(api_term)
                            if not candidate_docs:
                                continue

                            if filter_info:
                                candidate_docs = self._filter_docs_by_lot_block(candidate_docs, filter_info)
                            elif prop.legal_description:
                                candidate_docs = self._filter_docs_by_relevance(candidate_docs, prop)

                            if not candidate_docs:
                                logger.info(f"All results filtered out for term: {term}, trying next term")
                                continue

                            docs = candidate_docs
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
                        candidate_docs = self.ori_scraper.search_by_legal(term)
                        if not candidate_docs:
                            continue

                        if filter_info:
                            candidate_docs = self._filter_docs_by_lot_block(candidate_docs, filter_info)
                        elif prop.legal_description:
                            candidate_docs = self._filter_docs_by_relevance(candidate_docs, prop)

                        if not candidate_docs:
                            logger.info(f"All results filtered out for term: {term}, trying next term")
                            continue

                        docs = candidate_docs
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
                            candidate_docs = self.ori_scraper.search_by_legal_sync(term, headless=True)
                            if not candidate_docs:
                                continue

                            if filter_info:
                                candidate_docs = self._filter_docs_by_lot_block(candidate_docs, filter_info)
                            elif prop.legal_description:
                                candidate_docs = self._filter_docs_by_relevance(candidate_docs, prop)

                            if not candidate_docs:
                                logger.info(f"All results filtered out for term: {term}, trying next term")
                                continue

                            docs = candidate_docs
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

            # If we don't have lot/block filtering (metes-and-bounds, acreage, etc.),
            # apply a relevance filter against the property's legal description to avoid
            # ingesting large, irrelevant result sets from broad wildcard searches.
            if docs and not filter_info and prop.legal_description:
                docs = self._filter_docs_by_relevance(docs, prop)
                logger.info(f"After relevance filtering: {len(docs)} documents")

            if not docs:
                # Fallback: try party search by current owner name (helps metes-and-bounds cases
                # where the legal description is hard to match in ORI).
                owner_name = getattr(prop, "owner_name", None)
                owner_search = None
                if owner_name and not is_institutional_name(owner_name):
                    owner_search = self._normalize_party_name_for_search(owner_name)

                if owner_search and owner_name:
                    owner_terms = self._generate_owner_party_search_terms(owner_name)
                    logger.info(f"No legal results; trying ORI party search by owner: {owner_terms}")
                    try:
                        for term in owner_terms:
                            docs = self.ori_scraper.search_by_party(term)
                            if not docs:
                                # Browser name search bypasses the 25-result API limit.
                                docs = self.ori_scraper.search_by_party_browser_sync(term, headless=True)
                            if not docs:
                                continue

                            docs = self._filter_docs_by_relevance(docs, prop)
                            if docs:
                                logger.info(f"Owner party search yielded {len(docs)} relevant documents")
                                successful_term = f"OWNER_PARTY:{term}"
                                docs_from_browser = True
                                break
                    except Exception as e:
                        logger.warning(f"Owner party search failed for '{owner_search}': {e}")

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

            # Download and analyze PDF only for analyzable document types.
            # This extracts party data from the PDF which is more reliable than ORI indexing.
            if self.analyze_pdfs and self._should_analyze_document(doc):
                download_result = self._download_and_analyze_document(doc, prop.parcel_id, prefer_browser=docs_from_browser)
                if download_result:
                    # Always set file_path if download succeeded
                    mapped_doc["file_path"] = download_result.get("file_path")
                    # Set extracted data if vision analysis succeeded
                    if download_result.get("extracted_data"):
                        mapped_doc["vision_extracted_data"] = download_result["extracted_data"]
                        # Update party1/party2 from vLLM extraction if missing
                        mapped_doc = self._update_parties_from_extraction(mapped_doc, download_result["extracted_data"])

            # Save to DB (after party updates)
            doc_id = self.db.save_document(prop.parcel_id, mapped_doc)
            mapped_doc["id"] = doc_id

            # Update extracted data in DB if we have it
            if mapped_doc.get("vision_extracted_data"):
                self._update_document_with_extracted_data(doc_id, mapped_doc["vision_extracted_data"])

            processed_docs.append(mapped_doc)

        # Track instruments we've found so far
        existing_instruments = {str(doc.get("instrument_number", "")) for doc in processed_docs if doc.get("instrument_number")}

        # 2. Build Chain
        logger.info("Building Chain of Title...")
        analysis = self.chain_service.build_chain_and_analyze(processed_docs)

        # 3. Gap-filling: If chain has gaps, search by party name
        gaps = analysis.get("gaps", [])
        if gaps:
            logger.info(f"Chain has {len(gaps)} gaps, attempting gap-fill...")
            gap_docs = self._fill_chain_gaps(gaps, existing_instruments, filter_info, prop)

            if gap_docs:
                # Process new documents
                new_grouped = self._group_ori_records_by_instrument(gap_docs)
                for doc in new_grouped:
                    mapped_doc = self._map_grouped_ori_doc(doc, prop)

                    if self.analyze_pdfs and self._should_analyze_document(doc):
                        download_result = self._download_and_analyze_document(
                            doc, prop.parcel_id, prefer_browser=docs_from_browser
                        )
                        if download_result:
                            mapped_doc["file_path"] = download_result.get("file_path")
                            if download_result.get("extracted_data"):
                                mapped_doc["vision_extracted_data"] = download_result["extracted_data"]
                                mapped_doc = self._update_parties_from_extraction(mapped_doc, download_result["extracted_data"])

                    doc_id = self.db.save_document(prop.parcel_id, mapped_doc)
                    mapped_doc["id"] = doc_id

                    if mapped_doc.get("vision_extracted_data"):
                        self._update_document_with_extracted_data(doc_id, mapped_doc["vision_extracted_data"])

                    processed_docs.append(mapped_doc)

                # Rebuild chain with new documents
                logger.info("Rebuilding chain with gap-fill documents...")
                analysis = self.chain_service.build_chain_and_analyze(processed_docs)

        # 4. Verify current owner against HCPA
        chain_owner = analysis.get("summary", {}).get("current_owner", "")
        hcpa_owner = getattr(prop, "owner_name", None)

        if chain_owner and chain_owner != "Unknown":
            owner_docs = self._verify_current_owner(chain_owner, hcpa_owner, filter_info, existing_instruments)

            if owner_docs:
                # Process new owner documents
                new_grouped = self._group_ori_records_by_instrument(owner_docs)
                for doc in new_grouped:
                    mapped_doc = self._map_grouped_ori_doc(doc, prop)

                    if self.analyze_pdfs and self._should_analyze_document(doc):
                        download_result = self._download_and_analyze_document(
                            doc, prop.parcel_id, prefer_browser=docs_from_browser
                        )
                        if download_result:
                            mapped_doc["file_path"] = download_result.get("file_path")
                            if download_result.get("extracted_data"):
                                mapped_doc["vision_extracted_data"] = download_result["extracted_data"]
                                mapped_doc = self._update_parties_from_extraction(mapped_doc, download_result["extracted_data"])

                    doc_id = self.db.save_document(prop.parcel_id, mapped_doc)
                    mapped_doc["id"] = doc_id

                    if mapped_doc.get("vision_extracted_data"):
                        self._update_document_with_extracted_data(doc_id, mapped_doc["vision_extracted_data"])

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
        defendant: Optional[str] = None,
        skip_db_writes: bool = False,
    ) -> Optional[Dict]:
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
            skip_db_writes: If True, return data instead of writing to DB (for queueing)

        Returns:
            If skip_db_writes=True: dict with 'documents' and 'chain_data' for queued writes
            Otherwise: None
        """
        if not plaintiff and not defendant:
            logger.warning(f"No party names provided for {prop.case_number}, cannot search ORI")
            return None

        logger.info(f"Ingesting ORI by party for case {prop.case_number}")
        if plaintiff:
            logger.info(f"  Plaintiff: {plaintiff}")
        if defendant:
            logger.info(f"  Defendant: {defendant}")

        all_docs: List[dict] = []

        # Search by defendant first (borrower - more likely to have title documents)
        # Use browser-based search to avoid 25-record API limit
        if defendant:
            search_name = self._normalize_party_name_for_search(defendant)
            if search_name:
                logger.info(f"Searching ORI by defendant (browser): {search_name}")
                try:
                    docs = self.ori_scraper.search_by_party_browser_sync(search_name)
                    if docs:
                        logger.info(f"Found {len(docs)} documents for defendant")
                        all_docs.extend(docs)
                except Exception as e:
                    logger.warning(f"Party search failed for defendant '{search_name}': {e}")

        # Also search by plaintiff (may find lis pendens, mortgages)
        if plaintiff:
            search_name = self._normalize_party_name_for_search(plaintiff)
            if search_name:
                logger.info(f"Searching ORI by plaintiff (browser): {search_name}")
                try:
                    docs = self.ori_scraper.search_by_party_browser_sync(search_name)
                    if docs:
                        logger.info(f"Found {len(docs)} documents for plaintiff")
                        all_docs.extend(docs)
                except Exception as e:
                    logger.warning(f"Party search failed for plaintiff '{search_name}': {e}")

        if not all_docs:
            logger.warning(f"No ORI documents found by party search for {prop.case_number}")
            return None

        # Group by instrument to dedupe
        grouped_docs = self._group_ori_records_by_instrument(all_docs)
        logger.info(f"Grouped {len(all_docs)} records into {len(grouped_docs)} unique documents")

        # Process documents (same as standard ingestion)
        processed_docs = []
        property_id = prop.parcel_id or prop.case_number

        for doc in grouped_docs:
            mapped_doc = self._map_grouped_ori_doc(doc, prop)

            if self.analyze_pdfs:
                download_result = self._download_and_analyze_document(doc, property_id)
                if download_result:
                    mapped_doc["file_path"] = download_result.get("file_path")
                    if download_result.get("extracted_data"):
                        mapped_doc["vision_extracted_data"] = download_result["extracted_data"]
                        mapped_doc = self._update_parties_from_extraction(mapped_doc, download_result["extracted_data"])

            if not skip_db_writes:
                doc_id = self.db.save_document(property_id, mapped_doc)
                mapped_doc["id"] = doc_id
                if mapped_doc.get("vision_extracted_data"):
                    self._update_document_with_extracted_data(doc_id, mapped_doc["vision_extracted_data"])

            processed_docs.append(mapped_doc)

        # Build Chain of Title
        logger.info("Building Chain of Title from party search results...")
        analysis = self.chain_service.build_chain_and_analyze(processed_docs)
        db_data = self._transform_analysis_for_db(analysis)

        if skip_db_writes:
            # Return data for caller to queue writes
            logger.info(f"Party-based ingestion prepared {len(processed_docs)} docs for {prop.case_number} (writes deferred)")
            return {
                "property_id": property_id,
                "documents": processed_docs,
                "chain_data": db_data,
            }

        self.db.save_chain_of_title(property_id, db_data)
        logger.success(f"Party-based ingestion complete for {prop.case_number} ({len(processed_docs)} docs)")
        return {
            "property_id": property_id,
            "documents": processed_docs,
            "chain_data": db_data,
        }

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
        for pattern in [" A/K/A ", " AKA ", " F/K/A ", " FKA ", " D/B/A ", " DBA "]:
            if pattern in search:
                search = search.split(pattern)[0].strip()
                break

        # Remove common legal suffixes at the end
        suffixes_to_remove = [
            " ET AL",
            " ET AL.",
            " ET UX",
            " ET VIR",
            " A/K/A",
            " AKA",
            " F/K/A",
            " FKA",
            " D/B/A",
            " DBA",
            " AS TRUSTEE",
            " INDIVIDUALLY",
            " AND ALL",
            " AND",
        ]
        for suffix in suffixes_to_remove:
            if search.endswith(suffix):
                search = search[: -len(suffix)].strip()

        # Remove any content in parentheses
        search = re.sub(r"\([^)]*\)", "", search).strip()

        # If name has comma (LAST, FIRST format), convert to LAST FIRST
        if "," in search:
            parts = search.split(",", 1)
            search = f"{parts[0].strip()} {parts[1].strip()}"

        # Clean up multiple spaces
        search = " ".join(search.split())

        if len(search) < 3:
            return None

        # Add wildcard for partial matching
        if not search.endswith("*"):
            search += "*"

        return search

    def _generate_owner_party_search_terms(self, owner_name: str) -> List[str]:
        """
        Generate multiple party search strings for an owner name.

        ORI names may be indexed as:
        - "FIRST MIDDLE LAST"
        - "LAST FIRST MIDDLE"
        - "LAST FIRST"

        This is a best-effort helper used only as a fallback when legal searches fail.
        """
        base = self._normalize_party_name_for_search(owner_name)
        if not base:
            return []

        terms = [base]

        # Try "LAST FIRST*" if it looks like a personal name without a comma.
        raw = owner_name.strip().upper()
        if "," not in raw:
            tokens = [t for t in raw.replace(".", " ").split() if t]
            if len(tokens) >= 2:
                last = tokens[-1]
                first = tokens[0]
                swapped = f"{last} {first}*"
                if swapped not in terms:
                    terms.append(swapped)

        return terms

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
        filter_info = None

        if not docs:
            # Get search terms from Property (set by pipeline) or fall back to legal description
            search_terms = getattr(prop, "legal_search_terms", None) or []

            # If no pre-built search terms, try to build from legal_description
            if not search_terms:
                search_term = self._clean_legal_description(prop.legal_description)
                if search_term:
                    search_terms = [search_term]

            if not search_terms:
                error_msg = (
                    f"No valid legal description/search terms for ORI ingestion "
                    f"(case={prop.case_number}, folio={prop.parcel_id})"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)

            # Extract filter info and actual search terms (matches sync version)
            actual_search_terms = []
            for term in search_terms:
                if isinstance(term, tuple) and term[0] == "__filter__":
                    filter_info = term[1]
                else:
                    actual_search_terms.append(term)

            # Try each search term until we get results with filtering
            successful_term = None
            for search_term in actual_search_terms:
                logger.info(f"Searching ORI for: {search_term}")
                try:
                    # Use browser-based search (async) to avoid API blocking
                    candidate_docs = await self.ori_scraper.search_by_legal_browser(search_term, headless=True)
                    if not candidate_docs:
                        logger.debug(f"No results for: {search_term}")
                        continue

                    # Apply filtering (matches sync version logic)
                    if filter_info:
                        candidate_docs = self._filter_docs_by_lot_block(candidate_docs, filter_info)
                    elif prop.legal_description:
                        candidate_docs = self._filter_docs_by_relevance(candidate_docs, prop)

                    if not candidate_docs:
                        logger.info(f"All results filtered out for term: {search_term}, trying next term")
                        continue

                    docs = candidate_docs
                    successful_term = search_term
                    logger.info(f"Found {len(docs)} documents with term: {search_term}")
                    break
                except Exception as e:
                    logger.warning(f"Search failed for '{search_term}': {e}")
                    continue

            # API fallback: if browser search failed, try API with PARALLEL search
            if not docs:
                logger.info("Browser search failed, trying parallel API fallback...")
                try:
                    # Strip wildcards from search terms for API
                    clean_terms = [t.rstrip("*") for t in actual_search_terms if t]

                    # Strategy: Try first 4 terms, then owner name, then remaining terms
                    first_batch = clean_terms[:4]
                    remaining_terms = clean_terms[4:]

                    # Step 1: Try first 4 terms in parallel
                    if first_batch:
                        candidate_docs = self.ori_scraper.search_by_legal_parallel(first_batch, max_workers=4)
                        if candidate_docs:
                            if filter_info:
                                candidate_docs = self._filter_docs_by_lot_block(candidate_docs, filter_info)
                            elif prop.legal_description:
                                candidate_docs = self._filter_docs_by_relevance(candidate_docs, prop)

                            if candidate_docs:
                                docs = candidate_docs
                                successful_term = f"API_PARALLEL:first_4_terms"
                                logger.info(f"Parallel API (first 4) found {len(docs)} documents")

                    # Step 2: If first batch failed, try owner name search to discover ORI legal format
                    if not docs:
                        owner_name = getattr(prop, "owner_name", None)
                        if owner_name and not is_institutional_name(owner_name):
                            owner_search_name = self._normalize_party_name_for_search(owner_name)
                            if owner_search_name:
                                logger.info(f"First 4 terms failed, trying early owner search: {owner_search_name}")
                                try:
                                    owner_docs = self.ori_scraper.search_by_party(owner_search_name)
                                    if owner_docs:
                                        # Try to find a doc with legal matching our property
                                        ori_legal = self._find_matching_ori_legal_from_docs(
                                            owner_docs, prop.legal_description, filter_info
                                        )
                                        if ori_legal:
                                            # Use ORI's indexed legal format for targeted search
                                            logger.info(f"Using ORI legal from owner search: {ori_legal[:50]}...")
                                            targeted_docs = self.ori_scraper.search_by_legal(ori_legal)
                                            if targeted_docs:
                                                if filter_info:
                                                    targeted_docs = self._filter_docs_by_lot_block(targeted_docs, filter_info)
                                                if targeted_docs:
                                                    docs = targeted_docs
                                                    successful_term = f"OWNER_ORI_LEGAL:{ori_legal[:30]}"
                                                    logger.info(f"Owner-discovered ORI legal found {len(docs)} documents")
                                except Exception as e:
                                    logger.debug(f"Early owner search failed: {e}")

                    # Step 3: If still no results, try remaining terms
                    if not docs and remaining_terms:
                        logger.info(f"Trying remaining {len(remaining_terms)} search terms...")
                        candidate_docs = self.ori_scraper.search_by_legal_parallel(remaining_terms, max_workers=5)
                        if candidate_docs:
                            if filter_info:
                                candidate_docs = self._filter_docs_by_lot_block(candidate_docs, filter_info)
                            elif prop.legal_description:
                                candidate_docs = self._filter_docs_by_relevance(candidate_docs, prop)

                            if candidate_docs:
                                docs = candidate_docs
                                successful_term = f"API_PARALLEL:remaining_{len(remaining_terms)}_terms"
                                logger.info(f"Parallel API (remaining) found {len(docs)} documents")

                except Exception as e:
                    logger.warning(f"Parallel API search failed: {e}")

            # Fallback: try party search by current owner name (matches sync version)
            if not docs:
                owner_name = getattr(prop, "owner_name", None)
                owner_search = None
                if owner_name and not is_institutional_name(owner_name):
                    owner_search = self._normalize_party_name_for_search(owner_name)

                if owner_search and owner_name:
                    owner_terms = self._generate_owner_party_search_terms(owner_name)
                    logger.info(f"No legal results; trying ORI party search by owner: {owner_terms}")
                    for term in owner_terms:
                        try:
                            # Use browser party search async (if available, otherwise wrap sync)
                            loop = asyncio.get_running_loop()
                            # Note: ORI Scraper needs search_by_party_browser_async ideally, but we wrap sync
                            candidate_docs = await loop.run_in_executor(
                                None, functools.partial(self.ori_scraper.search_by_party_browser_sync, term, headless=True)
                            )

                            if candidate_docs:
                                candidate_docs = self._filter_docs_by_relevance(candidate_docs, prop)
                                if candidate_docs:
                                    docs = candidate_docs
                                    successful_term = f"OWNER_PARTY:{term}"
                                    logger.info(f"Owner party search yielded {len(docs)} relevant documents")
                                    break
                        except Exception as e:
                            logger.warning(f"Owner party search failed: {e}")

            if not docs:
                logger.warning(f"No documents found after trying {len(actual_search_terms)} search terms.")
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

            # Download and analyze PDF only for analyzable document types.
            # This extracts party data from the PDF which is more reliable than ORI indexing.
            if self.analyze_pdfs and self._should_analyze_document(doc):
                loop = asyncio.get_running_loop()
                # Run sync download/analysis in thread pool, guarded by semaphore
                async with VisionService.global_semaphore():
                    download_result = await loop.run_in_executor(
                        None,
                        functools.partial(
                            self._download_and_analyze_document, doc, prop.parcel_id, prefer_browser=docs_from_browser
                        ),
                    )

                if download_result:
                    # Always set file_path if download succeeded
                    mapped_doc["file_path"] = download_result.get("file_path")
                    # Set extracted data if vision analysis succeeded
                    if download_result.get("extracted_data"):
                        mapped_doc["vision_extracted_data"] = download_result["extracted_data"]
                        # Update party1/party2 from vLLM extraction if missing
                        mapped_doc = self._update_parties_from_extraction(mapped_doc, download_result["extracted_data"])

            # Save to DB (after party updates)
            # Save to DB (via Writer if available)
            if self.db_writer:
                doc_id = await self.db_writer.execute_with_result(self.db.save_document, prop.parcel_id, mapped_doc)
            else:
                doc_id = self.db.save_document(prop.parcel_id, mapped_doc)
            mapped_doc["id"] = doc_id

            # Update extracted data in DB if we have it
            if mapped_doc.get("vision_extracted_data"):
                if self.db_writer:
                    await self.db_writer.execute_with_result(
                        self._update_document_with_extracted_data, doc_id, mapped_doc["vision_extracted_data"]
                    )
                else:
                    self._update_document_with_extracted_data(doc_id, mapped_doc["vision_extracted_data"])

            processed_docs.append(mapped_doc)

        # Track instruments we've found so far (for gap-filling deduplication)
        existing_instruments = {str(doc.get("instrument_number", "")) for doc in processed_docs if doc.get("instrument_number")}

        # 2. Build Chain
        logger.info("Building Chain of Title...")
        loop = asyncio.get_running_loop()
        analysis = await loop.run_in_executor(None, self.chain_service.build_chain_and_analyze, processed_docs)

        # 3. Gap-filling: If chain has gaps, search by party name (matches sync version)
        gaps = analysis.get("gaps", [])
        if gaps:
            logger.info(f"Chain has {len(gaps)} gaps, attempting gap-fill...")
            gap_docs = await loop.run_in_executor(None, self._fill_chain_gaps, gaps, existing_instruments, filter_info, prop)

            if gap_docs:
                # Process new documents
                new_grouped = self._group_ori_records_by_instrument(gap_docs)
                for doc in new_grouped:
                    mapped_doc = self._map_grouped_ori_doc(doc, prop)

                    if self.analyze_pdfs and self._should_analyze_document(doc):
                        async with VisionService.global_semaphore():
                            download_result = await loop.run_in_executor(
                                None,
                                functools.partial(
                                    self._download_and_analyze_document, doc, prop.parcel_id, prefer_browser=docs_from_browser
                                ),
                            )
                        if download_result:
                            mapped_doc["file_path"] = download_result.get("file_path")
                            if download_result.get("extracted_data"):
                                mapped_doc["vision_extracted_data"] = download_result["extracted_data"]
                                mapped_doc = self._update_parties_from_extraction(mapped_doc, download_result["extracted_data"])

                    if self.db_writer:
                        doc_id = await self.db_writer.execute_with_result(self.db.save_document, prop.parcel_id, mapped_doc)
                    else:
                        doc_id = self.db.save_document(prop.parcel_id, mapped_doc)
                    mapped_doc["id"] = doc_id

                    if mapped_doc.get("vision_extracted_data"):
                        if self.db_writer:
                            await self.db_writer.execute_with_result(
                                self._update_document_with_extracted_data, doc_id, mapped_doc["vision_extracted_data"]
                            )
                        else:
                            self._update_document_with_extracted_data(doc_id, mapped_doc["vision_extracted_data"])

                    processed_docs.append(mapped_doc)
                    existing_instruments.add(str(mapped_doc.get("instrument_number", "")))

                # Rebuild chain with new documents
                logger.info("Rebuilding chain with gap-fill documents...")
                analysis = await loop.run_in_executor(None, self.chain_service.build_chain_and_analyze, processed_docs)

        # 4. Verify current owner against HCPA (matches sync version)
        chain_owner = analysis.get("summary", {}).get("current_owner", "")
        hcpa_owner = getattr(prop, "owner_name", None)

        if chain_owner and chain_owner != "Unknown":
            owner_docs = await loop.run_in_executor(
                None, self._verify_current_owner, chain_owner, hcpa_owner, filter_info, existing_instruments
            )

            if owner_docs:
                # Process new owner documents
                new_grouped = self._group_ori_records_by_instrument(owner_docs)
                for doc in new_grouped:
                    mapped_doc = self._map_grouped_ori_doc(doc, prop)

                    if self.analyze_pdfs and self._should_analyze_document(doc):
                        async with VisionService.global_semaphore():
                            download_result = await loop.run_in_executor(
                                None,
                                functools.partial(
                                    self._download_and_analyze_document, doc, prop.parcel_id, prefer_browser=docs_from_browser
                                ),
                            )
                        if download_result:
                            mapped_doc["file_path"] = download_result.get("file_path")
                            if download_result.get("extracted_data"):
                                mapped_doc["vision_extracted_data"] = download_result["extracted_data"]
                                mapped_doc = self._update_parties_from_extraction(mapped_doc, download_result["extracted_data"])

                    if self.db_writer:
                        doc_id = await self.db_writer.execute_with_result(self.db.save_document, prop.parcel_id, mapped_doc)
                    else:
                        doc_id = self.db.save_document(prop.parcel_id, mapped_doc)
                    mapped_doc["id"] = doc_id

                    if mapped_doc.get("vision_extracted_data"):
                        if self.db_writer:
                            await self.db_writer.execute_with_result(
                                self._update_document_with_extracted_data, doc_id, mapped_doc["vision_extracted_data"]
                            )
                        else:
                            self._update_document_with_extracted_data(doc_id, mapped_doc["vision_extracted_data"])

                    processed_docs.append(mapped_doc)

                # Rebuild chain with owner verification documents
                logger.info("Rebuilding chain with owner verification documents...")
                analysis = await loop.run_in_executor(None, self.chain_service.build_chain_and_analyze, processed_docs)

        # Transform for DB
        db_data = self._transform_analysis_for_db(analysis)

        # 5. Save Analysis
        if self.db_writer:
            await self.db_writer.execute_with_result(self.db.save_chain_of_title, prop.parcel_id, db_data)
        else:
            self.db.save_chain_of_title(prop.parcel_id, db_data)
        logger.success(f"Ingestion complete for {prop.case_number} ({len(processed_docs)} total docs)")

    def _should_analyze_document(self, doc: dict) -> bool:
        """Check if document type is worth downloading and analyzing."""
        doc_type = doc.get("DocType") or doc.get("doc_type") or doc.get("document_type", "")
        # Extract just the code: "(MTG) MORTGAGE" -> "MTG"
        code = doc_type.replace("(", "").replace(")", "").split()[0].upper() if doc_type else ""
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
        instrument = doc.get("instrument") or doc.get("Instrument", "")
        doc_type = doc.get("doc_type") or doc.get("DocType", "UNKNOWN")

        if not instrument:
            return None

        try:
            # Create output directory
            output_dir = Path(f"data/properties/{folio}/documents")
            output_dir.mkdir(parents=True, exist_ok=True)

            # Build ORI doc format for download_pdf (needs ID or instrument)
            ori_doc = {
                "Instrument": instrument.strip() if isinstance(instrument, str) else instrument,
                "DocType": doc_type,
                "ID": doc.get("ID") or doc.get("id"),  # May not have ID yet
            }

            # Download PDF - use browser if docs came from browser search
            pdf_path = self.ori_scraper.download_pdf(ori_doc, output_dir, prefer_browser=prefer_browser)
            if not pdf_path or not pdf_path.exists():
                logger.debug(f"Could not download PDF for {instrument}")
                return None

            # Start result with file path (always set if download succeeded)
            result = {"file_path": str(pdf_path)}

            # Only run vision extraction for doc types where it adds financial data
            # (amounts, rates, sale prices). For others (NOC, ASG, AFF, etc.),
            # ORI metadata already provides parties, dates, and instrument numbers.
            code = doc_type.replace("(", "").replace(")", "").split()[0].upper() if doc_type else ""
            if code not in VISION_EXTRACT_DOC_TYPES:
                logger.debug(f"Skipping vision for {doc_type} {instrument} (ORI metadata sufficient)")
                return result

            logger.info(f"Analyzing PDF: {pdf_path.name}")

            # Analyze with vision service
            extracted_data = self.doc_analyzer.analyze_document(str(pdf_path), doc_type, instrument)
            if extracted_data:
                logger.success(f"Extracted data from {doc_type}: {instrument}")
                result["extracted_data"] = extracted_data

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
        doc_type = (mapped_doc.get("document_type") or "").upper()

        # Determine which extraction fields to use based on document type
        if "DEED" in doc_type or doc_type in ["D", "WD", "QC", "TD", "CD"]:
            party1_field = "grantor"
            party2_field = "grantee"
        elif "MORTGAGE" in doc_type or "MTG" in doc_type:
            party1_field = "borrower"
            party2_field = "lender"
        elif "LIEN" in doc_type or "JUDGMENT" in doc_type:
            party1_field = "debtor"
            party2_field = "creditor"
        elif "SATISFACTION" in doc_type or "SAT" in doc_type:
            party1_field = "releasing_party"
            party2_field = "released_party"
        elif "ASSIGNMENT" in doc_type or "ASG" in doc_type:
            party1_field = "assignor"
            party2_field = "assignee"
        else:
            # Generic fallback
            party1_field = "grantor"
            party2_field = "grantee"

        # Update party1 if missing
        if not mapped_doc.get("party1") or mapped_doc["party1"].strip() == "":
            extracted_party1 = extracted_data.get(party1_field) or extracted_data.get("party1")
            if extracted_party1:
                mapped_doc["party1"] = extracted_party1
                logger.info(f"  Updated party1 from vLLM: {extracted_party1[:50]}")

        # Update party2 if missing
        if not mapped_doc.get("party2") or mapped_doc["party2"].strip() == "":
            extracted_party2 = extracted_data.get(party2_field) or extracted_data.get("party2")
            if extracted_party2:
                mapped_doc["party2"] = extracted_party2
                logger.info(f"  Updated party2 from vLLM: {extracted_party2[:50]}")

        return mapped_doc

    def _update_document_with_extracted_data(self, doc_id: int, extracted_data: dict):
        """Update document record with vision-extracted data."""
        try:
            # Store as JSON in extracted_data field
            self.db.conn.execute(
                """
                UPDATE documents
                SET extracted_data = ?
                WHERE id = ?
            """,
                [json.dumps(extracted_data), doc_id],
            )
            self.db.conn.commit()
        except Exception as e:
            logger.warning(f"Failed to update document {doc_id} with extracted data: {e}")

    def _transform_analysis_for_db(self, analysis: dict) -> dict:
        if analysis.get("ownership_timeline"):
            # Prefer service-produced timeline (includes inferred/implied links)
            timeline = []
            for period in analysis["ownership_timeline"]:
                timeline.append({
                    "owner": period.get("owner"),
                    "acquired_from": period.get("acquired_from"),
                    "acquisition_date": period.get("acquisition_date"),
                    "disposition_date": period.get("disposition_date"),
                    "acquisition_instrument": period.get("acquisition_instrument"),
                    "acquisition_doc_type": period.get("acquisition_doc_type"),
                    "acquisition_price": period.get("acquisition_price"),
                    "link_status": period.get("link_status"),
                    "confidence_score": period.get("confidence_score"),
                    "encumbrances": [],
                })

            # Attach encumbrances by date interval (best effort)
            encumbrances = analysis.get("encumbrances", [])
            for i, period in enumerate(timeline):
                start_date = self._parse_date(period.get("acquisition_date"))
                end_date = self._parse_date(timeline[i + 1].get("acquisition_date")) if i < len(timeline) - 1 else None
                for enc in encumbrances:
                    enc_date = self._parse_date(enc.get("date"))
                    if enc_date and start_date and enc_date >= start_date and (end_date is None or enc_date < end_date):
                        period["encumbrances"].append(self._map_encumbrance(enc))

            return {"ownership_timeline": timeline}

        chain = analysis.get("chain", [])
        encumbrances = analysis.get("encumbrances", [])

        timeline = []

        for i, deed in enumerate(chain):
            start_date = self._parse_date(deed.get("date"))
            end_date = None
            if i < len(chain) - 1:
                end_date = self._parse_date(chain[i + 1].get("date"))

            # Find encumbrances in this period
            period_encs = []
            for enc in encumbrances:
                enc_date = self._parse_date(enc.get("date"))
                # If enc_date is None, we can't place it. Maybe put in last period?
                if enc_date and start_date and enc_date >= start_date and (end_date is None or enc_date < end_date):
                    period_encs.append(self._map_encumbrance(enc))

            timeline.append({
                "owner": deed.get("grantee"),
                "acquired_from": deed.get("grantor"),
                "acquisition_date": deed.get("date"),
                "disposition_date": chain[i + 1].get("date") if i < len(chain) - 1 else None,
                "acquisition_instrument": deed.get("instrument"),
                "acquisition_doc_type": deed.get("doc_type"),
                "acquisition_price": deed.get("sales_price"),
                "link_status": deed.get("link_status"),
                "confidence_score": deed.get("confidence_score"),
                "encumbrances": period_encs,
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
                "link_status": "INFERRED",
                "confidence_score": 0.0,
                "encumbrances": [self._map_encumbrance(e) for e in encumbrances],
            })

        return {"ownership_timeline": timeline}

    def _map_encumbrance(self, enc: dict) -> dict:
        """Map analysis encumbrance to DB format."""
        bk, pg = None, None
        if enc.get("book_page"):
            parts = enc["book_page"].split("/")
            if len(parts) == 2:
                bk, pg = parts

        return {
            "type": enc.get("type"),
            "creditor": enc.get("creditor"),
            "debtor": enc.get("debtor"),  # Now included
            "amount": self._parse_amount(enc.get("amount")),
            "recording_date": enc.get("date"),
            "instrument": enc.get("instrument"),
            "book": bk,
            "page": pg,
            "is_satisfied": enc.get("status") == "SATISFIED",
            "satisfaction_instrument": enc.get("satisfaction_ref"),
            "satisfaction_date": None,  # Not in analysis dict?
            "survival_status": None,
            # Document resolution fields (passed through)
            "party2_resolution_method": enc.get("party2_resolution_method"),
            "is_self_transfer": enc.get("is_self_transfer"),
            "self_transfer_type": enc.get("self_transfer_type"),
        }

    def _parse_date(self, date_val: Any) -> Optional[datetime]:
        if not date_val:
            return None
        if isinstance(date_val, datetime):
            return date_val
        if isinstance(date_val, date):
            return datetime.combine(date_val, datetime.min.time())

        date_str = str(date_val)
        try:
            return datetime.strptime(date_str, "%m/%d/%Y")
        except ValueError:
            try:
                return datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                return None

    def _parse_amount(self, amt: Any) -> float:
        if not amt or amt == "Unknown":
            return 0.0
        try:
            return float(str(amt).replace("$", "").replace(",", ""))
        except ValueError:
            return 0.0

    def _clean_legal_description(self, legal: str | None) -> Optional[str]:
        if not legal:
            return None
        # Remove common prefixes/suffixes or noise
        # Example: "LOT 5 BLOCK 3 SUBDIVISION NAME" -> "SUBDIVISION NAME" is hard without NLP
        # But usually searching for the whole string works if it's exact, or we use CONTAINS.
        # The API uses CONTAINS.
        # So we should try to pick the most unique part.
        # For now, return the first 60 chars as a heuristic to avoid super long strings
        return legal[:60]

    def _find_matching_ori_legal_from_docs(
        self, docs: List[Dict], prop_legal: str | None, filter_info: Optional[Dict] = None
    ) -> Optional[str]:
        """
        Find a document whose ORI-indexed legal matches our property.

        Used after owner name search to discover ORI's indexed legal format,
        which can then be used for more targeted legal description searches.

        Args:
            docs: List of ORI documents from owner/party search
            prop_legal: Our property's legal description from HCPA
            filter_info: Optional filter info with lot/block/subdivision

        Returns:
            The ORI-indexed legal description if a match is found, else None
        """
        if not docs or not prop_legal:
            return None

        prop_upper = prop_legal.upper()

        # Extract key tokens from our property's legal description
        # Focus on subdivision name words (skip common words)
        skip_words = {
            "LOT",
            "LOTS",
            "BLOCK",
            "BLK",
            "UNIT",
            "PHASE",
            "SECTION",
            "MAP",
            "OF",
            "THE",
            "AND",
            "TO",
            "IN",
            "AT",
            "A",
            "AN",
            "ACCORDING",
            "PLAT",
            "BOOK",
            "PAGE",
            "PAGES",
            "PUBLIC",
            "RECORDS",
            "HILLSBOROUGH",
            "COUNTY",
            "FLORIDA",
            "LESS",
            "FEET",
            "FT",
            "THEREOF",
            "NORTH",
            "SOUTH",
            "EAST",
            "WEST",
            "N",
            "S",
            "E",
            "W",
            "PT",
            "PART",
            "PORTION",
            "NO",
            "NUMBER",
        }

        # Get significant words from property legal
        prop_words = set(re.findall(r"[A-Z]+", prop_upper)) - skip_words
        # Filter to words with 3+ chars (avoid noise like "II", "1A")
        prop_words = {w for w in prop_words if len(w) >= 3}

        # Also get lot/block from filter_info if available
        target_lot = None
        target_block = None
        if filter_info:
            lot_val = filter_info.get("lot")
            if isinstance(lot_val, list) and lot_val:
                target_lot = str(lot_val[0])
            elif lot_val:
                target_lot = str(lot_val)
            target_block = filter_info.get("block")

        best_match: Optional[str] = None
        best_score = 0

        for doc in docs:
            ori_legal = doc.get("Legal") or doc.get("legal") or ""
            if not ori_legal:
                continue

            ori_upper = ori_legal.upper()
            ori_words = set(re.findall(r"[A-Z]+", ori_upper)) - skip_words

            # Count matching significant words
            matches = prop_words & ori_words
            score = len(matches)

            # Bonus for matching lot/block
            if target_lot and re.search(rf"\bL(?:OT)?\s*{re.escape(target_lot)}\b", ori_upper):
                score += 3
            if target_block and re.search(rf"\bB(?:LK|LOCK)?\s*{re.escape(str(target_block))}\b", ori_upper):
                score += 3

            # Need at least 2 matching words to consider it a match
            if score > best_score and len(matches) >= 2:
                best_score = score
                best_match = ori_legal

        if best_match:
            logger.info(f"Found matching ORI legal from owner search (score={best_score}): {best_match[:60]}...")

        return best_match

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

        lot_value = filter_info.get("lot")
        block = filter_info.get("block")
        require_all_lots = bool(filter_info.get("require_all_lots"))

        lots: List[str] = []
        if isinstance(lot_value, (list, tuple, set)):
            lots = [str(x) for x in lot_value if x is not None and str(x).strip()]
        elif lot_value:
            lots = [str(lot_value).strip()]

        if not lots and not block:
            return docs

        subdivision = (filter_info.get("subdivision") or "").upper()
        subdiv_words = re.findall(r"[A-Z0-9]+", subdivision)

        # Build a small set of required subdivision tokens to use when BLOCK is missing
        # in ORI's indexed legal (very common).
        required_subdiv_tokens: List[str] = []
        if subdiv_words:
            required_subdiv_tokens.append(subdiv_words[0])  # always require first token

            generic_first = {
                "OAK",
                "BLOOMINGDALE",
                "LAKE",
                "PARK",
                "COUNTRY",
                "TAMPA",
                "THE",
            }
            if subdiv_words[0] in generic_first and len(subdiv_words) >= 2:
                required_subdiv_tokens.append(subdiv_words[1])

            if "SECTION" in subdiv_words:
                i = subdiv_words.index("SECTION")
                if i + 1 < len(subdiv_words):
                    required_subdiv_tokens.append(subdiv_words[i + 1])
            if "UNIT" in subdiv_words:
                i = subdiv_words.index("UNIT")
                if i + 1 < len(subdiv_words):
                    nxt = subdiv_words[i + 1]
                    if nxt == "NO" and i + 2 < len(subdiv_words):
                        nxt = subdiv_words[i + 2]
                    required_subdiv_tokens.append(nxt)

        filtered = []
        for doc in docs:
            # Handle both API format (Legal) and browser format (legal)
            legal = doc.get("Legal") or doc.get("legal") or ""
            legal_upper = legal.upper()

            # Check lot match - various formats: "L 1", "L1", "LOT 1", "L 1 AND 2", "L 1-3"
            lot_match = True
            if lots:
                lot_hits = 0
                for lot in lots:
                    # Primary pattern: "L 1" or "LOT 1" at start of lot reference
                    lot_pattern = rf"\bL(?:OT)?\s*{re.escape(lot)}\b"
                    # Secondary pattern: "AND 2" or ", 2" for multi-lot (e.g., "L 1 AND 2")
                    lot_and_pattern = rf"\b(?:AND|,)\s*{re.escape(lot)}\b"
                    # Range pattern: "L 1-3" covers lots 1, 2, 3 (check if lot is in range)
                    range_match = False
                    range_pattern = re.search(r"\bL(?:OT)?\s*(\d+)-(\d+)\b", legal_upper)
                    if range_pattern and lot.isdigit():
                        range_start, range_end = int(range_pattern.group(1)), int(range_pattern.group(2))
                        if range_start <= int(lot) <= range_end:
                            range_match = True
                    if re.search(lot_pattern, legal_upper) or re.search(lot_and_pattern, legal_upper) or range_match:
                        lot_hits += 1
                lot_match = lot_hits == len(lots) if require_all_lots else lot_hits > 0

            # Check block match - various formats: "B 1", "B1", "BLOCK 1", "BLK 1"
            block_match = True
            has_any_block = bool(re.search(r"\bB(?:LK|LOCK)?\s*[A-Z0-9]+\b", legal_upper))
            if block:
                block_pattern = rf"\bB(?:LK|LOCK)?\s*{re.escape(block)}\b"
                block_match = bool(re.search(block_pattern, legal_upper))

            # If the record doesn't include any BLOCK in the indexed legal, fall back to
            # subdivision token requirements to avoid discarding correct records.
            if block and not has_any_block:
                if required_subdiv_tokens:
                    block_match = all(re.search(rf"\\b{re.escape(tok)}\\b", legal_upper) for tok in required_subdiv_tokens)
                else:
                    block_match = True

            if lot_match and block_match:
                filtered.append(doc)

        logger.info(f"Filtered {len(docs)} -> {len(filtered)} docs (lot={lots or None}, block={block})")
        return filtered

    def _filter_docs_by_relevance(self, docs: List[Dict], prop: Property) -> List[Dict]:
        prop_legal = (prop.legal_description or "").strip()
        if not prop_legal:
            return docs

        prop_info = {
            "legal_description": prop_legal,
            "property_address": prop.address or "",
            "folio": prop.parcel_id or "",
        }

        prop_upper = prop_legal.upper().lstrip()
        similarity_threshold = (
            0.90 if prop_upper.startswith(("COM ", "BEG ", "BEGIN", "COMMENCE", "COMMENCING", "TRACT")) else 0.80
        )

        kept = []
        scored = []
        for d in docs:
            doc_legal = d.get("legal") or d.get("Legal") or d.get("legal_description") or ""
            doc_addr = d.get("property_address") or d.get("Address") or ""
            checks = verify_document_relevance(
                {"legal_description": doc_legal, "property_address": doc_addr, "folio": d.get("folio") or ""},
                prop_info,
            )
            scored.append((float(checks.get("similarity_score", 0.0) or 0.0), bool(checks.get("is_relevant", False)), d))

        for score, is_relevant, doc in scored:
            if is_relevant and score >= similarity_threshold:
                kept.append(doc)

        # If nothing passes strict threshold, keep a conservative top-N by similarity
        if not kept:
            top = sorted(scored, key=lambda t: t[0], reverse=True)[:25]
            kept = [doc for score, is_relevant, doc in top if is_relevant and score > 0.0]

        return kept

    def _fill_chain_gaps(
        self, gaps: List, existing_instruments: set, filter_info: Optional[dict], prop: "Property"
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
        stopwords = {"LLC", "INC", "CORP", "THE", "OF", "AND", "TRUST", "TRUSTEE"}
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
                    "page_count": doc.get("PageCount"),  # Number of pages in document
                    "uuid": doc.get("UUID"),  # Unique document identifier
                    "book_type": doc.get("BookType"),  # Book type (OR, etc.)
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
                elif (
                    name
                    and ("PARTY 2" in person_type or "GRANTEE" in person_type)
                    and name not in by_instrument[instrument]["party2_names"]
                ):
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
        DEED_TYPES = {
            "(D) DEED",
            "(WD) WARRANTY DEED",
            "(QC) QUIT CLAIM",
            "(CD) CORRECTIVE DEED",
            "(TD) TRUSTEE DEED",
            "(TAXDEED) TAX DEED",
        }

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
        DEED_TYPES = {
            "(D) DEED",
            "(WD) WARRANTY DEED",
            "(QC) QUIT CLAIM",
            "(CD) CORRECTIVE DEED",
            "(TD) TRUSTEE DEED",
            "(TAXDEED) TAX DEED",
        }

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
            from datetime import UTC, date, datetime as dt

            # Handle date object (from DB/testing)
            if isinstance(date_val, (date, dt)):
                rec_date = date_val.strftime("%Y-%m-%d")
            # Check if it's a Unix timestamp (int or float)
            elif isinstance(date_val, (int, float)):
                try:
                    parsed = dt.fromtimestamp(date_val, tz=UTC)
                    rec_date = parsed.strftime("%Y-%m-%d")
                except (ValueError, OSError) as e:
                    logger.debug(f"Could not parse timestamp {date_val!r} for document: {e}")
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
                    # Parse the datetime and convert to ISO format for SQLite
                    from datetime import datetime as dt

                    # Try parsing with time
                    for fmt in ["%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"]:
                        try:
                            parsed = dt.strptime(date_str, fmt)
                            rec_date = parsed.strftime("%Y-%m-%d")  # ISO format for SQLite
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
                rec_date = datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%d")
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
            "extracted_data": ori_doc,  # Store raw data too
        }

    async def shutdown(self) -> None:
        """Release async resources (Playwright browser context)."""
        if not self.ori_scraper:
            return
        try:
            await self.ori_scraper.close_browser()
        except Exception as exc:
            logger.warning(f"Failed to close ORI browser cleanly: {exc}")
