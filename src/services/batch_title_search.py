"""
Batch Title Search Service

Searches ORI for all properties in the database and saves documents.
Uses multiple search methods per property:
1. Legal Description Search (CQID=321) - Primary method using raw_legal1/raw_legal2
2. Name Search (CQID=326) - Using owner names
3. Book/Page Search (CQID=319) - From sales_history records
4. Instrument Lookup (CQID=320) - Direct lookup by instrument number

Integrates with ScraperStorage to:
- Save raw search results to data/properties/{id}/raw/
- Record searches in scraper_outputs table
- Track source URLs in property_sources
- Use needs_refresh() to skip recently-searched properties

Reference: archive/legalSearchdirect.md
"""
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import quote
from loguru import logger
from src.utils.time import today_local

from src.scrapers.ori_scraper import ORIScraper
from src.scrapers.ori_api_scraper import ORIApiScraper
from src.db.operations import PropertyDB
from src.db.sqlite_paths import resolve_sqlite_db_path_str
from src.services.scraper_storage import ScraperStorage
from pathlib import Path


class BatchTitleSearch:
    """
    Batch processor for searching ORI documents for all properties.
    Integrates with ScraperStorage for organized file storage and tracking.
    """

    SCRAPER_NAME = "ori_title"  # Used for scraper_outputs tracking
    BASE_SEARCH_URL = "https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html"

    def __init__(self, db_path: str | None = None, max_age_days: int = 30):
        if db_path is None:
            db_path = resolve_sqlite_db_path_str()
        self.db = PropertyDB(db_path)
        self.storage = ScraperStorage(db_path)
        self.ori_scraper = ORIScraper()
        self.ori_api = ORIApiScraper()  # For PDF downloads
        self.max_age_days = max_age_days
        self.stats = {
            "properties_processed": 0,
            "properties_skipped": 0,
            "documents_found": 0,
            "documents_saved": 0,
            "pdfs_downloaded": 0,
            "errors": 0,
        }

    def build_legal_search_term(self, legal1: str, legal2: str) -> Optional[str]:
        """
        Build ORI search term from HCPA legal description.

        Strategy: Extract LOT/UNIT number + first word of subdivision name + wildcard

        Examples:
            "TUSCANY SUBDIVISION AT TAMPA PALMS" + "LOT 198" -> "L 198 TUSCANY*"
            "THE QUARTER AT YBOR" + "UNIT 5315..." -> "UNIT 5315 QUARTER*"
            "STILLWATER PHASE 1" + "LOT 38" -> "L 38 STILLWATER*"
        """
        if not legal1 or not legal2:
            return None

        legal1 = legal1.upper().strip()
        legal2 = legal2.upper().strip()

        # Extract lot/unit/tract/block number from legal2
        lot_match = re.match(r'(LOT|L)\s*(\d+)', legal2)
        unit_match = re.match(r'(UNIT)\s*(\d+)', legal2)
        tract_match = re.match(r'(TRACT)\s*(\d+)', legal2)
        block_match = re.match(r'(BLOCK|BLK)\s*(\d+)', legal2)

        if lot_match:
            lot_part = f"L {lot_match.group(2)}"
        elif unit_match:
            lot_part = f"UNIT {unit_match.group(2)}"
        elif tract_match:
            lot_part = f"TRACT {tract_match.group(2)}"
        elif block_match:
            lot_part = f"BLK {block_match.group(2)}"
        else:
            # Try to extract any number at the start
            num_match = re.match(r'(\d+)', legal2)
            if num_match:
                lot_part = num_match.group(1)
            else:
                # Last resort: use first few characters of legal2
                lot_part = legal2.split()[0] if legal2 else None
                if not lot_part:
                    return None

        # Extract subdivision name from legal1
        # Remove common suffixes and get first significant word
        subdiv = legal1
        for remove in ['SUBDIVISION', 'SUBD', 'SUB', 'PHASE', 'PH', 'UNIT', 'SECTION', 'SEC']:
            subdiv = re.sub(rf'\b{remove}\b.*', '', subdiv)

        # Also remove "A CONDOMINIUM", "HOMEOWNERS", etc.
        subdiv = re.sub(r'\bA CONDOMINIUM\b.*', '', subdiv)
        subdiv = re.sub(r'\bCONDOMINIUM\b.*', '', subdiv)
        subdiv = re.sub(r'\bHOMEOWNERS\b.*', '', subdiv)

        # Get first word (the main subdivision name)
        subdiv = subdiv.strip()
        words = subdiv.split()
        if not words:
            return None

        # Skip common prefixes like "THE"
        subdiv_name = words[1] if words[0] == 'THE' and len(words) > 1 else words[0]

        # Build final search term with wildcard
        return f"{lot_part} {subdiv_name}*"

    def build_search_url(self, search_term: str, cqid: int = 321) -> str:
        """Build the full ORI search URL for tracking."""
        if cqid == 321:  # Legal search
            return f"{self.BASE_SEARCH_URL}?CQID=321&OBKey__1011_1={quote(search_term)}"
        if cqid == 326:  # Name search
            return f"{self.BASE_SEARCH_URL}?CQID=326&OBKey__486_1={quote(search_term)}"
        if cqid == 319:  # Book/Page search
            parts = search_term.split("/")
            if len(parts) == 2:
                return f"{self.BASE_SEARCH_URL}?CQID=319&OBKey__1530_1=O&OBKey__573_1={parts[0]}&OBKey__1049_1={parts[1]}"
        if cqid == 320:  # Instrument search
            return f"{self.BASE_SEARCH_URL}?CQID=320&OBKey__1006_1={quote(search_term)}"
        return self.BASE_SEARCH_URL

    def get_sales_history(self, strap: str) -> List[Dict]:
        """
        Get sales history records for a property from the database.
        Returns list of dicts with book, page, instrument numbers.
        """
        try:
            results = self.db.conn.execute("""
                SELECT book, page, instrument, sale_date, doc_type, sale_price
                FROM sales_history
                WHERE strap = ?
                ORDER BY sale_date DESC
            """, [strap]).fetchall()

            columns = ['book', 'page', 'instrument', 'sale_date', 'doc_type', 'sale_price']
            return [dict(zip(columns, row, strict=True)) for row in results]
        except Exception as e:
            logger.debug(f"Error getting sales history: {e}")
            return []

    def search_by_book_page(self, book: str, page: str) -> List[Dict]:
        """
        Search ORI by Book/Page (CQID=319).
        Wrapper around ori_scraper method.
        """
        return self.ori_scraper.search_by_book_page_sync(book, page)

    def fetch_instrument(self, instrument_number: str) -> Optional[Dict]:
        """
        Fetch document metadata by instrument number (CQID=320).
        Wrapper around ori_scraper method.
        """
        return self.ori_scraper.fetch_instrument_sync(instrument_number)

    def lookup_and_save_instrument(self, folio: str, instrument_number: str) -> Optional[int]:
        """
        Look up a specific instrument by number and save it to the documents table.

        Args:
            folio: Property folio/strap to associate document with
            instrument_number: ORI instrument number to look up

        Returns:
            Document ID if saved, None if not found
        """
        self.db.connect()

        # Check if we already have this document
        existing = self.db.conn.execute("""
            SELECT id FROM documents
            WHERE folio = ? AND instrument_number = ?
        """, [folio, instrument_number]).fetchone()

        if existing:
            logger.info(f"Instrument {instrument_number} already exists for {folio}")
            return existing[0]

        # Fetch from ORI
        logger.info(f"Looking up instrument: {instrument_number}")
        result = self.fetch_instrument(instrument_number)

        if not result:
            logger.warning(f"Instrument {instrument_number} not found")
            return None

        # Parse and save
        rec_date = None
        date_str = result.get("Recording Date Time", "")
        if date_str:
            try:
                rec_date = datetime.strptime(date_str.split()[0], "%m/%d/%Y").date()
            except Exception as e:
                logger.debug(f"Could not parse ORI recording date: {date_str!r}: {e}")

        doc_data = {
            "document_type": result.get("ORI - Doc Type", ""),
            "recording_date": rec_date,
            "book": result.get("Book #", ""),
            "page": result.get("Page #", ""),
            "instrument_number": result.get("Instrument #", instrument_number),
            "party1": result.get("Name", "") if result.get("ORI - Person Type") == "PARTY 1" else "",
            "party2": result.get("Name", "") if result.get("ORI - Person Type") == "PARTY 2" else "",
            "legal_description": result.get("Legal Description", ""),
        }

        try:
            doc_id = self.db.save_document(folio, doc_data)
            logger.info(f"Saved instrument {instrument_number} as document {doc_id}")
            return doc_id
        except Exception as e:
            logger.error(f"Error saving instrument {instrument_number}: {e}")
            return None

    def get_document_by_instrument_api(self, instrument_number: str, legal_desc: str | None = None) -> Optional[Dict]:
        """
        Search the ORI API for a document by instrument number.
        Returns the API result with ID field needed for PDF download.

        Args:
            instrument_number: ORI instrument number
            legal_desc: Optional legal description to narrow search
        """
        # First get document details from PAVDirectSearch
        pav_result = self.fetch_instrument(instrument_number)
        if not pav_result:
            logger.debug(f"Could not find instrument {instrument_number} in PAVDirectSearch")
            return None

        book = pav_result.get("Book #", "")
        page = pav_result.get("Page #", "")
        legal = legal_desc or pav_result.get("Legal Description", "")
        record_date = pav_result.get("Recording Date Time", "")

        # Parse recording date to narrow search window
        start_date = "01/01/1900"
        end_date = today_local().strftime("%m/%d/%Y")
        if record_date:
            try:
                rec_dt = datetime.strptime(record_date.split()[0], "%m/%d/%Y")
                # Search within same year
                start_date = f"01/01/{rec_dt.year}"
                end_date = f"12/31/{rec_dt.year}"
            except Exception as exc:
                logger.debug("Could not parse record_date %s: %s", record_date, exc)

        # Try searching API by legal description (returns ID field we need)
        if legal:
            try:
                # Use full legal description for better matching
                payload = {
                    "DocType": self.ori_api.TITLE_DOC_TYPES,
                    "RecordDateBegin": start_date,
                    "RecordDateEnd": end_date,
                    "Legal": ["CONTAINS", legal],
                }
                results = self.ori_api._execute_search(payload)  # noqa: SLF001

                # Find matching instrument (note: API returns instrument as int)
                for r in results:
                    if str(r.get("Instrument")) == instrument_number:
                        logger.debug(f"Found instrument {instrument_number} via legal search")
                        return r

            except Exception as e:
                logger.debug(f"Legal search failed: {e}")

        # Fallback: Try book/page search if we have them
        if book and page:
            try:
                payload = {
                    "DocType": self.ori_api.TITLE_DOC_TYPES,
                    "RecordDateBegin": start_date,
                    "RecordDateEnd": end_date,
                    "BookNum": int(book) if book.isdigit() else book,
                    "PageNum": int(page) if page.isdigit() else page,
                }
                results = self.ori_api._execute_search(payload)  # noqa: SLF001

                for r in results:
                    if str(r.get("Instrument")) == instrument_number:
                        logger.debug(f"Found instrument {instrument_number} via book/page search")
                        return r

                # If exact match not found but we got single result, use it
                if len(results) == 1:
                    return results[0]

            except Exception as e:
                logger.debug(f"Book/Page API search failed: {e}")

        return None

    def download_document_pdf(self, strap: str, instrument_number: str, doc_type: str = "") -> Optional[Path]:
        """
        Download PDF for a document by instrument number.

        Args:
            strap: Property strap/folio for organizing the file
            instrument_number: ORI instrument number
            doc_type: Document type for filename

        Returns:
            Path to downloaded PDF, or None if failed
        """
        # Get document from API (need the ID field)
        api_doc = self.get_document_by_instrument_api(instrument_number)

        if not api_doc:
            logger.warning(f"Could not find document {instrument_number} in API for PDF download")
            return None

        # Create output directory using ScraperStorage's internal method
        output_dir = self.storage._get_property_dir(strap) / "documents"  # noqa: SLF001
        output_dir.mkdir(parents=True, exist_ok=True)

        # Download PDF
        pdf_path = self.ori_api.download_pdf(api_doc, output_dir)

        if pdf_path:
            logger.info(f"Downloaded PDF: {pdf_path}")
            self.stats["pdfs_downloaded"] += 1

            # Update document record with PDF path
            try:
                self.db.conn.execute("""
                    UPDATE documents
                    SET file_path = ?
                    WHERE folio = ? AND instrument_number = ?
                """, [str(pdf_path), strap, instrument_number])
            except Exception as e:
                logger.debug(f"Could not update document record: {e}")

        return pdf_path

    def download_property_pdfs(self, strap: str, doc_types: Optional[List[str]] = None) -> int:
        """
        Download PDFs for all documents of a property.

        Args:
            strap: Property strap
            doc_types: Optional list of document types to download (e.g., ["(D) DEED", "(MTG) MORTGAGE"])
                      If None, downloads all documents

        Returns:
            Number of PDFs downloaded
        """
        self.db.connect()

        # Get documents for this property
        query = "SELECT instrument_number, document_type, file_path FROM documents WHERE folio = ?"
        results = self.db.conn.execute(query, [strap]).fetchall()

        downloaded = 0
        for instrument, doc_type, existing_path in results:
            # Skip if already downloaded
            if existing_path and Path(existing_path).exists():
                continue

            # Filter by doc type if specified
            if doc_types and doc_type not in doc_types:
                continue

            pdf_path = self.download_document_pdf(strap, instrument, doc_type)
            if pdf_path:
                downloaded += 1
                time.sleep(1)  # Be nice to the server

        logger.info(f"Downloaded {downloaded} PDFs for {strap}")
        return downloaded

    def get_properties_to_search(self, limit: Optional[int] = None, force: bool = False) -> List[Dict]:
        """
        Get properties that need document search.

        Joins auctions with bulk_parcels to get legal descriptions.
        Uses ScraperStorage.needs_refresh() to skip recently-searched properties.
        """
        self.db.connect()

        # Get all auctions with legal descriptions from bulk_parcels
        query = """
            SELECT DISTINCT
                a.parcel_id as strap,
                b.folio,
                a.property_address,
                b.owner_name,
                b.raw_legal1,
                b.raw_legal2
            FROM auctions a
            JOIN bulk_parcels b ON a.parcel_id = b.strap
            WHERE a.parcel_id != 'Property Appraiser'
            AND b.raw_legal1 IS NOT NULL
        """

        results = self.db.conn.execute(query).fetchall()
        columns = ['strap', 'folio', 'address', 'owner_name', 'raw_legal1', 'raw_legal2']
        all_properties = [dict(zip(columns, row, strict=True)) for row in results]

        # Filter by needs_refresh unless force=True
        if force:
            properties = all_properties
        else:
            properties = []
            for prop in all_properties:
                strap = prop.get('strap')
                if not strap:
                    continue
                if self.storage.needs_refresh(strap, self.SCRAPER_NAME, max_age_days=self.max_age_days):
                    properties.append(prop)
                else:
                    self.stats["properties_skipped"] += 1

        if limit:
            properties = properties[:limit]

        logger.info(f"Found {len(properties)} properties needing document search "
                   f"({self.stats['properties_skipped']} skipped - recently searched)")
        return properties

    def search_property(self, prop: Dict) -> Tuple[List[Dict], Set[str], List[str]]:
        """
        Search ORI for all documents related to a property.

        Uses multiple search methods:
        1. Legal Description Search (CQID=321)
        2. Name Search (CQID=326)
        3. Book/Page Search (CQID=319) - from sales_history table

        Returns:
            Tuple of (documents_list, instrument_numbers_set, search_urls_used)
        """
        all_results = []
        seen_instruments: Set[str] = set()
        search_urls = []

        strap = prop.get('strap') or ""
        legal1 = prop.get('raw_legal1') or ""
        legal2 = prop.get('raw_legal2') or ""
        owner_name = prop.get('owner_name') or ""

        # Method 1: Legal Description Search
        search_term = self.build_legal_search_term(legal1, legal2) if legal1 else None
        if search_term:
            search_url = self.build_search_url(search_term, cqid=321)
            search_urls.append(search_url)
            logger.info(f"Searching legal: {search_term}")
            logger.debug(f"  URL: {search_url}")
            try:
                results = self.ori_scraper.search_by_legal_sync(search_term)
                for r in results:
                    inst = r.get("Instrument #", "")
                    if inst and inst not in seen_instruments:
                        seen_instruments.add(inst)
                        r["_source"] = "legal"
                        r["_search_url"] = search_url
                        all_results.append(r)
                logger.info(f"  Legal search found {len(results)} rows")
            except Exception as e:
                logger.error(f"  Legal search error: {e}")

        # Method 2: Name Search (if we have owner name)
        if owner_name and len(owner_name) > 3:
            # Format: "LAST FIRST MIDDLE" or company name
            # Try first owner if multiple
            name = owner_name.split(';')[0].strip() if ';' in owner_name else owner_name
            if name:
                name_url = self.build_search_url(name, cqid=326)
                search_urls.append(name_url)
                logger.info(f"Searching name: {name[:30]}...")
                try:
                    results = self.ori_scraper.search_by_name_sync(name)
                    # Filter to only include results with matching legal description
                    for r in results:
                        inst = r.get("Instrument #", "")
                        legal_in_result = r.get("Legal Description", "").upper()

                        # Check if legal description matches our property
                        if search_term:
                            # Extract the key parts from our search term
                            parts = search_term.replace("*", "").split()
                            if len(parts) >= 2 and all(p in legal_in_result for p in parts) and inst and inst not in seen_instruments:
                                seen_instruments.add(inst)
                                r["_source"] = f"name:{name[:20]}"
                                r["_search_url"] = name_url
                                all_results.append(r)
                except Exception as e:
                    logger.error(f"  Name search error: {e}")

        # Method 3: Book/Page Search from sales history
        sales = self.get_sales_history(strap) if strap else []
        if sales:
            logger.info(f"Searching {len(sales)} sales records by book/page...")
            for sale in sales:
                book = sale.get('book')
                page = sale.get('page')
                if book and page:
                    bp_url = self.build_search_url(f"{book}/{page}", cqid=319)
                    search_urls.append(bp_url)
                    try:
                        results = self.search_by_book_page(book, page)
                        new_found = 0
                        for r in results:
                            inst = r.get("Instrument #", "")
                            if inst and inst not in seen_instruments:
                                seen_instruments.add(inst)
                                r["_source"] = f"book_page:{book}/{page}"
                                r["_search_url"] = bp_url
                                all_results.append(r)
                                new_found += 1
                        if new_found > 0:
                            logger.info(f"    Book {book}/Page {page}: {new_found} new documents")
                    except Exception as e:
                        logger.error(f"    Book/Page search error for {book}/{page}: {e}")

        return all_results, seen_instruments, search_urls

    def group_by_instrument(self, results: List[Dict]) -> Dict[str, Dict]:
        """
        Group search results by instrument number.
        Multiple rows may have the same instrument (different parties).
        """
        by_instrument = {}

        for r in results:
            inst = r.get("Instrument #", "")
            if not inst:
                continue

            if inst not in by_instrument:
                by_instrument[inst] = {
                    "instrument": inst,
                    "doc_type": r.get("ORI - Doc Type", ""),
                    "record_date": r.get("Recording Date Time", ""),
                    "legal": r.get("Legal Description", ""),
                    "book": r.get("Book #", ""),
                    "page": r.get("Page #", ""),
                    "parties": [],
                    "_sources": set(),
                }

            by_instrument[inst]["_sources"].add(r.get("_source", "unknown"))

            # Add party info
            party_type = r.get("ORI - Person Type", "")
            party_name = r.get("Name", "")
            if party_name:
                by_instrument[inst]["parties"].append({
                    "type": party_type,
                    "name": party_name,
                })

        return by_instrument

    def save_documents(self, folio: str, documents: Dict[str, Dict]) -> int:
        """
        Save grouped documents to the database.

        Returns:
            Number of documents saved
        """
        saved = 0

        for inst, doc in documents.items():
            # Parse recording date
            rec_date = None
            date_str = doc.get("record_date", "")
            if date_str:
                try:
                    rec_date = datetime.strptime(date_str.split()[0], "%m/%d/%Y").date()
                except Exception as e:
                    logger.debug(f"Could not parse ORI recording date: {date_str!r}: {e}")

            # Get party1 and party2 from parties list
            parties = doc.get("parties", [])
            party1_list = [p["name"] for p in parties if p.get("type") == "PARTY 1"]
            party2_list = [p["name"] for p in parties if p.get("type") == "PARTY 2"]

            doc_data = {
                "document_type": doc.get("doc_type", ""),
                "recording_date": rec_date,
                "book": doc.get("book", ""),
                "page": doc.get("page", ""),
                "instrument_number": doc.get("instrument", ""),
                "party1": "; ".join(party1_list),
                "party2": "; ".join(party2_list),
                "legal_description": doc.get("legal", ""),
            }

            try:
                self.db.save_document(folio, doc_data)
                saved += 1
            except Exception as e:
                logger.error(f"Error saving {inst}: {e}")

        return saved

    def process_property(self, prop: Dict) -> int:
        """
        Process a single property: search, save raw data, save documents, record in scraper_outputs.

        Returns:
            Number of documents saved
        """
        strap = prop.get('strap') or ""
        folio = prop.get('folio') or strap
        address = prop.get('address') or 'Unknown'

        if not strap:
            logger.warning(f"Skipping property with no strap: {address}")
            return 0

        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {address}")
        logger.info(f"  Strap: {strap}, Folio: {folio}")

        # Search for documents
        results, instruments, search_urls = self.search_property(prop)

        # Save raw search results to storage
        raw_data = {
            "property": {
                "strap": strap,
                "folio": folio,
                "address": address,
                "legal1": prop.get('raw_legal1'),
                "legal2": prop.get('raw_legal2'),
                "owner": prop.get('owner_name'),
            },
            "search_urls": search_urls,
            "search_term": self.build_legal_search_term(prop.get('raw_legal1') or "", prop.get('raw_legal2') or ""),
            "total_rows": len(results),
            "unique_instruments": len(instruments),
            "results": results,
        }

        raw_path = self.storage.save_raw_data(
            property_id=strap,
            scraper=self.SCRAPER_NAME,
            data=raw_data,
            context="search_results"
        )

        if not results:
            logger.info(f"  No documents found")
            # Record the search anyway (so we know we searched)
            self.storage.record_scrape(
                property_id=strap,
                scraper=self.SCRAPER_NAME,
                raw_data_path=raw_path,
                source_url=search_urls[0] if search_urls else None,
                success=True,
                error=None
            )
            return 0

        # Group by instrument
        grouped = self.group_by_instrument(results)
        logger.info(f"  Found {len(results)} rows / {len(grouped)} unique documents")

        # Save to documents table
        saved = self.save_documents(strap, grouped)
        logger.info(f"  Saved {saved} documents")

        # Record in scraper_outputs
        self.storage.record_scrape(
            property_id=strap,
            scraper=self.SCRAPER_NAME,
            raw_data_path=raw_path,
            source_url=search_urls[0] if search_urls else None,
            vision_data={
                "documents_found": len(grouped),
                "documents_saved": saved,
                "search_term": raw_data.get("search_term"),
            },
            prompt_version="v1",
            success=True,
            error=None
        )

        self.stats["documents_found"] += len(grouped)
        return saved

    def run(self, limit: Optional[int] = None, delay_seconds: float = 2.0, force: bool = False):
        """
        Run batch title search for all properties.

        Args:
            limit: Max number of properties to process (None = all)
            delay_seconds: Delay between properties to avoid rate limiting
            force: If True, ignore needs_refresh and search all properties
        """
        logger.info("Starting batch title search...")
        logger.info(f"  Max age: {self.max_age_days} days, Force: {force}")

        self.db.connect()

        # Get properties to search
        properties = self.get_properties_to_search(limit, force=force)

        if not properties:
            logger.info("No properties need document search")
            return self.stats

        logger.info(f"Processing {len(properties)} properties...")

        for i, prop in enumerate(properties):
            try:
                docs_saved = self.process_property(prop)
                self.stats["properties_processed"] += 1
                self.stats["documents_saved"] += docs_saved

                # Progress update
                if (i + 1) % 5 == 0:
                    logger.info(f"\nProgress: {i+1}/{len(properties)} properties")
                    logger.info(f"Stats: {self.stats}")

                # Delay to avoid rate limiting
                if delay_seconds > 0 and i < len(properties) - 1:
                    time.sleep(delay_seconds)

            except Exception as e:
                logger.error(f"Error processing {prop.get('strap')}: {e}")
                self.stats["errors"] += 1

        self.db.close()

        logger.info(f"\n{'='*60}")
        logger.info("Batch title search complete!")
        logger.info(f"Final stats: {self.stats}")

        return self.stats


def main():
    """Run batch title search."""
    import argparse

    parser = argparse.ArgumentParser(description="Batch title search for ORI documents")
    parser.add_argument("--limit", type=int, help="Max properties to process")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay between properties (seconds)")
    parser.add_argument("--max-age", type=int, default=30, help="Max age in days before re-searching")
    parser.add_argument("--force", action="store_true", help="Force search even if recently searched")
    args = parser.parse_args()

    searcher = BatchTitleSearch(max_age_days=args.max_age)
    stats = searcher.run(limit=args.limit, delay_seconds=args.delay, force=args.force)

    print(f"\nResults: {stats}")


if __name__ == "__main__":
    main()
