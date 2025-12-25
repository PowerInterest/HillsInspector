"""
Iterative Discovery - Main discovery loop for building chains of title.

This module provides:
- Main discovery loop with stopping conditions
- Search execution via ORI API
- Document processing for new search vectors
- Progress tracking and statistics
"""

from dataclasses import dataclass
from datetime import date
from typing import Any, Optional

import duckdb
from loguru import logger

from config.step4v2 import (
    MAX_DOCUMENTS_PER_FOLIO,
    MAX_ITERATIONS_PER_FOLIO,
    MRTA_YEARS_REQUIRED,
    PRIORITY_LEGAL_BEGINS,
    PRIORITY_NAME_CHAIN,
)
from src.scrapers.ori_api_scraper import ORIApiScraper
from src.services.step4v2.name_matcher import NameMatcher
from src.services.step4v2.search_queue import SearchItem, SearchQueue
from src.utils.time import parse_date


@dataclass
class DiscoveryResult:
    """Result of a discovery run for a folio."""

    folio: str
    iterations: int
    documents_found: int
    chain_years: float
    is_complete: bool
    stopped_reason: str  # 'complete', 'exhausted', 'max_iterations', 'max_documents', 'error'


class IterativeDiscovery:
    """
    Main iterative discovery service.

    Runs the discovery loop for a property, executing searches and
    processing results to find new search vectors.
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        """Initialize the discovery service."""
        self.conn = conn
        self.search_queue = SearchQueue(conn)
        self.name_matcher = NameMatcher(conn)
        self.ori_scraper = ORIApiScraper()

    def run(
        self,
        folio: str,
        auction: dict,
        hcpa_data: Optional[dict] = None,
        final_judgment: Optional[dict] = None,
        bulk_parcel: Optional[dict] = None,
    ) -> DiscoveryResult:
        """
        Run iterative discovery for a folio.

        Returns a DiscoveryResult with statistics and completion status.
        """
        logger.info(f"Starting iterative discovery for {folio}")

        # Initialize search queue
        self.search_queue.initialize_for_folio(
            folio=folio,
            auction=auction,
            hcpa_data=hcpa_data,
            final_judgment=final_judgment,
            bulk_parcel=bulk_parcel,
        )

        iteration = 0
        documents_found = 0
        stopped_reason = "unknown"

        while iteration < MAX_ITERATIONS_PER_FOLIO:
            iteration += 1

            # Check stopping conditions
            if self._is_chain_complete(folio):
                stopped_reason = "complete"
                logger.info(f"Chain complete for {folio} after {iteration} iterations")
                break

            doc_count = self._get_document_count(folio)
            if doc_count >= MAX_DOCUMENTS_PER_FOLIO:
                stopped_reason = "max_documents"
                logger.warning(f"Max documents reached for {folio}: {doc_count}")
                break

            # Get next search
            search = self.search_queue.get_next_ready(folio)
            if not search:
                stopped_reason = "exhausted"
                logger.info(f"All searches exhausted for {folio}")
                break

            # Execute search
            try:
                self.search_queue.mark_in_progress(search.id)
                new_docs = self._execute_search(folio, search)
                documents_found += new_docs
                logger.debug(f"  Iteration {iteration}: {search.search_type} found {new_docs} new docs")

            except RateLimitError:
                self.search_queue.mark_rate_limited(search.id)
                logger.warning(f"Rate limited on search {search.id}")

            except Exception as e:
                self.search_queue.mark_failed(search.id, str(e))
                logger.error(f"Search failed: {e}")

        else:
            stopped_reason = "max_iterations"
            logger.warning(f"Max iterations reached for {folio}")

        # Calculate chain coverage
        chain_years = self._calculate_chain_years(folio)

        result = DiscoveryResult(
            folio=folio,
            iterations=iteration,
            documents_found=documents_found,
            chain_years=chain_years,
            is_complete=(stopped_reason == "complete"),
            stopped_reason=stopped_reason,
        )

        logger.info(
            f"Discovery complete for {folio}: {iteration} iterations, "
            f"{documents_found} docs, {chain_years:.1f} years, reason={stopped_reason}"
        )

        return result

    def _execute_search(self, folio: str, search: SearchItem) -> int:
        """
        Execute a search and process results.

        Returns the number of new documents found.
        """
        documents = self._run_search(search)

        if not documents:
            self.search_queue.mark_completed(search.id, 0, 0)
            return 0

        # Process each document
        new_count = 0
        for doc in documents:
            if self._save_document(folio, doc, search.id):
                new_count += 1
                self._extract_new_vectors(folio, doc, search.id)

        self.search_queue.mark_completed(search.id, len(documents), new_count)
        return new_count

    def _run_search(self, search: SearchItem) -> list[dict]:
        """Run a search via ORI API and return results."""
        try:
            if search.search_type == "legal":
                return self.ori_scraper.search_by_legal(
                    search.search_term,
                    start_date=self._format_date(search.date_from) or "01/01/1900",
                )

            if search.search_type == "name":
                return self.ori_scraper.search_by_party(
                    search.search_term,
                    start_date=self._format_date(search.date_from) or "01/01/1900",
                )

            if search.search_type == "instrument":
                return self.ori_scraper.search_by_instrument(search.search_term)

            if search.search_type == "book_page":
                # Parse book/page from search term (format: "BOOK/PAGE")
                parts = search.search_term.split("/")
                if len(parts) == 2:
                    book, page = parts
                    return self.ori_scraper.search_by_book_page_sync(book, page)
                logger.warning(f"Invalid book/page format: {search.search_term}")
                return []

            if search.search_type == "case":
                # Case number search - search by legal with case number as party name
                # The lis pendens for foreclosure cases often shows the case number
                logger.debug(f"Case search via party: {search.search_term}")
                return self.ori_scraper.search_by_party(search.search_term)

            logger.warning(f"Unknown search type: {search.search_type}")
            return []

        except Exception as e:
            logger.error(f"Search error: {e}")
            raise

    def _format_date(self, d: Optional[date]) -> Optional[str]:
        """Format date for ORI API."""
        if not d:
            return None
        return d.strftime("%m/%d/%Y")

    def _save_document(self, folio: str, doc: dict, search_id: int) -> bool:
        """
        Save a document if not already in database.

        Returns True if new document saved, False if duplicate.
        """
        instrument = doc.get("Instrument") or doc.get("instrument_number")
        if not instrument:
            return False

        # Ensure instrument is a string for VARCHAR column comparison
        instrument = str(instrument)

        # Check if already exists
        existing = self.conn.execute(
            "SELECT id FROM documents WHERE instrument_number = ? LIMIT 1",
            [instrument],
        ).fetchone()

        if existing:
            return False

        # Parse document data
        recording_date = self._parse_ori_date(doc.get("RecordDate"))
        doc_type = doc.get("DocType") or doc.get("document_type") or ""

        # Extract parties (handle both API formats)
        party1 = doc.get("party1")
        party2 = doc.get("party2")
        parties_one = doc.get("PartiesOne") or []
        parties_two = doc.get("PartiesTwo") or []

        if not party1 and parties_one:
            party1 = ", ".join(parties_one)
        if not party2 and parties_two:
            party2 = ", ".join(parties_two)

        # Insert document
        try:
            self.conn.execute(
                """
                INSERT INTO documents (
                    folio, document_type, instrument_number, recording_date,
                    book, page, party1, party2, legal_description,
                    sales_price, page_count, ori_uuid, ori_id, book_type,
                    triggered_by_search_id, parties_one, parties_two
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    folio,
                    self._clean_doc_type(doc_type),
                    instrument,
                    recording_date,
                    doc.get("Book") or doc.get("book"),
                    doc.get("Page") or doc.get("page"),
                    party1,
                    party2,
                    doc.get("Legal") or doc.get("legal_description"),
                    doc.get("SalesPrice") or doc.get("sales_price"),
                    doc.get("PageCount") or doc.get("page_count"),
                    doc.get("UUID") or doc.get("ori_uuid"),
                    doc.get("ID") or doc.get("ori_id"),
                    doc.get("BookType") or doc.get("book_type"),
                    search_id,
                    parties_one if parties_one else None,
                    parties_two if parties_two else None,
                ],
            )
            return True
        except Exception as e:
            logger.error(f"Failed to save document: {e}")
            return False

    def _parse_ori_date(self, date_val: Any) -> Optional[date]:
        """Parse ORI date format (Unix timestamp in seconds or milliseconds)."""
        if not date_val:
            return None

        if isinstance(date_val, (int, float)):
            try:
                from datetime import datetime, UTC

                # Detect if timestamp is in seconds or milliseconds
                # Timestamps > 4 billion are likely in milliseconds (dates after year 2096 in seconds)
                # The ORI API uses seconds, but we handle both for safety
                ts = date_val / 1000 if date_val > 4_000_000_000 else date_val
                return datetime.fromtimestamp(ts, tz=UTC).date()
            except Exception:
                return None

        return parse_date(date_val)

    def _clean_doc_type(self, doc_type: str) -> str:
        """Clean document type string."""
        if not doc_type:
            return ""
        # Remove parenthetical code like "(MTG)"
        import re
        cleaned = re.sub(r"\([^)]+\)\s*", "", doc_type).strip()
        return cleaned or doc_type

    def _extract_new_vectors(self, folio: str, doc: dict, search_id: int) -> None:
        """
        Extract new search vectors from a document.

        Queues new searches for:
        - Legal description variants
        - Party names with date bounds
        - Referenced instruments
        """
        recording_date = self._parse_ori_date(doc.get("RecordDate"))
        instrument = doc.get("Instrument") or doc.get("instrument_number")

        # 1. Legal description
        legal = doc.get("Legal") or doc.get("legal_description")
        if legal:
            self.search_queue.queue_legal_search(
                folio,
                legal,
                operator="BEGINS",
                priority=PRIORITY_LEGAL_BEGINS + 5,
                source_type="ori_document",
                triggered_by_instrument=instrument,
                triggered_by_search_id=search_id,
            )

        # 2. Party names
        parties_one = doc.get("PartiesOne") or []
        parties_two = doc.get("PartiesTwo") or []

        if not parties_one and doc.get("party1"):
            parties_one = [doc.get("party1")]
        if not parties_two and doc.get("party2"):
            parties_two = [doc.get("party2")]

        for party in parties_one:
            if party and not self.name_matcher.is_generic(party):
                # Grantor owned before this date
                self._save_party(folio, party, "grantor", recording_date, instrument)
                self.search_queue.queue_name_search(
                    folio,
                    party,
                    date_to=recording_date,
                    priority=PRIORITY_NAME_CHAIN,
                    triggered_by_instrument=instrument,
                    triggered_by_search_id=search_id,
                )

        for party in parties_two:
            if party and not self.name_matcher.is_generic(party):
                # Grantee owned after this date
                self._save_party(folio, party, "grantee", recording_date, instrument)
                self.search_queue.queue_name_search(
                    folio,
                    party,
                    date_from=recording_date,
                    priority=PRIORITY_NAME_CHAIN,
                    triggered_by_instrument=instrument,
                    triggered_by_search_id=search_id,
                )

        # 3. Check for linked identities (self-transfer)
        if parties_one and parties_two:
            p1 = parties_one[0] if isinstance(parties_one, list) else parties_one
            p2 = parties_two[0] if isinstance(parties_two, list) else parties_two
            self.name_matcher.detect_and_link(folio, p1, p2)

    def _save_party(
        self,
        folio: str,
        party_name: str,
        role: str,
        recording_date: Optional[date],
        source_instrument: Optional[str],
    ) -> None:
        """Save a party to the property_parties table."""
        normalized = self.name_matcher.normalize(party_name)
        is_generic = self.name_matcher.is_generic(party_name)

        # Determine date bounds based on role
        active_from = None
        active_to = None
        if role in ("grantee", "mortgagor"):
            active_from = recording_date
        elif role in ("grantor", "mortgagee"):
            active_to = recording_date

        try:
            self.conn.execute(
                """
                INSERT INTO property_parties (
                    folio, party_name, party_name_normalized, party_role,
                    active_from, active_to, source_instrument, recording_date, is_generic
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (folio, party_name, source_instrument) DO NOTHING
                """,
                [folio, party_name, normalized, role, active_from, active_to, source_instrument, recording_date, is_generic],
            )
        except Exception as e:
            logger.debug(f"Could not save party: {e}")

    def _get_document_count(self, folio: str) -> int:
        """Get current document count for a folio."""
        result = self.conn.execute(
            "SELECT COUNT(*) FROM documents WHERE folio = ?",
            [folio],
        ).fetchone()
        return result[0] if result else 0

    def _is_chain_complete(self, folio: str) -> bool:
        """
        Check if chain of title is complete.

        Complete means:
        1. At least MRTA_YEARS_REQUIRED years covered, OR
        2. Chain goes back to root of title (plat, government patent)
        """
        chain_years = self._calculate_chain_years(folio)
        if chain_years >= MRTA_YEARS_REQUIRED:
            return True

        # Check for root of title
        result = self.conn.execute(
            """
            SELECT document_type
            FROM documents
            WHERE folio = ?
              AND UPPER(document_type) IN ('PLAT', 'PATENT', 'GOVERNMENT DEED', 'GOV DEED')
            LIMIT 1
            """,
            [folio],
        ).fetchone()

        return result is not None

    def _calculate_chain_years(self, folio: str) -> float:
        """Calculate total years covered by documents."""
        result = self.conn.execute(
            """
            SELECT MIN(recording_date) as oldest, MAX(recording_date) as newest
            FROM documents
            WHERE folio = ? AND recording_date IS NOT NULL
            """,
            [folio],
        ).fetchone()

        if not result or not result[0] or not result[1]:
            return 0.0

        oldest = result[0]
        newest = result[1]

        if isinstance(oldest, str):
            oldest = parse_date(oldest)
        if isinstance(newest, str):
            newest = parse_date(newest)

        if not oldest or not newest:
            return 0.0

        days = (newest - oldest).days
        return days / 365.25

    def get_discovery_stats(self, folio: str) -> dict:
        """Get discovery statistics for a folio."""
        queue_stats = self.search_queue.get_stats(folio)
        doc_count = self._get_document_count(folio)
        chain_years = self._calculate_chain_years(folio)
        is_complete = self._is_chain_complete(folio)

        # Count by document type
        doc_types = self.conn.execute(
            """
            SELECT document_type, COUNT(*) as cnt
            FROM documents
            WHERE folio = ?
            GROUP BY document_type
            ORDER BY cnt DESC
            """,
            [folio],
        ).fetchall()

        # Count parties
        party_row = self.conn.execute(
            "SELECT COUNT(DISTINCT party_name) FROM property_parties WHERE folio = ?",
            [folio],
        ).fetchone()
        party_count = party_row[0] if party_row else 0

        # Count linked identities
        linked_row = self.conn.execute(
            """
            SELECT COUNT(DISTINCT linked_identity_id)
            FROM property_parties
            WHERE folio = ? AND linked_identity_id IS NOT NULL
            """,
            [folio],
        ).fetchone()
        linked_count = linked_row[0] if linked_row else 0

        return {
            "folio": folio,
            "document_count": doc_count,
            "chain_years": chain_years,
            "is_complete": is_complete,
            "party_count": party_count,
            "linked_identity_count": linked_count,
            "document_types": {row[0]: row[1] for row in doc_types},
            "search_queue": queue_stats,
        }


class RateLimitError(Exception):
    """Raised when ORI rate limits our requests."""

