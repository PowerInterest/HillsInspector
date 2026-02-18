"""
Iterative Discovery - Main discovery loop for building chains of title.

This module provides:
- Main discovery loop with stopping conditions
- Search execution via ORI API
- Document processing for new search vectors
- Progress tracking and statistics
"""

import json
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Optional

import sqlite3
from loguru import logger

from config.step4v2 import (
    ADJACENT_INSTRUMENT_RANGE,
    DEED_TYPES,
    MAX_ANCHOR_GAP_DAYS,
    MAX_DOCUMENTS_PER_FOLIO,
    MAX_ITERATIONS_PER_FOLIO,
    MAX_OWNERSHIP_GAP_DAYS,
    MRTA_YEARS_REQUIRED,
    PRIORITY_INSTRUMENT,
    PRIORITY_LEGAL_BEGINS,
    PRIORITY_NAME_CHAIN,
)
from src.db.type_normalizer import _DOC_TYPE_MAP, _PAREN_RE
from src.scrapers.ori_api_scraper import ORIApiScraper
from src.services.step4v2.name_matcher import NameMatcher
from src.services.step4v2.search_queue import SearchItem, SearchQueue
from src.utils.legal_description import parse_legal_description
from src.utils.time import parse_date


def _parse_json_list(val):
    """Parse a JSON list from a SQLite text column."""
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            if isinstance(parsed, list):
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass
    return []


@dataclass
class ChainGap:
    """Represents a gap in the chain of title."""

    start_date: date
    end_date: date
    gap_type: str  # 'anchor_to_first_deed', 'ownership_gap', 'to_current_owner'
    expected_grantor: Optional[str] = None  # Who should be selling
    expected_grantee: Optional[str] = None  # Who should be buying
    days: int = 0  # Number of days in the gap


@dataclass
class DiscoveryResult:
    """Result of a discovery run for a folio."""

    folio: str
    iterations: int
    documents_found: int
    chain_years: float
    is_complete: bool
    stopped_reason: str  # 'complete', 'exhausted', 'max_iterations', 'max_documents', 'error'


def _extract_lot_block_from_search(search_term: str) -> tuple[str | None, str | None]:
    """
    Extract lot and block from a search term like "L 4 B 8 TOUCHSTONE*".

    Returns (lot, block) tuple where either can be None.

    For multi-lot search terms like "L 1 AND 2 B 3" or "L 1, 2, 3 B 4",
    returns (None, block) to disable lot filtering - we intentionally
    searched for multiple lots so shouldn't filter by a single lot.
    """
    import re

    term = search_term.upper().rstrip("*").strip()

    # Detect multi-lot patterns that indicate we shouldn't filter by single lot
    # Patterns: "L 1 AND 2", "L 1, 2", "L 1 & 2", "L 1-5", "LOTS 1 AND 2"
    multi_lot_pattern = r"\bL(?:OT)?S?\s*\d+\s*(?:AND|&|,|-|THRU|THROUGH)\s*\d+"
    if re.search(multi_lot_pattern, term):
        # Multi-lot search - don't filter by specific lot
        # Still extract block for filtering
        block_match = re.search(r"\bB(?:LK|LOCK)?\s*([A-Z]?\d*[A-Z]?)\b", term)
        block = block_match.group(1) if block_match and block_match.group(1) else None
        return (None, block)

    # Match patterns like "L 4 B 8", "L4 B8", "L 40 B 1", etc.
    # Lot pattern: L/LOT followed by number/alphanumeric
    lot_match = re.search(r"\bL(?:OT)?\s*([A-Z]?\d+[A-Z]?)\b", term)
    lot = lot_match.group(1) if lot_match else None

    # Block pattern: B/BLK/BLOCK followed by number/alphanumeric
    block_match = re.search(r"\bB(?:LK|LOCK)?\s*([A-Z]?\d*[A-Z]?)\b", term)
    block = block_match.group(1) if block_match and block_match.group(1) else None

    return (lot, block)


def _normalize_subdivision(subdiv: str | None) -> str:
    """Normalize subdivision name for comparison (PH → PHASE, etc.)."""
    if not subdiv:
        return ""
    result = subdiv.upper()
    # Normalize common abbreviations
    result = re.sub(r"\bPH\s*(\d)", r"PHASE \1", result)  # "PH 2" → "PHASE 2"
    result = re.sub(r"\bPH\b", "PHASE", result)  # "PH" alone → "PHASE"
    return re.sub(r"\s+", " ", result).strip()  # Normalize whitespace


def _subdivisions_match(expected: str | None, actual: str | None) -> bool:
    """
    Check if two subdivision names refer to the same subdivision.

    Handles cases like:
    - "TOUCHSTONE PHASE 2" vs "TOUCHSTONE PH 2" → True
    - "TOUCHSTONE PHASE 2" vs "TOUCHSTONE PHASE 7" → False
    - "TOUCHSTONE PHASE 2" vs "TOUCHSTONE" → True (actual is less specific)
    """
    if not expected:
        return True  # No expected subdivision, any matches
    if not actual:
        return True  # Document has no subdivision info, allow (may be general doc)

    norm_expected = _normalize_subdivision(expected)
    norm_actual = _normalize_subdivision(actual)

    # Exact match after normalization
    if norm_expected == norm_actual:
        return True

    # Check if one is a prefix of the other (less specific → more specific is OK)
    # e.g., "TOUCHSTONE" matches "TOUCHSTONE PHASE 2"
    if norm_actual.startswith(norm_expected) or norm_expected.startswith(norm_actual):
        # But reject if phase numbers differ
        # Extract phase numbers from both
        expected_phase = re.search(r"PHASE\s*(\d+)", norm_expected)
        actual_phase = re.search(r"PHASE\s*(\d+)", norm_actual)

        if expected_phase and actual_phase:
            # Both have phase numbers - they must match
            return expected_phase.group(1) == actual_phase.group(1)

        # Only one has a phase number, or neither - allow the match
        return True

    return False


def _document_matches_lot_block(
    doc_legal: str | None,
    expected_lot: str | None,
    expected_block: str | None,
    expected_subdivision: str | None = None,
) -> bool:
    """
    Check if a document's legal description matches the expected lot/block.

    This prevents saving documents for Lot 40, 41 when searching for Lot 4.

    IMPORTANT: If a document's legal description mentions the same subdivision
    but we cannot parse a specific lot from it, the document is REJECTED rather
    than allowed. This prevents cross-property contamination from documents that
    apply to "any lot in the subdivision" (e.g., HOA docs, plat amendments).

    Args:
        doc_legal: Document's legal description from ORI
        expected_lot: Lot number we searched for (e.g., "4")
        expected_block: Block number we searched for (e.g., "8")
        expected_subdivision: Subdivision name to check against (e.g., "TOUCHSTONE PHASE 2")

    Returns:
        True if document matches (or if no lot/block filter applies)
    """
    # If no filter criteria, allow all documents
    if not expected_lot and not expected_block:
        return True

    # If document has no legal description, caller should handle this case
    # We can't verify, so return True (allow) - caller decides whether to filter
    if not doc_legal:
        return True

    # Parse the document's legal description
    parsed = parse_legal_description(doc_legal)

    # Check subdivision match first - reject documents from wrong phase/subdivision
    if expected_subdivision and parsed.subdivision and not _subdivisions_match(expected_subdivision, parsed.subdivision):
        return False

    # Check lot match if we have an expected lot
    if expected_lot:
        # Get all lots from the document (handles multi-lot properties)
        doc_lots = parsed.lots or ([parsed.lot] if parsed.lot else [])

        if not doc_lots:
            # Document has no lot info - check if it mentions our subdivision
            # If it does, this is likely a subdivision-wide document (HOA, plat, etc.)
            # that applies to ALL lots, not specifically to our lot - REJECT it
            if expected_subdivision and expected_subdivision.upper() in (doc_legal or "").upper():  # noqa: SIM103
                # Document mentions our subdivision but has no specific lot
                # This could be for ANY lot in the subdivision - reject it
                return False
            # Document doesn't mention our subdivision at all
            # Could be a general lien/mortgage - allow with caution
            return True

        # Check if expected lot matches any of the document's lots EXACTLY
        lot_match = any(doc_lot.upper() == expected_lot.upper() for doc_lot in doc_lots)
        if not lot_match:
            return False

    # Check block match if we have an expected block
    if expected_block:
        if parsed.block:
            # Document has a block - must match exactly
            if parsed.block.upper() != expected_block.upper():
                return False
        else:
            # Document has NO block but we expect one
            # If document has a specific lot, it should also have a block in a platted subdivision
            # Reject it to prevent cross-property contamination (e.g., LOT 4 from wrong phase)
            doc_lots = parsed.lots or ([parsed.lot] if parsed.lot else [])
            if doc_lots:
                # Document specifies a lot but no block - likely wrong property
                return False
            # Document has no lot or block - could be general document, allow

    return True


class IterativeDiscovery:
    """
    Main iterative discovery service.

    Runs the discovery loop for a property, executing searches and
    processing results to find new search vectors.
    """

    def __init__(self, conn: sqlite3.Connection):
        """Initialize the discovery service."""
        self.conn = conn
        self.search_queue = SearchQueue(conn)
        self.name_matcher = NameMatcher(conn)
        self.ori_scraper = ORIApiScraper()

        # Property-level lot/block filter for current folio
        # These are set during run() and apply to ALL documents saved
        self._folio_expected_lot: str | None = None
        self._folio_expected_block: str | None = None
        self._folio_subdivision: str | None = None

    def run(
        self,
        folio: str,
        auction: dict,
        hcpa_data: Optional[dict] = None,
        final_judgment: Optional[dict] = None,
        bulk_parcel: Optional[dict] = None,
        fallback_mode: bool = False,
    ) -> DiscoveryResult:
        """
        Run iterative discovery for a folio.

        When fallback_mode=True, passes through to search queue to seed
        plaintiff name search and skip legal/plat seeds.

        Returns a DiscoveryResult with statistics and completion status.
        """
        logger.info(f"Starting iterative discovery for {folio} (fallback_mode={fallback_mode})")

        # Extract lot/block filter from the known legal description
        # This will be applied to ALL documents to prevent cross-property contamination
        self._set_folio_filter(hcpa_data, final_judgment, bulk_parcel)

        # Initialize search queue
        self.search_queue.initialize_for_folio(
            folio=folio,
            auction=auction,
            hcpa_data=hcpa_data,
            final_judgment=final_judgment,
            bulk_parcel=bulk_parcel,
            fallback_mode=fallback_mode,
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
                self.conn.commit()
                new_docs = self._execute_search(folio, search)
                documents_found += new_docs
                self.conn.commit()
                logger.debug(f"  Iteration {iteration}: {search.search_type} found {new_docs} new docs")

            except RateLimitError:
                self.search_queue.mark_rate_limited(search.id)
                self.conn.commit()
                logger.warning(f"Rate limited on search {search.id}")

            except Exception as e:
                self.search_queue.mark_failed(search.id, str(e))
                self.conn.commit()
                logger.error(f"Search failed: {e}")

        else:
            stopped_reason = "max_iterations"
            logger.warning(f"Max iterations reached for {folio}")

        # If exhausted or hit max_iterations but chain incomplete, try gap-bounded searches
        if stopped_reason in ("exhausted", "max_iterations") and not self._is_chain_complete(folio):
            gaps_queued = self._queue_gap_bounded_searches(folio)
            if gaps_queued > 0:
                logger.info(f"Queued {gaps_queued} gap-bounded searches for {folio}")
                # Allow extra iterations for gap-bounded searches even if we hit max_iterations
                gap_iteration_limit = iteration + 25
                while iteration < gap_iteration_limit:
                    iteration += 1

                    if self._is_chain_complete(folio):
                        stopped_reason = "complete"
                        break

                    search = self.search_queue.get_next_ready(folio)
                    if not search:
                        stopped_reason = "exhausted"
                        break

                    try:
                        self.search_queue.mark_in_progress(search.id)
                        self.conn.commit()
                        new_docs = self._execute_search(folio, search)
                        documents_found += new_docs
                        self.conn.commit()
                        logger.debug(f"  Gap-search iteration {iteration}: found {new_docs} new docs")
                    except RateLimitError:
                        self.search_queue.mark_rate_limited(search.id)
                        self.conn.commit()
                        logger.warning(f"Rate limited on gap-search {search.id} for {folio}")
                    except Exception as e:
                        self.search_queue.mark_failed(search.id, str(e))
                        self.conn.commit()
                        logger.error(f"Gap-search {search.id} failed for {folio}: {e}")

        # Link name variations across documents (e.g., "ROSRIGUEZ" <-> "RODRIGUEZ")
        self._link_party_variations(folio)

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

    def _set_folio_filter(
        self,
        hcpa_data: Optional[dict],
        final_judgment: Optional[dict],
        bulk_parcel: Optional[dict],
    ) -> None:
        """
        Set the lot/block/subdivision filter for the current folio.

        This filter is applied to ALL documents saved during discovery
        to prevent cross-property contamination when searching by party name.
        """
        # Reset filter
        self._folio_expected_lot = None
        self._folio_expected_block = None
        self._folio_subdivision = None

        # Try to extract from HCPA first (most reliable)
        legal_text = None
        if hcpa_data and hcpa_data.get("legal_description"):
            legal_text = hcpa_data["legal_description"]
        elif final_judgment and final_judgment.get("legal_description"):
            legal_text = final_judgment["legal_description"]
        elif bulk_parcel and bulk_parcel.get("raw_legal1"):
            legal_text = bulk_parcel["raw_legal1"]

        if not legal_text:
            logger.debug("No legal description available for folio filter")
            return

        # Parse the legal description
        parsed = parse_legal_description(legal_text)

        # Only set filter if we have specific lot/block info
        # (not for metes and bounds or vague descriptions)
        if parsed.lot or (parsed.lots and len(parsed.lots) == 1):
            self._folio_expected_lot = (parsed.lot or parsed.lots[0]).upper() if (parsed.lot or parsed.lots) else None
        if parsed.block:
            self._folio_expected_block = parsed.block.upper()
        if parsed.subdivision:
            self._folio_subdivision = parsed.subdivision.upper()

        if self._folio_expected_lot or self._folio_expected_block:
            logger.info(
                f"  Folio filter set: lot={self._folio_expected_lot} "
                f"block={self._folio_expected_block} subdiv={self._folio_subdivision}"
            )

    def _execute_search(self, folio: str, search: SearchItem) -> int:
        """
        Execute a search and process results.

        Returns the number of new documents found.
        """
        documents = self._run_search(search)

        if not documents:
            self.search_queue.mark_completed(search.id, 0, 0)
            return 0

        # Determine lot/block filter to use:
        # 1. For instrument searches, use folio filter (adjacent docs may be different properties)
        # 2. For legal searches, extract from search term (most specific)
        # 3. For name searches, use folio-level filter (prevents cross-property contamination)
        expected_lot: str | None = None
        expected_block: str | None = None

        if search.search_type == "instrument":
            # Instrument searches find docs by instrument number from adjacent searches
            # But adjacent docs could be for DIFFERENT properties recorded nearby
            # We MUST still apply lot/block filter to verify correct property
            expected_lot = self._folio_expected_lot
            expected_block = self._folio_expected_block
        elif search.search_type == "legal":
            # Use search-term-specific filter for legal searches
            expected_lot, expected_block = _extract_lot_block_from_search(search.search_term)
        else:
            # For name/case searches, use folio-level filter
            # This is CRITICAL to prevent cross-property contamination
            expected_lot = self._folio_expected_lot
            expected_block = self._folio_expected_block

        if expected_lot or expected_block:
            logger.debug(f"  Filtering for lot={expected_lot} block={expected_block}")

        # For instrument searches, collect known party names to validate docs without legal desc
        known_parties: set[str] = set()
        if search.search_type == "instrument":
            rows = self.conn.execute(
                "SELECT party_name_normalized FROM property_parties WHERE folio = ?",
                [folio],
            ).fetchall()
            known_parties = {dict(r)["party_name_normalized"] for r in rows if dict(r).get("party_name_normalized")}

        # Process each document
        new_count = 0
        filtered_count = 0
        for doc in documents:
            # Apply lot/block filter to verify document is for correct property
            if expected_lot or expected_block:
                doc_legal = doc.get("Legal") or doc.get("legal_description") or doc.get("legal")
                if doc_legal:
                    if not _document_matches_lot_block(
                        doc_legal, expected_lot, expected_block, self._folio_subdivision
                    ):
                        filtered_count += 1
                        continue
                elif search.search_type == "instrument" and known_parties:
                    # No legal description on an adjacent-instrument doc — require party overlap
                    doc_p1 = self.name_matcher.normalize(doc.get("Party1") or doc.get("party1") or "")
                    doc_p2 = self.name_matcher.normalize(doc.get("Party2") or doc.get("party2") or "")
                    if doc_p1 not in known_parties and doc_p2 not in known_parties:
                        filtered_count += 1
                        continue

            if self._save_document(folio, doc, search.id):
                new_count += 1
                self._extract_new_vectors(folio, doc, search.id)

        if filtered_count > 0:
            logger.debug(f"  Filtered out {filtered_count} docs not matching lot/block")

        # SHORT-CIRCUIT: If a legal search yields NEW documents (not just duplicates),
        # cancel broader fallback searches to save time on redundant permutations.
        if search.search_type == "legal" and new_count > 0:
            self.search_queue.cancel_pending_searches(folio, "legal")

        self.search_queue.mark_completed(search.id, len(documents), new_count)
        return new_count

    def _run_search(self, search: SearchItem) -> list[dict]:
        """Run a search via ORI API and return results."""
        try:
            if search.search_type == "legal":
                return self.ori_scraper.search_by_legal(
                    search.search_term,
                    start_date=self._format_date(search.date_from) or "01/01/1900",
                    end_date=self._format_date(search.date_to),
                )

            if search.search_type == "name":
                return self.ori_scraper.search_by_party(
                    search.search_term,
                    start_date=self._format_date(search.date_from) or "01/01/1900",
                    end_date=self._format_date(search.date_to),
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

            if search.search_type == "plat":
                # Parse plat book/page from search term (format: "P:BOOK/PAGE")
                # Plats use book_type="P" (Subdivision Plat Map), not "OR"
                term = search.search_term.removeprefix("P:")
                parts = term.split("/")
                if len(parts) == 2:
                    plat_book, plat_page = parts
                    logger.info(f"Searching plat: Book {plat_book} Page {plat_page}")
                    return self.ori_scraper.search_by_book_page_sync(plat_book, plat_page, book_type="P")
                logger.warning(f"Invalid plat format: {search.search_term}")
                return []

            if search.search_type == "case":
                # Case number search - uses dedicated CaseNum API field
                logger.debug(f"Case search: {search.search_term}")
                return self.ori_scraper.search_by_case_number(search.search_term)

            logger.warning(f"Unknown search type: {search.search_type}")
            return []

        except Exception as e:
            logger.error(
                "Search error for folio={folio} id={search_id} type={stype} term={term} "
                "date_from={date_from} date_to={date_to}: {err}",
                folio=search.folio,
                search_id=search.id,
                stype=search.search_type,
                term=search.search_term,
                date_from=search.date_from,
                date_to=search.date_to,
                err=e,
            )
            raise

    def _format_date(self, d: Optional[date | str]) -> Optional[str]:
        """Format date for ORI API. Handles both date objects and ISO strings from SQLite."""
        if not d:
            return None
        if isinstance(d, str):
            d = parse_date(d)
            if not d:
                return None
        return d.strftime("%m/%d/%Y")

    def _save_document(self, folio: str, doc: dict, search_id: int) -> bool:
        """
        Save a document if not already in database.

        Returns True if new document saved, False if duplicate.
        """
        instrument = doc.get("Instrument") or doc.get("instrument_number") or doc.get("instrument")
        if not instrument:
            return False

        # Ensure instrument is a clean string for VARCHAR column comparison
        instrument = str(instrument).strip()

        # Check if already exists
        existing = self.conn.execute(
            "SELECT id FROM documents WHERE instrument_number = ? LIMIT 1",
            [instrument],
        ).fetchone()

        if existing:
            return False

        # Parse document data
        recording_date = self._parse_ori_date(doc.get("RecordDate") or doc.get("record_date"))
        doc_type = doc.get("DocType") or doc.get("document_type") or doc.get("doc_type") or ""

        # Extract parties (handle both API formats)
        party1 = doc.get("party1")
        party2 = doc.get("party2")
        parties_one = doc.get("PartiesOne") or []
        parties_two = doc.get("PartiesTwo") or []

        # Handle instrument/book_page search format: single name + person_type per row
        if not party1 and not party2 and not parties_one and not parties_two:
            name = doc.get("name")
            person_type = doc.get("person_type") or doc.get("PersonType") or ""
            if name:
                if person_type == "1":
                    party1 = name
                elif person_type == "2":
                    party2 = name
                else:
                    party1 = name  # Default to grantor if unknown

        if not party1 and parties_one:
            party1 = ", ".join(parties_one)
        if not party2 and parties_two:
            party2 = ", ".join(parties_two)

        # Detect self-transfers (e.g., person to their trust, name variations)
        is_self_transfer = 0
        if party1 and party2:
            p1 = party1.strip().upper()
            p2 = party2.strip().upper()
            # Exact match or one name is a substring of the other
            if p1 == p2 or (len(p1) > 3 and p1 in p2) or (len(p2) > 3 and p2 in p1):
                is_self_transfer = 1

        # Insert document
        try:
            self.conn.execute(
                """
                INSERT INTO documents (
                    folio, document_type, instrument_number, recording_date,
                    book, page, party1, party2, legal_description,
                    sales_price, page_count, ori_uuid, ori_id, book_type,
                    triggered_by_search_id, parties_one, parties_two,
                    is_self_transfer
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    folio,
                    self._clean_doc_type(doc_type),
                    instrument,
                    recording_date,
                    (doc.get("Book") or doc.get("book") or doc.get("book_num") or "").strip() or None,
                    (doc.get("Page") or doc.get("page") or doc.get("page_num") or "").strip() or None,
                    party1,
                    party2,
                    doc.get("Legal") or doc.get("legal_description") or doc.get("legal"),
                    doc.get("SalesPrice") or doc.get("sales_price"),
                    doc.get("PageCount") or doc.get("page_count"),
                    doc.get("UUID") or doc.get("ori_uuid"),
                    doc.get("ID") or doc.get("ori_id"),
                    doc.get("BookType") or doc.get("book_type"),
                    search_id,
                    json.dumps(parties_one) if parties_one else None,
                    json.dumps(parties_two) if parties_two else None,
                    is_self_transfer,
                ],
            )
            return True
        except Exception as e:
            logger.error(
                "Failed to save document for folio={folio} search_id={search_id} "
                "instrument={instrument} doc_type={doc_type} "
                "parties_one_type={p1_type} parties_two_type={p2_type}: {err}",
                folio=folio,
                search_id=search_id,
                instrument=instrument,
                doc_type=doc_type,
                p1_type=type(parties_one).__name__,
                p2_type=type(parties_two).__name__,
                err=e,
            )
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
            except Exception as e:
                logger.debug(f"_parse_ori_date failed for value {date_val!r} (type={type(date_val).__name__}): {e}")
                return None

        return parse_date(date_val)

    def _clean_doc_type(self, doc_type: str) -> str:
        """Normalize document type string to canonical form."""
        if not doc_type:
            return ""
        from src.db.type_normalizer import normalize_document_type
        return normalize_document_type(doc_type)

    def _extract_new_vectors(self, folio: str, doc: dict, search_id: int) -> None:
        """
        Extract new search vectors from a document.

        Queues new searches for:
        - Legal description variants
        - Party names with date bounds
        - Referenced instruments
        """
        recording_date = self._parse_ori_date(doc.get("RecordDate") or doc.get("record_date"))
        instrument = doc.get("Instrument") or doc.get("instrument_number") or doc.get("instrument")

        # 1. Legal description
        legal = doc.get("Legal") or doc.get("legal_description") or doc.get("legal")
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

        # 2. Party names (may be list from API or JSON string from SQLite)
        parties_one = _parse_json_list(doc.get("PartiesOne") or doc.get("parties_one"))
        parties_two = _parse_json_list(doc.get("PartiesTwo") or doc.get("parties_two"))

        if not parties_one and doc.get("party1"):
            parties_one = [doc.get("party1")]
        if not parties_two and doc.get("party2"):
            parties_two = [doc.get("party2")]

        # Handle instrument/book_page search format: single name + person_type per row
        if not parties_one and not parties_two:
            name = doc.get("name")
            person_type = doc.get("person_type") or doc.get("PersonType") or ""
            if name:
                if person_type == "1":
                    parties_one = [name]
                elif person_type == "2":
                    parties_two = [name]
                else:
                    parties_one = [name]  # Default to grantor if unknown

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
            p1 = parties_one[0] if parties_one else ""
            p2 = parties_two[0] if parties_two else ""
            if p1 and p2:
                self.name_matcher.detect_and_link(folio, p1, p2)

        # 4. Extract instrument references from document text
        # Documents may reference other instruments (e.g., "CLK #2019437669")
        referenced_instruments = self._extract_instrument_references(doc)
        for ref_instrument in referenced_instruments:
            # Queue a search for each referenced instrument
            try:
                self.conn.execute(
                    """
                    INSERT OR IGNORE INTO ori_search_queue (
                        folio, search_type, search_term, priority, status,
                        triggered_by_search_id, triggered_by_instrument
                    )
                    VALUES (?, 'instrument', ?, ?, 'ready', ?, ?)
                    """,
                    [folio, ref_instrument, PRIORITY_INSTRUMENT, search_id, instrument],
                )
                logger.debug(f"  Queued referenced instrument: {ref_instrument}")
            except Exception as e:
                logger.debug(f"Could not queue instrument {ref_instrument}: {e}")

        # 5. Queue adjacent instrument searches for deeds/mortgages
        # Deeds and mortgages are often recorded sequentially on the same day
        doc_type = doc.get("DocType") or doc.get("document_type") or doc.get("doc_type") or ""
        if instrument:
            self._queue_adjacent_instruments(folio, str(instrument), doc_type, search_id)

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

    def _extract_instrument_references(self, doc: dict) -> list[str]:
        """
        Extract instrument number references from document fields.

        Documents often reference related instruments with patterns like:
        - "CLK #2019437669"
        - "INST #2019437669"
        - "INSTRUMENT NO. 2019437669"
        - "O.R. BOOK 12345 PAGE 678" (converted to instrument lookup)

        Returns list of instrument numbers found.
        """
        import re

        references: set[str] = set()

        # Fields that may contain references
        fields_to_check = [
            doc.get("Legal") or doc.get("legal_description") or doc.get("legal") or "",
            doc.get("Comments") or doc.get("comments") or "",
            doc.get("party1") or "",
            doc.get("party2") or "",
        ]

        # Also check parties arrays
        fields_to_check.extend(doc.get("PartiesOne") or [])
        fields_to_check.extend(doc.get("PartiesTwo") or [])

        text = " ".join(str(f) for f in fields_to_check if f)

        # Pattern 1: CLK #NNNNNNNNNN or CLK#NNNNNNNNNN
        references.update(match.group(1) for match in re.finditer(r"CLK\s*#?\s*(\d{7,10})", text, re.IGNORECASE))

        # Pattern 2: INST #NNNNNNNNNN or INSTRUMENT NO. NNNNNNNNNN
        references.update(
            match.group(1) for match in re.finditer(r"INST(?:RUMENT)?\s*(?:#|NO\.?)?\s*(\d{7,10})", text, re.IGNORECASE)
        )

        # Pattern 3: O.R. NNNNNNNNNN or OR NNNNNNNNNN (direct instrument)
        references.update(match.group(1) for match in re.finditer(r"O\.?R\.?\s+(\d{7,10})", text, re.IGNORECASE))

        # Don't include the document's own instrument number
        own_instrument = str(doc.get("Instrument") or doc.get("instrument_number") or doc.get("instrument") or "")
        references.discard(own_instrument)

        return list(references)

    def _queue_adjacent_instruments(
        self,
        folio: str,
        instrument: str,
        doc_type: str,
        search_id: int,
    ) -> int:
        """
        Queue searches for adjacent instrument numbers.

        When we find a mortgage or deed, the related deed/mortgage is often
        recorded the same day with a sequential instrument number.

        Example: Deed 2019437668, Mortgage 2019437669

        Args:
            folio: Property folio number
            instrument: The instrument number we found
            doc_type: Document type (to determine what to look for)
            search_id: ID of the search that found this document

        Returns:
            Number of adjacent instrument searches queued
        """
        # Only queue adjacents for deeds and mortgages — these are recorded
        # together (e.g. Deed 2019437668, Mortgage 2019437669).
        # NOT judgments, lis pendens, liens (standalone filings).
        doc_type_str = (doc_type or "").strip()
        m = _PAREN_RE.match(doc_type_str)
        if m:
            canonical = _DOC_TYPE_MAP.get(m.group(1).upper(), "")
        else:
            # Try as ORI code; fall back to the string itself (handles
            # already-normalized values like "deed", "mortgage" from DB)
            canonical = _DOC_TYPE_MAP.get(
                doc_type_str.upper(), doc_type_str.lower()
            )
        if canonical not in ("deed", "mortgage"):
            return 0

        # Parse instrument as integer
        try:
            base_instrument = int(str(instrument).strip())
        except (ValueError, TypeError):
            logger.debug(f"Cannot parse instrument as integer: {instrument}")
            return 0

        queued = 0
        for offset in range(-ADJACENT_INSTRUMENT_RANGE, ADJACENT_INSTRUMENT_RANGE + 1):
            if offset == 0:
                continue  # Skip the original instrument

            adjacent_instrument = str(base_instrument + offset)

            # Check if we already have this instrument
            existing = self.conn.execute(
                "SELECT id FROM documents WHERE instrument_number = ? LIMIT 1",
                [adjacent_instrument],
            ).fetchone()

            if existing:
                continue  # Already have this document

            # Check if already queued
            already_queued = self.conn.execute(
                """
                SELECT id FROM ori_search_queue
                WHERE folio = ? AND search_type = 'instrument' AND search_term = ?
                LIMIT 1
                """,
                [folio, adjacent_instrument],
            ).fetchone()

            if already_queued:
                continue

            # Queue the adjacent instrument search
            try:
                self.conn.execute(
                    """
                    INSERT INTO ori_search_queue (
                        folio, search_type, search_term, priority, status,
                        triggered_by_search_id, triggered_by_instrument
                    )
                    VALUES (?, 'instrument', ?, ?, 'ready', ?, ?)
                    """,
                    [
                        folio,
                        adjacent_instrument,
                        PRIORITY_INSTRUMENT,
                        search_id,
                        instrument,
                    ],
                )
                queued += 1
            except Exception as e:
                logger.debug(f"Could not queue adjacent instrument {adjacent_instrument}: {e}")

        if queued > 0:
            logger.debug(f"  Queued {queued} adjacent instruments for {instrument} (±{ADJACENT_INSTRUMENT_RANGE})")

        return queued

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

        Complete means: unbroken sequence of ownership from anchor to present.

        Checks:
        1. Have anchor (plat/root of title) with known date
        2. Have at least one deed
        3. First deed is within MAX_ANCHOR_GAP_DAYS of anchor
        4. Each deed's grantee matches next deed's grantor
        5. Last deed's grantee matches current owner
        """
        # Fallback: if chain covers MRTA years, it's marketable regardless
        chain_years = self._calculate_chain_years(folio)
        if chain_years >= MRTA_YEARS_REQUIRED:
            logger.debug(f"Chain complete via MRTA: {chain_years:.1f} years")
            return True

        # Get anchor date (plat or oldest root of title)
        anchor_date = self._get_anchor_date(folio)
        if not anchor_date:
            logger.debug(f"Chain incomplete: no anchor date found for {folio}")
            return False

        # Get all deeds sorted by date
        deeds = self._get_deeds(folio)
        if not deeds:
            logger.debug(f"Chain incomplete: no deeds found for {folio}")
            return False

        # Check 1: First deed should be near anchor
        first_deed = deeds[0]
        first_deed_date = first_deed["recording_date"]
        if isinstance(first_deed_date, str):
            first_deed_date = parse_date(first_deed_date)

        if first_deed_date:
            days_from_anchor = (first_deed_date - anchor_date).days
            if days_from_anchor > MAX_ANCHOR_GAP_DAYS:
                logger.debug(
                    f"Chain incomplete: {days_from_anchor} days gap between "
                    f"anchor ({anchor_date}) and first deed ({first_deed_date})"
                )
                return False

        # Check 2: Each grantee matches next grantor
        for i in range(len(deeds) - 1):
            current_grantee = deeds[i].get("grantee") or deeds[i].get("party_two")
            next_grantor = deeds[i + 1].get("grantor") or deeds[i + 1].get("party_one")

            if current_grantee and next_grantor:
                match = self.name_matcher.match(current_grantee, next_grantor)
                if not match.is_match:
                    logger.debug(f"Chain break: '{current_grantee}' != '{next_grantor}' between deed {i + 1} and {i + 2}")
                    return False

        # Check 3: Last grantee should match current owner
        current_owner = self._get_current_owner(folio)
        if current_owner:
            last_grantee = deeds[-1].get("grantee") or deeds[-1].get("party_two")
            if last_grantee:
                match = self.name_matcher.match(last_grantee, current_owner)
                if not match.is_match:
                    logger.debug(f"Chain incomplete: last grantee '{last_grantee}' != current owner '{current_owner}'")
                    return False

        # Check 4: Log any large gaps (warning only, not failure)
        for i in range(len(deeds) - 1):
            date1 = deeds[i]["recording_date"]
            date2 = deeds[i + 1]["recording_date"]
            if isinstance(date1, str):
                date1 = parse_date(date1)
            if isinstance(date2, str):
                date2 = parse_date(date2)
            if date1 and date2:
                gap_days = (date2 - date1).days
                if gap_days > MAX_OWNERSHIP_GAP_DAYS:
                    logger.warning(f"Large gap of {gap_days} days between deeds for {folio}")

        logger.info(f"Chain complete for {folio}: {len(deeds)} deeds from anchor")
        return True

    def _get_anchor_date(self, folio: str) -> Optional[date]:
        """
        Get the anchor (root of title) date for a folio.

        Priority:
        1. Plat document recording date
        2. Government patent/deed
        3. Oldest deed if no plat (for properties predating modern records)
        """
        # Check for plat in documents
        result = self.conn.execute(
            """
            SELECT recording_date
            FROM documents
            WHERE folio = ?
              AND UPPER(document_type) LIKE '%PLAT%'
            ORDER BY recording_date ASC
            LIMIT 1
            """,
            [folio],
        ).fetchone()

        if result and result[0]:
            anchor = result[0]
            if isinstance(anchor, str):
                anchor = parse_date(anchor)
            return anchor

        # Check for government patent
        result = self.conn.execute(
            """
            SELECT recording_date
            FROM documents
            WHERE folio = ?
              AND UPPER(document_type) IN ('PATENT', 'GOVERNMENT DEED', 'GOV DEED')
            ORDER BY recording_date ASC
            LIMIT 1
            """,
            [folio],
        ).fetchone()

        if result and result[0]:
            anchor = result[0]
            if isinstance(anchor, str):
                anchor = parse_date(anchor)
            return anchor

        # Fallback: oldest deed (for properties without plat in our data)
        result = self.conn.execute(
            """
            SELECT MIN(recording_date)
            FROM documents
            WHERE folio = ?
              AND recording_date IS NOT NULL
            """,
            [folio],
        ).fetchone()

        if result and result[0]:
            anchor = result[0]
            if isinstance(anchor, str):
                anchor = parse_date(anchor)
            return anchor

        return None

    def _get_deeds(self, folio: str) -> list[dict]:
        """Get all deed documents for a folio, sorted by recording date."""
        # Build deed type filter - handle both short codes and full names
        deed_patterns = []
        for dt in DEED_TYPES:
            deed_patterns.append(f"UPPER(document_type) = '{dt}'")
            deed_patterns.append(f"UPPER(document_type) LIKE '%({dt})%'")

        where_clause = " OR ".join(deed_patterns)

        result = self.conn.execute(
            f"""
            SELECT
                instrument_number,
                recording_date,
                document_type,
                party1 as grantor,
                party2 as grantee
            FROM documents
            WHERE folio = ?
              AND ({where_clause})
            ORDER BY recording_date ASC
            """,
            [folio],
        ).fetchall()

        return [dict(row) for row in result]

    def _get_current_owner(self, folio: str) -> Optional[str]:
        """Get the current owner from HCPA data or auctions table."""
        query_conn = self.conn

        # Try parcels table first (HCPA data)
        try:
            result = query_conn.execute(
                """
                SELECT owner_name
                FROM parcels
                WHERE folio = ?
                LIMIT 1
                """,
                [folio],
            ).fetchone()

            if result and result[0]:
                return result[0]
        except Exception as e:
            logger.debug(f"Could not query parcels table: {e}")

        # Try auctions table (defendant is usually the current owner)
        try:
            result = query_conn.execute(
                """
                SELECT defendant
                FROM auctions
                WHERE parcel_id = ? OR folio = ?
                LIMIT 1
                """,
                [folio, folio],
            ).fetchone()

            if result and result[0]:
                return result[0]
        except Exception as e:
            logger.debug(f"Could not query auctions table: {e}")

        return None

    def _calculate_chain_years(self, folio: str) -> float:
        """Calculate total years covered by deed documents."""
        # Build deed type filter - handle both short codes and full ORI format
        deed_patterns = []
        for dt in DEED_TYPES:
            deed_patterns.append(f"UPPER(document_type) = '{dt}'")
            deed_patterns.append(f"UPPER(document_type) LIKE '%({dt})%'")

        where_clause = " OR ".join(deed_patterns)

        result = self.conn.execute(
            f"""
            SELECT MIN(recording_date) as oldest, MAX(recording_date) as newest
            FROM documents
            WHERE folio = ? AND recording_date IS NOT NULL
              AND ({where_clause})
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

    def _get_chain_gaps(self, folio: str) -> list[ChainGap]:
        """
        Identify gaps in the chain of title.

        Returns a list of ChainGap objects describing:
        - Gap between anchor (plat) and first deed
        - Gaps between consecutive deeds (ownership breaks)
        - Gap between last deed and current owner

        These gaps can be used to bound name searches to specific date ranges,
        making searches like "LENNAR HOMES" tractable (6000+ unbounded results
        vs ~10 bounded results).
        """
        from datetime import datetime, UTC

        gaps: list[ChainGap] = []
        today = datetime.now(tz=UTC).date()

        # Get anchor date
        anchor_date = self._get_anchor_date(folio)

        # Get all deeds sorted by date
        deeds = self._get_deeds(folio)

        # Get developer name from plat if available
        developer = self._get_developer_from_plat(folio)

        # Get current owner
        current_owner = self._get_current_owner(folio)

        # Gap 1: Anchor to first deed
        if anchor_date and deeds:
            first_deed = deeds[0]
            first_deed_date = first_deed["recording_date"]
            if isinstance(first_deed_date, str):
                first_deed_date = parse_date(first_deed_date)

            if first_deed_date:
                gap_days = (first_deed_date - anchor_date).days
                if gap_days > MAX_ANCHOR_GAP_DAYS:
                    # First deed is too far from anchor - need intermediate deed
                    first_grantor = first_deed.get("grantor")
                    gaps.append(
                        ChainGap(
                            start_date=anchor_date,
                            end_date=first_deed_date,
                            gap_type="anchor_to_first_deed",
                            expected_grantor=developer,  # Plat developer should be selling
                            expected_grantee=first_grantor,  # First deed's grantor bought from someone
                            days=gap_days,
                        )
                    )
        elif anchor_date and not deeds:
            # Have anchor but NO deeds - major gap
            gaps.append(
                ChainGap(
                    start_date=anchor_date,
                    end_date=today,
                    gap_type="anchor_to_first_deed",
                    expected_grantor=developer,
                    expected_grantee=current_owner,
                    days=(today - anchor_date).days,
                )
            )

        # Gap 2: Between consecutive deeds (ownership breaks)
        for i in range(len(deeds) - 1):
            current_deed = deeds[i]
            next_deed = deeds[i + 1]

            current_grantee = current_deed.get("grantee")
            next_grantor = next_deed.get("grantor")

            # Check if grantee matches next grantor
            if current_grantee and next_grantor:
                match = self.name_matcher.match(current_grantee, next_grantor)
                if not match.is_match:
                    # Chain break - different people
                    date1 = current_deed["recording_date"]
                    date2 = next_deed["recording_date"]
                    if isinstance(date1, str):
                        date1 = parse_date(date1)
                    if isinstance(date2, str):
                        date2 = parse_date(date2)

                    if date1 and date2:
                        gap_days = (date2 - date1).days
                        gaps.append(
                            ChainGap(
                                start_date=date1,
                                end_date=date2,
                                gap_type="ownership_gap",
                                expected_grantor=current_grantee,  # Who should be selling
                                expected_grantee=next_grantor,  # Who should be buying
                                days=gap_days,
                            )
                        )

        # Gap 3: Last deed to current owner
        if deeds and current_owner:
            last_deed = deeds[-1]
            last_grantee = last_deed.get("grantee")

            if last_grantee:
                match = self.name_matcher.match(last_grantee, current_owner)
                if not match.is_match:
                    # Last deed grantee doesn't match current owner
                    last_date = last_deed["recording_date"]
                    if isinstance(last_date, str):
                        last_date = parse_date(last_date)

                    if last_date:
                        gap_days = (today - last_date).days
                        gaps.append(
                            ChainGap(
                                start_date=last_date,
                                end_date=today,
                                gap_type="to_current_owner",
                                expected_grantor=last_grantee,  # Last known owner
                                expected_grantee=current_owner,  # Current owner
                                days=gap_days,
                            )
                        )

        if gaps:
            logger.info(f"Found {len(gaps)} chain gaps for {folio}")
            for gap in gaps:
                logger.debug(
                    f"  Gap: {gap.gap_type} from {gap.start_date} to {gap.end_date} "
                    f"({gap.days} days), expected: {gap.expected_grantor} → {gap.expected_grantee}"
                )

        return gaps

    def _get_developer_from_plat(self, folio: str) -> Optional[str]:
        """Get the developer/owner name from the plat document."""
        result = self.conn.execute(
            """
            SELECT party1, party2, parties_one
            FROM documents
            WHERE folio = ?
              AND UPPER(document_type) LIKE '%PLAT%'
            ORDER BY recording_date ASC
            LIMIT 1
            """,
            [folio],
        ).fetchone()

        if result:
            # party1 or parties_one typically has the developer
            if result[0]:
                return result[0]
            parties = _parse_json_list(result[2])
            if parties:
                return parties[0]

        return None

    def _queue_gap_bounded_searches(self, folio: str) -> int:
        """
        Queue name searches bounded by chain gap dates.

        When normal discovery exhausts, analyze the chain for gaps and queue
        targeted searches with specific date bounds. This makes searches like
        "LENNAR HOMES" tractable (6000+ unbounded results vs ~10 bounded).

        Returns the number of searches queued.
        """
        gaps = self._get_chain_gaps(folio)
        if not gaps:
            return 0

        queued = 0
        for gap in gaps:
            # Queue search for expected grantor (seller)
            if gap.expected_grantor and not self.name_matcher.is_generic(gap.expected_grantor):
                try:
                    self.conn.execute(
                        """
                        INSERT OR IGNORE INTO ori_search_queue (
                            folio, search_type, search_term, priority, status,
                            date_from, date_to, triggered_by_instrument
                        )
                        VALUES (?, 'name', ?, ?, 'ready', ?, ?, ?)
                        """,
                        [
                            folio,
                            gap.expected_grantor,
                            PRIORITY_NAME_CHAIN - 5,  # Higher priority for gap searches
                            gap.start_date,
                            gap.end_date,
                            f"gap:{gap.gap_type}",
                        ],
                    )
                    queued += 1
                    logger.debug(f"  Queued gap search: {gap.expected_grantor} ({gap.start_date} to {gap.end_date})")
                except Exception as e:
                    logger.debug(f"Could not queue gap search: {e}")

            # Queue search for expected grantee (buyer)
            if gap.expected_grantee and not self.name_matcher.is_generic(gap.expected_grantee):
                try:
                    self.conn.execute(
                        """
                        INSERT OR IGNORE INTO ori_search_queue (
                            folio, search_type, search_term, priority, status,
                            date_from, date_to, triggered_by_instrument
                        )
                        VALUES (?, 'name', ?, ?, 'ready', ?, ?, ?)
                        """,
                        [
                            folio,
                            gap.expected_grantee,
                            PRIORITY_NAME_CHAIN - 5,
                            gap.start_date,
                            gap.end_date,
                            f"gap:{gap.gap_type}",
                        ],
                    )
                    queued += 1
                    logger.debug(f"  Queued gap search: {gap.expected_grantee} ({gap.start_date} to {gap.end_date})")
                except Exception as e:
                    logger.debug(f"Could not queue gap search: {e}")

        return queued

    def _link_party_variations(self, folio: str) -> int:
        """
        Link spelling variations of party names across documents.

        After discovery completes, compare all party names for this folio
        and link any that are spelling variations of each other.

        For example: "ROSRIGUEZ APONTE" and "RODRIGUEZ APONTE" would be
        linked to the same identity.

        Uses Union-Find clustering to properly group all variations before
        creating identities, avoiding the problem of overwriting links when
        A↔B and A↔C are both matches.

        Returns the number of new links created.
        """
        # Get all distinct party names for this folio that aren't already linked
        parties = self.conn.execute(
            """
            SELECT DISTINCT party_name
            FROM property_parties
            WHERE folio = ? AND linked_identity_id IS NULL
            """,
            [folio],
        ).fetchall()

        if len(parties) < 2:
            return 0

        party_names = [row[0] for row in parties]

        # Union-Find data structure for clustering
        parent: dict[str, str] = {name: name for name in party_names}

        def find(name: str) -> str:
            """Find the root of a name's cluster with path compression."""
            if parent[name] != name:
                parent[name] = find(parent[name])
            return parent[name]

        def union(name1: str, name2: str) -> None:
            """Merge two clusters."""
            root1, root2 = find(name1), find(name2)
            if root1 != root2:
                parent[root1] = root2

        # Build clusters by comparing pairs
        for i, name1 in enumerate(party_names):
            for name2 in party_names[i + 1 :]:
                # Skip if already in same cluster
                if find(name1) == find(name2):
                    continue

                match_result = self.name_matcher.match(name1, name2)
                if match_result.is_match and match_result.link_type == "spelling_variation":
                    union(name1, name2)

        # Group names by their cluster root
        clusters: dict[str, list[str]] = {}
        for name in party_names:
            root = find(name)
            if root not in clusters:
                clusters[root] = []
            clusters[root].append(name)

        # Create one identity per cluster (skip single-member clusters)
        links_created = 0
        for members in clusters.values():
            if len(members) < 2:
                continue

            # Use the first member as canonical name
            canonical = members[0]

            # Create the linked identity
            identity_id = self.name_matcher.get_or_create_linked_identity(
                canonical,
                canonical,
                "spelling_variation",
                0.85,  # Default threshold for spelling variations
            )

            # Link ALL members to this single identity
            for member in members:
                self.name_matcher.link_party_to_identity(folio, member, identity_id)

            logger.info(f"  Linked spelling cluster: {members} → identity {identity_id}")
            links_created += len(members) - 1  # Count links, not members

        if links_created > 0:
            logger.info(f"Created {links_created} name variation links for {folio}")

        return links_created

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
