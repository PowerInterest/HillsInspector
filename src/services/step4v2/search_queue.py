"""
Search Queue - Manages the ORI search queue for iterative discovery.

This module provides:
- Queue initialization from various data sources
- Adding searches with priority and deduplication
- Getting next pending search
- Marking searches as completed/failed/rate-limited
- Search tracking and statistics
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

import sqlite3
from loguru import logger

from config.step4v2 import (
    MAX_SEARCHES_PER_FOLIO,
    PRIORITY_BOOK_PAGE,
    PRIORITY_CASE,
    PRIORITY_INSTRUMENT,
    PRIORITY_LEGAL_BEGINS,
    PRIORITY_LEGAL_CONTAINS,
    PRIORITY_NAME_CHAIN,
    PRIORITY_NAME_GENERIC,
    PRIORITY_NAME_OWNER,
    PRIORITY_PLAT,
    RATE_LIMIT_BACKOFF_SECONDS,
)
from src.utils.legal_description import parse_legal_description, generate_search_permutations
from src.services.step4v2.name_matcher import NameMatcher


@dataclass
class SearchItem:
    """A search item from the queue."""

    id: int
    folio: str
    search_type: str  # 'plat', 'book_page', 'instrument', 'legal', 'name', 'case'
    search_term: str
    search_operator: str  # 'EQUALS', 'BEGINS', 'CONTAINS' (for legal)
    priority: int
    status: str  # 'pending', 'in_progress', 'completed', 'failed', 'rate_limited'
    attempt_count: int
    date_from: Optional[date]
    date_to: Optional[date]
    triggered_by_instrument: Optional[str]
    triggered_by_search_id: Optional[int]


class SearchQueue:
    """
    Search queue manager for iterative discovery.

    Handles queueing, deduplication, and prioritization of ORI searches.
    """

    def __init__(self, conn: sqlite3.Connection):
        """Initialize the search queue manager."""
        self.conn = conn
        self.name_matcher = NameMatcher(conn)
        self._limit_warned_folios: set[str] = set()  # Track folios that hit search limit  # Track folios that hit search limit

    def initialize_for_folio(
        self,
        folio: str,
        auction: dict,
        hcpa_data: Optional[dict] = None,
        final_judgment: Optional[dict] = None,
        bulk_parcel: Optional[dict] = None,
    ) -> int:
        """
        Initialize the search queue for a folio from all available data sources.

        Returns the number of searches queued.
        """
        count = 0

        # =====================================================================
        # TIER 1: EXACT LOOKUPS (No false positives - search these FIRST)
        # =====================================================================

        # 1a. Plat Book/Page from Final Judgment - ROOT OF TITLE
        # The plat is when the lot legally came into existence. This is the
        # absolute beginning of the chain of title for platted properties.
        # 95.6% of FJs have plat_book/plat_page extracted.
        # IMPORTANT: Plats use book_type="P" (Subdivision Plat Map), not "OR"
        if final_judgment:
            plat_book = final_judgment.get("plat_book")
            plat_page = final_judgment.get("plat_page")
            if plat_book and plat_page and self.queue_plat_search(folio, str(plat_book), str(plat_page)):
                count += 1
                logger.debug(f"Queued plat search: Plat Book {plat_book} Page {plat_page}")

        # 1b. Book/Page AND Instrument from HCPA Sales History
        # Modern recordings (post ~2010) use instrument numbers only.
        # Older recordings have book/page. We search for BOTH to cover all eras.
        sales_history = self._get_sales_history(folio)
        for sale in sales_history:
            # Queue instrument search (modern recordings - post ~2010)
            instrument = sale.get("instrument")
            if instrument and self.queue_instrument_search(folio, str(instrument)):
                count += 1
            # Queue book/page search (older recordings - pre ~2010)
            if sale.get("book") and sale.get("page") and self.queue_book_page_search(folio, sale["book"], sale["page"]):
                count += 1

        # =====================================================================
        # TIER 2: HIGH-CONFIDENCE LEGAL DESCRIPTIONS
        # =====================================================================

        # 2a. Final Judgment legal description (most accurate - courts verify this)
        if final_judgment and final_judgment.get("legal_description"):
            count += self.queue_legal_search(
                folio,
                final_judgment["legal_description"],
                operator="BEGINS",
                priority=PRIORITY_LEGAL_BEGINS,
                source_type="final_judgment",
            )

        # 2b. HCPA legal description
        if hcpa_data and hcpa_data.get("legal_description"):
            count += self.queue_legal_search(
                folio,
                hcpa_data["legal_description"],
                operator="BEGINS",
                priority=PRIORITY_LEGAL_BEGINS + 5,  # Slightly lower priority than final judgment
                source_type="hcpa",
            )

        # =====================================================================
        # TIER 3: LOWER CONFIDENCE SOURCES (Last resort)
        # =====================================================================

        # 3. Bulk parcel legal descriptions (raw_legal1-4)
        if bulk_parcel:
            for i in range(1, 5):
                legal = bulk_parcel.get(f"raw_legal{i}")
                if legal and legal.strip():
                    count += self.queue_legal_search(
                        folio,
                        legal,
                        operator="CONTAINS",
                        priority=PRIORITY_LEGAL_CONTAINS,
                        source_type="bulk_import",
                    )

        # 4. Case number search (for foreclosure docs)
        case_number = auction.get("case_number")
        if case_number:
            self.queue_case_search(folio, case_number)
            count += 1

        # 4. Party names with date bounds
        auction_date = auction.get("auction_date")

        defendant = auction.get("defendant")
        if defendant and not self.name_matcher.is_generic(defendant):
            self.queue_name_search(
                folio,
                defendant,
                date_to=auction_date,
                priority=PRIORITY_NAME_OWNER,
            )
            count += 1

        # Final judgment defendant (may differ from auction)
        if final_judgment:
            fj_defendant = final_judgment.get("defendant")
            if fj_defendant and fj_defendant != defendant and not self.name_matcher.is_generic(fj_defendant):
                self.queue_name_search(
                    folio,
                    fj_defendant,
                    date_to=auction_date,
                    priority=PRIORITY_NAME_OWNER,
                )
                count += 1

        # HCPA owner
        if hcpa_data:
            owner = hcpa_data.get("owner_name")
            if owner and not self.name_matcher.is_generic(owner):
                last_sale_date = hcpa_data.get("last_sale_date")
                self.queue_name_search(
                    folio,
                    owner,
                    date_from=last_sale_date,
                    priority=PRIORITY_NAME_OWNER,
                )
                count += 1

        logger.info(f"Initialized search queue for {folio}: {count} searches queued")
        return count

    def _get_sales_history(self, folio: str) -> list[dict]:
        """Get sales history records for a folio."""
        try:
            result = self.conn.execute(
                """
                SELECT book, page, instrument, sale_date, doc_type, sale_price, grantor, grantee
                FROM sales_history
                WHERE folio = ? OR strap = ?
                ORDER BY sale_date DESC
                """,
                [folio, folio],
            ).fetchall()

            return [dict(row) for row in result]
        except Exception:
            # sales_history may not exist
            return []

    def queue_legal_search(
        self,
        folio: str,
        legal_text: str,
        operator: str = "BEGINS",
        priority: int = PRIORITY_LEGAL_BEGINS,
        source_type: str = "unknown",
        triggered_by_instrument: Optional[str] = None,
        triggered_by_search_id: Optional[int] = None,
    ) -> int:
        """
        Queue legal description searches using parsed permutations.

        Parses the legal description and generates multiple search terms
        with different formats (e.g., "L 16 B 11 KINGS LAKE*", "KINGS LAKE PHASE 3*").

        Returns the number of searches queued.
        """
        if not legal_text or not legal_text.strip():
            return 0

        # Clean the legal text
        legal_text = legal_text.strip()[:500]

        # Save raw legal to variations table
        self._save_legal_variation(folio, legal_text, source_type, triggered_by_instrument, priority)

        # Parse and generate permutations
        parsed = parse_legal_description(legal_text)
        permutations = generate_search_permutations(parsed, raw_legal=legal_text, max_permutations=5)

        if not permutations:
            # Fallback to raw text if parsing fails
            permutations = [legal_text[:100]]

        queued = 0
        for i, perm in enumerate(permutations):
            # Increment priority for each permutation (first is highest priority)
            perm_priority = priority + i
            if self._queue_search(
                folio=folio,
                search_type="legal",
                search_term=perm,
                search_operator=operator,
                priority=perm_priority,
                triggered_by_instrument=triggered_by_instrument,
                triggered_by_search_id=triggered_by_search_id,
            ):
                queued += 1

        return queued

    def queue_book_page_search(
        self,
        folio: str,
        book: str,
        page: str,
        triggered_by_instrument: Optional[str] = None,
        triggered_by_search_id: Optional[int] = None,
    ) -> bool:
        """Queue a book/page search (highest priority)."""
        if not book or not page:
            return False

        search_term = f"{book}/{page}"
        return self._queue_search(
            folio=folio,
            search_type="book_page",
            search_term=search_term,
            priority=PRIORITY_BOOK_PAGE,
            triggered_by_instrument=triggered_by_instrument,
            triggered_by_search_id=triggered_by_search_id,
        )

    def queue_plat_search(
        self,
        folio: str,
        plat_book: str,
        plat_page: str,
        triggered_by_instrument: Optional[str] = None,
        triggered_by_search_id: Optional[int] = None,
    ) -> bool:
        """
        Queue a plat book/page search (ROOT OF TITLE - highest priority).

        Plats use book_type="P" (Subdivision Plat Map), not "OR" (Official Records).
        The plat recording is when the lot legally came into existence.
        This is the absolute beginning of the chain of title.

        Args:
            folio: Property folio
            plat_book: Plat book number (e.g., "135")
            plat_page: Plat page number (e.g., "12")

        Returns:
            True if queued, False if duplicate or invalid
        """
        if not plat_book or not plat_page:
            return False

        # Use "P:" prefix to indicate this is a plat search (book_type="P")
        search_term = f"P:{plat_book}/{plat_page}"
        return self._queue_search(
            folio=folio,
            search_type="plat",
            search_term=search_term,
            priority=PRIORITY_PLAT,
            triggered_by_instrument=triggered_by_instrument,
            triggered_by_search_id=triggered_by_search_id,
        )

    def queue_instrument_search(
        self,
        folio: str,
        instrument: str,
        triggered_by_instrument: Optional[str] = None,
        triggered_by_search_id: Optional[int] = None,
    ) -> bool:
        """Queue an instrument number search."""
        if not instrument:
            return False

        return self._queue_search(
            folio=folio,
            search_type="instrument",
            search_term=instrument.strip(),
            priority=PRIORITY_INSTRUMENT,
            triggered_by_instrument=triggered_by_instrument,
            triggered_by_search_id=triggered_by_search_id,
        )

    def queue_case_search(
        self,
        folio: str,
        case_number: str,
        triggered_by_search_id: Optional[int] = None,
    ) -> bool:
        """Queue a case number search."""
        if not case_number:
            return False

        return self._queue_search(
            folio=folio,
            search_type="case",
            search_term=case_number.strip(),
            priority=PRIORITY_CASE,
            triggered_by_search_id=triggered_by_search_id,
        )

    def queue_name_search(
        self,
        folio: str,
        party_name: str,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        priority: int = PRIORITY_NAME_CHAIN,
        triggered_by_instrument: Optional[str] = None,
        triggered_by_search_id: Optional[int] = None,
    ) -> bool:
        """Queue a party name search with optional date bounds."""
        if not party_name or not party_name.strip():
            return False

        # Check if generic
        if self.name_matcher.is_generic(party_name):
            priority = PRIORITY_NAME_GENERIC  # Demote to lowest priority

        return self._queue_search(
            folio=folio,
            search_type="name",
            search_term=party_name.strip(),
            priority=priority,
            date_from=date_from,
            date_to=date_to,
            triggered_by_instrument=triggered_by_instrument,
            triggered_by_search_id=triggered_by_search_id,
        )

    def _queue_search(
        self,
        folio: str,
        search_type: str,
        search_term: str,
        priority: int,
        search_operator: str = "",
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        triggered_by_instrument: Optional[str] = None,
        triggered_by_search_id: Optional[int] = None,
    ) -> bool:
        """
        Internal method to queue a search with deduplication.

        Returns True if queued, False if duplicate.
        """
        # Check search limit
        count = self.get_queue_count(folio)
        if count >= MAX_SEARCHES_PER_FOLIO:
            # Only log warning once per folio to avoid spam
            if folio not in self._limit_warned_folios:
                logger.warning(f"Search limit reached for {folio}: {count}/{MAX_SEARCHES_PER_FOLIO}")
                self._limit_warned_folios.add(folio)
            return False

        try:
            self.conn.execute(
                """
                INSERT INTO ori_search_queue (
                    folio, search_type, search_term, search_operator, priority,
                    date_from, date_to, triggered_by_instrument, triggered_by_search_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (folio, search_type, search_term, search_operator) DO NOTHING
                """,
                [
                    folio,
                    search_type,
                    search_term,
                    search_operator,
                    priority,
                    date_from,
                    date_to,
                    triggered_by_instrument,
                    triggered_by_search_id,
                ],
            )
            return True
        except Exception as e:
            logger.debug(f"Could not queue search: {e}")
            return False

    def _save_legal_variation(
        self,
        folio: str,
        variation_text: str,
        source_type: str,
        source_instrument: Optional[str],
        priority: int,
    ) -> None:
        """Save a legal description variation to the database."""
        try:
            self.conn.execute(
                """
                INSERT INTO legal_variations (
                    folio, variation_text, source_type, source_instrument, priority
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (folio, variation_text) DO NOTHING
                """,
                [folio, variation_text, source_type, source_instrument, priority],
            )
        except Exception as e:
            logger.debug(f"Could not save legal variation: {e}")

    def get_next_pending(self, folio: str) -> Optional[SearchItem]:
        """
        Get the next pending search for a folio.

        Searches are ordered by:
        1. Priority (lower first)
        2. Queue time (older first)
        """
        result = self.conn.execute(
            """
            SELECT id, folio, search_type, search_term, search_operator, priority,
                   status, attempt_count, date_from, date_to,
                   triggered_by_instrument, triggered_by_search_id
            FROM ori_search_queue
            WHERE folio = ?
              AND status = 'pending'
              AND attempt_count < max_attempts
            ORDER BY priority ASC, queued_at ASC
            LIMIT 1
            """,
            [folio],
        ).fetchone()

        if not result:
            return None

        return SearchItem(
            id=result[0],
            folio=result[1],
            search_type=result[2],
            search_term=result[3],
            search_operator=result[4],
            priority=result[5],
            status=result[6],
            attempt_count=result[7],
            date_from=result[8],
            date_to=result[9],
            triggered_by_instrument=result[10],
            triggered_by_search_id=result[11],
        )

    def get_next_ready(self, folio: str) -> Optional[SearchItem]:
        """
        Get the next search ready to execute (pending or rate_limited with expired backoff).
        """
        # First try pending
        pending = self.get_next_pending(folio)
        if pending:
            return pending

        # Check for rate-limited searches that can retry
        result = self.conn.execute(
            """
            SELECT id, folio, search_type, search_term, search_operator, priority,
                   status, attempt_count, date_from, date_to,
                   triggered_by_instrument, triggered_by_search_id
            FROM ori_search_queue
            WHERE folio = ?
              AND status = 'rate_limited'
              AND next_retry_at <= ?
              AND attempt_count < max_attempts
            ORDER BY priority ASC, queued_at ASC
            LIMIT 1
            """,
            [folio, datetime.now()],
        ).fetchone()

        if not result:
            return None

        return SearchItem(
            id=result[0],
            folio=result[1],
            search_type=result[2],
            search_term=result[3],
            search_operator=result[4],
            priority=result[5],
            status=result[6],
            attempt_count=result[7],
            date_from=result[8],
            date_to=result[9],
            triggered_by_instrument=result[10],
            triggered_by_search_id=result[11],
        )

    def mark_in_progress(self, search_id: int) -> None:
        """Mark a search as in progress."""
        self.conn.execute(
            """
            UPDATE ori_search_queue
            SET status = 'in_progress', started_at = ?
            WHERE id = ?
            """,
            [datetime.now(), search_id],
        )

    def mark_completed(
        self,
        search_id: int,
        result_count: int,
        new_documents_found: int,
    ) -> None:
        """Mark a search as completed with results."""
        self.conn.execute(
            """
            UPDATE ori_search_queue
            SET status = 'completed',
                completed_at = ?,
                result_count = ?,
                new_documents_found = ?
            WHERE id = ?
            """,
            [datetime.now(), result_count, new_documents_found, search_id],
        )

    def mark_failed(self, search_id: int, error_message: str) -> None:
        """Mark a search as failed."""
        self.conn.execute(
            """
            UPDATE ori_search_queue
            SET status = 'failed',
                completed_at = ?,
                error_message = ?,
                attempt_count = attempt_count + 1
            WHERE id = ?
            """,
            [datetime.now(), error_message[:500], search_id],
        )

    def mark_rate_limited(self, search_id: int, backoff_seconds: int = RATE_LIMIT_BACKOFF_SECONDS) -> None:
        """Mark a search as rate limited with retry time."""
        next_retry = datetime.now() + timedelta(seconds=backoff_seconds)
        self.conn.execute(
            """
            UPDATE ori_search_queue
            SET status = 'rate_limited',
                next_retry_at = ?,
                attempt_count = attempt_count + 1
            WHERE id = ?
            """,
            [next_retry, search_id],
        )

    def mark_exhausted(self, search_id: int) -> None:
        """Mark a search as exhausted (no more results possible)."""
        self.conn.execute(
            """
            UPDATE ori_search_queue
            SET status = 'exhausted', completed_at = ?
            WHERE id = ?
            """,
            [datetime.now(), search_id],
        )

    def get_queue_count(self, folio: str) -> int:
        """Get total number of searches in queue for a folio."""
        result = self.conn.execute(
            "SELECT COUNT(*) FROM ori_search_queue WHERE folio = ?",
            [folio],
        ).fetchone()
        return result[0] if result else 0

    def get_pending_count(self, folio: str) -> int:
        """Get number of pending searches for a folio."""
        result = self.conn.execute(
            "SELECT COUNT(*) FROM ori_search_queue WHERE folio = ? AND status = 'pending'",
            [folio],
        ).fetchone()
        return result[0] if result else 0

    def get_completed_count(self, folio: str) -> int:
        """Get number of completed searches for a folio."""
        result = self.conn.execute(
            "SELECT COUNT(*) FROM ori_search_queue WHERE folio = ? AND status = 'completed'",
            [folio],
        ).fetchone()
        return result[0] if result else 0

    def has_pending_searches(self, folio: str) -> bool:
        """Check if folio has any pending searches."""
        return self.get_pending_count(folio) > 0

    def all_searches_exhausted(self, folio: str) -> bool:
        """Check if all searches for a folio are completed or exhausted."""
        result = self.conn.execute(
            """
            SELECT COUNT(*)
            FROM ori_search_queue
            WHERE folio = ?
              AND status NOT IN ('completed', 'exhausted', 'failed')
            """,
            [folio],
        ).fetchone()
        return result[0] == 0 if result else True

    def get_stats(self, folio: str) -> dict:
        """Get search statistics for a folio."""
        result = self.conn.execute(
            """
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) as in_progress,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN status = 'rate_limited' THEN 1 ELSE 0 END) as rate_limited,
                SUM(COALESCE(result_count, 0)) as total_results,
                SUM(COALESCE(new_documents_found, 0)) as total_new_docs
            FROM ori_search_queue
            WHERE folio = ?
            """,
            [folio],
        ).fetchone()

        if not result:
            return {
                "total": 0,
                "pending": 0,
                "in_progress": 0,
                "completed": 0,
                "failed": 0,
                "rate_limited": 0,
                "total_results": 0,
                "total_new_docs": 0,
            }

        return {
            "total": result[0] or 0,
            "pending": result[1] or 0,
            "in_progress": result[2] or 0,
            "completed": result[3] or 0,
            "failed": result[4] or 0,
            "rate_limited": result[5] or 0,
            "total_results": result[6] or 0,
            "total_new_docs": result[7] or 0,
        }

    def clear_queue(self, folio: str) -> int:
        """Clear all searches for a folio. Returns count deleted."""
        cursor = self.conn.execute(
            "DELETE FROM ori_search_queue WHERE folio = ?",
            [folio],
        )
        return cursor.rowcount

    def cancel_pending_searches(self, folio: str, search_type: str) -> int:
        """
        Cancel all pending searches of a specific type for a folio.

        Used when a higher-priority search yields results, making lower-priority
        fallbacks redundant (short-circuiting).
        """
        cursor = self.conn.execute(
            """
            DELETE FROM ori_search_queue
            WHERE folio = ?
              AND search_type = ?
              AND status = 'pending'
            """,
            [folio, search_type],
        )

        count = cursor.rowcount
        if count > 0:
            logger.info(f"Short-circuit: Cancelled {count} pending '{search_type}' searches for {folio}")
        return count
