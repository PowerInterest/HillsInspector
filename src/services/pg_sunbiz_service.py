"""
PostgreSQL Sunbiz UCC/FLR Service -- read-only queries against hills_sunbiz.

Provides:
- UCC filing lookup by debtor name (exact and fuzzy via pg_trgm)
- UCC filing lookup by secured party name
- Filing detail retrieval (filing + parties + events)
- Active lien detection for a debtor
- Owner UCC exposure summary for auction pre-screening

This service supplements the live Sunbiz web scraper (src/scrapers/sunbiz_scraper.py).
The live scraper handles entity/officer lookups (LLC/Corp status, officers, registered
agents) which are NOT available in the bulk FLR data. This service handles UCC/FLR
filing lookups which ARE available in bulk and don't require Playwright.

Data source: Florida Secretary of State SFTP bulk FLR files, loaded into PostgreSQL
by sunbiz/pg_loader.py (load_sunbiz_flr command). The FLR dataset consists of:
  - sunbiz_flr_filings  -- Main filing records (doc_number, dates, status)
  - sunbiz_flr_parties   -- Debtor and secured party records per filing
  - sunbiz_flr_events    -- Amendment, continuation, termination events

Filing status codes: A=Active, T=Terminated, L=Lapsed
Filing type codes: U=UCC, F=Federal Lien Registration (FLR)

Integration Pattern (for orchestrator/enrichment):
-----------------------------------------------------
In _enrich_property() or the Sunbiz pipeline step, BEFORE or alongside the live
Sunbiz web scrape:

    from src.services.pg_sunbiz_service import PgSunbizService

    pg_sunbiz = PgSunbizService()
    if pg_sunbiz.available:
        ucc_summary = pg_sunbiz.get_ucc_summary_for_auction(
            owner_name=parcel_owner_name,
            defendant_name=auction_defendant_name,
        )
        if ucc_summary["has_liens"]:
            # Store in enrichment data / auctions table
            # e.g. db.update_auction_ucc_data(auction_id, ucc_summary)
            logger.info(
                f"UCC exposure for {parcel_owner_name}: "
                f"{ucc_summary['active_count']} active liens"
            )

The live Sunbiz scrape still runs for entity/officer lookups (PG doesn't have that
data). The two complement each other:
  - PG bulk data: Fast, complete UCC/FLR filing coverage for all of Florida
  - Live scraper: Entity status, officers, registered agent for specific entities
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from loguru import logger
from sqlalchemy import text

from sunbiz.db import get_engine, resolve_pg_dsn


# UCC filings lapse after 5 years from filing date unless a continuation is filed.
UCC_LAPSE_YEARS = 5

# Filing status codes
STATUS_ACTIVE = "A"
STATUS_TERMINATED = "T"
STATUS_LAPSED = "L"

# Filing type codes
TYPE_UCC = "U"
TYPE_FLR = "F"


class PgSunbizService:
    """Read-only service for Sunbiz UCC/FLR filing queries from PostgreSQL.

    Follows the same graceful-degradation pattern as PgSalesService:
    if PostgreSQL is unreachable at init time, all methods return empty
    results rather than raising.
    """

    def __init__(self, dsn: str | None = None):
        self._available = False
        self._engine = None
        try:
            resolved = resolve_pg_dsn(dsn)
            self._engine = get_engine(resolved)
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self._available = True
            self._ensure_indexes()
            logger.info("PostgreSQL Sunbiz UCC service connected")
        except Exception as e:
            logger.warning(f"PostgreSQL Sunbiz UCC service unavailable: {e}")
            self._engine = None

    @property
    def available(self) -> bool:
        return self._available

    def _ensure_indexes(self) -> None:
        """Create trigram GIN index on party name if it doesn't exist.

        This enables fast fuzzy search via similarity() / % operator.
        Idempotent -- safe to call on every init.
        """
        if not self._engine:
            return
        try:
            with self._engine.connect() as conn:
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_sunbiz_flr_parties_name_trgm
                    ON sunbiz_flr_parties
                    USING gin (name gin_trgm_ops)
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_sunbiz_flr_filings_expiration
                    ON sunbiz_flr_filings (expiration_date)
                    WHERE filing_status = 'A'
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_sunbiz_entity_filings_name_trgm
                    ON sunbiz_entity_filings
                    USING gin (entity_name gin_trgm_ops)
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_sunbiz_entity_parties_name_trgm
                    ON sunbiz_entity_parties
                    USING gin (party_name gin_trgm_ops)
                """))
                conn.commit()
        except Exception as e:
            logger.debug(f"Index creation skipped (non-fatal): {e}")

    # ------------------------------------------------------------------
    # Search by debtor name
    # ------------------------------------------------------------------

    def search_filings_by_debtor(
        self,
        name: str,
        fuzzy: bool = True,
        threshold: float = 0.3,
        limit: int = 50,
    ) -> list[dict]:
        """Search UCC/FLR filings where the debtor name matches.

        Args:
            name: Debtor name to search for.
            fuzzy: If True, use pg_trgm trigram similarity matching.
                   If False, use case-insensitive substring (ILIKE).
            threshold: Minimum trigram similarity score (0.0-1.0) for fuzzy mode.
            limit: Maximum results to return.

        Returns:
            List of dicts with: filing_number, filing_date, filing_status,
            filing_type, expiration_date, secured_parties (list), debtor_name,
            similarity_score (fuzzy only).
        """
        if not self._available or not name:
            return []
        try:
            with self._engine.connect() as conn:
                if fuzzy:
                    # Set the similarity threshold for the % operator
                    # Cast to real explicitly -- pg_trgm's set_limit() expects real, not double
                    conn.execute(
                        text("SELECT set_limit(:threshold ::real)"),
                        {"threshold": threshold},
                    )
                    rows = conn.execute(
                        text("""
                            SELECT DISTINCT ON (f.doc_number)
                                f.doc_number,
                                f.filing_date,
                                f.filing_status,
                                f.filing_type,
                                f.expiration_date,
                                dp.name AS debtor_name,
                                similarity(dp.name, :name) AS sim_score
                            FROM sunbiz_flr_parties dp
                            JOIN sunbiz_flr_filings f ON f.doc_number = dp.doc_number
                            WHERE dp.party_role = 'debtor'
                              AND dp.name % :name
                            ORDER BY f.doc_number, similarity(dp.name, :name) DESC
                            LIMIT :limit
                        """),
                        {"name": name.upper(), "limit": limit},
                    ).fetchall()
                else:
                    rows = conn.execute(
                        text("""
                            SELECT DISTINCT ON (f.doc_number)
                                f.doc_number,
                                f.filing_date,
                                f.filing_status,
                                f.filing_type,
                                f.expiration_date,
                                dp.name AS debtor_name,
                                1.0 AS sim_score
                            FROM sunbiz_flr_parties dp
                            JOIN sunbiz_flr_filings f ON f.doc_number = dp.doc_number
                            WHERE dp.party_role = 'debtor'
                              AND dp.name ILIKE :pattern
                            ORDER BY f.doc_number, dp.name
                            LIMIT :limit
                        """),
                        {"pattern": f"%{name}%", "limit": limit},
                    ).fetchall()

                results = []
                for row in rows:
                    filing = {
                        "filing_number": row[0],
                        "filing_date": row[1],
                        "filing_status": _status_label(row[2]),
                        "filing_status_code": row[2],
                        "filing_type": _type_label(row[3]),
                        "filing_type_code": row[3],
                        "expiration_date": row[4],
                        "debtor_name": row[5],
                        "similarity_score": float(row[6]) if row[6] else 0.0,
                        "secured_parties": [],
                    }
                    results.append(filing)

                # Batch-fetch secured parties for all matching filings
                if results:
                    doc_numbers = [r["filing_number"] for r in results]
                    secured = self._get_secured_parties_batch(conn, doc_numbers)
                    for r in results:
                        r["secured_parties"] = secured.get(r["filing_number"], [])

                # Sort by similarity score descending
                results.sort(key=lambda r: r["similarity_score"], reverse=True)
                return results
        except Exception as e:
            logger.warning(f"search_filings_by_debtor({name!r}) failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Search by secured party name
    # ------------------------------------------------------------------

    def search_filings_by_secured_party(
        self,
        name: str,
        limit: int = 50,
    ) -> list[dict]:
        """Search UCC/FLR filings where the secured party (lender) matches.

        Args:
            name: Secured party name to search for (case-insensitive substring).
            limit: Maximum results to return.

        Returns:
            List of dicts with: filing_number, filing_date, filing_status,
            filing_type, expiration_date, secured_party_name, debtors (list).
        """
        if not self._available or not name:
            return []
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT DISTINCT ON (f.doc_number)
                            f.doc_number,
                            f.filing_date,
                            f.filing_status,
                            f.filing_type,
                            f.expiration_date,
                            sp.name AS secured_party_name
                        FROM sunbiz_flr_parties sp
                        JOIN sunbiz_flr_filings f ON f.doc_number = sp.doc_number
                        WHERE sp.party_role = 'secured'
                          AND sp.name ILIKE :pattern
                        ORDER BY f.doc_number, sp.name
                        LIMIT :limit
                    """),
                    {"pattern": f"%{name}%", "limit": limit},
                ).fetchall()

                results = []
                for row in rows:
                    filing = {
                        "filing_number": row[0],
                        "filing_date": row[1],
                        "filing_status": _status_label(row[2]),
                        "filing_status_code": row[2],
                        "filing_type": _type_label(row[3]),
                        "filing_type_code": row[3],
                        "expiration_date": row[4],
                        "secured_party_name": row[5],
                        "debtors": [],
                    }
                    results.append(filing)

                # Batch-fetch debtors
                if results:
                    doc_numbers = [r["filing_number"] for r in results]
                    debtors = self._get_debtors_batch(conn, doc_numbers)
                    for r in results:
                        r["debtors"] = debtors.get(r["filing_number"], [])

                return results
        except Exception as e:
            logger.warning(f"search_filings_by_secured_party({name!r}) failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Filing detail
    # ------------------------------------------------------------------

    def get_filing_details(self, filing_number: str) -> dict | None:
        """Get full details for a specific UCC/FLR filing.

        Args:
            filing_number: The doc_number (filing number) to look up.

        Returns:
            Dict with full filing info including all parties and events,
            or None if not found.
        """
        if not self._available or not filing_number:
            return None
        try:
            with self._engine.connect() as conn:
                # Filing record
                frow = conn.execute(
                    text("""
                        SELECT doc_number, filing_date, pages, total_pages,
                               filing_status, filing_type,
                               assessment_date, cancellation_date, expiration_date,
                               trans_utility,
                               filing_event_count,
                               total_debtor_count, total_secured_count,
                               current_debtor_count, current_secured_count
                        FROM sunbiz_flr_filings
                        WHERE doc_number = :doc
                    """),
                    {"doc": filing_number},
                ).fetchone()
                if not frow:
                    return None

                filing = {
                    "filing_number": frow[0],
                    "filing_date": frow[1],
                    "pages": frow[2],
                    "total_pages": frow[3],
                    "filing_status": _status_label(frow[4]),
                    "filing_status_code": frow[4],
                    "filing_type": _type_label(frow[5]),
                    "filing_type_code": frow[5],
                    "assessment_date": frow[6],
                    "cancellation_date": frow[7],
                    "expiration_date": frow[8],
                    "trans_utility": frow[9],
                    "filing_event_count": frow[10],
                    "total_debtor_count": frow[11],
                    "total_secured_count": frow[12],
                    "current_debtor_count": frow[13],
                    "current_secured_count": frow[14],
                    "debtors": [],
                    "secured_parties": [],
                    "events": [],
                }

                # Parties
                parties = conn.execute(
                    text("""
                        SELECT party_role, name, name_format,
                               address1, address2, city, state, zip_code, country,
                               sequence_number, relation_to_filing,
                               original_party, filing_status
                        FROM sunbiz_flr_parties
                        WHERE doc_number = :doc
                        ORDER BY party_role, sequence_number
                    """),
                    {"doc": filing_number},
                ).fetchall()

                for p in parties:
                    party = {
                        "role": p[0],
                        "name": p[1],
                        "name_format": "Corporate" if p[2] == "C" else "Personal" if p[2] == "P" else p[2],
                        "address": _format_address(p[3], p[4], p[5], p[6], p[7], p[8]),
                        "sequence_number": p[9],
                        "relation_to_filing": p[10],
                        "is_original_party": p[11] == "Y" if p[11] else None,
                        "status": _status_label(p[12]),
                    }
                    if p[0] == "debtor":
                        filing["debtors"].append(party)
                    else:
                        filing["secured_parties"].append(party)

                # Events
                events = conn.execute(
                    text("""
                        SELECT event_doc_number, event_orig_doc_number,
                               event_sequence_number, event_date,
                               action_sequence_number, action_code, action_verbage,
                               action_name
                        FROM sunbiz_flr_events
                        WHERE event_doc_number = :doc
                           OR event_orig_doc_number = :doc
                        ORDER BY event_date, event_sequence_number, action_sequence_number
                    """),
                    {"doc": filing_number},
                ).fetchall()

                for e in events:
                    filing["events"].append({
                        "event_doc_number": e[0],
                        "orig_doc_number": e[1],
                        "event_sequence": e[2],
                        "event_date": e[3],
                        "action_sequence": e[4],
                        "action_code": e[5],
                        "action_description": e[6],
                        "action_name": e[7],
                    })

                return filing
        except Exception as e:
            logger.warning(f"get_filing_details({filing_number!r}) failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Active liens for a debtor
    # ------------------------------------------------------------------

    def get_active_liens_for_debtor(
        self,
        name: str,
        fuzzy: bool = True,
        threshold: float = 0.3,
    ) -> list[dict]:
        """Get only active (not terminated/lapsed) UCC/FLR liens for a debtor.

        UCC filings lapse after 5 years unless continued. This method filters
        by filing_status = 'A' AND expiration_date >= today (if set).

        Args:
            name: Debtor name to search.
            fuzzy: Use trigram similarity matching.
            threshold: Minimum similarity score for fuzzy mode.

        Returns:
            List of active filing dicts (same shape as search_filings_by_debtor).
        """
        if not self._available or not name:
            return []
        try:
            with self._engine.connect() as conn:
                if fuzzy:
                    conn.execute(
                        text("SELECT set_limit(:threshold ::real)"),
                        {"threshold": threshold},
                    )
                    rows = conn.execute(
                        text("""
                            SELECT DISTINCT ON (f.doc_number)
                                f.doc_number,
                                f.filing_date,
                                f.filing_status,
                                f.filing_type,
                                f.expiration_date,
                                dp.name AS debtor_name,
                                similarity(dp.name, :name) AS sim_score
                            FROM sunbiz_flr_parties dp
                            JOIN sunbiz_flr_filings f ON f.doc_number = dp.doc_number
                            WHERE dp.party_role = 'debtor'
                              AND dp.name % :name
                              AND f.filing_status = 'A'
                              AND (f.expiration_date IS NULL OR f.expiration_date >= CURRENT_DATE)
                            ORDER BY f.doc_number, similarity(dp.name, :name) DESC
                        """),
                        {"name": name.upper()},
                    ).fetchall()
                else:
                    rows = conn.execute(
                        text("""
                            SELECT DISTINCT ON (f.doc_number)
                                f.doc_number,
                                f.filing_date,
                                f.filing_status,
                                f.filing_type,
                                f.expiration_date,
                                dp.name AS debtor_name,
                                1.0 AS sim_score
                            FROM sunbiz_flr_parties dp
                            JOIN sunbiz_flr_filings f ON f.doc_number = dp.doc_number
                            WHERE dp.party_role = 'debtor'
                              AND dp.name ILIKE :pattern
                              AND f.filing_status = 'A'
                              AND (f.expiration_date IS NULL OR f.expiration_date >= CURRENT_DATE)
                            ORDER BY f.doc_number, dp.name
                        """),
                        {"pattern": f"%{name}%"},
                    ).fetchall()

                results = []
                for row in rows:
                    filing = {
                        "filing_number": row[0],
                        "filing_date": row[1],
                        "filing_status": _status_label(row[2]),
                        "filing_status_code": row[2],
                        "filing_type": _type_label(row[3]),
                        "filing_type_code": row[3],
                        "expiration_date": row[4],
                        "debtor_name": row[5],
                        "similarity_score": float(row[6]) if row[6] else 0.0,
                        "secured_parties": [],
                    }
                    results.append(filing)

                if results:
                    doc_numbers = [r["filing_number"] for r in results]
                    secured = self._get_secured_parties_batch(conn, doc_numbers)
                    for r in results:
                        r["secured_parties"] = secured.get(r["filing_number"], [])

                results.sort(key=lambda r: r["similarity_score"], reverse=True)
                return results
        except Exception as e:
            logger.warning(f"get_active_liens_for_debtor({name!r}) failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Quick owner UCC exposure check
    # ------------------------------------------------------------------

    def check_owner_ucc_exposure(self, owner_name: str) -> dict:
        """Quick check: does this property owner have UCC/FLR liens?

        Searches by debtor name (fuzzy) and returns a summary. This is a
        lightweight pre-screen -- call get_active_liens_for_debtor() for
        full details.

        Args:
            owner_name: Property owner name to check.

        Returns:
            Dict with: has_liens, active_count, total_count,
            latest_filing_date, secured_parties (unique list).
        """
        empty_result: dict[str, Any] = {
            "has_liens": False,
            "active_count": 0,
            "total_count": 0,
            "latest_filing_date": None,
            "secured_parties": [],
        }
        if not self._available or not owner_name:
            return empty_result
        try:
            # Get all filings (active + inactive) for the debtor
            all_filings = self.search_filings_by_debtor(
                owner_name, fuzzy=True, threshold=0.35, limit=100
            )
            if not all_filings:
                return empty_result

            active = [
                f for f in all_filings
                if f["filing_status_code"] == STATUS_ACTIVE
                and (
                    f["expiration_date"] is None
                    or (
                        isinstance(f["expiration_date"], dt.date)
                        and f["expiration_date"] >= dt.datetime.now(dt.UTC).date()
                    )
                )
            ]

            # Collect unique secured party names across all active filings
            secured_set: set[str] = set()
            for f in active:
                for sp in f.get("secured_parties", []):
                    if sp:
                        secured_set.add(sp)

            latest_date = None
            for f in all_filings:
                fd = f.get("filing_date")
                if fd and (latest_date is None or fd > latest_date):
                    latest_date = fd

            return {
                "has_liens": len(active) > 0,
                "active_count": len(active),
                "total_count": len(all_filings),
                "latest_filing_date": latest_date,
                "secured_parties": sorted(secured_set),
            }
        except Exception as e:
            logger.warning(f"check_owner_ucc_exposure({owner_name!r}) failed: {e}")
            return empty_result

    # ------------------------------------------------------------------
    # Auction pre-screen summary
    # ------------------------------------------------------------------

    def get_ucc_summary_for_auction(
        self,
        owner_name: str,
        defendant_name: str | None = None,
    ) -> dict:
        """Pre-screen for auction analysis: check both owner and defendant names.

        Merges results from both names (if different) to catch cases where the
        foreclosure defendant differs from the current property owner.

        Args:
            owner_name: Property owner name (from HCPA / parcels).
            defendant_name: Foreclosure defendant name (from auction listing).
                           If None or same as owner_name, only owner is checked.

        Returns:
            Dict suitable for display in the web dashboard:
            {
                "has_liens": bool,
                "active_count": int,
                "total_count": int,
                "latest_filing_date": date | None,
                "secured_parties": list[str],
                "active_filings": list[dict],  # Summarized active filings
                "names_checked": list[str],
            }
        """
        empty: dict[str, Any] = {
            "has_liens": False,
            "active_count": 0,
            "total_count": 0,
            "latest_filing_date": None,
            "secured_parties": [],
            "active_filings": [],
            "names_checked": [],
        }
        if not self._available:
            return empty

        names_to_check = [owner_name] if owner_name else []
        if defendant_name and defendant_name.upper() != (owner_name or "").upper():
            names_to_check.append(defendant_name)

        if not names_to_check:
            return empty

        all_active: dict[str, dict] = {}  # keyed by filing_number to deduplicate
        total_count = 0
        latest_date: dt.date | None = None
        secured_set: set[str] = set()

        for name in names_to_check:
            # Get all filings for exposure count
            all_filings = self.search_filings_by_debtor(
                name, fuzzy=True, threshold=0.35, limit=100
            )
            total_count += len(all_filings)

            # Get active liens
            active_filings = self.get_active_liens_for_debtor(
                name, fuzzy=True, threshold=0.35
            )
            for f in active_filings:
                fnum = f["filing_number"]
                if fnum not in all_active:
                    all_active[fnum] = f
                for sp in f.get("secured_parties", []):
                    if sp:
                        secured_set.add(sp)

            for f in all_filings:
                fd = f.get("filing_date")
                if fd and (latest_date is None or fd > latest_date):
                    latest_date = fd

        # Build summarized active filings for dashboard display
        active_summaries = []
        for f in all_active.values():
            active_summaries.append({
                "filing_number": f["filing_number"],
                "filing_date": (
                    f["filing_date"].isoformat()
                    if isinstance(f["filing_date"], dt.date)
                    else str(f["filing_date"]) if f["filing_date"] else None
                ),
                "expiration_date": (
                    f["expiration_date"].isoformat()
                    if isinstance(f["expiration_date"], dt.date)
                    else str(f["expiration_date"]) if f["expiration_date"] else None
                ),
                "filing_type": f["filing_type"],
                "debtor_name": f.get("debtor_name", ""),
                "secured_parties": f.get("secured_parties", []),
            })

        return {
            "has_liens": len(all_active) > 0,
            "active_count": len(all_active),
            "total_count": total_count,
            "latest_filing_date": latest_date,
            "secured_parties": sorted(secured_set),
            "active_filings": active_summaries,
            "names_checked": names_to_check,
        }

    # ------------------------------------------------------------------
    # Entity datasets (COR/GEN)
    # ------------------------------------------------------------------

    def search_entities_by_name(
        self,
        name: str,
        dataset_types: list[str] | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """Search entity filings (LLC/corporation/partnership) by name."""
        if not self._available or not name:
            return []
        types = dataset_types or ["cor", "gen"]
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT
                            dataset_type,
                            doc_number,
                            entity_name,
                            status,
                            filing_type,
                            filed_date,
                            cancellation_date,
                            expiration_date
                        FROM sunbiz_entity_filings
                        WHERE dataset_type = ANY(:types)
                          AND entity_name ILIKE :pattern
                        ORDER BY entity_name
                        LIMIT :limit
                    """),
                    {"types": types, "pattern": f"%{name}%", "limit": limit},
                ).fetchall()
                return [
                    {
                        "dataset_type": row[0],
                        "doc_number": row[1],
                        "entity_name": row[2],
                        "status": row[3],
                        "filing_type": row[4],
                        "filed_date": row[5],
                        "cancellation_date": row[6],
                        "expiration_date": row[7],
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.warning(f"search_entities_by_name({name!r}) failed: {e}")
            return []

    def get_entity_profile(self, dataset_type: str, doc_number: str) -> dict | None:
        """Get filing + related parties + events for one entity."""
        if not self._available or not dataset_type or not doc_number:
            return None
        try:
            with self._engine.connect() as conn:
                filing = conn.execute(
                    text("""
                        SELECT
                            dataset_type, doc_number, entity_name, status, filing_type,
                            filed_date, effective_date, cancellation_date, expiration_date,
                            fei_number, state_country,
                            principal_address1, principal_address2, principal_city,
                            principal_state, principal_zip, principal_country,
                            mailing_address1, mailing_address2, mailing_city,
                            mailing_state, mailing_zip, mailing_country
                        FROM sunbiz_entity_filings
                        WHERE dataset_type = :dataset_type
                          AND doc_number = :doc_number
                    """),
                    {"dataset_type": dataset_type, "doc_number": doc_number},
                ).fetchone()
                if not filing:
                    return None

                parties = conn.execute(
                    text("""
                        SELECT
                            party_role, party_title, party_name, party_name_format,
                            party_corp_number, party_sequence,
                            address1, address2, city, state, zip_code, country
                        FROM sunbiz_entity_parties
                        WHERE dataset_type = :dataset_type
                          AND doc_number = :doc_number
                        ORDER BY party_sequence NULLS LAST, party_name
                    """),
                    {"dataset_type": dataset_type, "doc_number": doc_number},
                ).fetchall()

                events = conn.execute(
                    text("""
                        SELECT
                            event_doc_number, event_orig_doc_number,
                            event_sequence_number, event_code, event_description,
                            event_effective_date, event_filing_date,
                            event_cancellation_date, event_expiration_date,
                            event_name
                        FROM sunbiz_entity_events
                        WHERE dataset_type = :dataset_type
                          AND (event_doc_number = :doc_number OR event_orig_doc_number = :doc_number)
                        ORDER BY event_filing_date, event_sequence_number
                    """),
                    {"dataset_type": dataset_type, "doc_number": doc_number},
                ).fetchall()

                return {
                    "dataset_type": filing[0],
                    "doc_number": filing[1],
                    "entity_name": filing[2],
                    "status": filing[3],
                    "filing_type": filing[4],
                    "filed_date": filing[5],
                    "effective_date": filing[6],
                    "cancellation_date": filing[7],
                    "expiration_date": filing[8],
                    "fei_number": filing[9],
                    "state_country": filing[10],
                    "principal_address": _format_address(
                        filing[11], filing[12], filing[13], filing[14], filing[15], filing[16]
                    ),
                    "mailing_address": _format_address(
                        filing[17], filing[18], filing[19], filing[20], filing[21], filing[22]
                    ),
                    "parties": [
                        {
                            "role": row[0],
                            "title": row[1],
                            "name": row[2],
                            "name_format": row[3],
                            "corp_number": row[4],
                            "sequence": row[5],
                            "address": _format_address(
                                row[6], row[7], row[8], row[9], row[10], row[11]
                            ),
                        }
                        for row in parties
                    ],
                    "events": [
                        {
                            "event_doc_number": row[0],
                            "event_orig_doc_number": row[1],
                            "event_sequence_number": row[2],
                            "event_code": row[3],
                            "event_description": row[4],
                            "event_effective_date": row[5],
                            "event_filing_date": row[6],
                            "event_cancellation_date": row[7],
                            "event_expiration_date": row[8],
                            "event_name": row[9],
                        }
                        for row in events
                    ],
                }
        except Exception as e:
            logger.warning(
                f"get_entity_profile({dataset_type!r}, {doc_number!r}) failed: {e}"
            )
            return None

    def get_entity_table_stats(self) -> dict:
        """Get row counts for structured entity tables."""
        if not self._available:
            return {"filings": 0, "parties": 0, "events": 0}
        try:
            with self._engine.connect() as conn:
                filings = conn.execute(
                    text("SELECT COUNT(*) FROM sunbiz_entity_filings")
                ).scalar()
                parties = conn.execute(
                    text("SELECT COUNT(*) FROM sunbiz_entity_parties")
                ).scalar()
                events = conn.execute(
                    text("SELECT COUNT(*) FROM sunbiz_entity_events")
                ).scalar()
                return {
                    "filings": filings or 0,
                    "parties": parties or 0,
                    "events": events or 0,
                }
        except Exception as e:
            logger.debug(f"get_entity_table_stats() failed: {e}")
            return {"filings": 0, "parties": 0, "events": 0}

    # ------------------------------------------------------------------
    # Stats / diagnostics
    # ------------------------------------------------------------------

    def get_table_stats(self) -> dict:
        """Get row counts for the FLR tables -- useful for diagnostics."""
        if not self._available:
            return {"filings": 0, "parties": 0, "events": 0}
        try:
            with self._engine.connect() as conn:
                filings = conn.execute(
                    text("SELECT COUNT(*) FROM sunbiz_flr_filings")
                ).scalar()
                parties = conn.execute(
                    text("SELECT COUNT(*) FROM sunbiz_flr_parties")
                ).scalar()
                events = conn.execute(
                    text("SELECT COUNT(*) FROM sunbiz_flr_events")
                ).scalar()
                return {
                    "filings": filings or 0,
                    "parties": parties or 0,
                    "events": events or 0,
                }
        except Exception as e:
            logger.debug(f"get_table_stats() failed: {e}")
            return {"filings": 0, "parties": 0, "events": 0}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_secured_parties_batch(
        self, conn: Any, doc_numbers: list[str]
    ) -> dict[str, list[str]]:
        """Fetch secured party names for a batch of filing numbers.

        Returns: {doc_number: [party_name, ...]}
        """
        if not doc_numbers:
            return {}
        # Use ANY(array) for batch lookup
        rows = conn.execute(
            text("""
                SELECT doc_number, name
                FROM sunbiz_flr_parties
                WHERE doc_number = ANY(:docs)
                  AND party_role = 'secured'
                  AND name IS NOT NULL
                ORDER BY doc_number, sequence_number
            """),
            {"docs": doc_numbers},
        ).fetchall()
        result: dict[str, list[str]] = {}
        for row in rows:
            result.setdefault(row[0], []).append(row[1])
        return result

    def _get_debtors_batch(
        self, conn: Any, doc_numbers: list[str]
    ) -> dict[str, list[str]]:
        """Fetch debtor names for a batch of filing numbers.

        Returns: {doc_number: [debtor_name, ...]}
        """
        if not doc_numbers:
            return {}
        rows = conn.execute(
            text("""
                SELECT doc_number, name
                FROM sunbiz_flr_parties
                WHERE doc_number = ANY(:docs)
                  AND party_role = 'debtor'
                  AND name IS NOT NULL
                ORDER BY doc_number, sequence_number
            """),
            {"docs": doc_numbers},
        ).fetchall()
        result: dict[str, list[str]] = {}
        for row in rows:
            result.setdefault(row[0], []).append(row[1])
        return result


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

def _status_label(code: str | None) -> str:
    """Convert filing status code to human-readable label."""
    if code == STATUS_ACTIVE:
        return "Active"
    if code == STATUS_TERMINATED:
        return "Terminated"
    if code == STATUS_LAPSED:
        return "Lapsed"
    return code or "Unknown"


def _type_label(code: str | None) -> str:
    """Convert filing type code to human-readable label."""
    if code == TYPE_UCC:
        return "UCC"
    if code == TYPE_FLR:
        return "Federal Lien (FLR)"
    return code or "Unknown"


def _format_address(
    addr1: str | None,
    addr2: str | None,
    city: str | None,
    state: str | None,
    zip_code: str | None,
    country: str | None,
) -> str | None:
    """Format address fields into a single string."""
    parts = []
    if addr1:
        parts.append(addr1.strip())
    if addr2:
        parts.append(addr2.strip())
    city_state_zip = []
    if city:
        city_state_zip.append(city.strip())
    if state:
        city_state_zip.append(state.strip())
    if zip_code:
        city_state_zip.append(zip_code.strip())
    if city_state_zip:
        parts.append(", ".join(city_state_zip))
    if country and country.strip() and country.strip().upper() not in ("US", "USA", ""):
        parts.append(country.strip())
    return ", ".join(parts) if parts else None
