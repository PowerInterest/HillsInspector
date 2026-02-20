"""
PostgreSQL Clerk Service -- read-only queries against clerk civil bulk data.

Provides:
- Case metadata lookup by case number
- Docket/event history for a case
- Party lookups (plaintiff, defendant, attorney)
- Foreclosure case filtering by date range
- Fuzzy party name search via pg_trgm trigram matching
- Related case discovery (CC<->CA chain via shared parties)
- Disposed/closed case status checks

Tables queried:
- clerk_civil_cases
- clerk_civil_events
- clerk_civil_parties
- clerk_disposed_cases
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger
from sqlalchemy import text

if TYPE_CHECKING:
    import datetime as dt

from sunbiz.db import get_engine, resolve_pg_dsn


class PgClerkService:
    """Read-only service for PostgreSQL clerk civil data queries."""

    def __init__(self, dsn: str | None = None):
        self._available = False
        try:
            resolved = resolve_pg_dsn(dsn)
            self._engine = get_engine(resolved)
            # Quick connectivity + table existence check
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1 FROM clerk_civil_cases LIMIT 0"))
            self._available = True
            logger.info("PostgreSQL clerk service connected")
        except Exception as e:
            logger.warning(f"PostgreSQL clerk service unavailable: {e}")
            self._engine = None

    @property
    def available(self) -> bool:
        return self._available

    # ------------------------------------------------------------------
    # Case lookup
    # ------------------------------------------------------------------

    def get_case(self, case_number: str) -> dict | None:
        """
        Get case metadata by case number.

        Returns dict with: case_number, style, case_type, division, filing_date,
        judge, cause_of_action, cause_description, case_status, judgment_code,
        judgment_description, judgment_date, is_foreclosure.
        """
        if not self._available or not case_number:
            return None
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text("""
                        SELECT case_number, style, case_type, division,
                               filing_date, judge, cause_of_action,
                               cause_description, case_status,
                               judgment_code, judgment_description,
                               judgment_date, is_foreclosure
                        FROM clerk_civil_cases
                        WHERE case_number = :case_number
                    """),
                    {"case_number": case_number},
                ).fetchone()
                if not row:
                    return None
                return {
                    "case_number": row[0],
                    "style": row[1],
                    "case_type": row[2],
                    "division": row[3],
                    "filing_date": row[4],
                    "judge": row[5],
                    "cause_of_action": row[6],
                    "cause_description": row[7],
                    "case_status": row[8],
                    "judgment_code": row[9],
                    "judgment_description": row[10],
                    "judgment_date": row[11],
                    "is_foreclosure": row[12],
                }
        except Exception as e:
            logger.warning(f"get_case({case_number}) failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Events / docket
    # ------------------------------------------------------------------

    def get_case_events(self, case_number: str) -> list[dict]:
        """
        Get all docket events for a case, ordered by date descending.

        Returns list of dicts with: event_code, event_description, event_date,
        party_first_name, party_middle_name, party_last_name.
        """
        if not self._available or not case_number:
            return []
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT event_code, event_description, event_date,
                               party_first_name, party_middle_name, party_last_name
                        FROM clerk_civil_events
                        WHERE case_number = :case_number
                        ORDER BY event_date DESC, id DESC
                    """),
                    {"case_number": case_number},
                ).fetchall()
                return [
                    {
                        "event_code": row[0],
                        "event_description": row[1],
                        "event_date": row[2],
                        "party_first_name": row[3],
                        "party_middle_name": row[4],
                        "party_last_name": row[5],
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.warning(f"get_case_events({case_number}) failed: {e}")
            return []

    def has_final_judgment_event(self, case_number: str) -> bool:
        """Check if a case has a Final Judgment docket event (event_code = 'FJ')."""
        if not self._available or not case_number:
            return False
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text("""
                        SELECT 1
                        FROM clerk_civil_events
                        WHERE case_number = :case_number
                          AND event_code = 'FJ'
                        LIMIT 1
                    """),
                    {"case_number": case_number},
                ).fetchone()
                return row is not None
        except Exception as e:
            logger.debug(f"has_final_judgment_event({case_number}) failed: {e}")
            return False

    def get_lis_pendens_event(self, case_number: str) -> dict | None:
        """Get the Lis Pendens filing event for a case (event_code = 'LPR')."""
        if not self._available or not case_number:
            return None
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text("""
                        SELECT event_code, event_description, event_date,
                               party_first_name, party_middle_name, party_last_name
                        FROM clerk_civil_events
                        WHERE case_number = :case_number
                          AND event_code = 'LPR'
                        ORDER BY event_date ASC
                        LIMIT 1
                    """),
                    {"case_number": case_number},
                ).fetchone()
                if not row:
                    return None
                return {
                    "event_code": row[0],
                    "event_description": row[1],
                    "event_date": row[2],
                    "party_first_name": row[3],
                    "party_middle_name": row[4],
                    "party_last_name": row[5],
                }
        except Exception as e:
            logger.debug(f"get_lis_pendens_event({case_number}) failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Parties
    # ------------------------------------------------------------------

    def get_case_parties(self, case_number: str) -> list[dict]:
        """
        Get all parties (plaintiff, defendant, attorney) for a case.

        Returns list of dicts with: party_type, name, first_name, middle_name,
        last_name, address1, address2, city, state, zip, bar_number, phone, email.
        """
        if not self._available or not case_number:
            return []
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT party_type, name, first_name, middle_name,
                               last_name, address1, address2, city, state, zip,
                               bar_number, phone, email
                        FROM clerk_civil_parties
                        WHERE case_number = :case_number
                        ORDER BY party_type, name
                    """),
                    {"case_number": case_number},
                ).fetchall()
                return [
                    {
                        "party_type": row[0],
                        "name": row[1],
                        "first_name": row[2],
                        "middle_name": row[3],
                        "last_name": row[4],
                        "address1": row[5],
                        "address2": row[6],
                        "city": row[7],
                        "state": row[8],
                        "zip": row[9],
                        "bar_number": row[10],
                        "phone": row[11],
                        "email": row[12],
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.warning(f"get_case_parties({case_number}) failed: {e}")
            return []

    def get_defendants(self, case_number: str) -> list[dict]:
        """Get just the defendants for a case."""
        if not self._available or not case_number:
            return []
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT name, first_name, middle_name, last_name,
                               address1, city, state, zip
                        FROM clerk_civil_parties
                        WHERE case_number = :case_number
                          AND party_type = 'Defendant'
                        ORDER BY name
                    """),
                    {"case_number": case_number},
                ).fetchall()
                return [
                    {
                        "name": row[0],
                        "first_name": row[1],
                        "middle_name": row[2],
                        "last_name": row[3],
                        "address1": row[4],
                        "city": row[5],
                        "state": row[6],
                        "zip": row[7],
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.debug(f"get_defendants({case_number}) failed: {e}")
            return []

    def get_plaintiff(self, case_number: str) -> dict | None:
        """Get the first plaintiff for a case (typically the foreclosing entity)."""
        if not self._available or not case_number:
            return None
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text("""
                        SELECT name, first_name, middle_name, last_name,
                               address1, city, state, zip
                        FROM clerk_civil_parties
                        WHERE case_number = :case_number
                          AND party_type = 'Plaintiff'
                        ORDER BY id ASC
                        LIMIT 1
                    """),
                    {"case_number": case_number},
                ).fetchone()
                if not row:
                    return None
                return {
                    "name": row[0],
                    "first_name": row[1],
                    "middle_name": row[2],
                    "last_name": row[3],
                    "address1": row[4],
                    "city": row[5],
                    "state": row[6],
                    "zip": row[7],
                }
        except Exception as e:
            logger.debug(f"get_plaintiff({case_number}) failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Case status / disposal
    # ------------------------------------------------------------------

    def is_case_disposed(self, case_number: str) -> bool:
        """
        Check if a case is closed/disposed.

        Checks both clerk_disposed_cases table and case_status in clerk_civil_cases.
        A disposed case can be skipped for ORI ingestion.
        """
        if not self._available or not case_number:
            return False
        try:
            with self._engine.connect() as conn:
                # Check disposed table first
                row = conn.execute(
                    text("""
                        SELECT 1 FROM clerk_disposed_cases
                        WHERE case_number = :case_number
                        LIMIT 1
                    """),
                    {"case_number": case_number},
                ).fetchone()
                if row:
                    return True

                # Fall back to case_status
                row = conn.execute(
                    text("""
                        SELECT case_status FROM clerk_civil_cases
                        WHERE case_number = :case_number
                    """),
                    {"case_number": case_number},
                ).fetchone()
                if row and row[0]:
                    return row[0].lower() in ("closed", "disposed", "dismissed")
                return False
        except Exception as e:
            logger.debug(f"is_case_disposed({case_number}) failed: {e}")
            return False

    # ------------------------------------------------------------------
    # Foreclosure queries
    # ------------------------------------------------------------------

    def get_foreclosure_cases(
        self,
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
    ) -> list[dict]:
        """
        Get all foreclosure cases filed in a date range.

        Args:
            start_date: Inclusive start of filing date range.
            end_date: Inclusive end of filing date range.

        Returns list of case dicts sorted by filing_date.
        """
        if not self._available:
            return []
        try:
            conditions = ["is_foreclosure = true"]
            params: dict = {}

            if start_date:
                conditions.append("filing_date >= :start_date")
                params["start_date"] = start_date
            if end_date:
                conditions.append("filing_date <= :end_date")
                params["end_date"] = end_date

            where = " AND ".join(conditions)
            query = f"""
                SELECT case_number, style, case_type, division,
                       filing_date, judge, case_status,
                       judgment_code, judgment_description, judgment_date
                FROM clerk_civil_cases
                WHERE {where}
                ORDER BY filing_date
            """

            with self._engine.connect() as conn:
                rows = conn.execute(text(query), params).fetchall()
                return [
                    {
                        "case_number": row[0],
                        "style": row[1],
                        "case_type": row[2],
                        "division": row[3],
                        "filing_date": row[4],
                        "judge": row[5],
                        "case_status": row[6],
                        "judgment_code": row[7],
                        "judgment_description": row[8],
                        "judgment_date": row[9],
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.warning(f"get_foreclosure_cases() failed: {e}")
            return []

    def count_foreclosure_cases(
        self,
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
    ) -> int:
        """Count foreclosure cases in a date range."""
        if not self._available:
            return 0
        try:
            conditions = ["is_foreclosure = true"]
            params: dict = {}
            if start_date:
                conditions.append("filing_date >= :start_date")
                params["start_date"] = start_date
            if end_date:
                conditions.append("filing_date <= :end_date")
                params["end_date"] = end_date

            where = " AND ".join(conditions)
            query = f"SELECT COUNT(*) FROM clerk_civil_cases WHERE {where}"

            with self._engine.connect() as conn:
                row = conn.execute(text(query), params).fetchone()
                return row[0] if row else 0
        except Exception as e:
            logger.debug(f"count_foreclosure_cases() failed: {e}")
            return 0

    # ------------------------------------------------------------------
    # Fuzzy name search
    # ------------------------------------------------------------------

    def search_cases_by_party(
        self,
        name: str,
        party_type: str | None = None,
        limit: int = 20,
        threshold: float = 0.3,
    ) -> list[dict]:
        """
        Fuzzy search cases by party name using pg_trgm trigram matching.

        Args:
            name: Party name to search for.
            party_type: Optional filter: 'Plaintiff', 'Defendant', 'Attorney'.
            limit: Max results to return.
            threshold: Similarity threshold (0.0-1.0, lower = more results).

        Returns list of dicts with: case_number, party_type, name, similarity.
        """
        if not self._available or not name:
            return []
        try:
            conditions = ["similarity(p.name, :name) > :threshold"]
            params: dict = {"name": name, "threshold": threshold, "limit": limit}

            if party_type:
                conditions.append("p.party_type = :party_type")
                params["party_type"] = party_type

            where = " AND ".join(conditions)
            query = f"""
                SELECT p.case_number, p.party_type, p.name,
                       similarity(p.name, :name) AS sim,
                       c.case_type, c.filing_date, c.case_status,
                       c.is_foreclosure
                FROM clerk_civil_parties p
                JOIN clerk_civil_cases c ON c.case_number = p.case_number
                WHERE {where}
                ORDER BY sim DESC
                LIMIT :limit
            """

            with self._engine.connect() as conn:
                rows = conn.execute(text(query), params).fetchall()
                return [
                    {
                        "case_number": row[0],
                        "party_type": row[1],
                        "name": row[2],
                        "similarity": float(row[3]) if row[3] else 0.0,
                        "case_type": row[4],
                        "filing_date": row[5],
                        "case_status": row[6],
                        "is_foreclosure": row[7],
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.warning(f"search_cases_by_party({name!r}) failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Related cases (CC <-> CA chain)
    # ------------------------------------------------------------------

    def get_related_cases(self, case_number: str) -> list[dict]:
        """
        Find other cases involving the same defendant parties.

        This is critical for the CC->CA chain: a County Court (CC) case against
        a homeowner often has a related Circuit Court (CA) mortgage foreclosure.
        We find related cases by matching defendant names.

        Returns list of dicts with: case_number, style, case_type, filing_date,
        case_status, is_foreclosure, shared_party.
        """
        if not self._available or not case_number:
            return []
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        WITH source_defendants AS (
                            SELECT name
                            FROM clerk_civil_parties
                            WHERE case_number = :case_number
                              AND party_type = 'Defendant'
                              AND name IS NOT NULL
                              AND name NOT ILIKE 'Unknown%%'
                        )
                        SELECT DISTINCT
                            p.case_number,
                            c.style,
                            c.case_type,
                            c.filing_date,
                            c.case_status,
                            c.is_foreclosure,
                            p.name AS shared_party
                        FROM clerk_civil_parties p
                        JOIN clerk_civil_cases c ON c.case_number = p.case_number
                        JOIN source_defendants sd ON p.name = sd.name
                        WHERE p.case_number != :case_number
                          AND p.party_type = 'Defendant'
                        ORDER BY c.filing_date DESC
                    """),
                    {"case_number": case_number},
                ).fetchall()
                return [
                    {
                        "case_number": row[0],
                        "style": row[1],
                        "case_type": row[2],
                        "filing_date": row[3],
                        "case_status": row[4],
                        "is_foreclosure": row[5],
                        "shared_party": row[6],
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.warning(f"get_related_cases({case_number}) failed: {e}")
            return []

    def find_ca_for_cc_case(self, cc_case_number: str) -> dict | None:
        """
        Given a County Court (CC) case number, find the related Circuit Court (CA)
        mortgage foreclosure case involving the same defendant.

        This is a pipeline helper: CC cases often appear in the auction list but
        the real foreclosure judgment lives in a CA case.

        Returns the most recent matching CA foreclosure case dict, or None.
        """
        if not self._available or not cc_case_number:
            return None
        related = self.get_related_cases(cc_case_number)
        # Filter to foreclosure cases only
        foreclosures = [r for r in related if r.get("is_foreclosure")]
        if not foreclosures:
            return None
        # Return the most recent one
        return foreclosures[0]

    # ------------------------------------------------------------------
    # Conversion: clerk case_number <-> pipeline case_number
    # ------------------------------------------------------------------

    def normalize_case_number(self, case_number: str) -> str | None:
        """
        Convert a pipeline case_number (e.g., 292026CA000019XXXXHC) to
        clerk format (e.g., 26-CA-000019) or vice versa.

        The clerk uses 'YY-TYPE-NNNNNN' format.
        The pipeline uses '29YYYYTTNNNNNN...' format (14-char Hillsborough UCN prefix).
        """
        if not case_number:
            return None

        # Already in clerk format?
        if "-" in case_number and len(case_number) <= 16:
            return case_number

        # Try to parse pipeline UCN format: 29YYYYTTNNNNNN...
        # County code 29 = Hillsborough, then YYYY, then TT (CA/CC), then 6-digit seq
        cleaned = case_number.strip()
        if len(cleaned) >= 14 and cleaned[:2] == "29":
            year_full = cleaned[2:6]
            case_type_code = cleaned[6:8]
            seq = cleaned[8:14]
            try:
                year_short = year_full[2:]  # last 2 digits
                return f"{year_short}-{case_type_code}-{seq}"
            except (ValueError, IndexError):
                pass

        return None

    def lookup_by_pipeline_case_number(self, pipeline_case_number: str) -> dict | None:
        """
        Look up a case using the pipeline's full case number format.

        Converts from '292026CA000019...' to '26-CA-000019' and queries.
        """
        clerk_num = self.normalize_case_number(pipeline_case_number)
        if not clerk_num:
            return None
        return self.get_case(clerk_num)
