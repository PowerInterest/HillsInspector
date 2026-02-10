"""
Chain Builder - Constructs chain of title from discovered documents.

This module provides:
- Building ownership periods from deeds
- Attaching encumbrances to ownership periods
- Calculating MRTA compliance
- Identifying gaps and issues in the chain
"""

from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import sqlite3
from loguru import logger

from config.step4v2 import (
    DEED_TYPES,
    ENCUMBRANCE_TYPES,
    MRTA_YEARS_REQUIRED,
    SATISFACTION_TYPES,
)
from src.services.step4v2.name_matcher import NameMatcher
from src.utils.time import parse_date, today_local


@dataclass
class OwnershipPeriod:
    """Represents a period of ownership in the chain."""

    id: Optional[int] = None
    owner_name: str = ""
    acquired_from: str = ""
    acquisition_date: Optional[date] = None
    disposition_date: Optional[date] = None
    acquisition_instrument: str = ""
    acquisition_doc_type: str = ""
    acquisition_price: float = 0.0
    years_covered: float = 0.0
    link_status: str = "unknown"  # 'linked', 'gap', 'root'
    confidence_score: float = 1.0
    mrta_status: str = "pending"  # 'complete', 'incomplete', 'root'


@dataclass
class Encumbrance:
    """Represents a lien, mortgage, or other encumbrance."""

    id: Optional[int] = None
    chain_period_id: Optional[int] = None
    encumbrance_type: str = ""
    creditor: str = ""
    debtor: str = ""
    amount: float = 0.0
    amount_confidence: str = "unknown"
    recording_date: Optional[date] = None
    instrument: str = ""
    book: str = ""
    page: str = ""
    is_satisfied: bool = False
    satisfaction_instrument: str = ""
    satisfaction_date: Optional[date] = None
    survival_status: str = "unknown"  # Set by LienSurvivalAnalyzer


@dataclass
class ChainResult:
    """Result of building a chain of title."""

    folio: str
    periods: list[OwnershipPeriod] = field(default_factory=list)
    encumbrances: list[Encumbrance] = field(default_factory=list)
    total_years: float = 0.0
    is_complete: bool = False
    mrta_status: str = "incomplete"
    gaps: list[dict] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)


class ChainBuilder:
    """
    Chain of title builder.

    Constructs ownership timeline and encumbrances from discovered documents.
    """

    def __init__(self, conn: sqlite3.Connection):
        """Initialize the chain builder."""
        self.conn = conn
        self.name_matcher = NameMatcher(conn)

    def build(self, folio: str) -> ChainResult:
        """
        Build the chain of title for a folio.

        This is the main entry point that:
        1. Gets all documents for the folio
        2. Identifies deed transfers (ownership periods)
        3. Links periods together
        4. Attaches encumbrances
        5. Calculates MRTA status
        """
        logger.info(f"Building chain of title for {folio}")

        # Get documents ordered by date
        documents = self._get_documents(folio)

        if not documents:
            logger.warning(f"No documents found for {folio}")
            return ChainResult(folio=folio, issues=["No documents found"])

        # Build ownership periods from deeds
        periods = self._build_periods(folio, documents)

        # Link periods together
        self._link_periods(periods)

        # Build encumbrances from mortgages/liens
        encumbrances = self._build_encumbrances(folio, documents, periods)

        # Match satisfactions to encumbrances
        self._match_satisfactions(folio, documents, encumbrances)

        # Calculate totals and MRTA status
        total_years = self._calculate_total_years(periods)
        is_complete = total_years >= MRTA_YEARS_REQUIRED

        # Identify gaps
        gaps = self._identify_gaps(periods)

        # Check for issues
        issues = self._check_issues(periods, encumbrances)

        # Determine MRTA status
        mrta_status = "complete" if is_complete else "incomplete"
        if self._has_root_of_title(documents):
            mrta_status = "root"
            is_complete = True

        # Save to database
        self._save_chain(folio, periods)
        self._save_encumbrances(folio, encumbrances)

        result = ChainResult(
            folio=folio,
            periods=periods,
            encumbrances=encumbrances,
            total_years=total_years,
            is_complete=is_complete,
            mrta_status=mrta_status,
            gaps=gaps,
            issues=issues,
        )

        logger.info(
            f"Chain built for {folio}: {len(periods)} periods, "
            f"{len(encumbrances)} encumbrances, {total_years:.1f} years"
        )

        return result

    def _get_documents(self, folio: str) -> list[dict]:
        """Get all documents for a folio ordered by date."""
        result = self.conn.execute(
            """
            SELECT
                id, document_type, instrument_number, recording_date,
                book, page, party1, party2, legal_description,
                sales_price, is_self_transfer, self_transfer_type
            FROM documents
            WHERE folio = ?
            ORDER BY (recording_date IS NULL), recording_date ASC
            """,
            [folio],
        ).fetchall()

        return [dict(row) for row in result]

    def _build_periods(self, folio: str, documents: list[dict]) -> list[OwnershipPeriod]:
        """Build ownership periods from deed documents."""
        periods = []

        for doc in documents:
            doc_type = (doc.get("document_type") or "").upper()

            # Check if this is a transfer document
            if not self._is_transfer_doc(doc_type):
                continue

            # Skip self-transfers (trust transfers, etc.)
            if doc.get("is_self_transfer"):
                continue

            grantee = doc.get("party2") or ""
            grantor = doc.get("party1") or ""
            recording_date = doc.get("recording_date")
            instrument = doc.get("instrument_number") or ""

            if isinstance(recording_date, str):
                recording_date = parse_date(recording_date)

            # Create ownership period
            period = OwnershipPeriod(
                owner_name=grantee,
                acquired_from=grantor,
                acquisition_date=recording_date,
                acquisition_instrument=instrument,
                acquisition_doc_type=doc_type,
                acquisition_price=doc.get("sales_price") or 0.0,
            )

            periods.append(period)

        # Sort by acquisition date
        periods.sort(key=lambda p: p.acquisition_date or date.min)

        return periods

    def _is_transfer_doc(self, doc_type: str) -> bool:
        """Check if document type is a transfer document."""
        doc_type_upper = doc_type.upper()
        return any(deed_type in doc_type_upper for deed_type in DEED_TYPES)

    def _link_periods(self, periods: list[OwnershipPeriod]) -> None:
        """Link ownership periods together and set disposition dates."""
        for i, period in enumerate(periods):
            # Set disposition date from next period's acquisition
            if i + 1 < len(periods):
                period.disposition_date = periods[i + 1].acquisition_date

            # Calculate years covered
            if period.acquisition_date:
                end_date = period.disposition_date or today_local()
                days = (end_date - period.acquisition_date).days
                period.years_covered = max(0, days / 365.25)

            # Check if linked to previous period
            if i > 0:
                prev = periods[i - 1]
                # Check if previous owner matches current grantor
                if self._names_match(prev.owner_name, period.acquired_from):
                    period.link_status = "linked"
                    period.confidence_score = 1.0
                else:
                    period.link_status = "gap"
                    period.confidence_score = 0.5
            else:
                period.link_status = "root"

    def _names_match(self, name1: str, name2: str) -> bool:
        """Check if two names match."""
        if not name1 or not name2:
            return False
        result = self.name_matcher.match(name1, name2)
        return result.is_match

    def _build_encumbrances(
        self,
        folio: str,
        documents: list[dict],
        periods: list[OwnershipPeriod],
    ) -> list[Encumbrance]:
        """Build encumbrances from mortgage/lien documents."""
        encumbrances = []

        for doc in documents:
            doc_type = (doc.get("document_type") or "").upper()

            if not self._is_encumbrance_doc(doc_type):
                continue

            recording_date = doc.get("recording_date")
            if isinstance(recording_date, str):
                recording_date = parse_date(recording_date)

            # Find which ownership period this belongs to
            period_id = self._find_period_for_date(periods, recording_date)

            encumbrance = Encumbrance(
                chain_period_id=period_id,
                encumbrance_type=self._classify_encumbrance(doc_type),
                creditor=doc.get("party2") or "",  # Lender
                debtor=doc.get("party1") or "",    # Borrower
                amount=doc.get("sales_price") or 0.0,
                amount_confidence="ori_api",
                recording_date=recording_date,
                instrument=doc.get("instrument_number") or "",
                book=doc.get("book") or "",
                page=doc.get("page") or "",
            )

            encumbrances.append(encumbrance)

        return encumbrances

    def _is_encumbrance_doc(self, doc_type: str) -> bool:
        """Check if document type is an encumbrance."""
        doc_type_upper = doc_type.upper()
        return any(enc_type in doc_type_upper for enc_type in ENCUMBRANCE_TYPES)

    def _classify_encumbrance(self, doc_type: str) -> str:
        """Classify encumbrance type from document type."""
        doc_type_upper = doc_type.upper()

        if "MORTGAGE" in doc_type_upper or "MTG" in doc_type_upper:
            return "mortgage"
        if "JUDGMENT" in doc_type_upper or "JUD" in doc_type_upper:
            return "judgment"
        if "LIS PENDENS" in doc_type_upper or "LP" in doc_type_upper:
            return "lis_pendens"
        if "LIEN" in doc_type_upper or "LN" in doc_type_upper:
            return "lien"
        return "other"

    def _find_period_for_date(
        self,
        periods: list[OwnershipPeriod],
        recording_date: Optional[date],
    ) -> Optional[int]:
        """Find the ownership period ID that contains a date."""
        if not recording_date:
            return None

        for period in periods:
            if period.id is None:
                continue

            start = period.acquisition_date or date.min
            end = period.disposition_date or date.max

            if start <= recording_date <= end:
                return period.id

        return None

    def _match_satisfactions(
        self,
        folio: str,
        documents: list[dict],
        encumbrances: list[Encumbrance],
    ) -> None:
        """Match satisfaction documents to encumbrances."""
        # Get satisfaction documents
        satisfactions = [
            doc for doc in documents
            if self._is_satisfaction_doc((doc.get("document_type") or "").upper())
        ]

        for sat in satisfactions:
            sat_party1 = sat.get("party1") or ""  # Who is being satisfied
            sat_party2 = sat.get("party2") or ""  # Who is satisfying
            sat_date = sat.get("recording_date")
            sat_instrument = sat.get("instrument_number") or ""

            if isinstance(sat_date, str):
                sat_date = parse_date(sat_date)

            # Find matching encumbrance
            for enc in encumbrances:
                if enc.is_satisfied:
                    continue

                # Match by creditor name
                if self._names_match(enc.creditor, sat_party1) or self._names_match(enc.creditor, sat_party2):
                    enc.is_satisfied = True
                    enc.satisfaction_instrument = sat_instrument
                    enc.satisfaction_date = sat_date
                    break

    def _is_satisfaction_doc(self, doc_type: str) -> bool:
        """Check if document type is a satisfaction."""
        doc_type_upper = doc_type.upper()
        return any(sat_type in doc_type_upper for sat_type in SATISFACTION_TYPES)

    def _calculate_total_years(self, periods: list[OwnershipPeriod]) -> float:
        """Calculate total years covered by the chain."""
        if not periods:
            return 0.0

        # Get earliest acquisition and latest disposition
        earliest = min(p.acquisition_date for p in periods if p.acquisition_date)
        latest = max(p.disposition_date or today_local() for p in periods)

        days = (latest - earliest).days
        return max(0, days / 365.25)

    def _has_root_of_title(self, documents: list[dict]) -> bool:
        """Check if any document is a root of title."""
        root_types = {"PLAT", "PATENT", "GOVERNMENT DEED", "GOV DEED"}
        for doc in documents:
            doc_type = (doc.get("document_type") or "").upper()
            if any(root in doc_type for root in root_types):
                return True
        return False

    def _identify_gaps(self, periods: list[OwnershipPeriod]) -> list[dict]:
        """Identify gaps in the chain."""
        gaps = []
        for period in periods:
            if period.link_status == "gap":
                gaps.append({
                    "after": period.acquired_from,
                    "before": period.owner_name,
                    "date": period.acquisition_date.isoformat() if period.acquisition_date else None,
                    "instrument": period.acquisition_instrument,
                })
        return gaps

    def _check_issues(
        self,
        periods: list[OwnershipPeriod],
        encumbrances: list[Encumbrance],
    ) -> list[str]:
        """Check for issues in the chain."""
        issues = []

        if not periods:
            issues.append("No ownership periods found")
            return issues

        # Check for gaps
        gap_count = sum(1 for p in periods if p.link_status == "gap")
        if gap_count > 0:
            issues.append(f"{gap_count} gap(s) in chain")

        # Check for unsatisfied encumbrances
        unsatisfied = sum(1 for e in encumbrances if not e.is_satisfied)
        if unsatisfied > 0:
            issues.append(f"{unsatisfied} unsatisfied encumbrance(s)")

        # Check for very short periods (< 1 month)
        short_periods = sum(1 for p in periods if p.years_covered < 0.1 and p.years_covered > 0)
        if short_periods > 0:
            issues.append(f"{short_periods} very short ownership period(s)")

        return issues

    def _save_chain(self, folio: str, periods: list[OwnershipPeriod]) -> None:
        """Save ownership periods to chain_of_title table."""
        # Clear existing chain
        self.conn.execute("DELETE FROM chain_of_title WHERE folio = ?", [folio])

        for period in periods:
            cursor = self.conn.execute(
                """
                INSERT INTO chain_of_title (
                    folio, owner_name, acquired_from, acquisition_date,
                    disposition_date, acquisition_instrument, acquisition_doc_type,
                    acquisition_price, link_status, confidence_score,
                    mrta_status, years_covered
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    folio,
                    period.owner_name,
                    period.acquired_from,
                    period.acquisition_date,
                    period.disposition_date,
                    period.acquisition_instrument,
                    period.acquisition_doc_type,
                    period.acquisition_price,
                    period.link_status,
                    period.confidence_score,
                    period.mrta_status,
                    period.years_covered,
                ],
            )
            period.id = cursor.lastrowid

    def _save_encumbrances(self, folio: str, encumbrances: list[Encumbrance]) -> None:
        """Save encumbrances to database."""
        # Clear existing encumbrances
        self.conn.execute("DELETE FROM encumbrances WHERE folio = ?", [folio])

        for enc in encumbrances:
            cursor = self.conn.execute(
                """
                INSERT INTO encumbrances (
                    folio, chain_period_id, encumbrance_type, creditor, debtor,
                    amount, amount_confidence, recording_date, instrument,
                    book, page, is_satisfied, satisfaction_instrument, satisfaction_date
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    folio,
                    enc.chain_period_id,
                    enc.encumbrance_type,
                    enc.creditor,
                    enc.debtor,
                    enc.amount,
                    enc.amount_confidence,
                    enc.recording_date,
                    enc.instrument,
                    enc.book,
                    enc.page,
                    enc.is_satisfied,
                    enc.satisfaction_instrument,
                    enc.satisfaction_date,
                ],
            )
            enc.id = cursor.lastrowid

    def get_chain(self, folio: str) -> list[OwnershipPeriod]:
        """Get existing chain of title from database."""
        result = self.conn.execute(
            """
            SELECT
                id, owner_name, acquired_from, acquisition_date,
                disposition_date, acquisition_instrument, acquisition_doc_type,
                acquisition_price, link_status, confidence_score,
                mrta_status, years_covered
            FROM chain_of_title
            WHERE folio = ?
            ORDER BY (acquisition_date IS NULL), acquisition_date ASC
            """,
            [folio],
        ).fetchall()

        periods = []
        for row in result:
            d = dict(row)
            period = OwnershipPeriod(
                id=d["id"],
                owner_name=d["owner_name"] or "",
                acquired_from=d["acquired_from"] or "",
                acquisition_date=d["acquisition_date"],
                disposition_date=d["disposition_date"],
                acquisition_instrument=d["acquisition_instrument"] or "",
                acquisition_doc_type=d["acquisition_doc_type"] or "",
                acquisition_price=d["acquisition_price"] or 0.0,
                link_status=d["link_status"] or "unknown",
                confidence_score=d["confidence_score"] or 1.0,
                mrta_status=d["mrta_status"] or "pending",
                years_covered=d["years_covered"] or 0.0,
            )
            periods.append(period)

        return periods

    def get_encumbrances(self, folio: str) -> list[Encumbrance]:
        """Get existing encumbrances from database."""
        result = self.conn.execute(
            """
            SELECT
                id, chain_period_id, encumbrance_type, creditor, debtor,
                amount, amount_confidence, recording_date, instrument,
                book, page, is_satisfied, satisfaction_instrument,
                satisfaction_date, survival_status
            FROM encumbrances
            WHERE folio = ?
            ORDER BY (recording_date IS NULL), recording_date ASC
            """,
            [folio],
        ).fetchall()

        encumbrances = []
        for row in result:
            d = dict(row)
            enc = Encumbrance(
                id=d["id"],
                chain_period_id=d["chain_period_id"],
                encumbrance_type=d["encumbrance_type"] or "",
                creditor=d["creditor"] or "",
                debtor=d["debtor"] or "",
                amount=d["amount"] or 0.0,
                amount_confidence=d["amount_confidence"] or "unknown",
                recording_date=d["recording_date"],
                instrument=d["instrument"] or "",
                book=d["book"] or "",
                page=d["page"] or "",
                is_satisfied=d["is_satisfied"] or False,
                satisfaction_instrument=d["satisfaction_instrument"] or "",
                satisfaction_date=d["satisfaction_date"],
                survival_status=d["survival_status"] or "unknown",
            )
            encumbrances.append(enc)

        return encumbrances
