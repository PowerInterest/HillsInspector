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
from src.db.type_normalizer import (
    CANONICAL_ENCUMBRANCE_TYPES,
    CANONICAL_SATISFACTION_TYPES,
    _PAREN_RE,
    _DOC_TYPE_MAP,
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
    mrta_status: str = "pending"
    data_source: str = "ori"  # 'ori', 'pg_sales', 'merged', 'inferred'  # 'complete', 'incomplete', 'root'


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

    def __init__(self, conn: sqlite3.Connection, pg_service=None):
        """Initialize the chain builder."""
        self.conn = conn
        self.name_matcher = NameMatcher(conn)
        self.pg_service = pg_service

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

        # Build ownership periods from deeds (ORI documents)
        ori_periods = self._build_periods(folio, documents)

        # Build ownership periods from PG sales (if available)
        pg_periods = self._build_periods_from_pg_sales(folio) if self.pg_service else []

        # Merge PG backbone with ORI periods
        if pg_periods:
            periods = self._merge_periods(pg_periods, ori_periods)
        else:
            periods = ori_periods

        # Infer missing ownership periods from mortgages (borrower = owner)
        self._infer_from_mortgages(folio, documents, periods)

        # Add HCPA current owner as chain terminus if missing
        self._add_hcpa_owner_terminus(folio, periods)

        # Link periods together
        self._link_periods(periods)

        # Save chain BEFORE building encumbrances so periods have real DB IDs
        # (_find_period_for_date skips periods with id=None)
        self._save_chain(folio, periods)

        # Build encumbrances from mortgages/liens (periods now have IDs)
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

        # Save encumbrances to database
        self._save_encumbrances(folio, encumbrances)
        self.conn.commit()

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

    def _build_periods_from_pg_sales(self, folio: str) -> list[OwnershipPeriod]:
        """Build ownership periods from PostgreSQL hcpa_allsales records."""
        from src.services.pg_sales_service import SALE_TYPE_MAP

        pg = self.pg_service
        if not pg or not pg.available:
            return []

        pg_folio = pg.resolve_strap_to_folio(folio)
        if not pg_folio:
            logger.debug(f"PG: no folio mapping for strap {folio}")
            return []

        sales = pg.get_sales_chain(pg_folio)
        if not sales:
            return []

        periods = []
        for sale in sales:
            grantee = (sale.get("grantee") or "").strip()
            grantor = (sale.get("grantor") or "").strip()
            if not grantee:
                continue

            # Skip self-transfers (same grantor/grantee after normalization)
            if grantee and grantor and grantee.upper() == grantor.upper():
                continue

            sale_date = sale.get("sale_date")
            if isinstance(sale_date, str):
                sale_date = parse_date(sale_date)

            sale_type = (sale.get("sale_type") or "").upper()
            doc_type = SALE_TYPE_MAP.get(sale_type, sale_type or "DEED")

            # Build instrument reference from doc_num or book/page
            instrument = sale.get("doc_num") or ""
            if not instrument and sale.get("or_book") and sale.get("or_page"):
                instrument = f"{sale['or_book']}/{sale['or_page']}"

            period = OwnershipPeriod(
                owner_name=grantee,
                acquired_from=grantor,
                acquisition_date=sale_date,
                acquisition_instrument=instrument,
                acquisition_doc_type=doc_type,
                acquisition_price=sale.get("sale_amount") or 0.0,
                confidence_score=0.95,
                link_status="pg_sales",
                data_source="pg_sales",
            )
            periods.append(period)

        periods.sort(key=lambda p: p.acquisition_date or date.min)
        if periods:
            logger.info(f"PG sales: {len(periods)} ownership periods for {folio}")
        return periods

    def _merge_periods(
        self,
        pg_periods: list[OwnershipPeriod],
        ori_periods: list[OwnershipPeriod],
    ) -> list[OwnershipPeriod]:
        """Merge PG sales backbone with ORI-derived periods.

        PG periods form the backbone. ORI periods are matched by:
        1. Exact instrument match
        2. Book/page match
        3. Date + name match (30-day window)

        Matched periods: enrich PG with ORI detail, bump confidence to 1.0.
        Unmatched ORI: append as-is.
        """

        if not pg_periods:
            return ori_periods
        if not ori_periods:
            return pg_periods

        merged = list(pg_periods)
        used_ori = set()

        for pg_p in merged:
            pg_inst = pg_p.acquisition_instrument or ""
            pg_date = pg_p.acquisition_date

            for i, ori_p in enumerate(ori_periods):
                if i in used_ori:
                    continue
                ori_inst = ori_p.acquisition_instrument or ""

                matched = False

                # Match 1: exact instrument number
                if pg_inst and ori_inst and pg_inst == ori_inst:
                    matched = True
                # Match 2: book/page (pg_inst might be "1234/567")
                elif "/" in pg_inst and ori_inst:
                    parts = pg_inst.split("/", 1)
                    if len(parts) == 2:
                        bp = f"{parts[0]}/{parts[1]}"
                        if ori_inst == bp:
                            matched = True
                # Match 3: date + name (30-day window)
                elif pg_date and ori_p.acquisition_date:
                    delta = abs((pg_date - ori_p.acquisition_date).days)
                    if delta <= 30 and self._names_match(
                        pg_p.owner_name, ori_p.owner_name
                    ):
                        matched = True

                if matched:
                    used_ori.add(i)
                    pg_p.confidence_score = 1.0
                    pg_p.data_source = "merged"
                    # Prefer ORI's more detailed doc_type if available
                    if ori_p.acquisition_doc_type and len(ori_p.acquisition_doc_type) > len(pg_p.acquisition_doc_type):
                        pg_p.acquisition_doc_type = ori_p.acquisition_doc_type
                    # Prefer ORI instrument if more specific
                    if ori_inst and (not pg_inst or "/" in pg_inst):
                        pg_p.acquisition_instrument = ori_inst
                    break

        # Append unmatched ORI periods
        for i, ori_p in enumerate(ori_periods):
            if i not in used_ori:
                merged.append(ori_p)

        # Sort and deduplicate: same grantee within 7-day window
        merged.sort(key=lambda p: p.acquisition_date or date.min)
        deduped = []
        for p in merged:
            if deduped and p.acquisition_date and deduped[-1].acquisition_date:
                delta = abs((p.acquisition_date - deduped[-1].acquisition_date).days)
                if delta <= 7 and self._names_match(p.owner_name, deduped[-1].owner_name):
                    # Keep the higher-confidence one
                    if p.confidence_score > deduped[-1].confidence_score:
                        deduped[-1] = p
                    continue
            deduped.append(p)

        logger.info(
            f"Merged chain: {len(pg_periods)} PG + {len(ori_periods)} ORI "
            f"→ {len(deduped)} periods ({len(ori_periods) - len([i for i in range(len(ori_periods)) if i not in used_ori])} matched)"
        )
        return deduped

    def _infer_from_mortgages(
        self, folio: str, documents: list[dict], periods: list[OwnershipPeriod]
    ) -> None:
        """Infer missing ownership periods from mortgage documents.

        If a mortgage's borrower (party1) doesn't match any existing chain owner
        AND the mortgage date is after the last known deed, create an inferred period.
        """
        if not periods:
            return

        existing_owners = {p.owner_name.upper() for p in periods if p.owner_name}
        last_deed_date = max((p.acquisition_date for p in periods if p.acquisition_date), default=date.min)

        for doc in documents:
            canonical, _ = self._canonical_type(doc.get("document_type") or "")
            if canonical != "mortgage":
                continue

            borrower = doc.get("party1") or ""
            recording_date = doc.get("recording_date")
            if isinstance(recording_date, str):
                recording_date = parse_date(recording_date)

            if not borrower or not recording_date:
                continue

            # Only infer if this borrower isn't already in the chain
            borrower_upper = borrower.upper()
            if any(self._names_match(borrower, owner) for owner in existing_owners):
                continue

            # Only infer if mortgage is after the last known deed
            if recording_date <= last_deed_date:
                continue

            period = OwnershipPeriod(
                owner_name=borrower,
                acquisition_date=recording_date,
                acquisition_doc_type="INFERRED FROM MORTGAGE",
                link_status="inferred",
                confidence_score=0.6,
            )
            periods.append(period)
            existing_owners.add(borrower_upper)
            logger.info(f"Inferred ownership period for {borrower} from mortgage on {recording_date}")

        # Re-sort
        periods.sort(key=lambda p: p.acquisition_date or date.min)

    def _add_hcpa_owner_terminus(self, folio: str, periods: list[OwnershipPeriod]) -> None:
        """Add HCPA current owner as chain terminus if not already present."""
        row = self.conn.execute(
            "SELECT owner_name FROM parcels WHERE folio = ?", [folio]
        ).fetchone()
        if not row:
            return

        hcpa_owner = dict(row).get("owner_name") or ""
        if not hcpa_owner:
            return

        # Check if HCPA owner already in chain
        if any(self._names_match(hcpa_owner, p.owner_name) for p in periods if p.owner_name):
            return

        # Add as final period with today's date (approximate)
        period = OwnershipPeriod(
            owner_name=hcpa_owner,
            acquisition_doc_type="HCPA CURRENT OWNER",
            link_status="inferred",
            confidence_score=0.5,
        )
        periods.append(period)
        logger.info(f"Added HCPA owner '{hcpa_owner}' as chain terminus for {folio}")

    @staticmethod
    def _canonical_type(doc_type: str) -> tuple[str, bool]:
        """Extract canonical type from raw or normalized doc_type.

        Returns (canonical_type, skip_fallback) where skip_fallback is True
        when the type was resolved via the ORI code map OR was recognized as
        a canonical form. Substring-based fallback checks run when
        skip_fallback is False.

        '(DRJUD) DOMESTIC RELATIONS JUDGMENT' → code 'DRJUD' → unmapped → ('', False)
          → allows substring fallback to match "JUDGMENT" in full description
        '(MTG) MORTGAGE' → code 'MTG' → ('mortgage', True)
        'mortgage' → already normalized → ('mortgage', True)
        'some_unknown_string' → not ORI, not mapped → ('some_unknown_string', False)
        """
        t = (doc_type or "").strip()
        if not t:
            return "", True
        m = _PAREN_RE.match(t)
        if m:
            code = m.group(1).upper()
            mapped = _DOC_TYPE_MAP.get(code)
            if mapped:
                return mapped, True
            # ORI parenthetical format but code not in map — allow substring
            # fallback so the full description text (e.g. "DOMESTIC RELATIONS
            # JUDGMENT") can still match encumbrance/satisfaction keywords.
            return "", False
        # Already normalized or bare code
        mapped = _DOC_TYPE_MAP.get(t.upper())
        if mapped:
            return mapped, True
        # Check if already a canonical form
        lower = t.lower()
        if lower in CANONICAL_ENCUMBRANCE_TYPES or lower in CANONICAL_SATISFACTION_TYPES:
            return lower, True
        return lower, False

    def _is_transfer_doc(self, doc_type: str) -> bool:
        """Check if document type is a transfer document."""
        canonical, mapped = self._canonical_type(doc_type)
        if canonical == "deed":
            return True
        if mapped:
            return False
        # Fallback: substring check only for completely unknown types
        doc_type_upper = (doc_type or "").upper()
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
            raw_doc_type = doc.get("document_type") or ""

            if not self._is_encumbrance_doc(raw_doc_type):
                continue

            recording_date = doc.get("recording_date")
            if isinstance(recording_date, str):
                recording_date = parse_date(recording_date)

            # Find which ownership period this belongs to
            period_id = self._find_period_for_date(periods, recording_date)

            # For judgments/liens/lis pendens: party1 = creditor (plaintiff/lienor)
            # For mortgages: party1 = borrower, party2 = lender
            canonical, _ = self._canonical_type(raw_doc_type)
            if canonical in ("judgment", "lis_pendens", "lien"):
                creditor = doc.get("party1") or ""
                debtor = doc.get("party2") or ""
            else:
                creditor = doc.get("party2") or ""
                debtor = doc.get("party1") or ""

            encumbrance = Encumbrance(
                chain_period_id=period_id,
                encumbrance_type=self._classify_encumbrance(raw_doc_type),
                creditor=creditor,
                debtor=debtor,
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
        canonical, mapped = self._canonical_type(doc_type)
        if canonical in CANONICAL_ENCUMBRANCE_TYPES:
            return True
        if mapped:
            return False
        # Fallback: substring check only for completely unknown types
        doc_type_upper = (doc_type or "").upper()
        return any(enc_type in doc_type_upper for enc_type in ENCUMBRANCE_TYPES)

    def _classify_encumbrance(self, doc_type: str) -> str:
        """Classify encumbrance type from document type."""
        from src.db.type_normalizer import normalize_encumbrance_type
        return normalize_encumbrance_type(doc_type)

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
        """Match satisfaction documents to encumbrances using multi-pass strategy.

        Pass 1: Instrument number proximity (same recording session, +-5 instruments)
        Pass 2: Creditor name match (sat_party1 against enc.creditor)
        Pass 3: Debtor name match (sat_party1 against enc.debtor) — for liens
        Pass 4: Cross-match (sat_party2 against enc.creditor or enc.debtor)
        """
        # Get satisfaction documents
        satisfactions = [
            doc for doc in documents
            if self._is_satisfaction_doc((doc.get("document_type") or "").upper())
        ]

        if not satisfactions or not encumbrances:
            return

        # Build lookup structures
        for sat in satisfactions:
            if isinstance(sat.get("recording_date"), str):
                sat["recording_date"] = parse_date(sat["recording_date"])

        def _date_ok(sat_date, enc_date) -> bool:
            """Check if satisfaction date is plausible relative to encumbrance."""
            if not sat_date or not enc_date:
                return True  # Allow match when either date is unknown
            ed = parse_date(enc_date) if isinstance(enc_date, str) else enc_date
            if not ed:
                return True
            return sat_date >= ed

        def _mark_satisfied(enc: Encumbrance, sat: dict) -> None:
            enc.is_satisfied = True
            enc.satisfaction_instrument = sat.get("instrument_number") or ""
            enc.satisfaction_date = sat.get("recording_date")

        def _split_names(name_str: str) -> list[str]:
            """Split comma-separated names into individual trimmed names."""
            return [n.strip() for n in (name_str or "").split(",") if n.strip()]

        # Track which satisfactions have been consumed
        used_sats = set()  # indices into satisfactions list

        # --- PASS 1: Instrument number proximity (strongest signal) ---
        # Documents recorded in the same session often have adjacent instrument numbers.
        # A satisfaction with instrument N likely satisfies an encumbrance with instrument N-1 to N-5.
        for sat_idx, sat in enumerate(satisfactions):
            if sat_idx in used_sats:
                continue
            sat_inst = (sat.get("instrument_number") or "").strip()
            if not sat_inst:
                continue
            try:
                sat_inst_num = int(sat_inst)
            except (ValueError, TypeError):
                continue

            best_enc = None
            best_dist = 6  # anything > 5 is no match
            for enc in encumbrances:
                if enc.is_satisfied:
                    continue
                enc_inst = (enc.instrument or "").strip()
                if not enc_inst:
                    continue
                try:
                    enc_inst_num = int(enc_inst)
                except (ValueError, TypeError):
                    continue
                dist = abs(sat_inst_num - enc_inst_num)
                if 0 < dist <= 5 and dist < best_dist:
                    # Also verify at least one name overlaps to avoid cross-property matches
                    sat_names = _split_names(sat.get("party1")) + _split_names(sat.get("party2"))
                    enc_all_names = _split_names(enc.creditor) + _split_names(enc.debtor)
                    if any(
                        self._names_match(sn, en)
                        for sn in sat_names for en in enc_all_names
                        if sn and en
                    ):
                        best_enc = enc
                        best_dist = dist

            if best_enc is not None:
                _mark_satisfied(best_enc, sat)
                used_sats.add(sat_idx)

        # --- PASS 2: Creditor name match (sat.party1 against enc.creditor) ---
        for sat_idx, sat in enumerate(satisfactions):
            if sat_idx in used_sats:
                continue
            sat_date = sat.get("recording_date")
            sat_p1_names = _split_names(sat.get("party1"))
            if not sat_p1_names:
                continue

            for enc in encumbrances:
                if enc.is_satisfied:
                    continue
                if not _date_ok(sat_date, enc.recording_date):
                    continue
                enc_cred_names = _split_names(enc.creditor)
                if enc_cred_names and any(
                    self._names_match(en, sn)
                    for en in enc_cred_names for sn in sat_p1_names
                    if en and sn
                ):
                    _mark_satisfied(enc, sat)
                    used_sats.add(sat_idx)
                    break

        # --- PASS 3: Debtor name match (sat.party1 against enc.debtor) ---
        # For liens: creditor = property owner, debtor = lien holder.
        # The satisfaction party1 = the entity being satisfied = the lien holder = enc.debtor.
        for sat_idx, sat in enumerate(satisfactions):
            if sat_idx in used_sats:
                continue
            sat_date = sat.get("recording_date")
            sat_p1_names = _split_names(sat.get("party1"))
            if not sat_p1_names:
                continue

            for enc in encumbrances:
                if enc.is_satisfied:
                    continue
                if not _date_ok(sat_date, enc.recording_date):
                    continue
                enc_debt_names = _split_names(enc.debtor)
                if enc_debt_names and any(
                    self._names_match(en, sn)
                    for en in enc_debt_names for sn in sat_p1_names
                    if en and sn
                ):
                    _mark_satisfied(enc, sat)
                    used_sats.add(sat_idx)
                    break

        # --- PASS 4: Cross-match (sat.party2 against enc.creditor or enc.debtor) ---
        # sat.party2 = borrower/debtor; enc.debtor = borrower (for mortgages)
        # or enc.creditor = borrower (for liens where roles are swapped)
        for sat_idx, sat in enumerate(satisfactions):
            if sat_idx in used_sats:
                continue
            sat_date = sat.get("recording_date")
            sat_p2_names = _split_names(sat.get("party2"))
            if not sat_p2_names:
                continue

            for enc in encumbrances:
                if enc.is_satisfied:
                    continue
                if not _date_ok(sat_date, enc.recording_date):
                    continue
                # Match sat.party2 against enc.debtor (mortgage borrower match)
                enc_all_names = _split_names(enc.creditor) + _split_names(enc.debtor)
                if enc_all_names and any(
                    self._names_match(en, sn)
                    for en in enc_all_names for sn in sat_p2_names
                    if en and sn
                ):
                    _mark_satisfied(enc, sat)
                    used_sats.add(sat_idx)
                    break

    def _is_satisfaction_doc(self, doc_type: str) -> bool:
        """Check if document type is a satisfaction."""
        canonical, mapped = self._canonical_type(doc_type)
        if canonical in CANONICAL_SATISFACTION_TYPES:
            return True
        if mapped:
            return False
        # Fallback: substring check only for completely unknown types
        doc_type_upper = (doc_type or "").upper()
        return any(sat_type in doc_type_upper for sat_type in SATISFACTION_TYPES)

    def rematch_all_satisfactions(self) -> dict:
        """Re-run satisfaction matching on all folios that have unsatisfied encumbrances.

        This is a standalone operation that works directly with DB data, without
        rebuilding the full chain. Useful for backfilling satisfaction links after
        improving the matching algorithm.

        Returns a summary dict with counts.
        """
        # Find folios with unsatisfied encumbrances
        folios = [
            row[0] for row in self.conn.execute(
                "SELECT DISTINCT folio FROM encumbrances WHERE is_satisfied = 0 OR is_satisfied IS NULL"
            ).fetchall()
        ]
        logger.info(f"Rematching satisfactions for {len(folios)} folios with unsatisfied encumbrances")

        total_newly_satisfied = 0
        total_folios_changed = 0

        for folio in folios:
            # Get documents for this folio
            documents = self._get_documents(folio)

            # Get current encumbrances from DB
            enc_rows = self.conn.execute(
                """
                SELECT id, encumbrance_type, creditor, debtor, amount, amount_confidence,
                       recording_date, instrument, book, page, is_satisfied,
                       satisfaction_instrument, satisfaction_date, chain_period_id
                FROM encumbrances WHERE folio = ?
                ORDER BY recording_date
                """,
                [folio],
            ).fetchall()

            if not enc_rows:
                continue

            # Reconstruct Encumbrance objects
            encumbrances = []
            for row in enc_rows:
                rd = dict(row)
                rec_date = rd.get("recording_date")
                if isinstance(rec_date, str):
                    rec_date = parse_date(rec_date)
                sat_date = rd.get("satisfaction_date")
                if isinstance(sat_date, str):
                    sat_date = parse_date(sat_date)
                encumbrances.append(Encumbrance(
                    id=rd.get("id"),
                    chain_period_id=rd.get("chain_period_id"),
                    encumbrance_type=rd.get("encumbrance_type") or "",
                    creditor=rd.get("creditor") or "",
                    debtor=rd.get("debtor") or "",
                    amount=rd.get("amount") or 0.0,
                    amount_confidence=rd.get("amount_confidence") or "unknown",
                    recording_date=rec_date,
                    instrument=rd.get("instrument") or "",
                    book=rd.get("book") or "",
                    page=rd.get("page") or "",
                    is_satisfied=bool(rd.get("is_satisfied")),
                    satisfaction_instrument=rd.get("satisfaction_instrument") or "",
                    satisfaction_date=sat_date,
                ))

            # Track which encumbrances were already satisfied before rematch
            previously_satisfied_ids = {e.id for e in encumbrances if e.is_satisfied}

            # Run the matching
            self._match_satisfactions(folio, documents, encumbrances)

            # Update DB only for NEWLY satisfied encumbrances
            newly_satisfied = 0
            for enc in encumbrances:
                if enc.is_satisfied and enc.id is not None and enc.id not in previously_satisfied_ids:
                    newly_satisfied += 1
                    self.conn.execute(
                        """
                        UPDATE encumbrances
                        SET is_satisfied = 1, satisfaction_instrument = ?, satisfaction_date = ?
                        WHERE id = ?
                        """,
                        [enc.satisfaction_instrument, enc.satisfaction_date, enc.id],
                    )

            if newly_satisfied > 0:
                total_newly_satisfied += newly_satisfied
                total_folios_changed += 1

        self.conn.commit()
        summary = {
            "folios_processed": len(folios),
            "folios_changed": total_folios_changed,
            "newly_satisfied": total_newly_satisfied,
        }
        logger.info(f"Satisfaction rematch complete: {summary}")
        return summary

    def _calculate_total_years(self, periods: list[OwnershipPeriod]) -> float:
        """Calculate total years covered by the chain."""
        if not periods:
            return 0.0

        # Get earliest acquisition and latest disposition
        valid_dates = [parse_date(p.acquisition_date) for p in periods if p.acquisition_date]
        valid_dates = [d for d in valid_dates if d is not None]
        if not valid_dates:
            return 0.0
        earliest = min(valid_dates)
        disp_dates = [parse_date(p.disposition_date) or today_local() for p in periods]
        disp_dates = [d for d in disp_dates if d is not None]
        latest = max(disp_dates) if disp_dates else today_local()

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
        if not periods:
            # Don't delete existing chain entries if we have nothing to replace them with.
            # This preserves judgment-inferred entries for folios with no deed documents.
            return

        # Clear existing chain
        self.conn.execute("DELETE FROM chain_of_title WHERE folio = ?", [folio])

        for period in periods:
            cursor = self.conn.execute(
                """
                INSERT INTO chain_of_title (
                    folio, owner_name, acquired_from, acquisition_date,
                    disposition_date, acquisition_instrument, acquisition_doc_type,
                    acquisition_price, link_status, confidence_score,
                    mrta_status, years_covered, data_source
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    period.data_source,
                ],
            )
            period.id = cursor.lastrowid

    def _save_encumbrances(self, folio: str, encumbrances: list[Encumbrance]) -> None:
        """Save encumbrances to database, preserving prior survival analysis data."""
        # Preserve existing survival data before clearing
        prior_survival = {}
        for row in self.conn.execute("SELECT * FROM encumbrances WHERE folio = ?", [folio]).fetchall():
            row_d = dict(row)
            inst = (row_d.get("instrument") or "").strip()
            book = (row_d.get("book") or "").strip()
            page = (row_d.get("page") or "").strip()
            if inst:
                key = f"INST:{inst}"
            elif book and page:
                key = f"BKPG:{book}/{page}"
            else:
                key = f"DTYPE:{row_d.get('recording_date')}_{row_d.get('encumbrance_type')}"
            prior_survival[key] = {
                "survival_status": row_d.get("survival_status"),
                "survival_reason": row_d.get("survival_reason"),
                "is_joined": row_d.get("is_joined"),
                "is_inferred": row_d.get("is_inferred"),
            }

        # Clear existing encumbrances
        self.conn.execute("DELETE FROM encumbrances WHERE folio = ?", [folio])

        for enc in encumbrances:
            # Look up prior survival data by instrument or book/page
            prior = None
            inst = (enc.instrument or "").strip()
            book = (enc.book or "").strip()
            page = (enc.page or "").strip()
            if inst:
                prior = prior_survival.get(f"INST:{inst}")
            if prior is None and book and page:
                prior = prior_survival.get(f"BKPG:{book}/{page}")

            survival_status = prior.get("survival_status") if prior else None
            survival_reason = prior.get("survival_reason") if prior else None
            is_joined = prior.get("is_joined") if prior else None
            is_inferred = prior.get("is_inferred") if prior else None

            cursor = self.conn.execute(
                """
                INSERT INTO encumbrances (
                    folio, chain_period_id, encumbrance_type, creditor, debtor,
                    amount, amount_confidence, recording_date, instrument,
                    book, page, is_satisfied, satisfaction_instrument, satisfaction_date,
                    survival_status, survival_reason, is_joined, is_inferred
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    survival_status,
                    survival_reason,
                    is_joined,
                    is_inferred,
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
                mrta_status, years_covered, data_source
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
                acquisition_date=parse_date(d["acquisition_date"]),
                disposition_date=parse_date(d["disposition_date"]),
                acquisition_instrument=d["acquisition_instrument"] or "",
                acquisition_doc_type=d["acquisition_doc_type"] or "",
                acquisition_price=d["acquisition_price"] or 0.0,
                link_status=d["link_status"] or "unknown",
                confidence_score=d["confidence_score"] or 1.0,
                mrta_status=d["mrta_status"] or "pending",
                years_covered=d["years_covered"] or 0.0,
                data_source=d.get("data_source") or "ori",
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
                recording_date=parse_date(d["recording_date"]),
                instrument=d["instrument"] or "",
                book=d["book"] or "",
                page=d["page"] or "",
                is_satisfied=d["is_satisfied"] or False,
                satisfaction_instrument=d["satisfaction_instrument"] or "",
                satisfaction_date=parse_date(d["satisfaction_date"]),
                survival_status=d["survival_status"] or "unknown",
            )
            encumbrances.append(enc)

        return encumbrances

    def infer_encumbrances_from_judgment(
        self,
        folio: str,
        auction: dict,
        final_judgment: dict,
    ) -> ChainResult:
        """
        Create judgment-inferred chain period and encumbrance when ORI discovery
        found no encumbrance-type documents.

        Every foreclosure has a foreclosing lien -- if ORI didn't find it, we
        infer it from the judgment data so downstream survival analysis has
        something to work with.  Records are marked ``is_inferred=1``.

        Returns a ChainResult with the inferred period and encumbrance(s).
        """
        plaintiff = (
            final_judgment.get("plaintiff")
            or auction.get("plaintiff")
            or ""
        )
        defendant = (
            final_judgment.get("defendant")
            or auction.get("defendant")
            or ""
        )
        case_number = auction.get("case_number") or ""

        if not plaintiff:
            logger.warning(
                f"Cannot infer encumbrances for {folio}: no plaintiff in judgment/auction data"
            )
            return ChainResult(folio=folio, issues=["No plaintiff for inference"])

        # Determine encumbrance type from case number and plaintiff name
        plaintiff_upper = plaintiff.upper()
        is_cc = "CC" in case_number[6:8] if len(case_number) >= 8 else False
        is_hoa = any(
            kw in plaintiff_upper
            for kw in ("ASSOCIATION", "HOA", "CONDO", "HOMEOWNER", "PROPERTY OWNER")
        )

        # HOA/CC foreclosures use 'lien'; mortgage foreclosures use 'mortgage'
        enc_type = "lien" if is_cc or is_hoa else "mortgage"

        # Extract mortgage details from judgment
        foreclosed_mortgage = final_judgment.get("foreclosed_mortgage") or {}
        original_amount = foreclosed_mortgage.get("original_amount") or 0.0
        judgment_amount = final_judgment.get("judgment_amount")
        if not original_amount and judgment_amount:
            try:
                original_amount = float(judgment_amount)
            except (ValueError, TypeError):
                original_amount = 0.0

        recording_book = foreclosed_mortgage.get("recording_book") or ""
        recording_page = foreclosed_mortgage.get("recording_page") or ""
        instrument_number = foreclosed_mortgage.get("instrument_number") or ""
        original_date = parse_date(foreclosed_mortgage.get("original_date"))
        recording_date = parse_date(foreclosed_mortgage.get("recording_date")) or original_date

        # Create an inferred ownership period for the defendant
        period = OwnershipPeriod(
            owner_name=defendant,
            acquisition_date=recording_date,
            acquisition_doc_type="INFERRED",
            link_status="inferred",
            confidence_score=0.3,
            data_source="inferred",
        )

        # Check if an inferred chain period already exists (idempotency)
        existing_chain = self.conn.execute(
            "SELECT id FROM chain_of_title WHERE folio = ? AND data_source = 'inferred'",
            [folio],
        ).fetchone()

        if existing_chain:
            period.id = existing_chain[0]
            logger.debug(f"Reusing existing inferred chain period {period.id} for {folio}")
        else:
            cursor = self.conn.execute(
                """
                INSERT INTO chain_of_title (
                    folio, owner_name, acquisition_date, acquisition_doc_type,
                    link_status, confidence_score, data_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    folio,
                    period.owner_name,
                    period.acquisition_date,
                    period.acquisition_doc_type,
                    period.link_status,
                    period.confidence_score,
                    period.data_source,
                ],
            )
            period.id = cursor.lastrowid

        # Check if an inferred encumbrance already exists (idempotency)
        existing_enc = self.conn.execute(
            "SELECT id FROM encumbrances WHERE folio = ? AND is_inferred = 1",
            [folio],
        ).fetchone()

        if existing_enc:
            logger.info(
                f"Inferred encumbrance already exists for {folio} (id={existing_enc[0]}), skipping"
            )
            enc = Encumbrance(
                id=existing_enc[0],
                chain_period_id=period.id,
                encumbrance_type=enc_type,
                creditor=plaintiff,
                debtor=defendant,
                amount=original_amount,
                recording_date=recording_date,
                instrument=instrument_number,
                book=recording_book,
                page=recording_page,
            )
        else:
            enc = Encumbrance(
                chain_period_id=period.id,
                encumbrance_type=enc_type,
                creditor=plaintiff,
                debtor=defendant,
                amount=original_amount,
                amount_confidence="judgment_inferred",
                recording_date=recording_date,
                instrument=instrument_number,
                book=recording_book,
                page=recording_page,
            )
            cursor = self.conn.execute(
                """
                INSERT INTO encumbrances (
                    folio, chain_period_id, encumbrance_type, creditor, debtor,
                    amount, amount_confidence, recording_date, instrument,
                    book, page, is_inferred
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
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
                ],
            )
            enc.id = cursor.lastrowid
            logger.info(
                f"Created inferred {enc_type} encumbrance for {folio}: "
                f"creditor={plaintiff}, amount={original_amount}, id={enc.id}"
            )

        self.conn.commit()

        return ChainResult(
            folio=folio,
            periods=[period],
            encumbrances=[enc],
            total_years=0.0,
            is_complete=False,
            mrta_status="incomplete",
            issues=["Judgment-inferred: ORI found no encumbrance-type documents"],
        )
