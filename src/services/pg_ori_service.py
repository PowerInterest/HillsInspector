"""Phase B Step 3: ORI document search -> PG ori_encumbrances.

Searches the Official Records Index for encumbrance documents (mortgages,
liens, judgments, lis pendens, satisfactions) related to each active
foreclosure, then writes them to PG ``ori_encumbrances``.

Uses ORIApiScraper for the actual API/Playwright searches, and applies
the same document-type classification logic as the SQLite chain builder.

Multi-pass iterative discovery (modeled on step4v2/discovery.py):
  Pass 0  -- legal description search (seed)
  Pass 1+ -- expand from found documents:
             * referenced instruments (CLK #NNNNN, INST #NNNNN)
             * party name searches (grantor/grantee with date bounds)
             * book/page lookups
"""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import text

from src.db.type_normalizer import (
    CANONICAL_ENCUMBRANCE_TYPES,
    CANONICAL_SATISFACTION_TYPES,
    normalize_encumbrance_type,
    normalize_document_type,
)
from sunbiz.db import get_engine, resolve_pg_dsn
from datetime import UTC

# Valid PG encumbrance_type_enum values
_PG_ENCUMBRANCE_TYPES = frozenset({
    "mortgage", "judgment", "lis_pendens", "lien", "easement",
    "satisfaction", "release", "assignment", "other",
})

_PAREN_RE = re.compile(r"\(([^)]+)\)\s*(.*)")

# Generic party names that produce too many ORI results to be useful.
# Loaded lazily from config/generic_names.txt in _load_generic_names().
_GENERIC_NAMES: set[str] | None = None

# Instrument reference patterns (same as step4v2/discovery.py)
_INST_REF_PATTERNS = [
    re.compile(r"CLK\s*#?\s*(\d{7,10})", re.IGNORECASE),
    re.compile(r"INST(?:RUMENT)?\s*(?:#|NO\.?)?\s*(\d{7,10})", re.IGNORECASE),
    re.compile(r"O\.?R\.?\s+(\d{7,10})", re.IGNORECASE),
]

# Max iterations for iterative discovery per property
_MAX_ITERATIONS = 10
# Max total documents (across all passes) before we stop expanding
_MAX_DOCUMENTS = 500


def _load_generic_names() -> set[str]:
    """Load generic party names from config file (cached)."""
    global _GENERIC_NAMES
    if _GENERIC_NAMES is not None:
        return _GENERIC_NAMES

    _GENERIC_NAMES = set()
    generic_path = Path(__file__).parent.parent.parent / "config" / "generic_names.txt"
    if generic_path.exists():
        with open(generic_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    _GENERIC_NAMES.add(line.upper())
    return _GENERIC_NAMES


def _is_generic_name(name: str) -> bool:
    """Check if a party name is too generic for ORI search."""
    if not name or len(name) < 3:
        return True
    generic = _load_generic_names()
    name_upper = name.upper()
    # Exact match
    if name_upper in generic:
        return True
    # Partial match (e.g. "WELLS FARGO" in "WELLS FARGO BANK NA")
    return any(g in name_upper for g in generic)


def _get_instrument(doc: dict) -> str:
    """Extract instrument number from an ORI doc dict."""
    return str(
        doc.get("Instrument")
        or doc.get("instrument_number")
        or doc.get("instrument")
        or ""
    ).strip()


def _get_parties(doc: dict) -> tuple[list[str], list[str]]:
    """Extract party1 list and party2 list from an ORI doc dict.

    Returns (grantors, grantees) as flat string lists.
    """
    # PartiesOne / PartiesTwo come from the ORI Search API as lists
    parties_one = doc.get("PartiesOne") or []
    parties_two = doc.get("PartiesTwo") or []

    grantors: list[str] = []
    grantees: list[str] = []

    # From PartiesOne (list of names or dicts)
    for p in parties_one:
        name = p.get("Name", "") if isinstance(p, dict) else str(p)
        name = name.strip()
        if name:
            grantors.append(name)

    # From PartiesTwo
    for p in parties_two:
        name = p.get("Name", "") if isinstance(p, dict) else str(p)
        name = name.strip()
        if name:
            grantees.append(name)

    # Fallback: party1 / party2 flat strings
    if not grantors:
        p1 = (doc.get("party1") or "").strip()
        if p1:
            grantors = [p1]
    if not grantees:
        p2 = (doc.get("party2") or "").strip()
        if p2:
            grantees = [p2]

    # Fallback: instrument/book_page search format (single name + person_type)
    if not grantors and not grantees:
        name = (doc.get("name") or "").strip()
        person_type = doc.get("person_type") or doc.get("PersonType") or ""
        if name:
            if person_type == "2":
                grantees = [name]
            else:
                grantors = [name]

    return grantors, grantees


def _extract_instrument_references(doc: dict) -> list[str]:
    """Extract instrument number references from document fields.

    Mirrors step4v2/discovery.py::_extract_instrument_references.
    """
    refs: set[str] = set()

    fields_to_check = [
        doc.get("Legal") or doc.get("legal_description") or doc.get("legal") or "",
        doc.get("Comments") or doc.get("comments") or "",
        doc.get("party1") or "",
        doc.get("party2") or "",
    ]
    for p in doc.get("PartiesOne") or []:
        fields_to_check.append(str(p) if not isinstance(p, str) else p)
    for p in doc.get("PartiesTwo") or []:
        fields_to_check.append(str(p) if not isinstance(p, str) else p)

    combined = " ".join(str(f) for f in fields_to_check if f)

    for pattern in _INST_REF_PATTERNS:
        refs.update(m.group(1) for m in pattern.finditer(combined))

    # Exclude own instrument
    own = _get_instrument(doc)
    refs.discard(own)
    return list(refs)


def _format_mm_dd_yyyy(iso_date: str | None) -> str | None:
    """Convert YYYY-MM-DD to MM/DD/YYYY for ORI API."""
    if not iso_date:
        return None
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", iso_date)
    if m:
        return f"{m.group(2)}/{m.group(3)}/{m.group(1)}"
    return None


# ------------------------------------------------------------------
# Search queue item
# ------------------------------------------------------------------

@dataclass
class _SearchItem:
    """In-memory search queue item for iterative discovery."""

    search_type: str  # "legal", "party", "instrument", "book_page", "case"
    term: str
    date_from: str | None = None  # MM/DD/YYYY
    date_to: str | None = None  # MM/DD/YYYY
    priority: int = 50  # lower = higher priority
    source_instrument: str | None = None  # which doc triggered this


@dataclass
class _DiscoveryState:
    """Per-property discovery state kept in memory."""

    seen_instruments: set[str] = field(default_factory=set)
    seen_search_keys: set[str] = field(default_factory=set)
    all_docs: list[dict] = field(default_factory=list)
    queue: list[_SearchItem] = field(default_factory=list)
    iteration: int = 0

    def enqueue(self, item: _SearchItem) -> bool:
        """Add item to queue if not already searched. Returns True if added."""
        key = f"{item.search_type}|{item.term.upper()}"
        if key in self.seen_search_keys:
            return False
        self.seen_search_keys.add(key)
        self.queue.append(item)
        return True

    def pop_next(self) -> _SearchItem | None:
        """Pop the highest-priority (lowest number) item from queue."""
        if not self.queue:
            return None
        # Sort by priority (stable), pop first
        self.queue.sort(key=lambda s: s.priority)
        return self.queue.pop(0)

    def add_doc(self, doc: dict) -> bool:
        """Add a document if its instrument hasn't been seen. Returns True if new."""
        inst = _get_instrument(doc)
        if not inst or inst in self.seen_instruments:
            return False
        self.seen_instruments.add(inst)
        self.all_docs.append(doc)
        return True


class PgOriService:
    """Search ORI for encumbrances, write to PG ori_encumbrances."""

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = resolve_pg_dsn(dsn)
        self.engine = get_engine(self.dsn)

    def run(self, *, limit: int | None = None) -> dict[str, Any]:
        """Find foreclosures needing ORI search, run searches, save to PG."""
        targets = self._find_targets(limit)
        if not targets:
            return {"skipped": True, "reason": "no_foreclosures_need_ori"}

        logger.info(f"ORI search: {len(targets)} foreclosures to process")
        return asyncio.run(self._search_all(targets))

    # ------------------------------------------------------------------
    # Target selection
    # ------------------------------------------------------------------

    def _find_targets(self, limit: int | None) -> list[dict[str, Any]]:
        """Find foreclosures needing ORI search."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT f.foreclosure_id, f.case_number_raw, f.strap, f.folio,
                           f.judgment_data,
                           bp.raw_legal1, bp.raw_legal2, bp.raw_legal3, bp.raw_legal4,
                           bp.owner_name
                    FROM foreclosures f
                    LEFT JOIN hcpa_bulk_parcels bp ON f.strap = bp.strap
                    WHERE f.step_ori_searched IS NULL
                      AND f.archived_at IS NULL
                      AND f.strap IS NOT NULL
                    ORDER BY f.auction_date
                    LIMIT :limit
                """),
                {"limit": limit or 1000},
            ).fetchall()

        targets = []
        for r in rows:
            jdata = r[4] or {}
            if isinstance(jdata, str):
                try:
                    jdata = json.loads(jdata)
                except (json.JSONDecodeError, TypeError):
                    jdata = {}

            targets.append({
                "foreclosure_id": r[0],
                "case_number": r[1],
                "strap": r[2],
                "folio": r[3],
                "judgment_data": jdata,
                "legal1": r[5] or "",
                "legal2": r[6] or "",
                "legal3": r[7] or "",
                "legal4": r[8] or "",
                "owner_name": r[9] or "",
            })

        return targets

    # ------------------------------------------------------------------
    # Search orchestration
    # ------------------------------------------------------------------

    async def _search_all(self, targets: list[dict]) -> dict[str, Any]:
        from src.scrapers.ori_api_scraper import ORIApiScraper

        scraper = ORIApiScraper()
        total_docs = 0
        total_saved = 0
        errors = 0

        for i, target in enumerate(targets):
            fid = target["foreclosure_id"]
            case = target["case_number"]
            strap = target["strap"]
            folio = target["folio"]

            logger.info(
                f"[{i+1}/{len(targets)}] ORI search for {case} "
                f"(strap={strap})"
            )

            try:
                docs, iterations = self._iterative_search(scraper, target)
                total_docs += len(docs)

                logger.info(
                    f"  Iterative discovery: {iterations} iterations, "
                    f"{len(docs)} unique documents"
                )

                # Save encumbrance-type documents to PG
                saved = self._save_documents(strap, folio, docs)
                total_saved += saved

                # If no encumbrances found, try judgment-inferred
                if saved == 0:
                    inferred = self._infer_from_judgment(
                        strap, folio, target
                    )
                    total_saved += inferred

                self._mark_searched(fid)

            except Exception as exc:
                logger.error(f"ORI search error for {case}: {exc}")
                errors += 1

        return {
            "targets": len(targets),
            "total_documents_found": total_docs,
            "encumbrances_saved": total_saved,
            "errors": errors,
        }

    # ------------------------------------------------------------------
    # Iterative discovery
    # ------------------------------------------------------------------

    def _iterative_search(
        self,
        scraper: Any,
        target: dict,
        *,
        max_iterations: int = _MAX_ITERATIONS,
    ) -> tuple[list[dict], int]:
        """Multi-pass iterative discovery for a single property.

        1. Seed the queue with legal-description search terms.
        2. Execute searches, collect new documents.
        3. For each new document, extract vectors (instruments, parties,
           book/page) and enqueue them.
        4. Repeat until the queue is empty or max_iterations reached.

        Returns (all_docs, iterations_used).
        """
        state = _DiscoveryState()
        case = target.get("case_number", "")

        # --- Seed: legal description searches (priority 10) ---
        search_terms = self._build_search_terms(target)
        if not search_terms:
            logger.warning(f"No search terms for {case}")
            return [], 0

        for term in search_terms:
            state.enqueue(_SearchItem(
                search_type="legal",
                term=term,
                priority=10,
            ))

        # --- Seed: case number search (priority 15) ---
        if case:
            state.enqueue(_SearchItem(
                search_type="case",
                term=case,
                priority=15,
            ))

        # --- Seed: judgment party names (priority 20) ---
        jdata = target.get("judgment_data") or {}
        plaintiff = (jdata.get("plaintiff") or "").strip()
        defendant = (jdata.get("defendant") or "").strip()
        if plaintiff and not _is_generic_name(plaintiff):
            state.enqueue(_SearchItem(
                search_type="party",
                term=plaintiff,
                priority=20,
            ))
        if defendant and not _is_generic_name(defendant):
            state.enqueue(_SearchItem(
                search_type="party",
                term=defendant,
                priority=20,
            ))

        # --- Main loop ---
        while state.iteration < max_iterations:
            item = state.pop_next()
            if item is None:
                logger.debug(f"  Queue exhausted after {state.iteration} iterations")
                break

            state.iteration += 1

            if len(state.all_docs) >= _MAX_DOCUMENTS:
                logger.warning(
                    f"  Max documents ({_MAX_DOCUMENTS}) reached for {case}"
                )
                break

            new_count = self._execute_search_item(scraper, state, item)
            logger.debug(
                f"  Iter {state.iteration}: {item.search_type} "
                f"'{item.term[:60]}' -> {new_count} new docs "
                f"(total={len(state.all_docs)}, queue={len(state.queue)})"
            )

        return state.all_docs, state.iteration

    def _execute_search_item(
        self,
        scraper: Any,
        state: _DiscoveryState,
        item: _SearchItem,
    ) -> int:
        """Execute one search and process results. Returns count of new docs."""
        docs = self._run_search(scraper, item)
        if not docs:
            return 0

        new_count = 0
        for doc in docs:
            if state.add_doc(doc):
                new_count += 1
                # Extract new search vectors from this document
                self._extract_vectors(state, doc, item)

        return new_count

    def _run_search(self, scraper: Any, item: _SearchItem) -> list[dict]:
        """Dispatch a search to the appropriate ORIApiScraper method."""
        try:
            if item.search_type == "legal":
                return scraper.search_by_legal(
                    item.term,
                    start_date=item.date_from or "01/01/1900",
                    end_date=item.date_to,
                )

            if item.search_type == "party":
                return scraper.search_by_party(
                    item.term,
                    start_date=item.date_from or "01/01/1900",
                    end_date=item.date_to,
                )

            if item.search_type == "instrument":
                return scraper.search_by_instrument(item.term)

            if item.search_type == "case":
                return scraper.search_by_case_number(item.term)

            if item.search_type == "book_page":
                parts = item.term.split("/")
                if len(parts) == 2:
                    return scraper.search_by_book_page_sync(parts[0], parts[1])
                logger.warning(f"Invalid book_page format: {item.term}")
                return []

            logger.warning(f"Unknown search type: {item.search_type}")
            return []

        except Exception as exc:
            logger.warning(
                f"Search failed ({item.search_type} '{item.term}'): {exc}"
            )
            return []

    def _extract_vectors(
        self,
        state: _DiscoveryState,
        doc: dict,
        source: _SearchItem,
    ) -> None:
        """Extract new search vectors from a found document and enqueue them.

        Vectors extracted:
        1. Referenced instruments (from legal desc / party fields)
        2. Party names (grantor/grantee) with date bounds
        3. Book/page references
        """
        own_instrument = _get_instrument(doc)
        recording_date = self._parse_date(
            doc.get("RecordDate") or doc.get("record_date")
        )

        # --- 1. Referenced instruments (priority 25) ---
        for ref_inst in _extract_instrument_references(doc):
            state.enqueue(_SearchItem(
                search_type="instrument",
                term=ref_inst,
                priority=25,
                source_instrument=own_instrument,
            ))

        # --- 2. Party name searches (priority 30) ---
        grantors, grantees = _get_parties(doc)

        for name in grantors:
            if _is_generic_name(name):
                continue
            # Grantor owned *before* this recording date
            date_to = _format_mm_dd_yyyy(recording_date)
            state.enqueue(_SearchItem(
                search_type="party",
                term=name,
                date_to=date_to,
                priority=30,
                source_instrument=own_instrument,
            ))

        for name in grantees:
            if _is_generic_name(name):
                continue
            # Grantee owned *after* this recording date
            date_from = _format_mm_dd_yyyy(recording_date)
            state.enqueue(_SearchItem(
                search_type="party",
                term=name,
                date_from=date_from,
                priority=30,
                source_instrument=own_instrument,
            ))

        # --- 3. Book/page references (priority 35) ---
        book = (
            doc.get("Book") or doc.get("book") or doc.get("book_num") or ""
        ).strip()
        page = (
            doc.get("Page") or doc.get("page") or doc.get("page_num") or ""
        ).strip()
        if book and page:
            state.enqueue(_SearchItem(
                search_type="book_page",
                term=f"{book}/{page}",
                priority=35,
                source_instrument=own_instrument,
            ))

    # ------------------------------------------------------------------
    # Search term generation
    # ------------------------------------------------------------------

    def _build_search_terms(self, target: dict) -> list[str]:
        """Generate ORI search terms from legal description fields."""
        terms: list[str] = []
        seen: set[str] = set()

        # From bulk parcels legal description
        for field_name in ("legal1", "legal2", "legal3", "legal4"):
            val = (target.get(field_name) or "").strip()
            if val and len(val) >= 5 and val.upper() not in seen:
                # Take first part (usually subdivision name)
                # e.g. "TOWN N COUNTRY PARK UNIT 7 LOT 3 BLOCK 2"
                # -> search for "TOWN N COUNTRY PARK UNIT 7"
                parts = val.split()
                # Use first 5-8 words as search term
                search = " ".join(parts[:min(8, len(parts))])
                if search.upper() not in seen:
                    seen.add(search.upper())
                    terms.append(search)

        # From judgment data legal description
        jdata = target.get("judgment_data") or {}
        legal_desc = jdata.get("legal_description", "")
        if legal_desc and len(legal_desc) >= 10:
            # Extract key phrases (subdivision, lot, block)
            first_line = legal_desc.split("\n")[0].strip()
            if first_line and first_line.upper() not in seen:
                search = " ".join(first_line.split()[:8])
                if search.upper() not in seen:
                    seen.add(search.upper())
                    terms.append(search)

        return terms[:5]  # Cap at 5 search terms

    # ------------------------------------------------------------------
    # Document classification and saving
    # ------------------------------------------------------------------

    def _save_documents(
        self,
        strap: str,
        folio: str | None,
        documents: list[dict],
    ) -> int:
        """Classify ORI documents and save encumbrances to PG."""
        saved = 0
        with self.engine.begin() as conn:
            for doc in documents:
                raw_type = (
                    doc.get("DocType")
                    or doc.get("document_type")
                    or doc.get("doc_type")
                    or ""
                )
                canonical = normalize_document_type(raw_type)
                enc_type = normalize_encumbrance_type(canonical or raw_type)

                # Only save encumbrance-type, satisfaction-type, and
                # assignment-type documents (skip deeds, NOCs, affidavits, etc.)
                is_encumbrance = canonical in CANONICAL_ENCUMBRANCE_TYPES
                is_satisfaction = canonical in CANONICAL_SATISFACTION_TYPES
                is_assignment = enc_type == "assignment"
                if not (is_encumbrance or is_satisfaction or is_assignment):
                    continue

                if enc_type not in _PG_ENCUMBRANCE_TYPES:
                    enc_type = "other"

                instrument = str(
                    doc.get("Instrument")
                    or doc.get("instrument_number")
                    or doc.get("instrument")
                    or ""
                ).strip()
                if not instrument:
                    continue

                # Parse parties
                party1 = doc.get("party1") or ""
                party2 = doc.get("party2") or ""
                parties_one = doc.get("PartiesOne") or []
                parties_two = doc.get("PartiesTwo") or []
                if not party1 and parties_one:
                    party1 = ", ".join(
                        (p.get("Name", "") if isinstance(p, dict) else str(p))
                        for p in parties_one
                        if p
                    )
                if not party2 and parties_two:
                    party2 = ", ".join(
                        (p.get("Name", "") if isinstance(p, dict) else str(p))
                        for p in parties_two
                        if p
                    )

                # Fields are pre-normalized by ORIApiScraper._normalize_result()
                recording_date = self._parse_date(
                    doc.get("RecordDate") or doc.get("record_date")
                )
                book = (
                    doc.get("Book") or doc.get("book") or ""
                ).strip() or None
                page = (
                    doc.get("Page") or doc.get("page") or ""
                ).strip() or None
                book_type = str(
                    doc.get("BookType") or doc.get("book_type") or "OR"
                ).strip()
                amount = doc.get("SalesPrice") or doc.get("sales_price")
                case_number = doc.get("CaseNum") or doc.get("case_number")
                legal = (
                    doc.get("Legal")
                    or doc.get("legal_description")
                    or doc.get("legal")
                )
                ori_uuid = doc.get("UUID") or doc.get("ori_uuid")
                # Pre-truncated to 64 chars by _normalize_result()
                ori_id = doc.get("ID") or doc.get("ori_id")

                # Determine if satisfaction
                is_sat = canonical in CANONICAL_SATISFACTION_TYPES

                conn.execute(text("SAVEPOINT ori_doc"))
                try:
                    conn.execute(
                        text("""
                            INSERT INTO ori_encumbrances (
                                folio, strap, instrument_number,
                                book, page, book_type,
                                ori_uuid, ori_id,
                                raw_document_type,
                                encumbrance_type,
                                party1, party2,
                                parties_one_json, parties_two_json,
                                amount, recording_date,
                                case_number, legal_description,
                                is_satisfied,
                                discovered_at, updated_at
                            ) VALUES (
                                :folio, :strap, :instrument,
                                :book, :page, :book_type,
                                :ori_uuid, :ori_id,
                                :raw_type,
                                CAST(:enc_type AS encumbrance_type_enum),
                                :party1, :party2,
                                CAST(:p1_json AS JSONB),
                                CAST(:p2_json AS JSONB),
                                :amount,
                                CAST(NULLIF(:rec_date, '') AS DATE),
                                :case_number, :legal,
                                :is_sat,
                                now(), now()
                            )
                            ON CONFLICT (folio, COALESCE(instrument_number, ''),
                                         COALESCE(book, ''), COALESCE(page, ''),
                                         COALESCE(book_type, 'OR'))
                            DO UPDATE SET
                                party1 = COALESCE(EXCLUDED.party1, ori_encumbrances.party1),
                                party2 = COALESCE(EXCLUDED.party2, ori_encumbrances.party2),
                                amount = COALESCE(EXCLUDED.amount, ori_encumbrances.amount),
                                updated_at = now()
                        """),
                        {
                            "folio": folio,
                            "strap": strap,
                            "instrument": instrument,
                            "book": book,
                            "page": page,
                            "book_type": book_type or "OR",
                            "ori_uuid": ori_uuid,
                            "ori_id": str(ori_id) if ori_id else None,
                            "raw_type": raw_type,
                            "enc_type": enc_type,
                            "party1": party1 or None,
                            "party2": party2 or None,
                            "p1_json": (
                                json.dumps(parties_one)
                                if parties_one else None
                            ),
                            "p2_json": (
                                json.dumps(parties_two)
                                if parties_two else None
                            ),
                            "amount": float(amount) if amount else None,
                            "rec_date": recording_date or "",
                            "case_number": case_number,
                            "legal": legal,
                            "is_sat": is_sat,
                        },
                    )
                    conn.execute(text("RELEASE SAVEPOINT ori_doc"))
                    saved += 1
                except Exception as exc:
                    conn.execute(text("ROLLBACK TO SAVEPOINT ori_doc"))
                    logger.warning(
                        f"Skip document {instrument}: {exc}"
                    )

        return saved

    def _infer_from_judgment(
        self,
        strap: str,
        folio: str | None,
        target: dict,
    ) -> int:
        """Create inferred encumbrance from judgment data when ORI finds nothing."""
        jdata = target.get("judgment_data") or {}
        plaintiff = jdata.get("plaintiff") or ""
        defendant = jdata.get("defendant") or ""
        if not plaintiff:
            return 0

        case_number = target.get("case_number", "")
        is_cc = len(case_number) >= 8 and "CC" in case_number[6:8]
        plaintiff_upper = plaintiff.upper()
        is_hoa = any(
            kw in plaintiff_upper
            for kw in ("ASSOCIATION", "HOA", "CONDO", "HOMEOWNER")
        )
        enc_type = "lien" if is_cc or is_hoa else "mortgage"

        # Extract amount from judgment
        foreclosed = jdata.get("foreclosed_mortgage") or {}
        amount = foreclosed.get("original_amount") or jdata.get("judgment_amount")
        if amount:
            try:
                amount = float(amount)
            except (ValueError, TypeError):
                amount = None

        recording_date = foreclosed.get("recording_date") or foreclosed.get("original_date")
        instrument = f"INFERRED-{case_number}"

        with self.engine.begin() as conn:
            # Check idempotency
            existing = conn.execute(
                text(
                    "SELECT id FROM ori_encumbrances "
                    "WHERE strap = :strap AND instrument_number = :inst"
                ),
                {"strap": strap, "inst": instrument},
            ).fetchone()

            if existing:
                return 0

            conn.execute(
                text("""
                    INSERT INTO ori_encumbrances (
                        folio, strap, instrument_number, book_type,
                        encumbrance_type, party1, party2,
                        amount, recording_date, case_number,
                        discovered_at, updated_at
                    ) VALUES (
                        :folio, :strap, :instrument, 'OR',
                        CAST(:enc_type AS encumbrance_type_enum),
                        :party1, :party2,
                        :amount,
                        CAST(NULLIF(:rec_date, '') AS DATE),
                        :case_number,
                        now(), now()
                    )
                """),
                {
                    "folio": folio,
                    "strap": strap,
                    "instrument": instrument,
                    "enc_type": enc_type,
                    "party1": plaintiff,
                    "party2": defendant,
                    "amount": amount,
                    "rec_date": recording_date or "",
                    "case_number": case_number,
                },
            )

        logger.info(
            f"Inferred {enc_type} encumbrance for {case_number}: "
            f"plaintiff={plaintiff}"
        )
        return 1

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _mark_searched(self, foreclosure_id: int) -> None:
        """Mark foreclosure as ORI-searched."""
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE foreclosures SET step_ori_searched = now() "
                    "WHERE foreclosure_id = :fid"
                ),
                {"fid": foreclosure_id},
            )

    @staticmethod
    def _parse_date(val: Any) -> str | None:
        """Parse ORI date formats to YYYY-MM-DD string."""
        if not val:
            return None
        # Handle Unix timestamp (integer or numeric string)
        if isinstance(val, (int, float)):
            from datetime import datetime
            try:
                dt = datetime.fromtimestamp(val, tz=UTC)
                return dt.strftime("%Y-%m-%d")
            except (ValueError, OSError, OverflowError):
                return None
        s = str(val).strip()
        if not s:
            return None
        # Handle numeric string (Unix timestamp)
        if s.isdigit() and len(s) >= 9:
            from datetime import datetime
            try:
                dt = datetime.fromtimestamp(int(s), tz=UTC)
                return dt.strftime("%Y-%m-%d")
            except (ValueError, OSError, OverflowError):
                pass
        # Handle "MM/DD/YYYY" format
        if "/" in s:
            parts = s.split("/")
            if len(parts) == 3:
                try:
                    return f"{parts[2]}-{parts[0]:>02}-{parts[1]:>02}"
                except (ValueError, IndexError):
                    pass
        # Handle ISO format
        if re.match(r"\d{4}-\d{2}-\d{2}", s):
            return s[:10]
        return None
