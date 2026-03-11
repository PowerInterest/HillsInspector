"""Phase B Step 3: ORI document search -> PG ori_encumbrances.

PG-first, phased discovery for encumbrances:
1. Case-number anchors + deed-chain seeds from ``hcpa_allsales``.
2. Adjacent instrument expansion + reference chase (CLK/INST/book-page).
3. Guarded fallback (clerk case seeds + targeted legal/address/party search).
4. Targeted live NOC fallback for recent permit-backed properties that still
   have no discovered Notice of Commencement after the normal passes.
5. Lis pendens recovery for judged active cases that still have no persisted
   LP, including case-only retries when parcel identity is missing.

Most ORI calls use the Hyland PAV ``CustomQuery/KeywordSearch`` API with
truncation splitting for bounded date-window searches. A narrower full-text
probe is reserved for exact-address NOC lookups when the local seed data and
standard live passes still leave a recent permit-backed property without a NOC.

``_save_documents()`` persists encumbrances, satisfactions, assignments, and
NOCs (Notices of Commencement). NOCs are stored with encumbrance_type='noc'
but excluded from survival analysis and lien counts downstream. Discovery keeps
NOCs through the final filter and requires property-text evidence
(legal/address), so owner-only NOC matches do not pollute unrelated parcels.
See docs/NOC_PERMIT_LINKING.md and docs/external/HYLAND_PAV_NOC_DISCOVERY.md.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from loguru import logger
from sqlalchemy import text

from src.services.pav_cache import pav_cache_get, pav_cache_put
from src.utils.legal_description import combine_legal_fields
from src.utils.legal_description import legal_descriptions_match
from src.utils.legal_description import parse_legal_description

from src.db.type_normalizer import (
    CANONICAL_ENCUMBRANCE_TYPES,
    CANONICAL_LIFECYCLE_TYPES,
    CANONICAL_NOC_TYPES,
    CANONICAL_SATISFACTION_TYPES,
    normalize_encumbrance_type,
    normalize_document_type,
)
from sunbiz.db import get_engine, resolve_pg_dsn

# Valid PG encumbrance_type_enum values
_PG_ENCUMBRANCE_TYPES = frozenset({
    "mortgage",
    "judgment",
    "lis_pendens",
    "lien",
    "easement",
    "satisfaction",
    "release",
    "assignment",
    "noc",
    "other",
})

_PAREN_RE = re.compile(r"\(([^)]+)\)\s*(.*)")

# Generic party names that produce too many ORI results to be useful.
# Loaded lazily from config/generic_names.txt in _load_generic_names().
_GENERIC_NAMES: set[str] | None = None

# Instrument reference patterns for cross-document reference chasing.
_INST_REF_PATTERNS = [
    re.compile(r"CLK\s*#?\s*(\d{7,10})", re.IGNORECASE),
    re.compile(r"INST(?:RUMENT)?\s*(?:#|NO\.?)?\s*(\d{7,10})", re.IGNORECASE),
    re.compile(r"O\.?R\.?\s+(\d{7,10})", re.IGNORECASE),
]
_BKPG_REF_PATTERN = re.compile(
    r"(?:OR|O\.?R\.?)\s*(?:BK|BOOK)\s*(\d+)\s*(?:PG|PAGE)\s*(\d+)",
    re.IGNORECASE,
)
_CASE_VARIANT_RE = re.compile(r"^\d{2}(\d{4})([A-Z]{2})(\d{6}).*$")
_PARCEL_SEGMENT_RE = re.compile(
    r"^(?:U-)?(\d{2})-(\d{2})-(\d{2})-([A-Z0-9]+)-(\d+)-(\d+)\.(\d)$"
)
_ADDRESS_LINE_RE = re.compile(r"\b\d{3,6}[A-Z]?(?:\s+[A-Z0-9]+){1,4}\b", re.IGNORECASE)
_LEGAL_LOCATOR_RE = re.compile(
    r"\b(LOT|UNIT|BLOCK|BLK)\s*(?:NO\.?\s*)?([A-Z0-9]+)\b",
    re.IGNORECASE,
)
_TARGET_LEGAL_EXPR = (
    "UPPER(COALESCE(raw_legal1, '') || ' ' || COALESCE(raw_legal2, '') || "
    "' ' || COALESCE(raw_legal3, '') || ' ' || COALESCE(raw_legal4, ''))"
)
_TARGET_MAX_LEGAL_CANDIDATES = 40
_PLACEHOLDER_PARCEL_IDS = frozenset({
    "STRING OR NULL",
    "N/A",
    "NONE",
    "UNKNOWN",
    "MULTIPLE PARCEL",
})
_GENERIC_SUBDIVISION_TERMS = frozenset({
    "THE",
    "OF",
    "IN",
    "AT",
    "A",
    "AN",
    "AND",
    "OR",
    "LOT",
    "BLOCK",
    "UNIT",
    "PHASE",
    "SECTION",
    "SUBDIVISION",
    "SUBDIV",
    "PLAT",
    "BOOK",
    "PAGE",
})
_STREET_STOP_TOKENS = frozenset({
    "ALLEY",
    "AVE",
    "AVENUE",
    "BLVD",
    "BOULEVARD",
    "CIR",
    "CIRCLE",
    "COURT",
    "CT",
    "DR",
    "DRIVE",
    "HWY",
    "LANE",
    "LN",
    "PKWY",
    "PLACE",
    "PL",
    "RD",
    "ROAD",
    "ST",
    "STREET",
    "TER",
    "TERRACE",
    "TRAIL",
    "TRL",
    "WAY",
})

# PAV CustomQuery endpoint
_PAV_KEYWORD_URL = "https://publicaccess.hillsclerk.com/PAVDirectSearch/api/CustomQuery/KeywordSearch"
_PAV_FULL_TEXT_URL = "https://publicaccess.hillsclerk.com/PAVDirectSearch/api/DocumentType/FullTextSearch"
_PAV_HEADERS = {
    "Content-Type": "application/json",
    "Origin": "https://publicaccess.hillsclerk.com",
    "Referer": "https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html",
}
FORECLOSURE_DATA_DIR = Path("data/Foreclosure")
_CASE_ONLY_ORI_STAGE_FILENAME = "case_only_unresolved_documents.json"
_CASE_ONLY_LP_STAGE_FILENAME = "case_only_unresolved_lis_pendens_docs.json"
_PAV_QUERY_LIMIT = 500
_PAV_MAX_RETRIES = 3
_PAV_SPLIT_DEPTH = 6
_PAV_TIMEOUT_SECONDS = 30
_PAV_FULL_TEXT_TIMEOUT_SECONDS = 60
_PAV_FULL_TEXT_RETRIES = 1

_PAV_NOC_DOC_TYPE = "(NOC) NOTICE OF COMMENCEMENT"
_PAV_NOC_DOC_TYPE_ID = 1138
_RECENT_PERMIT_FALLBACK_YEARS = 5

# Query caps to keep per-property execution bounded.
_MAX_DEEDS_TO_SCAN = 20
_MAX_ADJACENT_SEARCHES = 120
_MAX_REFERENCE_CHASE = 120
_MAX_CLERK_CASE_SEEDS = 3
_MIN_DOCS_FOR_NO_FALLBACK = 5

# Max iterations for iterative discovery per property
_MAX_ITERATIONS = 10
# Max total documents (across all passes) before we stop expanding
_MAX_DOCUMENTS = 500
_MAX_OFFICIAL_RECORDS_CANDIDATES = 400
_MIN_OFFICIAL_MATCH_SCORE = 4
_MAX_SAT_PARENT_CHASE = 20  # instrument lookups per property for SAT parent chase

# Generic legal description boilerplate — too common to be discriminating tokens.
# These appear in thousands of condo/HOA/subdivision legal descriptions county-wide.
_LEGAL_BOILERPLATE_TOKENS = frozenset({
    "CONDOMINIUM",
    "CONDO",
    "CONDOMINIUMS",
    "ASSOCIATION",
    "ASSOC",
    "UNIT",
    "UNITS",
    "BLOCK",
    "BLK",
    "COMMON",
    "ELEMENTS",
    "UNDIV",
    "UNDIVIDED",
    "AND",
    "THE",
    "FOR",
    "INT",
    "INC",
    "INTEREST",
    "TOGETHER",
    "WITH",
    "PLAT",
    "BOOK",
    "PAGE",
    "TRACT",
    "PHASE",
    "ADDITION",
    "REPLAT",
    "SUBDIVISION",
    "SUBD",
    "SUB",
    "SECTION",
    "SEC",
    "TOWNSHIP",
    "RANGE",
    "COUNTY",
    "HILLSBOROUGH",
    "FLORIDA",
    "LIEN",
    "MORTGAGE",
    "SATISFACTION",
    "PARCEL",
})


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
    """Check if a party name is too generic for ORI search.

    Uses word-boundary matching so that short generic terms like "INC" only
    match when they appear as whole words (e.g. "ACME INC") and NOT as
    substrings of longer words (e.g. "SINCLAIR").
    """
    if not name or len(name) < 3:
        return True
    generic = _load_generic_names()
    name_upper = name.upper()
    # Exact match
    if name_upper in generic:
        return True
    # Word-boundary match (e.g. "WELLS FARGO" matches "WELLS FARGO BANK NA"
    # but "INC" does NOT match "SINCLAIR")
    return any(re.search(r"\b" + re.escape(g) + r"\b", name_upper) for g in generic)


def _get_instrument(doc: dict) -> str:
    """Extract instrument number from an ORI doc dict."""
    return str(doc.get("Instrument") or doc.get("instrument_number") or doc.get("instrument") or "").strip()


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
    """Extract instrument number references from document fields."""
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


def _is_encumbrance_type(doc: dict[str, Any]) -> bool:
    raw = doc.get("DocType") or doc.get("document_type") or doc.get("doc_type") or ""
    canonical = normalize_document_type(raw)
    return canonical in CANONICAL_ENCUMBRANCE_TYPES


def _is_assignment_type(doc: dict[str, Any]) -> bool:
    raw = doc.get("DocType") or doc.get("document_type") or doc.get("doc_type") or ""
    canonical = normalize_document_type(raw)
    enc_type = normalize_encumbrance_type(canonical or raw)
    return enc_type == "assignment"


def _is_noc_type(doc: dict[str, Any]) -> bool:
    raw = doc.get("DocType") or doc.get("document_type") or doc.get("doc_type") or ""
    canonical = normalize_document_type(raw)
    enc_type = normalize_encumbrance_type(canonical or raw)
    return canonical in CANONICAL_NOC_TYPES or enc_type == "noc"


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


@dataclass(slots=True)
class _RecoveredTargetIdentity:
    strap: str
    folio: str
    property_address: str | None
    legal1: str
    legal2: str
    legal3: str
    legal4: str
    owner_name: str | None
    legal_description: str
    reason: str


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
        self._pav_session = requests.Session()
        self._pav_session.headers.update(_PAV_HEADERS)
        self._official_noc_coverage_start_cache: date | None = None
        self._last_save_documents_stats = {
            "saved": 0,
            "skipped": 0,
            "eligible": 0,
        }
        self._last_infer_from_judgment_stats = {
            "saved": 0,
            "reason": "not_run",
        }

    def run(self, *, limit: int | None = None) -> dict[str, Any]:
        """Find foreclosures needing ORI search, run searches, save to PG."""
        targets = self._find_targets(limit)
        if not targets:
            return {"skipped": True, "reason": "no_foreclosures_need_ori"}

        logger.info(f"ORI search: {len(targets)} foreclosures to process")
        return asyncio.run(self._search_all(targets))

    def run_lis_pendens_backfill(
        self,
        *,
        limit: int | None = None,
        foreclosure_ids: list[int] | None = None,
        dry_run: bool = False,
        require_ori_searched: bool = True,
    ) -> dict[str, Any]:
        """Re-probe active foreclosure cases that still have no persisted LP."""
        before_targets = self._find_lis_pendens_gap_targets(
            limit=None,
            foreclosure_ids=foreclosure_ids,
            require_ori_searched=require_ori_searched,
        )
        targets = before_targets[:limit] if limit is not None else before_targets
        if not targets:
            return {"skipped": True, "reason": "no_lis_pendens_gap_targets"}

        total_docs = 0
        total_saved = 0
        total_inferred = 0
        errors = 0
        total_api_calls = 0
        total_retries = 0
        total_truncated = 0
        total_unresolved_truncations = 0
        targets_with_docs = 0
        per_target: list[dict[str, Any]] = []

        for index, target in enumerate(targets, start=1):
            target, recovered_identity = self._prepare_target_identity(
                target,
                persist_update=not dry_run,
            )
            fid = target["foreclosure_id"]
            case = target["case_number"]
            strap = (target.get("strap") or "").strip() or None
            folio = (target.get("folio") or "").strip() or None
            logger.info(
                "[{}/{}] LP backfill for {} (strap={})",
                index,
                len(targets),
                case,
                strap,
            )
            try:
                discovered_docs, metrics = self._discover_property(target)
                lp_docs = [
                    doc
                    for doc in discovered_docs
                    if normalize_encumbrance_type(normalize_document_type(doc.get("DocType") or "")) == "lis_pendens"
                ]
                saved = 0
                persisted_lp = self._has_persisted_lis_pendens(case, strap)
                staged_path: str | None = None
                if lp_docs and not dry_run:
                    if not self._has_persistable_identity(target):
                        staged_path = self._stage_case_only_documents(
                            target=target,
                            documents=lp_docs,
                            lp_only=True,
                        )
                    else:
                        saved = self._save_documents(strap, folio, lp_docs)
                        if saved > 0:
                            self._clear_case_only_stage_files(case)
                        persisted_lp = self._has_persisted_lis_pendens(case, strap)
                    if persisted_lp:
                        self._mark_searched(fid)

                total_docs += len(lp_docs)
                total_saved += saved
                total_api_calls += metrics["api_calls"]
                total_retries += metrics["retries"]
                total_truncated += metrics["truncated"]
                total_unresolved_truncations += metrics["unresolved_truncations"]
                if lp_docs:
                    targets_with_docs += 1
                per_target.append({
                    "foreclosure_id": fid,
                    "case_number": case,
                    "strap": strap,
                    "folio": folio,
                    "docs_found": len(lp_docs),
                    "saved": saved,
                    "persisted_lp": persisted_lp,
                    "identity_recovered": recovered_identity is not None,
                    "identity_recovery_reason": recovered_identity.reason if recovered_identity else None,
                    "case_only_stage_path": staged_path,
                    "inferred": 0,
                    "api_calls": metrics["api_calls"],
                    "retries": metrics["retries"],
                    "truncated": metrics["truncated"],
                    "unresolved_truncations": metrics["unresolved_truncations"],
                    "official_seed_docs": metrics["official_seed_docs"],
                    "deed_count": metrics["deed_count"],
                    "clerk_case_count": metrics["clerk_case_count"],
                    "instruments": [_get_instrument(doc) for doc in lp_docs if _get_instrument(doc)],
                })
            except Exception as exc:
                logger.exception(
                    "LP backfill error for case={} foreclosure_id={}",
                    case,
                    fid,
                )
                errors += 1
                per_target.append({
                    "foreclosure_id": fid,
                    "case_number": case,
                    "strap": target.get("strap"),
                    "folio": target.get("folio"),
                    "docs_found": 0,
                    "saved": 0,
                    "inferred": 0,
                    "error": str(exc),
                })

        after_remaining = len(
            self._find_lis_pendens_gap_targets(
                limit=None,
                foreclosure_ids=foreclosure_ids,
                require_ori_searched=require_ori_searched,
            )
        )
        return {
            "targets": len(targets),
            "targets_with_lp_docs": targets_with_docs,
            "total_lp_docs_found": total_docs,
            "total_saved": total_saved,
            "total_inferred": total_inferred,
            "errors": errors,
            "api_calls": total_api_calls,
            "retries": total_retries,
            "truncated": total_truncated,
            "unresolved_truncations": total_unresolved_truncations,
            "remaining_lp_gaps_before": len(before_targets),
            "remaining_lp_gaps_after": after_remaining,
            "dry_run": dry_run,
            "per_target": per_target,
        }

    def run_recent_permit_noc_backfill(
        self,
        *,
        limit: int | None = None,
        foreclosure_ids: list[int] | None = None,
        dry_run: bool = False,
        require_ori_searched: bool = True,
    ) -> dict[str, Any]:
        """Probe live PAV for NOCs on recent permit-backed active foreclosures."""
        before_targets = self._find_recent_permit_no_noc_targets(
            limit=None,
            foreclosure_ids=foreclosure_ids,
            require_ori_searched=require_ori_searched,
        )
        targets = before_targets[:limit] if limit is not None else before_targets
        if not targets:
            return {"skipped": True, "reason": "no_recent_permit_no_noc_targets"}

        total_docs = 0
        total_saved = 0
        targets_with_docs = 0
        errors = 0
        total_api_calls = 0
        total_retries = 0
        total_truncated = 0
        total_unresolved_truncations = 0
        per_target: list[dict[str, Any]] = []
        latest_date = datetime.now(tz=UTC).date()

        for i, target in enumerate(targets):
            fid = target["foreclosure_id"]
            case = target["case_number"]
            strap = target["strap"]
            folio = target["folio"]
            logger.info(
                "[{}/{}] Live NOC backfill for {} (strap={})",
                i + 1,
                len(targets),
                case,
                strap,
            )
            try:
                ownership_chain = self._get_ownership_chain(strap)
                property_tokens = self._build_property_tokens(target, ownership_chain)
                earliest_date = self._earliest_relevant_date(ownership_chain, target)
                stats = {
                    "api_calls": 0,
                    "retries": 0,
                    "truncated": 0,
                    "unresolved_truncations": 0,
                }
                docs = self._run_live_noc_fallback(
                    target=target,
                    ownership_chain=ownership_chain,
                    property_tokens=property_tokens,
                    earliest_date=earliest_date,
                    latest_date=latest_date,
                    stats=stats,
                )
                saved = 0
                if docs and not dry_run:
                    saved = self._save_documents(strap, folio, docs)

                total_docs += len(docs)
                total_saved += saved
                total_api_calls += stats["api_calls"]
                total_retries += stats["retries"]
                total_truncated += stats["truncated"]
                total_unresolved_truncations += stats["unresolved_truncations"]
                if docs:
                    targets_with_docs += 1

                per_target.append({
                    "foreclosure_id": fid,
                    "case_number": case,
                    "strap": strap,
                    "folio": folio,
                    "docs_found": len(docs),
                    "saved": saved,
                    "api_calls": stats["api_calls"],
                    "truncated": stats["truncated"],
                    "instruments": [_get_instrument(doc) for doc in docs if _get_instrument(doc)],
                })
            except Exception as exc:
                logger.exception(
                    "Live NOC backfill error for case={} strap={} foreclosure_id={}",
                    case,
                    strap,
                    fid,
                )
                errors += 1
                per_target.append({
                    "foreclosure_id": fid,
                    "case_number": case,
                    "strap": strap,
                    "folio": folio,
                    "docs_found": 0,
                    "saved": 0,
                    "error": str(exc),
                })

        after_remaining = len(
            self._find_recent_permit_no_noc_targets(
                limit=None,
                foreclosure_ids=foreclosure_ids,
                require_ori_searched=require_ori_searched,
            )
        )
        return {
            "targets": len(targets),
            "targets_with_live_noc": targets_with_docs,
            "total_noc_docs_found": total_docs,
            "total_saved": total_saved,
            "errors": errors,
            "api_calls": total_api_calls,
            "retries": total_retries,
            "truncated": total_truncated,
            "unresolved_truncations": total_unresolved_truncations,
            "remaining_recent_permit_no_noc_before": len(before_targets),
            "remaining_recent_permit_no_noc_after": after_remaining,
            "dry_run": dry_run,
            "per_target": per_target,
        }

    def backfill_missing_ori_ids(
        self,
        *,
        limit: int | None = None,
        straps: list[str] | None = None,
        enc_types: list[str] | None = None,
        only_unextracted: bool = False,
    ) -> dict[str, Any]:
        """Resolve PAV document IDs for encumbrances seeded without one.

        Encumbrances seeded from ``official_records_daily_instruments`` via
        ``_seed_from_official_records()`` carry an instrument number but no PAV
        document ID (``ori_id``).  Without ``ori_id`` the extraction service
        cannot build the download URL for the PDF.

        This method:
        1. Queries ``ori_encumbrances`` rows with ``ori_id IS NULL`` and a real
           (non-inferred) instrument number.
           When ``only_unextracted`` is true, it further scopes to
           ``extracted_data IS NULL`` so extraction-limited runs spend their
           budget only on still-extractable rows.
        2. For each instrument, searches the PAV KeywordSearch API (query 320,
           keyword 1006) which is the same path ``_search_instrument_pav()``
           uses.
        3. When exactly one matching document is returned (or exactly one whose
           instrument matches), writes ``ori_id`` back to the row.

        The method is idempotent, uses the PAV disk cache, respects rate limits
        via ``_post_pav`` retry logic, and is safe to call repeatedly.
        """
        sql = """
            SELECT id, instrument_number
            FROM ori_encumbrances
            WHERE ori_id IS NULL
              AND instrument_number NOT LIKE 'INFERRED-%%'
        """
        params: dict[str, Any] = {}
        if only_unextracted:
            sql += " AND extracted_data IS NULL"
        if straps:
            sql += " AND strap = ANY(:straps)"
            params["straps"] = list(straps)
        if enc_types:
            sql += " AND encumbrance_type = ANY(:enc_types)"
            params["enc_types"] = list(enc_types)
        sql += " ORDER BY recording_date ASC NULLS LAST"
        if limit is not None:
            sql += " LIMIT :lim"
            params["lim"] = limit

        with self.engine.connect() as conn:
            rows = conn.execute(text(sql), params).mappings().all()

        if not rows:
            logger.info("ori_id backfill: nothing to do (0 rows with NULL ori_id)")
            return {"skipped": True, "reason": "no_rows_need_ori_id"}

        logger.info("ori_id backfill: {} encumbrances to resolve", len(rows))

        resolved = 0
        not_found = 0
        ambiguous = 0
        errors = 0
        stats: dict[str, int] = {
            "api_calls": 0,
            "retries": 0,
            "truncated": 0,
            "unresolved_truncations": 0,
            "cache_hits": 0,
        }

        for idx, row in enumerate(rows, start=1):
            enc_id = row["id"]
            instrument = str(row["instrument_number"]).strip()
            if not instrument:
                continue

            try:
                docs = self._search_instrument_pav(instrument, stats)

                # Find documents whose instrument matches exactly
                matching = [
                    d for d in docs
                    if str(d.get("Instrument") or "").strip() == instrument
                    and d.get("ID")
                ]

                if len(matching) == 1:
                    pav_id = str(matching[0]["ID"])
                    with self.engine.begin() as conn:
                        conn.execute(
                            text("""
                                UPDATE ori_encumbrances
                                SET ori_id = :ori_id, updated_at = now()
                                WHERE id = :enc_id AND ori_id IS NULL
                            """),
                            {"ori_id": pav_id, "enc_id": enc_id},
                        )
                    resolved += 1
                    if idx % 25 == 0 or idx == len(rows):
                        logger.info(
                            "ori_id backfill progress: {}/{} processed, {} resolved",
                            idx,
                            len(rows),
                            resolved,
                        )
                elif len(matching) == 0:
                    not_found += 1
                    logger.debug(
                        "ori_id backfill: no PAV match for instrument={}",
                        instrument,
                    )
                else:
                    ambiguous += 1
                    logger.debug(
                        "ori_id backfill: {} PAV matches for instrument={}, skipping",
                        len(matching),
                        instrument,
                    )
            except Exception as exc:
                errors += 1
                logger.warning(
                    "ori_id backfill error for instrument={}: {}",
                    instrument,
                    exc,
                )

        logger.info(
            "ori_id backfill complete: resolved={} not_found={} ambiguous={} errors={} api_calls={}",
            resolved,
            not_found,
            ambiguous,
            errors,
            stats["api_calls"],
        )
        return {
            "targets": len(rows),
            "resolved": resolved,
            "not_found": not_found,
            "ambiguous": ambiguous,
            "errors": errors,
            "api_calls": stats["api_calls"],
            "cache_hits": stats.get("cache_hits", 0),
            "retries": stats["retries"],
        }

    def resolve_inferred_encumbrances(
        self,
        *,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Replace or delete INFERRED-* placeholder encumbrances.

        Two-pass algorithm:

        **Pass 1 — Local party match** (no API calls):
        For each inferred row, check if any real (non-inferred) encumbrance on
        the same strap has a party matching the inferred ``party1`` (the
        foreclosing plaintiff). Uses PG ``entity_match_score()`` function. If
        match >= 0.60 → delete the inferred row (it's redundant).

        **Pass 2 — ORI case search** (API calls for remaining):
        For inferred rows that survive Pass 1, search ORI by case number via
        ``_search_case_pav()``. If real docs are found and saved → delete the
        inferred row. If the case search returns nothing, keep the inferred row
        as a placeholder for survival analysis.
        """
        # ---- Find all inferred rows ----
        sql = """
            SELECT id, strap, folio, instrument_number, case_number,
                   encumbrance_type, party1
            FROM ori_encumbrances
            WHERE instrument_number LIKE 'INFERRED-%%'
        """
        params: dict[str, Any] = {}
        if limit is not None:
            sql += " LIMIT :lim"
            params["lim"] = limit

        with self.engine.connect() as conn:
            rows = conn.execute(text(sql), params).mappings().all()

        if not rows:
            logger.info("resolve_inferred: nothing to do (0 inferred rows)")
            return {"skipped": True, "reason": "no_inferred_rows"}

        logger.info("resolve_inferred: {} inferred encumbrances to process", len(rows))

        # ---- Pass 1: Local party match ----
        pass1_deleted = 0
        pass1_unresolved: list[dict[str, Any]] = []

        for row in rows:
            enc_id = row["id"]
            strap = row["strap"]
            party1 = row["party1"] or ""

            if not strap or not party1:
                pass1_unresolved.append(dict(row))
                continue

            # Atomically identify the best real match and delete the inferred row.
            # Truncate to 250 chars — PG levenshtein() has a 255-char limit.
            match_and_delete_sql = text("""
                WITH best_match AS (
                    SELECT id, instrument_number, encumbrance_type,
                           entity_match_score(LEFT(:party, 250), LEFT(COALESCE(party1, ''), 250)) AS p1_score,
                           entity_match_score(LEFT(:party, 250), LEFT(COALESCE(party2, ''), 250)) AS p2_score
                    FROM ori_encumbrances
                    WHERE strap = :strap
                      AND instrument_number NOT LIKE 'INFERRED-%%'
                      AND (
                          entity_match_score(LEFT(:party, 250), LEFT(COALESCE(party1, ''), 250)) >= 0.60
                          OR entity_match_score(LEFT(:party, 250), LEFT(COALESCE(party2, ''), 250)) >= 0.60
                      )
                    ORDER BY GREATEST(
                        entity_match_score(LEFT(:party, 250), LEFT(COALESCE(party1, ''), 250)),
                        entity_match_score(LEFT(:party, 250), LEFT(COALESCE(party2, ''), 250))
                    ) DESC,
                    id DESC
                    LIMIT 1
                ),
                deleted AS (
                    DELETE FROM ori_encumbrances
                    WHERE id = :id
                      AND EXISTS (SELECT 1 FROM best_match)
                    RETURNING id
                )
                SELECT bm.id, bm.instrument_number, bm.encumbrance_type,
                       bm.p1_score, bm.p2_score
                FROM best_match bm
                JOIN deleted d ON TRUE
            """)

            with self.engine.begin() as conn:
                match = conn.execute(
                    match_and_delete_sql,
                    {"id": enc_id, "party": party1, "strap": strap},
                ).fetchone()

            if match is not None:
                pass1_deleted += 1
                best_score = max(match[3], match[4])
                logger.info(
                    "resolve_inferred: deleted id={} (matched real id={} inst={} score={:.2f})",
                    enc_id,
                    match[0],
                    match[1],
                    best_score,
                )
            else:
                pass1_unresolved.append(dict(row))

        logger.info(
            "resolve_inferred pass 1 complete: deleted={} unresolved={}",
            pass1_deleted,
            len(pass1_unresolved),
        )

        # ---- Pass 2: ORI case search for remaining ----
        pass2_deleted = 0
        pass2_saved = 0
        pass2_kept = 0
        stats: dict[str, int] = {
            "api_calls": 0,
            "retries": 0,
            "truncated": 0,
            "unresolved_truncations": 0,
            "cache_hits": 0,
        }

        for row in pass1_unresolved:
            enc_id = row["id"]
            strap = row["strap"]
            folio = row["folio"]
            case_number = row["case_number"] or ""

            if not case_number:
                pass2_kept += 1
                continue

            try:
                docs = self._search_case_pav(case_number, stats)
                if docs and strap and folio:
                    saved = self._save_documents(strap, folio, docs)
                    if saved > 0:
                        # Real docs now exist — delete the inferred placeholder
                        with self.engine.begin() as conn:
                            conn.execute(
                                text("DELETE FROM ori_encumbrances WHERE id = :id"),
                                {"id": enc_id},
                            )
                        pass2_deleted += 1
                        pass2_saved += saved
                        logger.info(
                            "resolve_inferred: case search for {} found {} docs, saved {}, deleted inferred id={}",
                            case_number,
                            len(docs),
                            saved,
                            enc_id,
                        )
                        continue

                # Docs found but 0 new saves — they likely already exist.
                # Atomically delete the inferred row only if a real encumbrance
                # for the same case/party already exists on the same strap.
                if docs and strap:
                    inferred_party = row.get("party1") or ""
                    # Threshold 0.60 matches Pass 1 (line ~958) and is
                    # intentionally looser than the project-wide 0.72 used
                    # in is_same_entity().  Inferred rows are low-confidence
                    # placeholders — a moderately close party match on a real
                    # ORI doc is sufficient evidence to drop the placeholder.
                    delete_sql = text("""
                        DELETE FROM ori_encumbrances
                        WHERE id = :id
                          AND EXISTS (
                              SELECT 1 FROM ori_encumbrances r
                              WHERE r.strap = :strap
                                AND r.instrument_number NOT LIKE 'INFERRED-%%'
                                AND (
                                    r.case_number = :case_number
                                    OR (
                                        LENGTH(:party) > 0
                                        AND entity_match_score(
                                            LEFT(:party, 250),
                                            LEFT(COALESCE(r.party1, ''), 250)
                                        ) >= 0.60
                                    )
                                )
                          )
                    """)
                    with self.engine.begin() as conn:
                        result = conn.execute(
                            delete_sql,
                            {"id": enc_id, "strap": strap, "case_number": case_number, "party": inferred_party},
                        )
                    if result.rowcount:
                        pass2_deleted += 1
                        logger.info(
                            "resolve_inferred: case search for {} returned {} docs (already saved); "
                            "deleted redundant inferred id={}",
                            case_number,
                            len(docs),
                            enc_id,
                        )
                        continue

                pass2_kept += 1
                logger.info(
                    "resolve_inferred: case search for {} returned {} docs but 0 new saves; keeping inferred id={}",
                    case_number,
                    len(docs) if docs else 0,
                    enc_id,
                )
            except Exception as exc:
                pass2_kept += 1
                logger.warning(
                    "resolve_inferred: error searching case={} for inferred id={}: {}",
                    case_number,
                    enc_id,
                    exc,
                )

        logger.info(
            "resolve_inferred pass 2 complete: deleted={} new_docs_saved={} kept={}",
            pass2_deleted,
            pass2_saved,
            pass2_kept,
        )

        total_deleted = pass1_deleted + pass2_deleted
        logger.info(
            "resolve_inferred complete: {}/{} inferred rows deleted, {} kept",
            total_deleted,
            len(rows),
            pass2_kept,
        )
        return {
            "targets": len(rows),
            "pass1_deleted": pass1_deleted,
            "pass2_deleted": pass2_deleted,
            "pass2_new_docs": pass2_saved,
            "kept": pass2_kept,
            "total_deleted": total_deleted,
        }

    def run_targeted_recovery(
        self,
        *,
        foreclosure_ids: list[int],
        limit: int | None = None,
        dry_run: bool = False,
        force_satisfaction_relink: bool = True,
        retry_reasons: dict[int, list[str]] | None = None,
    ) -> dict[str, Any]:
        """Re-run the full ORI discovery pipeline for a selected foreclosure scope.

        This is used by the encumbrance audit recovery loop. It does not infer
        new facts on its own; it simply reuses the existing ORI discovery and
        persistence path for cases the audit identified as likely incomplete.
        """
        target_ids = sorted({int(fid) for fid in foreclosure_ids if fid})
        if not target_ids:
            return {"skipped": True, "reason": "no_targeted_recovery_ids"}

        before_targets = self._find_targeted_recovery_targets(
            foreclosure_ids=target_ids,
            limit=None,
        )
        targets = before_targets[:limit] if limit is not None else before_targets
        if not targets:
            return {"skipped": True, "reason": "no_targeted_recovery_targets"}

        total_docs = 0
        total_saved = 0
        total_inferred = 0
        total_linked = 0
        errors = 0
        total_api_calls = 0
        total_retries = 0
        total_truncated = 0
        total_unresolved_truncations = 0
        total_official_seed_docs = 0
        per_target: list[dict[str, Any]] = []

        for index, base_target in enumerate(targets, start=1):
            target = dict(base_target)
            target["ori_run_context"] = "targeted_recovery"
            if force_satisfaction_relink:
                target["force_satisfaction_relink"] = True
            target_retry_reasons = [
                str(reason)
                for reason in (retry_reasons or {}).get(int(target["foreclosure_id"]), [])
                if reason
            ]
            if target_retry_reasons:
                target["ori_retry_reasons"] = target_retry_reasons

            case = target["case_number"]
            strap = target.get("strap")
            logger.info(
                "[{}/{}] Targeted ORI recovery for {} (strap={})",
                index,
                len(targets),
                case,
                strap,
            )
            try:
                result = self._process_target(target, persist=not dry_run)
                total_docs += result["docs_found"]
                total_saved += result["saved"]
                total_inferred += result["inferred"]
                total_linked += result["satisfactions_linked"]
                total_api_calls += result["api_calls"]
                total_retries += result["retries"]
                total_truncated += result["truncated"]
                total_unresolved_truncations += result["unresolved_truncations"]
                total_official_seed_docs += result["official_seed_docs"]
                per_target.append(result)
            except Exception as exc:
                logger.exception(
                    "Targeted ORI recovery error for case={} foreclosure_id={}",
                    case,
                    target["foreclosure_id"],
                )
                errors += 1
                per_target.append({
                    "foreclosure_id": target["foreclosure_id"],
                    "case_number": case,
                    "strap": strap,
                    "folio": target.get("folio"),
                    "docs_found": 0,
                    "saved": 0,
                    "inferred": 0,
                    "satisfactions_linked": 0,
                    "error": str(exc),
                })

        return {
            "targets_requested": len(target_ids),
            "targets": len(targets),
            "total_documents_found": total_docs,
            "encumbrances_saved": total_saved,
            "inferred_saved": total_inferred,
            "satisfactions_linked": total_linked,
            "errors": errors,
            "api_calls": total_api_calls,
            "retries": total_retries,
            "truncated": total_truncated,
            "unresolved_truncations": total_unresolved_truncations,
            "official_seed_docs": total_official_seed_docs,
            "dry_run": dry_run,
            "per_target": per_target,
        }

    # ------------------------------------------------------------------
    # Target selection
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_target(row: Any) -> dict[str, Any]:
        jdata = row[4] or {}
        if isinstance(jdata, str):
            try:
                jdata = json.loads(jdata)
            except (json.JSONDecodeError, TypeError):
                logger.warning(
                    "Invalid judgment_data JSON for foreclosure_id={} case={}; using empty object",
                    row[0],
                    row[1],
                )
                jdata = {}

        return {
            "foreclosure_id": row[0],
            "case_number": row[1],
            "strap": row[2],
            "folio": row[3],
            "judgment_data": jdata,
            "auction_date": row[5],
            "filing_date": row[6],
            "legal1": row[7] or "",
            "legal2": row[8] or "",
            "legal3": row[9] or "",
            "legal4": row[10] or "",
            "owner_name": row[11] or "",
            "property_address": row[12] or "",
        }

    def _find_targets(self, limit: int | None) -> list[dict[str, Any]]:
        """Find foreclosures needing ORI search or LP recovery."""
        standard_targets = self._find_standard_targets(limit=None)
        lp_gap_targets = self._find_lis_pendens_gap_targets(
            limit=None,
            require_ori_searched=None,
        )
        return self._merge_targets(standard_targets, lp_gap_targets, limit=limit)

    def _find_standard_targets(self, limit: int | None) -> list[dict[str, Any]]:
        """Find foreclosures needing the normal parcel-backed ORI pass."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT f.foreclosure_id, f.case_number_raw, f.strap, f.folio,
                           f.judgment_data, f.auction_date, f.filing_date,
                           bp.raw_legal1, bp.raw_legal2, bp.raw_legal3, bp.raw_legal4,
                           bp.owner_name, bp.property_address
                    FROM foreclosures f
                    LEFT JOIN hcpa_bulk_parcels bp ON f.strap = bp.strap
                    WHERE f.step_ori_searched IS NULL
                      AND f.archived_at IS NULL
                      AND f.strap IS NOT NULL
                      AND f.strap <> 'MULTIPLE PARCEL'
                      AND f.folio IS NOT NULL
                    ORDER BY f.auction_date
                    LIMIT :limit
                """),
                {"limit": limit or 1000},
            ).fetchall()

        return [self._row_to_target(r) for r in rows]

    def _find_targeted_recovery_targets(
        self,
        *,
        foreclosure_ids: list[int],
        limit: int | None,
    ) -> list[dict[str, Any]]:
        """Load active judged foreclosures by id for audit-driven ORI retries."""
        if not foreclosure_ids:
            return []

        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT f.foreclosure_id, f.case_number_raw, f.strap, f.folio,
                           f.judgment_data, f.auction_date, f.filing_date,
                           bp.raw_legal1, bp.raw_legal2, bp.raw_legal3, bp.raw_legal4,
                           bp.owner_name,
                           COALESCE(NULLIF(btrim(f.property_address), ''), bp.property_address) AS property_address
                    FROM foreclosures f
                    LEFT JOIN hcpa_bulk_parcels bp ON f.strap = bp.strap
                    WHERE f.archived_at IS NULL
                      AND f.judgment_data IS NOT NULL
                      AND f.foreclosure_id = ANY(:foreclosure_ids)
                    ORDER BY f.auction_date NULLS LAST, f.foreclosure_id
                    LIMIT :limit
                """),
                {
                    "foreclosure_ids": foreclosure_ids,
                    "limit": limit or 100000,
                },
            ).fetchall()

        return [self._row_to_target(r) for r in rows]

    @staticmethod
    def _lp_case_sql(alias: str) -> str:
        return (
            f"({alias}.case_number_norm LIKE '%-CA-%' "
            f"OR COALESCE({alias}.clerk_case_type, '') ILIKE 'CC Real Property/Mortgage Foreclosure%')"
        )

    @staticmethod
    def _decorate_lis_pendens_gap_target(target: dict[str, Any]) -> dict[str, Any]:
        decorated = dict(target)
        decorated["lp_recovery_mode"] = True
        decorated["skip_inferred_fallback"] = True
        decorated["skip_live_noc_fallback"] = True
        # LP gap retries should only mark ORI searched after a persisted LP exists.
        decorated["mark_ori_searched"] = False
        return decorated

    @staticmethod
    def _has_persistable_identity(target: dict[str, Any]) -> bool:
        strap = (target.get("strap") or "").strip()
        folio = (target.get("folio") or "").strip()
        return bool(strap and strap != "MULTIPLE PARCEL" and folio)

    @staticmethod
    def _segmented_parcel_to_strap(parcel_id: str) -> str | None:
        token = parcel_id.strip().upper()
        match = _PARCEL_SEGMENT_RE.match(token)
        if not match:
            return None
        seg1, seg2, seg3, seg4, seg5, seg6, decimal = match.groups()
        return f"{seg3}{seg2}{seg1}{seg4}{seg5}{seg6}{decimal}U"

    @staticmethod
    def _subdivision_terms(subdivision: str | None) -> list[str]:
        if not subdivision:
            return []
        words = re.findall(r"[A-Z0-9]+", subdivision.upper())
        terms: list[str] = []
        for word in words:
            if len(word) < 4 or word in _GENERIC_SUBDIVISION_TERMS:
                continue
            if word not in terms:
                terms.append(word)
            if len(terms) >= 3:
                break
        return terms

    @staticmethod
    def _target_legal_description(target: dict[str, Any]) -> str:
        legal_fields = combine_legal_fields(
            target.get("legal1") or "",
            target.get("legal2"),
            target.get("legal3"),
            target.get("legal4"),
        ).strip()
        if legal_fields:
            return legal_fields

        judgment_data = target.get("judgment_data") or {}
        judgment_legal = (judgment_data.get("legal_description") or "").strip()
        if judgment_legal:
            return judgment_legal

        parsed = parse_legal_description("")
        parsed.subdivision = (judgment_data.get("subdivision") or "").strip() or None
        parsed.lot = (judgment_data.get("lot") or "").strip() or None
        parsed.block = (judgment_data.get("block") or "").strip() or None
        parsed.unit = (judgment_data.get("unit") or "").strip() or None
        if parsed.subdivision:
            parts = [parsed.subdivision]
            if parsed.lot:
                parts.append(f"LOT {parsed.lot}")
            if parsed.block:
                parts.append(f"BLOCK {parsed.block}")
            if parsed.unit:
                parts.append(f"UNIT {parsed.unit}")
            return " ".join(parts)
        return ""

    @staticmethod
    def _row_to_recovered_identity(row: Any, *, reason: str) -> _RecoveredTargetIdentity:
        legal_description = combine_legal_fields(
            row.get("raw_legal1") or "",
            row.get("raw_legal2"),
            row.get("raw_legal3"),
            row.get("raw_legal4"),
        )
        return _RecoveredTargetIdentity(
            strap=str(row.get("strap") or "").strip(),
            folio=str(row.get("folio") or "").strip(),
            property_address=(row.get("property_address") or "").strip() or None,
            legal1=(row.get("raw_legal1") or "").strip(),
            legal2=(row.get("raw_legal2") or "").strip(),
            legal3=(row.get("raw_legal3") or "").strip(),
            legal4=(row.get("raw_legal4") or "").strip(),
            owner_name=(row.get("owner_name") or "").strip() or None,
            legal_description=legal_description,
            reason=reason,
        )

    def _lookup_identity_by_existing_identifiers(
        self,
        conn: Any,
        *,
        strap: str | None,
        folio: str | None,
    ) -> _RecoveredTargetIdentity | None:
        strap_value = (strap or "").strip()
        folio_value = (folio or "").strip()
        folio_digits = re.sub(r"\D", "", folio_value)
        row = conn.execute(
            text(
                r"""
                SELECT
                    strap,
                    folio,
                    property_address,
                    raw_legal1,
                    raw_legal2,
                    raw_legal3,
                    raw_legal4,
                    owner_name
                FROM hcpa_bulk_parcels
                WHERE (:strap <> '' AND strap = :strap)
                   OR (:folio <> '' AND folio = :folio)
                   OR (
                        :folio_digits <> ''
                        AND regexp_replace(COALESCE(folio, ''), '\D', '', 'g') = :folio_digits
                   )
                ORDER BY
                    CASE
                        WHEN (:strap <> '' AND strap = :strap) AND (:folio <> '' AND folio = :folio) THEN 0
                        WHEN :strap <> '' AND strap = :strap THEN 1
                        WHEN :folio <> '' AND folio = :folio THEN 2
                        ELSE 3
                    END
                LIMIT 1
                """
            ),
            {
                "strap": strap_value,
                "folio": folio_value,
                "folio_digits": folio_digits,
            },
        ).mappings().first()
        if row is None:
            return None
        identity = self._row_to_recovered_identity(row, reason="existing_identifier_lookup")
        if not identity.strap or not identity.folio:
            return None
        return identity

    def _recover_target_identity(
        self,
        conn: Any,
        target: dict[str, Any],
    ) -> _RecoveredTargetIdentity | None:
        judgment_data = target.get("judgment_data") or {}
        parcel_id = (judgment_data.get("parcel_id") or "").strip().upper()
        if parcel_id and parcel_id not in _PLACEHOLDER_PARCEL_IDS:
            parcel_candidates: list[tuple[str, str]] = []
            parcel_candidates.append(("parcel_id_exact", parcel_id))
            segmented = self._segmented_parcel_to_strap(parcel_id)
            if segmented:
                parcel_candidates.append(("parcel_id_segmented", segmented))

            for reason, token in parcel_candidates:
                row = conn.execute(
                    text(
                        r"""
                        SELECT
                            strap,
                            folio,
                            property_address,
                            raw_legal1,
                            raw_legal2,
                            raw_legal3,
                            raw_legal4,
                            owner_name
                        FROM hcpa_bulk_parcels
                        WHERE strap = :token OR folio = :token
                        ORDER BY source_file_id DESC NULLS LAST
                        LIMIT 1
                        """
                    ),
                    {"token": token},
                ).mappings().first()
                if row is None:
                    continue
                identity = self._row_to_recovered_identity(row, reason=reason)
                if identity.strap and identity.folio:
                    return identity

            digits = re.sub(r"\D", "", parcel_id)
            if digits:
                row = conn.execute(
                    text(
                        r"""
                        SELECT
                            strap,
                            folio,
                            property_address,
                            raw_legal1,
                            raw_legal2,
                            raw_legal3,
                            raw_legal4,
                            owner_name
                        FROM hcpa_bulk_parcels
                        WHERE regexp_replace(COALESCE(strap, ''), '\D', '', 'g') = :digits
                           OR regexp_replace(COALESCE(folio, ''), '\D', '', 'g') = :digits
                        ORDER BY source_file_id DESC NULLS LAST
                        LIMIT 1
                        """
                    ),
                    {"digits": digits},
                ).mappings().first()
                if row is not None:
                    identity = self._row_to_recovered_identity(row, reason="parcel_id_digits")
                    if identity.strap and identity.folio:
                        return identity

        judgment_legal = self._target_legal_description(target)
        if not judgment_legal:
            return None

        parsed = parse_legal_description(judgment_legal)
        subdivision = (
            (judgment_data.get("subdivision") or "").strip()
            or (parsed.subdivision or "").strip()
        )
        lot = ((judgment_data.get("lot") or "").strip() or (parsed.lot or "").strip())
        block = ((judgment_data.get("block") or "").strip() or (parsed.block or "").strip())
        unit = ((judgment_data.get("unit") or "").strip() or (parsed.unit or "").strip())
        # Parsed plat-book/page extraction is too noisy on narrative legals
        # like "... PLAT BOOK 28, PAGE 41 ..." and can misread "28/41" as "2/8".
        # Only trust explicit judgment_data plat fields here.
        clauses: list[str] = []
        params: dict[str, Any] = {"limit": _TARGET_MAX_LEGAL_CANDIDATES}
        for index, term in enumerate(self._subdivision_terms(subdivision)):
            key = f"sub_term_{index}"
            clauses.append(f"{_TARGET_LEGAL_EXPR} LIKE :{key}")
            params[key] = f"%{term}%"

        if lot:
            clauses.append(f"{_TARGET_LEGAL_EXPR} ~* :lot_regex")
            params["lot_regex"] = (
                rf"(^|[^A-Z0-9])(LOT|L|LT)\s*{re.escape(lot.upper())}([^A-Z0-9]|$)"
            )

        if block:
            clauses.append(f"{_TARGET_LEGAL_EXPR} ~* :block_regex")
            params["block_regex"] = (
                rf"(^|[^A-Z0-9])(BLOCK|BLK|B)\s*{re.escape(block.upper())}"
                r"([^A-Z0-9]|$)"
            )

        if unit:
            clauses.append(f"{_TARGET_LEGAL_EXPR} ~* :unit_regex")
            params["unit_regex"] = (
                rf"(^|[^A-Z0-9])(UNIT|U|UN)\s*{re.escape(unit.upper())}"
                r"([^A-Z0-9]|$)"
            )

        if not clauses:
            return None

        rows = conn.execute(
            text(
                f"""
                SELECT
                    strap,
                    folio,
                    property_address,
                    raw_legal1,
                    raw_legal2,
                    raw_legal3,
                    raw_legal4,
                    owner_name
                FROM hcpa_bulk_parcels
                WHERE {' AND '.join(clauses)}
                ORDER BY source_file_id DESC NULLS LAST
                LIMIT :limit
                """
            ),
            params,
        ).mappings().fetchall()
        if not rows:
            return None

        scored: list[tuple[float, _RecoveredTargetIdentity]] = []
        for row in rows:
            identity = self._row_to_recovered_identity(row, reason="judgment_legal_match")
            if not identity.strap or not identity.folio or not identity.legal_description:
                continue
            matched, confidence, _ = legal_descriptions_match(
                judgment_legal,
                identity.legal_description,
                threshold=0.82,
            )
            if matched:
                scored.append((confidence, identity))

        if not scored:
            return None

        scored.sort(key=lambda item: item[0], reverse=True)
        if len(scored) == 1:
            return scored[0][1]

        top_score, top_identity = scored[0]
        second_score = scored[1][0]
        if top_score >= 0.92 and (top_score - second_score) >= 0.08:
            return top_identity
        return None

    def _prepare_target_identity(
        self,
        target: dict[str, Any],
        *,
        persist_update: bool,
    ) -> tuple[dict[str, Any], _RecoveredTargetIdentity | None]:
        prepared = dict(target)
        strap = (prepared.get("strap") or "").strip() or None
        folio = (prepared.get("folio") or "").strip() or None
        needs_identity = not self._has_persistable_identity(prepared)
        needs_context = not any(
            (prepared.get("property_address"), prepared.get("legal1"), prepared.get("legal2"), prepared.get("legal3"), prepared.get("legal4"))
        )
        if not needs_identity and not needs_context:
            return prepared, None

        try:
            context_manager = self.engine.begin() if persist_update else self.engine.connect()
            with context_manager as conn:
                identity = None
                if strap or folio:
                    identity = self._lookup_identity_by_existing_identifiers(
                        conn,
                        strap=strap,
                        folio=folio,
                    )
                if identity is None:
                    identity = self._recover_target_identity(conn, prepared)
                if identity is None:
                    return prepared, None

                prepared["strap"] = prepared.get("strap") or identity.strap
                prepared["folio"] = prepared.get("folio") or identity.folio
                prepared["property_address"] = prepared.get("property_address") or identity.property_address or ""
                prepared["legal1"] = prepared.get("legal1") or identity.legal1
                prepared["legal2"] = prepared.get("legal2") or identity.legal2
                prepared["legal3"] = prepared.get("legal3") or identity.legal3
                prepared["legal4"] = prepared.get("legal4") or identity.legal4
                prepared["owner_name"] = prepared.get("owner_name") or identity.owner_name or ""

                if persist_update and prepared.get("foreclosure_id"):
                    conn.execute(
                        text(
                            """
                            UPDATE foreclosures
                            SET strap = COALESCE(strap, :strap),
                                folio = COALESCE(folio, :folio),
                                property_address = COALESCE(NULLIF(btrim(property_address), ''), :property_address)
                            WHERE foreclosure_id = :foreclosure_id
                            """
                        ),
                        {
                            "foreclosure_id": int(prepared["foreclosure_id"]),
                            "strap": prepared["strap"],
                            "folio": prepared["folio"],
                            "property_address": prepared["property_address"] or None,
                        },
                    )
                logger.info(
                    "Recovered ORI target identity for case={} foreclosure_id={} via {} -> strap={} folio={}",
                    prepared.get("case_number") or "",
                    prepared.get("foreclosure_id"),
                    identity.reason,
                    prepared.get("strap") or "",
                    prepared.get("folio") or "",
                )
                return prepared, identity
        except Exception:
            logger.exception(
                "ORI target identity recovery failed for case={} foreclosure_id={}",
                prepared.get("case_number") or "",
                prepared.get("foreclosure_id"),
            )
            return prepared, None

    @staticmethod
    def _case_only_stage_path(case_number: str, *, lp_only: bool) -> Path:
        filename = _CASE_ONLY_LP_STAGE_FILENAME if lp_only else _CASE_ONLY_ORI_STAGE_FILENAME
        return FORECLOSURE_DATA_DIR / case_number / "ori" / filename

    def _stage_case_only_documents(
        self,
        *,
        target: dict[str, Any],
        documents: list[dict[str, Any]],
        lp_only: bool,
    ) -> str | None:
        case_number = (target.get("case_number") or "").strip()
        if not case_number or not documents:
            return None

        path = self._case_only_stage_path(case_number, lp_only=lp_only)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "foreclosure_id": target.get("foreclosure_id"),
            "case_number": case_number,
            "lp_only": lp_only,
            "saved_at": datetime.now(tz=UTC).isoformat(),
            "reason": "missing_property_identity",
            "target": {
                "strap": target.get("strap"),
                "folio": target.get("folio"),
                "property_address": target.get("property_address"),
            },
            "judgment_data": target.get("judgment_data") or {},
            "documents": documents,
        }
        path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        logger.warning(
            "Staged {} unresolved {} ORI doc(s) for case={} at {}",
            len(documents),
            "LP-only" if lp_only else "case-only",
            case_number,
            path,
        )
        return str(path)

    def _clear_case_only_stage_files(self, case_number: str) -> None:
        clean_case = (case_number or "").strip()
        if not clean_case:
            return
        for lp_only in (False, True):
            path = self._case_only_stage_path(clean_case, lp_only=lp_only)
            if path.exists():
                path.unlink()

    @staticmethod
    def _merge_targets(
        primary: list[dict[str, Any]],
        secondary: list[dict[str, Any]],
        *,
        limit: int | None,
    ) -> list[dict[str, Any]]:
        merged: list[dict[str, Any]] = []
        seen: set[int] = set()
        for target in [*primary, *secondary]:
            foreclosure_id = int(target["foreclosure_id"])
            if foreclosure_id in seen:
                continue
            seen.add(foreclosure_id)
            merged.append(target)
            if limit is not None and len(merged) >= limit:
                break
        return merged

    @staticmethod
    def _describe_zero_persistence_reason(
        *,
        docs_found: int,
        eligible_documents: int,
        save_skips: int,
    ) -> str:
        if save_skips > 0:
            return "eligible documents were discovered but savepoint rollbacks prevented persistence"
        if docs_found <= 0:
            return "discovery found no documents"
        if eligible_documents <= 0:
            return "discovery found only non-persistable documents"
        return "eligible documents were already persisted unchanged"

    @staticmethod
    def _describe_infer_from_judgment_reason(reason: str | None) -> str:
        mapping = {
            "missing_property_identity": "judgment fallback could not run without strap/folio",
            "missing_plaintiff": "judgment fallback had no plaintiff to infer the foreclosing lien",
            "existing_inferred_encumbrance": "judgment fallback was already persisted",
            "inserted": "judgment fallback inserted an inferred encumbrance",
            "not_run": "judgment fallback was not attempted",
        }
        return mapping.get(reason or "", "judgment fallback returned no new encumbrance")

    def _existing_encumbrance_snapshot(
        self,
        strap: str | None,
        case_number: str | None,
    ) -> dict[str, int]:
        snapshot = {
            "total": 0,
            "core_liens": 0,
            "foreclosing": 0,
            "lis_pendens": 0,
            "noc": 0,
        }
        case_candidates = [candidate for candidate in self._case_variants(case_number or "") if candidate]
        match_clauses: list[str] = []
        params: dict[str, Any] = {}
        if strap:
            match_clauses.append("oe.strap = :strap")
            params["strap"] = strap
        if case_candidates:
            match_clauses.append("oe.case_number = ANY(:case_numbers)")
            params["case_numbers"] = case_candidates
        if not match_clauses:
            return snapshot

        sql = f"""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (
                    WHERE oe.encumbrance_type IN ('mortgage', 'lien', 'judgment')
                ) AS core_liens,
                COUNT(*) FILTER (
                    WHERE COALESCE(oe.survival_status, '') = 'FORECLOSING'
                ) AS foreclosing,
                COUNT(*) FILTER (WHERE oe.encumbrance_type = 'lis_pendens') AS lis_pendens,
                COUNT(*) FILTER (WHERE oe.encumbrance_type = 'noc') AS noc
            FROM ori_encumbrances oe
            WHERE {" OR ".join(match_clauses)}
        """
        with self.engine.connect() as conn:
            row = conn.execute(text(sql), params).mappings().first() or {}

        return {key: int(row.get(key) or 0) for key in snapshot}

    def _find_lis_pendens_gap_targets(
        self,
        *,
        limit: int | None,
        foreclosure_ids: list[int] | None = None,
        require_ori_searched: bool | None = True,
    ) -> list[dict[str, Any]]:
        """Find active judged foreclosures that still lack a persisted LP."""
        where_clauses = [
            "f.archived_at IS NULL",
            "f.judgment_data IS NOT NULL",
            self._lp_case_sql("f"),
            """
            NOT EXISTS (
                SELECT 1
                FROM ori_encumbrances oe
                WHERE oe.encumbrance_type = 'lis_pendens'
                  AND (
                        (f.strap IS NOT NULL AND oe.strap = f.strap)
                     OR oe.case_number = f.case_number_raw
                     OR oe.case_number = f.case_number_norm
                  )
            )
            """,
        ]
        params: dict[str, Any] = {"limit": limit or 100000}

        if require_ori_searched is True:
            where_clauses.append("f.step_ori_searched IS NOT NULL")
        elif require_ori_searched is False:
            where_clauses.append("f.step_ori_searched IS NULL")

        if foreclosure_ids:
            where_clauses.append("f.foreclosure_id = ANY(:foreclosure_ids)")
            params["foreclosure_ids"] = foreclosure_ids

        sql = f"""
            SELECT
                f.foreclosure_id,
                f.case_number_raw,
                f.strap,
                f.folio,
                f.judgment_data,
                f.auction_date,
                f.filing_date,
                bp.raw_legal1,
                bp.raw_legal2,
                bp.raw_legal3,
                bp.raw_legal4,
                bp.owner_name,
                COALESCE(NULLIF(btrim(f.property_address), ''), bp.property_address) AS property_address
            FROM foreclosures f
            LEFT JOIN hcpa_bulk_parcels bp ON f.strap = bp.strap
            WHERE {" AND ".join(where_clauses)}
            ORDER BY f.auction_date NULLS LAST, f.foreclosure_id
            LIMIT :limit
        """
        with self.engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
        return [self._decorate_lis_pendens_gap_target(self._row_to_target(row)) for row in rows]

    def _has_persisted_lis_pendens(
        self,
        case_number: str | None,
        strap: str | None,
    ) -> bool:
        case_candidates = [candidate for candidate in self._case_variants(case_number or "") if candidate]
        clauses = ["oe.encumbrance_type = 'lis_pendens'"]
        match_clauses: list[str] = []
        params: dict[str, Any] = {}

        if strap:
            match_clauses.append("oe.strap = :strap")
            params["strap"] = strap
        if case_candidates:
            match_clauses.append("oe.case_number = ANY(:case_numbers)")
            params["case_numbers"] = case_candidates
        if not match_clauses:
            return False

        sql = f"""
            SELECT EXISTS (
                SELECT 1
                FROM ori_encumbrances oe
                WHERE {" AND ".join(clauses)}
                  AND ({" OR ".join(match_clauses)})
            )
        """
        with self.engine.connect() as conn:
            return bool(conn.execute(text(sql), params).scalar())

    def _find_recent_permit_no_noc_targets(
        self,
        *,
        limit: int | None,
        foreclosure_ids: list[int] | None = None,
        require_ori_searched: bool = True,
    ) -> list[dict[str, Any]]:
        """Find active foreclosures with recent permit signal but no persisted NOC."""
        coverage_start = self._official_noc_coverage_start()
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    WITH scope AS (
                        SELECT
                            f.foreclosure_id,
                            f.case_number_raw,
                            f.strap,
                            f.folio,
                            f.judgment_data,
                            f.auction_date,
                            f.filing_date,
                            bp.raw_legal1,
                            bp.raw_legal2,
                            bp.raw_legal3,
                            bp.raw_legal4,
                            bp.owner_name,
                            COALESCE(NULLIF(btrim(f.property_address), ''), bp.property_address) AS property_address
                        FROM foreclosures f
                        LEFT JOIN hcpa_bulk_parcels bp ON bp.strap = f.strap
                        WHERE f.archived_at IS NULL
                          AND f.strap IS NOT NULL
                          AND f.strap <> 'MULTIPLE PARCEL'
                          AND f.folio IS NOT NULL
                          AND (:require_ori_searched = FALSE OR f.step_ori_searched IS NOT NULL)
                    ),
                    no_noc AS (
                        SELECT s.*
                        FROM scope s
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM ori_encumbrances oe
                            WHERE oe.encumbrance_type = 'noc'
                              AND (
                                    oe.strap = s.strap
                                 OR (oe.folio IS NOT NULL AND oe.folio = s.folio)
                              )
                        )
                    )
                    SELECT *
                    FROM no_noc s
                    WHERE (
                        EXISTS (
                            SELECT 1
                            FROM county_permits cp
                            WHERE regexp_replace(
                                    COALESCE(cp.folio_clean, cp.folio_raw, ''),
                                    '[^0-9]',
                                    '',
                                    'g'
                                  ) = regexp_replace(s.folio, '[^0-9]', '', 'g')
                              AND (
                                    cp.issue_date >= :coverage_start
                                 OR (
                                        COALESCE(cp.permit_number, '') ~ '^HC-(BLD|BTR)-[0-9]{2}-'
                                    AND 2000 + CAST(
                                            split_part(COALESCE(cp.permit_number, ''), '-', 3) AS INTEGER
                                        ) >= :coverage_year
                                 )
                              )
                        )
                        OR EXISTS (
                            SELECT 1
                            FROM tampa_accela_records tr
                            WHERE s.property_address IS NOT NULL
                              AND btrim(s.property_address) <> ''
                              AND btrim(COALESCE(tr.address_normalized, tr.address_raw, '')) <> ''
                              AND upper(trim(
                                    split_part(
                                        replace(COALESCE(tr.address_normalized, tr.address_raw, ''), E'\\t', ' '),
                                        ',',
                                        1
                                    )
                                  )) = upper(trim(
                                    split_part(replace(s.property_address, E'\\t', ' '), ',', 1)
                                  ))
                              AND COALESCE(tr.is_violation, FALSE) = FALSE
                              AND COALESCE(tr.module, '') <> 'Business'
                              AND COALESCE(tr.record_number, '') NOT LIKE 'BTX-%'
                              AND COALESCE(tr.record_type, '') NOT ILIKE 'Tax Receipt%'
                              AND (
                                    tr.record_date >= :coverage_start
                                 OR (
                                        COALESCE(tr.record_number, '') ~ '^(BLD|BTR)-[0-9]{2}-'
                                    AND 2000 + CAST(
                                            split_part(COALESCE(tr.record_number, ''), '-', 2) AS INTEGER
                                        ) >= :coverage_year
                                 )
                              )
                        )
                    )
                    ORDER BY s.auction_date NULLS LAST, s.foreclosure_id
                    LIMIT :limit
                    """
                ),
                {
                    "coverage_start": coverage_start,
                    "coverage_year": coverage_start.year,
                    "require_ori_searched": require_ori_searched,
                    "limit": limit or 100000,
                },
            ).fetchall()

        targets = [self._row_to_target(r) for r in rows]
        if foreclosure_ids:
            wanted = set(foreclosure_ids)
            targets = [target for target in targets if int(target["foreclosure_id"]) in wanted]
        return targets

    # ------------------------------------------------------------------
    # Search orchestration
    # ------------------------------------------------------------------

    async def _search_all(self, targets: list[dict]) -> dict[str, Any]:
        total_docs = 0
        total_saved = 0
        total_inferred = 0
        errors = 0
        total_api_calls = 0
        total_retries = 0
        total_truncated = 0
        total_unresolved_truncations = 0
        total_official_seed_docs = 0
        total_save_skips = 0
        total_staged_targets = 0
        total_search_marks = 0

        for i, target in enumerate(targets):
            fid = target["foreclosure_id"]
            case = target["case_number"]
            strap = target["strap"]

            logger.info(f"[{i + 1}/{len(targets)}] ORI search for {case} (strap={strap})")

            try:
                scoped_target = dict(target)
                scoped_target.setdefault("ori_run_context", "standard_search")
                result = self._process_target(scoped_target, persist=True)
                total_docs += result["docs_found"]
                total_saved += result["saved"]
                total_inferred += result["inferred"]
                total_api_calls += result["api_calls"]
                total_retries += result["retries"]
                total_truncated += result["truncated"]
                total_unresolved_truncations += result["unresolved_truncations"]
                total_official_seed_docs += result["official_seed_docs"]
                total_save_skips += result["save_skips"]
                total_staged_targets += int(bool(result["case_only_stage_path"]))
                total_search_marks += int(bool(result["marked_ori_searched"]))

            except Exception:
                logger.exception(
                    "ORI search error for case={} strap={} foreclosure_id={}",
                    case,
                    strap,
                    fid,
                )
                errors += 1

        return {
            "targets": len(targets),
            "total_documents_found": total_docs,
            "encumbrances_saved": total_saved,
            "inferred_saved": total_inferred,
            "errors": errors,
            "api_calls": total_api_calls,
            "retries": total_retries,
            "truncated_responses": total_truncated,
            "unresolved_truncations": total_unresolved_truncations,
            "official_seed_docs": total_official_seed_docs,
            "save_skips": total_save_skips,
            "staged_targets": total_staged_targets,
            "targets_marked_searched": total_search_marks,
        }

    def _process_target(
        self,
        target: dict[str, Any],
        *,
        persist: bool,
    ) -> dict[str, Any]:
        """Discover, save, and optionally mark a single ORI target."""
        target, recovered_identity = self._prepare_target_identity(
            target,
            persist_update=persist,
        )
        foreclosure_id = int(target["foreclosure_id"])
        case_number = target.get("case_number") or ""
        strap = (target.get("strap") or "").strip() or None
        folio = (target.get("folio") or "").strip() or None
        run_context = str(target.get("ori_run_context") or "standard_search")
        retry_reasons = [str(reason) for reason in (target.get("ori_retry_reasons") or []) if reason]

        docs, metrics = self._discover_property(target)
        logger.info(
            "  Discovery complete: docs={} api_calls={} retries={} "
            "truncated={} unresolved_trunc={} seeds=(deeds={}, clerk_cases={}, official={})",
            len(docs),
            metrics["api_calls"],
            metrics["retries"],
            metrics["truncated"],
            metrics["unresolved_truncations"],
            metrics["deed_count"],
            metrics["clerk_case_count"],
            metrics["official_seed_docs"],
        )

        saved = 0
        inferred = 0
        linked = 0
        staged_path: str | None = None
        save_skips = 0
        eligible_documents = 0
        marked_searched = False
        if persist:
            if not self._has_persistable_identity(target):
                if docs:
                    staged_path = self._stage_case_only_documents(
                        target=target,
                        documents=docs,
                        lp_only=bool(target.get("lp_recovery_mode")),
                    )
                else:
                    logger.warning(
                        "ORI target still missing strap/folio after recovery for case={} foreclosure_id={}; leaving unmarked",
                        case_number,
                        foreclosure_id,
                    )
            else:
                saved = self._save_documents(strap, folio, docs)
                save_stats = dict(self._last_save_documents_stats)
                save_skips = int(save_stats.get("skipped", 0))
                eligible_documents = int(save_stats.get("eligible", 0))
                if saved == 0 and not bool(target.get("skip_inferred_fallback")):
                    inferred = self._infer_from_judgment(strap, folio, target)
                    saved += inferred
                    if inferred == 0:
                        zero_reason = self._describe_zero_persistence_reason(
                            docs_found=len(docs),
                            eligible_documents=eligible_documents,
                            save_skips=save_skips,
                        )
                        infer_reason_key = self._last_infer_from_judgment_stats.get("reason")
                        infer_reason = self._describe_infer_from_judgment_reason(
                            infer_reason_key if isinstance(infer_reason_key, str) else None,
                        )
                        existing_snapshot = self._existing_encumbrance_snapshot(
                            strap,
                            case_number,
                        )
                        logger.warning(
                            "No new encumbrances persisted for case={} strap={} context={} retry_reasons={} why={} inferred_why={} docs_found={} eligible_docs={} save_skips={} existing_total={} existing_core_liens={} existing_foreclosing={} existing_lis_pendens={} existing_noc={}",
                            case_number,
                            strap or "",
                            run_context,
                            ",".join(retry_reasons) if retry_reasons else "none",
                            zero_reason,
                            infer_reason,
                            len(docs),
                            eligible_documents,
                            save_skips,
                            existing_snapshot["total"],
                            existing_snapshot["core_liens"],
                            existing_snapshot["foreclosing"],
                            existing_snapshot["lis_pendens"],
                            existing_snapshot["noc"],
                        )
                if saved > 0 or inferred > 0:
                    self._clear_case_only_stage_files(case_number)
            if strap and saved > 0:
                linked = self._link_satisfactions(strap)
                self._link_modifications(strap)
                # Chase CLK# refs from unlinked SATs to discover parent mortgages
                chase_linked = self._chase_unlinked_sat_parents(strap, folio)
                linked += chase_linked
            if (
                bool(target.get("mark_ori_searched", True))
                and self._has_persistable_identity(target)
                and not staged_path
                and save_skips == 0
                and (saved + inferred + linked) > 0
            ):
                self._mark_searched(foreclosure_id)
                marked_searched = True

        return {
            "foreclosure_id": foreclosure_id,
            "case_number": case_number,
            "strap": strap,
            "folio": folio,
            "docs_found": len(docs),
            "saved": saved,
            "inferred": inferred,
            "satisfactions_linked": linked,
            "identity_recovered": recovered_identity is not None,
            "identity_recovery_reason": recovered_identity.reason if recovered_identity else None,
            "case_only_stage_path": staged_path,
            "api_calls": metrics["api_calls"],
            "retries": metrics["retries"],
            "truncated": metrics["truncated"],
            "unresolved_truncations": metrics["unresolved_truncations"],
            "official_seed_docs": metrics["official_seed_docs"],
            "deed_count": metrics["deed_count"],
            "clerk_case_count": metrics["clerk_case_count"],
            "save_skips": save_skips,
            "eligible_documents": eligible_documents,
            "marked_ori_searched": marked_searched,
            "run_context": run_context,
            "retry_reasons": retry_reasons,
            "instruments": [_get_instrument(doc) for doc in docs if _get_instrument(doc)],
        }

    # ------------------------------------------------------------------
    # Phase-based discovery (PG-first chain+adjacent)
    # ------------------------------------------------------------------

    def _discover_property(self, target: dict[str, Any]) -> tuple[list[dict], dict[str, int]]:
        """Discover ORI documents for one property with bounded, phased search."""
        case_number = (target.get("case_number") or "").strip()
        strap = (target.get("strap") or "").strip()
        judgment_data = target.get("judgment_data") or {}
        plaintiff = (judgment_data.get("plaintiff") or "").strip()
        defendant = (judgment_data.get("defendant") or "").strip()
        lp_recovery_mode = bool(target.get("lp_recovery_mode"))
        skip_live_noc_fallback = bool(target.get("skip_live_noc_fallback"))

        stats = {
            "api_calls": 0,
            "retries": 0,
            "truncated": 0,
            "unresolved_truncations": 0,
            "deed_count": 0,
            "clerk_case_count": 0,
            "official_seed_docs": 0,
            "live_noc_docs": 0,
        }

        docs_by_inst: dict[str, dict[str, Any]] = {}
        chased_instruments: set[str] = set()
        queued_instruments: list[str] = []
        queued_book_pages: list[tuple[str, str]] = []
        seen_book_pages: set[str] = set()

        ownership_chain = self._get_ownership_chain(strap)
        stats["deed_count"] = len(ownership_chain)
        if not ownership_chain:
            logger.warning(
                "No ownership chain rows found for case={} strap={}",
                case_number,
                strap,
            )

        earliest_date = self._earliest_relevant_date(ownership_chain, target)
        latest_date = datetime.now(tz=UTC).date()
        property_tokens = self._build_property_tokens(target, ownership_chain)

        # Phase 0: Seed from local Official Records daily index snapshots.
        # This provides low-latency linkage for recently recorded docs without
        # waiting on external ORI API calls.
        official_seed_docs = self._seed_from_official_records(
            target=target,
            earliest_date=earliest_date,
            latest_date=latest_date,
            property_tokens=property_tokens,
        )
        if official_seed_docs:
            self._merge_docs(docs_by_inst, official_seed_docs)
            stats["official_seed_docs"] = len(official_seed_docs)

        # Phase 1A: Case-number anchors.
        for variant in self._case_variants(case_number):
            docs = self._search_case_pav(
                variant,
                stats,
                persist_case_number=case_number,
                bypass_cache=lp_recovery_mode,
            )
            self._merge_docs(docs_by_inst, docs)

        # Phase 1B: Ownership chain + adjacent instruments.
        deeds = ownership_chain[-_MAX_DEEDS_TO_SCAN:]
        adjacent_searches = 0
        ct_cd_cases: set[str] = set()
        for deed in deeds:
            deed_inst = deed.get("doc_num") or ""
            if not deed_inst:
                continue

            deed_docs = self._search_instrument_pav(deed_inst, stats)
            filtered = [d for d in deed_docs if self._matches_property(d, property_tokens)]
            self._merge_docs(docs_by_inst, filtered)

            if deed.get("sale_type") in {"CT", "CD"}:
                for doc in deed_docs:
                    for ct_case in self._extract_case_numbers(doc):
                        if ct_case != case_number:
                            ct_cd_cases.add(ct_case)

            try:
                base = int(deed_inst)
            except (TypeError, ValueError):
                continue

            for offset in (-3, -2, -1, 1, 2, 3, 4, 5, 6, 7):
                if adjacent_searches >= _MAX_ADJACENT_SEARCHES:
                    break
                adjacent_searches += 1
                candidate = str(base + offset)
                candidate_docs = self._search_instrument_pav(candidate, stats)
                known_instruments, known_book_pages = self._reference_anchor_sets(docs_by_inst.values())
                filtered = [
                    d
                    for d in candidate_docs
                    if self._matches_property_or_reference(
                        d,
                        property_tokens,
                        anchor_instruments=known_instruments,
                        anchor_book_pages=known_book_pages,
                    )
                ]
                self._merge_docs(docs_by_inst, filtered)

        # Phase 1B+: Mortgage/lien chain — search adjacent to discovered
        # encumbrance instruments (catches 2nd/3rd mortgages, ASG, SAT recorded
        # near the original mortgage but not adjacent to any deed).
        searched_deed_insts = {d.get("doc_num") or "" for d in deeds}
        enc_instruments = []
        for doc in docs_by_inst.values():
            inst = _get_instrument(doc)
            if inst and inst not in searched_deed_insts:
                canonical = normalize_document_type(doc.get("DocType") or "")
                if canonical in CANONICAL_ENCUMBRANCE_TYPES or canonical in CANONICAL_SATISFACTION_TYPES:
                    enc_instruments.append(inst)
        for enc_inst in enc_instruments:
            try:
                base = int(enc_inst)
            except (TypeError, ValueError):
                continue
            for offset in (-3, -2, -1, 1, 2, 3, 4, 5, 6, 7):
                if adjacent_searches >= _MAX_ADJACENT_SEARCHES:
                    break
                adjacent_searches += 1
                candidate = str(base + offset)
                if candidate in docs_by_inst:
                    continue
                candidate_docs = self._search_instrument_pav(candidate, stats)
                known_instruments, known_book_pages = self._reference_anchor_sets(docs_by_inst.values())
                filtered = [
                    d
                    for d in candidate_docs
                    if self._matches_property_or_reference(
                        d,
                        property_tokens,
                        anchor_instruments=known_instruments,
                        anchor_book_pages=known_book_pages,
                    )
                ]
                self._merge_docs(docs_by_inst, filtered)

        # Phase 1C: Related foreclosure cases from CT/CD transfer docs.
        for ct_case in sorted(ct_cd_cases):
            docs = self._search_case_pav(
                ct_case,
                stats,
                persist_case_number=ct_case,
            )
            filtered = [d for d in docs if self._matches_property(d, property_tokens)]
            self._merge_docs(docs_by_inst, filtered)

        # Phase 2: reference chase.
        for doc in list(docs_by_inst.values()):
            inst = _get_instrument(doc)
            if not inst:
                continue
            canonical = normalize_document_type(doc.get("DocType") or "")
            if (
                _is_encumbrance_type(doc)
                or _is_assignment_type(doc)
                or canonical in CANONICAL_SATISFACTION_TYPES
                or canonical in CANONICAL_LIFECYCLE_TYPES
            ):
                queued_instruments.append(inst)

            inst_refs, bkpg_refs = self._extract_references_from_doc(doc)
            for ref in inst_refs:
                if ref not in chased_instruments:
                    queued_instruments.append(ref)
            for book, page in bkpg_refs:
                key = f"{book}/{page}"
                if key not in seen_book_pages:
                    seen_book_pages.add(key)
                    queued_book_pages.append((book, page))

        chase_count = 0
        while queued_instruments and chase_count < _MAX_REFERENCE_CHASE:
            chase_count += 1
            instrument = queued_instruments.pop(0)
            if not instrument or instrument in chased_instruments:
                continue
            chased_instruments.add(instrument)

            ref_docs = self._search_legal_pav(
                f"CLK #{instrument}",
                stats,
                from_date=earliest_date,
                to_date=latest_date,
                split_on_truncated=True,
            )
            if not ref_docs:
                ref_docs = self._search_legal_pav(
                    instrument,
                    stats,
                    from_date=earliest_date,
                    to_date=latest_date,
                    split_on_truncated=True,
                )

            known_instruments, known_book_pages = self._reference_anchor_sets(docs_by_inst.values())
            filtered = [
                d
                for d in ref_docs
                if self._matches_property_or_reference(
                    d,
                    property_tokens,
                    anchor_instruments=known_instruments | {instrument},
                    anchor_book_pages=known_book_pages,
                )
            ]
            self._merge_docs(docs_by_inst, filtered)

            for doc in filtered:
                inst_refs, bkpg_refs = self._extract_references_from_doc(doc)
                for ref in inst_refs:
                    if ref not in chased_instruments:
                        queued_instruments.append(ref)
                for book, page in bkpg_refs:
                    key = f"{book}/{page}"
                    if key not in seen_book_pages:
                        seen_book_pages.add(key)
                        queued_book_pages.append((book, page))

        # Optional book/page chase using PAV API (not browser).
        for book, page in queued_book_pages[:30]:
            docs = self._search_book_page_pav(book, page, stats)
            filtered = [
                d
                for d in docs
                if self._matches_property_or_reference(
                    d,
                    property_tokens,
                    anchor_book_pages={(book, page)},
                )
            ]
            self._merge_docs(docs_by_inst, filtered)

        # Phase 3: guarded fallback (clerk cases + legal/address + party).
        # Run Phase 3 when doc count is low OR when there's a specific
        # coverage gap (zero mortgages, no lien for CC cases, etc.).
        _has_mortgage = any(normalize_document_type(d.get("DocType") or "") == "mortgage" for d in docs_by_inst.values())
        _has_lien = any(normalize_document_type(d.get("DocType") or "") == "lien" for d in docs_by_inst.values())
        _is_cc_case = len(case_number) >= 8 and "CC" in case_number[6:8]
        _needs_targeted_fallback = (
            not _has_mortgage  # every foreclosure must have a mortgage
            or not _has_lien  # superpriority liens (code enforcement, utility, tax) are never adjacent to deeds
            or _is_cc_case  # CC cases (enforce lien, real property) need broader search
        )
        _run_phase3 = len(docs_by_inst) < _MIN_DOCS_FOR_NO_FALLBACK or _needs_targeted_fallback
        if _run_phase3:
            _reason = "low_docs" if len(docs_by_inst) < _MIN_DOCS_FOR_NO_FALLBACK else "coverage_gap"
            _gaps = []
            if not _has_mortgage:
                _gaps.append("no_mortgage")
            if not _has_lien:
                _gaps.append("no_lien")
            if _is_cc_case:
                _gaps.append("cc_case")
            logger.info(
                "Fallback discovery enabled for case={} strap={} docs={} reason={} gaps={}",
                case_number,
                strap,
                len(docs_by_inst),
                _reason,
                ",".join(_gaps) if _gaps else "low_docs",
            )
            clerk_cases = self._get_clerk_case_seeds(target, ownership_chain)
            stats["clerk_case_count"] = len(clerk_cases)
            for cnum in clerk_cases[:_MAX_CLERK_CASE_SEEDS]:
                docs = self._search_case_pav(
                    cnum,
                    stats,
                    persist_case_number=cnum,
                    bypass_cache=lp_recovery_mode,
                )
                filtered = [d for d in docs if self._matches_property(d, property_tokens)]
                self._merge_docs(docs_by_inst, filtered)

            fallback_terms: list[str] = []
            for term in self._build_search_terms(target):
                if term not in fallback_terms:
                    fallback_terms.append(term)
            legal_line = self._extract_primary_legal_line(target)
            if legal_line and legal_line not in fallback_terms:
                fallback_terms.append(legal_line)
            street = self._extract_street_only(target.get("property_address") or "")
            if street and street not in fallback_terms:
                fallback_terms.append(street)

            for term in fallback_terms[:3]:
                docs = self._search_legal_pav(
                    term,
                    stats,
                    from_date=earliest_date,
                    to_date=latest_date,
                    split_on_truncated=True,
                )
                filtered = [d for d in docs if self._matches_property(d, property_tokens)]
                self._merge_docs(docs_by_inst, filtered)

            party_fallbacks: list[str] = []
            for name in (plaintiff, defendant, target.get("owner_name") or ""):
                clean = (name or "").strip()
                if clean and _is_generic_name(clean):
                    logger.debug("Skipping generic party '{}' for fallback search (case={})", clean, case_number)
                elif clean and clean not in party_fallbacks:
                    party_fallbacks.append(clean)
            for name in party_fallbacks[:2]:
                docs = self._search_party_pav(
                    name,
                    stats,
                    from_date=earliest_date,
                    to_date=latest_date,
                    split_on_truncated=True,
                )
                filtered = [d for d in docs if self._matches_property(d, property_tokens)]
                self._merge_docs(docs_by_inst, filtered)

        if not skip_live_noc_fallback and not any(_is_noc_type(doc) for doc in docs_by_inst.values()):
            live_noc_docs = self._run_live_noc_fallback(
                target=target,
                ownership_chain=ownership_chain,
                property_tokens=property_tokens,
                earliest_date=earliest_date,
                latest_date=latest_date,
                stats=stats,
            )
            if live_noc_docs:
                self._merge_docs(docs_by_inst, live_noc_docs)
                stats["live_noc_docs"] = len(live_noc_docs)

        # Final keep: only relevant document classes for saving.
        discovered = [
            d
            for d in docs_by_inst.values()
            if _is_encumbrance_type(d)
            or _is_assignment_type(d)
            or _is_noc_type(d)
            or normalize_document_type(d.get("DocType") or "") in CANONICAL_SATISFACTION_TYPES
            or normalize_document_type(d.get("DocType") or "") in CANONICAL_LIFECYCLE_TYPES
        ]
        return discovered, stats

    def _get_ownership_chain(self, strap: str) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT sale_date, grantor, grantee, sale_type, doc_num
                    FROM hcpa_allsales s
                    JOIN hcpa_bulk_parcels bp ON s.folio = bp.folio
                    WHERE bp.strap = :strap
                    ORDER BY sale_date
                """),
                {"strap": strap},
            ).fetchall()
        return [
            {
                "sale_date": r[0],
                "grantor": (r[1] or "").strip(),
                "grantee": (r[2] or "").strip(),
                "sale_type": (r[3] or "").strip(),
                "doc_num": str(r[4] or "").strip(),
            }
            for r in rows
            if str(r[4] or "").strip()
        ]

    def get_ownership_chain(self, strap: str) -> list[dict[str, Any]]:
        """Public wrapper for ownership-chain lookup used by adjacent services."""
        return self._get_ownership_chain(strap)

    def _get_clerk_case_seeds(
        self,
        target: dict[str, Any],
        chain: list[dict[str, Any]],
    ) -> list[str]:
        """Pull a small set of related case numbers from clerk parties table."""
        seeds: list[str] = []
        names: list[str] = []
        owner_name = (target.get("owner_name") or "").strip()
        if owner_name:
            names.append(owner_name)

        jdata = target.get("judgment_data") or {}
        defendant = (jdata.get("defendant") or "").strip()
        if defendant:
            names.append(defendant)

        for deed in chain[-10:]:
            grantee = (deed.get("grantee") or "").strip()
            if grantee:
                names.append(grantee)

        patterns = []
        for name in names:
            tokens = [t for t in re.split(r"\s+", name.upper()) if len(t) >= 3]
            if not tokens:
                continue
            token = tokens[0]
            pattern = f"%{token}%"
            if pattern not in patterns:
                patterns.append(pattern)

        this_case = (target.get("case_number") or "").upper()
        with self.engine.connect() as conn:
            for pattern in patterns[:8]:
                rows = conn.execute(
                    text("""
                        SELECT DISTINCT c.case_number
                        FROM clerk_civil_parties p
                        JOIN clerk_civil_cases c ON c.case_number = p.case_number
                        WHERE p.name ILIKE :pattern
                          AND (
                              UPPER(COALESCE(p.party_type, '')) LIKE '%DEF%'
                              OR UPPER(COALESCE(p.party_type, '')) LIKE '%RESP%'
                          )
                        LIMIT 10
                    """),
                    {"pattern": pattern},
                ).fetchall()
                for (case_number,) in rows:
                    cnum = str(case_number or "").upper().strip()
                    if not cnum or cnum == this_case or cnum in seeds:
                        continue
                    seeds.append(cnum)
                    if len(seeds) >= _MAX_CLERK_CASE_SEEDS:
                        return seeds
        return seeds

    @staticmethod
    def _case_variants(case_number: str) -> list[str]:
        variants: list[str] = []
        raw = (case_number or "").strip().upper()
        if not raw:
            return variants
        variants.append(raw)
        m = _CASE_VARIANT_RE.match(raw)
        if m:
            year4, ctype, seq = m.group(1), m.group(2), m.group(3)
            short = f"{year4[2:]}-{ctype}-{seq}"
            compact = f"{year4[2:]}{ctype}{seq}"
            for cand in (short, compact):
                if cand not in variants:
                    variants.append(cand)
        return variants

    def _merge_docs(self, docs_by_inst: dict[str, dict[str, Any]], docs: list[dict[str, Any]]) -> None:
        for doc in docs:
            instrument = _get_instrument(doc)
            if not instrument:
                continue
            if instrument not in docs_by_inst:
                docs_by_inst[instrument] = doc
                continue
            existing = docs_by_inst[instrument]
            if not existing.get("DocType") and doc.get("DocType"):
                existing["DocType"] = doc.get("DocType")
            if not existing.get("RecordDate") and doc.get("RecordDate"):
                existing["RecordDate"] = doc.get("RecordDate")
            if not existing.get("Legal") and doc.get("Legal"):
                existing["Legal"] = doc.get("Legal")
            if not existing.get("CaseNum") and doc.get("CaseNum"):
                existing["CaseNum"] = doc.get("CaseNum")
            if not existing.get("case_number") and doc.get("case_number"):
                existing["case_number"] = doc.get("case_number")
            # Preserve PAV document ID for downstream PDF download
            if not existing.get("ID") and doc.get("ID"):
                existing["ID"] = doc.get("ID")

            p1_existing = existing.get("PartiesOne") or []
            p2_existing = existing.get("PartiesTwo") or []
            p1_new = doc.get("PartiesOne") or []
            p2_new = doc.get("PartiesTwo") or []
            existing["PartiesOne"] = list(dict.fromkeys([*p1_existing, *p1_new]))
            existing["PartiesTwo"] = list(dict.fromkeys([*p2_existing, *p2_new]))

    @staticmethod
    def _earliest_relevant_date(
        chain: list[dict[str, Any]],
        target: dict[str, Any],
    ) -> date:
        sale_dates = [d["sale_date"] for d in chain if isinstance(d.get("sale_date"), date)]
        if sale_dates:
            return min(sale_dates)
        filing_date = target.get("filing_date")
        if isinstance(filing_date, date):
            return filing_date - timedelta(days=3650)
        return date(1990, 1, 1)

    @staticmethod
    def _extract_primary_legal_line(target: dict[str, Any]) -> str:
        for legal_field in ("legal1", "legal2", "legal3", "legal4"):
            value = (target.get(legal_field) or "").strip()
            if value and len(value) >= 8:
                return value
        return ""

    @staticmethod
    def _extract_street_only(address: str) -> str:
        if not address:
            return ""
        return address.split(",", maxsplit=1)[0].strip()

    @staticmethod
    def _extract_street_number(address: str) -> str:
        if not address:
            return ""
        match = re.match(r"\s*(\d{3,6}[A-Z]?)\b", address.strip().upper())
        return match.group(1) if match else ""

    def _seed_from_official_records(
        self,
        *,
        target: dict[str, Any],
        earliest_date: date,
        latest_date: date,
        property_tokens: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Pull high-confidence ORI docs from official_records_daily_instruments.

        Matching strategy:
        - Case number variants in legal/doc description (highest confidence)
        - Legal/address phrase matches
        - Owner/party token overlap in grantor/grantee text
        """
        case_variants = [v for v in self._case_variants(target.get("case_number") or "") if v]
        case_patterns = [f"%{v.lower()}%" for v in case_variants]
        case_variants_upper = [v.upper() for v in case_variants]

        legal_terms: list[str] = []
        for term in self._build_search_terms(target)[:3]:
            clean = (term or "").strip()
            if clean and clean.lower() not in {t.lower() for t in legal_terms}:
                legal_terms.append(clean)
        primary_legal = self._extract_primary_legal_line(target)
        if primary_legal and primary_legal.lower() not in {t.lower() for t in legal_terms}:
            legal_terms.append(primary_legal)
        street = self._extract_street_only(target.get("property_address") or "")
        if street and street.lower() not in {t.lower() for t in legal_terms}:
            legal_terms.append(street)
        legal_patterns = [f"%{term.lower()}%" for term in legal_terms if len(term) >= 4]
        legal_terms_upper = [term.upper() for term in legal_terms]

        party_tokens = self._seed_party_tokens(target)
        party_patterns = [f"%{token.lower()}%" for token in party_tokens]

        params: dict[str, Any] = {
            "from_date": earliest_date,
            "to_date": latest_date,
            "max_rows": _MAX_OFFICIAL_RECORDS_CANDIDATES,
        }
        predicates: list[str] = []

        for i, pattern in enumerate(case_patterns):
            key = f"case_like_{i}"
            params[key] = pattern
            predicates.append(f"LOWER(COALESCE(ori.legal_description, '')) LIKE :{key}")
            predicates.append(f"LOWER(COALESCE(ori.doc_description, '')) LIKE :{key}")

        for i, pattern in enumerate(legal_patterns):
            key = f"legal_like_{i}"
            params[key] = pattern
            predicates.append(f"LOWER(COALESCE(ori.legal_description, '')) LIKE :{key}")
            predicates.append(f"LOWER(COALESCE(ori.doc_description, '')) LIKE :{key}")

        for i, pattern in enumerate(party_patterns):
            key = f"party_like_{i}"
            params[key] = pattern
            predicates.append(f"LOWER(COALESCE(ori.parties_from_text, '')) LIKE :{key}")
            predicates.append(f"LOWER(COALESCE(ori.parties_to_text, '')) LIKE :{key}")

        if not predicates:
            return []

        sql = f"""
            SELECT
                ori.instrument_number,
                ori.doc_type,
                ori.facc_doc_type,
                ori.doc_description,
                ori.legal_description,
                ori.recording_date,
                ori.book_type,
                ori.book_number,
                ori.page_number,
                ori.parties_from_json,
                ori.parties_to_json,
                ori.parties_from_text,
                ori.parties_to_text
            FROM official_records_daily_instruments ori
            WHERE (
                ori.recording_date IS NULL
                OR (ori.recording_date BETWEEN :from_date AND :to_date)
            )
              AND ({" OR ".join(predicates)})
              AND ori.doc_type IN (
                  'MTG','MTGNT','MTGNIT','MTGREV','DOT','HELOC','AGD',
                  'JUD','CCJ','FJ','DRJUD','CTF',
                  'LN','LNCORPTX','FIN','MEDLN','HOA','MECH','CEL','SA','SPECASMT','ML',
                  'LP','RELLP',
                  'SAT','SATCORPTX','SATMTG','RELMTG',
                  'REL','PR','TER','PRREL',
                  'ASG','ASGT','ASGN','ASGNMTG','ASINT',
                  'ORD','DRCP',
                  'NOC','MOD','SUB','NCL','EAS'
              )
            ORDER BY ori.recording_date DESC NULLS LAST, ori.instrument_number
            LIMIT :max_rows
        """

        docs_by_inst: dict[str, dict[str, Any]] = {}
        with self.engine.connect() as conn:
            rows = conn.execute(text(sql), params).mappings().all()

        for row in rows:
            instrument = str(row.get("instrument_number") or "").strip()
            if not instrument:
                continue

            raw_doc_type = str(row.get("doc_type") or row.get("facc_doc_type") or "").strip().upper()
            if not raw_doc_type:
                continue

            parties_one = self._coerce_party_list(row.get("parties_from_json"))
            parties_two = self._coerce_party_list(row.get("parties_to_json"))
            party1_text = str(row.get("parties_from_text") or "").strip() or ", ".join(parties_one)
            party2_text = str(row.get("parties_to_text") or "").strip() or ", ".join(parties_two)

            legal = str(row.get("legal_description") or "").strip()
            doc_desc = str(row.get("doc_description") or "").strip()
            legal_blob = f"{legal} {doc_desc}".strip()

            doc = {
                "Instrument": instrument,
                "DocType": raw_doc_type,
                "RecordDate": (row.get("recording_date").isoformat() if row.get("recording_date") else ""),
                "BookType": "OR"
                if str(row.get("book_type") or "OR").strip() in ("O", "OR", "")
                else str(row.get("book_type")).strip(),
                "Book": str(row.get("book_number") or "").strip(),
                "Page": str(row.get("page_number") or "").strip(),
                "Legal": legal_blob,
                "PartiesOne": parties_one,
                "PartiesTwo": parties_two,
                "party1": party1_text,
                "party2": party2_text,
            }

            score = self._official_match_score(
                doc=doc,
                case_variants_upper=case_variants_upper,
                legal_terms_upper=legal_terms_upper,
                party_tokens_upper=party_tokens,
                property_tokens=property_tokens,
            )
            if score < _MIN_OFFICIAL_MATCH_SCORE:
                continue

            existing = docs_by_inst.get(instrument)
            if existing is None:
                docs_by_inst[instrument] = doc
                continue
            if not existing.get("Legal") and doc.get("Legal"):
                existing["Legal"] = doc["Legal"]
            existing["PartiesOne"] = list(dict.fromkeys([*(existing.get("PartiesOne") or []), *parties_one]))
            existing["PartiesTwo"] = list(dict.fromkeys([*(existing.get("PartiesTwo") or []), *parties_two]))

        return list(docs_by_inst.values())

    @staticmethod
    def _coerce_party_list(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        if isinstance(value, str):
            txt = value.strip()
            if not txt:
                return []
            try:
                parsed = json.loads(txt)
                if isinstance(parsed, list):
                    return [str(v).strip() for v in parsed if str(v).strip()]
            except json.JSONDecodeError:
                pass
            return [txt]
        return [str(value).strip()] if str(value).strip() else []

    @staticmethod
    def _seed_party_tokens(target: dict[str, Any]) -> list[str]:
        stop_words = {
            "THE",
            "AND",
            "BANK",
            "NATIONAL",
            "ASSOCIATION",
            "TRUST",
            "TRUSTEE",
            "TRUSTEES",
            "COMPANY",
            "CORP",
            "CORPORATION",
            "LLC",
            "INC",
            "NA",
            "FKA",
            "DBA",
            "MORTGAGE",
            "LOAN",
            "SERVICING",
        }
        tokens: list[str] = []
        jdata = target.get("judgment_data") or {}
        sources = [
            target.get("owner_name") or "",
            jdata.get("defendant") or "",
            jdata.get("plaintiff") or "",
        ]
        for source in sources:
            for token in re.split(r"[^A-Z0-9]+", str(source).upper()):
                if len(token) < 4 or token in stop_words:
                    continue
                if token not in tokens:
                    tokens.append(token)
                if len(tokens) >= 8:
                    return tokens
        return tokens

    def _official_noc_coverage_start(self) -> date:
        """Return the earliest NOC recording date present in the local seed feed."""
        if self._official_noc_coverage_start_cache is not None:
            return self._official_noc_coverage_start_cache

        fallback = datetime.now(tz=UTC).date() - timedelta(days=365 * _RECENT_PERMIT_FALLBACK_YEARS)
        sql = text("""
            SELECT MIN(recording_date)
            FROM official_records_daily_instruments
            WHERE COALESCE(recording_date, DATE '1900-01-01') > DATE '1900-01-01'
              AND (
                    UPPER(COALESCE(doc_type, '')) LIKE '%NOC%'
                 OR UPPER(COALESCE(facc_doc_type, '')) LIKE '%NOC%'
                 OR UPPER(COALESCE(doc_description, '')) LIKE '%NOTICE OF COMMENCEMENT%'
              )
        """)

        with self.engine.connect() as conn:
            value = conn.execute(sql).scalar()

        self._official_noc_coverage_start_cache = value if isinstance(value, date) else fallback
        return self._official_noc_coverage_start_cache

    def _target_has_recent_permit_signal(self, target: dict[str, Any]) -> bool:
        """Return True when a property has recent permit activity worth live NOC probing."""
        folio = (target.get("folio") or "").strip()
        property_address = (target.get("property_address") or "").strip()
        if not folio and not property_address:
            return False

        coverage_start = self._official_noc_coverage_start()
        sql = text("""
            SELECT
                EXISTS (
                    SELECT 1
                    FROM county_permits cp
                    WHERE :folio <> ''
                      AND regexp_replace(
                            COALESCE(cp.folio_clean, cp.folio_raw, ''),
                            '[^0-9]',
                            '',
                            'g'
                          ) = regexp_replace(:folio, '[^0-9]', '', 'g')
                      AND (
                            cp.issue_date >= :coverage_start
                         OR (
                                COALESCE(cp.permit_number, '') ~ '^HC-(BLD|BTR)-[0-9]{2}-'
                            AND 2000 + CAST(
                                    split_part(COALESCE(cp.permit_number, ''), '-', 3) AS INTEGER
                                ) >= :coverage_year
                         )
                      )
                ) AS county_recent,
                EXISTS (
                    SELECT 1
                    FROM tampa_accela_records tr
                    WHERE :property_address <> ''
                      AND btrim(COALESCE(tr.address_normalized, tr.address_raw, '')) <> ''
                      AND upper(trim(
                            split_part(
                                replace(COALESCE(tr.address_normalized, tr.address_raw, ''), E'\\t', ' '),
                                ',',
                                1
                            )
                          )) = upper(trim(
                            split_part(replace(:property_address, E'\\t', ' '), ',', 1)
                          ))
                      AND COALESCE(tr.is_violation, FALSE) = FALSE
                      AND COALESCE(tr.module, '') <> 'Business'
                      AND COALESCE(tr.record_number, '') NOT LIKE 'BTX-%'
                      AND COALESCE(tr.record_type, '') NOT ILIKE 'Tax Receipt%'
                      AND (
                            tr.record_date >= :coverage_start
                         OR (
                                COALESCE(tr.record_number, '') ~ '^(BLD|BTR)-[0-9]{2}-'
                            AND 2000 + CAST(
                                    split_part(COALESCE(tr.record_number, ''), '-', 2) AS INTEGER
                                ) >= :coverage_year
                         )
                      )
                ) AS tampa_recent
        """)

        with self.engine.connect() as conn:
            row = conn.execute(
                sql,
                {
                    "folio": folio,
                    "property_address": property_address,
                    "coverage_start": coverage_start,
                    "coverage_year": coverage_start.year,
                },
            ).one()

        return bool(row[0] or row[1])

    def _collect_noc_party_terms(
        self,
        target: dict[str, Any],
        ownership_chain: list[dict[str, Any]],
    ) -> list[str]:
        terms: list[str] = []

        def add(value: str) -> None:
            clean = value.strip()
            if not clean or _is_generic_name(clean) or clean in terms:
                return
            terms.append(clean)

        add(str(target.get("owner_name") or ""))

        jdata = target.get("judgment_data") or {}
        add(str(jdata.get("defendant") or ""))

        for deed in reversed(ownership_chain[-10:]):
            add(str(deed.get("grantee") or ""))
            add(str(deed.get("grantor") or ""))
            if len(terms) >= 6:
                break

        return terms[:6]

    def _run_live_noc_fallback(
        self,
        *,
        target: dict[str, Any],
        ownership_chain: list[dict[str, Any]],
        property_tokens: dict[str, Any],
        earliest_date: date,
        latest_date: date,
        stats: dict[str, int],
    ) -> list[dict[str, Any]]:
        """Run bounded live NOC discovery for recent permit-backed properties."""
        if not self._target_has_recent_permit_signal(target):
            return []

        case_number = (target.get("case_number") or "").strip()
        strap = (target.get("strap") or "").strip()
        logger.info(
            "Running targeted live NOC fallback for case={} strap={}",
            case_number,
            strap,
        )

        search_start = max(earliest_date, self._official_noc_coverage_start())
        docs_by_inst: dict[str, dict[str, Any]] = {}
        legal_terms: list[str] = []
        seen_terms: set[str] = set()

        for term in self._build_search_terms(target):
            clean = (term or "").strip()
            key = clean.upper()
            if clean and key not in seen_terms:
                legal_terms.append(clean)
                seen_terms.add(key)

        primary_legal = self._extract_primary_legal_line(target)
        if primary_legal and primary_legal.upper() not in seen_terms:
            legal_terms.append(primary_legal)

        for term in legal_terms[:3]:
            docs = self._search_noc_legal_pav(
                term,
                stats,
                from_date=search_start,
                to_date=latest_date,
                split_on_truncated=True,
            )
            filtered = [d for d in docs if self._matches_property(d, property_tokens)]
            self._merge_docs(docs_by_inst, filtered)

        if not docs_by_inst:
            for name in self._collect_noc_party_terms(target, ownership_chain)[:4]:
                docs = self._search_noc_party_pav(
                    name,
                    stats,
                    from_date=search_start,
                    to_date=latest_date,
                    split_on_truncated=True,
                )
                filtered = [d for d in docs if self._matches_property(d, property_tokens)]
                self._merge_docs(docs_by_inst, filtered)
                if docs_by_inst:
                    break

        if not docs_by_inst:
            street = self._extract_street_only(target.get("property_address") or "")
            if street:
                docs = self._search_noc_full_text_pav(
                    street,
                    stats,
                    from_date=search_start,
                    to_date=latest_date,
                )
                filtered = [d for d in docs if self._matches_property(d, property_tokens)]
                self._merge_docs(docs_by_inst, filtered)

        return list(docs_by_inst.values())

    def _official_match_score(
        self,
        *,
        doc: dict[str, Any],
        case_variants_upper: list[str],
        legal_terms_upper: list[str],
        party_tokens_upper: list[str],
        property_tokens: dict[str, Any],
    ) -> int:
        legal_text = (doc.get("Legal") or "").upper()
        parties_text = " ".join([
            *(doc.get("PartiesOne") or []),
            *(doc.get("PartiesTwo") or []),
            (doc.get("party1") or ""),
            (doc.get("party2") or ""),
        ]).upper()
        doc_type = normalize_document_type(doc.get("DocType") or "")
        matches_property = self._matches_property(doc, property_tokens)

        if doc_type == "noc" and not matches_property:
            return 0

        score = 0
        if any(v and v in legal_text for v in case_variants_upper):
            score += 5

        legal_hits = 0
        for term in legal_terms_upper:
            if term and term in legal_text:
                legal_hits += 1
        score += min(4, legal_hits * 2)

        party_hits = 0
        for token in party_tokens_upper:
            if token and token in parties_text:
                party_hits += 1
        score += min(2, party_hits)

        if matches_property:
            score += 2

        if (
            doc_type in CANONICAL_ENCUMBRANCE_TYPES
            or doc_type in CANONICAL_SATISFACTION_TYPES
            or normalize_encumbrance_type(doc_type) == "assignment"
        ):
            score += 1

        return score

    def _build_property_tokens(
        self,
        target: dict[str, Any],
        ownership_chain: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        legal_tokens: set[str] = set()
        legal_locators: set[tuple[str, str]] = set()
        for legal_field in ("legal1", "legal2", "legal3", "legal4"):
            value = (target.get(legal_field) or "").upper()
            words = [w for w in re.split(r"[^A-Z0-9]+", value) if len(w) >= 3 and w not in _LEGAL_BOILERPLATE_TOKENS]
            legal_tokens.update(words[:8])
            legal_locators.update((match.group(1).upper(), match.group(2).upper()) for match in _LEGAL_LOCATOR_RE.finditer(value))

        owner_names: set[str] = set()
        if target.get("owner_name"):
            owner_names.add(target["owner_name"].strip().upper())

        if ownership_chain:
            for deed in ownership_chain:
                grantee = str(deed.get("grantee") or "").strip().upper()
                grantor = str(deed.get("grantor") or "").strip().upper()
                if grantee and grantee != "NONE":
                    owner_names.add(grantee)
                if grantor and grantor != "NONE":
                    owner_names.add(grantor)

        street_tokens = {
            t
            for t in re.split(r"[^A-Z0-9]+", self._extract_street_only(target.get("property_address") or "").upper())
            if len(t) >= 3
        }
        street_name_tokens = {t for t in street_tokens if not re.fullmatch(r"\d+[A-Z]?", t) and t not in _STREET_STOP_TOKENS}
        return {
            "legal_tokens": legal_tokens,
            "legal_locators": list(legal_locators),
            "owner_names": list(owner_names),
            "street_number": self._extract_street_number(target.get("property_address") or ""),
            "street_name_tokens": street_name_tokens,
            "street_tokens": street_tokens,
            "case_number": (target.get("case_number") or "").strip().upper(),
        }

    def build_property_tokens(
        self,
        target: dict[str, Any],
        ownership_chain: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Public wrapper for property-token generation."""
        return self._build_property_tokens(target, ownership_chain)

    @staticmethod
    def _word_boundary_hits(tokens: set[str], text: str) -> int:
        """Count tokens that appear as whole words in text (not substrings)."""
        return sum(1 for t in tokens if re.search(rf"\b{re.escape(t)}\b", text))

    @staticmethod
    def _has_property_text_match(doc: dict[str, Any], tokens: dict[str, Any]) -> bool:
        legal_text = (doc.get("Legal") or "").upper()
        if not legal_text:
            return False

        legal_tokens = tokens.get("legal_tokens") or set()
        legal_hits = PgOriService._word_boundary_hits(legal_tokens, legal_text)

        street_tokens = tokens.get("street_tokens") or set()
        street_hits = PgOriService._word_boundary_hits(street_tokens, legal_text)
        street_name_tokens = tokens.get("street_name_tokens") or {
            t for t in street_tokens if not re.fullmatch(r"\d+[A-Z]?", str(t)) and str(t) not in _STREET_STOP_TOKENS
        }
        street_name_hits = PgOriService._word_boundary_hits(street_name_tokens, legal_text)

        legal_locators = tokens.get("legal_locators") or []
        locator_hits = 0
        locator_matches: dict[str, set[str]] = {}
        locator_expected: dict[str, set[str]] = {}
        for label, value in legal_locators:
            locator_expected.setdefault(label, set()).add(value)
            locator_pattern = rf"\b{re.escape(label)}\b(?:\s+NO\.?)?\s*{re.escape(value)}\b"
            if re.search(locator_pattern, legal_text):
                locator_hits += 1
                locator_matches.setdefault(label, set()).add(value)

        if _is_noc_type(doc):
            street_number = str(tokens.get("street_number") or "").strip().upper()
            has_explicit_street_address = bool(_ADDRESS_LINE_RE.search(legal_text))
            if has_explicit_street_address:
                if street_number:
                    return bool(re.search(rf"\b{re.escape(street_number)}\b", legal_text)) and street_name_hits >= 1
                return locator_hits > 0 and street_name_hits >= 1

            if "LOT" in locator_expected:
                lot_match = bool(locator_matches.get("LOT"))
                block_expected = "BLOCK" in locator_expected or "BLK" in locator_expected
                block_match = bool(locator_matches.get("BLOCK") or locator_matches.get("BLK"))
                if block_expected:
                    return lot_match and block_match and legal_hits >= 1
                return lot_match and legal_hits >= 1

            if "UNIT" in locator_expected:
                return bool(locator_matches.get("UNIT")) and legal_hits >= 1

            if "BLOCK" in locator_expected or "BLK" in locator_expected:
                return bool(locator_matches.get("BLOCK") or locator_matches.get("BLK")) and legal_hits >= 2

            if legal_locators:
                return locator_hits > 0 and legal_hits >= 1

            return bool(legal_tokens) and legal_hits >= min(3, len(legal_tokens))

        # Non-NOC documents: require locator match when we have locators
        # (prevents unrelated condo liens from matching on generic tokens).
        # Check LOT before UNIT — LOT/BLOCK is more specific than UNIT
        # (subdivisions may have a UNIT phase number + LOT/BLOCK identifiers).
        if "LOT" in locator_expected:
            lot_match = bool(locator_matches.get("LOT"))
            block_expected = "BLOCK" in locator_expected or "BLK" in locator_expected
            block_match = bool(locator_matches.get("BLOCK") or locator_matches.get("BLK"))
            if block_expected:
                return lot_match and block_match and legal_hits >= 1
            return lot_match and legal_hits >= 1

        if "UNIT" in locator_expected:
            return bool(locator_matches.get("UNIT")) and legal_hits >= 1

        if street_hits >= min(2, len(street_tokens)):
            return True

        # Scale threshold with token count — require at least 40% of tokens
        min_required = max(2, (len(legal_tokens) * 2 + 4) // 5)  # ~40%, min 2
        return bool(legal_tokens) and legal_hits >= min_required

    @staticmethod
    def _matches_property(doc: dict[str, Any], tokens: dict[str, Any]) -> bool:
        if PgOriService._has_property_text_match(doc, tokens):
            return True

        raw_type = doc.get("DocType") or doc.get("document_type") or doc.get("doc_type") or ""
        doc_type = normalize_document_type(raw_type)
        enc_type = normalize_encumbrance_type(doc_type or raw_type)
        prop_case = (tokens.get("case_number") or "").strip().upper()
        doc_case = (doc.get("CaseNum") or doc.get("case_number") or "").strip().upper()

        # LP/JUD documents with an explicit case number from a different
        # foreclosure must not attach to this parcel just because the owner name
        # overlaps (for example HUD- or bank-owned REO inventory).
        is_lp_or_judgment = doc_type in {"lis_pendens", "judgment"} or enc_type in {"lis_pendens", "judgment"}
        if is_lp_or_judgment and doc_case and prop_case and doc_case != prop_case:
            return False

        if _is_noc_type(doc):
            return False

        # Owner-name-only matching is too broad for parcel attachment. It can
        # pull in unrelated foreclosure docs for REO inventory or institutional
        # owners (for example HUD-owned parcels), even when the legal text and
        # case number do not match this property.
        # LP/JUD docs: keep only if they belong to this foreclosure case.
        return bool(
            is_lp_or_judgment
            and doc_case
            and prop_case
            and doc_case == prop_case
        )

    @staticmethod
    def matches_property(doc: dict[str, Any], tokens: dict[str, Any]) -> bool:
        """Public wrapper for property-level document matching."""
        return PgOriService._matches_property(doc, tokens)

    @staticmethod
    def _reference_anchor_sets(
        docs: Any,
    ) -> tuple[set[str], set[tuple[str, str]]]:
        instruments: set[str] = set()
        book_pages: set[tuple[str, str]] = set()
        for doc in docs:
            instrument = _get_instrument(doc)
            if instrument:
                instruments.add(instrument)
            book = str(doc.get("Book") or doc.get("book") or "").strip()
            page = str(doc.get("Page") or doc.get("page") or "").strip()
            if book and page:
                book_pages.add((book, page))
        return instruments, book_pages

    def _matches_property_or_reference(
        self,
        doc: dict[str, Any],
        property_tokens: dict[str, Any],
        *,
        anchor_instruments: set[str] | None = None,
        anchor_book_pages: set[tuple[str, str]] | None = None,
    ) -> bool:
        if self._matches_property(doc, property_tokens):
            return True
        if _is_noc_type(doc):
            return False

        raw_type = doc.get("DocType") or doc.get("document_type") or doc.get("doc_type") or ""
        canonical = normalize_document_type(raw_type)
        enc_type = normalize_encumbrance_type(canonical or raw_type)
        if not (
            canonical in CANONICAL_ENCUMBRANCE_TYPES
            or canonical in CANONICAL_SATISFACTION_TYPES
            or canonical in CANONICAL_LIFECYCLE_TYPES
            or enc_type == "assignment"
        ):
            return False

        instrument_refs, book_page_refs = self._extract_references_from_doc(doc)
        if anchor_instruments and any(ref in anchor_instruments for ref in instrument_refs):
            return True
        return bool(anchor_book_pages and any(ref in anchor_book_pages for ref in book_page_refs))

    def _extract_references_from_doc(
        self,
        doc: dict[str, Any],
    ) -> tuple[list[str], list[tuple[str, str]]]:
        text_blob = " ".join([
            doc.get("Legal") or "",
            doc.get("party1") or "",
            doc.get("party2") or "",
            " ".join(doc.get("PartiesOne") or []),
            " ".join(doc.get("PartiesTwo") or []),
        ])
        refs: set[str] = set()
        for pattern in _INST_REF_PATTERNS:
            refs.update(match.group(1) for match in pattern.finditer(text_blob))
        own = _get_instrument(doc)
        if own and own in refs:
            refs.remove(own)

        book_pages: list[tuple[str, str]] = []
        for match in _BKPG_REF_PATTERN.finditer(text_blob):
            book = match.group(1).strip()
            page = match.group(2).strip()
            if book and page:
                book_pages.append((book, page))
        return sorted(refs), book_pages

    @staticmethod
    def _extract_case_numbers(doc: dict[str, Any]) -> set[str]:
        values = [
            (doc.get("CaseNum") or ""),
            (doc.get("case_number") or ""),
            (doc.get("Legal") or ""),
        ]
        joined = " ".join(values).upper()
        found: set[str] = set()
        found.update(m.group(0) for m in re.finditer(r"\b\d{2}-[A-Z]{2}-\d{6}\b", joined))
        found.update(m.group(0) for m in re.finditer(r"\b\d{2}\d{4}[A-Z]{2}\d{6}[A-Z0-9]*\b", joined))
        return found

    def _search_case_pav(
        self,
        case_number: str,
        stats: dict[str, int],
        *,
        persist_case_number: str | None = None,
        bypass_cache: bool = False,
    ) -> list[dict[str, Any]]:
        docs = self._pav_search(
            query_id=350,
            keywords=[(1259, case_number)],
            query_label=f"case:{case_number}",
            stats=stats,
            bypass_cache=bypass_cache,
        )
        canonical_case = (persist_case_number or case_number or "").strip().upper()
        if canonical_case:
            for doc in docs:
                if not doc.get("CaseNum") and not doc.get("case_number"):
                    doc["CaseNum"] = canonical_case
        return docs

    def _search_instrument_pav(self, instrument: str, stats: dict[str, int]) -> list[dict[str, Any]]:
        return self._pav_search(
            query_id=320,
            keywords=[(1006, instrument)],
            query_label=f"instrument:{instrument}",
            stats=stats,
        )

    def _search_legal_pav(
        self,
        text_value: str,
        stats: dict[str, int],
        *,
        from_date: date,
        to_date: date,
        split_on_truncated: bool,
        depth: int = 0,
    ) -> list[dict[str, Any]]:
        return self._pav_search(
            query_id=321,
            keywords=[(1011, text_value)],
            query_label=f"legal:{text_value}",
            stats=stats,
            from_date=from_date,
            to_date=to_date,
            split_on_truncated=split_on_truncated,
            depth=depth,
        )

    def _search_noc_legal_pav(
        self,
        text_value: str,
        stats: dict[str, int],
        *,
        from_date: date,
        to_date: date,
        split_on_truncated: bool,
        depth: int = 0,
    ) -> list[dict[str, Any]]:
        docs = self._pav_search(
            query_id=321,
            keywords=[(1011, text_value), (1285, _PAV_NOC_DOC_TYPE)],
            query_label=f"noc_legal:{text_value}",
            stats=stats,
            from_date=from_date,
            to_date=to_date,
            split_on_truncated=split_on_truncated,
            depth=depth,
        )
        if docs:
            return docs
        fallback_docs = self._search_legal_pav(
            text_value,
            stats,
            from_date=from_date,
            to_date=to_date,
            split_on_truncated=split_on_truncated,
            depth=depth,
        )
        return [doc for doc in fallback_docs if _is_noc_type(doc)]

    def _search_party_pav(
        self,
        name: str,
        stats: dict[str, int],
        *,
        from_date: date,
        to_date: date,
        split_on_truncated: bool,
        depth: int = 0,
    ) -> list[dict[str, Any]]:
        return self._pav_search(
            query_id=326,
            keywords=[(486, name)],
            query_label=f"party:{name}",
            stats=stats,
            from_date=from_date,
            to_date=to_date,
            split_on_truncated=split_on_truncated,
            depth=depth,
        )

    def _search_noc_party_pav(
        self,
        name: str,
        stats: dict[str, int],
        *,
        from_date: date,
        to_date: date,
        split_on_truncated: bool,
        depth: int = 0,
    ) -> list[dict[str, Any]]:
        docs = self._pav_search(
            query_id=326,
            keywords=[(486, name), (1285, _PAV_NOC_DOC_TYPE)],
            query_label=f"noc_party:{name}",
            stats=stats,
            from_date=from_date,
            to_date=to_date,
            split_on_truncated=split_on_truncated,
            depth=depth,
        )
        if docs:
            return docs
        fallback_docs = self._search_party_pav(
            name,
            stats,
            from_date=from_date,
            to_date=to_date,
            split_on_truncated=split_on_truncated,
            depth=depth,
        )
        return [doc for doc in fallback_docs if _is_noc_type(doc)]

    def search_party_pav(
        self,
        name: str,
        stats: dict[str, int],
        *,
        from_date: date,
        to_date: date,
        split_on_truncated: bool,
        depth: int = 0,
    ) -> list[dict[str, Any]]:
        """Public wrapper for party-name PAV searches."""
        return self._search_party_pav(
            name,
            stats,
            from_date=from_date,
            to_date=to_date,
            split_on_truncated=split_on_truncated,
            depth=depth,
        )

    def search_legal_pav(
        self,
        text_value: str,
        stats: dict[str, int],
        *,
        from_date: date,
        to_date: date,
        split_on_truncated: bool,
        depth: int = 0,
        ) -> list[dict[str, Any]]:
        """Public wrapper for legal-description PAV searches."""
        return self._search_legal_pav(
            text_value,
            stats,
            from_date=from_date,
            to_date=to_date,
            split_on_truncated=split_on_truncated,
            depth=depth,
        )

    def discover_exact_references(
        self,
        *,
        strap: str,
        folio: str | None,
        instruments: list[str] | None = None,
        book_pages: list[tuple[str, str]] | None = None,
        allowed_types: set[str] | None = None,
    ) -> dict[str, Any]:
        """Discover and persist documents reachable by exact instrument/book-page refs."""

        dedup_instruments = sorted({
            str(instrument).strip()
            for instrument in (instruments or [])
            if str(instrument).strip()
        })
        dedup_book_pages = sorted({
            (str(book).strip(), str(page).strip())
            for book, page in (book_pages or [])
            if str(book).strip() and str(page).strip()
        })
        if not dedup_instruments and not dedup_book_pages:
            return {
                "searched_instruments": 0,
                "searched_book_pages": 0,
                "docs_found": 0,
                "saved": 0,
                "api_calls": 0,
                "linked_satisfactions": 0,
                "linked_modifications": 0,
            }

        stats: dict[str, int] = {
            "api_calls": 0,
            "retries": 0,
            "truncated": 0,
            "unresolved_truncations": 0,
            "cache_hits": 0,
        }
        docs_by_inst: dict[str, dict[str, Any]] = {}

        for instrument in dedup_instruments:
            self._merge_docs(docs_by_inst, self._search_instrument_pav(instrument, stats))

        for book, page in dedup_book_pages:
            self._merge_docs(docs_by_inst, self._search_book_page_pav(book, page, stats))

        docs = list(docs_by_inst.values())
        if allowed_types:
            docs = [
                doc
                for doc in docs
                if normalize_document_type(doc.get("DocType") or "") in allowed_types
            ]

        saved = self._save_documents(strap, folio, docs) if docs else 0
        linked_satisfactions = 0
        linked_modifications = 0
        if saved > 0:
            linked_satisfactions = self._link_satisfactions(strap)
            linked_modifications = self._link_modifications(strap)

        return {
            "searched_instruments": len(dedup_instruments),
            "searched_book_pages": len(dedup_book_pages),
            "docs_found": len(docs),
            "saved": saved,
            "api_calls": int(stats.get("api_calls", 0)),
            "linked_satisfactions": linked_satisfactions,
            "linked_modifications": linked_modifications,
        }

    def _search_book_page_pav(
        self,
        book: str,
        page: str,
        stats: dict[str, int],
    ) -> list[dict[str, Any]]:
        return self._pav_search(
            query_id=319,
            keywords=[(1530, "O"), (573, book), (1049, page)],
            query_label=f"book_page:{book}/{page}",
            stats=stats,
        )

    def _search_noc_full_text_pav(
        self,
        text_value: str,
        stats: dict[str, int],
        *,
        from_date: date,
        to_date: date,
    ) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {
            "DocTypeID": _PAV_NOC_DOC_TYPE_ID,
            "SearchText": text_value,
            "FromDate": from_date.strftime("%m/%d/%Y"),
            "ToDate": to_date.strftime("%m/%d/%Y"),
            "Keywords": [],
            "QueryLimit": _PAV_QUERY_LIMIT,
        }
        data = self._post_pav_full_text(payload, f"noc_full_text:{text_value}", stats)
        if data is None:
            return []

        docs = self._parse_pav_full_text_rows(data.get("Data") or [])
        if bool(data.get("Truncated")):
            stats["truncated"] += 1
        return docs

    def _pav_search(
        self,
        *,
        query_id: int,
        keywords: list[tuple[int, str]],
        query_label: str,
        stats: dict[str, int],
        from_date: date | None = None,
        to_date: date | None = None,
        split_on_truncated: bool = False,
        depth: int = 0,
        bypass_cache: bool = False,
    ) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {
            "QueryID": query_id,
            "Keywords": [{"Id": key_id, "Value": value} for key_id, value in keywords],
            "QueryLimit": _PAV_QUERY_LIMIT,
        }
        if from_date is not None:
            payload["FromDate"] = from_date.strftime("%m/%d/%Y")
        if to_date is not None:
            payload["ToDate"] = to_date.strftime("%m/%d/%Y")

        data = self._post_pav(
            payload,
            query_label,
            stats,
            bypass_cache=bypass_cache,
        )
        if data is None:
            return []

        docs = self._parse_pav_rows(data.get("Data") or [])
        truncated = bool(data.get("Truncated"))
        if truncated:
            stats["truncated"] += 1

        if (
            truncated
            and split_on_truncated
            and from_date is not None
            and to_date is not None
            and from_date < to_date
            and depth < _PAV_SPLIT_DEPTH
        ):
            midpoint = from_date + timedelta(days=(to_date - from_date).days // 2)
            left = self._pav_search(
                query_id=query_id,
                keywords=keywords,
                query_label=query_label,
                stats=stats,
                from_date=from_date,
                to_date=midpoint,
                split_on_truncated=split_on_truncated,
                depth=depth + 1,
            )
            right = self._pav_search(
                query_id=query_id,
                keywords=keywords,
                query_label=query_label,
                stats=stats,
                from_date=midpoint + timedelta(days=1),
                to_date=to_date,
                split_on_truncated=split_on_truncated,
                depth=depth + 1,
            )
            merged: dict[str, dict[str, Any]] = {}
            self._merge_docs(merged, left)
            self._merge_docs(merged, right)
            return list(merged.values())

        if truncated and split_on_truncated:
            stats["unresolved_truncations"] += 1
            logger.warning(
                "Skipping '{label}' from {from_date} to {to_date}: Search matched >1500 records. "
                "This usually happens for massive corporate entities (like 'LENNAR HOMES INC'). "
                "The system ignores these to prevent polluting the database with false positives.",
                label=query_label.replace("party:", ""),
                from_date=from_date,
                to_date=to_date,
            )

        return docs

    def _post_pav(
        self,
        payload: dict[str, Any],
        query_label: str,
        stats: dict[str, int],
        *,
        bypass_cache: bool = False,
    ) -> dict[str, Any] | None:
        # --- disk cache check ---
        if not bypass_cache:
            cached = pav_cache_get(payload)
            if cached is not None:
                stats.setdefault("cache_hits", 0)
                stats["cache_hits"] += 1
                return cached

        for attempt in range(1, _PAV_MAX_RETRIES + 1):
            stats["api_calls"] += 1
            try:
                response = self._pav_session.post(
                    _PAV_KEYWORD_URL,
                    json=payload,
                    timeout=_PAV_TIMEOUT_SECONDS,
                )
                if response.status_code == 200:
                    try:
                        data = response.json()
                    except ValueError as exc:
                        body_sample = (response.text or "").replace("\n", " ")[:200]
                        logger.warning(
                            "PAV returned invalid JSON label={} attempt={}/{}: {} body={}",
                            query_label,
                            attempt,
                            _PAV_MAX_RETRIES,
                            exc,
                            body_sample,
                        )
                    else:
                        if isinstance(data, dict):
                            if not bypass_cache:
                                pav_cache_put(payload, data)
                            return data
                        logger.warning(
                            "PAV returned non-object JSON label={} attempt={}/{} type={}",
                            query_label,
                            attempt,
                            _PAV_MAX_RETRIES,
                            type(data).__name__,
                        )
                logger.warning(
                    "PAV HTTP {} label={} attempt={}/{}",
                    response.status_code,
                    query_label,
                    attempt,
                    _PAV_MAX_RETRIES,
                )
            except requests.RequestException as exc:
                logger.warning(
                    "PAV request failed label={} attempt={}/{}: {}",
                    query_label,
                    attempt,
                    _PAV_MAX_RETRIES,
                    exc,
                )

            if attempt < _PAV_MAX_RETRIES:
                stats["retries"] += 1
                time.sleep(0.5 * attempt)

        logger.error(
            "PAV request failed after retries: label={} query_id={} keywords={} from={} to={}",
            query_label,
            payload.get("QueryID"),
            payload.get("Keywords"),
            payload.get("FromDate"),
            payload.get("ToDate"),
        )
        return None

    def _post_pav_full_text(
        self,
        payload: dict[str, Any],
        query_label: str,
        stats: dict[str, int],
    ) -> dict[str, Any] | None:
        cache_payload = {"_endpoint": "full_text", **payload}
        cached = pav_cache_get(cache_payload)
        if cached is not None:
            stats.setdefault("cache_hits", 0)
            stats["cache_hits"] += 1
            return cached

        for attempt in range(1, _PAV_FULL_TEXT_RETRIES + 1):
            stats["api_calls"] += 1
            try:
                response = self._pav_session.post(
                    _PAV_FULL_TEXT_URL,
                    json=payload,
                    timeout=_PAV_FULL_TEXT_TIMEOUT_SECONDS,
                )
                if response.status_code == 200:
                    try:
                        data = response.json()
                    except ValueError as exc:
                        body_sample = (response.text or "").replace("\n", " ")[:200]
                        logger.warning(
                            "PAV full-text returned invalid JSON label={} attempt={}/{}: {} body={}",
                            query_label,
                            attempt,
                            _PAV_FULL_TEXT_RETRIES,
                            exc,
                            body_sample,
                        )
                    else:
                        if isinstance(data, dict):
                            pav_cache_put(cache_payload, data)
                            return data
                        logger.warning(
                            "PAV full-text returned non-object JSON label={} attempt={}/{} type={}",
                            query_label,
                            attempt,
                            _PAV_FULL_TEXT_RETRIES,
                            type(data).__name__,
                        )
                logger.warning(
                    "PAV full-text HTTP {} label={} attempt={}/{}",
                    response.status_code,
                    query_label,
                    attempt,
                    _PAV_FULL_TEXT_RETRIES,
                )
            except requests.RequestException as exc:
                logger.warning(
                    "PAV full-text request failed label={} attempt={}/{}: {}",
                    query_label,
                    attempt,
                    _PAV_FULL_TEXT_RETRIES,
                    exc,
                )

            if attempt < _PAV_FULL_TEXT_RETRIES:
                stats["retries"] += 1
                time.sleep(0.5 * attempt)

        logger.error(
            "PAV full-text request failed after retries: label={} doc_type_id={} search={} from={} to={}",
            query_label,
            payload.get("DocTypeID"),
            payload.get("SearchText"),
            payload.get("FromDate"),
            payload.get("ToDate"),
        )
        return None

    def _parse_pav_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            doc_id = row.get("ID")
            cols = row.get("DisplayColumnValues") or []
            if len(cols) < 9:
                continue
            values = [str(col.get("Value") or "").strip() for col in cols[:9]]
            values.extend([""] * (9 - len(values)))

            person_type = values[0].upper()
            name = values[1]
            record_date = values[2]
            doc_type = values[3]
            book_type = "OR" if values[4] in {"O", "OR", ""} else values[4]
            book_num = values[5]
            page_num = values[6]
            legal = values[7]
            instrument = values[8]
            if not instrument:
                continue

            doc = grouped.get(instrument)
            if doc is None:
                doc = {
                    "Instrument": instrument,
                    "DocType": doc_type,
                    "RecordDate": record_date,
                    "BookType": book_type,
                    "Book": book_num,
                    "Page": page_num,
                    "Legal": legal,
                    "PartiesOne": [],
                    "PartiesTwo": [],
                    "ID": doc_id,
                }
                grouped[instrument] = doc

            if name:
                if "2" in person_type or "GRANTEE" in person_type:
                    if name not in doc["PartiesTwo"]:
                        doc["PartiesTwo"].append(name)
                elif name not in doc["PartiesOne"]:
                    doc["PartiesOne"].append(name)
        return list(grouped.values())

    def _parse_pav_full_text_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        docs: dict[str, dict[str, Any]] = {}
        for row in rows:
            name_blob = str(row.get("Name") or "").strip()
            summary = str(row.get("Summary") or "").strip()
            if not name_blob:
                continue

            instrument_match = re.search(r"Inst\.\s*#:\s*(\d{7,10})", name_blob, re.IGNORECASE)
            if not instrument_match:
                continue
            instrument = instrument_match.group(1)

            doc_type = name_blob.split(" Record Date -", 1)[0].strip() or "NOC"

            record_date = ""
            date_match = re.search(r"Record Date -\s*(.*?)\s+Name -", name_blob, re.IGNORECASE)
            if date_match:
                record_date = date_match.group(1).strip()

            parties_one: list[str] = []
            parties_two: list[str] = []
            parties_match = re.search(r"Name -\s*(.*?)\s*,\s*Inst\.\s*#:", name_blob, re.IGNORECASE)
            if parties_match:
                raw_parties = [part.strip() for part in parties_match.group(1).split(" - ") if part.strip()]
                if raw_parties:
                    parties_one = [raw_parties[0]]
                if len(raw_parties) > 1:
                    parties_two = raw_parties[1:]

            docs[instrument] = {
                "Instrument": instrument,
                "DocType": doc_type,
                "RecordDate": record_date,
                "BookType": "OR",
                "Book": "",
                "Page": "",
                "Legal": summary,
                "PartiesOne": parties_one,
                "PartiesTwo": parties_two,
                "ID": row.get("ID"),
            }

        return list(docs.values())

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
            logger.warning(f"No legal search terms for {case}")
        else:
            for term in search_terms:
                state.enqueue(
                    _SearchItem(
                        search_type="legal",
                        term=term,
                        priority=10,
                    )
                )

        # --- Seed: case number search (priority 15) ---
        if case:
            state.enqueue(
                _SearchItem(
                    search_type="case",
                    term=case,
                    priority=15,
                )
            )

        # --- Seed: judgment party names (priority 20) ---
        jdata = target.get("judgment_data") or {}
        plaintiff = (jdata.get("plaintiff") or "").strip()
        defendant = (jdata.get("defendant") or "").strip()
        if plaintiff and not _is_generic_name(plaintiff):
            state.enqueue(
                _SearchItem(
                    search_type="party",
                    term=plaintiff,
                    priority=20,
                )
            )
        elif plaintiff:
            logger.debug("Skipping generic plaintiff '{}' for iterative search", plaintiff)
        if defendant and not _is_generic_name(defendant):
            state.enqueue(
                _SearchItem(
                    search_type="party",
                    term=defendant,
                    priority=20,
                )
            )
        elif defendant:
            logger.debug("Skipping generic defendant '{}' for iterative search", defendant)

        if not state.queue:
            logger.warning(f"No iterative ORI seeds for {case}")
            return [], 0

        # --- Main loop ---
        while state.iteration < max_iterations:
            item = state.pop_next()
            if item is None:
                logger.debug(f"  Queue exhausted after {state.iteration} iterations")
                break

            state.iteration += 1

            if len(state.all_docs) >= _MAX_DOCUMENTS:
                logger.warning(f"  Max documents ({_MAX_DOCUMENTS}) reached for {case}")
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
            logger.warning(f"Search failed ({item.search_type} '{item.term}'): {exc}")
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
        recording_date = self._parse_date(doc.get("RecordDate") or doc.get("record_date"))

        # --- 1. Referenced instruments (priority 25) ---
        for ref_inst in _extract_instrument_references(doc):
            state.enqueue(
                _SearchItem(
                    search_type="instrument",
                    term=ref_inst,
                    priority=25,
                    source_instrument=own_instrument,
                )
            )

        # --- 2. Party name searches (priority 30) ---
        grantors, grantees = _get_parties(doc)

        for name in grantors:
            if _is_generic_name(name):
                logger.debug("Skipping generic grantor '{}' from doc {}", name, own_instrument)
                continue
            # Grantor owned *before* this recording date
            date_to = _format_mm_dd_yyyy(recording_date)
            state.enqueue(
                _SearchItem(
                    search_type="party",
                    term=name,
                    date_to=date_to,
                    priority=30,
                    source_instrument=own_instrument,
                )
            )

        for name in grantees:
            if _is_generic_name(name):
                logger.debug("Skipping generic grantee '{}' from doc {}", name, own_instrument)
                continue
            # Grantee owned *after* this recording date
            date_from = _format_mm_dd_yyyy(recording_date)
            state.enqueue(
                _SearchItem(
                    search_type="party",
                    term=name,
                    date_from=date_from,
                    priority=30,
                    source_instrument=own_instrument,
                )
            )

        # --- 3. Book/page references (priority 35) ---
        book = (doc.get("Book") or doc.get("book") or doc.get("book_num") or "").strip()
        page = (doc.get("Page") or doc.get("page") or doc.get("page_num") or "").strip()
        if book and page:
            state.enqueue(
                _SearchItem(
                    search_type="book_page",
                    term=f"{book}/{page}",
                    priority=35,
                    source_instrument=own_instrument,
                )
            )

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
                search = " ".join(parts[: min(8, len(parts))])
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
        strap: str | None,
        folio: str | None,
        documents: list[dict],
    ) -> int:
        """Classify ORI documents and save encumbrances to PG."""
        saved = 0
        skipped = 0
        eligible = 0
        with self.engine.begin() as conn:
            for doc in documents:
                raw_type = doc.get("DocType") or doc.get("document_type") or doc.get("doc_type") or ""
                canonical = normalize_document_type(raw_type)
                enc_type = normalize_encumbrance_type(canonical or raw_type)

                # Only save encumbrance-type, satisfaction-type, assignment-type,
                # and NOC documents (skip deeds, affidavits, etc.)
                is_encumbrance = canonical in CANONICAL_ENCUMBRANCE_TYPES
                is_satisfaction = canonical in CANONICAL_SATISFACTION_TYPES
                is_assignment = enc_type == "assignment"
                is_noc = canonical in CANONICAL_NOC_TYPES or enc_type == "noc"
                is_lifecycle = canonical in CANONICAL_LIFECYCLE_TYPES
                if not (is_encumbrance or is_satisfaction or is_assignment or is_noc or is_lifecycle):
                    continue

                if enc_type not in _PG_ENCUMBRANCE_TYPES:
                    enc_type = "other"

                instrument = str(doc.get("Instrument") or doc.get("instrument_number") or doc.get("instrument") or "").strip()
                if not instrument:
                    continue
                eligible += 1

                # Parse parties
                party1 = doc.get("party1") or ""
                party2 = doc.get("party2") or ""
                parties_one = doc.get("PartiesOne") or []
                parties_two = doc.get("PartiesTwo") or []
                if not party1 and parties_one:
                    party1 = ", ".join((p.get("Name", "") if isinstance(p, dict) else str(p)) for p in parties_one if p)
                if not party2 and parties_two:
                    party2 = ", ".join((p.get("Name", "") if isinstance(p, dict) else str(p)) for p in parties_two if p)

                # Fields are pre-normalized by ORIApiScraper._normalize_result()
                recording_date = self._parse_date(doc.get("RecordDate") or doc.get("record_date"))
                book = (doc.get("Book") or doc.get("book") or "").strip() or None
                page = (doc.get("Page") or doc.get("page") or "").strip() or None
                book_type = str(doc.get("BookType") or doc.get("book_type") or "OR").strip()
                # PAV API returns 'O' for Official Records; normalize to 'OR'
                if book_type == "O":
                    book_type = "OR"
                amount = doc.get("SalesPrice") or doc.get("sales_price")
                case_number = doc.get("CaseNum") or doc.get("case_number")
                if not case_number:
                    extracted_cases = sorted(self._extract_case_numbers(doc))
                    if extracted_cases:
                        case_number = extracted_cases[0]
                legal = doc.get("Legal") or doc.get("legal_description") or doc.get("legal")
                ori_uuid = doc.get("UUID") or doc.get("ori_uuid")
                # Pre-truncated to 64 chars by _normalize_result()
                ori_id = doc.get("ID") or doc.get("ori_id")

                # Determine if satisfaction
                is_sat = canonical in CANONICAL_SATISFACTION_TYPES

                params = {
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
                    "p1_json": (json.dumps(parties_one) if parties_one else None),
                    "p2_json": (json.dumps(parties_two) if parties_two else None),
                    "amount": float(amount) if amount else None,
                    "rec_date": recording_date or "",
                    "case_number": case_number,
                    "legal": legal,
                    "is_sat_insert": is_sat,
                    "is_sat_update": True if is_sat else None,
                }

                conn.execute(text("SAVEPOINT ori_doc"))
                try:
                    existing = conn.execute(
                        text("""
                            UPDATE ori_encumbrances
                            SET folio = COALESCE(:folio, folio),
                                strap = COALESCE(:strap, strap),
                                book = COALESCE(:book, book),
                                page = COALESCE(:page, page),
                                book_type = COALESCE(:book_type, book_type),
                                ori_uuid = COALESCE(:ori_uuid, ori_uuid),
                                ori_id = COALESCE(:ori_id, ori_id),
                                raw_document_type = COALESCE(:raw_type, raw_document_type),
                                encumbrance_type = COALESCE(
                                    CAST(:enc_type AS encumbrance_type_enum),
                                    encumbrance_type
                                ),
                                party1 = COALESCE(:party1, party1),
                                party2 = COALESCE(:party2, party2),
                                parties_one_json = COALESCE(CAST(:p1_json AS JSONB), parties_one_json),
                                parties_two_json = COALESCE(CAST(:p2_json AS JSONB), parties_two_json),
                                amount = COALESCE(:amount, amount),
                                recording_date = COALESCE(
                                    CAST(NULLIF(:rec_date, '') AS DATE),
                                    recording_date
                                ),
                                case_number = COALESCE(:case_number, case_number),
                                legal_description = COALESCE(:legal, legal_description),
                                is_satisfied = COALESCE(:is_sat_update, is_satisfied),
                                updated_at = now()
                            WHERE instrument_number = :instrument
                              AND (
                                  folio IS NOT DISTINCT FROM :folio
                                  OR strap = :strap
                              )
                              AND (
                                  (CAST(:folio AS TEXT) IS NOT NULL AND folio IS DISTINCT FROM :folio)
                                  OR (CAST(:strap AS TEXT) IS NOT NULL AND strap IS DISTINCT FROM :strap)
                                  OR (CAST(:book AS TEXT) IS NOT NULL AND book IS DISTINCT FROM :book)
                                  OR (CAST(:page AS TEXT) IS NOT NULL AND page IS DISTINCT FROM :page)
                                  OR (CAST(:book_type AS TEXT) IS NOT NULL AND book_type IS DISTINCT FROM :book_type)
                                  OR (CAST(:ori_uuid AS TEXT) IS NOT NULL AND ori_uuid IS DISTINCT FROM :ori_uuid)
                                  OR (CAST(:ori_id AS TEXT) IS NOT NULL AND ori_id IS DISTINCT FROM :ori_id)
                                  OR (CAST(:raw_type AS TEXT) IS NOT NULL AND raw_document_type IS DISTINCT FROM :raw_type)
                                  OR (
                                      CAST(:enc_type AS TEXT) IS NOT NULL
                                      AND encumbrance_type IS DISTINCT FROM CAST(:enc_type AS encumbrance_type_enum)
                                  )
                                  OR (CAST(:party1 AS TEXT) IS NOT NULL AND party1 IS DISTINCT FROM :party1)
                                  OR (CAST(:party2 AS TEXT) IS NOT NULL AND party2 IS DISTINCT FROM :party2)
                                  OR (
                                      CAST(:p1_json AS TEXT) IS NOT NULL
                                      AND parties_one_json IS DISTINCT FROM CAST(:p1_json AS JSONB)
                                  )
                                  OR (
                                      CAST(:p2_json AS TEXT) IS NOT NULL
                                      AND parties_two_json IS DISTINCT FROM CAST(:p2_json AS JSONB)
                                  )
                                  OR (CAST(:amount AS NUMERIC) IS NOT NULL AND amount IS DISTINCT FROM :amount)
                                  OR (
                                      CAST(:rec_date AS TEXT) != ''
                                      AND recording_date IS DISTINCT FROM CAST(NULLIF(:rec_date, '') AS DATE)
                                  )
                                  OR (
                                      CAST(:case_number AS TEXT) IS NOT NULL
                                      AND case_number IS DISTINCT FROM :case_number
                                  )
                                  OR (CAST(:legal AS TEXT) IS NOT NULL AND legal_description IS DISTINCT FROM :legal)
                                  OR (
                                      CAST(:is_sat_update AS BOOLEAN) IS TRUE
                                      AND is_satisfied IS DISTINCT FROM TRUE
                                  )
                              )
                        """),
                        params,
                    )
                    changed = int(existing.rowcount or 0)
                    if changed == 0:
                        inserted = conn.execute(
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
                                :is_sat_insert,
                                now(), now()
                            )
                            ON CONFLICT (folio, COALESCE(instrument_number, ''),
                                         COALESCE(book, ''), COALESCE(page, ''),
                                         COALESCE(book_type, 'OR'))
                            DO UPDATE SET
                                strap = COALESCE(EXCLUDED.strap, ori_encumbrances.strap),
                                ori_uuid = COALESCE(EXCLUDED.ori_uuid, ori_encumbrances.ori_uuid),
                                ori_id = COALESCE(EXCLUDED.ori_id, ori_encumbrances.ori_id),
                                raw_document_type = COALESCE(
                                    EXCLUDED.raw_document_type,
                                    ori_encumbrances.raw_document_type
                                ),
                                encumbrance_type = COALESCE(
                                    EXCLUDED.encumbrance_type,
                                    ori_encumbrances.encumbrance_type
                                ),
                                party1 = COALESCE(EXCLUDED.party1, ori_encumbrances.party1),
                                party2 = COALESCE(EXCLUDED.party2, ori_encumbrances.party2),
                                parties_one_json = COALESCE(
                                    EXCLUDED.parties_one_json,
                                    ori_encumbrances.parties_one_json
                                ),
                                parties_two_json = COALESCE(
                                    EXCLUDED.parties_two_json,
                                    ori_encumbrances.parties_two_json
                                ),
                                amount = COALESCE(EXCLUDED.amount, ori_encumbrances.amount),
                                recording_date = COALESCE(
                                    EXCLUDED.recording_date,
                                    ori_encumbrances.recording_date
                                ),
                                case_number = COALESCE(EXCLUDED.case_number, ori_encumbrances.case_number),
                                legal_description = COALESCE(
                                    EXCLUDED.legal_description,
                                    ori_encumbrances.legal_description
                                ),
                                is_satisfied = COALESCE(
                                    CASE
                                        WHEN EXCLUDED.is_satisfied IS TRUE THEN TRUE
                                        ELSE NULL
                                    END,
                                    ori_encumbrances.is_satisfied
                                ),
                                updated_at = now()
                            WHERE
                                (EXCLUDED.strap IS NOT NULL AND ori_encumbrances.strap IS DISTINCT FROM EXCLUDED.strap)
                                OR (
                                    EXCLUDED.ori_uuid IS NOT NULL
                                    AND ori_encumbrances.ori_uuid IS DISTINCT FROM EXCLUDED.ori_uuid
                                )
                                OR (
                                    EXCLUDED.ori_id IS NOT NULL
                                    AND ori_encumbrances.ori_id IS DISTINCT FROM EXCLUDED.ori_id
                                )
                                OR (
                                    EXCLUDED.raw_document_type IS NOT NULL
                                    AND ori_encumbrances.raw_document_type IS DISTINCT FROM EXCLUDED.raw_document_type
                                )
                                OR (
                                    EXCLUDED.encumbrance_type IS NOT NULL
                                    AND ori_encumbrances.encumbrance_type IS DISTINCT FROM EXCLUDED.encumbrance_type
                                )
                                OR (
                                    EXCLUDED.party1 IS NOT NULL
                                    AND ori_encumbrances.party1 IS DISTINCT FROM EXCLUDED.party1
                                )
                                OR (
                                    EXCLUDED.party2 IS NOT NULL
                                    AND ori_encumbrances.party2 IS DISTINCT FROM EXCLUDED.party2
                                )
                                OR (
                                    EXCLUDED.parties_one_json IS NOT NULL
                                    AND ori_encumbrances.parties_one_json IS DISTINCT FROM EXCLUDED.parties_one_json
                                )
                                OR (
                                    EXCLUDED.parties_two_json IS NOT NULL
                                    AND ori_encumbrances.parties_two_json IS DISTINCT FROM EXCLUDED.parties_two_json
                                )
                                OR (
                                    EXCLUDED.amount IS NOT NULL
                                    AND ori_encumbrances.amount IS DISTINCT FROM EXCLUDED.amount
                                )
                                OR (
                                    EXCLUDED.recording_date IS NOT NULL
                                    AND ori_encumbrances.recording_date IS DISTINCT FROM EXCLUDED.recording_date
                                )
                                OR (
                                    EXCLUDED.case_number IS NOT NULL
                                    AND ori_encumbrances.case_number IS DISTINCT FROM EXCLUDED.case_number
                                )
                                OR (
                                    EXCLUDED.legal_description IS NOT NULL
                                    AND ori_encumbrances.legal_description IS DISTINCT FROM EXCLUDED.legal_description
                                )
                                OR (
                                    EXCLUDED.is_satisfied IS TRUE
                                    AND ori_encumbrances.is_satisfied IS DISTINCT FROM TRUE
                                )
                        """),
                            params,
                        )
                        changed = int(inserted.rowcount or 0)
                    conn.execute(text("RELEASE SAVEPOINT ori_doc"))
                    saved += changed
                except Exception as exc:
                    conn.execute(text("ROLLBACK TO SAVEPOINT ori_doc"))
                    logger.warning(f"Skip document {instrument}: {exc}")
                    skipped += 1

        self._last_save_documents_stats = {
            "saved": saved,
            "skipped": skipped,
            "eligible": eligible,
        }
        return saved

    def _infer_from_judgment(
        self,
        strap: str | None,
        folio: str | None,
        target: dict,
    ) -> int:
        """Create inferred encumbrance from judgment data when ORI finds nothing."""
        case_number = target.get("case_number", "")
        self._last_infer_from_judgment_stats = {
            "saved": 0,
            "reason": "not_run",
        }
        if not folio or not strap or strap == "MULTIPLE PARCEL":
            self._last_infer_from_judgment_stats = {
                "saved": 0,
                "reason": "missing_property_identity",
            }
            logger.info(
                "Skip inferred encumbrance for case={} due to missing folio/strap",
                case_number,
            )
            return 0

        jdata = target.get("judgment_data") or {}
        plaintiff = jdata.get("plaintiff") or ""
        defendant = jdata.get("defendant") or ""
        if not plaintiff:
            self._last_infer_from_judgment_stats = {
                "saved": 0,
                "reason": "missing_plaintiff",
            }
            logger.info(
                "Skip inferred encumbrance for case={} because judgment plaintiff is missing",
                case_number,
            )
            return 0

        is_cc = len(case_number) >= 8 and "CC" in case_number[6:8]
        plaintiff_upper = plaintiff.upper()
        is_hoa = any(kw in plaintiff_upper for kw in ("ASSOCIATION", "HOA", "CONDO", "HOMEOWNER"))
        enc_type = "lien" if is_cc or is_hoa else "mortgage"

        # Extract amount from judgment
        foreclosed = jdata.get("foreclosed_mortgage") or {}
        amount = foreclosed.get("original_amount") or jdata.get("judgment_amount")
        if amount:
            try:
                amount = float(amount)
            except (ValueError, TypeError):
                amount = None

        recording_date = (
            foreclosed.get("recording_date")
            or foreclosed.get("original_date")
            or jdata.get("filing_date")
            or jdata.get("judgment_date")
            or target.get("filing_date")
        )
        instrument = f"INFERRED-{case_number}"

        with self.engine.begin() as conn:
            # Check idempotency
            existing = conn.execute(
                text("SELECT id FROM ori_encumbrances WHERE strap = :strap AND instrument_number = :inst"),
                {"strap": strap, "inst": instrument},
            ).fetchone()

            if existing:
                self._last_infer_from_judgment_stats = {
                    "saved": 0,
                    "reason": "existing_inferred_encumbrance",
                }
                logger.info(
                    "Skip inferred encumbrance for case={} because {} already exists",
                    case_number,
                    instrument,
                )
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

        self._last_infer_from_judgment_stats = {
            "saved": 1,
            "reason": "inserted",
        }
        logger.info(f"Inferred {enc_type} encumbrance for {case_number}: plaintiff={plaintiff}")
        return 1

    def _link_satisfactions(self, strap: str) -> int:
        """Link SAT/REL documents to their parent mortgages using PG data only.

        Matching strategies (in priority order):
        1. Instrument reference — SAT legal_description contains 'CLK #NNNN' or 'INST #NNNN'
        2. Book/page reference — SAT legal_description contains 'OR BK NNN PG NNN'
        3. Case number match — SAT and MTG share the same case_number

        Returns the number of mortgages newly marked as satisfied.
        """
        linked = 0
        with self.engine.begin() as conn:
            if not self._ori_satisfaction_link_columns_available(conn):
                logger.warning(
                    "Skipping satisfaction linking for strap={} because ori_encumbrances is missing satisfaction link columns",
                    strap,
                )
                return 0

            # Get all SAT/REL docs for this strap. Reruns can repair parent rows
            # that were overwritten by a later generic upsert.
            sat_rows = conn.execute(
                text("""
                    SELECT id, instrument_number, legal_description, party1,
                           party2, recording_date, case_number
                    FROM ori_encumbrances
                    WHERE strap = :strap
                      AND encumbrance_type IN ('satisfaction', 'release')
                """),
                {"strap": strap},
            ).fetchall()

            if not sat_rows:
                return 0

            # Get all parent encumbrances for this strap.
            enc_rows = conn.execute(
                text("""
                    SELECT id, instrument_number, book, page, case_number,
                           party1, party2, amount, recording_date
                    FROM ori_encumbrances
                    WHERE strap = :strap
                      AND encumbrance_type IN ('mortgage', 'lien', 'judgment')
                """),
                {"strap": strap},
            ).fetchall()

            if not enc_rows:
                return 0

            # Build lookup structures for encumbrances
            enc_by_inst: dict[str, Any] = {}
            enc_by_bkpg: dict[str, Any] = {}
            enc_by_case: dict[str, list] = {}
            for enc in enc_rows:
                inst = (enc[1] or "").strip()
                if inst:
                    enc_by_inst[inst] = enc
                bk = (enc[2] or "").strip()
                pg = (enc[3] or "").strip()
                if bk and pg:
                    enc_by_bkpg[f"{bk}/{pg}"] = enc
                cn = (enc[4] or "").strip()
                if cn:
                    enc_by_case.setdefault(cn, []).append(enc)

            for sat in sat_rows:
                sat_id = sat[0]
                sat_inst = sat[1] or ""
                legal = (sat[2] or "").upper()
                sat_case = (sat[6] or "").strip()

                matched_enc = None
                method = None

                # Strategy 1: Instrument reference in legal description
                inst_patterns = re.findall(r"(?:CLK\s*#|INST\s*#|INSTRUMENT\s*(?:NO\.?\s*)?#?)\s*(\d{7,})", legal)
                for ref_inst in inst_patterns:
                    if ref_inst in enc_by_inst:
                        matched_enc = enc_by_inst[ref_inst]
                        method = "instrument_reference"
                        break

                # Strategy 2: Book/page reference in legal description
                if not matched_enc:
                    bkpg_matches = re.findall(r"OR\s+BK\s+(\d+)\s+PG\s+(\d+)", legal)
                    for bk, pg in bkpg_matches:
                        key = f"{bk}/{pg}"
                        if key in enc_by_bkpg:
                            matched_enc = enc_by_bkpg[key]
                            method = "book_page_reference"
                            break

                # Strategy 3: Case number match (only if SAT has a case number)
                if not matched_enc and sat_case and sat_case in enc_by_case:
                    candidates = enc_by_case[sat_case]
                    if len(candidates) == 1:
                        matched_enc = candidates[0]
                        method = "case_number_match"

                # Strategy 4: Party name + date heuristic (cross-match)
                # ORI party roles flip between doc types:
                #   Mortgage: party1=borrower, party2=lender
                #   SAT/REL:  party1=lender,   party2=borrower
                # So we cross-match: sat_party1↔enc_party2 (bank-to-bank)
                # and sat_party2↔enc_party1 (borrower-to-borrower).
                if not matched_enc:
                    sat_party1 = (sat[3] or "").strip().upper()  # lender on SAT
                    sat_party2 = (sat[4] or "").strip().upper()  # borrower on SAT
                    sat_date = sat[5]  # recording_date
                    if (sat_party1 or sat_party2) and sat_date:
                        from rapidfuzz.fuzz import token_set_ratio

                        candidates = []
                        for enc in enc_rows:
                            enc_party1 = (enc[5] or "").strip().upper()  # borrower on MTG
                            enc_party2 = (enc[6] or "").strip().upper()  # lender on MTG
                            enc_date = enc[8]  # recording_date
                            if not enc_date:
                                continue
                            if sat_date <= enc_date:
                                continue
                            # Bank-to-bank OR borrower-to-borrower
                            bank_score = token_set_ratio(sat_party1, enc_party2) if sat_party1 and enc_party2 else 0
                            borrower_score = token_set_ratio(sat_party2, enc_party1) if sat_party2 and enc_party1 else 0
                            if bank_score >= 85 or borrower_score >= 85:
                                candidates.append(enc)
                        if len(candidates) == 1:
                            matched_enc = candidates[0]
                            method = "party_date_heuristic"

                if matched_enc:
                    enc_id = matched_enc[0]
                    # Mark the encumbrance as satisfied
                    parent_update = conn.execute(
                        text("""
                            UPDATE ori_encumbrances
                            SET is_satisfied = true,
                                satisfaction_date = :sat_date,
                                satisfaction_instrument = :sat_inst,
                                satisfaction_method = CAST(:method AS satisfaction_link_method),
                                updated_at = now()
                            WHERE id = :enc_id
                              AND (
                                  is_satisfied IS DISTINCT FROM TRUE
                                  OR satisfaction_date IS DISTINCT FROM :sat_date
                                  OR satisfaction_instrument IS DISTINCT FROM :sat_inst
                                  OR satisfaction_method IS DISTINCT FROM CAST(:method AS satisfaction_link_method)
                              )
                        """),
                        {
                            "sat_date": sat[5],
                            "sat_inst": sat_inst,
                            "method": method,
                            "enc_id": enc_id,
                        },
                    )
                    # Also link the satisfaction row back to the encumbrance
                    conn.execute(
                        text("""
                            UPDATE ori_encumbrances
                            SET satisfies_encumbrance_id = :enc_id,
                                satisfaction_method = CAST(:method AS satisfaction_link_method),
                                updated_at = now()
                            WHERE id = :sat_id
                              AND (
                                  satisfies_encumbrance_id IS DISTINCT FROM :enc_id
                                  OR satisfaction_method IS DISTINCT FROM CAST(:method AS satisfaction_link_method)
                              )
                        """),
                        {"enc_id": enc_id, "method": method, "sat_id": sat_id},
                    )
                    linked += parent_update.rowcount
                    logger.debug(
                        "Linked satisfaction {} → encumbrance {} via {} for strap={}",
                        sat_inst,
                        matched_enc[1],
                        method,
                        strap,
                    )

        if linked:
            logger.info("Linked {} satisfaction(s) for strap={}", linked, strap)
        return linked

    def _link_modifications(self, strap: str) -> int:
        """Link MOD/SUB/NCL/CTF lifecycle docs to parent encumbrances.

        Matching strategies (same priority order as satisfaction linking):
        1. Instrument reference — legal_description contains CLK#/INST# ref
        2. Book/page reference — legal_description contains OR BK/PG ref
        3. Case number match — shared case_number with unambiguous parent

        Returns count of newly linked modification docs.
        """
        linked = 0
        with self.engine.begin() as conn:
            # Guard: check column exists (migration may not have run yet)
            col_check = conn.execute(
                text("""
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'ori_encumbrances'
                      AND column_name = 'modifies_encumbrance_id'
                """)
            ).fetchone()
            if not col_check:
                return 0

            mod_rows = conn.execute(
                text("""
                    SELECT id, instrument_number, legal_description,
                           case_number, raw_document_type
                    FROM ori_encumbrances
                    WHERE strap = :strap
                      AND encumbrance_type = 'other'
                      AND raw_document_type IN ('MOD', 'SUB', 'NCL', 'CTF')
                      AND modifies_encumbrance_id IS NULL
                """),
                {"strap": strap},
            ).fetchall()

            if not mod_rows:
                return 0

            enc_rows = conn.execute(
                text("""
                    SELECT id, instrument_number, book, page, case_number
                    FROM ori_encumbrances
                    WHERE strap = :strap
                      AND encumbrance_type IN ('mortgage', 'lien', 'judgment')
                """),
                {"strap": strap},
            ).fetchall()

            if not enc_rows:
                return 0

            enc_by_inst: dict[str, Any] = {}
            enc_by_bkpg: dict[str, Any] = {}
            enc_by_case: dict[str, list] = {}
            for enc in enc_rows:
                inst = (enc[1] or "").strip()
                if inst:
                    enc_by_inst[inst] = enc
                bk = (enc[2] or "").strip()
                pg = (enc[3] or "").strip()
                if bk and pg:
                    enc_by_bkpg[f"{bk}/{pg}"] = enc
                cn = (enc[4] or "").strip()
                if cn:
                    enc_by_case.setdefault(cn, []).append(enc)

            for mod in mod_rows:
                mod_id = mod[0]
                legal = (mod[2] or "").upper()
                mod_case = (mod[3] or "").strip()
                matched = None

                # Strategy 1: Instrument reference
                for pat in _INST_REF_PATTERNS:
                    for ref_inst in pat.findall(legal):
                        if ref_inst in enc_by_inst:
                            matched = enc_by_inst[ref_inst]
                            break
                    if matched:
                        break

                # Strategy 2: Book/page reference
                if not matched:
                    for bk, pg in _BKPG_REF_PATTERN.findall(legal):
                        key = f"{bk}/{pg}"
                        if key in enc_by_bkpg:
                            matched = enc_by_bkpg[key]
                            break

                # Strategy 3: Case number (unambiguous only)
                if not matched and mod_case and mod_case in enc_by_case:
                    candidates = enc_by_case[mod_case]
                    if len(candidates) == 1:
                        matched = candidates[0]

                if matched:
                    conn.execute(
                        text("""
                            UPDATE ori_encumbrances
                            SET modifies_encumbrance_id = :parent_id,
                                updated_at = now()
                            WHERE id = :mod_id
                        """),
                        {"parent_id": matched[0], "mod_id": mod_id},
                    )
                    linked += 1

        if linked:
            logger.info("Linked {} modification doc(s) for strap={}", linked, strap)
        return linked

    def _chase_unlinked_sat_parents(self, strap: str, folio: str | None) -> int:
        """Chase CLK#/INST# refs in unlinked SAT/REL docs to discover parent mortgages.

        SAT/REL documents contain instrument references (CLK#, INST#, O.R.) to
        the parent mortgage they satisfy, but that parent mortgage may never have
        been discovered by the standard ORI search phases. This method extracts
        those references, looks up the missing instruments via PAV API, persists
        any newly discovered documents, and re-runs satisfaction linking.

        Returns the count of newly linked satisfactions from the re-link pass.
        """
        with self.engine.begin() as conn:
            # Guard: satisfies_encumbrance_id column may not exist on unmigrated DBs
            if not self._ori_satisfaction_link_columns_available(conn):
                logger.warning(
                    "Skipping SAT parent chase for strap={} because ori_encumbrances is missing satisfaction link columns",
                    strap,
                )
                return 0

            # 1. Query unlinked SAT/REL docs for this strap
            unlinked = conn.execute(
                text("""
                    SELECT id, instrument_number, legal_description
                    FROM ori_encumbrances
                    WHERE strap = :strap
                      AND encumbrance_type IN ('satisfaction', 'release')
                      AND satisfies_encumbrance_id IS NULL
                """),
                {"strap": strap},
            ).fetchall()

            if not unlinked:
                return 0

            # 2. Extract instrument references from legal_description
            ref_instruments: set[str] = set()
            # Map each extracted reference back to its source SAT row for diagnostics
            ref_source: dict[str, tuple[int, str]] = {}  # ref_inst -> (sat_id, sat_instrument)
            for row in unlinked:
                sat_id = row[0]
                sat_inst = (row[1] or "").strip()
                legal = (row[2] or "").upper()
                for pat in _INST_REF_PATTERNS:
                    for ref_inst in pat.findall(legal):
                        ref_inst = ref_inst.strip()
                        if ref_inst:
                            ref_instruments.add(ref_inst)
                            ref_source[ref_inst] = (sat_id, sat_inst)

            if not ref_instruments:
                return 0

            # 3. Build set of already-known instruments for this strap
            known_rows = conn.execute(
                text("SELECT instrument_number FROM ori_encumbrances WHERE strap = :strap"),
                {"strap": strap},
            ).fetchall()
            known_instruments = {(r[0] or "").strip() for r in known_rows}

            # 4. Filter to novel instruments not yet in DB
            novel = ref_instruments - known_instruments
            novel.discard("")

        if not novel:
            return 0

        # 5. Cap at _MAX_SAT_PARENT_CHASE to bound API calls
        novel_list = sorted(novel)[:_MAX_SAT_PARENT_CHASE]

        logger.info(
            "Chasing {} novel instrument ref(s) from unlinked SATs for strap={}",
            len(novel_list),
            strap,
        )

        # 6. Look up each novel instrument via PAV API
        stats: dict[str, int] = {"api_calls": 0, "retries": 0, "truncated": 0, "unresolved_truncations": 0}
        all_docs: list[dict[str, Any]] = []
        for instrument in novel_list:
            source_sat_id, source_sat_inst = ref_source.get(instrument, (None, ""))
            docs = self._search_instrument_pav(instrument, stats)
            if docs:
                logger.debug(
                    "SAT parent chase: PAV returned {} doc(s) for chased ref={} "
                    "(source SAT id={}, instrument={}), strap={}",
                    len(docs),
                    instrument,
                    source_sat_id,
                    source_sat_inst,
                    strap,
                )
                all_docs.extend(docs)
            else:
                logger.debug(
                    "SAT parent chase: PAV returned 0 docs for chased ref={} "
                    "(source SAT id={}, instrument={}), strap={}",
                    instrument,
                    source_sat_id,
                    source_sat_inst,
                    strap,
                )

        if not all_docs:
            return 0

        # 7. Persist discovered documents
        saved = self._save_documents(strap, folio, all_docs)
        logger.info(
            "SAT parent chase: saved {} doc(s) from {} API call(s) for strap={}",
            saved,
            stats["api_calls"],
            strap,
        )

        # 8. Re-run satisfaction linking if new docs were saved
        if saved > 0:
            chase_linked = self._link_satisfactions(strap)
            if chase_linked:
                logger.info(
                    "SAT parent chase: linked {} satisfaction(s) after discovery for strap={}",
                    chase_linked,
                    strap,
                )
            else:
                logger.warning(
                    "SAT parent chase: saved {} doc(s) but 0 linked for strap={} — "
                    "chased refs may not match parent encumbrances",
                    saved,
                    strap,
                )
            return chase_linked
        return 0

    def _ori_satisfaction_link_columns_available(self, conn: Any) -> bool:
        required = {
            "satisfies_encumbrance_id",
            "satisfaction_method",
            "satisfaction_date",
            "satisfaction_instrument",
        }
        rows = conn.execute(
            text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'ori_encumbrances'
                  AND column_name = ANY(:required)
            """),
            {"required": list(required)},
        ).fetchall()
        present = {str(row[0]) for row in rows}
        return required.issubset(present)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _mark_searched(self, foreclosure_id: int) -> None:
        """Mark foreclosure as ORI-searched."""
        with self.engine.begin() as conn:
            conn.execute(
                text("UPDATE foreclosures SET step_ori_searched = now() WHERE foreclosure_id = :fid"),
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
        token = s.split()[0]
        # Handle numeric string (Unix timestamp)
        if s.isdigit() and len(s) >= 9:
            from datetime import datetime

            try:
                dt = datetime.fromtimestamp(int(s), tz=UTC)
                return dt.strftime("%Y-%m-%d")
            except (ValueError, OSError, OverflowError):
                pass
        # Handle "M/D/YYYY HH:MM:SS AM" style by using first token.
        if "/" in token:
            parts = token.split("/")
            if len(parts) == 3:
                try:
                    month = int(parts[0])
                    day = int(parts[1])
                    year = int(parts[2])
                    return f"{year:04d}-{month:02d}-{day:02d}"
                except (ValueError, IndexError):
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

    @staticmethod
    def parse_date(val: Any) -> str | None:
        """Public wrapper for ORI date parsing."""
        return PgOriService._parse_date(val)
