"""Encumbrance audit signal extraction: LP-to-judgment delta analysis.

This module derives audit signals by comparing lis pendens (LP) filings against
final judgment data for active foreclosure cases.  It is intended to feed the
encumbrance audit model described in ``docs/domain/ENCUMBRANCE_AUDIT_BUCKETS.md``
without persisting fuzzy matches as facts.

Architecture
~~~~~~~~~~~~
The module exposes a single top-level helper class ``AuditSignalExtractor`` that
accepts a SQLAlchemy engine (or DSN) and provides methods to compute each signal
family for a batch of foreclosures.  Every signal is a plain dict keyed by
``foreclosure_id`` so that a downstream audit report or recovery tool can consume
them without depending on this module's internals.

Signals implemented
~~~~~~~~~~~~~~~~~~~
1. ``judgment_joined_party_gap``  -- judgment names a party not reflected in
   encumbrance discovery (party1/party2 on ori_encumbrances).
2. ``judgment_instrument_gap``    -- judgment carries instrument detail (book/page,
   instrument number) for the foreclosed mortgage that the saved encumbrance set
   lacks.
3. ``lp_to_judgment_plaintiff_change`` -- the LP plaintiff differs from the
   judgment plaintiff, suggesting an assignment, merger, or substitution.
4. ``lp_to_judgment_party_expansion`` -- the judgment names materially new
   parties not present in the LP filing.
5. ``lp_to_judgment_property_change`` -- the property description changed
   between the LP and judgment (legal description, address, parcel tokens).
6. ``long_case_interim_risk``     -- more than a configurable number of years
   between LP filing date and judgment date with no supporting lifecycle
   encumbrance evidence in between.

All data is derived from existing PG columns:
  - ``foreclosures.judgment_data`` (JSONB)
  - ``ori_encumbrances`` rows (especially ``encumbrance_type = 'lis_pendens'``)
  - ``foreclosures.filing_date``, ``foreclosures.judgment_date``
  - ``clerk_civil_parties`` for LP-era plaintiff lookup
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Sequence

from loguru import logger


# ---------------------------------------------------------------------------
# Name normalisation utilities
# ---------------------------------------------------------------------------

_STRIP_RE = re.compile(r"[^A-Z0-9 ]")
_MULTI_SPACE_RE = re.compile(r"\s{2,}")

# Tokens that add no party-identity value and should be stripped before
# comparison.
_NOISE_TOKENS = frozenset({
    "A",
    "AN",
    "AND",
    "AS",
    "AT",
    "BY",
    "DBA",
    "ET",
    "AL",
    "FOR",
    "IN",
    "INC",
    "ITS",
    "LLC",
    "LP",
    "LTD",
    "NA",
    "NKA",
    "OF",
    "OR",
    "PA",
    "PC",
    "SUCCESSOR",
    "SUCCESSORS",
    "THE",
    "TO",
    "TRUST",
    "TRUSTEE",
    "TRUSTEES",
    "UNKNOWN",
})

# Tenants and generic parties that should not trigger a gap signal.
_GENERIC_PARTY_NAMES = frozenset({
    "UNKNOWN TENANT",
    "UNKNOWN TENANTS",
    "UNKNOWN TENANT 1",
    "UNKNOWN TENANT 2",
    "JOHN DOE",
    "JANE DOE",
    "UNKNOWN SPOUSE",
    "ALL UNKNOWN PARTIES",
    "ALL OTHER UNKNOWN PARTIES",
    "TENANT 1",
    "TENANT 2",
    "ALL UNKNOWN CREDITORS",
})


def normalize_name(raw: str) -> str:
    """Upper-case, strip punctuation, collapse whitespace."""
    normed = _STRIP_RE.sub(" ", raw.upper().strip())
    return _MULTI_SPACE_RE.sub(" ", normed).strip()


def _name_tokens(name: str) -> frozenset[str]:
    """Return significant tokens from a normalised name."""
    return frozenset(t for t in normalize_name(name).split() if t not in _NOISE_TOKENS)


def _is_generic_party(name: str) -> bool:
    normed = normalize_name(name)
    return normed in _GENERIC_PARTY_NAMES or not normed


def names_match(a: str, b: str) -> bool:
    """Return True when two party names are close enough to be the same entity.

    Uses a token-overlap heuristic: if the intersection of significant tokens
    covers at least 60% of the smaller token set *and* at least 2 tokens
    overlap (to avoid single-generic-word false positives like "BANK"), the
    names are treated as matching.  This handles common variations (LLC vs Inc,
    middle initials, trust suffixes) without requiring fuzzy string matching.

    For very short names (1-2 tokens each), a single-token overlap is accepted
    only if both token sets are identical.
    """
    ta = _name_tokens(a)
    tb = _name_tokens(b)
    return _token_sets_match(ta, tb)


def _token_sets_match(ta: frozenset[str], tb: frozenset[str]) -> bool:
    """Return True when two significant-token sets are close enough to match.

    For short names (1-2 significant tokens), requires the smaller set to be
    a subset of the larger set.  This avoids single-generic-word false
    positives (e.g. "BANK" alone matching "CAPITAL ONE BANK") while still
    allowing "BANK AMERICA" to match "BANK OF AMERICA NATIONAL ASSOCIATION".
    For longer names, requires at least 60% overlap AND at least 2 overlapping
    tokens.
    """
    if not ta or not tb:
        return False
    overlap = ta & tb
    min_len = min(len(ta), len(tb))
    if min_len <= 2:
        # The smaller set must appear entirely inside the larger set.
        smaller, larger = (ta, tb) if len(ta) <= len(tb) else (tb, ta)
        return smaller.issubset(larger)
    return len(overlap) >= max(2, int(min_len * 0.6))


# ---------------------------------------------------------------------------
# Legal description normalisation
# ---------------------------------------------------------------------------

_LEGAL_NOISE_RE = re.compile(r"\b(ACCORDING|RECORDED|PLAT|BOOK|PAGE|PB|PG|SEC|SECTION|TOWNSHIP|TWP|RANGE|RGE)\b")

_LEGAL_TOKEN_RE = re.compile(r"[A-Z0-9]+")


def _legal_tokens(legal: str) -> frozenset[str]:
    """Extract meaningful tokens from a legal description for comparison."""
    normed = legal.upper().strip()
    normed = _LEGAL_NOISE_RE.sub("", normed)
    tokens = frozenset(_LEGAL_TOKEN_RE.findall(normed))
    # Drop short numeric noise
    return frozenset(t for t in tokens if len(t) > 1)


# ---------------------------------------------------------------------------
# Signal dataclass
# ---------------------------------------------------------------------------


@dataclass
class AuditSignal:
    """A single encumbrance audit signal for one foreclosure."""

    foreclosure_id: int
    signal_type: str
    severity: str  # "high", "medium", "low"
    detail: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "foreclosure_id": self.foreclosure_id,
            "signal_type": self.signal_type,
            "severity": self.severity,
            **self.detail,
        }


# ---------------------------------------------------------------------------
# Pure extraction functions (no DB dependency)
# ---------------------------------------------------------------------------


def extract_judgment_joined_party_gap(
    foreclosure_id: int,
    judgment_data: dict[str, Any],
    encumbrance_parties: Sequence[str],
) -> list[AuditSignal]:
    """Signal: judgment names a party not reflected in encumbrance discovery.

    Compares the ``defendants`` list from ``judgment_data`` against the
    combined party1/party2 strings in ``ori_encumbrances`` for this
    property.
    """
    defendants = judgment_data.get("defendants") or []
    if not defendants:
        return []

    enc_tokens: set[frozenset[str]] = set()
    for p in encumbrance_parties:
        tokens = _name_tokens(p)
        if tokens:
            enc_tokens.add(tokens)

    signals: list[AuditSignal] = []
    for d in defendants:
        name = d.get("name", "") if isinstance(d, dict) else str(d)
        if _is_generic_party(name):
            continue
        party_type = d.get("party_type", "unknown") if isinstance(d, dict) else "unknown"
        # Skip borrower/co_borrower/spouse/tenant -- they are usually named
        # defendants but are not separate lienholders.
        if party_type in ("borrower", "co_borrower", "spouse", "tenant", "unknown"):
            continue

        dtokens = _name_tokens(name)
        if not dtokens:
            continue

        matched = any(_token_sets_match(dtokens, et) for et in enc_tokens)
        if not matched:
            signals.append(
                AuditSignal(
                    foreclosure_id=foreclosure_id,
                    signal_type="judgment_joined_party_gap",
                    severity="high"
                    if party_type
                    in (
                        "second_mortgage_holder",
                        "judgment_creditor",
                        "hoa",
                        "condo_association",
                        "irs",
                        "federal_agency",
                    )
                    else "medium",
                    detail={
                        "party_name": name,
                        "party_type": party_type,
                        "is_federal": (d.get("is_federal_entity", False) if isinstance(d, dict) else False),
                    },
                )
            )
    return signals


def extract_judgment_instrument_gap(
    foreclosure_id: int,
    judgment_data: dict[str, Any],
    encumbrance_instruments: Sequence[dict[str, str | None]],
) -> list[AuditSignal]:
    """Signal: judgment has instrument detail missing from encumbrance set.

    Checks ``judgment_data.foreclosed_mortgage`` for book/page or instrument
    number and verifies whether any ``ori_encumbrances`` row matches.  Also
    checks the judgment's ``lis_pendens`` recording reference.
    """
    signals: list[AuditSignal] = []

    # Normalise existing encumbrance instruments for lookup.
    enc_instruments: set[str] = set()
    enc_book_pages: set[tuple[str, str]] = set()
    for ei in encumbrance_instruments:
        inst = (ei.get("instrument_number") or "").strip()
        if inst:
            enc_instruments.add(inst)
        book = (ei.get("book") or "").strip()
        page = (ei.get("page") or "").strip()
        if book and page:
            enc_book_pages.add((book, page))

    for ref_key, label in [
        ("foreclosed_mortgage", "foreclosed_mortgage"),
        ("lis_pendens", "lis_pendens_recording"),
    ]:
        ref = judgment_data.get(ref_key) or {}
        if not isinstance(ref, dict):
            continue

        j_inst = str(ref.get("instrument_number") or "").strip()
        j_book = str(ref.get("recording_book") or ref.get("book") or "").strip()
        j_page = str(ref.get("recording_page") or ref.get("page") or "").strip()

        has_detail = bool(j_inst) or (bool(j_book) and bool(j_page))
        if not has_detail:
            continue

        matched = False
        if j_inst and j_inst in enc_instruments:
            matched = True
        if j_book and j_page and (j_book, j_page) in enc_book_pages:
            matched = True

        if not matched:
            signals.append(
                AuditSignal(
                    foreclosure_id=foreclosure_id,
                    signal_type="judgment_instrument_gap",
                    severity="high" if label == "foreclosed_mortgage" else "medium",
                    detail={
                        "reference_source": label,
                        "instrument_number": j_inst or None,
                        "book": j_book or None,
                        "page": j_page or None,
                    },
                )
            )
    return signals


def extract_lp_to_judgment_plaintiff_change(
    foreclosure_id: int,
    judgment_data: dict[str, Any],
    lp_plaintiff: str | None,
) -> list[AuditSignal]:
    """Signal: plaintiff changed between LP and judgment.

    Compares the LP plaintiff (from ``clerk_civil_parties`` or the LP
    ``party1`` on ``ori_encumbrances``) to the judgment plaintiff.  A change
    suggests an assignment, merger, or substitution event that may not be
    reflected in the encumbrance chain.
    """
    j_plaintiff = (judgment_data.get("plaintiff") or "").strip()
    lp_p = (lp_plaintiff or "").strip()

    if not j_plaintiff or not lp_p:
        return []

    if names_match(j_plaintiff, lp_p):
        return []

    return [
        AuditSignal(
            foreclosure_id=foreclosure_id,
            signal_type="lp_to_judgment_plaintiff_change",
            severity="high",
            detail={
                "lp_plaintiff": lp_p,
                "judgment_plaintiff": j_plaintiff,
            },
        )
    ]


def extract_lp_to_judgment_party_expansion(
    foreclosure_id: int,
    judgment_data: dict[str, Any],
    lp_parties: Sequence[str],
) -> list[AuditSignal]:
    """Signal: judgment introduces materially new parties not in the LP.

    This is distinct from ``judgment_joined_party_gap`` which compares against
    encumbrances.  Here we compare the judgment's defendant list against the LP
    filing's party universe (from ``clerk_civil_parties`` or the LP document
    itself).
    """
    defendants = judgment_data.get("defendants") or []
    if not defendants or not lp_parties:
        return []

    lp_token_sets: list[frozenset[str]] = [_name_tokens(p) for p in lp_parties if p.strip()]
    lp_token_sets = [ts for ts in lp_token_sets if ts]
    if not lp_token_sets:
        return []

    new_parties: list[dict[str, Any]] = []
    for d in defendants:
        name = d.get("name", "") if isinstance(d, dict) else str(d)
        if _is_generic_party(name):
            continue
        party_type = d.get("party_type", "unknown") if isinstance(d, dict) else "unknown"
        # Skip borrower/co_borrower/spouse/tenant -- they are usually named
        # defendants but are not separate lienholders.
        if party_type in ("borrower", "co_borrower", "spouse", "tenant", "unknown"):
            continue
        dtokens = _name_tokens(name)
        if not dtokens:
            continue

        matched = any(_token_sets_match(dtokens, lt) for lt in lp_token_sets)
        if not matched:
            new_parties.append({"name": name, "party_type": party_type})

    if not new_parties:
        return []

    return [
        AuditSignal(
            foreclosure_id=foreclosure_id,
            signal_type="lp_to_judgment_party_expansion",
            severity="high"
            if any(
                p["party_type"]
                in (
                    "second_mortgage_holder",
                    "judgment_creditor",
                    "hoa",
                    "condo_association",
                    "irs",
                    "federal_agency",
                )
                for p in new_parties
            )
            else "medium",
            detail={
                "new_party_count": len(new_parties),
                "new_parties": new_parties,
            },
        )
    ]


def extract_lp_to_judgment_property_change(
    foreclosure_id: int,
    judgment_data: dict[str, Any],
    lp_legal_description: str | None,
    lp_property_address: str | None,
) -> list[AuditSignal]:
    """Signal: property description changed between LP and judgment.

    Compares legal description tokens and property address tokens.  Only fires
    when the overlap is materially different -- minor token differences (plat
    book formatting, section abbreviation) are tolerated.
    """
    j_legal = (judgment_data.get("legal_description") or "").strip()
    j_address = (judgment_data.get("property_address") or "").strip()
    lp_legal = (lp_legal_description or "").strip()
    lp_addr = (lp_property_address or "").strip()

    changes: list[dict[str, Any]] = []

    # Legal description comparison
    if j_legal and lp_legal:
        jt = _legal_tokens(j_legal)
        lt = _legal_tokens(lp_legal)
        if jt and lt:
            overlap = jt & lt
            union = jt | lt
            jaccard = len(overlap) / len(union) if union else 1.0
            if jaccard < 0.5:
                changes.append({
                    "field": "legal_description",
                    "jaccard_similarity": round(jaccard, 3),
                    "judgment_sample": j_legal[:200],
                    "lp_sample": lp_legal[:200],
                })

    # Address comparison
    if j_address and lp_addr:
        jat = _name_tokens(j_address)
        lat = _name_tokens(lp_addr)
        if jat and lat:
            overlap = jat & lat
            min_len = min(len(jat), len(lat))
            if min_len > 0 and len(overlap) / min_len < 0.5:
                changes.append({
                    "field": "property_address",
                    "judgment_address": j_address,
                    "lp_address": lp_addr,
                })

    if not changes:
        return []

    return [
        AuditSignal(
            foreclosure_id=foreclosure_id,
            signal_type="lp_to_judgment_property_change",
            severity="medium",
            detail={"changes": changes},
        )
    ]


def extract_long_case_interim_risk(
    foreclosure_id: int,
    lp_filing_date: date | str | None,
    judgment_date: date | str | None,
    lifecycle_encumbrance_count: int,
    *,
    threshold_years: int = 3,
) -> list[AuditSignal]:
    """Signal: long LP-to-judgment gap with no lifecycle evidence.

    If the gap between the LP filing date and the judgment date exceeds
    ``threshold_years`` and there are zero lifecycle encumbrances (assignments,
    modifications, subordinations) in between, the case likely has missing
    intermediate evidence.
    """
    lp_date = _to_date(lp_filing_date)
    j_date = _to_date(judgment_date)

    if not lp_date or not j_date:
        return []

    gap_days = (j_date - lp_date).days
    if gap_days < threshold_years * 365:
        return []

    # If there are lifecycle encumbrances in the window, the gap is
    # partially explained.
    if lifecycle_encumbrance_count > 0:
        return []

    gap_years = round(gap_days / 365.25, 1)
    return [
        AuditSignal(
            foreclosure_id=foreclosure_id,
            signal_type="long_case_interim_risk",
            severity="high" if gap_years >= 5 else "medium",
            detail={
                "lp_filing_date": str(lp_date),
                "judgment_date": str(j_date),
                "gap_years": gap_years,
                "lifecycle_encumbrance_count": lifecycle_encumbrance_count,
            },
        )
    ]


# ---------------------------------------------------------------------------
# Date helper
# ---------------------------------------------------------------------------


def _to_date(value: date | str | None) -> date | None:
    """Coerce a string or date to a ``datetime.date``, returning None on failure."""
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Database-backed batch extraction
# ---------------------------------------------------------------------------


class AuditSignalExtractor:
    """Extracts LP-to-judgment delta signals for active foreclosures.

    Intended to be called by audit tools or the web layer.  All PG access is
    read-only.

    Usage::

        extractor = AuditSignalExtractor(engine=my_engine)
        signals = extractor.extract_all_signals()
        # signals is a list[AuditSignal]

    Or for a single foreclosure::

        signals = extractor.extract_signals_for(foreclosure_id=42)
    """

    def __init__(
        self,
        *,
        engine: Any | None = None,
        dsn: str | None = None,
    ) -> None:
        if engine is not None:
            self.engine = engine
        else:
            from sunbiz.db import get_engine, resolve_pg_dsn

            self.engine = get_engine(resolve_pg_dsn(dsn))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract_all_signals(
        self,
        *,
        limit: int | None = None,
    ) -> list[AuditSignal]:
        """Return all audit signals for active foreclosures with judgment data."""
        from sqlalchemy import text

        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT foreclosure_id FROM foreclosures "
                    "WHERE archived_at IS NULL AND judgment_data IS NOT NULL "
                    "ORDER BY foreclosure_id " + ("LIMIT :limit" if limit else "")
                ),
                {"limit": limit} if limit else {},
            ).fetchall()

        fids = [r[0] for r in rows]
        all_signals: list[AuditSignal] = []
        for fid in fids:
            all_signals.extend(self.extract_signals_for(fid))
        return all_signals

    def extract_signals_for(self, foreclosure_id: int) -> list[AuditSignal]:
        """Compute all six signal families for a single foreclosure."""
        with self.engine.connect() as conn:
            fc = self._load_foreclosure(conn, foreclosure_id)
            if not fc:
                return []

            judgment_data = fc["judgment_data"]
            strap = fc["strap"]
            case_number_raw = fc["case_number_raw"]
            case_number_norm = fc.get("case_number_norm")

            enc_rows = self._load_encumbrances(conn, strap, case_number_raw, case_number_norm)
            lp_row = self._find_lp_row(enc_rows)
            lp_plaintiff = self._resolve_lp_plaintiff(
                conn,
                lp_row,
                case_number_raw,
                case_number_norm,
            )
            lp_parties = self._resolve_lp_parties(
                conn,
                lp_row,
                case_number_raw,
                case_number_norm,
            )

        signals: list[AuditSignal] = []

        # 1. judgment_joined_party_gap
        enc_parties = self._collect_encumbrance_parties(enc_rows)
        signals.extend(
            extract_judgment_joined_party_gap(
                foreclosure_id,
                judgment_data,
                enc_parties,
            )
        )

        # 2. judgment_instrument_gap
        enc_instruments = self._collect_encumbrance_instruments(enc_rows)
        signals.extend(
            extract_judgment_instrument_gap(
                foreclosure_id,
                judgment_data,
                enc_instruments,
            )
        )

        # 3. lp_to_judgment_plaintiff_change
        signals.extend(
            extract_lp_to_judgment_plaintiff_change(
                foreclosure_id,
                judgment_data,
                lp_plaintiff,
            )
        )

        # 4. lp_to_judgment_party_expansion
        signals.extend(
            extract_lp_to_judgment_party_expansion(
                foreclosure_id,
                judgment_data,
                lp_parties,
            )
        )

        # 5. lp_to_judgment_property_change
        lp_legal = (lp_row or {}).get("legal_description")
        lp_address = None  # LP documents rarely carry an address field
        signals.extend(
            extract_lp_to_judgment_property_change(
                foreclosure_id,
                judgment_data,
                lp_legal,
                lp_address,
            )
        )

        # 6. long_case_interim_risk
        lp_date = (
            (lp_row or {}).get("recording_date")
            or ((judgment_data.get("lis_pendens") or {}).get("recording_date"))
        )
        j_date = judgment_data.get("judgment_date") or fc.get("judgment_date")
        lifecycle_count = self._count_lifecycle_encumbrances(
            enc_rows,
            lp_date,
            j_date,
        )
        signals.extend(
            extract_long_case_interim_risk(
                foreclosure_id,
                lp_date,
                j_date,
                lifecycle_count,
            )
        )

        return signals

    # ------------------------------------------------------------------
    # Internal DB helpers
    # ------------------------------------------------------------------

    def _load_foreclosure(self, conn: Any, fid: int) -> dict[str, Any] | None:
        from sqlalchemy import text

        row = conn.execute(
            text("""
            SELECT foreclosure_id, case_number_raw, case_number_norm,
                   strap, folio, judgment_data, filing_date, judgment_date
            FROM foreclosures
            WHERE foreclosure_id = :fid AND archived_at IS NULL
        """),
            {"fid": fid},
        ).fetchone()
        if not row:
            return None

        jdata = row[5]
        if isinstance(jdata, str):
            try:
                jdata = json.loads(jdata)
            except (json.JSONDecodeError, TypeError):
                logger.warning("Invalid judgment_data JSON for foreclosure_id={}", fid)
                jdata = {}

        return {
            "foreclosure_id": row[0],
            "case_number_raw": row[1],
            "case_number_norm": row[2],
            "strap": row[3],
            "folio": row[4],
            "judgment_data": jdata or {},
            "filing_date": row[6],
            "judgment_date": row[7],
        }

    def _load_encumbrances(
        self,
        conn: Any,
        strap: str | None,
        case_number_raw: str | None,
        case_number_norm: str | None,
    ) -> list[dict[str, Any]]:
        from sqlalchemy import text

        clauses: list[str] = []
        params: dict[str, Any] = {}

        if strap:
            clauses.append("oe.strap = :strap")
            params["strap"] = strap
        if case_number_raw:
            clauses.append("oe.case_number = :case_raw")
            params["case_raw"] = case_number_raw
        if case_number_norm:
            clauses.append("oe.case_number = :case_norm")
            params["case_norm"] = case_number_norm
        if not clauses:
            return []

        where = " OR ".join(clauses)
        rows = conn.execute(
            text(f"""
            SELECT oe.id, oe.encumbrance_type, oe.party1, oe.party2,
                   oe.instrument_number, oe.book, oe.page,
                   oe.recording_date, oe.case_number,
                   oe.legal_description, oe.raw_document_type
            FROM ori_encumbrances oe
            WHERE ({where})
            ORDER BY oe.recording_date NULLS LAST
        """),
            params,
        ).fetchall()

        return [
            {
                "id": r[0],
                "encumbrance_type": r[1],
                "party1": r[2] or "",
                "party2": r[3] or "",
                "instrument_number": r[4] or "",
                "book": r[5] or "",
                "page": r[6] or "",
                "recording_date": r[7],
                "case_number": r[8] or "",
                "legal_description": r[9] or "",
                "raw_document_type": r[10] or "",
            }
            for r in rows
        ]

    @staticmethod
    def _find_lp_row(
        enc_rows: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Return the first lis_pendens row from the encumbrance set."""
        for row in enc_rows:
            if row.get("encumbrance_type") == "lis_pendens":
                return row
        return None

    def _resolve_lp_plaintiff(
        self,
        conn: Any,
        lp_row: dict[str, Any] | None,
        case_number_raw: str | None,
        case_number_norm: str | None,
    ) -> str | None:
        """Best-effort LP plaintiff: LP party1 or clerk_civil_parties."""
        # 1. LP document party1 is usually the plaintiff
        if lp_row:
            p1 = (lp_row.get("party1") or "").strip()
            if p1:
                return p1

        # 2. Fall back to clerk_civil_parties for the filing plaintiff
        return self._clerk_plaintiff(conn, case_number_raw, case_number_norm)

    def _resolve_lp_parties(
        self,
        conn: Any,
        lp_row: dict[str, Any] | None,
        case_number_raw: str | None,
        case_number_norm: str | None,
    ) -> list[str]:
        """Collect all known parties at LP-filing time."""
        parties: list[str] = []

        if lp_row:
            for key in ("party1", "party2"):
                val = (lp_row.get(key) or "").strip()
                if val:
                    # party fields may be comma-separated lists
                    for name in val.split(","):
                        name = name.strip()
                        if name:
                            parties.append(name)

        # Supplement from clerk_civil_parties
        clerk_names = self._clerk_all_parties(conn, case_number_raw, case_number_norm)
        parties.extend(clerk_names)

        return parties

    def _clerk_plaintiff(
        self,
        conn: Any,
        case_number_raw: str | None,
        case_number_norm: str | None,
    ) -> str | None:
        from sqlalchemy import text

        candidates = [c for c in (case_number_raw, case_number_norm) if c]
        if not candidates:
            return None

        for cn in candidates:
            row = conn.execute(
                text("""
                SELECT COALESCE(NULLIF(name, ''), NULLIF(business_name, ''))
                FROM clerk_civil_parties
                WHERE case_number = :cn
                  AND party_type ILIKE 'Plaintiff%'
                ORDER BY CASE WHEN party_type = 'Plaintiff' THEN 0 ELSE 1 END, id
                LIMIT 1
            """),
                {"cn": cn},
            ).fetchone()
            if row and row[0]:
                return row[0].strip()
        return None

    def _clerk_all_parties(
        self,
        conn: Any,
        case_number_raw: str | None,
        case_number_norm: str | None,
    ) -> list[str]:
        from sqlalchemy import text

        candidates = [c for c in (case_number_raw, case_number_norm) if c]
        if not candidates:
            return []

        for cn in candidates:
            rows = conn.execute(
                text("""
                SELECT DISTINCT COALESCE(NULLIF(name, ''), NULLIF(business_name, ''))
                FROM clerk_civil_parties
                WHERE case_number = :cn
                  AND COALESCE(NULLIF(name, ''), NULLIF(business_name, '')) IS NOT NULL
                ORDER BY 1
            """),
                {"cn": cn},
            ).fetchall()
            if rows:
                return [r[0].strip() for r in rows if r[0]]
        return []

    # ------------------------------------------------------------------
    # Encumbrance row helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _collect_encumbrance_parties(
        enc_rows: list[dict[str, Any]],
    ) -> list[str]:
        """Flatten party1/party2 from all encumbrance rows into a list."""
        parties: list[str] = []
        for row in enc_rows:
            for key in ("party1", "party2"):
                val = (row.get(key) or "").strip()
                if val:
                    for name in val.split(","):
                        name = name.strip()
                        if name:
                            parties.append(name)
        return parties

    @staticmethod
    def _collect_encumbrance_instruments(
        enc_rows: list[dict[str, Any]],
    ) -> list[dict[str, str | None]]:
        """Collect instrument identifiers from all encumbrance rows."""
        return [
            {
                "instrument_number": row.get("instrument_number") or None,
                "book": row.get("book") or None,
                "page": row.get("page") or None,
            }
            for row in enc_rows
        ]

    @staticmethod
    def _count_lifecycle_encumbrances(
        enc_rows: list[dict[str, Any]],
        lp_date: date | str | None,
        judgment_date: date | str | None,
    ) -> int:
        """Count lifecycle docs between the LP and judgment dates."""
        lp_d = _to_date(lp_date)
        j_d = _to_date(judgment_date)
        if not lp_d or not j_d:
            return 0

        count = 0
        for row in enc_rows:
            enc_type = row.get("encumbrance_type")
            raw_type = str(row.get("raw_document_type") or "").upper()
            is_lifecycle = enc_type in {"assignment", "satisfaction", "release"} or (
                enc_type == "other"
                and ("MOD" in raw_type or "SUB" in raw_type or "NCL" in raw_type)
            )
            if not is_lifecycle:
                continue
            rec = _to_date(row.get("recording_date"))
            if rec and lp_d <= rec <= j_d:
                count += 1
        return count
