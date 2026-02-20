"""
Async HTTP-based ORI encumbrance fetcher.

Thread-safe, single-property callable service that queries the Hillsborough
County PAV CustomQuery API via pure HTTP (no Playwright) and writes results
to PostgreSQL ``foreclosure_title_events``.

Searches ORI by legal description (CQID 321), party name (CQID 326), and
instrument number (CQID 320).  Groups raw party-document pairs by instrument
to produce unique document records, classifies each as an encumbrance type,
and inserts matching rows into ``foreclosure_title_events`` with
``event_source='ORI_ENCUMBRANCE'``.

Rate limited to 30 requests/minute with exponential backoff on errors.
"""

from __future__ import annotations

import asyncio
import re
import time
from datetime import datetime, UTC
from typing import Any

import httpx
from loguru import logger
from sqlalchemy import text

from sunbiz.db import get_engine, resolve_pg_dsn

# ---------------------------------------------------------------------------
# PAV API constants
# ---------------------------------------------------------------------------

PAV_API_URL = (
    "https://publicaccess.hillsclerk.com"
    "/PAVDirectSearch/api/CustomQuery/KeywordSearch"
)

PAV_HEADERS = {
    "Content-Type": "application/json",
    "Origin": "https://publicaccess.hillsclerk.com",
    "Referer": "https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html",
}

# Column order returned by PAV CQID 320/321/326 DisplayColumnValues
_PAV_COLUMNS = [
    "person_type",    # index 0
    "name",           # index 1
    "record_date",    # index 2
    "doc_type",       # index 3
    "book_type",      # index 4
    "book_num",       # index 5
    "page_num",       # index 6
    "legal",          # index 7
    "instrument",     # index 8
]

# ---------------------------------------------------------------------------
# Document type classification
# ---------------------------------------------------------------------------

# Raw ORI parenthetical code -> encumbrance category
# These are the short codes extracted from "(MTG) MORTGAGE" style strings.
_ENCUMBRANCE_CODE_MAP: dict[str, str] = {
    # Mortgages
    "MTG": "mortgage",
    "MTGREV": "mortgage",
    "DOT": "mortgage",
    "HELOC": "mortgage",
    # Judgments
    "JUD": "judgment",
    "CCJ": "judgment",
    "FJ": "judgment",
    "DRJUD": "judgment",
    # Lis Pendens
    "LP": "lis_pendens",
    "LISPEN": "lis_pendens",
    # Liens
    "LN": "lien",
    "MEDLN": "lien",
    "TL": "lien",
    "ML": "lien",
    "HOA": "lien",
    "COD": "lien",
    "MECH": "lien",
    "FIN": "lien",
    # Satisfactions (clear encumbrances)
    "SAT": "satisfaction",
    "SATMTG": "satisfaction",
    "RELMTG": "satisfaction",
    # Assignments (transfer encumbrances)
    "ASG": "assignment",
    "ASGT": "assignment",
}

_PAREN_RE = re.compile(r"\(([^)]+)\)")

# Rate-limiting: minimum seconds between consecutive requests
_MIN_REQUEST_INTERVAL = 60.0 / 30  # 30 req/min = 2 s between requests

# Exponential backoff
_INITIAL_BACKOFF = 2.0
_MAX_BACKOFF = 60.0
_MAX_RETRIES = 3


def _extract_code(raw_doc_type: str) -> str:
    """Extract the short code from ORI doc type strings like '(MTG) MORTGAGE'."""
    m = _PAREN_RE.search(raw_doc_type)
    return m.group(1).upper().strip() if m else raw_doc_type.upper().strip()


def _classify_doc_type(raw_type: str) -> str | None:
    """Classify a raw ORI doc type as an encumbrance category or None.

    Returns one of: mortgage, judgment, lis_pendens, lien, satisfaction,
    assignment, or None if the doc type is not an encumbrance.
    """
    code = _extract_code(raw_type)
    return _ENCUMBRANCE_CODE_MAP.get(code)


def _parse_record_date(raw: str) -> str | None:
    """Parse date string 'MM/DD/YYYY' to 'YYYY-MM-DD', or return None."""
    if not raw:
        return None
    try:
        dt = datetime.strptime(raw.strip(), "%m/%d/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def _build_legal_search_term(raw_legal1: str, raw_legal2: str) -> str | None:
    """Build ORI legal-description search term from HCPA raw_legal fields.

    Strategy: Extract LOT/UNIT number + first word of subdivision + wildcard.

    Examples:
        ("TUSCANY SUBDIVISION", "LOT 198") -> "L 198 TUSCANY*"
        ("THE QUARTER AT YBOR", "UNIT 5315") -> "UNIT 5315 QUARTER*"
    """
    if not raw_legal1 or not raw_legal2:
        return None

    legal1 = raw_legal1.upper().strip()
    legal2 = raw_legal2.upper().strip()

    # Extract lot/unit identifier from raw_legal2
    lot_match = re.match(r"(LOT|L)\s*(\d+)", legal2)
    unit_match = re.match(r"(UNIT)\s*(\d+)", legal2)
    tract_match = re.match(r"(TRACT)\s*(\d+)", legal2)
    block_match = re.match(r"(BLOCK|BLK)\s*(\d+)", legal2)

    if lot_match:
        lot_part = f"L {lot_match.group(2)}"
    elif unit_match:
        lot_part = f"UNIT {unit_match.group(2)}"
    elif tract_match:
        lot_part = f"TRACT {tract_match.group(2)}"
    elif block_match:
        lot_part = f"BLK {block_match.group(2)}"
    else:
        num_match = re.match(r"(\d+)", legal2)
        if num_match:
            lot_part = num_match.group(1)
        else:
            return None

    # Extract subdivision name from raw_legal1
    subdiv = legal1
    for remove in [
        "SUBDIVISION", "SUBD", "SUB", "PHASE", "PH",
        "UNIT", "SECTION", "SEC",
    ]:
        subdiv = re.sub(rf"\b{remove}\b.*", "", subdiv)
    subdiv = re.sub(r"\bA CONDOMINIUM\b.*", "", subdiv)
    subdiv = re.sub(r"\bCONDOMINIUM\b.*", "", subdiv)
    subdiv = re.sub(r"\bHOMEOWNERS\b.*", "", subdiv)
    subdiv = subdiv.strip()

    words = subdiv.split()
    if not words:
        return None

    # Skip filler words at the start
    first_word = words[0]
    if first_word in {"THE", "A", "AN"} and len(words) > 1:
        first_word = words[1]

    return f"{lot_part} {first_word}*"


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------


class EncumbranceFetcher:
    """Async HTTP-based ORI encumbrance fetcher.

    Thread-safe, single-property callable.  Reads property info from PG
    ``hcpa_bulk_parcels`` and ``foreclosures``, queries the PAV CustomQuery
    API via httpx, and writes encumbrance events to PG
    ``foreclosure_title_events``.
    """

    def __init__(self, dsn: str | None = None) -> None:
        resolved = resolve_pg_dsn(dsn)
        self._engine = get_engine(resolved)
        self._client: httpx.AsyncClient | None = None
        self._lock = asyncio.Lock()
        self._last_request_time: float = 0.0
        logger.info("EncumbranceFetcher initialized (PG + httpx)")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                headers=PAV_HEADERS,
                timeout=httpx.Timeout(30.0, connect=10.0),
                follow_redirects=True,
            )
        return self._client

    async def close(self) -> None:
        """Shut down the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # Rate limiter
    # ------------------------------------------------------------------

    async def _rate_limit(self) -> None:
        """Enforce max 30 requests/minute."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            if elapsed < _MIN_REQUEST_INTERVAL:
                wait = _MIN_REQUEST_INTERVAL - elapsed
                logger.debug(f"Rate limiter: sleeping {wait:.2f}s")
                await asyncio.sleep(wait)
            self._last_request_time = time.monotonic()

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    async def _post_pav(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """POST to PAV KeywordSearch with rate limiting and retries.

        Returns list of raw row dicts from the Data array, or empty list
        on failure/empty response.
        """
        client = await self._ensure_client()
        backoff = _INITIAL_BACKOFF

        for attempt in range(1, _MAX_RETRIES + 1):
            await self._rate_limit()
            try:
                logger.debug(
                    f"PAV POST attempt {attempt}/{_MAX_RETRIES}: "
                    f"CQID={payload.get('QueryID')}"
                )
                resp = await client.post(PAV_API_URL, json=payload)

                if resp.status_code == 200:
                    data = resp.json()
                    items = data.get("Data", [])
                    logger.debug(
                        f"PAV response: {len(items)} rows "
                        f"(CQID={payload.get('QueryID')})"
                    )
                    return items

                logger.warning(
                    f"PAV returned HTTP {resp.status_code} "
                    f"(attempt {attempt}/{_MAX_RETRIES})"
                )

            except httpx.TimeoutException:
                logger.warning(
                    f"PAV timeout (attempt {attempt}/{_MAX_RETRIES})"
                )
            except httpx.HTTPError as exc:
                logger.warning(
                    f"PAV HTTP error: {exc} "
                    f"(attempt {attempt}/{_MAX_RETRIES})"
                )

            if attempt < _MAX_RETRIES:
                logger.debug(f"Backing off {backoff:.1f}s before retry")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _MAX_BACKOFF)

        logger.error(
            f"PAV request failed after {_MAX_RETRIES} attempts: "
            f"CQID={payload.get('QueryID')}"
        )
        return []

    # ------------------------------------------------------------------
    # Search methods
    # ------------------------------------------------------------------

    async def _search_by_legal(self, legal: str) -> list[dict[str, Any]]:
        """CQID 321: Legal Description search."""
        payload = {
            "QueryID": 321,
            "Keywords": [{"Id": 1011, "Value": legal}],
        }
        logger.info(f"ORI legal search: {legal!r}")
        raw = await self._post_pav(payload)
        return self._parse_pav_response(raw)

    async def _search_by_party(self, name: str) -> list[dict[str, Any]]:
        """CQID 326: Party Name search."""
        payload = {
            "QueryID": 326,
            "Keywords": [{"Id": 486, "Value": name}],
        }
        logger.info(f"ORI party search: {name!r}")
        raw = await self._post_pav(payload)
        return self._parse_pav_response(raw)

    async def _search_by_instrument(self, instrument: str) -> list[dict[str, Any]]:
        """CQID 320: Instrument number search."""
        payload = {
            "QueryID": 320,
            "Keywords": [{"Id": 1006, "Value": instrument}],
        }
        logger.info(f"ORI instrument search: {instrument!r}")
        raw = await self._post_pav(payload)
        return self._parse_pav_response(raw)

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_pav_response(self, raw_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Parse PAV DisplayColumnValues rows, group by instrument.

        Each raw item is a party-document pair.  We group by instrument
        number (index 8) to produce one record per unique document, merging
        party names into grantor/grantee lists.

        Returns list of dicts with keys:
            instrument, doc_type, record_date, book_type, book_num,
            page_num, legal, grantors, grantees, encumbrance_type
        """
        if not raw_items:
            return []

        # First pass: extract fields and group by instrument
        by_instrument: dict[str, dict[str, Any]] = {}

        for item in raw_items:
            cols = item.get("DisplayColumnValues", [])
            if len(cols) < 9:
                continue

            row: dict[str, str] = {}
            for i, col_name in enumerate(_PAV_COLUMNS):
                if i < len(cols):
                    row[col_name] = (cols[i].get("Value") or "").strip()
                else:
                    row[col_name] = ""

            instrument = row.get("instrument", "")
            if not instrument:
                continue

            if instrument not in by_instrument:
                enc_type = _classify_doc_type(row.get("doc_type", ""))
                by_instrument[instrument] = {
                    "instrument": instrument,
                    "doc_type": row.get("doc_type", ""),
                    "record_date": _parse_record_date(row.get("record_date", "")),
                    "book_type": row.get("book_type", ""),
                    "book_num": row.get("book_num", ""),
                    "page_num": row.get("page_num", ""),
                    "legal": row.get("legal", ""),
                    "encumbrance_type": enc_type,
                    "grantors": [],
                    "grantees": [],
                }

            doc = by_instrument[instrument]
            person_type = (row.get("person_type", "")).upper().strip()
            name = row.get("name", "")
            if name:
                if person_type in {"PARTY 1", "GRANTOR", "1"}:
                    if name not in doc["grantors"]:
                        doc["grantors"].append(name)
                elif person_type in {"PARTY 2", "GRANTEE", "2"}:
                    if name not in doc["grantees"]:
                        doc["grantees"].append(name)
                # Default: Party 1 is grantor, Party 2 is grantee
                elif name not in doc["grantors"] and name not in doc["grantees"]:
                    doc["grantors"].append(name)

        return list(by_instrument.values())

    # ------------------------------------------------------------------
    # PG reads
    # ------------------------------------------------------------------

    def _get_parcel_info(self, strap: str) -> dict[str, Any] | None:
        """Look up folio + legal description from hcpa_bulk_parcels by strap."""
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT folio, strap, owner_name, property_address, "
                        "raw_legal1, raw_legal2, raw_legal3, raw_legal4 "
                        "FROM hcpa_bulk_parcels WHERE strap = :strap"
                    ),
                    {"strap": strap},
                ).mappings().fetchone()
                return dict(row) if row else None
        except Exception as exc:
            logger.warning(f"PG parcel lookup failed for strap={strap}: {exc}")
            return None

    def _get_foreclosures_for_strap(self, strap: str) -> list[dict[str, Any]]:
        """Find all foreclosure rows linked to this strap."""
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT foreclosure_id, case_number_raw, case_number_norm, "
                        "folio, strap "
                        "FROM foreclosures WHERE strap = :strap "
                        "ORDER BY auction_date DESC"
                    ),
                    {"strap": strap},
                ).mappings().fetchall()
                return [dict(r) for r in rows]
        except Exception as exc:
            logger.warning(
                f"PG foreclosure lookup failed for strap={strap}: {exc}"
            )
            return []

    # ------------------------------------------------------------------
    # PG writes
    # ------------------------------------------------------------------

    def _insert_title_events(
        self,
        foreclosure: dict[str, Any],
        documents: list[dict[str, Any]],
    ) -> int:
        """Insert encumbrance documents into foreclosure_title_events.

        Only inserts documents that have an encumbrance_type (mortgages,
        judgments, liens, lis pendens, satisfactions, assignments).

        Returns the number of rows inserted.
        """
        encumbrance_docs = [
            d for d in documents if d.get("encumbrance_type") is not None
        ]
        if not encumbrance_docs:
            return 0

        fid = foreclosure["foreclosure_id"]
        case_raw = foreclosure["case_number_raw"]
        case_norm = foreclosure.get("case_number_norm")
        folio = foreclosure.get("folio")
        strap = foreclosure.get("strap")

        inserted = 0
        try:
            with self._engine.begin() as conn:
                for doc in encumbrance_docs:
                    grantor = "; ".join(doc.get("grantors", []))
                    grantee = "; ".join(doc.get("grantees", []))
                    enc_type = doc["encumbrance_type"]
                    description = (
                        f"{doc.get('doc_type', '')} "
                        f"[{enc_type}]"
                    ).strip()

                    conn.execute(
                        text("""
                            INSERT INTO foreclosure_title_events (
                                foreclosure_id, case_number_raw,
                                case_number_norm, folio, strap,
                                event_date, event_source, event_subtype,
                                instrument_number, or_book, or_page,
                                grantor, grantee, description
                            ) VALUES (
                                :fid, :case_raw, :case_norm, :folio, :strap,
                                :event_date, 'ORI_ENCUMBRANCE', :subtype,
                                :instrument, :book, :page,
                                :grantor, :grantee, :description
                            )
                            ON CONFLICT DO NOTHING
                        """),
                        {
                            "fid": fid,
                            "case_raw": case_raw,
                            "case_norm": case_norm,
                            "folio": folio,
                            "strap": strap,
                            "event_date": doc.get("record_date")
                            or datetime.now(tz=UTC).strftime("%Y-%m-%d"),
                            "subtype": enc_type,
                            "instrument": doc.get("instrument"),
                            "book": doc.get("book_num") or None,
                            "page": doc.get("page_num") or None,
                            "grantor": grantor or None,
                            "grantee": grantee or None,
                            "description": description,
                        },
                    )
                    inserted += 1

        except Exception as exc:
            logger.error(
                f"Failed to insert title events for foreclosure_id={fid}: {exc}"
            )
            return 0

        if inserted:
            logger.info(
                f"Inserted {inserted} ORI_ENCUMBRANCE events for "
                f"foreclosure_id={fid} (case={case_raw})"
            )
        return inserted

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def fetch_for_property(self, strap: str) -> dict[str, Any]:
        """Fetch ORI encumbrances for a single property (by strap).

        Steps:
            1. Look up folio + legal description from hcpa_bulk_parcels
            2. Search ORI by legal description (CQID 321)
            3. Parse results, group by instrument
            4. Classify documents as encumbrances
            5. Insert into foreclosure_title_events for matching foreclosures
            6. Return summary dict

        Args:
            strap: HCPA strap identifier (e.g. ``203216D5N000000000090U``).

        Returns:
            Summary dict with keys: strap, folio, documents_found,
            encumbrances_found, events_inserted, foreclosures_matched,
            search_terms_used.
        """
        summary: dict[str, Any] = {
            "strap": strap,
            "folio": None,
            "documents_found": 0,
            "encumbrances_found": 0,
            "events_inserted": 0,
            "foreclosures_matched": 0,
            "search_terms_used": [],
        }

        # 1. PG parcel lookup
        parcel = self._get_parcel_info(strap)
        if not parcel:
            logger.warning(f"No parcel found in hcpa_bulk_parcels for strap={strap}")
            return summary

        folio = parcel.get("folio")
        summary["folio"] = folio

        # 2. Build legal description search term
        legal_term = _build_legal_search_term(
            parcel.get("raw_legal1", ""),
            parcel.get("raw_legal2", ""),
        )

        all_documents: list[dict[str, Any]] = []

        # 3. Search by legal description (primary)
        if legal_term:
            summary["search_terms_used"].append(f"legal:{legal_term}")
            docs = await self._search_by_legal(legal_term)
            all_documents.extend(docs)
            logger.info(
                f"Legal search for strap={strap}: "
                f"{len(docs)} documents found ({legal_term!r})"
            )

        # 4. If no results, fall back to owner name search
        if not all_documents:
            owner = parcel.get("owner_name", "")
            if owner:
                # Use last name + wildcard for broader matching
                owner_term = owner.split(",")[0].strip() + "*" if "," in owner else owner + "*"
                summary["search_terms_used"].append(f"party:{owner_term}")
                docs = await self._search_by_party(owner_term)
                all_documents.extend(docs)
                logger.info(
                    f"Party search fallback for strap={strap}: "
                    f"{len(docs)} documents found ({owner_term!r})"
                )

        # Deduplicate by instrument
        seen_instruments: set[str] = set()
        unique_docs: list[dict[str, Any]] = []
        for doc in all_documents:
            inst = doc.get("instrument", "")
            if inst and inst not in seen_instruments:
                seen_instruments.add(inst)
                unique_docs.append(doc)

        summary["documents_found"] = len(unique_docs)

        encumbrance_docs = [
            d for d in unique_docs if d.get("encumbrance_type") is not None
        ]
        summary["encumbrances_found"] = len(encumbrance_docs)

        if not unique_docs:
            logger.info(f"No ORI documents found for strap={strap}")
            return summary

        logger.info(
            f"strap={strap}: {len(unique_docs)} unique documents, "
            f"{len(encumbrance_docs)} encumbrances"
        )

        # 5. Find matching foreclosures and insert events
        foreclosures = self._get_foreclosures_for_strap(strap)
        summary["foreclosures_matched"] = len(foreclosures)

        if not foreclosures:
            logger.info(
                f"No foreclosures found for strap={strap} "
                f"({len(encumbrance_docs)} encumbrances discovered but not saved)"
            )
            return summary

        total_inserted = 0
        for fc in foreclosures:
            total_inserted += self._insert_title_events(fc, unique_docs)
        summary["events_inserted"] = total_inserted

        return summary
