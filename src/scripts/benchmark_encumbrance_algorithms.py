#!/usr/bin/env python3
"""Benchmark encumbrance discovery strategies for Hillsborough foreclosures.

This script compares multiple ORI discovery strategies on the same property
sample and scores:
- completeness (instrument recall vs per-case reference universe)
- business-rule coverage (LP, mortgage->release, NOC->permit)
- efficiency (API calls, runtime, truncation)

It is read-only against PostgreSQL.
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from loguru import logger
from sqlalchemy import text

# Ensure repository root is importable when run as a script from scripts/.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sunbiz.db import get_engine, resolve_pg_dsn  # noqa: E402

PAV_API_URL = (
    "https://publicaccess.hillsclerk.com"
    "/PAVDirectSearch/api/CustomQuery/KeywordSearch"
)
PAV_HEADERS = {
    "Content-Type": "application/json",
    "Origin": "https://publicaccess.hillsclerk.com",
    "Referer": "https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html",
}

CODE_RE = re.compile(r"\(([^)]+)\)")
INSTRUMENT_REFS = [
    re.compile(r"CLK\s*#?\s*(\d{7,10})", re.IGNORECASE),
    re.compile(r"INST(?:RUMENT)?\s*(?:#|NO\.?\s*)?(\d{7,10})", re.IGNORECASE),
    re.compile(r"\b(\d{7,10})\b"),
]

MORTGAGE_CODES = {"MTG", "MTGNIT", "MTGNT", "MTGREV", "DOT", "HELOC"}
RELEASE_CODES = {"SAT", "REL", "RELMTG", "PR", "RELLP", "SATCORPTX"}
JUDGMENT_CODES = {"JUD", "FNLJ", "CCJ"}
ENCUMBRANCE_CODES = {
    "MTG",
    "MTGNIT",
    "MTGNT",
    "MTGREV",
    "DOT",
    "HELOC",
    "JUD",
    "FNLJ",
    "CCJ",
    "LP",
    "LN",
    "MEDLN",
    "LNCORPTX",
    "SAT",
    "REL",
    "RELMTG",
    "PR",
    "ASG",
    "ASGT",
    "NOC",
    "RELLP",
}

DEFAULT_QUERY_LIMIT = 500
DEFAULT_MIN_INTERVAL_SECONDS = 0.30
DEFAULT_MAX_SPLIT_DEPTH = 7


@dataclass
class SaleRow:
    instrument: str
    sale_date: date | None
    grantor: str
    grantee: str


@dataclass
class PermitRow:
    source: str
    permit_number: str | None
    issue_date: date | None
    complete_date: date | None
    address: str | None
    status: str | None


@dataclass
class CaseContext:
    foreclosure_id: int
    case_number: str
    strap: str
    folio: str | None
    auction_date: date | None
    property_address: str
    legal1: str
    legal2: str
    legal3: str
    legal4: str
    owner_name: str
    plaintiff: str
    defendant: str
    filing_date: date | None
    sales_count: int
    sales_chain: list[SaleRow] = field(default_factory=list)
    clerk_defendants: list[str] = field(default_factory=list)
    permits: list[PermitRow] = field(default_factory=list)
    existing_instruments: set[str] = field(default_factory=set)


@dataclass
class OriDoc:
    instrument: str
    doc_type: str
    doc_code: str
    record_date: date | None
    legal: str
    book_type: str
    book_num: str
    page_num: str
    party1: list[str] = field(default_factory=list)
    party2: list[str] = field(default_factory=list)
    references: set[str] = field(default_factory=set)
    source_queries: set[str] = field(default_factory=set)


@dataclass
class StrategyRun:
    strategy: str
    case_number: str
    docs: dict[str, OriDoc]
    api_calls: int
    retries: int
    errors: int
    truncated_responses: int
    unresolved_truncations: int
    runtime_seconds: float


@dataclass
class ScoredRun:
    base: StrategyRun
    instrument_recall: float
    mortgage_lifecycle_recall: float
    lp_found: bool
    judgment_found: bool
    mortgage_count: int
    linked_mortgage_count: int
    noc_count: int
    noc_permit_link_count: int


class PropertyMatcher:
    """Simple property-level noise filter for ORI docs."""

    def __init__(self, case: CaseContext) -> None:
        self.case = case
        self.legal_sub_token = self._extract_sub_token(case.legal1)
        self.legal_lot_token = self._extract_lot_token(case.legal2)
        self.owner_tokens = self._name_tokens(case.owner_name)

    @staticmethod
    def _extract_sub_token(raw_legal1: str) -> str:
        txt = (raw_legal1 or "").upper().strip()
        if not txt:
            return ""
        txt = re.sub(
            r"\b(SUBDIVISION|SUBD|SUB|PHASE|PH|SECTION|SEC|UNIT|CONDOMINIUM)\b.*",
            "",
            txt,
        )
        words = [w for w in txt.split() if w not in {"THE", "A", "AN", "AT", "OF"}]
        return words[0] if words else ""

    @staticmethod
    def _extract_lot_token(raw_legal2: str) -> str:
        txt = (raw_legal2 or "").upper().strip()
        if not txt:
            return ""
        m = re.search(r"\b(LOT|L|UNIT|TRACT|BLK|BLOCK)\s*([A-Z0-9-]+)", txt)
        if not m:
            return ""
        return f"{m.group(1)} {m.group(2)}".replace("BLK", "B").replace("BLOCK", "B")

    @staticmethod
    def _name_tokens(name: str) -> set[str]:
        clean = normalize_name(name)
        return {x for x in clean.split() if len(x) > 2}

    def likely_matches_property(self, doc: OriDoc, *, sale: SaleRow | None = None) -> bool:
        legal = (doc.legal or "").upper()
        parties = " ".join(doc.party1 + doc.party2).upper()

        if (
            self.legal_lot_token
            and self.legal_sub_token
            and self.legal_lot_token in legal
            and self.legal_sub_token in legal
        ):
            return True

        if self.legal_sub_token and self.legal_sub_token in legal:
            party_tokens = {x for x in normalize_name(parties).split() if len(x) > 2}
            if party_tokens & self.owner_tokens:
                return True

        if sale is not None:
            sale_grantee_tokens = {x for x in normalize_name(sale.grantee).split() if len(x) > 2}
            party_tokens = {x for x in normalize_name(parties).split() if len(x) > 2}
            if sale_grantee_tokens and sale_grantee_tokens & party_tokens:
                return True

        return bool(
            doc.doc_code in {"LP"}
            and self.legal_sub_token
            and self.legal_sub_token in legal
        )


class PAVClient:
    """Thin PAV KeywordSearch client with truncation splitting."""

    def __init__(
        self,
        *,
        query_limit: int,
        min_interval: float,
        max_split_depth: int,
        timeout_seconds: float,
    ) -> None:
        self.query_limit = query_limit
        self.min_interval = min_interval
        self.max_split_depth = max_split_depth
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers.update(PAV_HEADERS)
        self.last_request_ts = 0.0
        self.api_calls = 0
        self.retries = 0
        self.errors = 0
        self.truncated_responses = 0
        self.unresolved_truncations = 0

    def close(self) -> None:
        self.session.close()

    def _throttle(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_request_ts
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request_ts = time.monotonic()

    def _post(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        for attempt in range(1, 4):
            self._throttle()
            self.api_calls += 1
            try:
                response = self.session.post(
                    PAV_API_URL,
                    json=payload,
                    timeout=self.timeout_seconds,
                )
                if response.status_code == 200:
                    return response.json()
                logger.warning(
                    "PAV HTTP {} for QueryID={} attempt={}",
                    response.status_code,
                    payload.get("QueryID"),
                    attempt,
                )
            except requests.RequestException as exc:
                logger.warning(
                    "PAV request error QueryID={} attempt={}: {}",
                    payload.get("QueryID"),
                    attempt,
                    exc,
                )

            if attempt < 3:
                self.retries += 1
                time.sleep(1.5 * attempt)

        self.errors += 1
        return None

    def search(
        self,
        *,
        query_id: int,
        keywords: list[tuple[int, str]],
        label: str,
        from_date: date | None = None,
        to_date: date | None = None,
        split_on_truncation: bool = True,
        depth: int = 0,
    ) -> list[OriDoc]:
        payload: dict[str, Any] = {
            "QueryID": query_id,
            "Keywords": [{"Id": key_id, "Value": value} for key_id, value in keywords],
            "QueryLimit": self.query_limit,
        }
        if from_date is not None:
            payload["FromDate"] = from_date.strftime("%m/%d/%Y")
        if to_date is not None:
            payload["ToDate"] = to_date.strftime("%m/%d/%Y")

        data = self._post(payload)
        if not data:
            return []

        docs = parse_pav_data(data.get("Data") or [], label)
        truncated = bool(data.get("Truncated"))
        if truncated:
            self.truncated_responses += 1

        if (
            truncated
            and split_on_truncation
            and from_date is not None
            and to_date is not None
            and from_date < to_date
            and depth < self.max_split_depth
        ):
            mid = from_date + timedelta(days=(to_date - from_date).days // 2)
            left = self.search(
                query_id=query_id,
                keywords=keywords,
                label=label,
                from_date=from_date,
                to_date=mid,
                split_on_truncation=split_on_truncation,
                depth=depth + 1,
            )
            right = self.search(
                query_id=query_id,
                keywords=keywords,
                label=label,
                from_date=mid + timedelta(days=1),
                to_date=to_date,
                split_on_truncation=split_on_truncation,
                depth=depth + 1,
            )
            return merge_doc_lists(left + right)

        if truncated and split_on_truncation:
            self.unresolved_truncations += 1
            logger.warning(
                "Truncated unresolved label={} query_id={} from={} to={}",
                label,
                query_id,
                from_date,
                to_date,
            )

        return docs


def parse_pav_data(rows: list[dict[str, Any]], source_label: str) -> list[OriDoc]:
    docs: dict[str, OriDoc] = {}
    for row in rows:
        cols = row.get("DisplayColumnValues") or []
        if len(cols) < 9:
            continue

        values = [str(col.get("Value") or "").strip() for col in cols[:9]]
        values.extend([""] * (9 - len(values)))

        person_type = values[0].upper()
        name = values[1]
        record_date = parse_record_date(values[2])
        doc_type = values[3]
        book_type = values[4]
        book_num = values[5]
        page_num = values[6]
        legal = values[7]
        instrument = only_digits(values[8])

        if not instrument:
            continue

        doc = docs.get(instrument)
        if doc is None:
            doc = OriDoc(
                instrument=instrument,
                doc_type=doc_type,
                doc_code=extract_doc_code(doc_type),
                record_date=record_date,
                legal=legal,
                book_type=book_type,
                book_num=book_num,
                page_num=page_num,
            )
            doc.references = extract_instrument_refs(legal)
            docs[instrument] = doc

        doc.source_queries.add(source_label)
        if name:
            if "2" in person_type or "GRANTEE" in person_type:
                if name not in doc.party2:
                    doc.party2.append(name)
            elif name not in doc.party1:
                doc.party1.append(name)

    return list(docs.values())


def normalize_name(name: str) -> str:
    upper = (name or "").upper()
    upper = re.sub(r"[^A-Z0-9 ]+", " ", upper)
    upper = re.sub(r"\s+", " ", upper)
    return upper.strip()


def only_digits(value: str) -> str:
    if not value:
        return ""
    digits = re.sub(r"\D", "", value)
    if 7 <= len(digits) <= 12:
        return digits
    return ""


def parse_record_date(raw: str) -> date | None:
    if not raw:
        return None
    token = raw.split()[0]
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(token, fmt).date()
        except ValueError:
            continue
    return None


def extract_doc_code(doc_type: str) -> str:
    m = CODE_RE.search(doc_type or "")
    if m:
        return m.group(1).upper().strip()
    return (doc_type or "").upper().strip()


def extract_instrument_refs(text_value: str) -> set[str]:
    refs: set[str] = set()
    txt = text_value or ""
    for pattern in INSTRUMENT_REFS:
        refs.update(only_digits(m.group(1)) for m in pattern.finditer(txt))
    return {x for x in refs if x}


def build_legal_term(raw_legal1: str, raw_legal2: str) -> str | None:
    legal1 = (raw_legal1 or "").upper().strip()
    legal2 = (raw_legal2 or "").upper().strip()
    if not legal1 or not legal2:
        return None

    m = re.search(r"\b(LOT|L|UNIT|TRACT|BLOCK|BLK)\s*([A-Z0-9-]+)", legal2)
    if m:
        lot = m.group(2)
        if m.group(1) in {"BLOCK", "BLK"}:
            lot_part = f"B {lot}"
        elif m.group(1) == "UNIT":
            lot_part = f"UNIT {lot}"
        else:
            lot_part = f"L {lot}"
    else:
        number = re.search(r"\b(\d+)\b", legal2)
        if not number:
            return None
        lot_part = number.group(1)

    legal1 = re.sub(
        r"\b(SUBDIVISION|SUBD|SUB|PHASE|SECTION|SEC|UNIT|CONDOMINIUM|A CONDOMINIUM)\b.*",
        "",
        legal1,
    )
    words = [w for w in legal1.split() if w not in {"THE", "A", "AN", "AT", "OF"}]
    if not words:
        return None

    return f"{lot_part} {words[0]}*"


def case_variants(case_number: str) -> list[str]:
    raw = (case_number or "").strip().upper()
    variants: list[str] = []

    def _add(value: str) -> None:
        value = value.strip().upper()
        if value and value not in variants:
            variants.append(value)

    _add(raw)
    m = re.match(r"^\d{2}(\d{4})([A-Z]{2})(\d{6}).*", raw)
    if m:
        year4, case_type, seq = m.group(1), m.group(2), m.group(3)
        _add(f"{year4[2:]}-{case_type}-{seq}")
        _add(f"{year4[2:]}{case_type}{seq}")
    return variants


def is_encumbrance_code(code: str) -> bool:
    return code.upper() in ENCUMBRANCE_CODES


def merge_doc_lists(docs: list[OriDoc]) -> list[OriDoc]:
    merged: dict[str, OriDoc] = {}
    for doc in docs:
        existing = merged.get(doc.instrument)
        if existing is None:
            merged[doc.instrument] = doc
            continue

        if not existing.doc_type and doc.doc_type:
            existing.doc_type = doc.doc_type
            existing.doc_code = doc.doc_code
        if existing.record_date is None and doc.record_date is not None:
            existing.record_date = doc.record_date
        if not existing.legal and doc.legal:
            existing.legal = doc.legal

        for name in doc.party1:
            if name not in existing.party1:
                existing.party1.append(name)
        for name in doc.party2:
            if name not in existing.party2:
                existing.party2.append(name)

        existing.references.update(doc.references)
        existing.source_queries.update(doc.source_queries)

    return list(merged.values())


def linked_mortgages(docs: list[OriDoc]) -> set[str]:
    mortgage_instruments = {
        d.instrument for d in docs if d.doc_code in MORTGAGE_CODES
    }
    if not mortgage_instruments:
        return set()

    linked: set[str] = set()
    release_docs = [d for d in docs if d.doc_code in RELEASE_CODES]
    for rel in release_docs:
        for ref in rel.references:
            if ref in mortgage_instruments:
                linked.add(ref)
    return linked


def resolve_noc_permit_links(case: CaseContext, docs: list[OriDoc]) -> tuple[int, int]:
    noc_docs = [d for d in docs if d.doc_code == "NOC"]
    if not noc_docs:
        return 0, 0

    if not case.permits:
        return len(noc_docs), 0

    linked = 0
    for noc in noc_docs:
        matched = False
        for permit in case.permits:
            if noc.record_date and permit.issue_date:
                if noc.record_date - timedelta(days=30) <= permit.issue_date <= noc.record_date + timedelta(days=730):
                    matched = True
                    break
            else:
                matched = True
                break
        if matched:
            linked += 1

    return len(noc_docs), linked


def today_utc_date() -> date:
    return datetime.now(tz=UTC).date()


class BenchmarkRunner:
    def __init__(self, dsn: str, *, sample_size: int, seed: int) -> None:
        self.engine = get_engine(resolve_pg_dsn(dsn))
        self.sample_size = sample_size
        self.seed = seed

    def load_cases(self) -> list[CaseContext]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        f.foreclosure_id,
                        f.case_number_raw,
                        f.strap,
                        f.folio,
                        f.auction_date,
                        f.property_address,
                        f.judgment_data,
                        f.filing_date,
                        bp.raw_legal1,
                        bp.raw_legal2,
                        bp.raw_legal3,
                        bp.raw_legal4,
                        bp.owner_name,
                        COALESCE(s.sale_count, 0) AS sale_count
                    FROM foreclosures f
                    LEFT JOIN hcpa_bulk_parcels bp ON bp.strap = f.strap
                    LEFT JOIN (
                        SELECT folio, COUNT(*) AS sale_count
                        FROM hcpa_allsales
                        GROUP BY folio
                    ) s ON s.folio = f.folio
                    WHERE f.archived_at IS NULL
                      AND f.strap IS NOT NULL
                      AND f.strap <> 'MULTIPLE PARCEL'
                    ORDER BY f.auction_date NULLS LAST, f.foreclosure_id
                    """
                )
            ).fetchall()

        contexts: list[CaseContext] = []
        for row in rows:
            judgment_data = row[6] or {}
            if isinstance(judgment_data, str):
                try:
                    judgment_data = json.loads(judgment_data)
                except json.JSONDecodeError:
                    judgment_data = {}

            plaintiff = (judgment_data.get("plaintiff") or "").strip()
            defendant = (judgment_data.get("defendant") or "").strip()

            contexts.append(
                CaseContext(
                    foreclosure_id=int(row[0]),
                    case_number=(row[1] or "").strip(),
                    strap=(row[2] or "").strip(),
                    folio=(row[3] or "").strip() or None,
                    auction_date=row[4],
                    property_address=(row[5] or "").strip(),
                    legal1=(row[8] or "").strip(),
                    legal2=(row[9] or "").strip(),
                    legal3=(row[10] or "").strip(),
                    legal4=(row[11] or "").strip(),
                    owner_name=(row[12] or "").strip(),
                    plaintiff=plaintiff,
                    defendant=defendant,
                    filing_date=row[7],
                    sales_count=int(row[13] or 0),
                )
            )

        logger.info("Loaded {} active foreclosure candidates", len(contexts))
        selected = self._select_stratified_sample(contexts)
        logger.info("Selected {} benchmark cases", len(selected))

        for case in selected:
            case.sales_chain = self._load_sales_chain(case)
            case.clerk_defendants = self._load_clerk_defendants(case)
            case.permits = self._load_permits(case)
            case.existing_instruments = self._load_existing_instruments(case)

        return selected

    def _select_stratified_sample(self, cases: list[CaseContext]) -> list[CaseContext]:
        low = [c for c in cases if c.sales_count <= 2]
        mid = [c for c in cases if 3 <= c.sales_count <= 6]
        high = [c for c in cases if c.sales_count >= 7]

        rng = random.Random(self.seed)  # noqa: S311
        rng.shuffle(low)
        rng.shuffle(mid)
        rng.shuffle(high)

        per_bucket = max(1, self.sample_size // 3)
        selected = low[:per_bucket] + mid[:per_bucket] + high[:per_bucket]

        if len(selected) < self.sample_size:
            remaining = [
                c
                for c in cases
                if c.foreclosure_id not in {x.foreclosure_id for x in selected}
            ]
            rng.shuffle(remaining)
            selected.extend(remaining[: self.sample_size - len(selected)])

        selected.sort(key=lambda c: (c.auction_date or date.max, c.case_number))
        return selected[: self.sample_size]

    def _load_sales_chain(self, case: CaseContext) -> list[SaleRow]:
        if not case.folio:
            return []
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT doc_num, sale_date, grantor, grantee
                    FROM hcpa_allsales
                    WHERE folio = :folio
                    ORDER BY sale_date
                    """
                ),
                {"folio": case.folio},
            ).fetchall()

        chain: list[SaleRow] = []
        for doc_num, sale_date, grantor, grantee in rows:
            instrument = only_digits(str(doc_num or ""))
            if not instrument:
                continue
            chain.append(
                SaleRow(
                    instrument=instrument,
                    sale_date=sale_date,
                    grantor=(grantor or "").strip(),
                    grantee=(grantee or "").strip(),
                )
            )
        return chain

    def _load_clerk_defendants(self, case: CaseContext) -> list[str]:
        if not case.case_number:
            return []

        variants = case_variants(case.case_number)
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT DISTINCT name
                    FROM clerk_civil_parties
                    WHERE UPPER(case_number) = ANY(:cases)
                      AND (
                        UPPER(COALESCE(party_type, '')) LIKE '%DEF%'
                        OR UPPER(COALESCE(party_type, '')) LIKE '%RESP%'
                      )
                      AND name IS NOT NULL
                    """
                ),
                {"cases": [v.upper() for v in variants]},
            ).fetchall()

        defendants = []
        for (name,) in rows:
            clean = (name or "").strip()
            if clean and clean not in defendants:
                defendants.append(clean)
        return defendants

    def _load_permits(self, case: CaseContext) -> list[PermitRow]:
        permits: list[PermitRow] = []
        with self.engine.connect() as conn:
            county_rows = []
            if case.folio:
                county_rows = conn.execute(
                    text(
                        """
                        SELECT permit_number, issue_date, complete_date, address, status
                        FROM county_permits
                        WHERE folio_clean = :folio
                        """
                    ),
                    {"folio": case.folio},
                ).fetchall()

            tampa_rows = []
            if case.property_address:
                prefix = case.property_address.split(",")[0].strip().upper()
                if prefix:
                    tampa_rows = conn.execute(
                        text(
                            """
                            SELECT record_number, record_date, expiration_date,
                                   address_normalized, status
                            FROM tampa_accela_records
                            WHERE UPPER(COALESCE(address_normalized, '')) LIKE :prefix
                            """
                        ),
                        {"prefix": f"{prefix}%"},
                    ).fetchall()

        for row in county_rows:
            permits.append(
                PermitRow(
                    source="county",
                    permit_number=row[0],
                    issue_date=row[1],
                    complete_date=row[2],
                    address=row[3],
                    status=row[4],
                )
            )
        for row in tampa_rows:
            permits.append(
                PermitRow(
                    source="tampa",
                    permit_number=row[0],
                    issue_date=row[1],
                    complete_date=row[2],
                    address=row[3],
                    status=row[4],
                )
            )

        return permits

    def _load_existing_instruments(self, case: CaseContext) -> set[str]:
        if not case.strap:
            return set()
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT instrument_number
                    FROM ori_encumbrances
                    WHERE strap = :strap
                      AND instrument_number IS NOT NULL
                    """
                ),
                {"strap": case.strap},
            ).fetchall()

        return {only_digits(str(r[0] or "")) for r in rows if only_digits(str(r[0] or ""))}


def add_docs(target: dict[str, OriDoc], docs: list[OriDoc]) -> None:
    merged = merge_doc_lists(list(target.values()) + docs)
    target.clear()
    for doc in merged:
        target[doc.instrument] = doc


def chase_references(
    client: PAVClient,
    doc_map: dict[str, OriDoc],
    *,
    from_date: date,
    to_date: date,
    max_depth: int = 2,
) -> None:
    queue: list[tuple[str, int]] = []
    seen: set[str] = set()

    for doc in list(doc_map.values()):
        if doc.doc_code in MORTGAGE_CODES | {"LP", "LN", "MEDLN", "LNCORPTX"}:
            queue.append((doc.instrument, 0))

    while queue:
        instrument, depth = queue.pop(0)
        if instrument in seen or depth > max_depth:
            continue
        seen.add(instrument)

        docs = client.search(
            query_id=321,
            keywords=[(1011, f"CLK #{instrument}")],
            label=f"ref:{instrument}",
            from_date=from_date,
            to_date=to_date,
            split_on_truncation=True,
        )
        before = set(doc_map.keys())
        add_docs(doc_map, docs)
        new_docs = [doc_map[x] for x in set(doc_map.keys()) - before]

        if not new_docs:
            docs = client.search(
                query_id=321,
                keywords=[(1011, instrument)],
                label=f"refraw:{instrument}",
                from_date=from_date,
                to_date=to_date,
                split_on_truncation=True,
            )
            before = set(doc_map.keys())
            add_docs(doc_map, docs)
            new_docs = [doc_map[x] for x in set(doc_map.keys()) - before]

        for new_doc in new_docs:
            if new_doc.doc_code in {"ASG", "ASGT", "SAT", "REL", "RELMTG", "PR"}:
                for ref in new_doc.references:
                    if ref and ref not in seen:
                        queue.append((ref, depth + 1))


def strategy_baseline_case_legal_party(case: CaseContext, client: PAVClient) -> dict[str, OriDoc]:
    docs: dict[str, OriDoc] = {}
    matcher = PropertyMatcher(case)

    earliest = date(1990, 1, 1)
    latest = today_utc_date()

    for variant in case_variants(case.case_number):
        add_docs(
            docs,
            client.search(
                query_id=350,
                keywords=[(1259, variant)],
                label=f"case:{variant}",
                split_on_truncation=False,
            ),
        )

    legal_term = build_legal_term(case.legal1, case.legal2)
    if legal_term:
        legal_docs = client.search(
            query_id=321,
            keywords=[(1011, legal_term)],
            label=f"legal:{legal_term}",
            from_date=earliest,
            to_date=latest,
            split_on_truncation=True,
        )
        filtered = [d for d in legal_docs if matcher.likely_matches_property(d)]
        add_docs(docs, filtered)

    party_terms: list[str] = []
    for name in (case.plaintiff, case.defendant, case.owner_name):
        clean = (name or "").strip()
        if clean and clean not in party_terms:
            party_terms.append(clean)

    for term in party_terms:
        party_docs = client.search(
            query_id=326,
            keywords=[(486, term)],
            label=f"party:{term}",
            from_date=earliest,
            to_date=latest,
            split_on_truncation=True,
        )
        filtered = [d for d in party_docs if matcher.likely_matches_property(d)]
        add_docs(docs, filtered)

    chase_references(client, docs, from_date=earliest, to_date=latest, max_depth=2)

    return {k: v for k, v in docs.items() if is_encumbrance_code(v.doc_code)}


def strategy_chain_adjacent(case: CaseContext, client: PAVClient) -> dict[str, OriDoc]:
    docs: dict[str, OriDoc] = {}
    matcher = PropertyMatcher(case)

    earliest = date(1990, 1, 1)
    latest = today_utc_date()

    for variant in case_variants(case.case_number):
        add_docs(
            docs,
            client.search(
                query_id=350,
                keywords=[(1259, variant)],
                label=f"case:{variant}",
                split_on_truncation=False,
            ),
        )

    sales = case.sales_chain
    if case.auction_date is not None:
        sales = [s for s in sales if s.sale_date is None or s.sale_date <= case.auction_date]

    for sale in sales:
        base_docs = client.search(
            query_id=320,
            keywords=[(1006, sale.instrument)],
            label=f"inst:{sale.instrument}",
            split_on_truncation=False,
        )
        add_docs(docs, base_docs)

        try:
            root_value = int(sale.instrument)
        except ValueError:
            continue

        for offset in (1, 2, 3):
            candidate = str(root_value + offset)
            adj_docs = client.search(
                query_id=320,
                keywords=[(1006, candidate)],
                label=f"adj:{candidate}",
                split_on_truncation=False,
            )
            filtered = [d for d in adj_docs if matcher.likely_matches_property(d, sale=sale)]
            add_docs(docs, filtered)

    chase_references(client, docs, from_date=earliest, to_date=latest, max_depth=2)

    return {k: v for k, v in docs.items() if is_encumbrance_code(v.doc_code)}


def strategy_chain_adjacent_clerk(case: CaseContext, client: PAVClient) -> dict[str, OriDoc]:
    docs = strategy_chain_adjacent(case, client)
    matcher = PropertyMatcher(case)

    start = case.filing_date - timedelta(days=730) if case.filing_date else date(1990, 1, 1)
    end = case.auction_date + timedelta(days=365) if case.auction_date else today_utc_date()

    for defendant in case.clerk_defendants:
        party_docs = client.search(
            query_id=326,
            keywords=[(486, defendant)],
            label=f"clerk_def:{defendant}",
            from_date=start,
            to_date=end,
            split_on_truncation=True,
        )
        filtered = [d for d in party_docs if matcher.likely_matches_property(d)]
        add_docs(docs, filtered)

    chase_references(client, docs, from_date=start, to_date=today_utc_date(), max_depth=2)

    return {k: v for k, v in docs.items() if is_encumbrance_code(v.doc_code)}


def strategy_chain_adjacent_clerk_legal_fallback(
    case: CaseContext,
    client: PAVClient,
) -> dict[str, OriDoc]:
    docs = strategy_chain_adjacent_clerk(case, client)
    matcher = PropertyMatcher(case)

    has_lp = any(d.doc_code == "LP" for d in docs.values())
    has_mortgage = any(d.doc_code in MORTGAGE_CODES for d in docs.values())
    if has_lp and has_mortgage:
        return docs

    start = date(1990, 1, 1)
    end = today_utc_date()

    legal_terms: list[str] = []
    primary = build_legal_term(case.legal1, case.legal2)
    if primary:
        legal_terms.append(primary)

    legal1_words = [w for w in normalize_name(case.legal1).split() if len(w) > 2]
    if legal1_words:
        legal_terms.append(" ".join(legal1_words[:4]))

    for term in legal_terms:
        legal_docs = client.search(
            query_id=321,
            keywords=[(1011, term)],
            label=f"fallback_legal:{term}",
            from_date=start,
            to_date=end,
            split_on_truncation=True,
        )
        filtered = [d for d in legal_docs if matcher.likely_matches_property(d)]
        add_docs(docs, filtered)

    chase_references(client, docs, from_date=start, to_date=end, max_depth=2)
    return {k: v for k, v in docs.items() if is_encumbrance_code(v.doc_code)}


def run_strategy(
    strategy_name: str,
    case: CaseContext,
    *,
    query_limit: int,
    min_interval: float,
    max_split_depth: int,
) -> StrategyRun:
    start_ts = time.perf_counter()
    client = PAVClient(
        query_limit=query_limit,
        min_interval=min_interval,
        max_split_depth=max_split_depth,
        timeout_seconds=45.0,
    )

    try:
        if strategy_name == "baseline_case_legal_party":
            docs = strategy_baseline_case_legal_party(case, client)
        elif strategy_name == "chain_adjacent":
            docs = strategy_chain_adjacent(case, client)
        elif strategy_name == "chain_adjacent_clerk":
            docs = strategy_chain_adjacent_clerk(case, client)
        elif strategy_name == "chain_adjacent_clerk_legal_fallback":
            docs = strategy_chain_adjacent_clerk_legal_fallback(case, client)
        else:
            raise ValueError(f"Unknown strategy {strategy_name}")
    except Exception:
        logger.exception(
            "Strategy failed strategy={} case={}",
            strategy_name,
            case.case_number,
        )
        docs = {}
        client.errors += 1
    finally:
        client.close()

    runtime = time.perf_counter() - start_ts

    return StrategyRun(
        strategy=strategy_name,
        case_number=case.case_number,
        docs=docs,
        api_calls=client.api_calls,
        retries=client.retries,
        errors=client.errors,
        truncated_responses=client.truncated_responses,
        unresolved_truncations=client.unresolved_truncations,
        runtime_seconds=runtime,
    )


def score_runs_for_case(case: CaseContext, runs: list[StrategyRun]) -> list[ScoredRun]:
    union_instruments = set(case.existing_instruments)
    union_docs: list[OriDoc] = []

    for run in runs:
        union_instruments.update(run.docs.keys())
        union_docs.extend(run.docs.values())

    if not union_instruments:
        union_instruments = set(case.existing_instruments)

    oracle_linked = linked_mortgages(union_docs)

    scored: list[ScoredRun] = []
    for run in runs:
        docs = list(run.docs.values())
        instruments = set(run.docs.keys())
        recall = 1.0 if not union_instruments else len(instruments & union_instruments) / len(union_instruments)

        linked = linked_mortgages(docs)
        lifecycle_recall = 1.0 if not oracle_linked else len(linked & oracle_linked) / len(oracle_linked)

        lp_found = any(d.doc_code == "LP" for d in docs)
        judgment_found = any(d.doc_code in JUDGMENT_CODES for d in docs)

        mortgages = [d for d in docs if d.doc_code in MORTGAGE_CODES]
        noc_count, noc_linked = resolve_noc_permit_links(case, docs)

        scored.append(
            ScoredRun(
                base=run,
                instrument_recall=round(recall, 4),
                mortgage_lifecycle_recall=round(lifecycle_recall, 4),
                lp_found=lp_found,
                judgment_found=judgment_found,
                mortgage_count=len(mortgages),
                linked_mortgage_count=len(linked),
                noc_count=noc_count,
                noc_permit_link_count=noc_linked,
            )
        )

    return scored


def aggregate(scored_runs: list[ScoredRun]) -> dict[str, Any]:
    grouped: dict[str, list[ScoredRun]] = defaultdict(list)
    for run in scored_runs:
        grouped[run.base.strategy].append(run)

    summary: dict[str, Any] = {}
    for strategy, runs in grouped.items():
        case_count = len(runs)
        if case_count == 0:
            continue

        lp_hits = sum(1 for r in runs if r.lp_found)
        judgment_hits = sum(1 for r in runs if r.judgment_found)
        noc_total = sum(r.noc_count for r in runs)
        noc_links = sum(r.noc_permit_link_count for r in runs)

        mortgage_cases = [r for r in runs if r.mortgage_count > 0]
        mortgage_case_count = len(mortgage_cases)
        mortgage_release_rule_hits = sum(
            1
            for r in mortgage_cases
            if r.linked_mortgage_count > 0
        )

        avg_recall = sum(r.instrument_recall for r in runs) / case_count
        avg_lifecycle = sum(r.mortgage_lifecycle_recall for r in runs) / case_count
        avg_calls = sum(r.base.api_calls for r in runs) / case_count
        avg_runtime = sum(r.base.runtime_seconds for r in runs) / case_count

        summary[strategy] = {
            "cases": case_count,
            "avg_instrument_recall": round(avg_recall, 4),
            "avg_mortgage_lifecycle_recall": round(avg_lifecycle, 4),
            "lp_found_rate": round(lp_hits / case_count, 4),
            "judgment_found_rate": round(judgment_hits / case_count, 4),
            "mortgage_release_rule_rate": (
                round(mortgage_release_rule_hits / mortgage_case_count, 4)
                if mortgage_case_count
                else None
            ),
            "noc_permit_link_rate": round(noc_links / noc_total, 4) if noc_total else None,
            "avg_api_calls_per_case": round(avg_calls, 2),
            "avg_runtime_seconds_per_case": round(avg_runtime, 2),
            "total_retries": sum(r.base.retries for r in runs),
            "total_errors": sum(r.base.errors for r in runs),
            "total_truncated": sum(r.base.truncated_responses for r in runs),
            "total_unresolved_truncations": sum(
                r.base.unresolved_truncations for r in runs
            ),
        }

    return summary


def rank(summary: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    def score(item: tuple[str, dict[str, Any]]) -> tuple[float, float, float, float]:
        _, metrics = item
        return (
            float(metrics.get("avg_instrument_recall") or 0.0),
            float(metrics.get("avg_mortgage_lifecycle_recall") or 0.0),
            -float(metrics.get("avg_api_calls_per_case") or 10_000.0),
            -float(metrics.get("avg_runtime_seconds_per_case") or 10_000.0),
        )

    return sorted(summary.items(), key=score, reverse=True)


def write_markdown(
    output_path: Path,
    *,
    benchmark: dict[str, Any],
    ranking: list[tuple[str, dict[str, Any]]],
) -> None:
    lines: list[str] = []
    lines.append("# Encumbrance Strategy Benchmark")
    lines.append("")
    lines.append(f"Generated: {benchmark['generated_at']}")
    lines.append(f"Sample size: {benchmark['sample_size']}")
    lines.append("")
    lines.append("## Ranking")
    lines.append("")

    for idx, (name, metrics) in enumerate(ranking, start=1):
        lines.append(f"{idx}. `{name}`")
        lines.append(
            "   "
            f"recall={metrics['avg_instrument_recall']}, "
            f"lifecycle={metrics['avg_mortgage_lifecycle_recall']}, "
            f"calls/case={metrics['avg_api_calls_per_case']}, "
            f"runtime/case={metrics['avg_runtime_seconds_per_case']}s"
        )

    lines.append("")
    lines.append("## Summary Table")
    lines.append("")
    lines.append(
        "| Strategy | Recall | Lifecycle | LP Rate | Release Rule | NOC->Permit | "
        "Calls/Case | Runtime/Case |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")

    for name, metrics in ranking:
        lines.append(
            "| "
            f"{name} | "
            f"{metrics['avg_instrument_recall']} | "
            f"{metrics['avg_mortgage_lifecycle_recall']} | "
            f"{metrics['lp_found_rate']} | "
            f"{metrics['mortgage_release_rule_rate']} | "
            f"{metrics['noc_permit_link_rate']} | "
            f"{metrics['avg_api_calls_per_case']} | "
            f"{metrics['avg_runtime_seconds_per_case']}s |"
        )

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark ORI encumbrance discovery strategies."
    )
    parser.add_argument("--dsn", default=None, help="Optional PG DSN override")
    parser.add_argument(
        "--sample-size",
        type=int,
        default=12,
        help="Number of properties in stratified benchmark sample",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for stratified selection",
    )
    parser.add_argument(
        "--query-limit",
        type=int,
        default=DEFAULT_QUERY_LIMIT,
        help="PAV QueryLimit for each request",
    )
    parser.add_argument(
        "--min-interval",
        type=float,
        default=DEFAULT_MIN_INTERVAL_SECONDS,
        help="Minimum seconds between PAV requests",
    )
    parser.add_argument(
        "--max-split-depth",
        type=int,
        default=DEFAULT_MAX_SPLIT_DEPTH,
        help="Max recursion depth when splitting truncated date ranges",
    )
    parser.add_argument(
        "--output-dir",
        default="logs",
        help="Directory for benchmark JSON and markdown output",
    )
    parser.add_argument(
        "--strategies",
        nargs="+",
        default=[
            "baseline_case_legal_party",
            "chain_adjacent",
            "chain_adjacent_clerk",
            "chain_adjacent_clerk_legal_fallback",
        ],
        choices=[
            "baseline_case_legal_party",
            "chain_adjacent",
            "chain_adjacent_clerk",
            "chain_adjacent_clerk_legal_fallback",
        ],
        help="Strategies to execute (space-separated)",
    )
    args = parser.parse_args()

    runner = BenchmarkRunner(
        dsn=resolve_pg_dsn(args.dsn),
        sample_size=max(3, args.sample_size),
        seed=args.seed,
    )
    cases = runner.load_cases()
    if not cases:
        raise SystemExit("No benchmark cases available")

    strategy_names = args.strategies

    scored_all: list[ScoredRun] = []
    case_level: list[dict[str, Any]] = []

    for idx, case in enumerate(cases, start=1):
        logger.info(
            "Benchmark case {}/{} {} strap={} folio={} sales={}",
            idx,
            len(cases),
            case.case_number,
            case.strap,
            case.folio,
            case.sales_count,
        )

        runs = []
        for strategy_name in strategy_names:
            logger.info("  Strategy start: {}", strategy_name)
            run = run_strategy(
                strategy_name,
                case,
                query_limit=args.query_limit,
                min_interval=args.min_interval,
                max_split_depth=args.max_split_depth,
            )
            logger.info(
                "  Strategy done: {} docs={} calls={} runtime={:.1f}s trunc={} errs={}",
                strategy_name,
                len(run.docs),
                run.api_calls,
                run.runtime_seconds,
                run.truncated_responses,
                run.errors,
            )
            runs.append(run)

        scored = score_runs_for_case(case, runs)
        scored_all.extend(scored)

        case_level.append(
            {
                "case_number": case.case_number,
                "strap": case.strap,
                "folio": case.folio,
                "sales_count": case.sales_count,
                "strategies": [
                    {
                        "name": s.base.strategy,
                        "documents": len(s.base.docs),
                        "api_calls": s.base.api_calls,
                        "runtime_seconds": round(s.base.runtime_seconds, 3),
                        "instrument_recall": s.instrument_recall,
                        "mortgage_lifecycle_recall": s.mortgage_lifecycle_recall,
                        "lp_found": s.lp_found,
                        "judgment_found": s.judgment_found,
                        "mortgage_count": s.mortgage_count,
                        "linked_mortgage_count": s.linked_mortgage_count,
                        "noc_count": s.noc_count,
                        "noc_permit_link_count": s.noc_permit_link_count,
                        "truncated_responses": s.base.truncated_responses,
                        "unresolved_truncations": s.base.unresolved_truncations,
                        "errors": s.base.errors,
                    }
                    for s in scored
                ],
            }
        )

    summary = aggregate(scored_all)
    ranking = rank(summary)

    generated = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    benchmark = {
        "generated_at": generated,
        "sample_size": len(cases),
        "strategies": summary,
        "ranking": [name for name, _ in ranking],
        "cases": case_level,
    }

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
    json_path = out_dir / f"encumbrance_benchmark_{stamp}.json"
    md_path = out_dir / f"encumbrance_benchmark_{stamp}.md"

    json_path.write_text(json.dumps(benchmark, indent=2, default=str), encoding="utf-8")
    write_markdown(md_path, benchmark=benchmark, ranking=ranking)

    logger.info("Benchmark JSON written: {}", json_path)
    logger.info("Benchmark markdown written: {}", md_path)

    if ranking:
        winner = ranking[0][0]
        logger.info("Recommended strategy: {}", winner)


if __name__ == "__main__":
    main()
