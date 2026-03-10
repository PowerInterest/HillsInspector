# ruff: noqa: N999
"""
Temple Terrace permit capture service (PostgreSQL-first).

Architectural purpose:
- Query Temple Terrace's Click2Gov permit search flow by address.
- Parse permit result rows and status-detail pages into normalized permit rows.
- Persist those rows into shared `tampa_accela_records` for downstream reuse.

How it fits in the broader system:
- Used by `PgPermitSinglePinService` when jurisdiction routes to Temple Terrace.
- Expands municipal permit coverage beyond Tampa + county ArcGIS exports.
"""

from __future__ import annotations

import json
import re
import time
from datetime import UTC
from datetime import date
from datetime import datetime
from typing import Any
from urllib.parse import parse_qs
from urllib.parse import urljoin
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from loguru import logger
import requests
from sqlalchemy import text

from src.services.TampaPermit import TampaPermitService
from sunbiz.db import get_engine
from sunbiz.db import resolve_pg_dsn
from sunbiz.models import Base


TEMPLE_TERRACE_BASE_URL = "https://temp-egov.aspgov.com/Click2GovBP"
TEMPLE_TERRACE_SELECT_URL = f"{TEMPLE_TERRACE_BASE_URL}/selectpermit.html"

_DIRECTION_MAP = {
    "N": "NORTH",
    "S": "SOUTH",
    "E": "EAST",
    "W": "WEST",
    "NE": "NORTH EAST",
    "NW": "NORTH WEST",
    "SE": "SOUTH EAST",
    "SW": "SOUTH WEST",
}

_SUFFIX_MAP = {
    "ALY": "ALLEY",
    "AVE": "AVENUE",
    "BLVD": "BOULEVARD",
    "BND": "BEND",
    "BR": "BRANCH",
    "BYU": "BAYOU",
    "CIR": "CIRCLE",
    "CRK": "CREEK",
    "CSWY": "CAUSEWAY",
    "CT": "COURT",
    "CV": "COVE",
    "DR": "DRIVE",
    "EXPY": "EXPRESSWAY",
    "EXT": "EXTENSION",
    "HTS": "HEIGHTS",
    "HWY": "HIGHWAY",
    "INLT": "INLET",
    "IS": "ISLAND",
    "ISLE": "ISLE",
    "JCT": "JUNCTION",
    "LK": "LAKE",
    "LN": "LANE",
    "LOOP": "LOOP",
    "MALL": "MALL",
    "MNR": "MANOR",
    "MT": "MOUNT",
    "OVAL": "OVAL",
    "PARK": "PARK",
    "PASS": "PASS",
    "PATH": "PATH",
    "PIKE": "PIKE",
    "PKWY": "PARKWAY",
    "PL": "PLACE",
    "PLZ": "PLAZA",
    "PT": "POINT",
    "RD": "ROAD",
    "RIV": "RIVER",
    "ROW": "ROW",
    "RUN": "RUN",
    "SQ": "SQUARE",
    "ST": "STREET",
    "TER": "TERRACE",
    "TRCE": "TRACE",
    "TRL": "TRAIL",
    "VW": "VIEW",
    "WALK": "WALK",
    "WAY": "WAY",
}


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value if text_value else None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    cleaned = re.sub(r"[^0-9.]", "", str(value))
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_date(value: Any) -> date | None:
    text_value = _clean_text(value)
    if not text_value:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text_value, fmt).date()
        except ValueError:
            continue
    return None


def _stable_record_number(raw_number: str | None) -> str:
    record_key = re.sub(r"[^A-Z0-9]+", "-", (raw_number or "").upper()).strip("-")
    if not record_key:
        record_key = "UNKNOWN"
    return f"TEMPLETERRACE:{record_key}"


class TempleTerracePermitService:
    """Search Temple Terrace Click2Gov records and upsert normalized permit rows."""

    def __init__(
        self,
        *,
        pg_dsn: str | None = None,
        timeout_seconds: int = 45,
        max_retries: int = 4,
        retry_backoff_seconds: float = 1.0,
    ) -> None:
        self.dsn = resolve_pg_dsn(pg_dsn)
        self._engine = get_engine(self.dsn)
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "HillsInspector/TempleTerracePermit/1.0",
                "Referer": f"{TEMPLE_TERRACE_BASE_URL}/index.html",
            }
        )
        self._ensure_pg_table()

    def _ensure_pg_table(self) -> None:
        table_name = "tampa_accela_records"
        if table_name not in Base.metadata.tables:
            raise RuntimeError(
                "tampa_accela_records model not found in Base.metadata; "
                "check sunbiz.models.TampaAccelaRecord"
            )
        Base.metadata.create_all(bind=self._engine, tables=[Base.metadata.tables[table_name]])

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> requests.Response:
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    data=data,
                    timeout=self.timeout_seconds,
                    allow_redirects=True,
                )
                response.raise_for_status()
                return response
            except Exception as exc:
                last_error = exc
                if attempt == self.max_retries:
                    break
                sleep_seconds = self.retry_backoff_seconds * attempt
                time.sleep(sleep_seconds)
        if last_error is None:
            raise RuntimeError("temple terrace request failed with unknown error")
        raise RuntimeError(f"temple terrace request failed: {last_error}") from last_error

    @staticmethod
    def _extract_owasp_token(html_text: str, response_url: str) -> str:
        query_token = parse_qs(urlparse(response_url).query).get("OWASP_CSRFTOKEN", [None])[0]
        query_token = _clean_text(query_token)
        if query_token:
            return query_token

        soup = BeautifulSoup(html_text, "html.parser")
        input_el = soup.find("input", attrs={"name": "OWASP_CSRFTOKEN"})
        value = _clean_text(input_el.get("value") if input_el else None)
        if value:
            return value

        raise RuntimeError("temple terrace owasp token missing in response")

    @staticmethod
    def _parse_address_components(address: str) -> dict[str, str]:
        street = _clean_text(address.split(",", 1)[0]) or ""
        tokens = street.split()
        if not tokens:
            return {
                "street_number": "",
                "street_direction": "",
                "street_name": "",
                "street_suffix": "",
            }

        idx = 0
        street_number = ""
        if tokens and re.fullmatch(r"\d+[A-Za-z]?", tokens[0]):
            street_number = tokens[0]
            idx = 1

        street_direction = ""
        if idx < len(tokens):
            maybe_dir = tokens[idx].upper().strip(".")
            if maybe_dir in _DIRECTION_MAP:
                street_direction = _DIRECTION_MAP[maybe_dir]
                idx += 1

        remaining = [token for token in tokens[idx:] if token]
        street_suffix = ""
        if remaining:
            maybe_suffix = remaining[-1].upper().strip(".")
            if maybe_suffix in _SUFFIX_MAP:
                street_suffix = _SUFFIX_MAP[maybe_suffix]
                remaining = remaining[:-1]

        street_name = " ".join(remaining).strip()
        if not street_name:
            street_name = street

        return {
            "street_number": street_number,
            "street_direction": street_direction,
            "street_name": street_name,
            "street_suffix": street_suffix,
        }

    def _build_search_payload(self, address: str, token: str) -> dict[str, str]:
        parsed = self._parse_address_components(address)
        return {
            "searchResultsView": "true",
            "searchType": "1",
            "parcel.streetNumber": parsed["street_number"],
            "parcel.streetDirection": parsed["street_direction"],
            "parcel.streetName": parsed["street_name"],
            "streetSearchType": "contains",
            "parcel.streetSuffix": parsed["street_suffix"],
            "target1": "Continue",
            "OWASP_CSRFTOKEN": token,
        }

    @staticmethod
    def _extract_search_rows(html_text: str, base_url: str) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html_text, "html.parser")
        rows: list[dict[str, Any]] = []
        seen_numbers: set[str] = set()

        for anchor in soup.select('a[href*="validatePermitView=true"]'):
            number = _clean_text(anchor.get_text(" ", strip=True))
            href = _clean_text(anchor.get("href"))
            if not number or not href:
                continue
            if number in seen_numbers:
                continue
            seen_numbers.add(number)

            tr = anchor.find_parent("tr")
            cells = tr.find_all("td") if tr else []
            address = _clean_text(cells[1].get_text(" ", strip=True)) if len(cells) > 1 else None
            parcel = _clean_text(cells[2].get_text(" ", strip=True)) if len(cells) > 2 else None
            detail_url = urljoin(base_url, href)
            rows.append(
                {
                    "application_number": number,
                    "address": address,
                    "parcel_id": parcel,
                    "detail_url": detail_url,
                }
            )
        return rows

    @staticmethod
    def _extract_labeled_value(lines: list[str], label: str) -> str | None:
        label_lower = label.lower()
        for idx, line in enumerate(lines):
            normalized = line.strip()
            if not normalized.lower().startswith(f"{label_lower}:"):
                continue
            remainder = normalized.split(":", 1)[1].strip()
            if remainder:
                return remainder
            if idx + 1 < len(lines):
                return _clean_text(lines[idx + 1])
        return None

    def _fetch_detail_fields(self, detail_url: str) -> dict[str, Any]:
        response = self._request("GET", detail_url)
        soup = BeautifulSoup(response.text, "html.parser")
        text_lines = [
            line.strip()
            for line in soup.get_text("\n", strip=True).splitlines()
            if line.strip()
        ]

        return {
            "application_number": self._extract_labeled_value(text_lines, "Application Number"),
            "application_type": self._extract_labeled_value(text_lines, "Application Type"),
            "application_date": self._extract_labeled_value(text_lines, "Application Date"),
            "application_status": self._extract_labeled_value(text_lines, "Application Status"),
            "address": self._extract_labeled_value(text_lines, "Address"),
            "parcel_id": self._extract_labeled_value(text_lines, "Parcel ID"),
            "owner": self._extract_labeled_value(text_lines, "Owner"),
            "valuation": self._extract_labeled_value(text_lines, "Valuation"),
            "detail_url": detail_url,
        }

    @staticmethod
    def _normalize_record(
        search_row: dict[str, Any],
        detail: dict[str, Any],
        *,
        query: str,
    ) -> dict[str, Any] | None:
        raw_number = _clean_text(detail.get("application_number")) or _clean_text(
            search_row.get("application_number")
        )
        if not raw_number:
            return None

        record_number = _stable_record_number(raw_number)
        record_type = _clean_text(detail.get("application_type")) or "Temple Terrace Permit"
        status = _clean_text(detail.get("application_status"))
        address_raw = _clean_text(detail.get("address")) or _clean_text(search_row.get("address"))
        address_parts = TampaPermitService.normalize_address(address_raw)
        module = "Building"

        is_violation = TampaPermitService.is_violation_record(module, record_type)
        is_open = TampaPermitService.is_open_status(status)
        needs_closeout = TampaPermitService.needs_closeout_for_record(
            record_number=record_number,
            module=module,
            record_type=record_type,
            status=status,
            is_violation=is_violation,
        )
        is_fix_record = TampaPermitService.is_fix_record(record_type, None)

        valuation = _to_float(detail.get("valuation"))
        estimated_cost_source = (
            "temple_terrace_status_detail" if valuation is not None else None
        )

        return {
            "record_number": record_number,
            "record_date": _parse_date(detail.get("application_date")),
            "record_type": record_type,
            "module": module,
            "short_notes": _clean_text(detail.get("owner")),
            "project_name": record_type,
            "status": status,
            "address_raw": address_parts.get("address_raw"),
            "address_normalized": address_parts.get("address_normalized"),
            "city": address_parts.get("city"),
            "state": address_parts.get("state"),
            "zip_code": address_parts.get("zip_code"),
            "is_violation": is_violation,
            "is_open": bool(is_open),
            "needs_closeout": bool(needs_closeout),
            "is_fix_record": bool(is_fix_record),
            "estimated_work_cost": valuation,
            "estimated_cost_source": estimated_cost_source,
            "detail_url": _clean_text(detail.get("detail_url")),
            "expiration_date": None,
            "source_query_text": f"temple_terrace:{query}",
            "source_export_url": TEMPLE_TERRACE_SELECT_URL,
            "source_payload": json.dumps(
                {
                    "source_system": "temple_terrace_click2gov",
                    "external_record_number": raw_number,
                    "search_row": search_row,
                    "detail": detail,
                },
                default=str,
            ),
        }

    def _upsert_rows(self, rows: list[dict[str, Any]], *, batch_size: int = 250) -> int:
        if not rows:
            return 0

        upsert_sql = text(
            """
            INSERT INTO tampa_accela_records (
                record_number,
                record_date,
                record_type,
                module,
                short_notes,
                project_name,
                status,
                address_raw,
                address_normalized,
                city,
                state,
                zip_code,
                is_violation,
                is_open,
                needs_closeout,
                is_fix_record,
                estimated_work_cost,
                estimated_cost_source,
                detail_url,
                expiration_date,
                source_query_text,
                source_export_url,
                source_payload,
                source_ingested_at,
                updated_at
            ) VALUES (
                :record_number,
                :record_date,
                :record_type,
                :module,
                :short_notes,
                :project_name,
                :status,
                :address_raw,
                :address_normalized,
                :city,
                :state,
                :zip_code,
                :is_violation,
                :is_open,
                :needs_closeout,
                :is_fix_record,
                :estimated_work_cost,
                :estimated_cost_source,
                :detail_url,
                :expiration_date,
                :source_query_text,
                :source_export_url,
                CAST(:source_payload AS jsonb),
                now(),
                now()
            )
            ON CONFLICT (record_number) DO UPDATE SET
                record_date = COALESCE(EXCLUDED.record_date, tampa_accela_records.record_date),
                record_type = COALESCE(EXCLUDED.record_type, tampa_accela_records.record_type),
                module = COALESCE(EXCLUDED.module, tampa_accela_records.module),
                short_notes = COALESCE(EXCLUDED.short_notes, tampa_accela_records.short_notes),
                project_name = COALESCE(EXCLUDED.project_name, tampa_accela_records.project_name),
                status = COALESCE(EXCLUDED.status, tampa_accela_records.status),
                address_raw = COALESCE(EXCLUDED.address_raw, tampa_accela_records.address_raw),
                address_normalized = COALESCE(
                    EXCLUDED.address_normalized,
                    tampa_accela_records.address_normalized
                ),
                city = COALESCE(EXCLUDED.city, tampa_accela_records.city),
                state = COALESCE(EXCLUDED.state, tampa_accela_records.state),
                zip_code = COALESCE(EXCLUDED.zip_code, tampa_accela_records.zip_code),
                is_violation = COALESCE(EXCLUDED.is_violation, tampa_accela_records.is_violation),
                is_open = COALESCE(EXCLUDED.is_open, tampa_accela_records.is_open),
                needs_closeout = COALESCE(
                    EXCLUDED.needs_closeout,
                    tampa_accela_records.needs_closeout
                ),
                is_fix_record = COALESCE(EXCLUDED.is_fix_record, tampa_accela_records.is_fix_record),
                estimated_work_cost = COALESCE(
                    EXCLUDED.estimated_work_cost,
                    tampa_accela_records.estimated_work_cost
                ),
                estimated_cost_source = CASE
                    WHEN EXCLUDED.estimated_work_cost IS NOT NULL
                    THEN EXCLUDED.estimated_cost_source
                    ELSE tampa_accela_records.estimated_cost_source
                END,
                detail_url = COALESCE(EXCLUDED.detail_url, tampa_accela_records.detail_url),
                expiration_date = COALESCE(
                    EXCLUDED.expiration_date,
                    tampa_accela_records.expiration_date
                ),
                source_query_text = COALESCE(
                    EXCLUDED.source_query_text,
                    tampa_accela_records.source_query_text
                ),
                source_export_url = COALESCE(
                    EXCLUDED.source_export_url,
                    tampa_accela_records.source_export_url
                ),
                source_payload = COALESCE(EXCLUDED.source_payload, tampa_accela_records.source_payload),
                source_ingested_at = now(),
                updated_at = now()
            """
        )

        written = 0
        with self._engine.begin() as conn:
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                conn.execute(upsert_sql, batch)
                written += len(batch)
        return written

    def sync_address_to_postgres(
        self,
        address: str,
        *,
        max_rows: int = 25,
    ) -> dict[str, Any]:
        query = _clean_text(address)
        if not query:
            return {"skipped": True, "reason": "missing_address"}

        max_rows = max(1, min(int(max_rows), 100))
        init_response = self._request(
            "GET",
            TEMPLE_TERRACE_SELECT_URL,
            params={"initialSearchView": "true"},
        )
        token = self._extract_owasp_token(init_response.text, str(init_response.url or ""))
        payload = self._build_search_payload(query, token)

        result_response = self._request(
            "POST",
            TEMPLE_TERRACE_SELECT_URL,
            data=payload,
        )
        search_rows = self._extract_search_rows(
            result_response.text,
            str(result_response.url or ""),
        )
        search_rows = search_rows[:max_rows]

        normalized_rows: list[dict[str, Any]] = []
        detail_errors = 0
        for row in search_rows:
            detail_url = _clean_text(row.get("detail_url"))
            if not detail_url:
                continue
            try:
                detail = self._fetch_detail_fields(detail_url)
            except Exception as exc:
                detail_errors += 1
                logger.warning(
                    "Temple Terrace detail fetch failed for {}: {}",
                    detail_url,
                    exc,
                )
                detail = {
                    "detail_url": detail_url,
                    "detail_fetch_error": str(exc),
                }
            normalized = self._normalize_record(row, detail, query=query)
            if normalized is not None:
                normalized_rows.append(normalized)

        written = self._upsert_rows(normalized_rows)
        stats = {
            "address_query": query,
            "records_observed": len(search_rows),
            "records_normalized": len(normalized_rows),
            "written": written,
            "detail_errors": detail_errors,
            "fetched_at_utc": datetime.now(tz=UTC).isoformat(),
        }
        logger.info("Temple Terrace permit sync complete: {}", stats)
        return stats
