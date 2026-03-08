# ruff: noqa: N999
"""
Plant City permit capture service (PostgreSQL-first).

Architectural purpose:
- Pull permit records from Plant City's public Maintstar portal.
- Normalize those records into the shared `tampa_accela_records` permit store so
  existing downstream permit/title-chain queries can consume them immediately.

How it fits in the broader system:
- Used by `PgPermitSinglePinService` when property jurisdiction routes to
  Plant City.
- Serves as municipal expansion coverage for non-Tampa incorporated areas.
"""

from __future__ import annotations

import json
import re
import time
from datetime import UTC
from datetime import date
from datetime import datetime
from typing import Any
from urllib.parse import urlencode

from loguru import logger
import requests
from sqlalchemy import text

from src.services.TampaPermit import TampaPermitService
from sunbiz.db import get_engine
from sunbiz.db import resolve_pg_dsn
from sunbiz.models import Base


PLANT_CITY_BASE_URL = "https://h8.maintstar.co/plantcity"
PLANT_CITY_SEARCH_URL = f"{PLANT_CITY_BASE_URL}/api/Public/Record/Search"


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value if text_value else None


def _to_int(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _parse_iso_date(value: Any) -> date | None:
    text_value = _clean_text(value)
    if not text_value:
        return None
    normalized = text_value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).date()
    except ValueError:
        return None


def _stable_record_number(raw_number: str | None, record_id: int | None) -> str:
    record_key = re.sub(r"[^A-Z0-9]+", "-", (raw_number or "").upper()).strip("-")
    if not record_key and record_id is not None:
        record_key = f"ID-{record_id}"
    if not record_key:
        record_key = "UNKNOWN"
    return f"PLANTCITY:{record_key}"


class PlantCityPermitService:
    """Search Plant City Maintstar records and upsert to `tampa_accela_records`."""

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
                "User-Agent": "HillsInspector/PlantCityPermit/1.0",
                "Accept": "application/json,text/plain,*/*",
                "Referer": f"{PLANT_CITY_BASE_URL}/portal/",
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

    def _request_json(
        self,
        *,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(
                    PLANT_CITY_SEARCH_URL,
                    params=params,
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                payload: dict[str, Any] = response.json()
                return payload
            except Exception as exc:
                last_error = exc
                if attempt == self.max_retries:
                    break
                sleep_seconds = self.retry_backoff_seconds * attempt
                time.sleep(sleep_seconds)
        if last_error is None:
            raise RuntimeError("plant city request failed with unknown error")
        raise RuntimeError(f"plant city request failed: {last_error}") from last_error

    @staticmethod
    def _normalize_record(
        record: dict[str, Any],
        *,
        query: str,
    ) -> dict[str, Any] | None:
        raw_number = _clean_text(record.get("number"))
        record_id = _to_int(record.get("id"))
        record_number = _stable_record_number(raw_number, record_id)
        if not record_number:
            return None

        ms_type = _clean_text(record.get("msType"))
        sub_type = _clean_text(record.get("type"))
        status = _clean_text(record.get("status"))
        address_raw = _clean_text(record.get("address"))
        address_parts = TampaPermitService.normalize_address(address_raw)

        record_type = " - ".join([part for part in [ms_type, sub_type] if part]) or "Plant City Permit"
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
        is_fix_record = TampaPermitService.is_fix_record(record_type, _clean_text(record.get("description")))

        detail_url = (
            f"{PLANT_CITY_BASE_URL}/portal/#/record/{record_id}"
            if record_id is not None
            else None
        )

        return {
            "record_number": record_number,
            "record_date": _parse_iso_date(record.get("dateVal")),
            "record_type": record_type,
            "module": module,
            "short_notes": _clean_text(record.get("description")),
            "project_name": ms_type or sub_type,
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
            "estimated_work_cost": None,
            "estimated_cost_source": None,
            "detail_url": detail_url,
            "expiration_date": None,
            "source_query_text": f"plant_city:{query}",
            "source_export_url": (
                f"{PLANT_CITY_SEARCH_URL}?{urlencode({'query': query, 'skip': 0, 'take': 100})}"
            ),
            "source_payload": json.dumps(
                {
                    "source_system": "plant_city_maintstar",
                    "external_record_number": raw_number,
                    "record": record,
                },
                default=str,
            ),
        }

    def _upsert_rows(self, rows: list[dict[str, Any]], *, batch_size: int = 500) -> int:
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
        max_rows: int = 100,
    ) -> dict[str, Any]:
        query = _clean_text(address)
        if not query:
            return {"skipped": True, "reason": "missing_address"}

        max_rows = max(1, min(int(max_rows), 1000))
        take = min(100, max_rows)
        skip = 0
        observed_rows: list[dict[str, Any]] = []
        show_more_mode = True

        while len(observed_rows) < max_rows and show_more_mode:
            payload = self._request_json(params={"query": query, "skip": skip, "take": take})
            batch_rows = payload.get("data") if isinstance(payload.get("data"), list) else []
            if not batch_rows:
                break
            observed_rows.extend(
                [row for row in batch_rows if isinstance(row, dict)]
            )
            skip += len(batch_rows)
            show_more_mode = bool(payload.get("showMoreMode"))
            if len(batch_rows) < take:
                break

        deduped: list[dict[str, Any]] = []
        seen_keys: set[str] = set()
        for row in observed_rows[:max_rows]:
            key = (
                f"id:{_to_int(row.get('id'))}"
                if _to_int(row.get("id")) is not None
                else f"num:{_clean_text(row.get('number'))}"
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped.append(row)

        normalized_rows = [
            normalized
            for row in deduped
            if isinstance(row, dict)
            for normalized in [self._normalize_record(row, query=query)]
            if normalized is not None
        ]
        written = self._upsert_rows(normalized_rows)

        stats = {
            "address_query": query,
            "records_observed": len(deduped),
            "records_normalized": len(normalized_rows),
            "written": written,
            "fetched_at_utc": datetime.now(tz=UTC).isoformat(),
        }
        logger.info("Plant City permit sync complete: {}", stats)
        return stats
