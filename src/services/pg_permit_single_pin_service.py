"""
Targeted single-PIN permit gap-fill service (PostgreSQL-first).

Architectural purpose:
- Run a deep permit fetch for a small set of foreclosure properties that still
  have permit coverage/value gaps after bulk permit ingestion.
- Reuse the HCPA parcel permit feed (`ParcelData?pin=...`) plus optional
  Accela/ArcGIS enrichment to extract additional permit detail.
- Persist results into existing permit tables only:
  - `county_permits`
  - `tampa_accela_records`
- When HCPA exposes a county permit that is not present in the ArcGIS export,
  write a fallback `county_permits` row using reserved source identity
  `(source_layer_id=-1, source_object_id=<HCPA permit row id>)` so downstream
  county-permit joins can still see the permit instead of dropping it.

How this fits the broader system:
- Intended as a fallback step inside `pg_pipeline_controller`, between permit
  bulk sync and title-chain generation.
- Avoids schema drift by writing to existing tables and preserving the same
  downstream joins/events.
- Designed to fail loudly on hard errors so permit data regressions are visible.
"""

from __future__ import annotations

import json
import re
from typing import Any
from typing import cast

from loguru import logger
from sqlalchemy import text

from src.services.CountyPermit import CountyPermitService
from src.services.PlantCityPermit import PlantCityPermitService
from src.services.TampaPermit import TampaPermitService
from src.services.TempleTerracePermit import TempleTerracePermitService
from src.tools.pg_permit_single_pin import PermitSinglePinFetcher
from sunbiz.db import get_engine
from sunbiz.db import resolve_pg_dsn

COUNTY_ARCGIS_LAYER_ID = 0
HCPA_SINGLE_PIN_LAYER_ID = -1


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value if text_value else None


def _normalize_folio(value: str | None) -> str | None:
    if not value:
        return None
    digits = "".join(ch for ch in value if ch.isdigit())
    return digits or None


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _extract_city_from_site_address(site_address: str | None) -> str | None:
    text_value = _clean_text(site_address)
    if not text_value:
        return None

    parsed_city = TampaPermitService.normalize_address(text_value).get("city")
    if parsed_city:
        return _clean_text(parsed_city)

    parts = [_clean_text(part) for part in text_value.split(",")]
    parts = [part for part in parts if part]
    if len(parts) >= 2:
        return parts[1]
    return None


def _city_key_from_site_address(site_address: str | None) -> str | None:
    city = _extract_city_from_site_address(site_address)
    if not city:
        return None
    return re.sub(r"[^A-Z]", "", city.upper())


def _street_query_from_site_address(site_address: str | None) -> str | None:
    text_value = _clean_text(site_address)
    if not text_value:
        return None
    return _clean_text(text_value.split(",", 1)[0]) or text_value


def _looks_like_tampa_record_number(record_number: str | None) -> bool:
    """
    Heuristic for Tampa Accela record ids.

    Example: BLD-25-0513202
    """
    value = _clean_text(record_number)
    if not value:
        return False
    return bool(re.match(r"^[A-Z]{3}-\d{2}-\d{6,10}$", value))


class PgPermitSinglePinService:
    """Sync single-pin permit payloads into existing PostgreSQL permit tables."""

    def __init__(
        self,
        *,
        dsn: str | None = None,
        timeout_seconds: int = 45,
        include_accela: bool = True,
        include_arcgis: bool = True,
        include_plant_city: bool = True,
        include_temple_terrace: bool = True,
    ) -> None:
        self.dsn = resolve_pg_dsn(dsn)
        self._engine = get_engine(self.dsn)
        self.fetcher = PermitSinglePinFetcher(
            timeout_seconds=timeout_seconds,
            include_accela=include_accela,
            include_arcgis=include_arcgis,
        )
        self.include_plant_city = include_plant_city
        self.include_temple_terrace = include_temple_terrace
        self._plant_city_service = (
            PlantCityPermitService(pg_dsn=self.dsn, timeout_seconds=timeout_seconds)
            if include_plant_city
            else None
        )
        self._temple_terrace_service = (
            TempleTerracePermitService(pg_dsn=self.dsn, timeout_seconds=timeout_seconds)
            if include_temple_terrace
            else None
        )

    @staticmethod
    def _route_jurisdiction(site_address: str | None) -> str:
        city_key = _city_key_from_site_address(site_address)
        if city_key == "PLANTCITY":
            return "plant_city"
        if city_key == "TEMPLETERRACE":
            return "temple_terrace"
        if city_key == "TAMPA":
            return "tampa"
        return "county"

    @staticmethod
    def _best_estimated_cost(permit: dict[str, Any]) -> tuple[float | None, str | None]:
        accela = permit.get("accela") if isinstance(permit.get("accela"), dict) else {}
        detail_extract = (
            accela.get("detail_extract") if isinstance(accela.get("detail_extract"), dict) else {}
        )
        search_extract = (
            accela.get("search_extract") if isinstance(accela.get("search_extract"), dict) else {}
        )

        candidates: list[tuple[float | None, str]] = [
            (detail_extract.get("job_value"), "accela_detail_job_value"),
            (detail_extract.get("alt_valuation"), "accela_detail_alt_valuation"),
            (search_extract.get("job_value"), "accela_search_job_value"),
            (search_extract.get("alt_valuation"), "accela_search_alt_valuation"),
            (permit.get("estimated_value"), "hcpa_est_value"),
        ]
        for value, source in candidates:
            if value is None:
                continue
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                continue
            if numeric > 0:
                return numeric, source
        return None, None

    @staticmethod
    def _is_tampa_candidate(permit: dict[str, Any]) -> bool:
        source_guess = _clean_text(permit.get("source_guess")) or ""
        permit_url = (_clean_text(permit.get("permit_url")) or "").lower()
        permit_number = _clean_text(permit.get("permit_number"))

        if source_guess == "tampa":
            return True
        if "accela.com/tampa" in permit_url:
            return True
        return _looks_like_tampa_record_number(permit_number)

    @staticmethod
    def _upsert_county_arcgis(conn: Any, attrs: dict[str, Any]) -> int:
        normalized = CountyPermitService.normalize_attributes(attrs)
        permit_number = _clean_text(normalized.get("permit_number"))
        source_object_id = normalized.get("source_object_id")
        if not permit_number or source_object_id is None:
            return 0

        payload = {
            "permit_number": permit_number,
            "source_layer_id": COUNTY_ARCGIS_LAYER_ID,
            "source_object_id": source_object_id,
            "source_payload": json.dumps(attrs, default=str),
            "folio_raw": normalized.get("folio_raw"),
            "folio_clean": normalized.get("folio_clean"),
            "address": normalized.get("address"),
            "city": normalized.get("city"),
            "status": normalized.get("status"),
            "category": normalized.get("category"),
            "permit_type": normalized.get("permit_type"),
            "type2": normalized.get("type2"),
            "description": normalized.get("description"),
            "occupancy_type": normalized.get("occupancy_type"),
            "occupancy_category": normalized.get("occupancy_category"),
            "bedrooms": normalized.get("bedrooms"),
            "bathrooms": normalized.get("bathrooms"),
            "house_count": normalized.get("house_count"),
            "unit_count": normalized.get("unit_count"),
            "sf_living": normalized.get("sf_living"),
            "sf_cover": normalized.get("sf_cover"),
            "sf_total": normalized.get("sf_total"),
            "permit_value": normalized.get("permit_value"),
            "issue_date": normalized.get("issue_date"),
            "complete_date": normalized.get("complete_date"),
            "combined_date": normalized.get("combined_date"),
            "aca_link": normalized.get("aca_link"),
        }

        upsert_sql = text(
            """
            INSERT INTO county_permits (
                permit_number,
                source_layer_id,
                source_object_id,
                source_payload,
                folio_raw,
                folio_clean,
                address,
                city,
                status,
                category,
                permit_type,
                type2,
                description,
                occupancy_type,
                occupancy_category,
                bedrooms,
                bathrooms,
                house_count,
                unit_count,
                sf_living,
                sf_cover,
                sf_total,
                permit_value,
                issue_date,
                complete_date,
                combined_date,
                aca_link,
                source_ingested_at,
                updated_at
            ) VALUES (
                :permit_number,
                :source_layer_id,
                :source_object_id,
                CAST(:source_payload AS jsonb),
                :folio_raw,
                :folio_clean,
                :address,
                :city,
                :status,
                :category,
                :permit_type,
                :type2,
                :description,
                :occupancy_type,
                :occupancy_category,
                :bedrooms,
                :bathrooms,
                :house_count,
                :unit_count,
                :sf_living,
                :sf_cover,
                :sf_total,
                :permit_value,
                :issue_date,
                :complete_date,
                :combined_date,
                :aca_link,
                now(),
                now()
            )
            ON CONFLICT (source_layer_id, source_object_id) DO UPDATE SET
                source_payload = COALESCE(EXCLUDED.source_payload, county_permits.source_payload),
                folio_raw = COALESCE(EXCLUDED.folio_raw, county_permits.folio_raw),
                folio_clean = COALESCE(EXCLUDED.folio_clean, county_permits.folio_clean),
                address = COALESCE(EXCLUDED.address, county_permits.address),
                city = COALESCE(EXCLUDED.city, county_permits.city),
                status = COALESCE(EXCLUDED.status, county_permits.status),
                category = COALESCE(EXCLUDED.category, county_permits.category),
                permit_type = COALESCE(EXCLUDED.permit_type, county_permits.permit_type),
                type2 = COALESCE(EXCLUDED.type2, county_permits.type2),
                description = COALESCE(EXCLUDED.description, county_permits.description),
                occupancy_type = COALESCE(EXCLUDED.occupancy_type, county_permits.occupancy_type),
                occupancy_category = COALESCE(EXCLUDED.occupancy_category, county_permits.occupancy_category),
                bedrooms = COALESCE(EXCLUDED.bedrooms, county_permits.bedrooms),
                bathrooms = COALESCE(EXCLUDED.bathrooms, county_permits.bathrooms),
                house_count = COALESCE(EXCLUDED.house_count, county_permits.house_count),
                unit_count = COALESCE(EXCLUDED.unit_count, county_permits.unit_count),
                sf_living = COALESCE(EXCLUDED.sf_living, county_permits.sf_living),
                sf_cover = COALESCE(EXCLUDED.sf_cover, county_permits.sf_cover),
                sf_total = COALESCE(EXCLUDED.sf_total, county_permits.sf_total),
                permit_value = COALESCE(EXCLUDED.permit_value, county_permits.permit_value),
                issue_date = COALESCE(EXCLUDED.issue_date, county_permits.issue_date),
                complete_date = COALESCE(EXCLUDED.complete_date, county_permits.complete_date),
                combined_date = COALESCE(EXCLUDED.combined_date, county_permits.combined_date),
                aca_link = COALESCE(EXCLUDED.aca_link, county_permits.aca_link),
                source_ingested_at = now(),
                updated_at = now()
            """
        )
        return conn.execute(upsert_sql, payload).rowcount or 0

    @staticmethod
    def _backfill_county_from_hcpa(
        conn: Any,
        *,
        permit_number: str | None,
        folio_raw: str | None,
        folio_clean: str | None,
        address: str | None,
        city: str | None,
        status: str | None,
        category: str | None,
        permit_type: str | None,
        type2: str | None,
        estimated_value: float | None,
        issue_date: str | None,
        description: str | None,
        permit_url: str | None,
        source_payload: str | None,
    ) -> int:
        permit_number = _clean_text(permit_number)
        if not permit_number:
            return 0

        sql = text(
            """
            UPDATE county_permits
            SET folio_raw = COALESCE(county_permits.folio_raw, :folio_raw),
                folio_clean = COALESCE(county_permits.folio_clean, :folio_clean),
                address = COALESCE(county_permits.address, :address),
                city = COALESCE(county_permits.city, :city),
                status = COALESCE(county_permits.status, :status),
                category = COALESCE(county_permits.category, :category),
                permit_type = COALESCE(county_permits.permit_type, :permit_type),
                type2 = COALESCE(county_permits.type2, :type2),
                source_payload = COALESCE(county_permits.source_payload, CAST(:source_payload AS jsonb)),
                permit_value = COALESCE(county_permits.permit_value, :permit_value),
                issue_date = COALESCE(county_permits.issue_date, :issue_date),
                description = COALESCE(county_permits.description, :description),
                aca_link = COALESCE(county_permits.aca_link, :permit_url),
                source_ingested_at = now(),
                updated_at = now()
            WHERE permit_number = :permit_number
            """
        )
        return conn.execute(
            sql,
            {
                "permit_number": permit_number,
                "folio_raw": folio_raw,
                "folio_clean": folio_clean,
                "address": address,
                "city": city,
                "status": status,
                "category": category,
                "permit_type": permit_type,
                "type2": type2,
                "permit_value": estimated_value,
                "issue_date": issue_date,
                "description": description,
                "permit_url": permit_url,
                "source_payload": source_payload,
            },
        ).rowcount or 0

    @staticmethod
    def _upsert_county_hcpa_single_pin(
        conn: Any,
        *,
        pin: str,
        permit: dict[str, Any],
        parcel_context: dict[str, Any],
        site_address: str | None,
    ) -> int:
        permit_number = _clean_text(permit.get("permit_number"))
        source_object_id = _to_int(permit.get("source_row_id"))
        if not permit_number or source_object_id is None:
            return 0

        accela = permit.get("accela") if isinstance(permit.get("accela"), dict) else {}
        detail_extract = (
            accela.get("detail_extract") if isinstance(accela.get("detail_extract"), dict) else {}
        )
        search_extract = (
            accela.get("search_extract") if isinstance(accela.get("search_extract"), dict) else {}
        )
        permit_url = (
            _clean_text(accela.get("detail_url")) or _clean_text(permit.get("permit_url"))
        )
        folio_raw = _clean_text(parcel_context.get("folio"))
        payload = {
            "permit_number": permit_number,
            "source_layer_id": HCPA_SINGLE_PIN_LAYER_ID,
            "source_object_id": source_object_id,
            "source_payload": json.dumps(
                {
                    "pin": pin,
                    "parcel_context": parcel_context,
                    "permit": permit,
                },
                default=str,
            ),
            "folio_raw": folio_raw,
            "folio_clean": _normalize_folio(folio_raw),
            "address": site_address,
            "city": _extract_city_from_site_address(site_address),
            "status": _clean_text(detail_extract.get("status"))
            or _clean_text(search_extract.get("status")),
            "category": "HCPA_SINGLE_PIN",
            "permit_type": _clean_text(permit.get("permit_type_code")),
            "type2": _clean_text(permit.get("property_type_code")),
            "description": _clean_text(permit.get("description")),
            "permit_value": permit.get("estimated_value"),
            "issue_date": _clean_text(permit.get("issue_date")),
            "aca_link": permit_url,
        }

        sql = text(
            """
            INSERT INTO county_permits (
                permit_number,
                source_layer_id,
                source_object_id,
                source_payload,
                folio_raw,
                folio_clean,
                address,
                city,
                status,
                category,
                permit_type,
                type2,
                description,
                permit_value,
                issue_date,
                aca_link,
                source_ingested_at,
                updated_at
            ) VALUES (
                :permit_number,
                :source_layer_id,
                :source_object_id,
                CAST(:source_payload AS jsonb),
                :folio_raw,
                :folio_clean,
                :address,
                :city,
                :status,
                :category,
                :permit_type,
                :type2,
                :description,
                :permit_value,
                :issue_date,
                :aca_link,
                now(),
                now()
            )
            ON CONFLICT (source_layer_id, source_object_id) DO UPDATE SET
                permit_number = EXCLUDED.permit_number,
                source_payload = COALESCE(EXCLUDED.source_payload, county_permits.source_payload),
                folio_raw = COALESCE(EXCLUDED.folio_raw, county_permits.folio_raw),
                folio_clean = COALESCE(EXCLUDED.folio_clean, county_permits.folio_clean),
                address = COALESCE(EXCLUDED.address, county_permits.address),
                city = COALESCE(EXCLUDED.city, county_permits.city),
                status = COALESCE(EXCLUDED.status, county_permits.status),
                category = COALESCE(EXCLUDED.category, county_permits.category),
                permit_type = COALESCE(EXCLUDED.permit_type, county_permits.permit_type),
                type2 = COALESCE(EXCLUDED.type2, county_permits.type2),
                description = COALESCE(EXCLUDED.description, county_permits.description),
                permit_value = COALESCE(EXCLUDED.permit_value, county_permits.permit_value),
                issue_date = COALESCE(EXCLUDED.issue_date, county_permits.issue_date),
                aca_link = COALESCE(EXCLUDED.aca_link, county_permits.aca_link),
                source_ingested_at = now(),
                updated_at = now()
            """
        )
        return conn.execute(sql, payload).rowcount or 0

    @staticmethod
    def _upsert_tampa_from_single_pin(
        conn: Any,
        *,
        pin: str,
        permit: dict[str, Any],
        site_address: str | None,
    ) -> int:
        permit_number = _clean_text(permit.get("permit_number"))
        if not permit_number:
            return 0

        if not PgPermitSinglePinService._is_tampa_candidate(permit):
            return 0

        accela = permit.get("accela") if isinstance(permit.get("accela"), dict) else {}
        detail_extract = (
            accela.get("detail_extract") if isinstance(accela.get("detail_extract"), dict) else {}
        )
        search_extract = (
            accela.get("search_extract") if isinstance(accela.get("search_extract"), dict) else {}
        )

        status = _clean_text(detail_extract.get("status")) or _clean_text(search_extract.get("status"))
        expiration_date = _clean_text(detail_extract.get("expiration_date")) or _clean_text(
            search_extract.get("expiration_date")
        )
        estimated_cost, estimated_cost_source = PgPermitSinglePinService._best_estimated_cost(permit)
        is_open = TampaPermitService.is_open_status(status)
        needs_closeout = is_open

        address_parts = TampaPermitService.normalize_address(_clean_text(site_address))
        payload = {
            "record_number": permit_number,
            "record_date": _clean_text(permit.get("issue_date")),
            "record_type": _clean_text(permit.get("permit_type_code")) or "HCPA_PIN_PERMIT",
            "module": "Building",
            "short_notes": _clean_text(permit.get("description")),
            "project_name": _clean_text(permit.get("description")),
            "status": status,
            "address_raw": address_parts.get("address_raw"),
            "address_normalized": address_parts.get("address_normalized"),
            "city": address_parts.get("city"),
            "state": address_parts.get("state"),
            "zip_code": address_parts.get("zip_code"),
            "is_violation": False,
            "is_open": bool(is_open),
            "needs_closeout": bool(needs_closeout),
            "is_fix_record": TampaPermitService.is_fix_record(
                _clean_text(permit.get("permit_type_code")),
                _clean_text(permit.get("description")),
            ),
            "estimated_work_cost": estimated_cost,
            "estimated_cost_source": estimated_cost_source,
            "detail_url": _clean_text(accela.get("detail_url")),
            "expiration_date": expiration_date,
            "source_query_text": f"single_pin:{pin}",
            "source_export_url": _clean_text(permit.get("permit_url")),
            "source_payload": json.dumps(permit, default=str),
        }

        sql = text(
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
                record_date = COALESCE(tampa_accela_records.record_date, EXCLUDED.record_date),
                record_type = COALESCE(tampa_accela_records.record_type, EXCLUDED.record_type),
                module = COALESCE(tampa_accela_records.module, EXCLUDED.module),
                short_notes = COALESCE(tampa_accela_records.short_notes, EXCLUDED.short_notes),
                project_name = COALESCE(tampa_accela_records.project_name, EXCLUDED.project_name),
                status = COALESCE(tampa_accela_records.status, EXCLUDED.status),
                address_raw = COALESCE(tampa_accela_records.address_raw, EXCLUDED.address_raw),
                address_normalized = COALESCE(
                    tampa_accela_records.address_normalized,
                    EXCLUDED.address_normalized
                ),
                city = COALESCE(tampa_accela_records.city, EXCLUDED.city),
                state = COALESCE(tampa_accela_records.state, EXCLUDED.state),
                zip_code = COALESCE(tampa_accela_records.zip_code, EXCLUDED.zip_code),
                is_open = COALESCE(tampa_accela_records.is_open, EXCLUDED.is_open),
                needs_closeout = COALESCE(tampa_accela_records.needs_closeout, EXCLUDED.needs_closeout),
                is_fix_record = COALESCE(tampa_accela_records.is_fix_record, EXCLUDED.is_fix_record),
                estimated_work_cost = COALESCE(
                    tampa_accela_records.estimated_work_cost,
                    EXCLUDED.estimated_work_cost
                ),
                estimated_cost_source = CASE
                    WHEN tampa_accela_records.estimated_work_cost IS NULL
                     AND EXCLUDED.estimated_work_cost IS NOT NULL
                    THEN EXCLUDED.estimated_cost_source
                    ELSE tampa_accela_records.estimated_cost_source
                END,
                detail_url = COALESCE(tampa_accela_records.detail_url, EXCLUDED.detail_url),
                expiration_date = COALESCE(
                    tampa_accela_records.expiration_date,
                    EXCLUDED.expiration_date
                ),
                source_query_text = COALESCE(
                    tampa_accela_records.source_query_text,
                    EXCLUDED.source_query_text
                ),
                source_export_url = COALESCE(
                    tampa_accela_records.source_export_url,
                    EXCLUDED.source_export_url
                ),
                source_payload = COALESCE(tampa_accela_records.source_payload, EXCLUDED.source_payload),
                source_ingested_at = now(),
                updated_at = now()
            """
        )
        return conn.execute(sql, payload).rowcount or 0

    def sync_pin_to_postgres(
        self,
        pin: str,
        *,
        max_permits: int | None = None,
    ) -> dict[str, Any]:
        payload = self.fetcher.fetch_pin_permits(pin, max_permits=max_permits)
        permits = payload.get("permits") or []
        if not isinstance(permits, list):
            permits = []

        parcel_context_raw = payload.get("parcel_context")
        parcel_context: dict[str, Any] = (
            cast("dict[str, Any]", parcel_context_raw)
            if isinstance(parcel_context_raw, dict)
            else {}
        )
        site_address = _clean_text(parcel_context.get("site_address"))

        county_arcgis_upserts = 0
        county_backfill_updates = 0
        county_hcpa_upserts = 0
        tampa_upserts = 0
        plant_city_upserts = 0
        temple_terrace_upserts = 0
        arcgis_errors = 0
        accela_errors = 0
        municipal_errors = 0
        permits_with_any_write = 0

        with self._engine.begin() as conn:
            for permit in permits:
                wrote_this_permit = 0

                arcgis = permit.get("arcgis") if isinstance(permit.get("arcgis"), dict) else {}
                if arcgis.get("error"):
                    arcgis_errors += 1

                arcgis_matches = arcgis.get("matches") if isinstance(arcgis.get("matches"), list) else []
                for attrs in arcgis_matches:
                    count = self._upsert_county_arcgis(conn, attrs if isinstance(attrs, dict) else {})
                    county_arcgis_upserts += count
                    wrote_this_permit += count

                accela = permit.get("accela") if isinstance(permit.get("accela"), dict) else {}
                if accela.get("error"):
                    accela_errors += 1

                detail_url = _clean_text(accela.get("detail_url"))
                count = self._backfill_county_from_hcpa(
                    conn,
                    permit_number=_clean_text(permit.get("permit_number")),
                    folio_raw=_clean_text(parcel_context.get("folio")),
                    folio_clean=_normalize_folio(_clean_text(parcel_context.get("folio"))),
                    address=site_address,
                    city=_extract_city_from_site_address(site_address),
                    status=_clean_text(
                        (
                            accela.get("detail_extract")
                            if isinstance(accela.get("detail_extract"), dict)
                            else {}
                        ).get("status")
                    )
                    or _clean_text(
                        (
                            accela.get("search_extract")
                            if isinstance(accela.get("search_extract"), dict)
                            else {}
                        ).get("status")
                    ),
                    category="HCPA_SINGLE_PIN",
                    permit_type=_clean_text(permit.get("permit_type_code")),
                    type2=_clean_text(permit.get("property_type_code")),
                    estimated_value=permit.get("estimated_value"),
                    issue_date=_clean_text(permit.get("issue_date")),
                    description=_clean_text(permit.get("description")),
                    permit_url=detail_url or _clean_text(permit.get("permit_url")),
                    source_payload=json.dumps(
                        {
                            "pin": pin,
                            "parcel_context": parcel_context,
                            "permit": permit,
                        },
                        default=str,
                    ),
                )
                county_backfill_updates += count
                wrote_this_permit += count

                if count == 0 and not arcgis_matches and not self._is_tampa_candidate(permit):
                    count = self._upsert_county_hcpa_single_pin(
                        conn,
                        pin=pin,
                        permit=permit if isinstance(permit, dict) else {},
                        parcel_context=parcel_context,
                        site_address=site_address,
                    )
                    county_hcpa_upserts += count
                    wrote_this_permit += count

                count = self._upsert_tampa_from_single_pin(
                    conn,
                    pin=pin,
                    permit=permit if isinstance(permit, dict) else {},
                    site_address=site_address,
                )
                tampa_upserts += count
                wrote_this_permit += count

                if wrote_this_permit > 0:
                    permits_with_any_write += 1

        routed_jurisdiction = self._route_jurisdiction(site_address)
        municipal_query = _street_query_from_site_address(site_address) or site_address
        municipal_max_rows = max_permits if max_permits is not None and max_permits > 0 else 25

        plant_city_service = getattr(self, "_plant_city_service", None)
        temple_terrace_service = getattr(self, "_temple_terrace_service", None)
        municipal_error: str | None = None

        if routed_jurisdiction == "plant_city" and plant_city_service and municipal_query:
            try:
                result = plant_city_service.sync_address_to_postgres(
                    municipal_query,
                    max_rows=municipal_max_rows,
                )
                plant_city_upserts = int(result.get("written") or 0)
            except Exception as exc:
                municipal_errors += 1
                municipal_error = (
                    f"Plant City permit sync failed for pin={pin} query='{municipal_query}': {exc}"
                )
                logger.exception(
                    "Plant City permit sync failed for pin={} query='{}': {}",
                    pin,
                    municipal_query,
                    exc,
                )

        if (
            routed_jurisdiction == "temple_terrace"
            and temple_terrace_service
            and municipal_query
        ):
            try:
                result = temple_terrace_service.sync_address_to_postgres(
                    municipal_query,
                    max_rows=municipal_max_rows,
                )
                temple_terrace_upserts = int(result.get("written") or 0)
            except Exception as exc:
                municipal_errors += 1
                municipal_error = (
                    f"Temple Terrace permit sync failed for pin={pin} query='{municipal_query}': {exc}"
                )
                logger.exception(
                    "Temple Terrace permit sync failed for pin={} query='{}': {}",
                    pin,
                    municipal_query,
                    exc,
                )

        if municipal_error is not None:
            raise RuntimeError(municipal_error)

        stats = {
            "pin": pin,
            "site_address": site_address,
            "jurisdiction_route": routed_jurisdiction,
            "permit_count": len(permits),
            "county_arcgis_upserts": county_arcgis_upserts,
            "county_backfill_updates": county_backfill_updates,
            "county_hcpa_upserts": county_hcpa_upserts,
            "tampa_upserts": tampa_upserts,
            "plant_city_upserts": plant_city_upserts,
            "temple_terrace_upserts": temple_terrace_upserts,
            "total_writes": (
                county_arcgis_upserts
                + county_backfill_updates
                + county_hcpa_upserts
                + tampa_upserts
                + plant_city_upserts
                + temple_terrace_upserts
            ),
            "permits_with_any_write": permits_with_any_write,
            "arcgis_errors": arcgis_errors,
            "accela_errors": accela_errors,
            "municipal_errors": municipal_errors,
        }
        logger.info("Single-pin permit sync complete: {}", stats)
        return stats

    def sync_pins_to_postgres(
        self,
        pins: list[str],
        *,
        max_permits_per_pin: int | None = None,
        fail_on_pin_error: bool = True,
    ) -> dict[str, Any]:
        ordered_unique_pins = list(dict.fromkeys([p for p in pins if _clean_text(p)]))
        attempted = 0
        failed = 0
        total_permits = 0
        total_writes = 0
        total_arcgis_errors = 0
        total_accela_errors = 0
        total_municipal_errors = 0
        total_plant_city_upserts = 0
        total_temple_terrace_upserts = 0
        per_pin: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []

        for pin in ordered_unique_pins:
            attempted += 1
            try:
                stats = self.sync_pin_to_postgres(pin, max_permits=max_permits_per_pin)
                per_pin.append(stats)
                total_permits += int(stats.get("permit_count") or 0)
                total_writes += int(stats.get("total_writes") or 0)
                total_arcgis_errors += int(stats.get("arcgis_errors") or 0)
                total_accela_errors += int(stats.get("accela_errors") or 0)
                total_municipal_errors += int(stats.get("municipal_errors") or 0)
                total_plant_city_upserts += int(stats.get("plant_city_upserts") or 0)
                total_temple_terrace_upserts += int(stats.get("temple_terrace_upserts") or 0)
            except Exception as exc:
                failed += 1
                err = {"pin": pin, "error": str(exc)}
                errors.append(err)
                logger.exception("Single-pin permit sync failed for pin={}: {}", pin, exc)
                if fail_on_pin_error:
                    raise RuntimeError(
                        f"single-pin permit sync failed for pin={pin}: {exc}"
                    ) from exc

        summary = {
            "pins_targeted": len(ordered_unique_pins),
            "pins_attempted": attempted,
            "pins_failed": failed,
            "permits_observed_total": total_permits,
            "total_writes": total_writes,
            "arcgis_errors_total": total_arcgis_errors,
            "accela_errors_total": total_accela_errors,
            "municipal_errors_total": total_municipal_errors,
            "plant_city_upserts_total": total_plant_city_upserts,
            "temple_terrace_upserts_total": total_temple_terrace_upserts,
            "per_pin": per_pin,
            "errors": errors,
        }
        logger.info("Single-pin permit batch sync summary: {}", summary)
        return summary
