# ruff: noqa: N999
"""
County permit bulk service for Hillsborough County (PostgreSQL-first).

Primary source:
- ArcGIS FeatureServer layer 0 (merged ISSUED + CO):
  https://services.arcgis.com/apTfC6SUmnNfnxuF/arcgis/rest/services/AccelaDashBoard_MapService20211019/FeatureServer/0

This service paginates through the full dataset (not UI-limited), normalizes
records, and persists to PostgreSQL table `county_permits`.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

import requests
from loguru import logger
from sqlalchemy import text

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from sunbiz.db import get_engine, resolve_pg_dsn
from sunbiz.models import Base

ARCGIS_SERVICE_ROOT = (
    "https://services.arcgis.com/apTfC6SUmnNfnxuF/arcgis/rest/services/"
    "AccelaDashBoard_MapService20211019/FeatureServer"
)
DEFAULT_LAYER_ID = 0
MAX_RESULT_RECORD_COUNT = 2000

ISSUED_PERMITS_XLS_URL = (
    "https://hillsborough.maps.arcgis.com/sharing/rest/content/items/"
    "3fd625c127734f0cafed1cfa648e1612/data"
)
COMPLETED_CO_XLS_URL = (
    "https://hillsborough.maps.arcgis.com/sharing/rest/content/items/"
    "1c344ad625254e4d8bf7d63992e852dc/data"
)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _epoch_ms_to_iso_date(value: Any) -> str | None:
    if value is None or value == "":
        return None
    try:
        ms = int(float(value))
        if ms <= 0:
            return None
        return datetime.fromtimestamp(ms / 1000, tz=UTC).date().isoformat()
    except (TypeError, ValueError, OSError):
        return None


def _normalize_folio(value: str | None) -> str | None:
    if not value:
        return None
    digits = "".join(ch for ch in value if ch.isdigit())
    return digits or None


class CountyPermitService:
    """Bulk pull + normalization + PostgreSQL upsert for county permits."""

    def __init__(
        self,
        layer_id: int = DEFAULT_LAYER_ID,
        *,
        page_size: int = MAX_RESULT_RECORD_COUNT,
        timeout_seconds: int = 45,
        max_retries: int = 4,
        retry_backoff_seconds: float = 1.5,
        session: requests.Session | None = None,
        pg_dsn: str | None = None,
    ) -> None:
        self.layer_id = layer_id
        self.layer_url = f"{ARCGIS_SERVICE_ROOT}/{layer_id}"
        self.page_size = min(max(1, page_size), MAX_RESULT_RECORD_COUNT)
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "HillsInspector/CountyPermit/1.0"})

        resolved_dsn = resolve_pg_dsn(pg_dsn)
        self._engine = get_engine(resolved_dsn)
        self._ensure_pg_table()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _ensure_pg_table(self) -> None:
        """Create `county_permits` table/indexes if missing."""
        table_name = "county_permits"
        if table_name not in Base.metadata.tables:
            raise RuntimeError(
                "county_permits table model not found in Base.metadata; "
                "check sunbiz.models.CountyPermit"
            )
        Base.metadata.create_all(bind=self._engine, tables=[Base.metadata.tables[table_name]])
        # Lightweight migration for existing installs that previously keyed by permit_number.
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    "ALTER TABLE county_permits "
                    "DROP CONSTRAINT IF EXISTS uq_county_permits_permit_number"
                )
            )
            conn.execute(
                text(
                    "ALTER TABLE county_permits "
                    "ALTER COLUMN source_object_id SET NOT NULL"
                )
            )
            conn.execute(
                text(
                    """
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1
                            FROM pg_constraint
                            WHERE conname = 'uq_county_permits_layer_object'
                              AND conrelid = 'county_permits'::regclass
                        ) THEN
                            ALTER TABLE county_permits
                            ADD CONSTRAINT uq_county_permits_layer_object
                            UNIQUE (source_layer_id, source_object_id);
                        END IF;
                    END
                    $$;
                    """
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_county_permits_permit_number "
                    "ON county_permits (permit_number)"
                )
            )

    # ------------------------------------------------------------------
    # ArcGIS query helpers
    # ------------------------------------------------------------------

    def _request_json(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.layer_url}{endpoint}"
        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout_seconds)
                if response.status_code in (429, 500, 502, 503, 504):
                    response.raise_for_status()
                response.raise_for_status()
                payload: dict[str, Any] = response.json()
                if "error" in payload:
                    details = payload.get("error", {}).get("details") or []
                    message = payload.get("error", {}).get("message", "ArcGIS error")
                    raise RuntimeError(f"{message} | details={details}")
                return payload
            except Exception as exc:
                last_error = exc
                if attempt == self.max_retries:
                    break
                sleep_seconds = self.retry_backoff_seconds * attempt
                logger.warning(
                    f"CountyPermit request failed (attempt {attempt}/{self.max_retries}): {exc}. "
                    f"Retrying in {sleep_seconds:.1f}s"
                )
                time.sleep(sleep_seconds)

        if last_error is None:
            raise RuntimeError("Unknown ArcGIS request failure")
        logger.error(
            "CountyPermit request failed after retries: endpoint={}, params={}, error={}",
            endpoint,
            params,
            last_error,
        )
        raise RuntimeError(f"ArcGIS request failed after retries: {last_error}") from last_error

    def get_total_count(self, where: str = "1=1") -> int:
        params = {"where": where, "returnCountOnly": "true", "f": "json"}
        payload = self._request_json("/query", params)
        return int(payload.get("count", 0))

    def get_max_object_id(self, where: str = "1=1") -> int | None:
        stats = [
            {
                "statisticType": "max",
                "onStatisticField": "OBJECTID",
                "outStatisticFieldName": "max_oid",
            }
        ]
        params = {"where": where, "outStatistics": json.dumps(stats), "f": "json"}
        payload = self._request_json("/query", params)
        features = payload.get("features") or []
        if not features:
            return None
        attrs = features[0].get("attributes") or {}
        return _to_int(attrs.get("max_oid"))

    def iter_raw_attributes(
        self,
        *,
        where: str = "1=1",
        out_fields: str = "*",
        order_by: str = "OBJECTID ASC",
        freeze_snapshot: bool = True,
        page_size: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        page_size = min(max(1, page_size or self.page_size), MAX_RESULT_RECORD_COUNT)
        scoped_where = where.strip() or "1=1"

        if freeze_snapshot:
            max_oid = self.get_max_object_id(scoped_where)
            if max_oid is not None:
                scoped_where = f"({scoped_where}) AND OBJECTID <= {max_oid}"

        total = self.get_total_count(scoped_where)
        logger.info(
            f"County permits pull started (layer={self.layer_id}, total={total}, page_size={page_size})"
        )

        fetched = 0
        offset = 0
        while fetched < total:
            params = {
                "where": scoped_where,
                "outFields": out_fields,
                "orderByFields": order_by,
                "resultOffset": offset,
                "resultRecordCount": page_size,
                "returnGeometry": "false",
                "f": "json",
            }
            payload = self._request_json("/query", params)
            features = payload.get("features") or []
            if not features:
                logger.warning(
                    f"Empty permit page returned at offset={offset}; fetched={fetched}/{total}"
                )
                break

            for feature in features:
                yield feature.get("attributes") or {}

            page_count = len(features)
            fetched += page_count
            offset += page_count

            if page_count < page_size:
                break

            if fetched % (page_size * 5) == 0 or fetched >= total:
                logger.info(f"County permits progress: {fetched}/{total}")

        logger.info(f"County permits pull finished: {fetched}/{total} rows")

    # ------------------------------------------------------------------
    # Normalization
    # ------------------------------------------------------------------

    @staticmethod
    def normalize_attributes(attrs: dict[str, Any]) -> dict[str, Any]:
        permit_number = _clean_text(attrs.get("PERMIT__"))
        folio_raw = _clean_text(attrs.get("PARCEL")) or _clean_text(attrs.get("FOLIO"))

        return {
            "source_object_id": _to_int(attrs.get("OBJECTID")),
            "permit_number": permit_number,
            "folio_raw": folio_raw,
            "folio_clean": _normalize_folio(folio_raw),
            "status": _clean_text(attrs.get("STATUS_1")) or _clean_text(attrs.get("STATUS")),
            "category": _clean_text(attrs.get("CATEGORY")),
            "permit_type": _clean_text(attrs.get("TYPE")),
            "type2": _clean_text(attrs.get("TYPE2")),
            "description": _clean_text(attrs.get("DESCRIPTION")),
            "issue_date": _epoch_ms_to_iso_date(attrs.get("ISSUED_DATE")),
            "complete_date": _epoch_ms_to_iso_date(attrs.get("COMPLETE_DATE")),
            "combined_date": _epoch_ms_to_iso_date(attrs.get("COMBINED_DATE")),
            "address": _clean_text(attrs.get("ADDRESS")),
            "city": _clean_text(attrs.get("CITY_1")) or _clean_text(attrs.get("CITY")),
            "permit_value": _to_float(attrs.get("Value")),
            "occupancy_type": _clean_text(attrs.get("OCCUPANCY_TYPE")),
            "occupancy_category": _clean_text(attrs.get("OCCUPANCY_CATEGORY")),
            "bedrooms": _to_float(attrs.get("BEDROOMS"))
            if attrs.get("BEDROOMS") is not None
            else _to_float(attrs.get("BEDROOMS_Int")),
            "bathrooms": _to_float(attrs.get("BATHROOMS"))
            if attrs.get("BATHROOMS") is not None
            else _to_float(attrs.get("BATHROOMS_Int")),
            "house_count": _to_int(attrs.get("House_Cnt"))
            if attrs.get("House_Cnt") is not None
            else _to_int(attrs.get("House_Cnt_Int")),
            "unit_count": _to_int(attrs.get("Unit_Cnt"))
            if attrs.get("Unit_Cnt") is not None
            else _to_int(attrs.get("Unit_Cnt_Int")),
            "sf_living": _to_float(attrs.get("SF_Living"))
            if attrs.get("SF_Living") is not None
            else _to_float(attrs.get("SF_Living_Int")),
            "sf_cover": _to_float(attrs.get("SF_Cover"))
            if attrs.get("SF_Cover") is not None
            else _to_float(attrs.get("SF_Cover_Int")),
            "sf_total": _to_float(attrs.get("SF_Total"))
            if attrs.get("SF_Total") is not None
            else _to_float(attrs.get("SF_Total_Int")),
            "aca_link": _clean_text(attrs.get("ACA_LINK")),
        }

    def iter_normalized(
        self,
        *,
        where: str = "1=1",
        freeze_snapshot: bool = True,
        page_size: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        for attrs in self.iter_raw_attributes(
            where=where,
            freeze_snapshot=freeze_snapshot,
            page_size=page_size,
        ):
            yield self.normalize_attributes(attrs)

    def fetch_all_normalized(
        self,
        *,
        where: str = "1=1",
        freeze_snapshot: bool = True,
        page_size: int | None = None,
    ) -> list[dict[str, Any]]:
        return list(
            self.iter_normalized(
                where=where,
                freeze_snapshot=freeze_snapshot,
                page_size=page_size,
            )
        )

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save_parquet(
        self,
        output_path: str | Path = "data/bulk_data/permits/hillsborough_county_permits.parquet",
        *,
        where: str = "1=1",
        freeze_snapshot: bool = True,
        page_size: int | None = None,
    ) -> dict[str, Any]:
        import polars as pl

        rows = self.fetch_all_normalized(
            where=where,
            freeze_snapshot=freeze_snapshot,
            page_size=page_size,
        )
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(rows).write_parquet(path)
        logger.info(f"Wrote {len(rows)} permits to parquet: {path}")
        return {"rows": len(rows), "path": str(path)}

    def sync_postgres(
        self,
        *,
        where: str = "1=1",
        clear_existing: bool = False,
        page_size: int | None = None,
        batch_size: int = 2000,
    ) -> dict[str, int]:
        """
        Upsert ArcGIS county permits into PostgreSQL `county_permits`.

        Uses `permit_number` as conflict key.
        """
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
                source_layer_id = EXCLUDED.source_layer_id,
                source_object_id = EXCLUDED.source_object_id,
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

        seen = 0
        written = 0
        skipped_missing_permit = 0
        skipped_missing_object_id = 0
        batch: list[dict[str, Any]] = []

        with self._engine.begin() as conn:
            if clear_existing:
                logger.warning("Truncating county_permits before sync")
                conn.execute(text("TRUNCATE TABLE county_permits"))

            for attrs in self.iter_raw_attributes(
                where=where,
                freeze_snapshot=True,
                page_size=page_size,
            ):
                seen += 1
                normalized = self.normalize_attributes(attrs)
                permit_number = normalized.get("permit_number")
                if not permit_number:
                    skipped_missing_permit += 1
                    continue
                source_object_id = normalized.get("source_object_id")
                if source_object_id is None:
                    skipped_missing_object_id += 1
                    continue

                normalized["permit_number"] = permit_number
                normalized["source_layer_id"] = self.layer_id
                normalized["source_object_id"] = source_object_id
                normalized["source_payload"] = json.dumps(attrs, default=str)
                batch.append(normalized)

                if len(batch) >= batch_size:
                    conn.execute(upsert_sql, batch)
                    written += len(batch)
                    batch.clear()

            if batch:
                conn.execute(upsert_sql, batch)
                written += len(batch)

        logger.info(
            f"PostgreSQL permit sync complete: seen={seen}, written={written}, "
            f"skipped_missing_permit={skipped_missing_permit}, "
            f"skipped_missing_object_id={skipped_missing_object_id}"
        )
        return {
            "seen": seen,
            "written": written,
            "skipped_missing_permit": skipped_missing_permit,
            "skipped_missing_object_id": skipped_missing_object_id,
        }

    def download_bulk_xls_files(
        self,
        output_dir: str | Path = "data/bulk_data/permits",
        *,
        include_issued: bool = True,
        include_completed: bool = True,
    ) -> list[str]:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        downloaded: list[str] = []

        targets: list[tuple[str, str]] = []
        if include_issued:
            targets.append((ISSUED_PERMITS_XLS_URL, "GIS_Dashboard_Issued.xls"))
        if include_completed:
            targets.append((COMPLETED_CO_XLS_URL, "GIS_Dashboard_Completed.xls"))

        for url, filename in targets:
            path = output_path / filename
            logger.info(f"Downloading {url} -> {path}")
            with self.session.get(url, stream=True, timeout=self.timeout_seconds) as response:
                response.raise_for_status()
                with path.open("wb") as f:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            downloaded.append(str(path))
            logger.info(f"Downloaded {path}")

        return downloaded


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Bulk pull Hillsborough county permits from ArcGIS"
    )
    parser.add_argument("--where", default="1=1", help="ArcGIS SQL where clause")
    parser.add_argument("--page-size", type=int, default=2000)
    parser.add_argument(
        "--pg-dsn",
        default=None,
        help="Override SUNBIZ_PG_DSN for PostgreSQL sync",
    )
    parser.add_argument(
        "--sync-pg",
        action="store_true",
        help="Upsert normalized permits into PostgreSQL county_permits table",
    )
    parser.add_argument(
        "--truncate-pg",
        action="store_true",
        help="Truncate county_permits table before sync",
    )
    parser.add_argument(
        "--to-parquet",
        default=None,
        help="Optional parquet export path",
    )
    parser.add_argument(
        "--download-xls",
        action="store_true",
        help="Download published ArcGIS xls files (issued + completed)",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    service = CountyPermitService(page_size=args.page_size, pg_dsn=args.pg_dsn)

    # Default action if no explicit action flags were set.
    if not args.sync_pg and not args.to_parquet and not args.download_xls:
        args.sync_pg = True

    if args.download_xls:
        files = service.download_bulk_xls_files()
        logger.info(f"Downloaded {len(files)} files")

    if args.to_parquet:
        service.save_parquet(
            output_path=args.to_parquet,
            where=args.where,
            page_size=args.page_size,
        )

    if args.sync_pg:
        stats = service.sync_postgres(
            where=args.where,
            page_size=args.page_size,
            clear_existing=args.truncate_pg,
        )
        logger.info(f"PG sync stats: {stats}")


if __name__ == "__main__":
    main()
