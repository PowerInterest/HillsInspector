# ruff: noqa: N999
"""
Tampa Accela bulk capture service (PostgreSQL-first).

What this service captures:
- Record exports from Tampa Accela Building module date windows.
- Core record data (record number/type/module/status/address/date).
- Operational flags:
  - violation indicator
  - open vs closed
  - needs closeout
  - fix/remedial-type record indicator
- Estimated work cost (best-effort from export fields; optionally enriched from detail pages).

Primary UI source:
https://aca-prod.accela.com/Tampa/Cap/CapHome.aspx?module=Building&TabName=Building
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

from bs4 import BeautifulSoup
from loguru import logger
import requests
from sqlalchemy import text

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from sunbiz.db import get_engine
from sunbiz.db import resolve_pg_dsn
from sunbiz.models import Base


CAP_HOME_URL = (
    "https://aca-prod.accela.com/Tampa/Cap/CapHome.aspx?module=Building&TabName=Building"
)
GLOBAL_SEARCH_URL = (
    "https://aca-prod.accela.com/TAMPA/Cap/GlobalSearchResults.aspx"
    "?isNewQuery=yes&QueryText={query}"
)

DEFAULT_DOWNLOAD_DIR = Path("data/bulk_data/permits/tampa_accela")
DEFAULT_MAX_EXPORT_ROWS = 1000

VIOLATION_KEYWORDS = {
    "violation",
    "code case",
    "complaint",
    "remedial",
    "enforcement",
}

FIX_KEYWORDS = {
    "fix",
    "repair",
    "revision",
    "remedial",
    "corrective",
    "correction",
    "rework",
}

CLOSED_STATUS_KEYWORDS = {
    "closed",
    "complete",
    "completed",
    "final",
    "finaled",
    "void",
    "cancel",
    "expired",
    "withdrawn",
    "denied",
}


SHOWING_TEXT_RE = re.compile(
    r"Showing\s+\d+\s*-\s*\d+\s+of\s+[^\n|]+",
    re.IGNORECASE,
)
SHOWING_TOTAL_RE = re.compile(
    r"Showing\s+\d+\s*-\s*\d+\s+of\s+(?P<total>[\d,]+)",
    re.IGNORECASE,
)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value if text_value else None


def _parse_mmddyyyy(value: str | None) -> date | None:
    value = _clean_text(value)
    if not value:
        return None
    try:
        return datetime.strptime(value, "%m/%d/%Y").date()
    except ValueError:
        return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    text_value = str(value).strip().replace(",", "")
    if not text_value:
        return None
    try:
        return float(text_value)
    except ValueError:
        return None


def _extract_showing_total(showing_text: str | None) -> int | None:
    showing_text = _clean_text(showing_text)
    if not showing_text:
        return None
    match = SHOWING_TOTAL_RE.search(showing_text)
    if not match:
        return None
    total = match.group("total").replace(",", "")
    if not total.isdigit():
        return None
    return int(total)


@dataclass
class WindowCaptureResult:
    start_date: date
    end_date: date
    csv_path: Path | None
    row_count: int
    export_url: str | None
    showing_text: str | None = None


@dataclass
class QueryCaptureResult:
    query_text: str
    csv_path: Path | None
    row_count: int
    export_url: str | None
    showing_text: str | None = None


class TampaPermitService:
    """Capture Tampa Accela record exports and sync to PostgreSQL."""

    def __init__(
        self,
        *,
        pg_dsn: str | None = None,
        headless: bool = True,
        timeout_seconds: int = 90,
        download_dir: str | Path = DEFAULT_DOWNLOAD_DIR,
        max_export_rows: int = DEFAULT_MAX_EXPORT_ROWS,
    ) -> None:
        resolved_dsn = resolve_pg_dsn(pg_dsn)
        self._engine = get_engine(resolved_dsn)
        self.headless = headless
        self.timeout_seconds = timeout_seconds
        self.download_dir = Path(download_dir)
        self.max_export_rows = max_export_rows
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self._ensure_pg_table()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _ensure_pg_table(self) -> None:
        table_name = "tampa_accela_records"
        if table_name not in Base.metadata.tables:
            raise RuntimeError(
                "tampa_accela_records model not found in Base.metadata; "
                "check sunbiz.models.TampaAccelaRecord"
            )
        Base.metadata.create_all(bind=self._engine, tables=[Base.metadata.tables[table_name]])

    # ------------------------------------------------------------------
    # Normalization helpers
    # ------------------------------------------------------------------

    @staticmethod
    def normalize_address(raw_address: str | None) -> dict[str, str | None]:
        """Normalize exported address text and parse city/state/zip."""
        address_raw = _clean_text(raw_address)
        if not address_raw:
            return {
                "address_raw": None,
                "address_normalized": None,
                "city": None,
                "state": None,
                "zip_code": None,
            }

        normalized = re.sub(
            r",\s*T,\s*(\d{5}(?:-\d{4})?)$",
            r", TAMPA, FL \1",
            address_raw,
            flags=re.IGNORECASE,
        )

        city = None
        state = None
        zip_code = None

        full_pattern = re.compile(
            r"^(?P<street>.*?),\s*(?P<city>[^,]+),\s*(?P<state>[A-Z]{2})\s*(?P<zip>\d{5}(?:-\d{4})?)$",
            re.IGNORECASE,
        )
        match = full_pattern.match(normalized)
        if match:
            city = _clean_text(match.group("city"))
            state = _clean_text(match.group("state"))
            zip_code = _clean_text(match.group("zip"))
        else:
            # Common export variant: "..., T, 33602" -> Tampa/FL.
            short_pattern = re.compile(
                r"^(?P<street>.*?),\s*T,\s*(?P<zip>\d{5}(?:-\d{4})?)$",
                re.IGNORECASE,
            )
            short_match = short_pattern.match(address_raw)
            if short_match:
                city = "TAMPA"
                state = "FL"
                zip_code = _clean_text(short_match.group("zip"))
                normalized = re.sub(
                    r",\s*T,\s*(\d{5}(?:-\d{4})?)$",
                    r", TAMPA, FL \1",
                    address_raw,
                    flags=re.IGNORECASE,
                )

        if city:
            city = city.upper()
        if state:
            state = state.upper()

        return {
            "address_raw": address_raw,
            "address_normalized": normalized,
            "city": city,
            "state": state,
            "zip_code": zip_code,
        }

    @staticmethod
    def is_violation_record(module: str | None, record_type: str | None) -> bool:
        module_text = (module or "").lower()
        record_type_text = (record_type or "").lower()
        haystack = f"{module_text} {record_type_text}"
        return any(keyword in haystack for keyword in VIOLATION_KEYWORDS)

    @staticmethod
    def is_fix_record(record_type: str | None, short_notes: str | None = None) -> bool:
        record_text = (record_type or "").lower()
        notes_text = (short_notes or "").lower()
        haystack = f"{record_text} {notes_text}"
        return any(keyword in haystack for keyword in FIX_KEYWORDS)

    @staticmethod
    def is_open_status(status: str | None) -> bool:
        status_text = (status or "").strip().lower()
        if not status_text:
            return False
        return not any(keyword in status_text for keyword in CLOSED_STATUS_KEYWORDS)

    @staticmethod
    def estimate_cost_from_export(
        project_name: str | None,
        short_notes: str | None,
        module: str | None = None,
    ) -> tuple[float | None, str | None]:
        """Best-effort cost extraction from export text fields."""
        module_text = (module or "").strip().lower()
        if module_text != "building":
            return None, None

        candidates = [project_name or "", short_notes or ""]
        money_pattern = re.compile(r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{4,})(?:\.[0-9]{1,2})?")
        context_keywords = {"value", "cost", "estimate", "job", "valuation", "contract"}

        for raw in candidates:
            raw_lower = raw.lower()
            has_context = "$" in raw or any(keyword in raw_lower for keyword in context_keywords)
            if not has_context:
                continue
            for match in money_pattern.finditer(raw):
                amount = _to_float(match.group(1))
                if amount is None:
                    continue
                # Guardrail: discard obvious IDs and unrealistic tiny values.
                if amount < 250:
                    continue
                if amount > 500_000_000:
                    continue
                return amount, "export_text"
        return None, None

    def normalize_csv_row(
        self,
        row: dict[str, Any],
        *,
        source_start_date: date | None = None,
        source_end_date: date | None = None,
        source_query_text: str | None = None,
        source_csv_name: str | None = None,
        source_export_url: str | None = None,
    ) -> dict[str, Any] | None:
        record_number = _clean_text(row.get("Record Number"))
        if not record_number:
            return None

        record_type = _clean_text(row.get("Record Type"))
        module = _clean_text(row.get("Module"))
        short_notes = _clean_text(row.get("Short Notes"))
        project_name = _clean_text(row.get("Project Name"))
        status = _clean_text(row.get("Status"))
        record_date = _parse_mmddyyyy(_clean_text(row.get("Date")))

        address_parts = self.normalize_address(_clean_text(row.get("Address")))
        estimated_work_cost, estimated_cost_source = self.estimate_cost_from_export(
            project_name, short_notes, module
        )

        is_violation = self.is_violation_record(module, record_type)
        is_open = self.is_open_status(status)
        needs_closeout = is_open and not is_violation
        is_fix_record = self.is_fix_record(record_type, short_notes)

        return {
            "record_number": record_number,
            "record_date": record_date,
            "record_type": record_type,
            "module": module,
            "short_notes": short_notes,
            "project_name": project_name,
            "status": status,
            "address_raw": address_parts["address_raw"],
            "address_normalized": address_parts["address_normalized"],
            "city": address_parts["city"],
            "state": address_parts["state"],
            "zip_code": address_parts["zip_code"],
            "is_violation": is_violation,
            "is_open": is_open,
            "needs_closeout": needs_closeout,
            "is_fix_record": is_fix_record,
            "estimated_work_cost": estimated_work_cost,
            "estimated_cost_source": estimated_cost_source,
            "source_start_date": source_start_date,
            "source_end_date": source_end_date,
            "source_query_text": source_query_text,
            "source_csv_name": source_csv_name,
            "source_export_url": source_export_url,
            "source_payload": json.dumps(row, default=str),
        }

    def parse_export_csv(
        self,
        csv_path: str | Path,
        *,
        source_start_date: date | None = None,
        source_end_date: date | None = None,
        source_query_text: str | None = None,
        source_export_url: str | None = None,
    ) -> list[dict[str, Any]]:
        path = Path(csv_path)
        rows: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                normalized = self.normalize_csv_row(
                    row,
                    source_start_date=source_start_date,
                    source_end_date=source_end_date,
                    source_query_text=source_query_text,
                    source_csv_name=path.name,
                    source_export_url=source_export_url,
                )
                if normalized:
                    rows.append(normalized)
        return rows

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def sync_csv_to_postgres(
        self,
        csv_path: str | Path,
        *,
        source_start_date: date | None = None,
        source_end_date: date | None = None,
        source_query_text: str | None = None,
        source_export_url: str | None = None,
        batch_size: int = 2000,
    ) -> dict[str, int]:
        rows = self.parse_export_csv(
            csv_path,
            source_start_date=source_start_date,
            source_end_date=source_end_date,
            source_query_text=source_query_text,
            source_export_url=source_export_url,
        )
        if not rows:
            return {"parsed": 0, "written": 0}

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
                source_start_date,
                source_end_date,
                source_query_text,
                source_csv_name,
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
                :source_start_date,
                :source_end_date,
                :source_query_text,
                :source_csv_name,
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
                address_normalized = COALESCE(EXCLUDED.address_normalized, tampa_accela_records.address_normalized),
                city = COALESCE(EXCLUDED.city, tampa_accela_records.city),
                state = COALESCE(EXCLUDED.state, tampa_accela_records.state),
                zip_code = COALESCE(EXCLUDED.zip_code, tampa_accela_records.zip_code),
                is_violation = COALESCE(EXCLUDED.is_violation, tampa_accela_records.is_violation),
                is_open = COALESCE(EXCLUDED.is_open, tampa_accela_records.is_open),
                needs_closeout = COALESCE(EXCLUDED.needs_closeout, tampa_accela_records.needs_closeout),
                is_fix_record = COALESCE(EXCLUDED.is_fix_record, tampa_accela_records.is_fix_record),
                estimated_work_cost = COALESCE(EXCLUDED.estimated_work_cost, tampa_accela_records.estimated_work_cost),
                estimated_cost_source = CASE
                    WHEN EXCLUDED.estimated_work_cost IS NOT NULL THEN EXCLUDED.estimated_cost_source
                    ELSE tampa_accela_records.estimated_cost_source
                END,
                source_start_date = COALESCE(EXCLUDED.source_start_date, tampa_accela_records.source_start_date),
                source_end_date = COALESCE(EXCLUDED.source_end_date, tampa_accela_records.source_end_date),
                source_query_text = COALESCE(EXCLUDED.source_query_text, tampa_accela_records.source_query_text),
                source_csv_name = COALESCE(EXCLUDED.source_csv_name, tampa_accela_records.source_csv_name),
                source_export_url = COALESCE(EXCLUDED.source_export_url, tampa_accela_records.source_export_url),
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

            # Safety cleanup: export-text heuristics are only valid for Building records.
            conn.execute(
                text(
                    """
                    UPDATE tampa_accela_records
                    SET estimated_work_cost = NULL,
                        estimated_cost_source = NULL,
                        updated_at = now()
                    WHERE estimated_cost_source = 'export_text'
                      AND COALESCE(module, '') <> 'Building'
                    """
                )
            )

        return {"parsed": len(rows), "written": written}

    # ------------------------------------------------------------------
    # Browser capture
    # ------------------------------------------------------------------

    def _count_csv_rows(self, csv_path: Path) -> int:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            next(reader, None)
            return sum(1 for _ in reader)

    def capture_window_export(
        self,
        start_date: date,
        end_date: date,
    ) -> WindowCaptureResult:
        """
        Capture a date-window export using Accela Building search page.

        Returns a csv path and parsed row count. If no records were found, csv_path is None.
        """
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright

        start_str = start_date.strftime("%m/%d/%Y")
        end_str = end_date.strftime("%m/%d/%Y")

        timestamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
        out_path = self.download_dir / f"records_{start_date:%Y%m%d}_{end_date:%Y%m%d}_{timestamp}.csv"

        showing_text: str | None = None
        export_url: str | None = None

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()
            try:
                page.goto(CAP_HOME_URL, wait_until="domcontentloaded", timeout=self.timeout_seconds * 1000)
                page.wait_for_timeout(1500)

                if "Error.aspx" in page.url:
                    raise RuntimeError(
                        f"Tampa Accela returned Error page for date window {start_str} - {end_str}: {page.url}"
                    )

                # General Search is default; force it for consistency.
                page.select_option("#ctl00_PlaceHolderMain_ddlSearchType", "0")
                page.wait_for_timeout(500)

                page.fill("#ctl00_PlaceHolderMain_generalSearchForm_txtGSStartDate", start_str)
                page.fill("#ctl00_PlaceHolderMain_generalSearchForm_txtGSEndDate", end_str)

                page.click("#ctl00_PlaceHolderMain_btnNewSearch")
                page.wait_for_timeout(2500)

                body_text = page.text_content("body") or ""
                if "No records found" in body_text:
                    return WindowCaptureResult(
                        start_date=start_date,
                        end_date=end_date,
                        csv_path=None,
                        row_count=0,
                        export_url=None,
                        showing_text="No records found",
                    )

                export_button = page.locator(
                    "#ctl00_PlaceHolderMain_CapView_gdvPermitList_gdvPermitListtop4btnExport"
                )
                if export_button.count() == 0:
                    return WindowCaptureResult(
                        start_date=start_date,
                        end_date=end_date,
                        csv_path=None,
                        row_count=0,
                        export_url=None,
                        showing_text=None,
                    )

                # Best-effort showing text for diagnostics.
                body_text = page.text_content("body") or ""
                showing_match = SHOWING_TEXT_RE.search(body_text)
                if showing_match:
                    showing_text = showing_match.group(0).strip()

                with page.expect_download(timeout=self.timeout_seconds * 1000) as dl_info:
                    export_button.click()
                download = dl_info.value
                download.save_as(str(out_path))

                iframe = page.locator("iframe#iframeexport")
                if iframe.count() > 0:
                    src = iframe.first.get_attribute("src")
                    if src:
                        export_url = src

            except PlaywrightTimeoutError as exc:
                raise RuntimeError(
                    f"Tampa export timed out for {start_str} - {end_str}: {exc}"
                ) from exc
            finally:
                context.close()
                browser.close()

        row_count = self._count_csv_rows(out_path)
        return WindowCaptureResult(
            start_date=start_date,
            end_date=end_date,
            csv_path=out_path,
            row_count=row_count,
            export_url=export_url,
            showing_text=showing_text,
        )

    def capture_query_export(self, query_text: str) -> QueryCaptureResult:
        """Capture a CSV export from GlobalSearchResults for a query text."""
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright

        if not _clean_text(query_text):
            raise ValueError("query_text must be non-empty")

        query = query_text.strip()
        timestamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
        safe_query = re.sub(r"[^A-Za-z0-9._-]+", "_", query)[:80]
        out_path = self.download_dir / f"records_query_{safe_query}_{timestamp}.csv"

        showing_text: str | None = None
        export_url: str | None = None

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()
            try:
                page.goto(
                    GLOBAL_SEARCH_URL.format(query=quote(query)),
                    wait_until="domcontentloaded",
                    timeout=self.timeout_seconds * 1000,
                )
                page.wait_for_timeout(1500)

                if "Error.aspx" in page.url:
                    raise RuntimeError(
                        f"Tampa Accela returned Error page for query '{query}': {page.url}"
                    )

                export_button = page.locator(
                    "#ctl00_PlaceHolderMain_CapView_gdvPermitList_gdvPermitListtop4btnExport"
                )
                if export_button.count() == 0:
                    return QueryCaptureResult(
                        query_text=query,
                        csv_path=None,
                        row_count=0,
                        export_url=None,
                        showing_text=None,
                    )

                body_text = page.text_content("body") or ""
                showing_match = SHOWING_TEXT_RE.search(body_text)
                if showing_match:
                    showing_text = showing_match.group(0).strip()

                with page.expect_download(timeout=self.timeout_seconds * 1000) as dl_info:
                    export_button.click()
                download = dl_info.value
                download.save_as(str(out_path))

                iframe = page.locator("iframe#iframeexport")
                if iframe.count() > 0:
                    src = iframe.first.get_attribute("src")
                    if src:
                        export_url = src

            except PlaywrightTimeoutError as exc:
                raise RuntimeError(f"Tampa query export timed out for '{query}': {exc}") from exc
            finally:
                context.close()
                browser.close()

        row_count = self._count_csv_rows(out_path)
        return QueryCaptureResult(
            query_text=query,
            csv_path=out_path,
            row_count=row_count,
            export_url=export_url,
            showing_text=showing_text,
        )

    def sync_date_range(
        self,
        *,
        start_date: date,
        end_date: date,
        keep_csv: bool = True,
    ) -> dict[str, int]:
        if end_date < start_date:
            raise ValueError("end_date must be >= start_date")

        queue: list[tuple[date, date]] = [(start_date, end_date)]
        total_windows = 0
        total_export_rows = 0
        total_written = 0
        total_parsed = 0
        split_windows = 0

        while queue:
            win_start, win_end = queue.pop(0)
            total_windows += 1
            logger.info(f"Capturing Tampa export window {win_start} -> {win_end}")
            result = self.capture_window_export(win_start, win_end)

            if result.row_count == 0 or result.csv_path is None:
                logger.info(f"No records returned for window {win_start} -> {win_end}")
                continue

            showing_total = _extract_showing_total(result.showing_text)

            # Export appears capped around 1000 rows. Split windows to avoid truncation.
            if result.row_count >= self.max_export_rows and win_start < win_end:
                logger.warning(
                    f"Window {win_start} -> {win_end} returned {result.row_count} rows; "
                    "splitting to avoid potential export cap truncation"
                )
                midpoint = win_start + timedelta(days=(win_end - win_start).days // 2)
                if midpoint >= win_end:
                    midpoint = win_start
                left = (win_start, midpoint)
                right = (midpoint + timedelta(days=1), win_end)
                queue.insert(0, right)
                queue.insert(0, left)
                split_windows += 1
                if not keep_csv and result.csv_path.exists():
                    result.csv_path.unlink(missing_ok=True)
                continue

            if result.row_count >= self.max_export_rows and win_start == win_end:
                # Single-day windows cannot be split further by date.
                # Fail fast when truncation is likely instead of ingesting partial data.
                if showing_total is None or showing_total > result.row_count:
                    raise RuntimeError(
                        "Potential Tampa export truncation detected for one-day window "
                        f"{win_start} ({result.row_count} rows, showing={result.showing_text!r}). "
                        "Refusing to ingest partial results."
                    )
                logger.info(
                    f"Window {win_start} reached {result.row_count} rows but showing "
                    f"total={showing_total}; ingesting."
                )

            sync_stats = self.sync_csv_to_postgres(
                result.csv_path,
                source_start_date=win_start,
                source_end_date=win_end,
                source_query_text=None,
                source_export_url=result.export_url,
            )
            total_export_rows += result.row_count
            total_parsed += sync_stats["parsed"]
            total_written += sync_stats["written"]

            logger.info(
                f"Window {win_start} -> {win_end}: csv_rows={result.row_count}, "
                f"parsed={sync_stats['parsed']}, written={sync_stats['written']}"
            )

            if not keep_csv and result.csv_path.exists():
                result.csv_path.unlink(missing_ok=True)

            time.sleep(0.5)

        return {
            "windows_processed": total_windows,
            "windows_split": split_windows,
            "csv_rows_total": total_export_rows,
            "parsed_total": total_parsed,
            "written_total": total_written,
        }

    # ------------------------------------------------------------------
    # Detail enrichment (job value, closeout signals)
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_detail_fields(page_html: str) -> dict[str, Any]:
        soup = BeautifulSoup(page_html, "html.parser")
        text_value = soup.get_text(" ", strip=True)

        status_match = re.search(
            r"Record\s+Status:\s*([A-Za-z][A-Za-z /&()\-]{0,80})",
            text_value,
        )
        expiration_match = re.search(r"Expiration\s+Date:\s*(\d{2}/\d{2}/\d{4})", text_value)
        job_value_match = re.search(
            r"Job\s+Value:\s*\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{1,})",
            text_value,
        )

        status = _clean_text(status_match.group(1) if status_match else None)
        expiration_date = _parse_mmddyyyy(
            expiration_match.group(1) if expiration_match else None
        )
        estimated_work_cost = _to_float(job_value_match.group(1) if job_value_match else None)

        return {
            "status": status,
            "expiration_date": expiration_date,
            "estimated_work_cost": estimated_work_cost,
        }

    @staticmethod
    def _extract_cap_detail_url(page_html: str, fallback_url: str | None = None) -> str | None:
        detail_pattern = re.compile(r"CapDetail\.aspx\?[^\"']+", re.IGNORECASE)
        match = detail_pattern.search(page_html)
        if match:
            href = html.unescape(match.group(0))
            if href.lower().startswith("http"):
                return href
            return f"https://aca-prod.accela.com/TAMPA/Cap/{href}"
        if fallback_url and "CapDetail.aspx" in fallback_url:
            return fallback_url
        return None

    def enrich_missing_details(self, *, limit: int | None = None) -> dict[str, int]:
        """
        Enrich missing estimated work cost / expiration from detail pages.

        This uses direct HTTP on GlobalSearchResults-by-record-number:
        exact record queries frequently resolve directly to CapDetail content.
        """
        select_sql = """
            SELECT record_number
            FROM tampa_accela_records
            WHERE estimated_work_cost IS NULL
               OR expiration_date IS NULL
            ORDER BY record_date DESC NULLS LAST, record_number
        """
        if limit and limit > 0:
            select_sql += " LIMIT :limit"

        with self._engine.begin() as conn:
            params: dict[str, Any] = {}
            if limit and limit > 0:
                params["limit"] = limit
            record_numbers = [
                row[0] for row in conn.execute(text(select_sql), params).fetchall() if row[0]
            ]

        if not record_numbers:
            return {"selected": 0, "updated": 0, "errors": 0}

        updated = 0
        errors = 0

        session = requests.Session()
        session.headers.update({"User-Agent": "HillsInspector/TampaPermit/1.0"})

        for record_number in record_numbers:
            try:
                url = GLOBAL_SEARCH_URL.format(query=quote(record_number))
                response = session.get(url, timeout=self.timeout_seconds)
                response.raise_for_status()

                parsed = self._extract_detail_fields(response.text)
                detail_url = self._extract_cap_detail_url(response.text, response.url)
                status = parsed["status"]
                expiration_date = parsed["expiration_date"]
                estimated_work_cost = parsed["estimated_work_cost"]

                # If the lookup is not a detail page, fields can be missing.
                # We still keep detail_url if resolvable and mark open-state from status.
                is_open = self.is_open_status(status)
                needs_closeout = is_open

                with self._engine.begin() as conn:
                    conn.execute(
                        text(
                            """
                            UPDATE tampa_accela_records
                            SET detail_url = COALESCE(:detail_url, detail_url),
                                status = COALESCE(:status, status),
                                expiration_date = COALESCE(:expiration_date, expiration_date),
                                estimated_work_cost = COALESCE(:estimated_work_cost, estimated_work_cost),
                                estimated_cost_source = CASE
                                    WHEN :estimated_work_cost IS NOT NULL THEN 'cap_detail'
                                    ELSE estimated_cost_source
                                END,
                                is_open = CASE
                                    WHEN :status IS NOT NULL THEN :is_open
                                    ELSE is_open
                                END,
                                needs_closeout = CASE
                                    WHEN :status IS NOT NULL THEN :needs_closeout
                                    ELSE needs_closeout
                                END,
                                updated_at = now()
                            WHERE record_number = :record_number
                            """
                        ),
                        {
                            "record_number": record_number,
                            "detail_url": detail_url,
                            "status": status,
                            "expiration_date": expiration_date,
                            "estimated_work_cost": estimated_work_cost,
                            "is_open": is_open,
                            "needs_closeout": needs_closeout,
                        },
                    )
                updated += 1
            except Exception as exc:
                errors += 1
                logger.warning(f"Detail enrichment failed for {record_number}: {exc}")

            time.sleep(0.2)

        return {"selected": len(record_numbers), "updated": updated, "errors": errors}


def _parse_iso_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Expected YYYY-MM-DD."
        ) from exc


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Capture Tampa Accela Building exports into PG")
    parser.add_argument("--start-date", type=_parse_iso_date, required=False)
    parser.add_argument("--end-date", type=_parse_iso_date, required=False)
    parser.add_argument(
        "--query-text",
        default=None,
        help="Global search query text for direct export (example: 33602)",
    )
    parser.add_argument(
        "--pg-dsn",
        default=None,
        help="Override SUNBIZ_PG_DSN for PostgreSQL sync",
    )
    parser.add_argument(
        "--download-dir",
        default=str(DEFAULT_DOWNLOAD_DIR),
        help="Folder for raw exported CSV files",
    )
    parser.add_argument(
        "--max-export-rows",
        type=int,
        default=DEFAULT_MAX_EXPORT_ROWS,
        help="Split windows when export reaches this row count",
    )
    parser.add_argument(
        "--from-csv",
        default=None,
        help="Ingest a pre-downloaded CSV file directly into PG",
    )
    parser.add_argument(
        "--enrich-details",
        action="store_true",
        help="Enrich missing estimated cost/expiration by opening record detail pages",
    )
    parser.add_argument(
        "--enrich-limit",
        type=int,
        default=None,
        help="Limit number of records enriched",
    )
    parser.add_argument(
        "--delete-csv",
        action="store_true",
        help="Delete downloaded CSV files after successful ingest",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    service = TampaPermitService(
        pg_dsn=args.pg_dsn,
        headless=True,
        download_dir=args.download_dir,
        max_export_rows=max(1, args.max_export_rows),
    )

    # Default window: yesterday -> today to avoid oversized first pull.
    today = datetime.now(tz=UTC).date()
    default_start = today - timedelta(days=1)
    start_date = args.start_date or default_start
    end_date = args.end_date or today

    if args.from_csv:
        stats = service.sync_csv_to_postgres(
            args.from_csv,
            source_start_date=start_date,
            source_end_date=end_date,
            source_query_text=None,
            source_export_url=None,
        )
        logger.info(f"CSV sync stats: {stats}")
    elif args.query_text:
        capture = service.capture_query_export(args.query_text)
        if capture.csv_path is None or capture.row_count == 0:
            logger.info(
                f"No query export rows returned for '{args.query_text}' "
                f"(showing={capture.showing_text})"
            )
        else:
            stats = service.sync_csv_to_postgres(
                capture.csv_path,
                source_start_date=start_date,
                source_end_date=end_date,
                source_query_text=args.query_text,
                source_export_url=capture.export_url,
            )
            logger.info(
                f"Query export sync stats: rows={capture.row_count}, parsed={stats['parsed']}, "
                f"written={stats['written']}, showing={capture.showing_text}"
            )
            if args.delete_csv:
                capture.csv_path.unlink(missing_ok=True)
    else:
        stats = service.sync_date_range(
            start_date=start_date,
            end_date=end_date,
            keep_csv=not args.delete_csv,
        )
        logger.info(f"Date-range sync stats: {stats}")

    if args.enrich_details:
        enrich_stats = service.enrich_missing_details(limit=args.enrich_limit)
        logger.info(f"Detail enrichment stats: {enrich_stats}")


if __name__ == "__main__":
    main()
