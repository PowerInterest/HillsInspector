#!/usr/bin/env python3
"""
PostgreSQL loader for Sunbiz bulk files and HCPA bulk parcel datasets.

Examples:
  uv run python sunbiz/pg_loader.py init-db
  uv run python sunbiz/pg_loader.py load-sunbiz-raw --root data/sunbiz/public/doc
  uv run python sunbiz/pg_loader.py load-sunbiz-flr --root data/sunbiz/public/doc
  uv run python sunbiz/pg_loader.py load-hcpa --parcel-file data/parquet/bulk_parcels_latest.parquet
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import html
import re
import sys
import tempfile
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any

import polars as pl
from sqlalchemy import delete
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as pg_insert

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

from sunbiz.db import get_engine
from sunbiz.db import get_session_factory
from sunbiz.db import resolve_pg_dsn
from sunbiz.models import Base
from sunbiz.models import HcpaAllSale
from sunbiz.models import HcpaBulkParcel
from sunbiz.models import HcpaLatLon
from sunbiz.models import HcpaParcelDorName
from sunbiz.models import HcpaParcelSubName
from sunbiz.models import HcpaSpecialDistrictCdd
from sunbiz.models import HcpaSpecialDistrictLd
from sunbiz.models import HcpaSpecialDistrictSd
from sunbiz.models import HcpaSpecialDistrictSd2
from sunbiz.models import HcpaSpecialDistrictTif
from sunbiz.models import HcpaSubdivision
from sunbiz.models import IngestFile
from sunbiz.models import SunbizFlrEvent
from sunbiz.models import SunbizFlrFiling
from sunbiz.models import SunbizFlrParty
from sunbiz.models import SunbizRawRecord


SUNBIZ_TEXT_SUFFIXES = {".txt", ".dat"}
DEFAULT_SUNBIZ_ROOT = Path("data/sunbiz/public/doc")
DEFAULT_BATCH_SIZE = 2000
LOADER_VERSION = "pg_loader_v1"
DEFAULT_HCPA_DOWNLOADS_DIR = Path("data/bulk_data/hcpa")
HCPA_DOWNLOADS_URL = "https://downloads.hcpafl.org/"
PG_MAX_BIND_PARAMS = 65535

HCPA_DATASET_PATTERNS = {
    "hcparcel": re.compile(r"^HCparcel_4_public_\d{2}_\d{2}_\d{4}\.zip$", re.IGNORECASE),
    "parcel": re.compile(r"^parcel_\d{2}_\d{2}_\d{4}\.zip$", re.IGNORECASE),
    "allsales": re.compile(r"^allsales_\d{2}_\d{2}_\d{4}\.zip$", re.IGNORECASE),
    "subdivisions": re.compile(r"^subdivisions_\d{2}_\d{2}_\d{4}\.zip$", re.IGNORECASE),
    "special_districts": re.compile(
        r"^special_districts_\d{2}_\d{2}_\d{4}\.zip$", re.IGNORECASE
    ),
    "latlon": re.compile(r"^LatLon_Table_\d{2}_\d{2}_\d{4}\.zip$", re.IGNORECASE),
}


try:
    from dbfread import DBF
except ImportError:
    DBF = None  # type: ignore[assignment,misc]


@dataclass(frozen=True)
class HcpaDownloadItem:
    filename: str
    event_target: str
    size_label: str
    updated_label: str


def _require_dbfread() -> None:
    if DBF is None:
        raise ImportError(
            "dbfread is required for HCPA DBF ingestion. Install with: uv add dbfread"
        )


def _as_text(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _parse_float_value(value: object | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_date_mdy(value: object | None) -> dt.date | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m%d%Y", "%Y%m%d"):
        try:
            return dt.datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _parse_hcpa_date_from_filename(filename: str) -> dt.date | None:
    match = re.search(r"(\d{2}_\d{2}_\d{4})", filename)
    if not match:
        return None
    try:
        return dt.datetime.strptime(match.group(1), "%m_%d_%Y").date()
    except ValueError:
        return None


def _extract_hidden_field(page_html: str, field_name: str) -> str:
    pattern = re.compile(
        rf'name="{re.escape(field_name)}"\s+id="{re.escape(field_name)}"\s+value="([^"]*)"',
        re.IGNORECASE,
    )
    match = pattern.search(page_html)
    if not match:
        raise RuntimeError(f"Could not find hidden field {field_name} on HCPA page.")
    return html.unescape(match.group(1))


def _fetch_hcpa_listing() -> tuple[dict[str, str], list[HcpaDownloadItem]]:
    request = urllib.request.Request(  # noqa: S310 - fixed HTTPS endpoint constant
        HCPA_DOWNLOADS_URL,
        headers={"User-Agent": "HillsInspector/1.0"},
    )
    with urllib.request.urlopen(  # noqa: S310 - fixed HTTPS endpoint constant
        request, timeout=60
    ) as resp:
        page_html = resp.read().decode("utf-8", errors="replace")

    form_fields = {
        "__VIEWSTATE": _extract_hidden_field(page_html, "__VIEWSTATE"),
        "__VIEWSTATEGENERATOR": _extract_hidden_field(page_html, "__VIEWSTATEGENERATOR"),
        "__EVENTVALIDATION": _extract_hidden_field(page_html, "__EVENTVALIDATION"),
    }

    row_pattern = re.compile(
        r"__doPostBack\(&#39;([^&#]+)&#39;,\s*&#39;&#39;\)\">.*?</i>\s*([^<]+)</a></b></td>"
        r"\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>",
        re.IGNORECASE | re.DOTALL,
    )

    items: list[HcpaDownloadItem] = []
    for match in row_pattern.finditer(page_html):
        items.append(
            HcpaDownloadItem(
                filename=html.unescape(match.group(2).strip()),
                event_target=html.unescape(match.group(1).strip()),
                size_label=html.unescape(match.group(3).strip()),
                updated_label=html.unescape(match.group(4).strip()),
            )
        )
    if not items:
        raise RuntimeError("No downloadable HCPA items discovered on listing page.")
    return form_fields, items


def _select_latest_hcpa_files(
    items: list[HcpaDownloadItem],
    datasets: list[str],
) -> dict[str, HcpaDownloadItem]:
    selected: dict[str, HcpaDownloadItem] = {}
    for dataset in datasets:
        pattern = HCPA_DATASET_PATTERNS.get(dataset)
        if not pattern:
            raise ValueError(f"Unsupported HCPA dataset: {dataset}")
        matches = [item for item in items if pattern.match(item.filename)]
        if not matches:
            continue
        matches.sort(
            key=lambda item: (
                _parse_hcpa_date_from_filename(item.filename) or dt.date.min,
                item.filename,
            ),
            reverse=True,
        )
        selected[dataset] = matches[0]
    return selected


def _download_hcpa_item(
    form_fields: dict[str, str],
    item: HcpaDownloadItem,
    output_path: Path,
) -> None:
    payload = {
        "__EVENTTARGET": item.event_target,
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATE": form_fields["__VIEWSTATE"],
        "__VIEWSTATEGENERATOR": form_fields["__VIEWSTATEGENERATOR"],
        "__EVENTVALIDATION": form_fields["__EVENTVALIDATION"],
        "ScriptManager1_TSM": "",
    }
    data = urllib.parse.urlencode(payload).encode("utf-8")
    request = urllib.request.Request(  # noqa: S310 - fixed HTTPS endpoint constant
        HCPA_DOWNLOADS_URL,
        data=data,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "HillsInspector/1.0",
            "Referer": HCPA_DOWNLOADS_URL,
        },
        method="POST",
    )
    with urllib.request.urlopen(  # noqa: S310 - fixed HTTPS endpoint constant
        request, timeout=300
    ) as resp:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("wb") as fp:
            while True:
                chunk = resp.read(1024 * 1024)
                if not chunk:
                    break
                fp.write(chunk)
    if output_path.stat().st_size == 0:
        raise RuntimeError(f"Downloaded file is empty: {output_path}")


def sync_hcpa_downloads(
    output_dir: Path,
    datasets: list[str],
    force: bool = False,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    form_fields, items = _fetch_hcpa_listing()
    selected = _select_latest_hcpa_files(items, datasets)

    results: dict[str, str] = {}
    downloaded = 0
    skipped = 0
    missing: list[str] = []

    for dataset in datasets:
        item = selected.get(dataset)
        if item is None:
            missing.append(dataset)
            continue
        target = output_dir / item.filename
        if target.exists() and not force:
            skipped += 1
            results[dataset] = target.as_posix()
            continue
        _download_hcpa_item(form_fields, item, target)
        downloaded += 1
        results[dataset] = target.as_posix()

    manifest = {
        "source_url": HCPA_DOWNLOADS_URL,
        "synced_at_utc": _utc_now().isoformat(),
        "datasets_requested": datasets,
        "datasets_downloaded": downloaded,
        "datasets_skipped": skipped,
        "datasets_missing": missing,
        "files": results,
    }
    manifest_path = output_dir / "hcpa_sync_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))

    return manifest


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def _clean_text(value: str) -> str | None:
    text = value.strip()
    return text if text else None


def _parse_int(value: str) -> int | None:
    text = value.strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _parse_date_yyyymmdd(value: str) -> dt.date | None:
    text = value.strip()
    if not text or text == "00000000":
        return None
    try:
        return dt.datetime.strptime(text, "%m%d%Y").date()
    except ValueError:
        try:
            return dt.datetime.strptime(text, "%Y%m%d").date()
        except ValueError:
            return None


def _chunked(items: list[dict], chunk_size: int):
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def _effective_batch_size(requested_batch_size: int, columns_per_row: int) -> int:
    if requested_batch_size <= 0:
        requested_batch_size = 1
    if columns_per_row <= 0:
        return requested_batch_size
    # Keep margin for dialect/bookkeeping parameters in complex statements.
    max_rows = max(1, (PG_MAX_BIND_PARAMS - 512) // columns_per_row)
    return max(1, min(requested_batch_size, max_rows))


def _compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _slice(line: str, start: int, end: int) -> str:
    if start >= len(line):
        return ""
    return line[start:end]


def _collect_input_files(
    root: Path,
    pattern: str | None,
    limit_files: int | None,
) -> list[Path]:
    if not root.exists():
        raise FileNotFoundError(f"Input root does not exist: {root}")

    regex = re.compile(pattern) if pattern else None
    files = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        suffix = path.suffix.lower()
        if suffix not in SUNBIZ_TEXT_SUFFIXES and suffix != ".zip":
            continue
        relative = path.relative_to(root).as_posix()
        if regex and not regex.search(relative):
            continue
        files.append(path)

    files.sort()
    if limit_files is not None:
        files = files[:limit_files]
    return files


def _iter_text_records(path: Path):
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as zf:
            members = sorted(
                name for name in zf.namelist() if not name.endswith("/") and "." in name
            )
            for member in members:
                lower = member.lower()
                if not lower.endswith(".txt") and not lower.endswith(".dat"):
                    continue
                with zf.open(member) as fp:
                    for line_no, raw_line in enumerate(fp, start=1):
                        text = (
                            raw_line.decode("latin-1", errors="replace")
                            .replace("\x00", "")
                            .rstrip("\r\n")
                        )
                        yield member, line_no, text
    else:
        with path.open("rb") as fp:
            for line_no, raw_line in enumerate(fp, start=1):
                text = (
                    raw_line.decode("latin-1", errors="replace")
                    .replace("\x00", "")
                    .rstrip("\r\n")
                )
                yield path.name, line_no, text


def _get_existing_ingest_file(
    session: Session,
    source_system: str,
    relative_path: str,
) -> IngestFile | None:
    stmt = select(IngestFile).where(
        IngestFile.source_system == source_system,
        IngestFile.relative_path == relative_path,
    )
    return session.execute(stmt).scalar_one_or_none()


def _upsert_ingest_file(
    session: Session,
    source_system: str,
    category: str,
    relative_path: str,
    file_sha256: str,
    file_size_bytes: int,
    file_modified_at: dt.datetime,
    status: str,
) -> int:
    stmt = (
        pg_insert(IngestFile)
        .values(
            source_system=source_system,
            category=category,
            relative_path=relative_path,
            file_sha256=file_sha256,
            file_size_bytes=file_size_bytes,
            file_modified_at=file_modified_at,
            discovered_at=_utc_now(),
            loaded_at=None,
            loader_version=LOADER_VERSION,
            status=status,
            row_count=None,
            error_message=None,
        )
        .on_conflict_do_update(
            index_elements=[IngestFile.source_system, IngestFile.relative_path],
            set_={
                "category": category,
                "file_sha256": file_sha256,
                "file_size_bytes": file_size_bytes,
                "file_modified_at": file_modified_at,
                "discovered_at": _utc_now(),
                "loader_version": LOADER_VERSION,
                "status": status,
                "row_count": None,
                "error_message": None,
            },
        )
        .returning(IngestFile.id)
    )
    return int(session.execute(stmt).scalar_one())


def _mark_ingest_file(
    session: Session,
    file_id: int,
    status: str,
    row_count: int | None = None,
    error_message: str | None = None,
) -> None:
    stmt = (
        update(IngestFile)
        .where(IngestFile.id == file_id)
        .values(
            status=status,
            row_count=row_count,
            error_message=error_message,
            loaded_at=_utc_now() if status == "loaded" else None,
        )
    )
    session.execute(stmt)


def _init_db(dsn: str) -> None:
    engine = get_engine(dsn)
    Base.metadata.create_all(bind=engine)


def load_sunbiz_raw(
    dsn: str,
    root: Path,
    pattern: str | None,
    limit_files: int | None,
    limit_lines: int | None,
    batch_size: int,
    skip_unchanged: bool,
) -> dict:
    files = _collect_input_files(root=root, pattern=pattern, limit_files=limit_files)
    session_factory = get_session_factory(dsn)

    stats = {
        "files_discovered": len(files),
        "files_loaded": 0,
        "files_skipped": 0,
        "raw_rows_loaded": 0,
    }

    with session_factory() as session:
        for path in files:
            rel = path.relative_to(root).as_posix()
            sha = _compute_sha256(path)
            st = path.stat()
            modified_at = dt.datetime.fromtimestamp(st.st_mtime, tz=dt.UTC)

            existing = _get_existing_ingest_file(session, "sunbiz", rel)
            if (
                skip_unchanged
                and existing
                and existing.status == "loaded"
                and existing.file_sha256 == sha
                and existing.file_size_bytes == st.st_size
            ):
                stats["files_skipped"] += 1
                continue

            file_id = _upsert_ingest_file(
                session=session,
                source_system="sunbiz",
                category="raw",
                relative_path=rel,
                file_sha256=sha,
                file_size_bytes=st.st_size,
                file_modified_at=modified_at,
                status="loading",
            )
            session.commit()

            try:
                session.execute(
                    delete(SunbizRawRecord).where(SunbizRawRecord.file_id == file_id)
                )
                session.commit()

                loaded_rows = 0
                batch: list[dict] = []
                for source_member, line_no, line in _iter_text_records(path):
                    if limit_lines is not None and loaded_rows >= limit_lines:
                        break
                    record = {
                        "file_id": file_id,
                        "source_member": source_member,
                        "line_number": line_no,
                        "record_type": _clean_text(_slice(line, 0, 1)),
                        "doc_number": _clean_text(_slice(line, 0, 12)),
                        "raw_line": line,
                        "loaded_at": _utc_now(),
                    }
                    batch.append(record)
                    loaded_rows += 1
                    if len(batch) >= batch_size:
                        session.execute(pg_insert(SunbizRawRecord), batch)
                        session.commit()
                        batch.clear()

                if batch:
                    session.execute(pg_insert(SunbizRawRecord), batch)
                    session.commit()

                _mark_ingest_file(session, file_id, "loaded", row_count=loaded_rows)
                session.commit()
                stats["files_loaded"] += 1
                stats["raw_rows_loaded"] += loaded_rows
            except Exception as exc:
                session.rollback()
                _mark_ingest_file(
                    session,
                    file_id,
                    "failed",
                    row_count=None,
                    error_message=str(exc)[:4000],
                )
                session.commit()
                raise

    return stats


def _parse_flrf_line(
    line: str, file_id: int, source_member: str, line_no: int
) -> dict | None:
    doc_number = _clean_text(_slice(line, 0, 12))
    if not doc_number:
        return None
    return {
        "doc_number": doc_number,
        "filing_date": _parse_date_yyyymmdd(_slice(line, 12, 20)),
        "pages": _parse_int(_slice(line, 20, 25)),
        "total_pages": _parse_int(_slice(line, 25, 30)),
        "filing_status": _clean_text(_slice(line, 30, 31)),
        "filing_type": _clean_text(_slice(line, 31, 32)),
        "assessment_date": _parse_date_yyyymmdd(_slice(line, 32, 40)),
        "cancellation_date": _parse_date_yyyymmdd(_slice(line, 40, 48)),
        "expiration_date": _parse_date_yyyymmdd(_slice(line, 48, 56)),
        "trans_utility": _clean_text(_slice(line, 56, 57)) == "Y",
        "filing_event_count": _parse_int(_slice(line, 57, 62)),
        "total_debtor_count": _parse_int(_slice(line, 62, 67)),
        "total_secured_count": _parse_int(_slice(line, 67, 72)),
        "current_debtor_count": _parse_int(_slice(line, 72, 77)),
        "current_secured_count": _parse_int(_slice(line, 77, 82)),
        "source_file_id": file_id,
        "source_member": source_member,
        "source_line_number": line_no,
        "updated_at": _utc_now(),
    }


def _parse_flr_party_line(
    line: str,
    file_id: int,
    source_member: str,
    line_no: int,
    party_role: str,
) -> dict | None:
    doc_number = _clean_text(_slice(line, 1, 13))
    if not doc_number:
        return None
    return {
        "doc_number": doc_number,
        "party_role": party_role,
        "filing_type": _clean_text(_slice(line, 0, 1)),
        "name": _clean_text(_slice(line, 13, 68)),
        "name_format": _clean_text(_slice(line, 68, 69)),
        "address1": _clean_text(_slice(line, 69, 113)),
        "address2": _clean_text(_slice(line, 113, 157)),
        "city": _clean_text(_slice(line, 157, 185)),
        "state": _clean_text(_slice(line, 185, 187)),
        "zip_code": _clean_text(_slice(line, 187, 196)),
        "country": _clean_text(_slice(line, 196, 198)),
        "sequence_number": _parse_int(_slice(line, 198, 203)),
        "relation_to_filing": _clean_text(_slice(line, 203, 204)),
        "original_party": _clean_text(_slice(line, 204, 205)),
        "filing_status": _clean_text(_slice(line, 205, 206)),
        "source_file_id": file_id,
        "source_member": source_member,
        "source_line_number": line_no,
        "loaded_at": _utc_now(),
    }


def _parse_flre_line(
    line: str,
    file_id: int,
    source_member: str,
    line_no: int,
) -> dict | None:
    event_doc_number = _clean_text(_slice(line, 0, 12))
    if not event_doc_number:
        return None
    return {
        "event_doc_number": event_doc_number,
        "event_orig_doc_number": _clean_text(_slice(line, 12, 24)),
        "event_action_count": _parse_int(_slice(line, 24, 29)),
        "event_sequence_number": _parse_int(_slice(line, 29, 34)),
        "event_pages": _parse_int(_slice(line, 34, 39)),
        "event_date": _parse_date_yyyymmdd(_slice(line, 39, 47)),
        "action_sequence_number": _parse_int(_slice(line, 47, 52)),
        "action_code": _clean_text(_slice(line, 52, 55)),
        "action_verbage": _clean_text(_slice(line, 55, 125)),
        "action_name": _clean_text(_slice(line, 125, 180)),
        "action_address1": _clean_text(_slice(line, 180, 224)),
        "action_address2": _clean_text(_slice(line, 224, 268)),
        "action_city": _clean_text(_slice(line, 268, 296)),
        "action_state": _clean_text(_slice(line, 296, 298)),
        "action_zip": _clean_text(_slice(line, 298, 307)),
        "action_country": _clean_text(_slice(line, 307, 309)),
        "action_old_name_seq": _parse_int(_slice(line, 309, 314)),
        "action_new_name_seq": _parse_int(_slice(line, 314, 319)),
        "action_name_type": _clean_text(_slice(line, 319, 320)),
        "source_file_id": file_id,
        "source_member": source_member,
        "source_line_number": line_no,
        "loaded_at": _utc_now(),
    }


def load_sunbiz_flr(
    dsn: str,
    root: Path,
    pattern: str | None,
    limit_files: int | None,
    limit_lines: int | None,
    batch_size: int,
) -> dict:
    files = _collect_input_files(root=root, pattern=pattern, limit_files=limit_files)
    session_factory = get_session_factory(dsn)

    stats = {
        "files_scanned": len(files),
        "filings_upserted": 0,
        "parties_inserted": 0,
        "events_inserted": 0,
    }

    with session_factory() as session:
        for path in files:
            rel = path.relative_to(root).as_posix()
            sha = _compute_sha256(path)
            st = path.stat()
            modified_at = dt.datetime.fromtimestamp(st.st_mtime, tz=dt.UTC)

            file_id = _upsert_ingest_file(
                session=session,
                source_system="sunbiz",
                category="flr_structured",
                relative_path=rel,
                file_sha256=sha,
                file_size_bytes=st.st_size,
                file_modified_at=modified_at,
                status="loading",
            )
            session.commit()

            filings: list[dict] = []
            parties: list[dict] = []
            events: list[dict] = []

            for source_member, line_no, line in _iter_text_records(path):
                member = Path(source_member).name.lower()
                if limit_lines is not None and (
                    len(filings) + len(parties) + len(events) >= limit_lines
                ):
                    break

                if member == "flrf.txt":
                    parsed = _parse_flrf_line(line, file_id, source_member, line_no)
                    if parsed:
                        filings.append(parsed)
                elif member == "flrd.txt":
                    parsed = _parse_flr_party_line(
                        line, file_id, source_member, line_no, party_role="debtor"
                    )
                    if parsed:
                        parties.append(parsed)
                elif member == "flrs.txt":
                    parsed = _parse_flr_party_line(
                        line, file_id, source_member, line_no, party_role="secured"
                    )
                    if parsed:
                        parties.append(parsed)
                elif member == "flre.txt":
                    parsed = _parse_flre_line(line, file_id, source_member, line_no)
                    if parsed:
                        events.append(parsed)

            if filings:
                for chunk in _chunked(filings, batch_size):
                    stmt = pg_insert(SunbizFlrFiling).values(chunk)
                    update_cols = {
                        col.name: getattr(stmt.excluded, col.name)
                        for col in SunbizFlrFiling.__table__.columns
                        if col.name != "doc_number"
                    }
                    stmt = stmt.on_conflict_do_update(
                        index_elements=[SunbizFlrFiling.doc_number],
                        set_=update_cols,
                    )
                    session.execute(stmt)
                stats["filings_upserted"] += len(filings)

            if parties:
                for chunk in _chunked(parties, batch_size):
                    stmt = pg_insert(SunbizFlrParty).values(chunk)
                    stmt = stmt.on_conflict_do_nothing(
                        constraint="uq_sunbiz_flr_parties_doc_role_seq_name"
                    )
                    session.execute(stmt)
                stats["parties_inserted"] += len(parties)

            if events:
                for chunk in _chunked(events, batch_size):
                    stmt = pg_insert(SunbizFlrEvent).values(chunk)
                    stmt = stmt.on_conflict_do_nothing(
                        constraint="uq_sunbiz_flr_events_identity"
                    )
                    session.execute(stmt)
                stats["events_inserted"] += len(events)

            _mark_ingest_file(
                session=session,
                file_id=file_id,
                status="loaded",
                row_count=len(filings) + len(parties) + len(events),
            )
            session.commit()

    return stats


def _find_zip_dbf_member(
    zip_path: Path,
    candidate_names: list[str] | None = None,
    pattern: str | None = None,
    exclude_names: list[str] | None = None,
) -> str:
    with zipfile.ZipFile(zip_path) as zf:
        members = [name for name in zf.namelist() if name.lower().endswith(".dbf")]
    if not members:
        raise FileNotFoundError(f"No DBF member found in {zip_path}")

    exclude = {name.lower() for name in (exclude_names or [])}
    filtered = [m for m in members if Path(m).name.lower() not in exclude]
    if not filtered:
        filtered = members

    if candidate_names:
        candidate_lookup = {name.lower() for name in candidate_names}
        for member in filtered:
            if Path(member).name.lower() in candidate_lookup:
                return member

    if pattern:
        regex = re.compile(pattern, re.IGNORECASE)
        for member in filtered:
            if regex.search(Path(member).name):
                return member

    return filtered[0]


def _iter_dbf_rows_from_zip(zip_path: Path, member_name: str):
    _require_dbfread()
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        with zipfile.ZipFile(zip_path) as zf:
            zf.extract(member_name, tmpdir_path)
        dbf_path = tmpdir_path / member_name
        table = DBF(str(dbf_path), encoding="latin-1", load=False)
        for line_no, row in enumerate(table, start=1):
            normalized = {str(k).lower(): v for k, v in row.items()}
            yield line_no, normalized


def _start_hcpa_ingest_file(
    session: Session,
    category: str,
    source_path: Path,
    relative_path: str | None = None,
) -> int:
    st = source_path.stat()
    return _upsert_ingest_file(
        session=session,
        source_system="hcpa",
        category=category,
        relative_path=relative_path or source_path.as_posix(),
        file_sha256=_compute_sha256(source_path),
        file_size_bytes=st.st_size,
        file_modified_at=dt.datetime.fromtimestamp(st.st_mtime, tz=dt.UTC),
        status="loading",
    )


def _clear_previous_source_rows(session: Session, model: Any, file_id: int) -> None:
    session.execute(delete(model).where(model.source_file_id == file_id))


def _parse_int_value(value: object | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        try:
            return int(float(text))
        except ValueError:
            return None


def _upsert_hcpa_dor_names(session: Session, rows: list[dict]) -> None:
    if not rows:
        return
    deduped: dict[str, dict] = {}
    for row in rows:
        key = _as_text(row.get("dor_code"))
        if key:
            deduped[key] = row
    if not deduped:
        return
    stmt = pg_insert(HcpaParcelDorName).values(list(deduped.values()))
    stmt = stmt.on_conflict_do_update(
        index_elements=[HcpaParcelDorName.dor_code],
        set_={
            "description": stmt.excluded.description,
            "source_file_id": stmt.excluded.source_file_id,
            "updated_at": stmt.excluded.updated_at,
        },
    )
    session.execute(stmt)


def _upsert_hcpa_sub_names(session: Session, rows: list[dict]) -> None:
    if not rows:
        return
    deduped: dict[str, dict] = {}
    for row in rows:
        key = _as_text(row.get("sub_code"))
        if key:
            deduped[key] = row
    if not deduped:
        return
    stmt = pg_insert(HcpaParcelSubName).values(list(deduped.values()))
    stmt = stmt.on_conflict_do_update(
        index_elements=[HcpaParcelSubName.sub_code],
        set_={
            "sub_name": stmt.excluded.sub_name,
            "plat_bk": stmt.excluded.plat_bk,
            "page": stmt.excluded.page,
            "source_file_id": stmt.excluded.source_file_id,
            "updated_at": stmt.excluded.updated_at,
        },
    )
    session.execute(stmt)


def load_hcpa_parcel_sidecars(
    dsn: str,
    parcel_zip: Path,
    batch_size: int,
    limit_rows: int | None = None,
) -> dict:
    sidecars = [
        (
            "parcel_dor_names.dbf",
            "parcel_dor_names",
            _upsert_hcpa_dor_names,
            lambda row, file_id, _: {
                "dor_code": _as_text(row.get("dorcode")),
                "description": _as_text(row.get("dordescr")),
                "source_file_id": file_id,
                "updated_at": _utc_now(),
            },
        ),
        (
            "parcel_sub_names.dbf",
            "parcel_sub_names",
            _upsert_hcpa_sub_names,
            lambda row, file_id, _: {
                "sub_code": _as_text(row.get("subcode")),
                "sub_name": _as_text(row.get("subname")),
                "plat_bk": _as_text(row.get("plat_bk")),
                "page": _as_text(row.get("page")),
                "source_file_id": file_id,
                "updated_at": _utc_now(),
            },
        ),
    ]

    stats: dict[str, int] = {"parcel_dor_names_upserted": 0, "parcel_sub_names_upserted": 0}
    session_factory = get_session_factory(dsn)

    with session_factory() as session:
        for member_name, category, upsert_fn, row_builder in sidecars:
            member = _find_zip_dbf_member(parcel_zip, candidate_names=[member_name])
            file_id = _start_hcpa_ingest_file(
                session=session,
                category=category,
                source_path=parcel_zip,
                relative_path=f"{parcel_zip.as_posix()}::{member}",
            )
            session.commit()

            rows: list[dict] = []
            count = 0
            effective_batch = batch_size
            for line_no, dbf_row in _iter_dbf_rows_from_zip(parcel_zip, member):
                if limit_rows is not None and count >= limit_rows:
                    break
                mapped = row_builder(dbf_row, file_id, line_no)
                code_key = "dor_code" if category == "parcel_dor_names" else "sub_code"
                if not mapped.get(code_key):
                    continue
                rows.append(mapped)
                if count == 0:
                    effective_batch = _effective_batch_size(batch_size, len(mapped))
                count += 1
                if len(rows) >= effective_batch:
                    upsert_fn(session, rows)
                    session.commit()
                    rows.clear()

            if rows:
                upsert_fn(session, rows)
                session.commit()

            _mark_ingest_file(
                session=session,
                file_id=file_id,
                status="loaded",
                row_count=count,
                error_message=None,
            )
            session.commit()
            stats[f"{category}_upserted"] = count

    return stats


def load_hcpa_allsales(
    dsn: str,
    allsales_zip: Path,
    batch_size: int,
    limit_rows: int | None = None,
) -> dict:
    member = _find_zip_dbf_member(allsales_zip, pattern=r"allsales.*\.dbf$")
    session_factory = get_session_factory(dsn)
    inserted = 0

    with session_factory() as session:
        file_id = _start_hcpa_ingest_file(
            session=session,
            category="allsales",
            source_path=allsales_zip,
            relative_path=f"{allsales_zip.as_posix()}::{member}",
        )
        session.commit()
        _clear_previous_source_rows(session, HcpaAllSale, file_id)
        session.commit()

        batch: list[dict] = []
        effective_batch = batch_size
        for line_no, row in _iter_dbf_rows_from_zip(allsales_zip, member):
            if limit_rows is not None and inserted >= limit_rows:
                break
            mapped = {
                "pin": _as_text(row.get("pin")),
                "folio": _as_text(row.get("folio")),
                "dor_code": _as_text(row.get("dor_code")),
                "nbhc": _as_text(row.get("nbhc")),
                "sale_date": _parse_date_mdy(row.get("s_date")),
                "vacant_improved": _as_text(row.get("vi")),
                "qualification_code": _as_text(row.get("qu")),
                "reason_code": _as_text(row.get("rea_cd")),
                "sale_amount": _parse_float_value(row.get("s_amt")),
                "sub_code": _as_text(row.get("sub")),
                "street_code": _as_text(row.get("str")),
                "sale_type": _as_text(row.get("s_type")),
                "or_book": _as_text(row.get("or_bk")),
                "or_page": _as_text(row.get("or_pg")),
                "grantor": _as_text(row.get("grantor")),
                "grantee": _as_text(row.get("grantee")),
                "doc_num": _as_text(row.get("doc_num")),
                "source_file_id": file_id,
                "source_line_number": line_no,
                "loaded_at": _utc_now(),
            }
            if inserted == 0:
                effective_batch = _effective_batch_size(batch_size, len(mapped))
            batch.append(mapped)
            inserted += 1
            if len(batch) >= effective_batch:
                session.execute(pg_insert(HcpaAllSale), batch)
                session.commit()
                batch.clear()

        if batch:
            session.execute(pg_insert(HcpaAllSale), batch)
            session.commit()

        _mark_ingest_file(
            session=session,
            file_id=file_id,
            status="loaded",
            row_count=inserted,
            error_message=None,
        )
        session.commit()

    return {"allsales_inserted": inserted}


def load_hcpa_subdivisions(
    dsn: str,
    subdivisions_zip: Path,
    batch_size: int,
    limit_rows: int | None = None,
) -> dict:
    member = _find_zip_dbf_member(subdivisions_zip, pattern=r"subdivisions.*\.dbf$")
    session_factory = get_session_factory(dsn)
    inserted = 0

    with session_factory() as session:
        file_id = _start_hcpa_ingest_file(
            session=session,
            category="subdivisions",
            source_path=subdivisions_zip,
            relative_path=f"{subdivisions_zip.as_posix()}::{member}",
        )
        session.commit()
        _clear_previous_source_rows(session, HcpaSubdivision, file_id)
        session.commit()

        batch: list[dict] = []
        effective_batch = batch_size
        for line_no, row in _iter_dbf_rows_from_zip(subdivisions_zip, member):
            if limit_rows is not None and inserted >= limit_rows:
                break
            mapped = {
                "object_id": _parse_int_value(row.get("objectid")),
                "legal1": _as_text(row.get("legal1")),
                "sub_code": _as_text(row.get("subcode")),
                "plat_bk": _as_text(row.get("plat_bk")),
                "page": _as_text(row.get("page")),
                "area": _parse_float_value(row.get("area")),
                "shape_star": _parse_float_value(row.get("shape_star")),
                "shape_stle": _parse_float_value(row.get("shape_stle")),
                "source_file_id": file_id,
                "source_line_number": line_no,
                "loaded_at": _utc_now(),
            }
            if inserted == 0:
                effective_batch = _effective_batch_size(batch_size, len(mapped))
            batch.append(mapped)
            inserted += 1
            if len(batch) >= effective_batch:
                session.execute(pg_insert(HcpaSubdivision), batch)
                session.commit()
                batch.clear()

        if batch:
            session.execute(pg_insert(HcpaSubdivision), batch)
            session.commit()

        _mark_ingest_file(
            session=session,
            file_id=file_id,
            status="loaded",
            row_count=inserted,
            error_message=None,
        )
        session.commit()

    return {"subdivisions_inserted": inserted}


def load_hcpa_special_districts(
    dsn: str,
    special_zip: Path,
    batch_size: int,
    limit_rows: int | None = None,
) -> dict:
    member_map = [
        (
            "tifs.dbf",
            "special_district_tifs",
            HcpaSpecialDistrictTif,
            lambda row, file_id, line_no: {
                "tif_code": _as_text(row.get("tifs")),
                "name": _as_text(row.get("name")),
                "area": _parse_float_value(row.get("area")),
                "perimeter": _parse_float_value(row.get("perimeter")),
                "source_file_id": file_id,
                "source_line_number": line_no,
                "loaded_at": _utc_now(),
            },
        ),
        (
            "cdds.dbf",
            "special_district_cdds",
            HcpaSpecialDistrictCdd,
            lambda row, file_id, line_no: {
                "cdd_code": _as_text(row.get("cdd")),
                "name": _as_text(row.get("name")),
                "area": _parse_float_value(row.get("area")),
                "perimeter": _parse_float_value(row.get("perimeter")),
                "source_file_id": file_id,
                "source_line_number": line_no,
                "loaded_at": _utc_now(),
            },
        ),
        (
            "sd.dbf",
            "special_district_sd",
            HcpaSpecialDistrictSd,
            lambda row, file_id, line_no: {
                "sp_name": _as_text(row.get("sp_name")),
                "ord_value": _as_text(row.get("ord_")),
                "dist_type": _as_text(row.get("dist_type")),
                "dist_num": _parse_int_value(row.get("dist_num")),
                "dist_tp": _as_text(row.get("dist_tp")),
                "area": _parse_float_value(row.get("area")),
                "perimeter": _parse_float_value(row.get("perimeter")),
                "source_file_id": file_id,
                "source_line_number": line_no,
                "loaded_at": _utc_now(),
            },
        ),
        (
            "sd2.dbf",
            "special_district_sd2",
            HcpaSpecialDistrictSd2,
            lambda row, file_id, line_no: {
                "sd_code": _as_text(row.get("sd")),
                "sp_name": _as_text(row.get("sp_name")),
                "area": _parse_float_value(row.get("area")),
                "perimeter": _parse_float_value(row.get("perimeter")),
                "source_file_id": file_id,
                "source_line_number": line_no,
                "loaded_at": _utc_now(),
            },
        ),
        (
            "lds.dbf",
            "special_district_lds",
            HcpaSpecialDistrictLd,
            lambda row, file_id, line_no: {
                "ld_code": _as_text(row.get("ld")),
                "name": _as_text(row.get("name")),
                "area": _parse_float_value(row.get("area")),
                "perimeter": _parse_float_value(row.get("perimeter")),
                "source_file_id": file_id,
                "source_line_number": line_no,
                "loaded_at": _utc_now(),
            },
        ),
    ]

    stats: dict[str, int] = {}
    session_factory = get_session_factory(dsn)

    with session_factory() as session:
        for member_name, category, model, row_builder in member_map:
            member = _find_zip_dbf_member(special_zip, candidate_names=[member_name])
            file_id = _start_hcpa_ingest_file(
                session=session,
                category=category,
                source_path=special_zip,
                relative_path=f"{special_zip.as_posix()}::{member}",
            )
            session.commit()
            _clear_previous_source_rows(session, model, file_id)
            session.commit()

            inserted = 0
            batch: list[dict] = []
            effective_batch = batch_size
            for line_no, row in _iter_dbf_rows_from_zip(special_zip, member):
                if limit_rows is not None and inserted >= limit_rows:
                    break
                mapped = row_builder(row, file_id, line_no)
                if inserted == 0:
                    effective_batch = _effective_batch_size(batch_size, len(mapped))
                batch.append(mapped)
                inserted += 1
                if len(batch) >= effective_batch:
                    session.execute(pg_insert(model), batch)
                    session.commit()
                    batch.clear()

            if batch:
                session.execute(pg_insert(model), batch)
                session.commit()

            _mark_ingest_file(
                session=session,
                file_id=file_id,
                status="loaded",
                row_count=inserted,
                error_message=None,
            )
            session.commit()
            stats[f"{category}_inserted"] = inserted

    return stats


def _find_latest_dataset_file(downloads_dir: Path, dataset: str) -> Path | None:
    pattern = HCPA_DATASET_PATTERNS.get(dataset)
    if pattern is None:
        raise ValueError(f"Unsupported dataset: {dataset}")
    candidates = [
        path
        for path in downloads_dir.glob("*.zip")
        if pattern.match(path.name)
    ]
    if not candidates:
        return None
    candidates.sort(
        key=lambda path: (_parse_hcpa_date_from_filename(path.name) or dt.date.min, path.name),
        reverse=True,
    )
    return candidates[0]


def _normalize_column_names(df: pl.DataFrame) -> pl.DataFrame:
    rename_map: dict[str, str] = {}
    for col in df.columns:
        normalized = col.strip().lower()
        if normalized != col:
            rename_map[col] = normalized
    if rename_map:
        df = df.rename(rename_map)
    return df


def _load_parcel_dataframe(parcel_file: Path) -> pl.DataFrame:
    suffix = parcel_file.suffix.lower()
    if suffix == ".parquet":
        return _normalize_column_names(pl.read_parquet(parcel_file))
    if suffix == ".csv":
        return _normalize_column_names(pl.read_csv(parcel_file, infer_schema_length=2000))
    if suffix == ".zip":
        from src.ingest.bulk_parcel_ingest import dbf_to_polars

        member = _find_zip_dbf_member(
            parcel_file,
            candidate_names=["parcel_4_public.dbf", "parcel.dbf"],
            pattern=r"parcel.*\.dbf$",
            exclude_names=["parcel_dor_names.dbf", "parcel_sub_names.dbf"],
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            with zipfile.ZipFile(parcel_file) as zf:
                zf.extract(member, tmpdir_path)
            dbf_path = tmpdir_path / member
            return _normalize_column_names(dbf_to_polars(dbf_path))
    if suffix == ".dbf":
        from src.ingest.bulk_parcel_ingest import dbf_to_polars

        return _normalize_column_names(dbf_to_polars(parcel_file))
    raise ValueError(f"Unsupported parcel file type: {parcel_file}")


def _load_latlon_dataframe(latlon_file: Path) -> pl.DataFrame:
    suffix = latlon_file.suffix.lower()
    if suffix == ".parquet":
        df = pl.read_parquet(latlon_file)
    elif suffix == ".csv":
        df = pl.read_csv(latlon_file, infer_schema_length=2000)
    elif suffix == ".zip":
        from src.ingest.bulk_parcel_ingest import load_latlon_data

        df = load_latlon_data(latlon_file)
    else:
        raise ValueError(f"Unsupported latlon file type: {latlon_file}")
    df = _normalize_column_names(df)

    folio_col = next((c for c in ("folio", "strap", "pin") if c in df.columns), None)
    lat_col = next((c for c in ("latitude", "lat") if c in df.columns), None)
    lon_col = next((c for c in ("longitude", "long", "lon") if c in df.columns), None)
    if not folio_col or not lat_col or not lon_col:
        raise ValueError(
            "LatLon file must include folio/strap and latitude/longitude columns."
        )

    return (
        df.select(
            pl.col(folio_col).cast(pl.Utf8).alias("folio"),
            pl.col(lat_col).cast(pl.Float64).alias("latitude"),
            pl.col(lon_col).cast(pl.Float64).alias("longitude"),
        )
        .filter(pl.col("folio").is_not_null())
        .with_columns(pl.col("folio").str.strip_chars())
    )


def _normalize_hcpa_parcels(df: pl.DataFrame) -> pl.DataFrame:
    aliases = {
        "owner": "owner_name",
        "site_addr": "property_address",
        "site_city": "city",
        "site_zip": "zip_code",
        "dor_code": "land_use",
        "lu_grp": "land_use_desc",
        "act": "year_built",
        "tbeds": "beds",
        "tbaths": "baths",
        "tstories": "stories",
        "tunits": "units",
        "tbldgs": "buildings",
        "heat_ar": "heated_area",
        "acreage": "lot_size",
        "asd_val": "assessed_value",
        "market_val": "market_value",
        "just": "just_value",
        "land": "land_value",
        "bldg": "building_value",
        "exf": "extra_features_value",
        "tax_val": "taxable_value",
        "s_date": "last_sale_date",
        "s_amt": "last_sale_price",
        "type": "raw_type",
        "sub": "raw_sub",
        "taxdist": "raw_taxdist",
        "muni": "raw_muni",
        "legal1": "raw_legal1",
        "legal2": "raw_legal2",
        "legal3": "raw_legal3",
        "legal4": "raw_legal4",
    }
    rename_map = {
        src: dest for src, dest in aliases.items() if src in df.columns and dest not in df.columns
    }
    if rename_map:
        df = df.rename(rename_map)

    if "folio" not in df.columns:
        if "strap" in df.columns:
            df = df.with_columns(pl.col("strap").cast(pl.Utf8).alias("folio"))
        else:
            raise ValueError("Parcel data must include folio or strap column.")

    if "last_sale_date" in df.columns:
        df = df.with_columns(
            pl.col("last_sale_date").cast(pl.Utf8).str.to_date(strict=False)
        )

    numeric_cols = [
        "year_built",
        "beds",
        "baths",
        "stories",
        "units",
        "buildings",
        "heated_area",
        "lot_size",
        "assessed_value",
        "market_value",
        "just_value",
        "land_value",
        "building_value",
        "extra_features_value",
        "taxable_value",
        "last_sale_price",
        "latitude",
        "longitude",
    ]
    for col in numeric_cols:
        if col in df.columns:
            dtype = pl.Float64
            if col in {"year_built", "units", "buildings"}:
                dtype = pl.Int64
            df = df.with_columns(pl.col(col).cast(dtype, strict=False))

    return (
        df.with_columns(pl.col("folio").cast(pl.Utf8).str.strip_chars())
        .filter(pl.col("folio").is_not_null() & (pl.col("folio") != ""))
        .unique(subset=["folio"], keep="first")
    )


def _upsert_hcpa_latlon(session: Session, rows: list[dict]) -> None:
    if not rows:
        return
    stmt = pg_insert(HcpaLatLon).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=[HcpaLatLon.folio],
        set_={
            "latitude": stmt.excluded.latitude,
            "longitude": stmt.excluded.longitude,
            "source_file_id": stmt.excluded.source_file_id,
            "updated_at": stmt.excluded.updated_at,
        },
    )
    session.execute(stmt)


def _upsert_hcpa_parcels(session: Session, rows: list[dict]) -> None:
    if not rows:
        return
    stmt = pg_insert(HcpaBulkParcel).values(rows)
    update_cols = {
        col.name: getattr(stmt.excluded, col.name)
        for col in HcpaBulkParcel.__table__.columns
        if col.name != "folio"
    }
    stmt = stmt.on_conflict_do_update(
        index_elements=[HcpaBulkParcel.folio],
        set_=update_cols,
    )
    session.execute(stmt)


def load_hcpa_bulk(
    dsn: str,
    parcel_file: Path,
    latlon_file: Path | None,
    batch_size: int,
    limit_rows: int | None = None,
) -> dict:
    parcels_df = _normalize_hcpa_parcels(_load_parcel_dataframe(parcel_file))
    if limit_rows is not None:
        parcels_df = parcels_df.head(limit_rows)
    latlon_df = _load_latlon_dataframe(latlon_file) if latlon_file else None

    if latlon_df is not None:
        parcels_df = parcels_df.join(latlon_df, on="folio", how="left", suffix="_latlon")
        if "latitude_latlon" in parcels_df.columns:
            parcels_df = parcels_df.with_columns(
                pl.coalesce("latitude", "latitude_latlon").alias("latitude"),
                pl.coalesce("longitude", "longitude_latlon").alias("longitude"),
            ).drop("latitude_latlon", "longitude_latlon")

    session_factory = get_session_factory(dsn)
    parcel_size = parcel_file.stat().st_size
    parcel_sha = _compute_sha256(parcel_file)
    parcel_modified = dt.datetime.fromtimestamp(parcel_file.stat().st_mtime, tz=dt.UTC)

    with session_factory() as session:
        parcel_file_id = _upsert_ingest_file(
            session=session,
            source_system="hcpa",
            category="bulk_parcels",
            relative_path=parcel_file.as_posix(),
            file_sha256=parcel_sha,
            file_size_bytes=parcel_size,
            file_modified_at=parcel_modified,
            status="loading",
        )
        session.commit()

        latlon_file_id = None
        if latlon_file:
            latlon_file_id = _upsert_ingest_file(
                session=session,
                source_system="hcpa",
                category="latlon",
                relative_path=latlon_file.as_posix(),
                file_sha256=_compute_sha256(latlon_file),
                file_size_bytes=latlon_file.stat().st_size,
                file_modified_at=dt.datetime.fromtimestamp(
                    latlon_file.stat().st_mtime, tz=dt.UTC
                ),
                status="loading",
            )
            session.commit()

        if latlon_df is not None and latlon_file_id is not None:
            latlon_rows = [
                {
                    "folio": row["folio"],
                    "latitude": row["latitude"],
                    "longitude": row["longitude"],
                    "source_file_id": latlon_file_id,
                    "updated_at": _utc_now(),
                }
                for row in latlon_df.iter_rows(named=True)
            ]
            effective_batch = batch_size
            if latlon_rows:
                effective_batch = _effective_batch_size(batch_size, len(latlon_rows[0]))
            for chunk in _chunked(latlon_rows, effective_batch):
                _upsert_hcpa_latlon(session, chunk)
                session.commit()
            _mark_ingest_file(
                session,
                latlon_file_id,
                status="loaded",
                row_count=len(latlon_rows),
                error_message=None,
            )
            session.commit()

        keep_cols = [
            "folio",
            "pin",
            "strap",
            "owner_name",
            "property_address",
            "city",
            "zip_code",
            "land_use",
            "land_use_desc",
            "year_built",
            "beds",
            "baths",
            "stories",
            "units",
            "buildings",
            "heated_area",
            "lot_size",
            "assessed_value",
            "market_value",
            "just_value",
            "land_value",
            "building_value",
            "extra_features_value",
            "taxable_value",
            "last_sale_date",
            "last_sale_price",
            "raw_type",
            "raw_sub",
            "raw_taxdist",
            "raw_muni",
            "raw_legal1",
            "raw_legal2",
            "raw_legal3",
            "raw_legal4",
            "latitude",
            "longitude",
        ]
        present_cols = [c for c in keep_cols if c in parcels_df.columns]
        parcels_df = parcels_df.select(present_cols)

        parcel_rows = []
        for row in parcels_df.iter_rows(named=True):
            row["source_file_id"] = parcel_file_id
            row["updated_at"] = _utc_now()
            parcel_rows.append(row)

        effective_batch = batch_size
        if parcel_rows:
            effective_batch = _effective_batch_size(batch_size, len(parcel_rows[0]))
        for chunk in _chunked(parcel_rows, effective_batch):
            _upsert_hcpa_parcels(session, chunk)
            session.commit()

        _mark_ingest_file(
            session=session,
            file_id=parcel_file_id,
            status="loaded",
            row_count=len(parcel_rows),
            error_message=None,
        )
        session.commit()

    return {
        "parcels_upserted": len(parcels_df),
        "latlon_upserted": 0 if latlon_df is None else len(latlon_df),
    }


def load_hcpa_suite(
    dsn: str,
    downloads_dir: Path,
    parcel_file: Path | None,
    allsales_file: Path | None,
    subdivisions_file: Path | None,
    special_districts_file: Path | None,
    latlon_file: Path | None,
    include_latlon: bool,
    sync_first: bool,
    force_sync: bool,
    batch_size: int,
    limit_rows: int | None = None,
) -> dict:
    if sync_first:
        requested = ["hcparcel", "allsales", "subdivisions", "special_districts"]
        if include_latlon:
            requested.append("latlon")
        sync_hcpa_downloads(downloads_dir, datasets=requested, force=force_sync)

    resolved_parcel = parcel_file
    if resolved_parcel is None:
        resolved_parcel = _find_latest_dataset_file(downloads_dir, "hcparcel")
    if resolved_parcel is None:
        resolved_parcel = _find_latest_dataset_file(downloads_dir, "parcel")

    resolved_allsales = allsales_file or _find_latest_dataset_file(downloads_dir, "allsales")
    resolved_subdivisions = subdivisions_file or _find_latest_dataset_file(
        downloads_dir, "subdivisions"
    )
    resolved_special = special_districts_file or _find_latest_dataset_file(
        downloads_dir, "special_districts"
    )
    resolved_latlon = latlon_file
    if include_latlon and resolved_latlon is None:
        resolved_latlon = _find_latest_dataset_file(downloads_dir, "latlon")

    missing = []
    if resolved_parcel is None:
        missing.append("hcparcel/parcel")
    if resolved_allsales is None:
        missing.append("allsales")
    if resolved_subdivisions is None:
        missing.append("subdivisions")
    if resolved_special is None:
        missing.append("special_districts")
    if missing:
        raise FileNotFoundError(
            f"Missing required HCPA files in {downloads_dir}: {', '.join(missing)}"
        )

    assert resolved_parcel is not None
    assert resolved_allsales is not None
    assert resolved_subdivisions is not None
    assert resolved_special is not None

    stats: dict[str, object] = {
        "files": {
            "parcel_file": resolved_parcel.as_posix(),
            "allsales_file": resolved_allsales.as_posix(),
            "subdivisions_file": resolved_subdivisions.as_posix(),
            "special_districts_file": resolved_special.as_posix(),
            "latlon_file": None if resolved_latlon is None else resolved_latlon.as_posix(),
        }
    }
    stats.update(
        load_hcpa_bulk(
            dsn=dsn,
            parcel_file=resolved_parcel,
            latlon_file=resolved_latlon if include_latlon else None,
            batch_size=batch_size,
            limit_rows=limit_rows,
        )
    )
    stats.update(
        load_hcpa_parcel_sidecars(
            dsn=dsn,
            parcel_zip=resolved_parcel,
            batch_size=batch_size,
            limit_rows=limit_rows,
        )
    )
    stats.update(
        load_hcpa_allsales(
            dsn=dsn,
            allsales_zip=resolved_allsales,
            batch_size=batch_size,
            limit_rows=limit_rows,
        )
    )
    stats.update(
        load_hcpa_subdivisions(
            dsn=dsn,
            subdivisions_zip=resolved_subdivisions,
            batch_size=batch_size,
            limit_rows=limit_rows,
        )
    )
    stats.update(
        load_hcpa_special_districts(
            dsn=dsn,
            special_zip=resolved_special,
            batch_size=batch_size,
            limit_rows=limit_rows,
        )
    )
    return stats


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="PostgreSQL loader for Sunbiz + HCPA.")
    parser.add_argument("--db-url", default=None, help="Postgres DSN override.")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Create database tables.")

    raw_cmd = sub.add_parser("load-sunbiz-raw", help="Load raw Sunbiz text lines.")
    raw_cmd.add_argument("--root", type=Path, default=DEFAULT_SUNBIZ_ROOT)
    raw_cmd.add_argument("--pattern", default=None)
    raw_cmd.add_argument("--limit-files", type=int, default=None)
    raw_cmd.add_argument("--limit-lines", type=int, default=None)
    raw_cmd.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    raw_cmd.add_argument(
        "--no-skip-unchanged",
        action="store_true",
        help="Force reload even when file hash and size are unchanged.",
    )

    flr_cmd = sub.add_parser(
        "load-sunbiz-flr", help="Parse FLR (flrf/flrd/flrs/flre) into structured tables."
    )
    flr_cmd.add_argument("--root", type=Path, default=DEFAULT_SUNBIZ_ROOT)
    flr_cmd.add_argument("--pattern", default=None)
    flr_cmd.add_argument("--limit-files", type=int, default=None)
    flr_cmd.add_argument("--limit-lines", type=int, default=None)
    flr_cmd.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)

    hcpa_cmd = sub.add_parser(
        "load-hcpa",
        help="Load HCPA bulk parcel data (+ optional LatLon data) into Postgres.",
    )
    hcpa_cmd.add_argument("--parcel-file", type=Path, required=True)
    hcpa_cmd.add_argument("--latlon-file", type=Path, default=None)
    hcpa_cmd.add_argument("--limit-rows", type=int, default=None)
    hcpa_cmd.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)

    sync_hcpa_cmd = sub.add_parser(
        "sync-hcpa",
        help="Download latest HCPA datasets from downloads.hcpafl.org.",
    )
    sync_hcpa_cmd.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_HCPA_DOWNLOADS_DIR,
    )
    sync_hcpa_cmd.add_argument(
        "--datasets",
        nargs="+",
        choices=sorted(HCPA_DATASET_PATTERNS.keys()),
        default=["hcparcel", "allsales", "subdivisions", "special_districts"],
    )
    sync_hcpa_cmd.add_argument("--force", action="store_true")

    suite_cmd = sub.add_parser(
        "load-hcpa-suite",
        help="Load HCPA weekly suite (parcel + sidecars + allsales + subdivisions + special districts).",
    )
    suite_cmd.add_argument(
        "--downloads-dir",
        type=Path,
        default=DEFAULT_HCPA_DOWNLOADS_DIR,
    )
    suite_cmd.add_argument("--parcel-file", type=Path, default=None)
    suite_cmd.add_argument("--allsales-file", type=Path, default=None)
    suite_cmd.add_argument("--subdivisions-file", type=Path, default=None)
    suite_cmd.add_argument("--special-districts-file", type=Path, default=None)
    suite_cmd.add_argument("--latlon-file", type=Path, default=None)
    suite_cmd.add_argument("--include-latlon", action="store_true")
    suite_cmd.add_argument("--sync-first", action="store_true")
    suite_cmd.add_argument("--force-sync", action="store_true")
    suite_cmd.add_argument("--limit-rows", type=int, default=None)
    suite_cmd.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)

    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    dsn = resolve_pg_dsn(args.db_url)

    if args.command == "sync-hcpa":
        stats = sync_hcpa_downloads(
            output_dir=args.output_dir,
            datasets=args.datasets,
            force=args.force,
        )
        print(stats)
        return 0

    if args.command == "init-db":
        _init_db(dsn)
        print("Initialized PostgreSQL schema.")
        return 0

    _init_db(dsn)

    if args.command == "load-sunbiz-raw":
        stats = load_sunbiz_raw(
            dsn=dsn,
            root=args.root,
            pattern=args.pattern,
            limit_files=args.limit_files,
            limit_lines=args.limit_lines,
            batch_size=args.batch_size,
            skip_unchanged=not args.no_skip_unchanged,
        )
        print(stats)
        return 0

    if args.command == "load-sunbiz-flr":
        stats = load_sunbiz_flr(
            dsn=dsn,
            root=args.root,
            pattern=args.pattern,
            limit_files=args.limit_files,
            limit_lines=args.limit_lines,
            batch_size=args.batch_size,
        )
        print(stats)
        return 0

    if args.command == "load-hcpa":
        stats = load_hcpa_bulk(
            dsn=dsn,
            parcel_file=args.parcel_file,
            latlon_file=args.latlon_file,
            batch_size=args.batch_size,
            limit_rows=args.limit_rows,
        )
        print(stats)
        return 0

    if args.command == "load-hcpa-suite":
        stats = load_hcpa_suite(
            dsn=dsn,
            downloads_dir=args.downloads_dir,
            parcel_file=args.parcel_file,
            allsales_file=args.allsales_file,
            subdivisions_file=args.subdivisions_file,
            special_districts_file=args.special_districts_file,
            latlon_file=args.latlon_file,
            include_latlon=args.include_latlon,
            sync_first=args.sync_first,
            force_sync=args.force_sync,
            batch_size=args.batch_size,
            limit_rows=args.limit_rows,
        )
        print(stats)
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
