#!/usr/bin/env python3
"""
PostgreSQL loader for Hillsborough County Clerk of Court civil bulk data.

Downloads and loads monthly CSV files from:
    https://publicrec.hillsclerk.com/Civil/bulkdata/
and disposed-case CSV files from:
    https://publicrec.hillsclerk.com/Civil/CircuitCivilDisposedCases/
and return-of-service/garnishment weekly CSV files from:
    https://publicrec.hillsclerk.com/Civil/Circuit%20and%20County%20Civil%20with%20Return%20of%20Service%20and%20Garnishment%20Data/

Examples:
    uv run python sunbiz/pg_loader_clerk.py download-clerk-bulk
    uv run python sunbiz/pg_loader_clerk.py load-clerk-cases --root data/bulk_data/clerk_civil
    uv run python sunbiz/pg_loader_clerk.py load-clerk-events --root data/bulk_data/clerk_civil
    uv run python sunbiz/pg_loader_clerk.py load-clerk-parties --root data/bulk_data/clerk_civil
    uv run python sunbiz/pg_loader_clerk.py load-clerk-garnishment --root data/bulk_data/clerk_civil
    uv run python sunbiz/pg_loader_clerk.py load-all --root data/bulk_data/clerk_civil
    uv run python sunbiz/pg_loader_clerk.py load-all --sync-first
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import TYPE_CHECKING

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

from loguru import logger
from sqlalchemy import select
from sqlalchemy import text as sa_text
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from sunbiz.db import get_engine
from sunbiz.db import get_session_factory
from sunbiz.db import resolve_pg_dsn
from sunbiz.models import Base
from sunbiz.models import IngestFile
from src.services.models_clerk import ClerkCivilCase
from src.services.models_clerk import ClerkCivilEvent
from src.services.models_clerk import ClerkCivilParty
from src.services.models_clerk import ClerkDisposedCase
from src.services.models_clerk import ClerkGarnishmentCase
from src.services.models_clerk import ClerkNameIndex

CLERK_BULK_URL = "https://publicrec.hillsclerk.com/Civil/bulkdata/"
CLERK_DISPOSED_URL = "https://publicrec.hillsclerk.com/Civil/CircuitCivilDisposedCases/"
CLERK_GARNISHMENT_URL = (
    "https://publicrec.hillsclerk.com/Civil/"
    "Circuit%20and%20County%20Civil%20with%20Return%20of%20Service%20and%20Garnishment%20Data/"
)
DEFAULT_CLERK_DIR = Path("data/bulk_data/clerk_civil")
DEFAULT_ALPHA_DIR = Path("data/bulk_data/clerk_alpha_index")
DEFAULT_BATCH_SIZE = 2000
LOADER_VERSION = "pg_loader_clerk_v1"
PG_MAX_BIND_PARAMS = 65535

# Filename patterns for each file type
CASE_FILE_PATTERN = re.compile(
    r"^Bulk Data Case File_ (\d{2}-\d{2}-\d{4})\.csv$", re.IGNORECASE
)
EVENT_FILE_PATTERN = re.compile(
    r"^Bulk Data Event File_ (\d{2}-\d{2}-\d{4})\.csv$", re.IGNORECASE
)
PARTY_FILE_PATTERN = re.compile(
    r"^Bulk Data Party File_ (\d{2}-\d{2}-\d{4})\.csv$", re.IGNORECASE
)
DISPOSED_FILE_PATTERN = re.compile(
    r"^Odyssey-JobOutput-.*\.csv$", re.IGNORECASE
)
GARNISHMENT_FILE_PATTERN = re.compile(
    r"^ReturnOfServiceAndGarnishmentData_\d{4}-\d{2}-\d{2}\.csv$",
    re.IGNORECASE,
)
NAME_INDEX_FILE_PATTERN = re.compile(
    r"CivilNameIndex", re.IGNORECASE,
)
# UCN format: county(2) + year(4) + court_type(2) + sequence(6) + party_designator(4) + location(2)
# Example: 292019CA123456A001HC → case_number = 292019CA123456
UCN_CASE_RE = re.compile(r"^(29\d{4}[A-Z]{2}\d{6})[A-Z]\d{3}[A-Z]{2}$")


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    return text if text else None


def _parse_date_mdy(value: str | None) -> dt.date | None:
    """Parse MM/DD/YYYY date strings from the clerk CSVs."""
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    for fmt in (
        "%m/%d/%Y",
        "%Y-%m-%d",
        "%m-%d-%Y",
        "%m/%d/%y",
        "%m-%d-%y",
        "%m/%d/%Y %I:%M:%S %p",
        "%Y-%m-%d %I:%M:%S %p",
    ):
        try:
            return dt.datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    # Some clerk fields include notes after a date, e.g.
    # "2006-03-01  -  2-21-06  ...". Parse the first date token.
    date_token = re.search(
        r"\b\d{4}-\d{2}-\d{2}\b|\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b",
        text,
    )
    if date_token:
        token = date_token.group(0)
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%m-%d-%y"):
            try:
                return dt.datetime.strptime(token, fmt).date()
            except ValueError:
                continue

    return None


def _parse_date_from_filename(filename: str) -> dt.date | None:
    """Extract date from known clerk filename patterns."""
    mdy_match = re.search(r"(\d{2}-\d{2}-\d{4})", filename)
    if mdy_match:
        try:
            return dt.datetime.strptime(mdy_match.group(1), "%m-%d-%Y").date()
        except ValueError:
            pass

    ymd_match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if ymd_match:
        try:
            return dt.datetime.strptime(ymd_match.group(1), "%Y-%m-%d").date()
        except ValueError:
            pass

    return None


def _compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _effective_batch_size(requested: int, columns_per_row: int) -> int:
    if requested <= 0:
        requested = 1
    if columns_per_row <= 0:
        return requested
    max_rows = max(1, (PG_MAX_BIND_PARAMS - 512) // columns_per_row)
    return max(1, min(requested, max_rows))


def _chunked(items: list[dict], chunk_size: int):
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def _read_csv_with_bom(path: Path) -> list[dict]:
    """Read a CSV file handling UTF-8 BOM encoding."""
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _iter_csv_with_bom(path: Path):
    """Iterate CSV rows lazily, handling UTF-8 BOM encoding."""
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        yield from reader


def _is_foreclosure(case_type: str | None) -> bool:
    """Determine if a case is a foreclosure based on case_type."""
    if not case_type:
        return False
    return "foreclos" in case_type.lower()


def _iter_pipe_delimited(path: Path):
    """Iterate rows from a pipe-delimited TXT file with header."""
    csv.field_size_limit(sys.maxsize)  # some rows have extremely long AKA fields
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="|")
        yield from reader


def _extract_case_number_from_ucn(ucn: str | None) -> str | None:
    """Extract case number (29YYYYCCNNNNNN) from Uniform Case Number."""
    if not ucn:
        return None
    ucn = ucn.strip()
    m = UCN_CASE_RE.match(ucn)
    return m.group(1) if m else None


def _parse_party_name(full_name: str | None) -> tuple[str | None, str | None, str | None]:
    """
    Attempt to parse 'Last, First Middle' or organizational names into components.

    Returns (first_name, middle_name, last_name).
    For organizational names (no comma), returns (None, None, full_name).
    """
    if not full_name:
        return None, None, None
    name = full_name.strip()
    if not name:
        return None, None, None

    # If no comma, treat as organizational/single name
    if "," not in name:
        return None, None, name

    parts = name.split(",", 1)
    last_name = parts[0].strip() or None
    remainder = parts[1].strip() if len(parts) > 1 else ""

    if not remainder:
        return None, None, last_name

    first_parts = remainder.split(None, 1)
    first_name = first_parts[0].strip() if first_parts else None
    middle_name = first_parts[1].strip() if len(first_parts) > 1 else None

    return first_name, middle_name, last_name


# ---------------------------------------------------------------------------
# Ingest file tracking (same pattern as pg_loader.py)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def _fetch_csv_listing(base_url: str) -> list[str]:
    """Fetch a clerk listing page and extract CSV filenames."""
    request = urllib.request.Request(  # noqa: S310 - fixed HTTPS endpoint constant
        base_url,
        headers={"User-Agent": "HillsInspector/1.0"},
    )
    with urllib.request.urlopen(request, timeout=60) as resp:  # noqa: S310 - fixed HTTPS endpoint
        page_html = resp.read().decode("utf-8", errors="replace")

    # Extract CSV links from the page
    csv_pattern = re.compile(r'href="([^"]*\.csv)"', re.IGNORECASE)
    filenames: list[str] = []
    for match in csv_pattern.finditer(page_html):
        href = match.group(1)
        # Extract just the filename from path or URL
        name = href.rsplit("/", 1)[-1]
        # URL-decode the filename
        name = urllib.parse.unquote(name)
        filenames.append(name)

    if not filenames:
        # Fallback: look for anchor text containing .csv
        text_pattern = re.compile(r">([^<]*\.csv)<", re.IGNORECASE)
        for match in text_pattern.finditer(page_html):
            filenames.append(match.group(1).strip())

    return filenames


def download_clerk_bulk(
    output_dir: Path,
    force: bool = False,
    file_types: list[str] | None = None,
) -> dict:
    """
    Download latest clerk civil bulk CSV files from the clerk website.

    Args:
        output_dir: Directory to save CSV files.
        force: Re-download even if file exists locally.
        file_types: Filter to specific types: 'case', 'event', 'party',
            'disposed', 'garnishment'. None = all.

    Returns:
        Dict with download stats.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    patterns = {
        "case": CASE_FILE_PATTERN,
        "event": EVENT_FILE_PATTERN,
        "party": PARTY_FILE_PATTERN,
        "disposed": DISPOSED_FILE_PATTERN,
        "garnishment": GARNISHMENT_FILE_PATTERN,
    }

    # Filter to requested types
    if file_types:
        active_patterns = {k: v for k, v in patterns.items() if k in file_types}
    else:
        active_patterns = patterns

    listing_by_category: dict[str, tuple[str, list[str]]] = {
        "case": (CLERK_BULK_URL, _fetch_csv_listing(CLERK_BULK_URL)),
        "event": (CLERK_BULK_URL, _fetch_csv_listing(CLERK_BULK_URL)),
        "party": (CLERK_BULK_URL, _fetch_csv_listing(CLERK_BULK_URL)),
        "disposed": (CLERK_DISPOSED_URL, _fetch_csv_listing(CLERK_DISPOSED_URL)),
        "garnishment": (
            CLERK_GARNISHMENT_URL,
            _fetch_csv_listing(CLERK_GARNISHMENT_URL),
        ),
    }

    downloaded = 0
    skipped = 0
    errors = 0
    files_found = 0
    seen_downloads: set[tuple[str, str]] = set()

    for category, pattern in active_patterns.items():
        base_url, filenames = listing_by_category[category]
        logger.info(
            "Found {} CSV files on {} listing page",
            len(filenames),
            base_url,
        )

        for filename in filenames:
            if not pattern.match(filename):
                continue

            files_found += 1
            dedup_key = (base_url, filename)
            if dedup_key in seen_downloads:
                continue
            seen_downloads.add(dedup_key)

            target = output_dir / filename
            if target.exists() and not force:
                logger.debug(f"Skipping (exists): {filename}")
                skipped += 1
                continue

            encoded = urllib.parse.quote(filename)
            url = f"{base_url}{encoded}"
            logger.info(f"Downloading {filename} ...")

            try:
                request = urllib.request.Request(  # noqa: S310 - clerk HTTPS endpoint
                    url,
                    headers={"User-Agent": "HillsInspector/1.0"},
                )
                with urllib.request.urlopen(request, timeout=120) as resp:  # noqa: S310 - clerk endpoint
                    content = resp.read()
                target.write_bytes(content)
                if target.stat().st_size == 0:
                    logger.warning(f"Downloaded file is empty: {filename}")
                    errors += 1
                    continue
                downloaded += 1
                logger.info(f"Downloaded: {filename} ({len(content):,} bytes)")
            except Exception as exc:
                logger.error(f"Failed to download {filename}: {exc}")
                errors += 1

    return {
        "files_found": files_found,
        "downloaded": downloaded,
        "skipped": skipped,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Case loader
# ---------------------------------------------------------------------------


def load_clerk_cases(
    dsn: str,
    root: Path,
    batch_size: int = DEFAULT_BATCH_SIZE,
    skip_unchanged: bool = True,
    limit_files: int | None = None,
) -> dict:
    """
    Load clerk civil case CSV files into clerk_civil_cases table.

    CSV columns: CaseNbr, Style, CaseType, Division, DtFile, Judge,
                 CauseOfAction, CauseDescription, CaseStatus,
                 JudgmentCode, JudgmentDescription, JudgmentDate
    """
    files = sorted(root.glob("Bulk Data Case File_*.csv"))
    if limit_files:
        files = files[:limit_files]

    if not files:
        logger.warning(f"No case files found in {root}")
        return {"files_found": 0, "files_loaded": 0, "rows_upserted": 0}

    logger.info(f"Found {len(files)} case files in {root}")
    session_factory = get_session_factory(dsn)
    stats = {"files_found": len(files), "files_loaded": 0, "files_skipped": 0, "rows_upserted": 0}

    with session_factory() as session:
        for path in files:
            rel = path.relative_to(root).as_posix() if root in path.parents else path.name
            sha = _compute_sha256(path)
            st = path.stat()
            modified_at = dt.datetime.fromtimestamp(st.st_mtime, tz=dt.UTC)

            existing = _get_existing_ingest_file(session, "clerk_civil", rel)
            if (
                skip_unchanged
                and existing
                and existing.status == "loaded"
                and existing.file_sha256 == sha
            ):
                logger.debug(f"Skipping unchanged: {path.name}")
                stats["files_skipped"] += 1
                continue

            file_id = _upsert_ingest_file(
                session=session,
                source_system="clerk_civil",
                category="cases",
                relative_path=rel,
                file_sha256=sha,
                file_size_bytes=st.st_size,
                file_modified_at=modified_at,
                status="loading",
            )
            session.commit()

            try:
                rows: list[dict] = []
                row_count = 0
                eff_batch = batch_size

                for csv_row in _iter_csv_with_bom(path):
                    case_number = _clean_text(csv_row.get("CaseNbr"))
                    if not case_number:
                        continue

                    case_type_val = _clean_text(csv_row.get("CaseType"))
                    mapped = {
                        "case_number": case_number,
                        "style": _clean_text(csv_row.get("Style")),
                        "case_type": case_type_val,
                        "division": _clean_text(csv_row.get("Division")),
                        "filing_date": _parse_date_mdy(csv_row.get("DtFile")),
                        "judge": _clean_text(csv_row.get("Judge")),
                        "cause_of_action": _clean_text(csv_row.get("CauseOfAction")),
                        "cause_description": _clean_text(csv_row.get("CauseDescription")),
                        "case_status": _clean_text(csv_row.get("CaseStatus")),
                        "judgment_code": _clean_text(csv_row.get("JudgmentCode")),
                        "judgment_description": _clean_text(csv_row.get("JudgmentDescription")),
                        "judgment_date": _parse_date_mdy(csv_row.get("JudgmentDate")),
                        "is_foreclosure": _is_foreclosure(case_type_val),
                        "source_file": path.name,
                        "loaded_at": _utc_now(),
                    }

                    if row_count == 0:
                        eff_batch = _effective_batch_size(batch_size, len(mapped))

                    rows.append(mapped)
                    row_count += 1

                    if len(rows) >= eff_batch:
                        _upsert_cases_batch(session, rows)
                        session.commit()
                        rows.clear()

                if rows:
                    _upsert_cases_batch(session, rows)
                    session.commit()

                _mark_ingest_file(session, file_id, "loaded", row_count=row_count)
                session.commit()
                stats["files_loaded"] += 1
                stats["rows_upserted"] += row_count
                logger.info(f"Loaded {row_count:,} cases from {path.name}")

            except Exception as exc:
                session.rollback()
                _mark_ingest_file(
                    session, file_id, "failed", error_message=str(exc)[:4000]
                )
                session.commit()
                logger.error(f"Failed to load {path.name}: {exc}")
                raise

    return stats


def _dedup_by_key(rows: list[dict], key: str) -> list[dict]:
    """Deduplicate rows within a batch by key, keeping last occurrence."""
    seen: dict[str | None, dict] = {}
    for row in rows:
        seen[row.get(key)] = row
    return list(seen.values())


def _upsert_cases_batch(session: Session, rows: list[dict]) -> None:
    if not rows:
        return
    rows = _dedup_by_key(rows, "case_number")
    stmt = pg_insert(ClerkCivilCase).values(rows)
    update_cols = {
        col.name: getattr(stmt.excluded, col.name)
        for col in ClerkCivilCase.__table__.columns
        if col.name != "case_number"
    }
    stmt = stmt.on_conflict_do_update(
        index_elements=[ClerkCivilCase.case_number],
        set_=update_cols,
    )
    session.execute(stmt)


# ---------------------------------------------------------------------------
# Event loader
# ---------------------------------------------------------------------------


def load_clerk_events(
    dsn: str,
    root: Path,
    batch_size: int = DEFAULT_BATCH_SIZE,
    skip_unchanged: bool = True,
    limit_files: int | None = None,
) -> dict:
    """
    Load clerk civil event CSV files into clerk_civil_events table.

    CSV columns: CaseNbr, EventCode, Event, EventDate1, NameFirst, NameMid, NameLast
    """
    files = sorted(root.glob("Bulk Data Event File_*.csv"))
    if limit_files:
        files = files[:limit_files]

    if not files:
        logger.warning(f"No event files found in {root}")
        return {"files_found": 0, "files_loaded": 0, "rows_inserted": 0}

    logger.info(f"Found {len(files)} event files in {root}")
    session_factory = get_session_factory(dsn)
    stats = {"files_found": len(files), "files_loaded": 0, "files_skipped": 0, "rows_inserted": 0}

    with session_factory() as session:
        for path in files:
            rel = path.relative_to(root).as_posix() if root in path.parents else path.name
            sha = _compute_sha256(path)
            st = path.stat()
            modified_at = dt.datetime.fromtimestamp(st.st_mtime, tz=dt.UTC)

            existing = _get_existing_ingest_file(session, "clerk_civil", rel)
            if (
                skip_unchanged
                and existing
                and existing.status == "loaded"
                and existing.file_sha256 == sha
            ):
                logger.debug(f"Skipping unchanged: {path.name}")
                stats["files_skipped"] += 1
                continue

            file_id = _upsert_ingest_file(
                session=session,
                source_system="clerk_civil",
                category="events",
                relative_path=rel,
                file_sha256=sha,
                file_size_bytes=st.st_size,
                file_modified_at=modified_at,
                status="loading",
            )
            session.commit()

            try:
                rows: list[dict] = []
                row_count = 0
                eff_batch = batch_size

                for csv_row in _iter_csv_with_bom(path):
                    case_number = _clean_text(csv_row.get("CaseNbr"))
                    if not case_number:
                        continue

                    mapped = {
                        "case_number": case_number,
                        "event_code": _clean_text(csv_row.get("EventCode")),
                        "event_description": _clean_text(csv_row.get("Event")),
                        "event_date": _parse_date_mdy(csv_row.get("EventDate1")),
                        "party_first_name": _clean_text(csv_row.get("NameFirst")),
                        "party_middle_name": _clean_text(csv_row.get("NameMid")),
                        "party_last_name": _clean_text(csv_row.get("NameLast")),
                        "source_file": path.name,
                        "loaded_at": _utc_now(),
                    }

                    if row_count == 0:
                        eff_batch = _effective_batch_size(batch_size, len(mapped))

                    rows.append(mapped)
                    row_count += 1

                    if len(rows) >= eff_batch:
                        _insert_events_batch(session, rows)
                        session.commit()
                        rows.clear()

                if rows:
                    _insert_events_batch(session, rows)
                    session.commit()

                _mark_ingest_file(session, file_id, "loaded", row_count=row_count)
                session.commit()
                stats["files_loaded"] += 1
                stats["rows_inserted"] += row_count
                logger.info(f"Loaded {row_count:,} events from {path.name}")

            except Exception as exc:
                session.rollback()
                _mark_ingest_file(
                    session, file_id, "failed", error_message=str(exc)[:4000]
                )
                session.commit()
                logger.error(f"Failed to load {path.name}: {exc}")
                raise

    return stats


def _dedup_events(rows: list[dict]) -> list[dict]:
    """Deduplicate event rows by constraint key within a batch."""
    seen: dict[tuple, dict] = {}
    for row in rows:
        key = (
            row.get("case_number"),
            row.get("event_code"),
            str(row.get("event_date")),
            row.get("party_last_name"),
        )
        seen[key] = row
    return list(seen.values())


def _insert_events_batch(session: Session, rows: list[dict]) -> None:
    if not rows:
        return
    rows = _dedup_events(rows)
    stmt = pg_insert(ClerkCivilEvent).values(rows)
    stmt = stmt.on_conflict_do_nothing(
        constraint="uq_clerk_events_case_code_date_party"
    )
    session.execute(stmt)


# ---------------------------------------------------------------------------
# Party loader
# ---------------------------------------------------------------------------


def load_clerk_parties(
    dsn: str,
    root: Path,
    batch_size: int = DEFAULT_BATCH_SIZE,
    skip_unchanged: bool = True,
    limit_files: int | None = None,
) -> dict:
    """
    Load clerk civil party CSV files into clerk_civil_parties table.

    CSV columns: CaseNbr, Party, Name, Address1, Address2, City, State, ZIP,
                 BarNum, PhoneNum, Email
    """
    files = sorted(root.glob("Bulk Data Party File_*.csv"))
    if limit_files:
        files = files[:limit_files]

    if not files:
        logger.warning(f"No party files found in {root}")
        return {"files_found": 0, "files_loaded": 0, "rows_upserted": 0}

    logger.info(f"Found {len(files)} party files in {root}")
    session_factory = get_session_factory(dsn)
    stats = {"files_found": len(files), "files_loaded": 0, "files_skipped": 0, "rows_upserted": 0}

    with session_factory() as session:
        for path in files:
            rel = path.relative_to(root).as_posix() if root in path.parents else path.name
            sha = _compute_sha256(path)
            st = path.stat()
            modified_at = dt.datetime.fromtimestamp(st.st_mtime, tz=dt.UTC)

            existing = _get_existing_ingest_file(session, "clerk_civil", rel)
            if (
                skip_unchanged
                and existing
                and existing.status == "loaded"
                and existing.file_sha256 == sha
            ):
                logger.debug(f"Skipping unchanged: {path.name}")
                stats["files_skipped"] += 1
                continue

            file_id = _upsert_ingest_file(
                session=session,
                source_system="clerk_civil",
                category="parties",
                relative_path=rel,
                file_sha256=sha,
                file_size_bytes=st.st_size,
                file_modified_at=modified_at,
                status="loading",
            )
            session.commit()

            try:
                rows: list[dict] = []
                row_count = 0
                eff_batch = batch_size

                for csv_row in _iter_csv_with_bom(path):
                    case_number = _clean_text(csv_row.get("CaseNbr"))
                    if not case_number:
                        continue

                    full_name = _clean_text(csv_row.get("Name"))
                    first_name, middle_name, last_name = _parse_party_name(full_name)

                    mapped = {
                        "case_number": case_number,
                        "party_type": _clean_text(csv_row.get("Party")),
                        "name": full_name,
                        "first_name": first_name,
                        "middle_name": middle_name,
                        "last_name": last_name,
                        "address1": _clean_text(csv_row.get("Address1")),
                        "address2": _clean_text(csv_row.get("Address2")),
                        "city": _clean_text(csv_row.get("City")),
                        "state": _clean_text(csv_row.get("State")),
                        "zip": _clean_text(csv_row.get("ZIP")),
                        "bar_number": _clean_text(csv_row.get("BarNum")),
                        "phone": _clean_text(csv_row.get("PhoneNum")),
                        "email": _clean_text(csv_row.get("Email")),
                        "source_file": path.name,
                        "loaded_at": _utc_now(),
                    }

                    if row_count == 0:
                        eff_batch = _effective_batch_size(batch_size, len(mapped))

                    rows.append(mapped)
                    row_count += 1

                    if len(rows) >= eff_batch:
                        _upsert_parties_batch(session, rows)
                        session.commit()
                        rows.clear()

                if rows:
                    _upsert_parties_batch(session, rows)
                    session.commit()

                _mark_ingest_file(session, file_id, "loaded", row_count=row_count)
                session.commit()
                stats["files_loaded"] += 1
                stats["rows_upserted"] += row_count
                logger.info(f"Loaded {row_count:,} parties from {path.name}")

            except Exception as exc:
                session.rollback()
                _mark_ingest_file(
                    session, file_id, "failed", error_message=str(exc)[:4000]
                )
                session.commit()
                logger.error(f"Failed to load {path.name}: {exc}")
                raise

    return stats


def _dedup_parties(rows: list[dict]) -> list[dict]:
    """Deduplicate party rows by constraint key within a batch."""
    seen: dict[tuple, dict] = {}
    for row in rows:
        key = (row.get("case_number"), row.get("party_type"), row.get("name"))
        seen[key] = row
    return list(seen.values())


def _upsert_parties_batch(session: Session, rows: list[dict]) -> None:
    if not rows:
        return
    rows = _dedup_parties(rows)
    stmt = pg_insert(ClerkCivilParty).values(rows)
    update_cols = {
        col.name: getattr(stmt.excluded, col.name)
        for col in ClerkCivilParty.__table__.columns
        if col.name not in ("id", "case_number", "party_type", "name")
    }
    stmt = stmt.on_conflict_do_update(
        constraint="uq_clerk_parties_case_type_name",
        set_=update_cols,
    )
    session.execute(stmt)


# ---------------------------------------------------------------------------
# Disposed cases loader
# ---------------------------------------------------------------------------


def load_clerk_disposed(
    dsn: str,
    root: Path,
    batch_size: int = DEFAULT_BATCH_SIZE,
    skip_unchanged: bool = True,
    limit_files: int | None = None,
) -> dict:
    """
    Load disposed cases CSV files into clerk_disposed_cases table.

    These files may not exist on the clerk site (not currently listed),
    but the table and loader are ready if they appear.
    """
    files = sorted(root.glob("Odyssey-JobOutput-*.csv"))
    if not files:
        files = sorted(root.glob("*disposed*.csv"))
    if limit_files:
        files = files[:limit_files]

    if not files:
        logger.info("No disposed case files found (this is normal if the clerk doesn't publish them)")
        return {"files_found": 0, "files_loaded": 0, "rows_upserted": 0}

    logger.info(f"Found {len(files)} disposed case files in {root}")
    session_factory = get_session_factory(dsn)
    stats = {"files_found": len(files), "files_loaded": 0, "files_skipped": 0, "rows_upserted": 0}

    with session_factory() as session:
        for path in files:
            rel = path.relative_to(root).as_posix() if root in path.parents else path.name
            sha = _compute_sha256(path)
            st = path.stat()
            modified_at = dt.datetime.fromtimestamp(st.st_mtime, tz=dt.UTC)

            existing = _get_existing_ingest_file(session, "clerk_civil", rel)
            if (
                skip_unchanged
                and existing
                and existing.status == "loaded"
                and existing.file_sha256 == sha
            ):
                stats["files_skipped"] += 1
                continue

            file_id = _upsert_ingest_file(
                session=session,
                source_system="clerk_civil",
                category="disposed",
                relative_path=rel,
                file_sha256=sha,
                file_size_bytes=st.st_size,
                file_modified_at=modified_at,
                status="loading",
            )
            session.commit()

            try:
                rows: list[dict] = []
                row_count = 0
                eff_batch = batch_size

                for csv_row in _iter_csv_with_bom(path):
                    case_number = _clean_text(
                        csv_row.get("CaseNbr")
                        or csv_row.get("Case Nbr")
                        or csv_row.get("Case Number")
                    )
                    if not case_number:
                        continue

                    mapped = {
                        "case_number": case_number,
                        "style": _clean_text(csv_row.get("Style") or csv_row.get("Case Style")),
                        "case_type": _clean_text(csv_row.get("CaseType") or csv_row.get("Case Type")),
                        "case_subtype": _clean_text(csv_row.get("CaseSubtype") or csv_row.get("Case Subtype")),
                        "closure_date": _parse_date_mdy(
                            csv_row.get("ClosureDate")
                            or csv_row.get("Closure Date")
                            or csv_row.get("S. Closure Date")
                        ),
                        "statistical_closure": _clean_text(
                            csv_row.get("StatisticalClosure") or csv_row.get("Statistical Closure")
                        ),
                        "closure_comment": _clean_text(
                            csv_row.get("ClosureComment")
                            or csv_row.get("Closure Comment")
                            or csv_row.get("Statistical Closure Comment")
                        ),
                        "status_date": _parse_date_mdy(
                            csv_row.get("StatusDate")
                            or csv_row.get("Status Date")
                            or csv_row.get("Case Status Date")
                        ),
                        "current_status": _clean_text(
                            csv_row.get("CurrentStatus")
                            or csv_row.get("Current Status")
                            or csv_row.get("Current Case Status")
                        ),
                        "source_file": path.name,
                        "loaded_at": _utc_now(),
                    }

                    if row_count == 0:
                        eff_batch = _effective_batch_size(batch_size, len(mapped))

                    rows.append(mapped)
                    row_count += 1

                    if len(rows) >= eff_batch:
                        _upsert_disposed_batch(session, rows)
                        session.commit()
                        rows.clear()

                if rows:
                    _upsert_disposed_batch(session, rows)
                    session.commit()

                _mark_ingest_file(session, file_id, "loaded", row_count=row_count)
                session.commit()
                stats["files_loaded"] += 1
                stats["rows_upserted"] += row_count
                logger.info(f"Loaded {row_count:,} disposed cases from {path.name}")

            except Exception as exc:
                session.rollback()
                _mark_ingest_file(
                    session, file_id, "failed", error_message=str(exc)[:4000]
                )
                session.commit()
                logger.error(f"Failed to load {path.name}: {exc}")
                raise

    return stats


def _upsert_disposed_batch(session: Session, rows: list[dict]) -> None:
    if not rows:
        return
    rows = _dedup_by_key(rows, "case_number")
    stmt = pg_insert(ClerkDisposedCase).values(rows)
    update_cols = {
        col.name: getattr(stmt.excluded, col.name)
        for col in ClerkDisposedCase.__table__.columns
        if col.name != "case_number"
    }
    stmt = stmt.on_conflict_do_update(
        index_elements=[ClerkDisposedCase.case_number],
        set_=update_cols,
    )
    session.execute(stmt)


# ---------------------------------------------------------------------------
# Return of service + garnishment loader
# ---------------------------------------------------------------------------


def load_clerk_garnishment(
    dsn: str,
    root: Path,
    batch_size: int = DEFAULT_BATCH_SIZE,
    skip_unchanged: bool = True,
    limit_files: int | None = None,
) -> dict:
    """
    Load weekly return-of-service and garnishment CSV files.

    Source pattern: ReturnOfServiceAndGarnishmentData_YYYY-MM-DD.csv
    """
    files = sorted(root.glob("ReturnOfServiceAndGarnishmentData_*.csv"))
    if limit_files:
        files = files[:limit_files]

    if not files:
        logger.info("No return-of-service/garnishment files found")
        return {"files_found": 0, "files_loaded": 0, "rows_inserted": 0}

    logger.info(f"Found {len(files)} garnishment files in {root}")
    session_factory = get_session_factory(dsn)
    stats = {"files_found": len(files), "files_loaded": 0, "files_skipped": 0, "rows_inserted": 0}

    with session_factory() as session:
        for path in files:
            rel = path.relative_to(root).as_posix() if root in path.parents else path.name
            sha = _compute_sha256(path)
            st = path.stat()
            modified_at = dt.datetime.fromtimestamp(st.st_mtime, tz=dt.UTC)

            existing = _get_existing_ingest_file(session, "clerk_civil", rel)
            if (
                skip_unchanged
                and existing
                and existing.status == "loaded"
                and existing.file_sha256 == sha
            ):
                stats["files_skipped"] += 1
                continue

            file_id = _upsert_ingest_file(
                session=session,
                source_system="clerk_civil",
                category="garnishment",
                relative_path=rel,
                file_sha256=sha,
                file_size_bytes=st.st_size,
                file_modified_at=modified_at,
                status="loading",
            )
            session.commit()

            snapshot_date = _parse_date_from_filename(path.name)

            try:
                rows: list[dict] = []
                row_count = 0
                eff_batch = batch_size

                for csv_row in _iter_csv_with_bom(path):
                    case_number = _clean_text(
                        csv_row.get("CaseNbr") or csv_row.get("Case Number")
                    )
                    if not case_number:
                        continue

                    plaintiff_name = _clean_text(
                        csv_row.get("PlaintiffName") or csv_row.get("Plaintiff Name")
                    )
                    garnishee_name = _clean_text(
                        csv_row.get("GarnisheeName") or csv_row.get("Garnishee Name")
                    )
                    defendant_name = _clean_text(
                        csv_row.get("DefendantName") or csv_row.get("Defendant Name")
                    )

                    service_return_raw = _clean_text(
                        csv_row.get("Service_Return_Date")
                        or csv_row.get("Service Return Date")
                    )
                    non_service_return_raw = _clean_text(
                        csv_row.get("Return_of_Non_Service_Date")
                        or csv_row.get("Return of Non Service Date")
                    )
                    writ_filed_raw = _clean_text(
                        csv_row.get("Date_Writ_of_Garnishment_filed")
                        or csv_row.get("Date Writ of Garnishment filed")
                    )
                    writ_issued_raw = _clean_text(
                        csv_row.get("Date_Writ_of_Garnishment_Issued")
                        or csv_row.get("Date Writ of Garnishment Issued")
                    )

                    filing_date = _parse_date_mdy(csv_row.get("DtFile"))
                    pre_trial_date = _parse_date_mdy(csv_row.get("Pre_Trial_Date"))
                    service_return_date = _parse_date_mdy(service_return_raw)
                    non_service_return_date = _parse_date_mdy(non_service_return_raw)
                    writ_filed_date = _parse_date_mdy(writ_filed_raw)
                    writ_issued_date = _parse_date_mdy(writ_issued_raw)

                    hash_material = "|".join([
                        case_number or "",
                        plaintiff_name or "",
                        garnishee_name or "",
                        defendant_name or "",
                        str(filing_date or ""),
                        str(service_return_date or ""),
                        str(non_service_return_date or ""),
                        str(writ_filed_date or ""),
                        str(writ_issued_date or ""),
                        path.name,
                    ])

                    mapped = {
                        "row_hash": hashlib.sha256(hash_material.encode("utf-8")).hexdigest(),
                        "case_number": case_number,
                        "filing_date": filing_date,
                        "plaintiff_name": plaintiff_name,
                        "garnishee_name": garnishee_name,
                        "defendant_name": defendant_name,
                        "address1": _clean_text(csv_row.get("Address1")),
                        "address2": _clean_text(csv_row.get("Address2")),
                        "address3": _clean_text(csv_row.get("Address3")),
                        "city": _clean_text(csv_row.get("City")),
                        "state": _clean_text(csv_row.get("State")),
                        "zip": _clean_text(csv_row.get("ZIP") or csv_row.get("Zip")),
                        "case_type_description": _clean_text(
                            csv_row.get("CaseTypeDesc") or csv_row.get("Case Type Desc")
                        ),
                        "case_status_description": _clean_text(
                            csv_row.get("CaseStatDesc") or csv_row.get("Case Status Desc")
                        ),
                        "pre_trial_date": pre_trial_date,
                        "service_return_raw": service_return_raw,
                        "service_return_date": service_return_date,
                        "non_service_return_date": non_service_return_date,
                        "writ_filed_date": writ_filed_date,
                        "writ_issued_date": writ_issued_date,
                        "snapshot_date": snapshot_date,
                        "source_file": path.name,
                        "loaded_at": _utc_now(),
                    }

                    if row_count == 0:
                        eff_batch = _effective_batch_size(batch_size, len(mapped))

                    rows.append(mapped)
                    row_count += 1

                    if len(rows) >= eff_batch:
                        _insert_garnishment_batch(session, rows)
                        session.commit()
                        rows.clear()

                if rows:
                    _insert_garnishment_batch(session, rows)
                    session.commit()

                _mark_ingest_file(session, file_id, "loaded", row_count=row_count)
                session.commit()
                stats["files_loaded"] += 1
                stats["rows_inserted"] += row_count
                logger.info(f"Loaded {row_count:,} garnishment rows from {path.name}")

            except Exception as exc:
                session.rollback()
                _mark_ingest_file(
                    session, file_id, "failed", error_message=str(exc)[:4000]
                )
                session.commit()
                logger.error(f"Failed to load {path.name}: {exc}")
                raise

    return stats


def _insert_garnishment_batch(session: Session, rows: list[dict]) -> None:
    if not rows:
        return
    rows = _dedup_by_key(rows, "row_hash")
    stmt = pg_insert(ClerkGarnishmentCase).values(rows)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=[ClerkGarnishmentCase.row_hash],
    )
    session.execute(stmt)


# ---------------------------------------------------------------------------
# Name index loader (alpha_index — complete party index, 20+ years)
# ---------------------------------------------------------------------------


def load_clerk_name_index(
    dsn: str,
    root: Path | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    skip_unchanged: bool = True,
    limit_files: int | None = None,
) -> dict:
    """
    Load pipe-delimited alpha_index TXT files into clerk_name_index table.

    Source: https://publicrec.hillsclerk.com/Civil/alpha_index/{Circuit,County}/
    Format: Pipe-delimited, 50 columns, 27 files per court type (A-Z + NonAlpha).

    This is the complete alphabetical party index covering ALL civil cases
    (Circuit + County) going back 20+ years. ~1.4 GB total.
    """
    if root is None:
        root = DEFAULT_ALPHA_DIR

    files = sorted(
        f for f in root.glob("*CivilNameIndex*.txt")
        if f.is_file() and f.stat().st_size > 0
    )
    if limit_files:
        files = files[:limit_files]

    if not files:
        logger.warning(f"No name index files found in {root}")
        return {"files_found": 0, "files_loaded": 0, "rows_inserted": 0}

    logger.info(f"Found {len(files)} name index files in {root}")
    session_factory = get_session_factory(dsn)
    stats = {
        "files_found": len(files),
        "files_loaded": 0,
        "files_skipped": 0,
        "rows_inserted": 0,
        "rows_skipped_no_ucn": 0,
    }

    with session_factory() as session:
        for path in files:
            rel = path.name
            sha = _compute_sha256(path)
            st = path.stat()
            modified_at = dt.datetime.fromtimestamp(st.st_mtime, tz=dt.UTC)

            existing = _get_existing_ingest_file(session, "clerk_civil", rel)
            if (
                skip_unchanged
                and existing
                and existing.status == "loaded"
                and existing.file_sha256 == sha
            ):
                logger.debug(f"Skipping unchanged: {path.name}")
                stats["files_skipped"] += 1
                continue

            file_id = _upsert_ingest_file(
                session=session,
                source_system="clerk_civil",
                category="name_index",
                relative_path=rel,
                file_sha256=sha,
                file_size_bytes=st.st_size,
                file_modified_at=modified_at,
                status="loading",
            )
            session.commit()

            try:
                rows: list[dict] = []
                row_count = 0
                skipped = 0
                eff_batch = batch_size

                for pipe_row in _iter_pipe_delimited(path):
                    ucn = _clean_text(pipe_row.get("Uniform Case Number"))
                    if not ucn:
                        skipped += 1
                        continue

                    court_type_raw = _clean_text(pipe_row.get("Court Type"))
                    case_type_val = _clean_text(pipe_row.get("Case Type"))
                    case_number = _extract_case_number_from_ucn(ucn)

                    mapped = {
                        "court_type": court_type_raw or ("Circuit" if "Circuit" in path.name else "County"),
                        "business_name": _clean_text(pipe_row.get("BusinessName")),
                        "last_name": _clean_text(pipe_row.get("LastName")),
                        "first_name": _clean_text(pipe_row.get("FirstName")),
                        "middle_name": _clean_text(pipe_row.get("MiddleName")),
                        "suffix": _clean_text(pipe_row.get("Suffix")),
                        "party_type": _clean_text(pipe_row.get("Party Connection Type")),
                        "ucn": ucn,
                        "case_number": case_number,
                        "case_type": case_type_val,
                        "division": _clean_text(pipe_row.get("Division")),
                        "judge_name": _clean_text(pipe_row.get("Judge Name")),
                        "date_filed": _parse_date_mdy(pipe_row.get("Date Filed")),
                        "current_status": _clean_text(pipe_row.get("Current Status")),
                        "status_date": _parse_date_mdy(pipe_row.get("Current Status Date")),
                        "address1": _clean_text(pipe_row.get("Party Address Line 1")),
                        "address2": _clean_text(pipe_row.get("Party Address Line 2")),
                        "city": _clean_text(pipe_row.get("Party Address City")),
                        "state": _clean_text(pipe_row.get("Party Address State")),
                        "zip_code": _clean_text(pipe_row.get("Party Address Zip Code")),
                        "disposition_code": _clean_text(pipe_row.get("Disposition Code")),
                        "disposition_desc": _clean_text(pipe_row.get("Disposition Description")),
                        "disposition_date": _parse_date_mdy(pipe_row.get("Disposition Date")),
                        "amount_paid": _clean_text(pipe_row.get("Amount Paid")),
                        "date_paid": _parse_date_mdy(pipe_row.get("Date Paid")),
                        "akas": _clean_text(pipe_row.get("AKAs")),
                        "is_foreclosure": _is_foreclosure(case_type_val),
                        "source_file": path.name,
                        "loaded_at": _utc_now(),
                    }

                    if row_count == 0:
                        eff_batch = _effective_batch_size(batch_size, len(mapped))

                    rows.append(mapped)
                    row_count += 1

                    if len(rows) >= eff_batch:
                        _insert_name_index_batch(session, rows)
                        session.commit()
                        rows.clear()

                if rows:
                    _insert_name_index_batch(session, rows)
                    session.commit()

                _mark_ingest_file(session, file_id, "loaded", row_count=row_count)
                session.commit()
                stats["files_loaded"] += 1
                stats["rows_inserted"] += row_count
                stats["rows_skipped_no_ucn"] += skipped
                logger.info(
                    f"Loaded {row_count:,} name index rows from {path.name} "
                    f"(skipped {skipped:,} no-UCN)"
                )

            except Exception as exc:
                session.rollback()
                _mark_ingest_file(
                    session, file_id, "failed", error_message=str(exc)[:4000]
                )
                session.commit()
                logger.error(f"Failed to load {path.name}: {exc}")
                raise

    return stats


def _dedup_name_index(rows: list[dict]) -> list[dict]:
    """Deduplicate name index rows by (ucn, disposition_code) within a batch."""
    seen: dict[tuple, dict] = {}
    for row in rows:
        key = (row.get("ucn"), row.get("disposition_code") or "")
        seen[key] = row
    return list(seen.values())


def _insert_name_index_batch(session: Session, rows: list[dict]) -> None:
    if not rows:
        return
    rows = _dedup_name_index(rows)
    stmt = pg_insert(ClerkNameIndex).values(rows)
    stmt = stmt.on_conflict_do_nothing(
        constraint="uq_clerk_name_index_ucn_disp"
    )
    session.execute(stmt)


# ---------------------------------------------------------------------------
# Init DB + Load All
# ---------------------------------------------------------------------------


def init_db(dsn: str) -> None:
    """Create all clerk tables (and any missing tables from models.py)."""
    engine = get_engine(dsn)
    # Ensure pg_trgm extension exists for GIN trigram indexes
    with engine.connect() as conn:
        conn.execute(sa_text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        conn.commit()
    Base.metadata.create_all(bind=engine)
    logger.info("Clerk civil tables initialized")


def load_all(
    dsn: str,
    root: Path,
    batch_size: int = DEFAULT_BATCH_SIZE,
    skip_unchanged: bool = True,
    sync_first: bool = False,
    force_sync: bool = False,
) -> dict:
    """Run all loaders in sequence."""
    all_stats: dict = {}

    if sync_first:
        logger.info("Downloading latest clerk bulk data files ...")
        dl_stats = download_clerk_bulk(root, force=force_sync)
        all_stats["download"] = dl_stats
        logger.info(
            f"Download complete: {dl_stats['downloaded']} new, "
            f"{dl_stats['skipped']} skipped, {dl_stats['errors']} errors"
        )

    init_db(dsn)

    logger.info("--- Loading cases ---")
    all_stats["cases"] = load_clerk_cases(dsn, root, batch_size, skip_unchanged)

    logger.info("--- Loading events ---")
    all_stats["events"] = load_clerk_events(dsn, root, batch_size, skip_unchanged)

    logger.info("--- Loading parties ---")
    all_stats["parties"] = load_clerk_parties(dsn, root, batch_size, skip_unchanged)

    logger.info("--- Loading disposed cases ---")
    all_stats["disposed"] = load_clerk_disposed(dsn, root, batch_size, skip_unchanged)

    logger.info("--- Loading return-of-service/garnishment ---")
    all_stats["garnishment"] = load_clerk_garnishment(
        dsn, root, batch_size, skip_unchanged
    )

    logger.info("--- Loading name index (alpha_index) ---")
    all_stats["name_index"] = load_clerk_name_index(
        dsn, DEFAULT_ALPHA_DIR, batch_size, skip_unchanged
    )

    return all_stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="PostgreSQL loader for Hillsborough County Clerk civil bulk data."
    )
    parser.add_argument("--db-url", default=None, help="Postgres DSN override.")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Create clerk civil database tables.")

    dl_cmd = sub.add_parser(
        "download-clerk-bulk",
        help="Download latest monthly CSV files from the clerk website.",
    )
    dl_cmd.add_argument(
        "--output-dir", type=Path, default=DEFAULT_CLERK_DIR,
        help="Directory to save downloaded CSV files.",
    )
    dl_cmd.add_argument("--force", action="store_true", help="Re-download existing files.")
    dl_cmd.add_argument(
        "--file-types",
        nargs="+",
        choices=["case", "event", "party", "disposed", "garnishment"],
        default=None, help="Filter to specific file types.",
    )

    cases_cmd = sub.add_parser("load-clerk-cases", help="Parse and load case CSV files.")
    cases_cmd.add_argument("--root", type=Path, default=DEFAULT_CLERK_DIR)
    cases_cmd.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    cases_cmd.add_argument("--no-skip-unchanged", action="store_true")
    cases_cmd.add_argument("--limit-files", type=int, default=None)

    events_cmd = sub.add_parser("load-clerk-events", help="Parse and load event CSV files.")
    events_cmd.add_argument("--root", type=Path, default=DEFAULT_CLERK_DIR)
    events_cmd.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    events_cmd.add_argument("--no-skip-unchanged", action="store_true")
    events_cmd.add_argument("--limit-files", type=int, default=None)

    parties_cmd = sub.add_parser("load-clerk-parties", help="Parse and load party CSV files.")
    parties_cmd.add_argument("--root", type=Path, default=DEFAULT_CLERK_DIR)
    parties_cmd.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parties_cmd.add_argument("--no-skip-unchanged", action="store_true")
    parties_cmd.add_argument("--limit-files", type=int, default=None)

    disposed_cmd = sub.add_parser("load-clerk-disposed", help="Parse disposed cases CSVs.")
    disposed_cmd.add_argument("--root", type=Path, default=DEFAULT_CLERK_DIR)
    disposed_cmd.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    disposed_cmd.add_argument("--no-skip-unchanged", action="store_true")
    disposed_cmd.add_argument("--limit-files", type=int, default=None)

    garnishment_cmd = sub.add_parser(
        "load-clerk-garnishment",
        help="Parse return-of-service and garnishment CSVs.",
    )
    garnishment_cmd.add_argument("--root", type=Path, default=DEFAULT_CLERK_DIR)
    garnishment_cmd.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    garnishment_cmd.add_argument("--no-skip-unchanged", action="store_true")
    garnishment_cmd.add_argument("--limit-files", type=int, default=None)

    ni_cmd = sub.add_parser(
        "load-clerk-name-index",
        help="Parse and load alpha_index pipe-delimited TXT files.",
    )
    ni_cmd.add_argument(
        "--root", type=Path, default=DEFAULT_ALPHA_DIR,
        help="Directory containing *CivilNameIndex*.txt files.",
    )
    ni_cmd.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    ni_cmd.add_argument("--no-skip-unchanged", action="store_true")
    ni_cmd.add_argument("--limit-files", type=int, default=None)

    all_cmd = sub.add_parser("load-all", help="Run all loaders (download + load).")
    all_cmd.add_argument("--root", type=Path, default=DEFAULT_CLERK_DIR)
    all_cmd.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    all_cmd.add_argument("--no-skip-unchanged", action="store_true")
    all_cmd.add_argument("--sync-first", action="store_true", help="Download files before loading.")
    all_cmd.add_argument("--force-sync", action="store_true", help="Re-download existing files.")

    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    dsn = resolve_pg_dsn(args.db_url)

    if args.command == "init-db":
        init_db(dsn)
        logger.info("Initialized clerk civil PostgreSQL schema.")
        return 0

    if args.command == "download-clerk-bulk":
        stats = download_clerk_bulk(
            output_dir=args.output_dir,
            force=args.force,
            file_types=args.file_types,
        )
        logger.info(f"Download stats: {stats}")
        return 0

    # For all load commands, ensure tables exist first
    init_db(dsn)

    if args.command == "load-clerk-cases":
        stats = load_clerk_cases(
            dsn=dsn,
            root=args.root,
            batch_size=args.batch_size,
            skip_unchanged=not args.no_skip_unchanged,
            limit_files=args.limit_files,
        )
        logger.info(f"Case loader stats: {stats}")
        return 0

    if args.command == "load-clerk-events":
        stats = load_clerk_events(
            dsn=dsn,
            root=args.root,
            batch_size=args.batch_size,
            skip_unchanged=not args.no_skip_unchanged,
            limit_files=args.limit_files,
        )
        logger.info(f"Event loader stats: {stats}")
        return 0

    if args.command == "load-clerk-parties":
        stats = load_clerk_parties(
            dsn=dsn,
            root=args.root,
            batch_size=args.batch_size,
            skip_unchanged=not args.no_skip_unchanged,
            limit_files=args.limit_files,
        )
        logger.info(f"Party loader stats: {stats}")
        return 0

    if args.command == "load-clerk-disposed":
        stats = load_clerk_disposed(
            dsn=dsn,
            root=args.root,
            batch_size=args.batch_size,
            skip_unchanged=not args.no_skip_unchanged,
            limit_files=args.limit_files,
        )
        logger.info(f"Disposed loader stats: {stats}")
        return 0

    if args.command == "load-clerk-garnishment":
        stats = load_clerk_garnishment(
            dsn=dsn,
            root=args.root,
            batch_size=args.batch_size,
            skip_unchanged=not args.no_skip_unchanged,
            limit_files=args.limit_files,
        )
        logger.info(f"Garnishment loader stats: {stats}")
        return 0

    if args.command == "load-clerk-name-index":
        stats = load_clerk_name_index(
            dsn=dsn,
            root=args.root,
            batch_size=args.batch_size,
            skip_unchanged=not args.no_skip_unchanged,
            limit_files=args.limit_files,
        )
        logger.info(f"Name index loader stats: {stats}")
        return 0

    if args.command == "load-all":
        stats = load_all(
            dsn=dsn,
            root=args.root,
            batch_size=args.batch_size,
            skip_unchanged=not args.no_skip_unchanged,
            sync_first=args.sync_first,
            force_sync=args.force_sync,
        )
        logger.info(f"Full load stats: {stats}")
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
