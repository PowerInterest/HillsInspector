"""Download example ORI PDFs by document type from the Hillsborough Clerk PAV system.

Queries PG ``official_records_daily_instruments`` for recent instrument numbers
per ``doc_type``, then searches the Hyland PAV API by instrument number to obtain
internal document IDs and downloads the corresponding PDF.  Files are saved to
``docs/example_docs/{TYPE_CODE}/{instrument_number}.pdf``.

This is a standalone research/exploration tool — it does NOT modify any PG data.
It is used to understand what useful information each ORI document type contains.

Usage::

    uv run python -m src.tools.download_ori_doc_examples
    uv run python -m src.tools.download_ori_doc_examples --doc-types MTG JUD LP LN
    uv run python -m src.tools.download_ori_doc_examples --limit-per-type 3 --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import urllib.parse
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Path bootstrap (for ``python -m src.tools.…`` invocation)
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from sqlalchemy import text  # noqa: E402

from sunbiz.db import get_engine, resolve_pg_dsn  # noqa: E402

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# PAV constants (same as pg_ori_service / backfill_mortgage_ori_id)
# ---------------------------------------------------------------------------
_PAV_KEYWORD_URL = (
    "https://publicaccess.hillsclerk.com"
    "/PAVDirectSearch/api/CustomQuery/KeywordSearch"
)
_PAV_DOC_URL = (
    "https://publicaccess.hillsclerk.com"
    "/PAVDirectSearch/api/Document/{doc_id}/?OverlayMode=View"
)
_PAV_HEADERS = {
    "Content-Type": "application/json",
    "Origin": "https://publicaccess.hillsclerk.com",
    "Referer": "https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html",
}

# Delays between API calls
_SEARCH_DELAY = 0.75  # seconds
_DOWNLOAD_DELAY = 1.0

# Output root (relative to project)
_OUTPUT_DIR = _PROJECT_ROOT / "docs" / "example_docs"


# ---------------------------------------------------------------------------
# PG helpers
# ---------------------------------------------------------------------------

def _fetch_doc_types(dsn: str | None) -> list[str]:
    """Return sorted list of distinct doc_type values from PG."""
    engine = get_engine(resolve_pg_dsn(dsn))
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT DISTINCT doc_type FROM official_records_daily_instruments "
                "WHERE doc_type IS NOT NULL ORDER BY doc_type"
            )
        ).fetchall()
    return [r[0] for r in rows]


def _fetch_instruments_for_type(
    dsn: str | None, doc_type: str, limit: int
) -> list[str]:
    """Return up to *limit* recent instrument numbers for *doc_type*."""
    engine = get_engine(resolve_pg_dsn(dsn))
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT instrument_number "
                "FROM official_records_daily_instruments "
                "WHERE doc_type = :dt "
                "ORDER BY recording_date DESC NULLS LAST "
                "LIMIT :lim"
            ),
            {"dt": doc_type, "lim": limit},
        ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# PAV helpers
# ---------------------------------------------------------------------------

def _pav_search_instrument(
    session: requests.Session, instrument: str
) -> str | None:
    """Search PAV by instrument number; return internal doc ID or None."""
    payload = {
        "QueryID": 320,
        "Keywords": [{"Id": 1006, "Value": instrument}],
        "QueryLimit": 5,
    }
    try:
        resp = session.post(_PAV_KEYWORD_URL, json=payload, timeout=30)
        if resp.status_code == 429 or resp.status_code >= 500:
            logger.warning("PAV HTTP %d for instrument %s — backing off", resp.status_code, instrument)
            time.sleep(5)
            resp = session.post(_PAV_KEYWORD_URL, json=payload, timeout=30)
        if resp.status_code != 200:
            logger.warning("PAV HTTP %d for instrument %s", resp.status_code, instrument)
            return None
        data = resp.json()
        rows = data.get("Data") or []
        if not rows:
            logger.debug("PAV: no results for instrument %s", instrument)
            return None
        doc_id = rows[0].get("ID")
        return str(doc_id) if doc_id else None
    except Exception as exc:
        logger.warning("PAV search error for %s: %s", instrument, exc)
        return None


def _pav_download_pdf(
    session: requests.Session, doc_id: str, dest: Path
) -> bool:
    """Download a PDF from PAV and save to *dest*. Return True on success."""
    encoded_id = urllib.parse.quote(doc_id, safe="")
    url = _PAV_DOC_URL.format(doc_id=encoded_id)
    try:
        resp = session.get(url, timeout=60)
        if resp.status_code != 200:
            logger.warning("PDF download HTTP %d for doc_id %s", resp.status_code, doc_id)
            return False
        content = resp.content
        # Validate PDF magic bytes and minimum size
        if content[:5] != b"%PDF-":
            logger.warning("Response is not a PDF for doc_id %s (got %r…)", doc_id, content[:20])
            return False
        if len(content) < 1024:
            logger.warning("PDF too small (%d bytes) for doc_id %s", len(content), doc_id)
            return False
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)
        return True
    except Exception as exc:
        logger.warning("PDF download error for doc_id %s: %s", doc_id, exc)
        return False


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Download example ORI PDFs by document type from PAV"
    )
    p.add_argument(
        "--doc-types",
        nargs="+",
        default=None,
        help="Limit to specific doc type codes (e.g. MTG JUD LP). Default: all.",
    )
    p.add_argument(
        "--limit-per-type",
        type=int,
        default=5,
        help="Number of example PDFs per type (default: 5)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Query PG and search PAV but skip PDF downloads",
    )
    p.add_argument(
        "--dsn",
        default=None,
        help="PostgreSQL DSN override",
    )
    return p


def main() -> None:
    args = _build_parser().parse_args()
    dsn: str | None = args.dsn

    # 1. Discover doc types
    if args.doc_types:
        doc_types = args.doc_types
        logger.info("Using %d user-specified doc types: %s", len(doc_types), doc_types)
    else:
        doc_types = _fetch_doc_types(dsn)
        logger.info("Discovered %d distinct doc types in PG", len(doc_types))

    # 2. Build work plan: {doc_type: [instrument, …]}
    work: dict[str, list[str]] = {}
    for dt in doc_types:
        instruments = _fetch_instruments_for_type(dsn, dt, args.limit_per_type)
        if instruments:
            work[dt] = instruments
            logger.info("  %s: %d instruments queued", dt, len(instruments))
        else:
            logger.warning("  %s: no instruments found in PG", dt)

    total_instruments = sum(len(v) for v in work.values())
    logger.info(
        "Work plan: %d types, %d total instruments", len(work), total_instruments
    )

    # 3. Process — search PAV + download PDFs
    session = requests.Session()
    session.headers.update(_PAV_HEADERS)

    stats = {"searched": 0, "found": 0, "downloaded": 0, "skipped_existing": 0}

    for dt, instruments in work.items():
        type_dir = _OUTPUT_DIR / dt
        for instrument in instruments:
            dest = type_dir / f"{instrument}.pdf"

            # Resume: skip if already downloaded
            if dest.exists() and dest.stat().st_size > 1024:
                logger.debug("Already exists: %s", dest)
                stats["skipped_existing"] += 1
                continue

            # Search PAV for doc ID
            time.sleep(_SEARCH_DELAY)
            stats["searched"] += 1
            doc_id = _pav_search_instrument(session, instrument)
            if not doc_id:
                logger.info("[%s] %s — not found in PAV", dt, instrument)
                continue
            stats["found"] += 1

            if args.dry_run:
                logger.info("[%s] %s — PAV doc_id=%s (dry-run, skip download)", dt, instrument, doc_id)
                continue

            # Download PDF
            time.sleep(_DOWNLOAD_DELAY)
            ok = _pav_download_pdf(session, doc_id, dest)
            if ok:
                stats["downloaded"] += 1
                sz = dest.stat().st_size
                logger.info("[%s] %s — saved (%d KB)", dt, instrument, sz // 1024)
            else:
                logger.warning("[%s] %s — download failed", dt, instrument)

    # 4. Summary
    logger.info("--- Summary ---")
    logger.info("  Searched PAV:     %d", stats["searched"])
    logger.info("  Found doc IDs:    %d", stats["found"])
    logger.info("  Downloaded PDFs:  %d", stats["downloaded"])
    logger.info("  Skipped existing: %d", stats["skipped_existing"])

    # Count actual files on disk
    if _OUTPUT_DIR.exists():
        pdf_count = len(list(_OUTPUT_DIR.rglob("*.pdf")))
        dir_count = len([d for d in _OUTPUT_DIR.iterdir() if d.is_dir()])
        logger.info("  On disk: %d PDFs in %d type dirs", pdf_count, dir_count)


if __name__ == "__main__":
    main()
