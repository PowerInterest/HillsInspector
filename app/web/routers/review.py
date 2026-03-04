"""
Review routes - triage queues for foreclosures missing data.
All reads from PostgreSQL — no SQLite.
"""

from typing import Any, cast

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from loguru import logger
from sqlalchemy import text

from app.web.template_filters import get_templates
from sunbiz.db import get_engine, resolve_pg_dsn
from src.services.audit.web_audit_service import get_encumbrance_audit_inbox

router = APIRouter()
templates = get_templates()


def _engine():
    return get_engine(resolve_pg_dsn())


@router.get("/hcpa-failures", response_class=HTMLResponse)
async def hcpa_failures(
    request: Request,
    page: int = 1,
    per_page: int = 25,
    message: str | None = None,
    error: str | None = None,
):
    """
    Foreclosures missing strap/folio (cannot do property enrichment).
    Replaces the old SQLite HCPA-failure review queue.
    """
    offset = (page - 1) * per_page
    try:
        with _engine().connect() as conn:
            total = conn.execute(
                text("SELECT COUNT(*) FROM foreclosures WHERE strap IS NULL")
            ).scalar() or 0

            rows = conn.execute(
                text("""
                    SELECT case_number_raw, case_number_norm, auction_date,
                           property_address, auction_status, folio, strap,
                           pdf_path,
                           (judgment_data IS NOT NULL) AS has_judgment
                    FROM foreclosures
                    WHERE strap IS NULL
                    ORDER BY auction_date DESC
                    LIMIT :lim OFFSET :off
                """),
                {"lim": per_page, "off": offset},
            ).mappings().fetchall()

            failed_auctions = [dict(r) for r in rows]
    except Exception:
        logger.exception("review query failed")
        failed_auctions = []
        total = 0

    total_pages = max(1, (total + per_page - 1) // per_page)

    return templates.TemplateResponse(
        "review/hcpa_failures.html",
        {
            "request": request,
            "auctions": failed_auctions,
            "total": total,
            "message": message,
            "error": error,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "total_pages": total_pages,
            },
        },
    )


@router.get("/encumbrance-audit", response_class=HTMLResponse)
async def encumbrance_audit(
    request: Request,
    bucket: str | None = None,
    family: str | None = None,
    q: str | None = None,
):
    """
    Global encumbrance audit inbox — read-only operator view.
    Runs the full audit on each request (population is small).
    """
    error_msg: str | None = None
    try:
        with _engine().connect() as conn:
            inbox = get_encumbrance_audit_inbox(conn=conn)
    except Exception:
        logger.exception("encumbrance audit inbox failed")
        inbox = {
            "summary_cards": {"open_issues": 0, "affected_foreclosures": 0, "top_bucket": None, "data_coverage_count": 0},
            "bucket_summaries": [],
            "rows": [],
        }
        error_msg = "Audit engine temporarily unavailable"

    rows = cast("list[dict[str, Any]]", inbox.get("rows", []))

    # Server-side filters
    if bucket:
        rows = [r for r in rows if r["bucket"] == bucket]
    if family:
        rows = [r for r in rows if r["family"] == family]
    if q:
        q_lower = q.lower()
        rows = [r for r in rows if q_lower in (r.get("property_address") or "").lower()
                or q_lower in (r.get("case_number") or "").lower()
                or q_lower in (r.get("reason") or "").lower()]

    # Distinct families and buckets for filter dropdowns
    all_rows = cast("list[dict[str, Any]]", inbox.get("rows", []))
    all_families = sorted(
        {str(r.get("family", "")) for r in all_rows if r.get("family")}
    )
    all_buckets = sorted(
        {str(r.get("bucket", "")) for r in all_rows if r.get("bucket")}
    )

    return templates.TemplateResponse(
        "review/encumbrance_audit.html",
        {
            "request": request,
            "summary_cards": inbox["summary_cards"],
            "bucket_summaries": inbox["bucket_summaries"],
            "rows": rows,
            "all_families": all_families,
            "all_buckets": all_buckets,
            "active_bucket": bucket,
            "active_family": family,
            "active_q": q or "",
            "error": error_msg,
        },
    )
