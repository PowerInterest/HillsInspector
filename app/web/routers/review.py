"""
Review routes - triage queues for foreclosures missing data.
All reads from PostgreSQL — no SQLite.
"""

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from loguru import logger
from sqlalchemy import text

from app.web.template_filters import get_templates
from sunbiz.db import get_engine, resolve_pg_dsn

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
                           property_address, auction_status, folio, strap
                    FROM foreclosures
                    WHERE strap IS NULL
                    ORDER BY auction_date DESC
                    LIMIT :lim OFFSET :off
                """),
                {"lim": per_page, "off": offset},
            ).mappings().fetchall()

            failed_auctions = [dict(r) for r in rows]
    except Exception as e:
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
