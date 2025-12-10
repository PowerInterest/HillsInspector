"""
Review routes - manual review queues for failed scrapes.
"""
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from app.web.database import (
    get_failed_hcpa_scrapes,
    get_failed_hcpa_count,
    mark_hcpa_reviewed
)

router = APIRouter()

# Templates
TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@router.get("/hcpa-failures", response_class=HTMLResponse)
async def hcpa_failures(
    request: Request,
    page: int = 1,
    per_page: int = 25
):
    """
    Review queue for auctions where HCPA scrape failed.
    These need manual intervention to get legal description for ORI search.
    """
    offset = (page - 1) * per_page

    failed_auctions = get_failed_hcpa_scrapes(limit=per_page, offset=offset)
    total = get_failed_hcpa_count()
    total_pages = (total + per_page - 1) // per_page if total > 0 else 1

    return templates.TemplateResponse(
        "review/hcpa_failures.html",
        {
            "request": request,
            "auctions": failed_auctions,
            "total": total,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "total_pages": total_pages
            }
        }
    )


@router.post("/hcpa-failures/{case_number}/mark-reviewed")
async def mark_reviewed(
    case_number: str,
    notes: str = Form(default="")
):
    """Mark an auction as manually reviewed."""
    mark_hcpa_reviewed(case_number, notes)
    return RedirectResponse(url="/review/hcpa-failures", status_code=303)
