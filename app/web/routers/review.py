"""
Review routes - manual review queues for failed scrapes.
"""
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from loguru import logger

from app.web.database import (
    get_failed_hcpa_scrapes,
    get_failed_hcpa_count,
    mark_hcpa_reviewed,
    DatabaseLockedError,
    DatabaseUnavailableError,
)

router = APIRouter()

# Templates
TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@router.get("/hcpa-failures", response_class=HTMLResponse)
async def hcpa_failures(
    request: Request,
    page: int = 1,
    per_page: int = 25,
    message: str | None = None,
    error: str | None = None
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
            "message": message,
            "error": error,
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
    request: Request,
    case_number: str,
    notes: str = Form(default="")
):
    """Mark an auction as manually reviewed."""
    try:
        success = mark_hcpa_reviewed(case_number, notes)
        if success:
            return RedirectResponse(
                url=f"/review/hcpa-failures?message=Case+{case_number}+marked+as+reviewed",
                status_code=303
            )
        return RedirectResponse(
            url=f"/review/hcpa-failures?error=Failed+to+update+case+{case_number}",
            status_code=303
        )
    except DatabaseLockedError:
        logger.warning(f"Database locked when trying to mark {case_number} as reviewed")
        return RedirectResponse(
            url="/review/hcpa-failures?error=Database+is+locked.+Please+try+again+later.",
            status_code=303
        )
    except DatabaseUnavailableError as e:
        logger.error(f"Database unavailable when marking {case_number} as reviewed: {e}")
        return RedirectResponse(
            url="/review/hcpa-failures?error=Database+unavailable.+Please+check+the+logs.",
            status_code=303
        )
    except Exception as e:
        logger.error(f"Error marking {case_number} as reviewed: {e}")
        return RedirectResponse(
            url=f"/review/hcpa-failures?error=Error:+{type(e).__name__}",
            status_code=303
        )
