"""
Dashboard routes - main auction list view.
"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from typing import Optional
from datetime import date

from app.web.database import (
    get_upcoming_auctions,
    get_upcoming_auctions_with_enrichments,
    get_auction_count,
    get_dashboard_stats,
    get_auctions_by_date
)

router = APIRouter()

# Templates
TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@router.get("/", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    auction_type: Optional[str] = "FORECLOSURE",  # Default to foreclosures only
    sort_by: str = "auction_date",
    sort_order: str = "asc",
    page: int = 1,
    per_page: int = 24,  # 24 cards = 4x6 or 3x8 grid
    view: str = "grid"  # grid or table
):
    """
    Main dashboard showing upcoming foreclosure auctions.
    Card-based grid view with enrichment badges.
    """
    offset = (page - 1) * per_page

    # Get auctions with enrichment data for grid view
    if view == "grid":
        auctions = get_upcoming_auctions_with_enrichments(
            days_ahead=60,
            auction_type=auction_type,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=per_page,
            offset=offset
        )
    else:
        auctions = get_upcoming_auctions(
            days_ahead=60,
            auction_type=auction_type,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=per_page,
            offset=offset
        )

    # Get total count for pagination
    total = get_auction_count(days_ahead=60, auction_type=auction_type)
    total_pages = (total + per_page - 1) // per_page

    # Get stats
    stats = get_dashboard_stats()

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "auctions": auctions,
            "stats": stats,
            "view": view,
            "filters": {
                "auction_type": auction_type,
                "sort_by": sort_by,
                "sort_order": sort_order
            },
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "total_pages": total_pages
            }
        }
    )


@router.get("/auctions", response_class=HTMLResponse)
async def auctions_list(
    request: Request,
    auction_type: Optional[str] = "FORECLOSURE",
    sort_by: str = "auction_date",
    sort_order: str = "asc",
    page: int = 1,
    per_page: int = 24,
    view: str = "grid"
):
    """
    HTMX partial - returns grid or table view.
    Used for filtering/sorting without full page reload.
    """
    offset = (page - 1) * per_page

    # Get auctions with or without enrichments based on view
    if view == "grid":
        auctions = get_upcoming_auctions_with_enrichments(
            days_ahead=60,
            auction_type=auction_type,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=per_page,
            offset=offset
        )
    else:
        auctions = get_upcoming_auctions(
            days_ahead=60,
            auction_type=auction_type,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=per_page,
            offset=offset
        )

    total = get_auction_count(days_ahead=60, auction_type=auction_type)
    total_pages = (total + per_page - 1) // per_page

    # Check if this is an HTMX request
    is_htmx = request.headers.get("HX-Request") == "true"

    template_name = "partials/property_grid.html" if view == "grid" else "partials/auction_table.html"

    if is_htmx:
        return templates.TemplateResponse(
            template_name,
            {
                "request": request,
                "auctions": auctions,
                "view": view,
                "filters": {
                    "auction_type": auction_type,
                    "sort_by": sort_by,
                    "sort_order": sort_order
                },
                "pagination": {
                    "page": page,
                    "per_page": per_page,
                    "total": total,
                    "total_pages": total_pages
                }
            }
        )

    # Full page response
    stats = get_dashboard_stats()
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "auctions": auctions,
            "stats": stats,
            "view": view,
            "filters": {
                "auction_type": auction_type,
                "sort_by": sort_by,
                "sort_order": sort_order
            },
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "total_pages": total_pages
            }
        }
    )


@router.get("/auctions/{auction_date}", response_class=HTMLResponse)
async def auctions_by_date(
    request: Request,
    auction_date: date
):
    """Get all auctions for a specific date."""
    auctions = get_auctions_by_date(auction_date)

    return templates.TemplateResponse(
        "auctions_date.html",
        {
            "request": request,
            "auctions": auctions,
            "auction_date": auction_date
        }
    )
