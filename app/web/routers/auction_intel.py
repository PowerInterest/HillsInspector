"""
Auction Intelligence routes — tactical dashboard for the next auction date.
"""

from datetime import date

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from app.web.pg_web import get_auction_intel_for_date
from app.web.template_filters import get_templates

router = APIRouter(prefix="/auction-intel", tags=["auction-intel"])

templates = get_templates()


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def auction_intel_dashboard(request: Request):
    """
    Main auction intelligence dashboard.

    Automatically targets the next auction date based on the 2 PM EST rule.
    """
    target_date, auctions, stats = get_auction_intel_for_date()

    return templates.TemplateResponse(
        "auction_intel.html",
        {
            "request": request,
            "target_date": target_date,
            "auctions": auctions,
            "stats": stats,
        },
    )


@router.get("/{target_date}", response_class=HTMLResponse)
async def auction_intel_by_date(request: Request, target_date: date):
    """View auction intelligence for a specific date."""
    target_date, auctions, stats = get_auction_intel_for_date(target_date)

    return templates.TemplateResponse(
        "auction_intel.html",
        {
            "request": request,
            "target_date": target_date,
            "auctions": auctions,
            "stats": stats,
        },
    )
