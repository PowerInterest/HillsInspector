from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from loguru import logger

from app.web.database import (
    get_auction_map_points,
    check_database_health,
    search_properties,
    DatabaseLockedError,
    DatabaseUnavailableError,
)

router = APIRouter(tags=["api"])


@router.get("/map-auctions")
async def map_auctions():
    """Get auction locations for map display."""
    try:
        rows = get_auction_map_points()
    except (DatabaseLockedError, DatabaseUnavailableError):
        # Let global handler deal with these
        raise
    except Exception as e:
        logger.error(f"Error fetching map auctions: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Failed to fetch map data", "features": []}
        )

    features = []
    for r in rows:
        try:
            lat = r.get("latitude")
            lon = r.get("longitude")
            address = r.get("property_address") or r.get("folio") or "Unknown address"
            features.append({
                "lat": lat,
                "lon": lon,
                "address": address,
                "case_number": r.get("case_number"),
                "auction_date": str(r.get("auction_date")) if r.get("auction_date") else None,
                "final_judgment_amount": r.get("final_judgment_amount"),
                "url": f"/property/{r.get('case_number')}" if r.get("case_number") else "#",
            })
        except Exception as e:
            # Skip malformed records but log them
            logger.warning(f"Skipping malformed map record: {e}")
            continue

    return JSONResponse({"features": features})


@router.get("/search")
async def api_search(request: Request, q: str = ""):
    """Search auctions by case_number, address, or owner_name. Returns HTML for HTMX dropdown."""
    if not q or len(q) < 2:
        return HTMLResponse("")

    try:
        results = search_properties(q, limit=10)
    except (DatabaseLockedError, DatabaseUnavailableError):
        raise
    except Exception as e:
        logger.error(f"Search error: {e}")
        return HTMLResponse('<div class="search-item">Search error</div>')

    if not results:
        return HTMLResponse('<div class="search-item muted">No results found</div>')

    html_items = []
    for r in results:
        folio = r.get("folio") or r.get("case_number") or ""
        address = r.get("property_address") or "No Address"
        case = r.get("case_number") or ""
        owner = r.get("owner_name") or ""
        date_str = r.get("auction_date") or ""
        html_items.append(
            f'<a href="/property/{folio}" class="search-item">'
            f'<strong>{address}</strong>'
            f'<span class="search-meta">{case} | {owner} | {date_str}</span>'
            f'</a>'
        )
    return HTMLResponse("\n".join(html_items))


@router.get("/health")
async def api_health():
    """API health check with database status."""
    db_status = check_database_health()
    return JSONResponse({
        "status": "ok" if db_status["available"] else "degraded",
        "database": db_status
    })
