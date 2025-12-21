from fastapi import APIRouter
from fastapi.responses import JSONResponse
from loguru import logger

from app.web.database import (
    get_auction_map_points,
    check_database_health,
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
                "auction_date": r.get("auction_date").isoformat() if r.get("auction_date") else None,
                "final_judgment_amount": r.get("final_judgment_amount"),
                "url": f"/property/{r.get('case_number')}" if r.get("case_number") else "#",
            })
        except Exception as e:
            # Skip malformed records but log them
            logger.warning(f"Skipping malformed map record: {e}")
            continue

    return JSONResponse({"features": features})


@router.get("/health")
async def api_health():
    """API health check with database status."""
    db_status = check_database_health()
    return JSONResponse({
        "status": "ok" if db_status["available"] else "degraded",
        "database": db_status
    })
