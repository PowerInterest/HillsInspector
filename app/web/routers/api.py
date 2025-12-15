from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.web.database import get_auction_map_points

router = APIRouter(tags=["api"])


@router.get("/map-auctions")
async def map_auctions():
    rows = get_auction_map_points()
    features = []
    for r in rows:
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
    return JSONResponse({"features": features})
