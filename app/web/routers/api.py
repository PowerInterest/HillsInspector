from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from loguru import logger

from app.web.pg_web import (
    get_auction_map_points,
    check_database_health,
    search_properties,
)
from app.web.pg_database import get_pg_queries

router = APIRouter(tags=["api"])


@router.get("/map-auctions")
async def map_auctions():
    """Get auction locations for map display."""
    try:
        rows = get_auction_map_points()
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
                "url": (
                    f"/property/{r.get('folio') or r.get('case_number')}"
                    if (r.get("folio") or r.get("case_number"))
                    else "#"
                ),
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


# -------------------------------------------------------------------------
# Fuzzy Search (PG-backed)
# -------------------------------------------------------------------------

@router.get("/search-fuzzy")
async def search_fuzzy(request: Request, q: str = "", limit: int = 25):
    """Fuzzy property search using PG trigram matching.

    Returns HTML for HTMX dropdown or JSON depending on Accept header.
    """
    if not q or len(q) < 2:
        if _wants_json(request):
            return JSONResponse({"results": []})
        return HTMLResponse("")

    pg = get_pg_queries()
    if not pg.available:
        # Fallback to broader PG-backed LIKE search
        try:
            results = search_properties(q, limit=limit)
        except Exception as e:
            logger.error(f"Fuzzy search fallback error: {e}")
            results = []
        if _wants_json(request):
            return JSONResponse({"results": results, "source": "pg_like_fallback"})
        return _render_search_results_html(results)

    results = pg.search_properties_fuzzy(q, limit=limit)

    if _wants_json(request):
        return JSONResponse({"results": results, "source": "pg_fuzzy"})

    return _render_search_results_html(results)


@router.get("/resolve-name")
async def resolve_name(q: str = "", threshold: float = 0.3):
    """Resolve a defendant/owner name to property folios via PG fuzzy matching."""
    if not q or len(q) < 2:
        return JSONResponse({"results": []})

    pg = get_pg_queries()
    if not pg.available:
        return JSONResponse(
            {"results": [], "error": "PostgreSQL unavailable"},
            status_code=503,
        )

    results = pg.resolve_by_name(q, threshold=threshold)
    return JSONResponse({"results": results})


# -------------------------------------------------------------------------
# Comparable Sales (PG-backed)
# -------------------------------------------------------------------------

@router.get("/property/{folio}/comparables")
async def get_comparables(folio: str, years: int = 3):
    """Comparable sales for a property from PG hcpa_allsales."""
    pg = get_pg_queries()
    if not pg.available:
        return JSONResponse(
            {"comparables": [], "error": "PostgreSQL unavailable"},
            status_code=503,
        )

    comps = pg.get_comparable_sales(folio, years=years)
    # Serialize dates
    for c in comps:
        if c.get("sale_date"):
            c["sale_date"] = str(c["sale_date"])
    return JSONResponse({"comparables": comps})


@router.get("/property/{folio}/pg-sales-history")
async def get_pg_sales_history(folio: str):
    """Full sales chain from PG hcpa_allsales (more complete than SQLite)."""
    pg = get_pg_queries()
    if not pg.available:
        return JSONResponse(
            {"sales": [], "error": "PostgreSQL unavailable"},
            status_code=503,
        )

    sales = pg.get_sales_history(folio)
    for s in sales:
        if s.get("sale_date"):
            s["sale_date"] = str(s["sale_date"])
    return JSONResponse({"sales": sales})


@router.get("/property/{folio}/subdivision")
async def get_subdivision(folio: str):
    """Subdivision info for a property."""
    pg = get_pg_queries()
    if not pg.available:
        return JSONResponse(
            {"subdivision": None, "error": "PostgreSQL unavailable"},
            status_code=503,
        )

    info = pg.get_subdivision_info(folio)
    return JSONResponse({"subdivision": info})


@router.get("/property/{folio}/multi-unit")
async def get_multi_unit(folio: str):
    """Check if property is multi-unit and get sibling units."""
    pg = get_pg_queries()
    if not pg.available:
        return JSONResponse(
            {"is_multi_unit": False, "units": [], "error": "PostgreSQL unavailable"},
            status_code=503,
        )

    multi = pg.is_multi_unit(folio)
    units = pg.get_units_for_property(folio) if multi else []
    return JSONResponse({
        "is_multi_unit": multi is not None,
        "multi_unit_info": multi,
        "units": units,
    })


# -------------------------------------------------------------------------
# County-Wide Analytics (PG-backed)
# -------------------------------------------------------------------------

@router.get("/analytics/sales-volume")
async def sales_volume(zip_code: str | None = None, months: int = 24):
    """Monthly sales volume chart data from PG."""
    pg = get_pg_queries()
    if not pg.available:
        return JSONResponse(
            {"data": [], "error": "PostgreSQL unavailable"},
            status_code=503,
        )

    data = pg.get_sales_volume_by_month(
        folio=None,
        months=months,
        zip_code=zip_code,
    )
    for d in data:
        if d.get("median_price") is not None:
            d["median_price"] = float(d["median_price"])
        if d.get("total_volume") is not None:
            d["total_volume"] = float(d["total_volume"])
    return JSONResponse({"data": data})


@router.get("/analytics/value-distribution")
async def value_distribution(zip_code: str | None = None):
    """Property value distribution histogram data from PG."""
    pg = get_pg_queries()
    if not pg.available:
        return JSONResponse(
            {"data": [], "error": "PostgreSQL unavailable"},
            status_code=503,
        )

    data = pg.get_property_value_distribution(zip_code=zip_code)
    return JSONResponse({"data": data})


@router.get("/analytics/property-stats-by-zip")
async def property_stats_by_zip():
    """Property distribution stats by zip code from PG."""
    pg = get_pg_queries()
    if not pg.available:
        return JSONResponse(
            {"data": [], "error": "PostgreSQL unavailable"},
            status_code=503,
        )

    data = pg.get_property_stats_by_zip()
    for d in data:
        for key in ("avg_just_value", "median_just_value", "total_just_value"):
            if d.get(key) is not None:
                d[key] = float(d[key])
    return JSONResponse({"data": data})


@router.get("/analytics/foreclosure-deeds")
async def foreclosure_deed_stats(months: int = 12):
    """Foreclosure and tax deed volume by month from PG."""
    pg = get_pg_queries()
    if not pg.available:
        return JSONResponse(
            {"data": [], "error": "PostgreSQL unavailable"},
            status_code=503,
        )

    data = pg.get_foreclosure_deed_stats(months=months)
    for d in data:
        if d.get("avg_fd_amount") is not None:
            d["avg_fd_amount"] = float(d["avg_fd_amount"])
    return JSONResponse({"data": data})


# -------------------------------------------------------------------------
# Health Check
# -------------------------------------------------------------------------

@router.get("/health")
async def api_health():
    """API health check with database status."""
    db_status = check_database_health()
    pg = get_pg_queries()
    return JSONResponse({
        "status": "ok" if db_status["available"] else "degraded",
        "database": db_status,
        "postgres": {
            "available": pg.available,
        }
    })


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def _wants_json(request: Request) -> bool:
    """Check if client prefers JSON over HTML."""
    accept = request.headers.get("Accept", "")
    return "application/json" in accept


def _render_search_results_html(results: list[dict]) -> HTMLResponse:
    """Render search results as HTML dropdown items for HTMX."""
    if not results:
        return HTMLResponse('<div class="search-item muted">No results found</div>')

    html_items = []
    for r in results:
        folio = r.get("strap") or r.get("folio") or ""
        address = r.get("property_address") or "No Address"
        owner = r.get("owner_name") or ""
        city = r.get("city") or ""
        score = r.get("similarity_score") or r.get("match_score")
        score_str = f" ({score:.0%})" if score else ""

        html_items.append(
            f'<a href="/property/{folio}" class="search-item">'
            f'<strong>{address}</strong>'
            f'<span class="search-meta">{owner} | {city}{score_str}</span>'
            f'</a>'
        )
    return HTMLResponse("\n".join(html_items))
