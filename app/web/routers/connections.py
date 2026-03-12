"""Connections graph — entity/person/property relationship explorer.

Provides a full-page interactive graph where users search for Sunbiz entities
and explore connections to officers, properties, and related entities through
a D3.js force-directed layout.

API endpoints:
  GET /connections              — renders the page
  GET /api/connections/search   — fuzzy entity name search (pg_trgm)
  GET /api/connections/entity/  — entity officers + registered addresses
  GET /api/connections/person   — person's entities + owned properties
  GET /api/connections/property — property owners + registered entities
"""

from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from loguru import logger
from sqlalchemy import text as sa_text

router = APIRouter()


def _pg_engine():
    from sunbiz.db import get_engine, resolve_pg_dsn

    return get_engine(resolve_pg_dsn())


def _age_years(filed_date) -> int | None:
    if filed_date is None:
        return None
    if isinstance(filed_date, str):
        try:
            filed_date = dt.date.fromisoformat(filed_date)
        except ValueError:
            return None
    today = dt.datetime.now(tz=dt.UTC).date()
    return today.year - filed_date.year - (
        (today.month, today.day) < (filed_date.month, filed_date.day)
    )


def _search_entities(query: str, limit: int = 10) -> list[dict]:
    """Fuzzy search sunbiz_entity_filings by entity_name using pg_trgm."""
    if not query or len(query) < 3:
        return []

    engine = _pg_engine()
    sql = sa_text("""
        SELECT doc_number, entity_name, status, filing_type, filed_date,
               similarity(entity_name, :q) AS sim
        FROM sunbiz_entity_filings
        WHERE entity_name % :q
        ORDER BY sim DESC
        LIMIT :lim
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"q": query.upper(), "lim": limit}).mappings().all()
    return [
        {
            "doc_number": r["doc_number"],
            "entity_name": r["entity_name"],
            "status": r["status"],
            "filing_type": r["filing_type"],
            "filed_date": str(r["filed_date"]) if r["filed_date"] else None,
            "age_years": _age_years(r["filed_date"]),
            "similarity": round(float(r["sim"]), 3),
        }
        for r in rows
    ]


# ---- Page route ----


@router.get("/connections", response_class=HTMLResponse)
async def connections_page(request: Request):
    from app.web.template_filters import get_templates

    templates = get_templates()
    return templates.TemplateResponse("connections.html", {"request": request})


# ---- API routes ----


@router.get("/api/connections/search")
async def api_search(q: str = Query("", min_length=0)):
    try:
        results = _search_entities(q)
    except Exception as exc:
        logger.error("connections search error: {}", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})
    return {"results": results}
