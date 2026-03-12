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


def _normalize_address(addr: str | None) -> str:
    """Normalize address for fuzzy comparison."""
    if not addr:
        return ""
    import re
    addr = addr.upper().strip()
    # Common abbreviations
    for full, abbr in [("STREET", "ST"), ("AVENUE", "AVE"), ("DRIVE", "DR"),
                       ("BOULEVARD", "BLVD"), ("ROAD", "RD"), ("LANE", "LN"),
                       ("COURT", "CT"), ("PLACE", "PL"), ("CIRCLE", "CIR")]:
        addr = re.sub(rf"\b{full}\b", abbr, addr)
        addr = re.sub(rf"\b{abbr}\.\B", abbr, addr)
    addr = re.sub(r"[.,#]", "", addr)
    addr = re.sub(r"\s+", " ", addr)
    return addr.strip()


def _expand_entity(doc_number: str) -> dict | None:
    """Get entity details, officers, and address-matched properties."""
    engine = _pg_engine()
    with engine.connect() as conn:
        # Entity metadata
        ent = conn.execute(sa_text("""
            SELECT doc_number, entity_name, status, filing_type, filed_date,
                   principal_address1, principal_city, principal_state, principal_zip,
                   mailing_address1, mailing_city, mailing_state, mailing_zip
            FROM sunbiz_entity_filings
            WHERE doc_number = :dn
        """), {"dn": doc_number}).mappings().first()
        if not ent:
            return None

        # Officers/parties
        parties = conn.execute(sa_text("""
            SELECT party_name, party_role, party_title
            FROM sunbiz_entity_parties
            WHERE doc_number = :dn AND dataset_type = :ds
        """), {"dn": doc_number, "ds": ent["filing_type"][:3].lower() if ent["filing_type"] else "cor"}).mappings().all()

        # Also try without dataset_type filter (covers mismatches)
        if not parties:
            parties = conn.execute(sa_text("""
                SELECT party_name, party_role, party_title
                FROM sunbiz_entity_parties
                WHERE doc_number = :dn
            """), {"dn": doc_number}).mappings().all()

        # Match principal/mailing address to properties
        addresses: list[dict] = []
        for addr_type, addr_col, city_col in [
            ("principal_address", "principal_address1", "principal_city"),
            ("mailing_address", "mailing_address1", "mailing_city"),
        ]:
            raw_addr = ent[addr_col]
            raw_city = ent[city_col]
            if not raw_addr:
                continue
            norm = _normalize_address(raw_addr)
            if len(norm) < 5:
                continue
            props = conn.execute(sa_text("""
                SELECT bp.folio, bp.property_address, bp.owner_name, bp.market_value,
                       EXISTS(SELECT 1 FROM foreclosures f WHERE f.strap = bp.strap AND f.archived_at IS NULL) AS in_foreclosure
                FROM hcpa_bulk_parcels bp
                WHERE UPPER(bp.property_address) LIKE :addr_pattern
                  AND UPPER(bp.city) = :city
                LIMIT 5
            """), {
                "addr_pattern": norm.split()[0] + "%" if norm else "%",
                "city": (raw_city or "").upper().strip(),
            }).mappings().all()
            for p in props:
                # Verify similarity of full address
                p_norm = _normalize_address(p["property_address"])
                if not p_norm or norm[:10] != p_norm[:10]:
                    continue
                addresses.append({
                    "folio": p["folio"],
                    "address": p["property_address"],
                    "owner_name": p["owner_name"],
                    "market_value": float(p["market_value"]) if p["market_value"] else None,
                    "in_foreclosure": bool(p["in_foreclosure"]),
                    "match_type": addr_type,
                })

    return {
        "entity": {
            "doc_number": ent["doc_number"],
            "entity_name": ent["entity_name"],
            "status": ent["status"],
            "filing_type": ent["filing_type"],
            "filed_date": str(ent["filed_date"]) if ent["filed_date"] else None,
            "age_years": _age_years(ent["filed_date"]),
        },
        "parties": [
            {"party_name": p["party_name"], "party_role": p["party_role"], "party_title": p["party_title"]}
            for p in parties
        ],
        "addresses": addresses,
    }


@router.get("/api/connections/entity/{doc_number}")
async def api_entity(doc_number: str):
    try:
        result = _expand_entity(doc_number)
    except Exception as exc:
        logger.error("connections entity error: {}", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})
    if result is None:
        return JSONResponse(status_code=404, content={"error": "Entity not found"})
    return result


def _expand_person(name: str) -> dict:
    """Get all entities and properties for a person by fuzzy name match."""
    if not name or len(name) < 3:
        return {"entities": [], "properties": []}

    engine = _pg_engine()
    with engine.connect() as conn:
        # Find entities where this person is a party
        entities = conn.execute(sa_text("""
            SELECT DISTINCT ON (f.doc_number)
                   f.doc_number, f.entity_name, p.party_role, f.status,
                   f.filing_type, f.filed_date
            FROM sunbiz_entity_parties p
            JOIN sunbiz_entity_filings f
              ON f.doc_number = p.doc_number AND f.dataset_type = p.dataset_type
            WHERE p.party_name % :name
            ORDER BY f.doc_number, similarity(p.party_name, :name) DESC
            LIMIT 20
        """), {"name": name.upper()}).mappings().all()

        # Find properties owned by this person
        properties = conn.execute(sa_text("""
            SELECT bp.folio, bp.property_address, bp.owner_name, bp.market_value,
                   EXISTS(SELECT 1 FROM foreclosures f WHERE f.strap = bp.strap AND f.archived_at IS NULL) AS in_foreclosure
            FROM hcpa_bulk_parcels bp
            WHERE bp.owner_name % :name
            ORDER BY similarity(bp.owner_name, :name) DESC
            LIMIT 10
        """), {"name": name.upper()}).mappings().all()

        # Also check allsales for historical ownership
        sales_props = conn.execute(sa_text("""
            SELECT DISTINCT ON (a.folio)
                   a.folio, bp.property_address, bp.owner_name, bp.market_value,
                   EXISTS(SELECT 1 FROM foreclosures f WHERE f.strap = bp.strap AND f.archived_at IS NULL) AS in_foreclosure
            FROM hcpa_allsales a
            JOIN hcpa_bulk_parcels bp ON bp.folio = a.folio
            WHERE (a.grantor % :name OR a.grantee % :name)
              AND a.folio NOT IN (SELECT folio FROM hcpa_bulk_parcels WHERE owner_name % :name)
            ORDER BY a.folio, a.sale_date DESC NULLS LAST
            LIMIT 10
        """), {"name": name.upper()}).mappings().all()

    all_properties = []
    seen_folios: set[str] = set()
    for p in list(properties) + list(sales_props):
        folio = p["folio"]
        if folio in seen_folios:
            continue
        seen_folios.add(folio)
        all_properties.append({
            "folio": folio,
            "address": p["property_address"],
            "owner_name": p["owner_name"],
            "market_value": float(p["market_value"]) if p["market_value"] else None,
            "in_foreclosure": bool(p["in_foreclosure"]),
        })

    return {
        "entities": [
            {
                "doc_number": e["doc_number"],
                "entity_name": e["entity_name"],
                "party_role": e["party_role"],
                "status": e["status"],
                "filing_type": e["filing_type"],
                "filed_date": str(e["filed_date"]) if e["filed_date"] else None,
                "age_years": _age_years(e["filed_date"]),
            }
            for e in entities
        ],
        "properties": all_properties,
    }


def _expand_property(folio: str) -> dict | None:
    """Get property details, owners from sales history, and entities registered at address."""
    engine = _pg_engine()
    with engine.connect() as conn:
        # Property metadata
        prop = conn.execute(sa_text("""
            SELECT bp.folio, bp.property_address, bp.city, bp.owner_name, bp.market_value, bp.strap,
                   EXISTS(SELECT 1 FROM foreclosures f WHERE f.strap = bp.strap AND f.archived_at IS NULL) AS in_foreclosure
            FROM hcpa_bulk_parcels bp
            WHERE bp.folio = :folio
        """), {"folio": folio}).mappings().first()
        if not prop:
            return None

        # Sales history (owners)
        owners = conn.execute(sa_text("""
            SELECT grantee AS name, 'Grantee' AS role, sale_date, sale_amount
            FROM hcpa_allsales
            WHERE folio = :folio
            ORDER BY sale_date DESC NULLS LAST
            LIMIT 20
        """), {"folio": folio}).mappings().all()

        # Also add grantors
        grantors = conn.execute(sa_text("""
            SELECT DISTINCT grantor AS name, 'Grantor' AS role, sale_date, sale_amount
            FROM hcpa_allsales
            WHERE folio = :folio AND grantor IS NOT NULL
            ORDER BY sale_date DESC NULLS LAST
            LIMIT 20
        """), {"folio": folio}).mappings().all()

        all_owners = [
            {
                "name": o["name"],
                "role": o["role"],
                "sale_date": str(o["sale_date"]) if o["sale_date"] else None,
                "sale_amount": float(o["sale_amount"]) if o["sale_amount"] else None,
            }
            for o in list(owners) + list(grantors) if o["name"]
        ]

        # Entities registered at this address
        addr = prop["property_address"]
        city = prop["city"]
        registered_entities: list[dict] = []
        if addr:
            norm = _normalize_address(addr)
            if len(norm) >= 5:
                first_token = norm.split()[0]
                ents = conn.execute(sa_text("""
                    SELECT doc_number, entity_name, status, filing_type, filed_date,
                           CASE
                             WHEN UPPER(principal_address1) LIKE :pat THEN 'principal_address'
                             ELSE 'mailing_address'
                           END AS match_type
                    FROM sunbiz_entity_filings
                    WHERE (UPPER(principal_address1) LIKE :pat AND UPPER(principal_city) = :city)
                       OR (UPPER(mailing_address1) LIKE :pat AND UPPER(mailing_city) = :city)
                    LIMIT 20
                """), {
                    "pat": first_token + "%",
                    "city": (city or "").upper().strip(),
                }).mappings().all()
                for e in ents:
                    registered_entities.append({
                        "doc_number": e["doc_number"],
                        "entity_name": e["entity_name"],
                        "status": e["status"],
                        "filing_type": e["filing_type"],
                        "filed_date": str(e["filed_date"]) if e["filed_date"] else None,
                        "age_years": _age_years(e["filed_date"]),
                        "match_type": e["match_type"],
                    })

    return {
        "property": {
            "folio": prop["folio"],
            "address": prop["property_address"],
            "owner_name": prop["owner_name"],
            "market_value": float(prop["market_value"]) if prop["market_value"] else None,
            "in_foreclosure": bool(prop["in_foreclosure"]),
        },
        "owners": all_owners,
        "registered_entities": registered_entities,
    }


@router.get("/api/connections/person")
async def api_person(name: str = Query("", min_length=0)):
    try:
        result = _expand_person(name)
    except Exception as exc:
        logger.error("connections person error: {}", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})
    return result


@router.get("/api/connections/property/{folio}")
async def api_property(folio: str):
    try:
        result = _expand_property(folio)
    except Exception as exc:
        logger.error("connections property error: {}", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})
    if result is None:
        return JSONResponse(status_code=404, content={"error": "Property not found"})
    return result
