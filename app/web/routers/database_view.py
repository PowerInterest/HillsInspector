"""
Database search page — dual-mode person and property search across all PG tables.

Replaces the former CloudBeaver embed with a powerful in-app search interface.
The page provides two HTMX-driven search boxes:

  1. **Person Search** — fuzzy name matching across 10 table categories (civil cases,
     criminal records, garnishments, property ownership, sales, UCC liens, corporate
     roles, official records, foreclosures, trust accounts).  Uses pg_trgm similarity()
     on tables with GIN trigram indexes and ILIKE fallback elsewhere.

  2. **Property Search** — identifier-based lookup (folio, strap, address, or case
     number) across 9 table categories (parcels, tax, foreclosures, market data,
     sales, encumbrances, title chain, permits, clerk events).

Architecture:
  - ``GET /database`` renders the main page with both search boxes.
  - ``POST /database/person-search`` returns an HTMX partial with categorized results.
  - ``POST /database/property-search`` returns an HTMX partial with categorized results.
  - Each individual category query is wrapped in try/except so a missing or altered
    table never breaks the entire search.
  - Property search resolves a small scope of related folios / straps / addresses
    first so case-number and address searches can fan back into permit, title,
    and market tables without requiring the user to know the exact storage key.
  - All SQL is parameterized (no string interpolation).
"""

from __future__ import annotations

from contextlib import suppress
from typing import Iterable

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse
from loguru import logger
from sqlalchemy import text as sa_text

router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pg_engine():
    """Deferred import to avoid startup failures if PG is unavailable."""
    from sunbiz.db import get_engine, resolve_pg_dsn
    return get_engine(resolve_pg_dsn())


def _dedupe_nonempty(values: Iterable[object]) -> list[str]:
    """Keep non-empty text values in original order."""
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _digits_only(value: str) -> str:
    """Return only the digits from an identifier-like value."""
    return "".join(ch for ch in value if ch.isdigit())


def _add_in_clause(
    clauses: list[str],
    params: dict[str, object],
    column_sql: str,
    values: Iterable[object],
    prefix: str,
) -> None:
    """Append a parameterized ``IN (...)`` clause if values are present."""
    cleaned = _dedupe_nonempty(values)
    if not cleaned:
        return

    placeholders: list[str] = []
    for idx, value in enumerate(cleaned):
        key = f"{prefix}_{idx}"
        params[key] = value
        placeholders.append(f":{key}")

    clauses.append(f"{column_sql} IN ({', '.join(placeholders)})")


def _add_upper_in_clause(
    clauses: list[str],
    params: dict[str, object],
    column_sql: str,
    values: Iterable[object],
    prefix: str,
) -> None:
    """Append an uppercase-normalized ``IN (...)`` clause if values exist."""
    cleaned = _dedupe_nonempty(str(value).upper() for value in values if value)
    if not cleaned:
        return

    placeholders: list[str] = []
    for idx, value in enumerate(cleaned):
        key = f"{prefix}_{idx}"
        params[key] = value
        placeholders.append(f":{key}")

    clauses.append(
        f"UPPER(COALESCE({column_sql}, '')) IN ({', '.join(placeholders)})"
    )


def _parse_person_name(raw: str) -> tuple[str, str]:
    """Parse a search string into (last_name, first_name).

    Rules:
      - Contains comma  -> "LAST, FIRST ..."
      - No comma        -> last word is last name, rest is first name
      - Single word     -> last name only, first is empty string
    """
    cleaned = raw.strip()
    if not cleaned:
        return ("", "")

    if "," in cleaned:
        parts = [p.strip() for p in cleaned.split(",", 1)]
        last = parts[0]
        first = parts[1] if len(parts) > 1 else ""
    else:
        words = cleaned.split()
        if len(words) == 1:
            last = words[0]
            first = ""
        else:
            last = words[-1]
            first = " ".join(words[:-1])

    return (last, first)


def _safe_query(conn, label: str, sql: str, params: dict) -> list[dict]:
    """Execute a query, returning list of dicts.  Logs and swallows errors.

    On failure, rolls back the implicit transaction so subsequent queries on
    the same connection are not poisoned by PG's ``InFailedSqlTransaction``.
    """
    try:
        rows = conn.execute(sa_text(sql), params).mappings().all()
        return [dict(r) for r in rows]
    except Exception:
        logger.opt(exception=True).warning(
            "Database search '{}' query failed",
            label,
        )
        with suppress(Exception):
            conn.rollback()
        return []


def _resolve_property_targets(conn, query: str) -> dict[str, list[str]]:
    """Resolve related property identifiers so downstream searches can fan out."""
    rows = _safe_query(
        conn,
        "Property target resolution",
        """
        SELECT DISTINCT
               folio,
               strap,
               property_address,
               case_number
        FROM (
            SELECT f.folio,
                   f.strap,
                   f.property_address,
                   f.case_number_raw AS case_number
            FROM foreclosures f
            WHERE f.folio = :q
               OR f.strap = :q
               OR f.case_number_raw = :q
               OR f.case_number_norm = :q
               OR f.property_address ILIKE '%' || :q || '%'

            UNION ALL

            SELECT fh.folio,
                   fh.strap,
                   fh.property_address,
                   fh.case_number_raw AS case_number
            FROM foreclosures_history fh
            WHERE fh.folio = :q
               OR fh.strap = :q
               OR fh.case_number_raw = :q
               OR fh.case_number_norm = :q
               OR fh.property_address ILIKE '%' || :q || '%'

            UNION ALL

            SELECT hp.folio,
                   hp.strap,
                   hp.property_address,
                   NULL AS case_number
            FROM hcpa_bulk_parcels hp
            WHERE hp.folio = :q
               OR hp.strap = :q
               OR hp.property_address ILIKE '%' || :q || '%'

            UNION ALL

            SELECT dp.folio,
                   dp.strap,
                   dp.property_address,
                   NULL AS case_number
            FROM dor_nal_parcels dp
            WHERE dp.folio = :q
               OR dp.strap = :q
               OR dp.property_address ILIKE '%' || :q || '%'

            UNION ALL

            SELECT pm.folio,
                   pm.strap,
                   hp.property_address,
                   pm.case_number
            FROM property_market pm
            LEFT JOIN hcpa_bulk_parcels hp ON hp.folio = pm.folio
            WHERE pm.folio = :q
               OR pm.strap = :q
               OR pm.case_number = :q
               OR hp.property_address ILIKE '%' || :q || '%'
        ) scoped
        LIMIT 100
        """,
        {"q": query},
    )

    folios = _dedupe_nonempty(row.get("folio") for row in rows)
    straps = _dedupe_nonempty(row.get("strap") for row in rows)
    addresses = _dedupe_nonempty(row.get("property_address") for row in rows)
    case_numbers = _dedupe_nonempty(row.get("case_number") for row in rows)

    if any(ch.isalpha() for ch in query):
        addresses = _dedupe_nonempty([query, *addresses])

    return {
        "folios": folios,
        "folio_clean": _dedupe_nonempty(_digits_only(folio) for folio in folios),
        "straps": straps,
        "addresses": addresses,
        "case_numbers": case_numbers,
    }


# ---------------------------------------------------------------------------
# GET /database — main page
# ---------------------------------------------------------------------------

@router.get("/database", response_class=HTMLResponse)
async def database_page(request: Request):
    """Render the database search page with person and property search boxes."""
    from app.web.main import templates
    return templates.TemplateResponse("database.html", {"request": request})


# ---------------------------------------------------------------------------
# POST /database/person-search
# ---------------------------------------------------------------------------

@router.post("/database/person-search", response_class=HTMLResponse)
async def person_search(request: Request, name: str = Form("")):
    """Fuzzy person search across all PG people-related tables.

    Returns an HTMX partial (``partials/person_search_results.html``) with
    results organised into labelled sections.
    """
    from app.web.main import templates

    query = name.strip()
    if not query:
        return HTMLResponse(
            '<div class="search-empty">Enter a name to search.</div>'
        )

    last, first = _parse_person_name(query)
    if not last:
        return HTMLResponse(
            '<div class="search-empty">Could not parse a last name from the query.</div>'
        )

    sections: dict[str, list[dict]] = {}
    counts: dict[str, int] = {}

    engine = _pg_engine()
    with engine.connect() as conn:
        # 1. Civil Cases — clerk_civil_parties (trigram indexes on last_name)
        rows = _safe_query(conn, "Civil Cases", """
            SELECT DISTINCT ON (p.case_number, p.party_type, p.name)
                   cc.court_type, p.last_name, p.first_name, p.middle_name,
                   p.case_number, cc.ucn, cc.case_type, cc.filing_date AS date_filed,
                   cc.case_status AS current_status,
                   cc.status_date, p.party_type, p.akas,
                   p.address1, p.city, p.state, p.zip AS zip_code,
                   p.disposition_code, p.disposition_desc, p.disposition_date,
                   similarity(p.last_name, :last) AS score
            FROM clerk_civil_parties p
            JOIN clerk_civil_cases cc ON cc.case_number = p.case_number
            WHERE p.last_name % :last
              AND similarity(p.last_name, :last) > 0.4
              AND (:first = '' OR p.first_name IS NULL OR similarity(p.first_name, :first) > 0.25)
            ORDER BY p.case_number, p.party_type, p.name, score DESC
            LIMIT 200
        """, {"last": last, "first": first})
        sections["Civil Cases"] = rows
        counts["Civil Cases"] = len(rows)

        # 2. Criminal Records — clerk_criminal_name_index (trigram indexes)
        rows = _safe_query(conn, "Criminal Records", """
            SELECT court_type, last_name, first_name, middle_name,
                   case_number, ucn, case_type, date_filed, current_status,
                   charge_description, count_level_degree, statute_violation,
                   offense_date, disposition_desc, disposition_date,
                   sex_gender, race, date_of_birth, akas,
                   address1, city, state, zip_code,
                   similarity(last_name, :last) AS score
            FROM clerk_criminal_name_index
            WHERE last_name % :last
              AND similarity(last_name, :last) > 0.4
              AND (:first = '' OR first_name IS NULL OR similarity(first_name, :first) > 0.25)
            ORDER BY date_filed DESC NULLS LAST
            LIMIT 200
        """, {"last": last, "first": first})
        sections["Criminal Records"] = rows
        counts["Criminal Records"] = len(rows)

        # 3. Garnishments — clerk_garnishment_cases (ILIKE)
        rows = _safe_query(conn, "Garnishments", """
            SELECT case_number, plaintiff_name, defendant_name, garnishee_name,
                   filing_date, case_status_description, writ_issued_date,
                   address1, city, state, zip
            FROM clerk_garnishment_cases
            WHERE defendant_name ILIKE '%' || :name || '%'
               OR plaintiff_name ILIKE '%' || :name || '%'
            ORDER BY filing_date DESC NULLS LAST
            LIMIT 100
        """, {"name": query})
        sections["Garnishments"] = rows
        counts["Garnishments"] = len(rows)

        # 4. Properties Owned — hcpa_bulk_parcels (ILIKE)
        rows = _safe_query(conn, "Properties Owned", """
            SELECT folio, strap, owner_name, property_address, city, zip_code,
                   land_use_desc, year_built, beds, baths, heated_area,
                   just_value, market_value, last_sale_date, last_sale_price
            FROM hcpa_bulk_parcels
            WHERE owner_name ILIKE '%' || :name || '%'
            ORDER BY just_value DESC NULLS LAST
            LIMIT 100
        """, {"name": query})
        sections["Properties Owned"] = rows
        counts["Properties Owned"] = len(rows)

        # 5. Sales History — hcpa_allsales (ILIKE)
        rows = _safe_query(conn, "Sales History", """
            SELECT hs.folio,
                   hp.strap,
                   hs.grantor,
                   hs.grantee,
                   hs.sale_date,
                   hs.sale_amount,
                   hs.sale_type,
                   hs.or_book,
                   hs.or_page,
                   hs.doc_num
            FROM hcpa_allsales hs
            LEFT JOIN hcpa_bulk_parcels hp ON hp.folio = hs.folio
            WHERE grantor ILIKE '%' || :name || '%'
               OR grantee ILIKE '%' || :name || '%'
            ORDER BY hs.sale_date DESC NULLS LAST
            LIMIT 100
        """, {"name": query})
        sections["Sales History"] = rows
        counts["Sales History"] = len(rows)

        # 6. UCC Liens — sunbiz_flr_parties + sunbiz_flr_filings (ILIKE)
        rows = _safe_query(conn, "UCC Liens", """
            SELECT sp.name, sp.party_role, sf.doc_number, sf.filing_date,
                   sf.filing_status, sf.expiration_date,
                   sp.address1, sp.city, sp.state, sp.zip_code
            FROM sunbiz_flr_parties sp
            JOIN sunbiz_flr_filings sf ON sp.doc_number = sf.doc_number
            WHERE sp.name ILIKE '%' || :name || '%'
            ORDER BY sf.filing_date DESC NULLS LAST
            LIMIT 100
        """, {"name": query})
        sections["UCC Liens"] = rows
        counts["UCC Liens"] = len(rows)

        # 7. Corporate Roles — sunbiz_entity_parties + sunbiz_entity_filings (ILIKE)
        rows = _safe_query(conn, "Corporate Roles", """
            SELECT sep.party_name, sep.party_role, sep.party_title,
                   sef.entity_name, sef.doc_number, sef.status AS entity_status,
                   sep.address1, sep.city, sep.state, sep.zip_code
            FROM sunbiz_entity_parties sep
            JOIN sunbiz_entity_filings sef ON sep.doc_number = sef.doc_number
            WHERE sep.party_name ILIKE '%' || :name || '%'
            ORDER BY sep.party_name
            LIMIT 100
        """, {"name": query})
        sections["Corporate Roles"] = rows
        counts["Corporate Roles"] = len(rows)

        # 8. Official Records — ori_encumbrances (ILIKE)
        rows = _safe_query(conn, "Official Records", """
            SELECT recording_date, encumbrance_type::text, party1, party2,
                   amount, instrument_number, book, page,
                   folio, strap, survival_status
            FROM ori_encumbrances
            WHERE party1 ILIKE '%' || :name || '%'
               OR party2 ILIKE '%' || :name || '%'
            ORDER BY recording_date DESC NULLS LAST
            LIMIT 100
        """, {"name": query})
        sections["Official Records"] = rows
        counts["Official Records"] = len(rows)

        # 9. Foreclosures — foreclosures + foreclosures_history (ILIKE)
        rows = _safe_query(conn, "Foreclosures", """
            SELECT case_number_raw, auction_date, auction_type, auction_status,
                   folio, strap, property_address, owner_name, sold_to,
                   final_judgment_amount, winning_bid, market_value
            FROM foreclosures
            WHERE owner_name ILIKE '%' || :name || '%'
               OR sold_to ILIKE '%' || :name || '%'
            UNION ALL
            SELECT case_number_raw, auction_date, auction_type, auction_status,
                   folio, strap, property_address, owner_name, sold_to,
                   final_judgment_amount, winning_bid, market_value
            FROM foreclosures_history
            WHERE owner_name ILIKE '%' || :name || '%'
               OR sold_to ILIKE '%' || :name || '%'
            ORDER BY auction_date DESC NULLS LAST
            LIMIT 50
        """, {"name": query})
        sections["Foreclosures"] = rows
        counts["Foreclosures"] = len(rows)

        # 10. Trust Accounts — "TrustAccount" (ILIKE)
        rows = _safe_query(conn, "Trust Accounts", """
            SELECT case_number,
                   plaintiff_name AS party_name,
                   amount,
                   movement_type AS transaction_type,
                   report_date AS transaction_date,
                   counterparty_type AS description
            FROM "TrustAccount"
            WHERE plaintiff_name ILIKE '%' || :name || '%'
            ORDER BY report_date DESC NULLS LAST
            LIMIT 50
        """, {"name": query})
        sections["Trust Accounts"] = rows
        counts["Trust Accounts"] = len(rows)

    total = sum(counts.values())
    logger.info(f"Person search '{query}' -> {total} total results across {len(sections)} categories")

    return templates.TemplateResponse(
        "partials/person_search_results.html",
        {
            "request": request,
            "query": query,
            "sections": sections,
            "counts": counts,
            "total": total,
        },
    )


# ---------------------------------------------------------------------------
# POST /database/property-search
# ---------------------------------------------------------------------------

@router.post("/database/property-search", response_class=HTMLResponse)
async def property_search(request: Request, identifier: str = Form("")):
    """Property search by folio, strap, address, or case number.

    Returns an HTMX partial (``partials/property_search_results.html``) with
    results organised into labelled sections.
    """
    from app.web.main import templates

    query = identifier.strip()
    if not query:
        return HTMLResponse(
            '<div class="search-empty">Enter a folio, strap, address, or case number.</div>'
        )

    sections: dict[str, list[dict]] = {}
    counts: dict[str, int] = {}

    engine = _pg_engine()
    with engine.connect() as conn:
        targets = _resolve_property_targets(conn, query)

        parcel_params: dict[str, object] = {"q": query}
        parcel_clauses = [
            "hp.folio = :q",
            "hp.strap = :q",
            "hp.property_address ILIKE '%' || :q || '%'",
        ]
        _add_in_clause(parcel_clauses, parcel_params, "hp.folio", targets["folios"], "parcel_folio")
        _add_in_clause(parcel_clauses, parcel_params, "hp.strap", targets["straps"], "parcel_strap")
        _add_upper_in_clause(
            parcel_clauses,
            parcel_params,
            "hp.property_address",
            targets["addresses"],
            "parcel_addr",
        )

        # 1. Parcel Info — hcpa_bulk_parcels
        rows = _safe_query(conn, "Parcel Info", f"""
            SELECT hp.folio,
                   hp.strap,
                   hp.owner_name,
                   hp.property_address,
                   hp.city,
                   hp.zip_code,
                   hp.land_use_desc,
                   hp.year_built,
                   hp.beds,
                   hp.baths,
                   hp.heated_area,
                   hp.just_value,
                   hp.market_value,
                   hp.assessed_value,
                   hp.last_sale_date,
                   hp.last_sale_price,
                   CASE
                       WHEN nal.homestead_exempt IS TRUE THEN 'Yes'
                       WHEN nal.homestead_exempt IS FALSE THEN 'No'
                       ELSE NULL
                   END AS homestead_flag
            FROM hcpa_bulk_parcels hp
            LEFT JOIN LATERAL (
                SELECT dp.homestead_exempt
                FROM dor_nal_parcels dp
                WHERE dp.folio = hp.folio
                ORDER BY dp.tax_year DESC NULLS LAST
                LIMIT 1
            ) nal ON TRUE
            WHERE {" OR ".join(parcel_clauses)}
            LIMIT 50
        """, parcel_params)
        sections["Parcel Info"] = rows
        counts["Parcel Info"] = len(rows)

        tax_params: dict[str, object] = {"q": query}
        tax_clauses = [
            "dp.folio = :q",
            "dp.strap = :q",
            "dp.property_address ILIKE '%' || :q || '%'",
        ]
        _add_in_clause(tax_clauses, tax_params, "dp.folio", targets["folios"], "tax_folio")
        _add_in_clause(tax_clauses, tax_params, "dp.strap", targets["straps"], "tax_strap")
        _add_upper_in_clause(
            tax_clauses,
            tax_params,
            "dp.property_address",
            targets["addresses"],
            "tax_addr",
        )

        # 2. Tax Info — dor_nal_parcels
        rows = _safe_query(conn, "Tax Info", f"""
            SELECT dp.folio,
                   dp.strap,
                   dp.owner_name,
                   dp.property_address,
                   dp.just_value,
                   dp.assessed_value_school,
                   dp.assessed_value_nonschool,
                   dp.taxable_value_school,
                   dp.taxable_value_nonschool,
                   dp.tax_year,
                   dp.property_use_code,
                   dp.homestead_exempt,
                   dp.estimated_annual_tax
            FROM dor_nal_parcels dp
            WHERE {" OR ".join(tax_clauses)}
            ORDER BY dp.tax_year DESC NULLS LAST
            LIMIT 20
        """, tax_params)
        sections["Tax Info"] = rows
        counts["Tax Info"] = len(rows)

        foreclosure_params: dict[str, object] = {"q": query}
        foreclosure_clauses_current = [
            "f.folio = :q",
            "f.strap = :q",
            "f.case_number_raw = :q",
            "f.case_number_norm = :q",
            "f.property_address ILIKE '%' || :q || '%'",
        ]
        foreclosure_clauses_history = [
            "fh.folio = :q",
            "fh.strap = :q",
            "fh.case_number_raw = :q",
            "fh.case_number_norm = :q",
            "fh.property_address ILIKE '%' || :q || '%'",
        ]
        _add_in_clause(foreclosure_clauses_current, foreclosure_params, "f.folio", targets["folios"], "fc_folio")
        _add_in_clause(foreclosure_clauses_current, foreclosure_params, "f.strap", targets["straps"], "fc_strap")
        _add_in_clause(
            foreclosure_clauses_current,
            foreclosure_params,
            "f.case_number_raw",
            targets["case_numbers"],
            "fc_case_raw",
        )
        _add_in_clause(
            foreclosure_clauses_current,
            foreclosure_params,
            "f.case_number_norm",
            targets["case_numbers"],
            "fc_case_norm",
        )
        _add_upper_in_clause(
            foreclosure_clauses_current,
            foreclosure_params,
            "f.property_address",
            targets["addresses"],
            "fc_addr",
        )
        _add_in_clause(foreclosure_clauses_history, foreclosure_params, "fh.folio", targets["folios"], "fch_folio")
        _add_in_clause(foreclosure_clauses_history, foreclosure_params, "fh.strap", targets["straps"], "fch_strap")
        _add_in_clause(
            foreclosure_clauses_history,
            foreclosure_params,
            "fh.case_number_raw",
            targets["case_numbers"],
            "fch_case_raw",
        )
        _add_in_clause(
            foreclosure_clauses_history,
            foreclosure_params,
            "fh.case_number_norm",
            targets["case_numbers"],
            "fch_case_norm",
        )
        _add_upper_in_clause(
            foreclosure_clauses_history,
            foreclosure_params,
            "fh.property_address",
            targets["addresses"],
            "fch_addr",
        )

        # 3. Foreclosures — foreclosures + foreclosures_history
        rows = _safe_query(conn, "Foreclosures", f"""
            SELECT case_number_raw, auction_date, auction_type, auction_status,
                   folio, strap, property_address, owner_name, sold_to,
                   final_judgment_amount, winning_bid, market_value,
                   buyer_type, clerk_case_status
            FROM foreclosures
            WHERE {" OR ".join(foreclosure_clauses_current)}
            UNION ALL
            SELECT case_number_raw, auction_date, auction_type, auction_status,
                   folio, strap, property_address, owner_name, sold_to,
                   final_judgment_amount, winning_bid, market_value,
                   buyer_type, clerk_case_status
            FROM foreclosures_history
            WHERE {" OR ".join(foreclosure_clauses_history)}
            ORDER BY auction_date DESC NULLS LAST
            LIMIT 50
        """, foreclosure_params)
        sections["Foreclosures"] = rows
        counts["Foreclosures"] = len(rows)

        market_params: dict[str, object] = {"q": query}
        market_clauses = [
            "pm.folio = :q",
            "pm.strap = :q",
            "pm.case_number = :q",
            "COALESCE(hp.property_address, f.property_address, '') ILIKE '%' || :q || '%'",
        ]
        _add_in_clause(market_clauses, market_params, "pm.folio", targets["folios"], "market_folio")
        _add_in_clause(market_clauses, market_params, "pm.strap", targets["straps"], "market_strap")
        _add_in_clause(
            market_clauses,
            market_params,
            "pm.case_number",
            targets["case_numbers"],
            "market_case",
        )
        _add_upper_in_clause(
            market_clauses,
            market_params,
            "COALESCE(hp.property_address, f.property_address, '')",
            targets["addresses"],
            "market_addr",
        )

        # 4. Market Data — property_market
        rows = _safe_query(conn, "Market Data", f"""
            SELECT pm.folio,
                   pm.strap,
                   COALESCE(hp.property_address, f.property_address) AS address,
                   COALESCE(hp.owner_name, f.owner_name) AS owner,
                   pm.zestimate,
                   pm.rent_zestimate AS rental_zestimate,
                   hp.last_sale_date,
                   hp.last_sale_price,
                   pm.property_type,
                   pm.beds AS bedrooms,
                   pm.baths AS bathrooms,
                   pm.sqft AS living_area,
                   pm.lot_size,
                   pm.year_built,
                   pm.updated_at AS fetched_at,
                   pm.list_price,
                   pm.listing_status,
                   pm.detail_url
            FROM property_market pm
            LEFT JOIN hcpa_bulk_parcels hp ON hp.folio = pm.folio
            LEFT JOIN LATERAL (
                SELECT f.property_address, f.owner_name
                FROM foreclosures f
                WHERE f.folio = pm.folio
                ORDER BY f.updated_at DESC NULLS LAST
                LIMIT 1
            ) f ON TRUE
            WHERE {" OR ".join(market_clauses)}
            LIMIT 10
        """, market_params)
        sections["Market Data"] = rows
        counts["Market Data"] = len(rows)

        sales_params: dict[str, object] = {"q": query}
        sales_clauses = [
            "hs.folio = :q",
            "hp.strap = :q",
            "hp.property_address ILIKE '%' || :q || '%'",
        ]
        _add_in_clause(sales_clauses, sales_params, "hs.folio", targets["folios"], "sales_folio")
        _add_in_clause(sales_clauses, sales_params, "hp.strap", targets["straps"], "sales_strap")
        _add_upper_in_clause(
            sales_clauses,
            sales_params,
            "hp.property_address",
            targets["addresses"],
            "sales_addr",
        )

        # 5. Sales History — hcpa_allsales
        rows = _safe_query(conn, "Sales History", f"""
            SELECT hs.folio,
                   hp.strap,
                   hs.grantor,
                   hs.grantee,
                   hs.sale_date,
                   hs.sale_amount,
                   hs.sale_type,
                   hs.or_book,
                   hs.or_page,
                   hs.doc_num
            FROM hcpa_allsales hs
            LEFT JOIN hcpa_bulk_parcels hp ON hp.folio = hs.folio
            WHERE {" OR ".join(sales_clauses)}
            ORDER BY hs.sale_date DESC NULLS LAST
            LIMIT 50
        """, sales_params)
        sections["Sales History"] = rows
        counts["Sales History"] = len(rows)

        encumbrance_params: dict[str, object] = {"q": query}
        encumbrance_clauses = ["oe.folio = :q", "oe.strap = :q"]
        _add_in_clause(
            encumbrance_clauses,
            encumbrance_params,
            "oe.folio",
            targets["folios"],
            "enc_folio",
        )
        _add_in_clause(
            encumbrance_clauses,
            encumbrance_params,
            "oe.strap",
            targets["straps"],
            "enc_strap",
        )

        # 6. Encumbrances — ori_encumbrances
        rows = _safe_query(conn, "Encumbrances", f"""
            SELECT recording_date, encumbrance_type::text, party1, party2,
                   amount, instrument_number, book, page,
                   folio, strap, survival_status
            FROM ori_encumbrances oe
            WHERE {" OR ".join(encumbrance_clauses)}
            ORDER BY recording_date DESC NULLS LAST
            LIMIT 100
        """, encumbrance_params)
        sections["Encumbrances"] = rows
        counts["Encumbrances"] = len(rows)

        title_params: dict[str, object] = {"q": query}
        title_clauses = [
            "ftc.folio = :q",
            "ftc.strap = :q",
            "ftc.case_number_raw = :q",
            "ftc.case_number_norm = :q",
        ]
        _add_in_clause(title_clauses, title_params, "ftc.folio", targets["folios"], "title_folio")
        _add_in_clause(title_clauses, title_params, "ftc.strap", targets["straps"], "title_strap")
        _add_in_clause(
            title_clauses,
            title_params,
            "ftc.case_number_raw",
            targets["case_numbers"],
            "title_case_raw",
        )
        _add_in_clause(
            title_clauses,
            title_params,
            "ftc.case_number_norm",
            targets["case_numbers"],
            "title_case_norm",
        )

        # 7. Title Chain — foreclosure_title_chain
        rows = _safe_query(conn, "Title Chain", f"""
            SELECT foreclosure_id,
                   sequence_no,
                   owner_name,
                   acquired_date,
                   disposed_date,
                   acquired_sale_type,
                   acquired_amount,
                   grantor,
                   grantee,
                   link_status,
                   link_score,
                   is_gap,
                   is_terminal
            FROM foreclosure_title_chain ftc
            WHERE {" OR ".join(title_clauses)}
            ORDER BY acquired_date DESC NULLS LAST, sequence_no ASC
            LIMIT 100
        """, title_params)
        sections["Title Chain"] = rows
        counts["Title Chain"] = len(rows)

        permit_params: dict[str, object] = {"q": query}
        county_clauses = [
            "cp.permit_number = :q",
            "cp.address ILIKE '%' || :q || '%'",
        ]
        query_folio_clean = _digits_only(query)
        if query_folio_clean:
            permit_params["q_folio_clean"] = query_folio_clean
            county_clauses.append("cp.folio_clean = :q_folio_clean")
        _add_in_clause(
            county_clauses,
            permit_params,
            "cp.folio_clean",
            targets["folio_clean"],
            "permit_folio",
        )
        _add_upper_in_clause(
            county_clauses,
            permit_params,
            "cp.address",
            targets["addresses"],
            "permit_addr",
        )

        tampa_clauses = [
            "tr.record_number = :q",
            "tr.address_raw ILIKE '%' || :q || '%'",
            "tr.address_normalized ILIKE '%' || :q || '%'",
        ]
        _add_upper_in_clause(
            tampa_clauses,
            permit_params,
            "tr.address_raw",
            targets["addresses"],
            "tampa_addr_raw",
        )
        _add_upper_in_clause(
            tampa_clauses,
            permit_params,
            "tr.address_normalized",
            targets["addresses"],
            "tampa_addr_norm",
        )

        # 8. Permits — county_permits + tampa_accela_records
        permit_rows = _safe_query(conn, "Permits (County)", f"""
            SELECT 'County' AS permit_source,
                   cp.folio_clean AS folio,
                   cp.permit_number AS record_number,
                   cp.permit_type,
                   cp.status,
                   cp.description,
                   cp.issue_date AS start_date,
                   cp.complete_date AS end_date,
                   cp.permit_value AS work_value,
                   cp.address,
                   cp.city,
                   cp.category AS record_group,
                   cp.aca_link AS detail_url
            FROM county_permits cp
            WHERE {" OR ".join(county_clauses)}
            ORDER BY cp.issue_date DESC NULLS LAST
            LIMIT 50
        """, permit_params)
        tampa_rows = _safe_query(conn, "Permits (Tampa)", f"""
            SELECT 'Tampa' AS permit_source,
                   NULL AS folio,
                   tr.record_number,
                   tr.record_type AS permit_type,
                   tr.status,
                   COALESCE(tr.short_notes, tr.project_name) AS description,
                   tr.record_date AS start_date,
                   tr.expiration_date AS end_date,
                   tr.estimated_work_cost AS work_value,
                   tr.address_raw AS address,
                   tr.city,
                   tr.module AS record_group,
                   tr.detail_url
            FROM tampa_accela_records tr
            WHERE {" OR ".join(tampa_clauses)}
            ORDER BY tr.record_date DESC NULLS LAST
            LIMIT 50
        """, permit_params)
        combined_permits = permit_rows + tampa_rows
        sections["Permits"] = combined_permits
        counts["Permits"] = len(combined_permits)

        event_params: dict[str, object] = {"q": query}
        event_clauses = [
            "fte.folio = :q",
            "fte.strap = :q",
            "fte.case_number_raw = :q",
            "fte.case_number_norm = :q",
        ]
        _add_in_clause(event_clauses, event_params, "fte.folio", targets["folios"], "event_folio")
        _add_in_clause(event_clauses, event_params, "fte.strap", targets["straps"], "event_strap")
        _add_in_clause(
            event_clauses,
            event_params,
            "fte.case_number_raw",
            targets["case_numbers"],
            "event_case_raw",
        )
        _add_in_clause(
            event_clauses,
            event_params,
            "fte.case_number_norm",
            targets["case_numbers"],
            "event_case_norm",
        )

        # 9. Clerk Events — foreclosure_title_events
        rows = _safe_query(conn, "Clerk Events", f"""
            SELECT foreclosure_id,
                   event_date,
                   event_source,
                   event_subtype,
                   description,
                   instrument_number,
                   or_book,
                   or_page,
                   grantor,
                   grantee,
                   amount
            FROM foreclosure_title_events fte
            WHERE {" OR ".join(event_clauses)}
            ORDER BY event_date DESC NULLS LAST
            LIMIT 100
        """, event_params)
        sections["Clerk Events"] = rows
        counts["Clerk Events"] = len(rows)

    total = sum(counts.values())
    logger.info(f"Property search '{query}' -> {total} total results across {len(sections)} categories")

    return templates.TemplateResponse(
        "partials/property_search_results.html",
        {
            "request": request,
            "query": query,
            "sections": sections,
            "counts": counts,
            "total": total,
        },
    )
