"""
Property detail routes.
"""
import json

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from pathlib import Path
from typing import Any

from loguru import logger
from app.web.pg_database import get_pg_queries
from app.web.template_filters import get_templates
from src.utils.time import today_local
from sqlalchemy import text as sa_text

from sunbiz.db import get_engine, resolve_pg_dsn

router = APIRouter()

templates = get_templates()


def _pg_engine():
    return get_engine(resolve_pg_dsn())


def _pg_case_numbers_for_property(identifier: str) -> list[str]:
    if not identifier:
        return []
    try:
        with _pg_engine().connect() as conn:
            rows = conn.execute(
                sa_text("""
                    SELECT DISTINCT case_number FROM (
                        SELECT f.case_number_raw AS case_number
                        FROM foreclosures f
                        WHERE f.case_number_raw = :identifier
                           OR f.strap = :identifier
                           OR f.folio = :identifier
                        UNION ALL
                        SELECT fh.case_number_raw AS case_number
                        FROM foreclosures_history fh
                        WHERE fh.case_number_raw = :identifier
                           OR fh.strap = :identifier
                           OR fh.folio = :identifier
                    ) t
                    WHERE case_number IS NOT NULL AND btrim(case_number) != ''
                    ORDER BY case_number
                """),
                {"identifier": identifier},
            ).fetchall()
            return [str(r[0]) for r in rows if r and r[0]]
    except Exception as exc:
        logger.exception(
            f"Case number lookup failed for identifier={identifier!r}: {exc}"
        )
        return []


def _resolve_chain_folio(
    conn: Any,
    identifier: str | None,
    case_number: str | None = None,
) -> str | None:
    if case_number:
        row = conn.execute(
            sa_text("""
                SELECT folio
                FROM (
                    SELECT folio, auction_date
                    FROM foreclosures
                    WHERE case_number_raw = :case_number
                       OR case_number_norm = :case_number
                    UNION ALL
                    SELECT folio, auction_date
                    FROM foreclosures_history
                    WHERE case_number_raw = :case_number
                       OR case_number_norm = :case_number
                ) x
                WHERE folio IS NOT NULL AND btrim(folio) <> ''
                ORDER BY auction_date DESC NULLS LAST
                LIMIT 1
            """),
            {"case_number": case_number},
        ).fetchone()
        if row and row[0]:
            return str(row[0])

    if identifier:
        row = conn.execute(
            sa_text("""
                SELECT folio
                FROM (
                    SELECT folio, auction_date
                    FROM foreclosures
                    WHERE folio = :identifier
                       OR strap = :identifier
                       OR case_number_raw = :identifier
                       OR case_number_norm = :identifier
                    UNION ALL
                    SELECT folio, auction_date
                    FROM foreclosures_history
                    WHERE folio = :identifier
                       OR strap = :identifier
                       OR case_number_raw = :identifier
                       OR case_number_norm = :identifier
                ) x
                WHERE folio IS NOT NULL AND btrim(folio) <> ''
                ORDER BY auction_date DESC NULLS LAST
                LIMIT 1
            """),
            {"identifier": identifier},
        ).fetchone()
        if row and row[0]:
            return str(row[0])

        row = conn.execute(
            sa_text("""
                SELECT folio
                FROM hcpa_bulk_parcels
                WHERE strap = :identifier OR folio = :identifier
                LIMIT 1
            """),
            {"identifier": identifier},
        ).fetchone()
        if row and row[0]:
            return str(row[0])

        row = conn.execute(
            sa_text("""
                SELECT folio
                FROM hcpa_allsales
                WHERE folio = :identifier
                LIMIT 1
            """),
            {"identifier": identifier},
        ).fetchone()
        if row and row[0]:
            return str(row[0])

    return None


def _pg_chain_for_property(identifier: str, case_number: str | None = None) -> list[dict[str, Any]]:
    try:
        with _pg_engine().connect() as conn:
            folio = _resolve_chain_folio(conn, identifier, case_number)
            if not folio:
                return []

            rows = conn.execute(
                sa_text("""
                    SELECT
                        seq_no,
                        sale_date,
                        sale_type,
                        sale_amount,
                        grantor,
                        grantee,
                        or_book,
                        or_page,
                        doc_num,
                        days_since_prev,
                        link_score,
                        link_ok,
                        link_reason
                    FROM fn_title_chain(:folio)
                    ORDER BY seq_no
                """),
                {"folio": folio},
            ).mappings().fetchall()

            chain_rows: list[dict[str, Any]] = []
            for row in rows:
                reason = str(row.get("link_reason") or "")
                if reason == "NAME_MISMATCH":
                    link_status = "BROKEN"
                elif reason == "MISSING_PARTY":
                    link_status = "INCOMPLETE"
                elif reason == "FUZZY_MATCH":
                    link_status = "FUZZY"
                elif reason == "ROOT_BOUNDARY":
                    link_status = "IMPLIED"
                else:
                    link_status = "LINKED"

                instrument = row.get("doc_num")
                if not instrument and row.get("or_book") and row.get("or_page"):
                    instrument = f"{row['or_book']}/{row['or_page']}"

                chain_rows.append(
                    {
                        "sequence_no": row.get("seq_no"),
                        "acquisition_date": row.get("sale_date"),
                        "acquisition_doc_type": row.get("sale_type"),
                        "acquisition_price": row.get("sale_amount"),
                        "acquired_from": row.get("grantor"),
                        "owner_name": row.get("grantee"),
                        "acquisition_instrument": instrument,
                        "link_status": link_status,
                        "link_score": row.get("link_score"),
                        "link_reason": reason,
                        "days_since_prev": row.get("days_since_prev"),
                    }
                )

            return chain_rows
    except Exception as exc:
        logger.exception(
            "Title chain lookup failed "
            f"for identifier={identifier!r} case_number={case_number!r}: {exc}"
        )
        return []


def _pg_chain_gaps_for_property(
    identifier: str,
    case_number: str | None = None,
) -> list[dict[str, Any]]:
    try:
        with _pg_engine().connect() as conn:
            folio = _resolve_chain_folio(conn, identifier, case_number)
            if not folio:
                return []
            rows = conn.execute(
                sa_text("""
                    SELECT
                        gap_type,
                        seq_prev,
                        seq_next,
                        expected_from_party,
                        observed_to_party,
                        missing_from_date,
                        missing_to_date,
                        recommended_source,
                        detail
                    FROM fn_title_chain_gaps(:folio)
                """),
                {"folio": folio},
            ).mappings().fetchall()
            return [dict(r) for r in rows]
    except Exception as exc:
        logger.exception(
            "Title chain gaps lookup failed "
            f"for identifier={identifier!r} case_number={case_number!r}: {exc}"
        )
        return []


def _pg_documents_for_property(identifier: str) -> list[dict[str, Any]]:
    project_root = Path(__file__).resolve().parents[3]
    foreclosure_root = (project_root / "data" / "Foreclosure").resolve()
    docs: list[dict[str, Any]] = []
    next_id = 1

    for case_num in _pg_case_numbers_for_property(identifier):
        doc_dir = foreclosure_root / case_num / "documents"
        if not doc_dir.is_dir():
            continue
        for pdf in sorted(doc_dir.glob("*.pdf")):
            try:
                rel_path = str(pdf.resolve().relative_to(project_root.resolve()))
            except Exception as exc:
                logger.warning(
                    f"Failed to resolve relative PDF path for case={case_num} file={pdf}: {exc}"
                )
                rel_path = str(pdf.resolve())
            docs.append(
                {
                    "id": next_id,
                    "folio": identifier,
                    "case_number": case_num,
                    "document_type": (
                        "FINAL_JUDGMENT"
                        if "judgment" in pdf.name.lower()
                        else "PDF"
                    ),
                    "file_path": rel_path,
                    "recording_date": None,
                    "instrument_number": None,
                    "party1": None,
                    "party2": None,
                }
            )
            next_id += 1
    return docs


def _pg_tax_status_for_property(folio: str) -> dict[str, Any]:
    try:
        with _pg_engine().connect() as conn:
            row = conn.execute(
                sa_text("""
                    SELECT tax_year, homestead_exempt, estimated_annual_tax
                    FROM dor_nal_parcels
                    WHERE folio = :id OR strap = :id OR parcel_id = :id
                    ORDER BY tax_year DESC
                    LIMIT 1
                """),
                {"id": folio},
            ).mappings().fetchone()
            if not row:
                return {
                    "has_tax_liens": False,
                    "tax_status": None,
                    "tax_warrant": False,
                    "total_amount_due": None,
                    "liens": [],
                }
            amount = row.get("estimated_annual_tax")
            return {
                "has_tax_liens": bool((amount or 0) > 0),
                "tax_status": f"Tax Year {row.get('tax_year')}",
                "tax_warrant": False,
                "total_amount_due": float(amount) if amount is not None else None,
                "liens": [],
            }
    except Exception as exc:
        logger.exception(f"Tax status lookup failed for folio={folio!r}: {exc}")
        return {
            "has_tax_liens": False,
            "tax_status": None,
            "tax_warrant": False,
            "total_amount_due": None,
            "liens": [],
        }


def _pg_permits_for_property(foreclosure_id: int) -> list[dict[str, Any]]:
    try:
        with _pg_engine().connect() as conn:
            rows = conn.execute(
                sa_text("""
                    SELECT
                        event_date AS issue_date,
                        instrument_number AS permit_number,
                        event_subtype AS permit_type,
                        description,
                        amount AS estimated_cost,
                        CASE
                            WHEN description ~* '(closed|complete|final|expired)'
                                THEN 'Closed'
                            ELSE 'Open'
                        END AS status
                    FROM foreclosure_title_events
                    WHERE foreclosure_id = :fid
                      AND event_source IN ('COUNTY_PERMIT', 'TAMPA_PERMIT')
                    ORDER BY event_date DESC
                """),
                {"fid": foreclosure_id},
            ).mappings().fetchall()
            return [dict(r) for r in rows]
    except Exception as exc:
        logger.exception(
            f"Permit lookup failed for foreclosure_id={foreclosure_id}: {exc}"
        )
        return []


def _pg_market_snapshot(auction: dict[str, Any], parcel: dict[str, Any] | None) -> dict[str, Any]:
    zestimate = auction.get("zestimate")
    list_price = auction.get("list_price")
    market_value = (
        zestimate
        or list_price
        or auction.get("market_value")
        or (parcel or {}).get("market_value")
    )
    estimates = {
        "zillow_zestimate": float(zestimate) if zestimate is not None else None,
        "homeharvest_estimated_value": None,
        "redfin_estimate": None,
        "realtor_estimate": None,
    }
    list_prices = {
        "redfin_list_price": float(list_price) if list_price is not None else None,
        "realtor_list_price": None,
        "homeharvest_list_price": None,
    }
    return {
        "blended_estimate": float(market_value) if market_value is not None else 0,
        "estimates": estimates,
        "list_prices": list_prices,
        "photos": [],
        "photos_with_fallback": [],
    }


def _pg_property_detail(identifier: str) -> dict[str, Any] | None:
    try:
        with _pg_engine().connect() as conn:
            row = conn.execute(
                sa_text("""
                    SELECT *
                    FROM foreclosures
                    WHERE case_number_raw = :identifier
                       OR strap = :identifier
                       OR folio = :identifier
                    ORDER BY auction_date DESC, updated_at DESC NULLS LAST
                    LIMIT 1
                """),
                {"identifier": identifier},
            ).mappings().fetchone()
            if not row:
                row = conn.execute(
                    sa_text("""
                        SELECT *
                        FROM foreclosures_history
                        WHERE case_number_raw = :identifier
                           OR strap = :identifier
                           OR folio = :identifier
                        ORDER BY auction_date DESC, updated_at DESC NULLS LAST
                        LIMIT 1
                    """),
                    {"identifier": identifier},
                ).mappings().fetchone()
            if not row:
                return None

            auction = dict(row)
            case_number = auction.get("case_number_raw")
            strap_or_folio = auction.get("strap") or auction.get("folio") or identifier

            parcel = None
            parcel_clauses: list[str] = []
            parcel_params: dict[str, Any] = {}
            if auction.get("strap"):
                parcel_clauses.append("strap = :strap")
                parcel_params["strap"] = auction.get("strap")
            if auction.get("folio"):
                parcel_clauses.append("folio = :folio")
                parcel_params["folio"] = auction.get("folio")
            if parcel_clauses:
                parcel = conn.execute(
                    sa_text(f"""
                        SELECT *
                        FROM hcpa_bulk_parcels
                        WHERE {" OR ".join(parcel_clauses)}
                        ORDER BY source_file_id DESC NULLS LAST
                        LIMIT 1
                    """),
                    parcel_params,
                ).mappings().fetchone()
            parcel_dict = dict(parcel) if parcel else {}

            judgment_data = auction.get("judgment_data")
            if isinstance(judgment_data, str):
                try:
                    judgment_data = json.loads(judgment_data)
                except json.JSONDecodeError:
                    judgment_data = None

            market = _pg_market_snapshot(auction, parcel_dict)
            market_value = (
                market.get("blended_estimate")
                or auction.get("market_value")
                or parcel_dict.get("market_value")
                or auction.get("assessed_value")
                or 0
            )
            net_equity = float(market_value or 0) - float(
                auction.get("final_judgment_amount") or 0
            )

            foreclosure_id = int(auction.get("foreclosure_id"))
            permits = _pg_permits_for_property(foreclosure_id)
            enrichments = {
                "permits_total": len(permits),
                "permits_open": sum(
                    1
                    for p in permits
                    if str(p.get("status") or "").lower() in {"open", "active", "issued"}
                ),
                "liens_surviving": 0,
                "liens_total_amount": 0,
                "liens_total": 0,
                "flood_zone": None,
                "flood_risk": None,
                "insurance_required": False,
                "has_enrichments": len(permits) > 0,
            }

            auction_payload = {
                "case_number": case_number,
                "auction_type": str(auction.get("auction_type") or "foreclosure").upper(),
                "auction_date": auction.get("auction_date"),
                "property_address": auction.get("property_address") or parcel_dict.get("property_address"),
                "assessed_value": auction.get("assessed_value"),
                "final_judgment_amount": auction.get("final_judgment_amount"),
                "opening_bid": auction.get("winning_bid"),
                "status": auction.get("auction_status"),
                "owner_name": auction.get("owner_name") or parcel_dict.get("owner_name"),
                "plaintiff_max_bid": None,
                "plaintiff": None,
                "foreclosure_type": None,
                "lis_pendens_date": None,
                "extracted_judgment_data": judgment_data,
                "judgment_extracted_at": auction.get("step_judgment_extracted"),
                "has_valid_parcel_id": bool(auction.get("strap") or auction.get("folio")),
                "folio": strap_or_folio,
            }

            return {
                "folio": strap_or_folio,
                "auction": auction_payload,
                "parcel": parcel_dict,
                "parcels_data": parcel_dict,
                "encumbrances": [],
                "chain": _pg_chain_for_property(strap_or_folio, case_number),
                "nocs": [],
                "sales": get_pg_queries().get_sales_history(strap_or_folio),
                "net_equity": net_equity,
                "market_value": market_value,
                "est_surviving_debt": 0,
                "is_toxic_title": bool(
                    (auction.get("unsatisfied_encumbrance_count") or 0) > 2
                ),
                "market": market,
                "enrichments": enrichments,
                "sources": [],
                "_foreclosure_id": foreclosure_id,
            }
    except Exception as exc:
        logger.exception(
            f"Property detail lookup failed for identifier={identifier!r}: {exc}"
        )
        return None


@router.get("/{folio}", response_class=HTMLResponse)
async def property_detail(request: Request, folio: str):
    """
    Full property detail page.
    """
    prop = _pg_property_detail(folio)

    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    # Enrich with PG data (graceful degradation)
    pg = get_pg_queries()
    pg_data = {}
    real_folio = prop.get("folio") or folio
    if pg.available:
        pg_data["subdivision"] = pg.get_subdivision_info(real_folio)
        pg_data["multi_unit"] = pg.is_multi_unit(real_folio)
        pg_data["pg_available"] = True
    else:
        pg_data["subdivision"] = None
        pg_data["multi_unit"] = None
        pg_data["pg_available"] = False

    return templates.TemplateResponse(
        "property.html",
        {
            "request": request,
            "property": prop,
            "auction": prop.get("auction", {}),
            "parcel": prop.get("parcel") or prop.get("parcels_data") or {},
            "encumbrances": prop.get("encumbrances", []),
            "net_equity": prop.get("net_equity", 0),
            "market_value": prop.get("market_value", 0),
            "market": prop.get("market", {}),
            "enrichments": prop.get("enrichments", {}),
            "pg_data": pg_data,
        }
    )


@router.get("/{folio}/liens", response_class=HTMLResponse)
async def property_liens(request: Request, folio: str):
    """
    HTMX partial - liens table for a property.
    """
    prop = _pg_property_detail(folio)

    if not prop:
        return HTMLResponse("<p>Property not found</p>")

    encumbrances = prop.get("encumbrances", [])
    real_folio = prop.get("folio") or folio

    return templates.TemplateResponse(
        "partials/lien_table.html",
        {
            "request": request,
            "liens": [],
            "encumbrances": encumbrances,
            "auction": prop.get("auction", {}),
            "folio": real_folio
        }
    )


@router.get("/{folio}/documents", response_class=HTMLResponse)
async def property_documents(request: Request, folio: str):
    """
    HTMX partial - documents list for a property.
    """
    documents = _pg_documents_for_property(folio)

    return templates.TemplateResponse(
        "partials/documents.html",
        {
            "request": request,
            "documents": documents,
            "folio": folio
        }
    )


@router.get("/{folio}/analysis", response_class=HTMLResponse)
async def property_analysis(request: Request, folio: str):
    """
    HTMX partial - equity analysis card.
    """
    prop = _pg_property_detail(folio)

    if not prop:
        return HTMLResponse("<p>Property not found</p>")

    return templates.TemplateResponse(
        "partials/analysis_card.html",
        {
            "request": request,
            "property": prop,
            "net_equity": prop.get("net_equity", 0),
            "market_value": prop.get("market_value", 0)
        }
    )


@router.get("/{folio}/sales", response_class=HTMLResponse)
async def property_sales_history(request: Request, folio: str):
    """
    HTMX partial - sales history for a property (PG-only).
    """
    pg = get_pg_queries()
    sales = pg.get_sales_history(folio) if pg.available else []
    return templates.TemplateResponse(
        "partials/pg_sales_history.html",
        {
            "request": request,
            "sales": sales,
            "folio": folio,
            "source": "pg" if pg.available else "pg_unavailable",
        },
    )


@router.get("/{folio}/market", response_class=HTMLResponse)
async def property_market(request: Request, folio: str):
    """
    HTMX partial - blended market data + HomeHarvest gallery.
    """
    prop = _pg_property_detail(folio)
    if not prop:
        return HTMLResponse("<p>Property not found</p>")
    market = prop.get("market") or {}
    return templates.TemplateResponse(
        "partials/market.html",
        {
            "request": request,
            "folio": prop.get("folio") or folio,
            "auction": prop.get("auction", {}),
            "market": market,
        },
    )


@router.get("/{folio}/tax", response_class=HTMLResponse)
async def property_tax(request: Request, folio: str):
    """
    HTMX partial - tax status and tax liens.
    """
    prop = _pg_property_detail(folio)
    if not prop:
        return HTMLResponse("<p>Property not found</p>")
    status = _pg_tax_status_for_property(prop.get("folio") or folio)
    return templates.TemplateResponse(
        "partials/tax.html",
        {
            "request": request,
            "folio": prop.get("folio") or folio,
            "auction": prop.get("auction", {}),
            "tax": status,
        },
    )


@router.get("/{folio}/permits", response_class=HTMLResponse)
async def property_permits(request: Request, folio: str):
    """
    HTMX partial - permits and NOCs.
    """
    prop = _pg_property_detail(folio)
    if not prop:
        return HTMLResponse("<p>Property not found</p>")

    permits = _pg_permits_for_property(prop.get("_foreclosure_id") or 0)
    nocs = []

    return templates.TemplateResponse(
        "partials/permits.html",
        {
            "request": request,
            "permits": permits,
            "nocs": nocs,
            "folio": prop.get("folio") or folio
        }
    )


@router.get("/{folio}/chain", response_class=HTMLResponse)
async def property_chain_of_title(request: Request, folio: str):
    """
    HTMX partial - chain of title for a property.
    """
    prop = _pg_property_detail(folio)

    if not prop:
        return HTMLResponse('<p class="text-muted">No chain of title data available.</p>')

    chain_of_title = _pg_chain_for_property(
        identifier=folio,
        case_number=(prop.get("auction") or {}).get("case_number"),
    )
    chain_gaps = _pg_chain_gaps_for_property(
        identifier=folio,
        case_number=(prop.get("auction") or {}).get("case_number"),
    )
    if not chain_of_title:
        chain_of_title = prop.get("chain", [])
    real_folio = prop.get("folio") or folio

    # Enhance chain with document links — only link when a file exists on disk
    for item in chain_of_title:
        item["document_id"] = None

    return templates.TemplateResponse(
        "partials/chain_of_title.html",
        {
            "request": request,
            "chain_of_title": chain_of_title,
            "chain_gaps": chain_gaps,
            "folio": real_folio
        }
    )


@router.get("/{folio}/judgment", response_class=HTMLResponse)
async def property_judgment(request: Request, folio: str):
    """
    HTMX partial - extracted final judgment data.
    """
    prop = _pg_property_detail(folio)
    if not prop:
        return HTMLResponse("<p>Property not found</p>")

    auction = prop.get("auction", {})
    judgment = {
        "case_number": auction.get("case_number"),
        "foreclosure_type": auction.get("foreclosure_type"),
        "lis_pendens_date": auction.get("lis_pendens_date"),
        "plaintiff": auction.get("plaintiff"),
        "defendant": auction.get("defendant"),
        "final_judgment_amount": auction.get("final_judgment_amount"),
        "judgment_extracted_at": auction.get("judgment_extracted_at"),
        "extracted_judgment_data": auction.get("extracted_judgment_data"),
    }

    return templates.TemplateResponse(
        "partials/judgment.html",
        {
            "request": request,
            "judgment": judgment,
            "folio": prop.get("folio") or folio
        }
    )


@router.get("/{folio}/comparables", response_class=HTMLResponse)
async def property_comparables(request: Request, folio: str, years: int = 3):
    """
    HTMX partial - comparable sales from PG.
    """
    pg = get_pg_queries()
    comps = []
    subdivision = None
    if pg.available:
        comps = pg.get_comparable_sales(folio, years=years)
        subdivision = pg.get_subdivision_info(folio)

    return templates.TemplateResponse(
        "partials/comparables.html",
        {
            "request": request,
            "comparables": comps,
            "subdivision": subdivision,
            "folio": folio,
            "years": years,
            "pg_available": pg.available,
        }
    )


def _sanitize_folio(folio: str) -> str:
    return (
        folio.replace("-", "")
        .replace(" ", "")
        .replace(":", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(",", "")
        .replace("#", "")
    )


@router.get("/{folio}/doc/{doc_id}")
async def property_document_file(folio: str, doc_id: int):
    """
    Serve a document file by its DB id.
    Checks data/Foreclosure/{case_number}/documents/ first, then data/properties/{folio}/.
    """
    docs = _pg_documents_for_property(folio)
    doc = next((d for d in docs if d.get("id") == doc_id), None)
    if not doc or not doc.get("file_path"):
        raise HTTPException(status_code=404, detail="Document not found")

    project_root = Path(__file__).resolve().parents[3]
    file_path = (project_root / doc["file_path"]).resolve()

    # Prevent path traversal — must be under project data dir
    data_dir = (project_root / "data").resolve()
    if not str(file_path).startswith(str(data_dir)):
        raise HTTPException(status_code=404, detail="Invalid document path")
    if not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found on disk")

    return FileResponse(path=file_path, filename=file_path.name)


@router.get("/{folio}/documents/{filename:path}")
async def serve_document_by_name(folio: str, filename: str):
    """
    Serve a document file by filename for a property.
    Looks in data/Foreclosure/{case_number}/documents/ and data/properties/{folio}/.
    """
    project_root = Path(__file__).resolve().parents[3]
    data_dir = (project_root / "data").resolve()

    # Sanitize filename
    if ".." in filename or filename.startswith("/"):
        raise HTTPException(status_code=400, detail="Invalid filename")

    # Try Foreclosure path first — look up case_number(s) from PG
    for case_num in _pg_case_numbers_for_property(folio):
        candidate = data_dir / "Foreclosure" / case_num / "documents" / filename
        if candidate.resolve().is_file() and str(candidate.resolve()).startswith(str(data_dir)):
            return FileResponse(path=candidate.resolve(), filename=filename)

    # Fallback: data/properties/{folio}/
    safe_folio = _sanitize_folio(folio)
    fallback = data_dir / "properties" / safe_folio / filename
    if fallback.resolve().is_file() and str(fallback.resolve()).startswith(str(data_dir)):
        return FileResponse(path=fallback.resolve(), filename=filename)

    raise HTTPException(status_code=404, detail="File not found on disk")


@router.get("/{folio}/photos/{filename}")
async def serve_photo(folio: str, filename: str):
    """Serve a locally downloaded property photo."""
    # Path traversal protection
    if ".." in filename or "/" in filename or filename.startswith("."):
        raise HTTPException(status_code=400, detail="Invalid filename")

    project_root = Path(__file__).resolve().parents[3]
    data_dir = (project_root / "data").resolve()

    # Look up case_number(s) from PG
    for case_num in _pg_case_numbers_for_property(folio):
        candidate = data_dir / "Foreclosure" / case_num / "photos" / filename
        resolved = candidate.resolve()
        if resolved.is_file() and str(resolved).startswith(str(data_dir)):
            return FileResponse(path=resolved, filename=filename)

    raise HTTPException(status_code=404, detail="Photo not found on disk")


@router.get("/{folio}/title-report", response_class=HTMLResponse)
async def property_title_report(request: Request, folio: str):
    """
    Generate a printable Title Report.
    """
    prop = _pg_property_detail(folio)
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    return templates.TemplateResponse(
        "title_report.html",
        {
            "request": request,
            "property": prop,
            "generated_date": today_local().strftime("%B %d, %Y")
        }
    )
