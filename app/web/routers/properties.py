"""
Property detail routes.
"""
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from app.web.database import (
    get_property_detail,
    get_property_by_case,
    get_liens_for_property,
    get_documents_for_property,
    get_sales_history
)

router = APIRouter()

# Templates
TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@router.get("/{folio}", response_class=HTMLResponse)
async def property_detail(request: Request, folio: str):
    """
    Full property detail page.
    """
    # Try to get by folio first
    prop = get_property_detail(folio)

    # If not found, try as case number
    if not prop:
        prop = get_property_by_case(folio)

    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    return templates.TemplateResponse(
        "property.html",
        {
            "request": request,
            "property": prop,
            "auction": prop.get("auction", {}),
            "parcel": prop.get("parcel", {}),
            "liens": prop.get("liens", []),
            "encumbrances": prop.get("encumbrances", []),
            "net_equity": prop.get("net_equity", 0),
            "market_value": prop.get("market_value", 0)
        }
    )


@router.get("/{folio}/liens", response_class=HTMLResponse)
async def property_liens(request: Request, folio: str):
    """
    HTMX partial - liens table for a property.
    """
    prop = get_property_detail(folio)
    if not prop:
        prop = get_property_by_case(folio)

    if not prop:
        return HTMLResponse("<p>Property not found</p>")

    case_number = prop.get("auction", {}).get("case_number")
    liens = get_liens_for_property(case_number) if case_number else []

    # Combine with encumbrances
    encumbrances = prop.get("encumbrances", [])

    return templates.TemplateResponse(
        "partials/lien_table.html",
        {
            "request": request,
            "liens": liens,
            "encumbrances": encumbrances,
            "folio": folio
        }
    )


@router.get("/{folio}/documents", response_class=HTMLResponse)
async def property_documents(request: Request, folio: str):
    """
    HTMX partial - documents list for a property.
    """
    documents = get_documents_for_property(folio)

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
    prop = get_property_detail(folio)
    if not prop:
        prop = get_property_by_case(folio)

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
    HTMX partial - sales history for a property.
    """
    sales = get_sales_history(folio)

    return templates.TemplateResponse(
        "partials/sales_history.html",
        {
            "request": request,
            "sales": sales,
            "folio": folio
        }
    )
@router.get("/{folio}/title-report", response_class=HTMLResponse)
async def property_title_report(request: Request, folio: str):
    """
    Generate a printable Title Report.
    """
    prop = get_property_detail(folio)
    if not prop:
        prop = get_property_by_case(folio)

    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    from datetime import date
    
    return templates.TemplateResponse(
        "title_report.html",
        {
            "request": request,
            "property": prop,
            "generated_date": date.today().strftime("%B %d, %Y")
        }
    )

@router.get("/{folio}/documents", response_class=HTMLResponse)
async def property_documents(request: Request, folio: str):
    """
    HTMX partial - documents list.
    """
    docs = get_documents_for_property(folio)
    return templates.TemplateResponse(
        "partials/documents.html",
        {
            "request": request,
            "documents": docs,
            "folio": folio
        }
    )

