"""
Property detail routes.
"""
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
from datetime import UTC, datetime
from pathlib import Path
import os

from app.web.database import (
    get_property_detail,
    get_property_by_case,
    get_liens_for_property,
    get_documents_for_property,
    get_sales_history,
    get_document_by_instrument,
    get_market_snapshot,
    get_tax_status_for_property,
    get_permits_for_property,
    get_nocs_for_property,
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
            "market_value": prop.get("market_value", 0),
            "market": prop.get("market", {}),
            "enrichments": prop.get("enrichments", {}),
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


@router.get("/{folio}/market", response_class=HTMLResponse)
async def property_market(request: Request, folio: str):
    """
    HTMX partial - blended market data + HomeHarvest gallery.
    """
    prop = get_property_detail(folio) or get_property_by_case(folio)
    if not prop:
        return HTMLResponse("<p>Property not found</p>")
    market = prop.get("market") or get_market_snapshot(prop.get("folio") or folio)
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
    prop = get_property_detail(folio) or get_property_by_case(folio)
    if not prop:
        return HTMLResponse("<p>Property not found</p>")
    status = get_tax_status_for_property(prop.get("folio") or folio)
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
    prop = get_property_detail(folio) or get_property_by_case(folio)
    if not prop:
        return HTMLResponse("<p>Property not found</p>")
        
    permits = get_permits_for_property(prop.get("folio") or folio)
    nocs = get_nocs_for_property(prop.get("folio") or folio)
    
    return templates.TemplateResponse(
        "partials/permits.html",
        {
            "request": request,
            "permits": permits,
            "nocs": nocs,
            "folio": prop.get("folio") or folio
        }
    )


def _names_match(grantee: str | None, grantor: str | None) -> bool:
    """
    Check if grantee from previous transfer matches grantor of current transfer.
    Handles multi-party names (comma-separated) and common abbreviations.
    """
    if not grantee or not grantor:
        return True  # Can't determine, assume OK

    # Normalize and split into individual names
    def normalize(name: str) -> set[str]:
        name = name.upper().strip()
        # Split on comma to get individual parties
        parties = [p.strip() for p in name.split(",")]
        # Extract key words (3+ chars, not common legal terms)
        stopwords = {"THE", "AND", "FOR", "INC", "LLC", "CORP", "CORPORATION",
                     "COMPANY", "TRUST", "TRUSTEE", "ATTY", "ATTORNEY"}
        words = set()
        for party in parties:
            for word in party.split():
                word = word.strip(".,;:").replace(".", "")
                if len(word) >= 3 and word not in stopwords:
                    words.add(word)
        return words

    # Alias mappings for known institutional name variations
    aliases = {
        "HUD": {"HOUSING", "URBAN", "DEVELOPMENT", "SECRETARY"},
        "CHASE": {"JPMORGAN", "MANHATTAN"},
        "WELLS": {"FARGO"},
        "FANNIE": {"FNMA", "FEDERAL", "NATIONAL"},
        "FREDDIE": {"FHLMC", "FEDERAL", "HOME", "LOAN"},
        "USA": {"UNITED", "STATES", "AMERICA"},
    }

    grantee_words = normalize(grantee)
    grantor_words = normalize(grantor)

    # Check for direct overlap (at least 2 significant words match)
    overlap = grantee_words & grantor_words
    if len(overlap) >= 2:
        return True

    # Check for single-word match on surnames (for individuals like WERNER ALAN)
    # If either set is small (individual name), one match is enough
    if len(overlap) >= 1 and (len(grantee_words) <= 3 or len(grantor_words) <= 3):
        return True

    # Check alias mappings
    for key, alias_set in aliases.items():
        if key in grantee_words or key in grantor_words:
            combined = alias_set | {key}
            if (grantee_words & combined) and (grantor_words & combined):
                return True

    return False


@router.get("/{folio}/chain", response_class=HTMLResponse)
async def property_chain_of_title(request: Request, folio: str):
    """
    HTMX partial - chain of title for a property.
    """
    prop = get_property_detail(folio)
    if not prop:
        prop = get_property_by_case(folio)

    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    chain_of_title = prop.get("chain", [])

    # Enhance chain with document links and chain break detection
    for i, item in enumerate(chain_of_title):
        doc = None
        if item.get("acquisition_instrument"):
            doc = get_document_by_instrument(folio, item["acquisition_instrument"])
        item["document_id"] = doc["id"] if doc else None

        # Pre-compute chain break detection
        if i > 0:
            prev_item = chain_of_title[i - 1]
            # Current grantee should match previous grantor (chain goes backwards in time)
            item["is_chain_break"] = not _names_match(
                item.get("owner_name"),  # Current grantee
                prev_item.get("acquired_from")  # Previous grantor
            )
        else:
            item["is_chain_break"] = False

    return templates.TemplateResponse(
        "partials/chain_of_title.html",
        {
            "request": request,
            "chain_of_title": chain_of_title,
            "folio": folio
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
    Serve a document file for a property if it exists under data/properties/{folio}/.
    """
    docs = get_documents_for_property(folio)
    doc = next((d for d in docs if d.get("id") == doc_id), None)
    if not doc or not doc.get("file_path"):
        raise HTTPException(status_code=404, detail="Document not found")

    base_dir = Path("data/properties")
    safe_folio = _sanitize_folio(folio)
    file_path = base_dir / safe_folio / doc["file_path"]
    file_path = file_path.resolve()

    # Prevent path traversal
    if base_dir.resolve() not in file_path.parents and base_dir.resolve() != file_path.parent:
        raise HTTPException(status_code=404, detail="Invalid document path")
    if not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found on disk")

    return FileResponse(path=file_path, filename=os.path.basename(file_path))


@router.get("/{folio}/title-report", response_class=HTMLResponse)
async def property_title_report(request: Request, folio: str):
    """
    Generate a printable Title Report.
    """
    prop = get_property_detail(folio) or get_property_by_case(folio)
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    return templates.TemplateResponse(
        "title_report.html",
        {
            "request": request,
            "property": prop,
            "generated_date": datetime.now(tz=UTC).strftime("%B %d, %Y")
        }
    )

