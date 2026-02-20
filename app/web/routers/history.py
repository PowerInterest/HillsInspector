from __future__ import annotations

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

import re

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from loguru import logger
from sqlalchemy import text as sa_text

from sunbiz.db import get_engine, resolve_pg_dsn

router = APIRouter()

def _pg_engine():
    return get_engine(resolve_pg_dsn())

CASE_NUMBER_RE = re.compile(
    r"id=[\"']case_number[\"'][^>]*value=[\"']([^\"']+)[\"']",
    re.IGNORECASE,
)

def _clean_case_number(value: str | None) -> str:
    if not value:
        return ""
    text_value = str(value).strip()
    return re.sub(r"\s*\(document link\)\s*$", "", text_value, flags=re.IGNORECASE).strip()


def _resolve_folio_for_history(identifier: str) -> str | None:
    if not identifier:
        return None
    with _pg_engine().connect() as conn:
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
                WHERE folio = :identifier OR strap = :identifier
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

def get_history_stats():
    """Query PostgreSQL for dashboard stats."""
    with _pg_engine().connect() as conn:
        # 1. Total Volume
        total_auctions = conn.execute(
            sa_text("SELECT COUNT(*) FROM foreclosures_history")
        ).scalar() or 0

        # 2. Third Party Share
        tp_share = conn.execute(sa_text("""
            SELECT
                SUM(CASE WHEN buyer_type = 'Third Party' THEN 1 ELSE 0 END) as tp_count,
                COUNT(*) as total
            FROM foreclosures_history
        """)).fetchone()

        tp_pct = (tp_share[0] / tp_share[1] * 100) if tp_share and tp_share[1] > 0 else 0

        # 3. Top Buyers (2+ purchases) with ROI when resale exists
        top_buyers = conn.execute(sa_text("""
            WITH buyer_rows AS (
                SELECT
                    COALESCE(normalize_party_name(a.sold_to::text), 'unknown') AS buyer_key,
                    COALESCE(NULLIF(TRIM(a.sold_to::text), ''), 'Unknown') AS sold_to_display,
                    a.winning_bid,
                    fs.roi
                FROM foreclosures_history a
                LEFT JOIN LATERAL first_valid_resale(
                    a.folio,
                    a.auction_date,
                    a.winning_bid,
                    a.appraised_value
                ) fs ON TRUE
                WHERE a.buyer_type = 'Third Party'
            )
            SELECT
                MIN(sold_to_display) AS sold_to,
                COUNT(*) AS buys,
                COALESCE(SUM(winning_bid), 0) AS volume,
                AVG(roi) AS avg_roi
            FROM buyer_rows
            GROUP BY buyer_key
            HAVING COUNT(*) >= 2
            ORDER BY buys DESC, sold_to
        """)).fetchall()

        # 4. Flip Analysis (PG-only from foreclosures_history + hcpa_allsales)
        flip_row = conn.execute(sa_text("""
            WITH flip_rows AS (
                SELECT
                    fs.gross_profit,
                    fs.roi,
                    fs.hold_days AS hold_time
                FROM foreclosures_history a
                LEFT JOIN LATERAL first_valid_resale(
                    a.folio,
                    a.auction_date,
                    a.winning_bid,
                    a.appraised_value
                ) fs ON TRUE
                WHERE fs.sale_date IS NOT NULL
            )
            SELECT
                COUNT(*) AS flip_count,
                COUNT(*) FILTER (WHERE gross_profit > 0) AS profitable_count,
                COUNT(*) FILTER (WHERE gross_profit <= 0) AS loss_count,
                AVG(gross_profit) AS avg_profit,
                AVG(hold_time) AS avg_hold,
                AVG(roi) AS avg_roi
            FROM flip_rows
        """)).mappings().one()

        return {
            "total_auctions": total_auctions,
            "tp_share_pct": round(tp_pct, 1),
            "profitable_flips": int(flip_row.get("profitable_count") or 0),
            "loss_flips": int(flip_row.get("loss_count") or 0),
            "avg_profit": round(float(flip_row.get("avg_profit") or 0), 0),
            "avg_hold_days": round(float(flip_row.get("avg_hold") or 0), 0),
            "avg_roi_mult": round(float(flip_row.get("avg_roi") or 0), 2),
            "top_buyers": top_buyers
        }

@router.get("/history", response_class=HTMLResponse)
async def history_page(request: Request):
    """Render the historical analysis dashboard."""
    from app.web.main import templates

    try:
        stats = get_history_stats()
    except Exception as e:
        logger.error(f"Error loading history stats: {e}")
        stats = {
            "total_auctions": 0, "tp_share_pct": 0, "profitable_flips": 0,
            "loss_flips": 0, "avg_profit": 0, "avg_hold_days": 0, "top_buyers": []
        }

    return templates.TemplateResponse("history.html", {
        "request": request,
        "stats": stats,
        "active_tab": "history"
    })

@router.get("/history/data")
async def history_data(limit: int = 5000):
    """Return JSON data from PostgreSQL for the history grid."""
    try:
        limit = max(1, min(limit, 50000))
        
        # Pull aged foreclosures with first valid post-auction sale.
        query = sa_text("""
            SELECT
                   a.*,
                   COALESCE(a.property_address, bp.property_address) AS resolved_address,
                   COALESCE(a.beds, bp.beds) AS resolved_beds,
                   COALESCE(a.baths, bp.baths) AS resolved_baths,
                   COALESCE(a.heated_area, bp.heated_area) AS resolved_sqft,
                   fs.sale_date as resale_date,
                   fs.sale_amount as resale_price,
                   fs.gross_profit,
                   fs.roi,
                   fs.hold_days as hold_time
            FROM foreclosures_history a
            LEFT JOIN LATERAL (
                SELECT bp2.property_address, bp2.beds, bp2.baths, bp2.heated_area
                FROM hcpa_bulk_parcels bp2
                WHERE (a.strap IS NOT NULL AND bp2.strap = a.strap)
                   OR (a.folio IS NOT NULL AND bp2.folio = a.folio)
                ORDER BY bp2.source_file_id DESC NULLS LAST
                LIMIT 1
            ) bp ON TRUE
            LEFT JOIN LATERAL (
                SELECT *
                FROM first_valid_resale(
                    a.folio,
                    a.auction_date,
                    a.winning_bid,
                    a.appraised_value
                )
            ) fs ON TRUE
            ORDER BY
                CASE WHEN fs.sale_date IS NULL THEN 1 ELSE 0 END,
                fs.sale_date DESC,
                a.auction_date DESC
            LIMIT :limit
        """)
        
        with _pg_engine().connect() as conn:
            rows = conn.execute(query, {"limit": limit}).fetchall()
            
        data = []
        for r in rows:
            row_dict = dict(r._asdict())
            listing_id = str(row_dict.get("listing_id") or "").strip()
            html_path = str(row_dict.get("html_path") or "")
            slug = ""
            if html_path:
                file_name = Path(html_path).name
                if "_" in file_name:
                    slug = file_name.split("_", 1)[1].rsplit(".", 1)[0]
            if listing_id:
                if slug:
                    pdf_url = f"https://www.hillsforeclosures.com/property-info/{listing_id}/{slug}"
                else:
                    pdf_url = f"https://www.hillsforeclosures.com/property-info/{listing_id}"
            else:
                pdf_url = None

            data.append({
                "auction_date": str(row_dict["auction_date"]),
                "case_number": row_dict["case_number_raw"],
                "address": row_dict["resolved_address"] or row_dict["property_address"],
                "buyer": row_dict["sold_to"],
                "folio": row_dict["folio"],
                "strap": row_dict["strap"],
                "bedrooms": float(row_dict["resolved_beds"]) if row_dict["resolved_beds"] else None,
                "bathrooms": float(row_dict["resolved_baths"]) if row_dict["resolved_baths"] else None,
                "sqft": int(row_dict["resolved_sqft"]) if row_dict["resolved_sqft"] else None,
                "winning_bid": float(row_dict["winning_bid"]) if row_dict["winning_bid"] else 0,
                "debt": float(row_dict["final_judgment_amount"]) if row_dict["final_judgment_amount"] else 0,
                "resale_date": str(row_dict["resale_date"]) if row_dict["resale_date"] else None,
                "resale_price": float(row_dict["resale_price"]) if row_dict["resale_price"] else None,
                "profit": float(row_dict["gross_profit"]) if row_dict["gross_profit"] is not None else None,
                "roi": float(row_dict["roi"]) if row_dict["roi"] is not None else None,
                "hold_time": int(row_dict["hold_time"]) if row_dict["hold_time"] is not None else None,
                "status": row_dict["auction_status"],
                "photo_urls": row_dict.get("photo_urls"),
                "permits_between_sale": 0,
                "permit_cost_between_sale": None,
                "pdf_url": pdf_url,
                "research_url": f"/property/{row_dict['strap']}" if row_dict["strap"] else None,
            })
        return data
    except Exception as e:
        logger.error(f"Error fetching history data: {e}")
        return []


@router.get("/history/chain-gaps/{identifier}")
async def history_chain_gaps(identifier: str):
    """Return chain diagnostics for a case/strap/folio identifier."""
    try:
        folio = _resolve_folio_for_history(identifier)
        if not folio:
            return {
                "folio": None,
                "gaps": [
                    {
                        "gap_type": "NO_FOLIO_MATCH",
                        "detail": "Could not resolve identifier to folio.",
                        "recommended_source": "HCPA",
                    }
                ],
            }

        with _pg_engine().connect() as conn:
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

        return {"folio": folio, "gaps": [dict(r) for r in rows]}
    except Exception as e:
        logger.error(f"Error fetching chain gaps for {identifier}: {e}")
        return {"folio": None, "gaps": []}
