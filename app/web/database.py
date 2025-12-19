"""
Database queries for web interface.
Uses the existing PropertyDB from src/db/operations.py
"""
import duckdb
import json
import os
from contextlib import suppress
from datetime import UTC, date, datetime, timedelta
from typing import List, Dict, Any, Optional
from pathlib import Path
from loguru import logger


def _resolve_db_path() -> Path:
    """Prefer a web-safe snapshot DB if present, else fall back to the main file."""
    data_dir = Path(__file__).resolve().parents[2] / "data"
    # Allow override via env for explicit control
    env_path = os.getenv("HILLS_WEB_DB")
    if env_path:
        return Path(env_path).expanduser().resolve()

    web_path = data_dir / "property_master_web.db"
    if web_path.exists():
        return web_path.resolve()

    return (data_dir / "property_master.db").resolve()


DB_PATH = _resolve_db_path()
SCRAPER_DB_PATH = Path(__file__).resolve().parents[2] / "data" / "scraper_outputs.db"


def get_connection() -> duckdb.DuckDBPyConnection:
    """Get a database connection."""
    return duckdb.connect(str(DB_PATH), read_only=True)


def get_upcoming_auctions(
    days_ahead: int = 60,
    auction_type: Optional[str] = None,
    sort_by: str = "auction_date",
    sort_order: str = "asc",
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """
    Get upcoming auctions with property details.

    Args:
        days_ahead: Number of days to look ahead
        auction_type: Filter by FORECLOSURE or TAX_DEED
        sort_by: Column to sort by
        sort_order: asc or desc
        limit: Max results
        offset: Pagination offset

    Returns:
        List of auction dicts with joined property data
    """
    conn = get_connection()

    today = datetime.now(tz=UTC).date()
    end_date = today + timedelta(days=days_ahead)

    # Build query with optional joins to bulk_parcels
    # Note: auctions.folio matches bulk_parcels.strap (not folio)
    query = """
        SELECT
            a.id,
            a.case_number,
            a.folio,
            a.auction_type,
            a.auction_date,
            a.property_address,
            a.assessed_value,
            a.final_judgment_amount,
            a.opening_bid,
            a.est_surviving_debt,
            a.is_toxic_title,
            a.status,
            a.plaintiff_max_bid,
            -- Joined from bulk_parcels (if available)
            bp.owner_name,
            bp.beds,
            bp.baths,
            bp.heated_area,
            bp.year_built,
            bp.market_value as hcpa_market_value,
            bp.land_use_desc,
            -- Calculated fields
            COALESCE(bp.market_value, a.assessed_value, 0) -
                COALESCE(a.final_judgment_amount, 0) -
                COALESCE(a.est_surviving_debt, 0) as net_equity
        FROM auctions a
        LEFT JOIN bulk_parcels bp ON a.folio = bp.strap
        WHERE a.auction_date >= ? AND a.auction_date <= ?
    """

    params = [today, end_date]

    if auction_type:
        query += " AND a.auction_type = ?"
        params.append(auction_type)

    # Validate sort column to prevent SQL injection
    valid_sort_cols = [
        "auction_date", "property_address", "assessed_value",
        "final_judgment_amount", "net_equity", "case_number"
    ]
    if sort_by not in valid_sort_cols:
        sort_by = "auction_date"

    sort_dir = "DESC" if sort_order.lower() == "desc" else "ASC"
    query += f" ORDER BY {sort_by} {sort_dir}"
    query += f" LIMIT {limit} OFFSET {offset}"

    try:
        results = conn.execute(query, params).fetchall()
        columns = [desc[0] for desc in conn.description]
        return [dict(zip(columns, row, strict=True)) for row in results]
    except Exception as e:
        logger.error(f"Error fetching auctions: {e}")
        # Fallback query without bulk_parcels join
        fallback_query = """
            SELECT
                a.*,
                NULL as owner_name,
                NULL as beds,
                NULL as baths,
                NULL as heated_area,
                NULL as year_built,
                NULL as hcpa_market_value,
                NULL as land_use_desc,
                COALESCE(a.assessed_value, 0) -
                    COALESCE(a.final_judgment_amount, 0) -
                    COALESCE(a.est_surviving_debt, 0) as net_equity
            FROM auctions a
            WHERE a.auction_date >= ? AND a.auction_date <= ?
            ORDER BY auction_date ASC
            LIMIT ? OFFSET ?
        """
        results = conn.execute(fallback_query, [today, end_date, limit, offset]).fetchall()
        columns = [desc[0] for desc in conn.description]
        return [dict(zip(columns, row, strict=True)) for row in results]
    finally:
        conn.close()


def get_auction_map_points(days_ahead: int = 60) -> List[Dict[str, Any]]:
    """Return auctions with lat/lon for map display (with graceful fallbacks)."""
    conn = get_connection()
    try:
        base_query = """
            SELECT
                a.case_number,
                a.auction_date,
                a.auction_type,
                a.property_address,
                a.final_judgment_amount,
                COALESCE(p.latitude, NULL) as latitude,
                COALESCE(p.longitude, NULL) as longitude,
                a.folio
            FROM auctions a
            LEFT JOIN parcels p ON a.folio = p.folio OR a.parcel_id = p.parcel_id
            {where_clause}
            ORDER BY a.auction_date
            LIMIT 200
        """
        upcoming_clause = f"WHERE a.auction_date >= CURRENT_DATE AND a.auction_date <= CURRENT_DATE + INTERVAL {days_ahead} DAY"
        results = conn.execute(base_query.format(where_clause=upcoming_clause)).fetchall()

        # Fallback: if no upcoming, pull recent/all
        if not results:
            recent_clause = "WHERE a.auction_date >= CURRENT_DATE - INTERVAL 180 DAY"
            results = conn.execute(base_query.format(where_clause=recent_clause)).fetchall()
        if not results:
            results = conn.execute(base_query.format(where_clause="")).fetchall()

        columns = [desc[0] for desc in conn.description]
        rows = [dict(zip(columns, row, strict=True)) for row in results]

        def fallback_coords(key: str) -> tuple[float, float]:
            """Deterministic jitter within Hillsborough area when no lat/lon stored."""
            import hashlib

            if not key:
                key = "default"
            h = hashlib.md5(key.encode()).hexdigest()
            # Tampa-ish box
            lat_min, lat_max = 27.6, 28.2
            lon_min, lon_max = -82.8, -82.0
            lat_span = lat_max - lat_min
            lon_span = lon_max - lon_min
            # Use hash slices for reproducible offsets
            lat_offset = int(h[:8], 16) / 0xFFFFFFFF
            lon_offset = int(h[8:16], 16) / 0xFFFFFFFF
            return lat_min + lat_span * lat_offset, lon_min + lon_span * lon_offset

        for r in rows:
            if r.get("latitude") is None or r.get("longitude") is None:
                # Fill synthetic coords so the map can render markers even without geocode
                lat, lon = fallback_coords(r.get("folio") or r.get("case_number") or r.get("property_address", ""))
                r["latitude"], r["longitude"] = lat, lon
        return rows
    except Exception as e:
        logger.error(f"Error fetching map points: {e}")
        return []
    finally:
        conn.close()


def get_auction_count(
    days_ahead: int = 60,
    auction_type: Optional[str] = None
) -> int:
    """Get total count of upcoming auctions."""
    conn = get_connection()

    today = datetime.now(tz=UTC).date()
    end_date = today + timedelta(days=days_ahead)

    query = """
        SELECT COUNT(*) FROM auctions
        WHERE auction_date >= ? AND auction_date <= ?
    """
    params = [today, end_date]

    if auction_type:
        query += " AND auction_type = ?"
        params.append(auction_type)

    try:
        result = conn.execute(query, params).fetchone()
        return result[0] if result else 0
    except Exception as e:
        logger.error(f"Error counting auctions: {e}")
        return 0
    finally:
        conn.close()


def get_property_detail(folio: str) -> Optional[Dict[str, Any]]:
    """
    Get full property details including auction, liens, and parcel data.

    Args:
        folio: Property folio number

    Returns:
        Dict with all property data or None
    """
    conn = get_connection()

    try:
        # Get auction data
        auction_query = """
            SELECT * FROM auctions WHERE folio = ?
            ORDER BY auction_date DESC LIMIT 1
        """
        auction_result = conn.execute(auction_query, [folio]).fetchone()

        if not auction_result:
            # Try by case_number if folio not found
            return None

        auction_cols = [desc[0] for desc in conn.description]
        auction = dict(zip(auction_cols, auction_result, strict=True))

        # Parse extracted judgment JSON for templates (DuckDB may return as str)
        with suppress(Exception):
            if isinstance(auction.get("extracted_judgment_data"), str):
                auction["extracted_judgment_data"] = json.loads(auction["extracted_judgment_data"])

        # Get bulk parcel data (join on strap, not folio)
        parcel = None
        try:
            parcel_query = "SELECT * FROM bulk_parcels WHERE strap = ?"
            parcel_result = conn.execute(parcel_query, [folio]).fetchone()
            if parcel_result:
                parcel_cols = [desc[0] for desc in conn.description]
                parcel = dict(zip(parcel_cols, parcel_result, strict=True))
        except Exception as err:
            logger.debug(f"bulk_parcels lookup failed for {folio}: {err}")

        # Get liens
        liens = []
        try:
            liens_query = """
                SELECT * FROM liens
                WHERE case_number = ?
                ORDER BY recording_date
            """
            liens_result = conn.execute(liens_query, [auction.get("case_number")]).fetchall()
            if liens_result:
                liens_cols = [desc[0] for desc in conn.description]
                liens = [dict(zip(liens_cols, row, strict=True)) for row in liens_result]
        except Exception as err:
            logger.debug(f"Error fetching liens for {folio}: {err}")

        # Get encumbrances (from chain analysis)
        encumbrances = []
        try:
            enc_query = """
                SELECT * FROM encumbrances
                WHERE folio = ?
                ORDER BY recording_date
            """
            enc_result = conn.execute(enc_query, [folio]).fetchall()
            if enc_result:
                enc_cols = [desc[0] for desc in conn.description]
                encumbrances = [dict(zip(enc_cols, row, strict=True)) for row in enc_result]
        except Exception as err:
            logger.debug(f"Error fetching encumbrances for {folio}: {err}")

        # Get Chain of Title
        chain = []
        try:
            chain_query = """
                SELECT * FROM chain_of_title
                WHERE folio = ?
                ORDER BY acquisition_date
            """
            chain_result = conn.execute(chain_query, [folio]).fetchall()
            if chain_result:
                chain_cols = [desc[0] for desc in conn.description]
                chain = [dict(zip(chain_cols, row, strict=True)) for row in chain_result]
        except Exception as err:
            logger.debug(f"Error fetching chain for {folio}: {err}")

        # Calculate net equity
        enrichments = get_property_enrichments(folio)
        market = get_market_snapshot(folio)

        market_value = (
            market.get("blended_estimate")
            or (parcel or {}).get("market_value")
            or auction.get("assessed_value")
            or 0
        )
        judgment = auction.get("final_judgment_amount") or 0
        surviving = auction.get("est_surviving_debt") or 0
        net_equity = market_value - judgment - surviving

        return {
            "folio": folio,
            "auction": auction,
            "parcel": parcel,
            "liens": liens,
            "encumbrances": encumbrances,
            "chain": chain,
            "nocs": get_nocs_for_property(folio),
            "sales": get_sales_history(folio),
            "net_equity": net_equity,
            "market_value": market_value,
            "market": market,
            "enrichments": enrichments,
            "sources": get_sources_for_property(folio)
        }

    except Exception as e:
        logger.error(f"Error fetching property {folio}: {e}")
        return None
    finally:
        conn.close()


def get_property_by_case(case_number: str) -> Optional[Dict[str, Any]]:
    """Get property by case number instead of folio."""
    conn = get_connection()

    try:
        query = "SELECT folio FROM auctions WHERE case_number = ?"
        result = conn.execute(query, [case_number]).fetchone()
        if result and result[0]:
            return get_property_detail(result[0])
        return None
    except Exception as e:
        logger.error(f"Error fetching by case {case_number}: {e}")
        return None
    finally:
        conn.close()


def get_sources_for_property(folio: str) -> List[Dict[str, Any]]:
    """Get data sources for a property."""
    conn = get_connection()
    try:
        # Check if table exists first
        try:
            conn.execute("SELECT 1 FROM property_sources LIMIT 1")
        except Exception:
            return []
            
        query = """
            SELECT * FROM property_sources
            WHERE folio = ?
            ORDER BY created_at DESC
        """
        results = conn.execute(query, [folio]).fetchall()
        if results:
            columns = [desc[0] for desc in conn.description]
            return [dict(zip(columns, row, strict=True)) for row in results]
        return []
    except Exception as e:
        logger.warning(f"Error fetching sources: {e}")
        return []
    finally:
        conn.close()


def get_nocs_for_property(folio: str) -> List[Dict[str, Any]]:
    """Fetch Notice of Commencement documents for a folio."""
    conn = get_connection()
    try:
        results = conn.execute(
            """
            SELECT
                id,
                document_type,
                recording_date,
                instrument_number,
                party1,
                party2,
                legal_description,
                ocr_text,
                file_path
            FROM documents
            WHERE folio = ?
              AND (
                  LOWER(COALESCE(document_type, '')) LIKE '%notice of commencement%'
                  OR LOWER(COALESCE(document_type, '')) LIKE '%noc%'
              )
            ORDER BY recording_date DESC NULLS LAST, created_at DESC
            """,
            [folio],
        ).fetchall()
        if not results:
            return []
        cols = [desc[0] for desc in conn.description]
        return [dict(zip(cols, row, strict=True)) for row in results]
    except Exception as e:
        logger.error(f"Error fetching NOCs for {folio}: {e}")
        return []
    finally:
        conn.close()


def get_permits_for_property(folio: str) -> List[Dict[str, Any]]:
    """Get all permits for a property."""
    conn = get_connection()
    try:
        results = conn.execute(
            """
            SELECT * FROM permits
            WHERE folio = ?
            ORDER BY issue_date DESC NULLS LAST
            """,
            [folio],
        ).fetchall()
        if not results:
            return []
        cols = [desc[0] for desc in conn.description]
        return [dict(zip(cols, row, strict=True)) for row in results]
    except Exception as e:
        logger.error(f"Error fetching permits for {folio}: {e}")
        return []
    finally:
        conn.close()


def search_properties(
    query: str,
    limit: int = 20
) -> List[Dict[str, Any]]:
    """
    Search properties by address, folio, or owner name.

    Args:
        query: Search term
        limit: Max results

    Returns:
        List of matching properties
    """
    conn = get_connection()
    search_term = f"%{query}%"

    try:
        # Search auctions (join on strap, not folio)
        sql = """
            SELECT
                a.folio,
                a.case_number,
                a.property_address,
                a.auction_date,
                a.auction_type,
                bp.owner_name
            FROM auctions a
            LEFT JOIN bulk_parcels bp ON a.folio = bp.strap
            WHERE
                a.property_address ILIKE ?
                OR a.folio ILIKE ?
                OR a.case_number ILIKE ?
                OR bp.owner_name ILIKE ?
            ORDER BY a.auction_date DESC
            LIMIT ?
        """
        results = conn.execute(sql, [search_term, search_term, search_term, search_term, limit]).fetchall()
        columns = [desc[0] for desc in conn.description]
        return [dict(zip(columns, row, strict=True)) for row in results]
    except Exception as e:
        logger.error(f"Error searching: {e}")
        return []
    finally:
        conn.close()


def get_auctions_by_date(auction_date: date) -> List[Dict[str, Any]]:
    """Get all auctions for a specific date."""
    conn = get_connection()

    try:
        # Join on strap, not folio
        query = """
            SELECT
                a.*,
                bp.owner_name,
                bp.beds,
                bp.baths,
                bp.heated_area,
                bp.year_built,
                bp.market_value as hcpa_market_value
            FROM auctions a
            LEFT JOIN bulk_parcels bp ON a.folio = bp.strap
            WHERE a.auction_date = ?
            ORDER BY a.case_number
        """
        results = conn.execute(query, [auction_date]).fetchall()
        columns = [desc[0] for desc in conn.description]
        return [dict(zip(columns, row, strict=True)) for row in results]
    except Exception as e:
        logger.error(f"Error fetching auctions for date {auction_date}: {e}")
        return []
    finally:
        conn.close()


def get_liens_for_property(case_number: str) -> List[Dict[str, Any]]:
    """Get all liens for a property by case number."""
    conn = get_connection()

    try:
        query = """
            SELECT * FROM liens
            WHERE case_number = ?
            ORDER BY recording_date
        """
        results = conn.execute(query, [case_number]).fetchall()
        columns = [desc[0] for desc in conn.description]
        return [dict(zip(columns, row, strict=True)) for row in results]
    except Exception as e:
        logger.error(f"Error fetching liens for {case_number}: {e}")
        return []
    finally:
        conn.close()


def get_documents_for_property(folio: str) -> List[Dict[str, Any]]:
    """Get all documents for a property."""
    conn = get_connection()

    try:
        # Check if documents table exists
        query = """
            SELECT * FROM documents
            WHERE folio = ?
            ORDER BY recording_date DESC
        """
        results = conn.execute(query, [folio]).fetchall()
        columns = [desc[0] for desc in conn.description]
        return [dict(zip(columns, row, strict=True)) for row in results]
    except Exception as e:
        logger.warning(f"Documents table may not exist or error: {e}")
        return []
    finally:
        conn.close()


def get_sales_history(folio: str) -> List[Dict[str, Any]]:
    """Get sales history for a property by folio or strap."""
    conn = get_connection()

    try:
        # Try by strap first (matches auction folio format)
        query = """
            SELECT * FROM sales_history
            WHERE strap = ?
            ORDER BY sale_date DESC
        """
        results = conn.execute(query, [folio]).fetchall()

        if not results:
            # Try by numeric folio
            query = """
                SELECT * FROM sales_history
                WHERE folio = ?
                ORDER BY sale_date DESC
            """
            results = conn.execute(query, [folio]).fetchall()

        if results:
            columns = [desc[0] for desc in conn.description]
            return [dict(zip(columns, row, strict=True)) for row in results]
        return []
    except Exception as e:
        logger.warning(f"Error fetching sales history: {e}")
        return []
    finally:
        conn.close()


def get_document_by_instrument(folio: str, instrument_number: str) -> Optional[Dict[str, Any]]:
    """Get a document by its instrument number for a specific folio."""
    conn = get_connection()
    try:
        query = """
            SELECT * FROM documents
            WHERE folio = ? AND instrument_number = ?
            LIMIT 1
        """
        result = conn.execute(query, [folio, instrument_number]).fetchone()
        if result:
            columns = [desc[0] for desc in conn.description]
            return dict(zip(columns, result, strict=True))
        return None
    except Exception as e:
        logger.warning(f"Error fetching document by instrument {instrument_number}: {e}")
        return None
    finally:
        conn.close()


def get_dashboard_stats() -> Dict[str, Any]:
    """Get summary statistics for the dashboard."""
    conn = get_connection()

    today = datetime.now(tz=UTC).date()

    try:
        stats = {}

        # Total upcoming auctions
        total_row = conn.execute("""
            SELECT COUNT(*) FROM auctions WHERE auction_date >= ?
        """, [today]).fetchone()
        stats["total_auctions"] = total_row[0] if total_row else 0

        # Foreclosures vs Tax Deeds
        fore_row = conn.execute("""
            SELECT COUNT(*) FROM auctions
            WHERE auction_date >= ? AND auction_type = 'FORECLOSURE'
        """, [today]).fetchone()
        stats["foreclosures"] = fore_row[0] if fore_row else 0

        tax_row = conn.execute("""
            SELECT COUNT(*) FROM auctions
            WHERE auction_date >= ? AND auction_type = 'TAX_DEED'
        """, [today]).fetchone()
        stats["tax_deeds"] = tax_row[0] if tax_row else 0

        # Auctions this week
        week_end = today + timedelta(days=7)
        week_row = conn.execute("""
            SELECT COUNT(*) FROM auctions
            WHERE auction_date >= ? AND auction_date <= ?
        """, [today, week_end]).fetchone()
        stats["this_week"] = week_row[0] if week_row else 0

        # Toxic titles flagged
        try:
            toxic_row = conn.execute("""
                SELECT COUNT(*) FROM auctions
                WHERE auction_date >= ? AND is_toxic_title = TRUE
            """, [today]).fetchone()
            stats["toxic_flagged"] = toxic_row[0] if toxic_row else 0
        except Exception:
            stats["toxic_flagged"] = 0

        return stats

    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return {
            "total_auctions": 0,
            "foreclosures": 0,
            "tax_deeds": 0,
            "this_week": 0,
            "toxic_flagged": 0
        }
    finally:
        conn.close()


# -------------------------------------------------------------------------
# Enrichment Data Functions
# -------------------------------------------------------------------------

def _default_enrichments() -> Dict[str, Any]:
    """Default enrichment values used when data is missing."""
    return {
        "flood_zone": None,
        "flood_risk": None,
        "insurance_required": False,
        "permits_total": 0,
        "permits_open": 0,
        "liens_surviving": 0,
        "liens_total_amount": 0,
        "sunbiz_entities": 0,
        "sunbiz_active": 0,
        "market_value": None,
        "zestimate": None,
        "has_enrichments": False
    }


def get_property_enrichments(folio: str) -> Dict[str, Any]:
    """
    Get all enrichment data for a property from scraper outputs.

    Returns dict with:
        - flood_zone: FEMA zone info
        - permits: Open/closed permit counts
        - liens: Surviving lien count and total
        - sunbiz: Owner business entity status
        - market: Zestimate/listing price if available
    """
    bulk = get_bulk_enrichments([folio])
    return bulk.get(folio, _default_enrichments())


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _safe_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        with suppress(Exception):
            return json.loads(value)
    return value


def get_tax_status_for_property(folio: str) -> Dict[str, Any]:
    """Tax status + liens (stored in liens table as document_type LIKE 'TAX%')."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT *
            FROM liens
            WHERE folio = ?
              AND UPPER(COALESCE(document_type, '')) LIKE 'TAX%'
            ORDER BY recording_date
            """,
            [folio],
        ).fetchall()
        if not rows:
            return {"has_tax_liens": False, "total_amount_due": None, "liens": []}
        cols = [desc[0] for desc in conn.description]
        liens = [dict(zip(cols, row, strict=True)) for row in rows]
        amounts = [_safe_float(l.get("amount")) for l in liens]
        total = sum([a for a in amounts if a is not None]) if any(amounts) else None
        return {"has_tax_liens": True, "total_amount_due": total, "liens": liens}
    except Exception as e:
        logger.debug(f"Error fetching tax status for {folio}: {e}")
        return {"has_tax_liens": False, "total_amount_due": None, "liens": []}
    finally:
        conn.close()


def get_market_snapshot(folio: str) -> Dict[str, Any]:
    """
    Market snapshot for a folio.

    - Blended estimate: simple mean of available *estimate* values (no bulk data).
    - Show Zestimate separately from list price.
    - Photos come from latest HomeHarvest record.
    """
    conn = get_connection()
    try:
        # Latest HomeHarvest row
        homeharvest = None
        photos: list[str] = []
        try:
            row = conn.execute(
                """
                SELECT *
                FROM home_harvest
                WHERE folio = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                [folio],
            ).fetchone()
            if row:
                cols = [desc[0] for desc in conn.description]
                homeharvest = dict(zip(cols, row, strict=True))
                primary = (homeharvest.get("primary_photo") or "").strip()
                if primary:
                    photos.append(primary)
                for field in ("photos", "alt_photos"):
                    extra = _safe_json(homeharvest.get(field))
                    if isinstance(extra, list):
                        for url in extra:
                            if isinstance(url, str) and url.strip():
                                photos.append(url.strip())
        except Exception as err:
            logger.debug(f"HomeHarvest lookup failed for {folio}: {err}")

        # Latest market_data per source
        zillow = None
        realtor = None
        try:
            z = conn.execute(
                """
                SELECT *
                FROM market_data
                WHERE folio = ? AND source = 'Zillow'
                ORDER BY created_at DESC
                LIMIT 1
                """,
                [folio],
            ).fetchone()
            if z:
                cols = [desc[0] for desc in conn.description]
                zillow = dict(zip(cols, z, strict=True))
        except Exception as err:
            logger.debug(f"Zillow market_data lookup failed for {folio}: {err}")

        try:
            r = conn.execute(
                """
                SELECT *
                FROM market_data
                WHERE folio = ? AND source = 'Realtor'
                ORDER BY created_at DESC
                LIMIT 1
                """,
                [folio],
            ).fetchone()
            if r:
                cols = [desc[0] for desc in conn.description]
                realtor = dict(zip(cols, r, strict=True))
        except Exception as err:
            logger.debug(f"Realtor market_data lookup failed for {folio}: {err}")

        # Estimates (for blending)
        zestimate = _safe_float((zillow or {}).get("zestimate"))
        homeharvest_estimated = _safe_float((homeharvest or {}).get("estimated_value"))
        realtor_estimate = _safe_float((realtor or {}).get("zestimate"))
        estimate_values = [v for v in [zestimate, homeharvest_estimated, realtor_estimate] if v is not None]
        blended = sum(estimate_values) / len(estimate_values) if estimate_values else None

        # List prices shown separately
        realtor_list_price = _safe_float((realtor or {}).get("list_price"))
        homeharvest_list_price = _safe_float((homeharvest or {}).get("list_price"))

        return {
            "blended_estimate": blended,
            "estimates": {
                "zillow_zestimate": zestimate,
                "homeharvest_estimated_value": homeharvest_estimated,
                "realtor_estimate": realtor_estimate,
            },
            "list_prices": {
                "realtor_list_price": realtor_list_price,
                "homeharvest_list_price": homeharvest_list_price,
            },
            "sources": {
                "zillow": zillow,
                "realtor": realtor,
                "homeharvest": homeharvest,
            },
            "photos": list(dict.fromkeys(photos)),  # stable de-dupe
        }
    finally:
        conn.close()


def get_bulk_homeharvest_photos(folios: List[str]) -> Dict[str, str]:
    """Return latest HomeHarvest primary_photo per folio (for cards)."""
    result: Dict[str, str] = {}
    if not folios:
        return result
    conn = get_connection()
    try:
        placeholders = ",".join(["?"] * len(folios))
        rows = conn.execute(
            f"""
            WITH latest AS (
                SELECT
                    folio,
                    primary_photo,
                    ROW_NUMBER() OVER (PARTITION BY folio ORDER BY created_at DESC) AS rn
                FROM home_harvest
                WHERE folio IN ({placeholders})
            )
            SELECT folio, primary_photo
            FROM latest
            WHERE rn = 1
            """,
            folios,
        ).fetchall()
        for folio, primary_photo in rows:
            if folio and primary_photo:
                result[str(folio)] = str(primary_photo)
        return result
    except Exception as e:
        logger.debug(f"HomeHarvest bulk photo lookup failed: {e}")
        return result
    finally:
        conn.close()


def get_bulk_enrichments(folios: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Get enrichments for multiple properties at once (for grid view).

    Returns dict keyed by folio with enrichment data for each.
    """
    result = {folio: _default_enrichments() for folio in folios}
    if not folios:
        return result

    folios_unique: List[str] = [str(f) for f in {f for f in folios if f}]

    # Batch read scraper_outputs for latest records per scraper/folio
    if SCRAPER_DB_PATH.exists() and folios_unique:
        try:
            conn = duckdb.connect(str(SCRAPER_DB_PATH), read_only=True)
            placeholders = ",".join(["?"] * len(folios_unique))

            for scraper in ["fema", "permits", "sunbiz", "realtor"]:
                try:
                    rows = conn.execute(f"""
                        WITH latest AS (
                            SELECT
                                property_id,
                                extracted_summary,
                                ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY scraped_at DESC) AS rn
                            FROM scraper_outputs
                            WHERE property_id IN ({placeholders})
                              AND scraper = ?
                              AND extraction_success = TRUE
                        )
                        SELECT property_id, extracted_summary
                        FROM latest
                        WHERE rn = 1
                    """, [*folios_unique, scraper]).fetchall()

                    for property_id, summary in rows:
                        if property_id not in result:
                            continue
                        data = result[property_id]
                        data["has_enrichments"] = True
                        with suppress(Exception):
                            summary = json.loads(summary) if isinstance(summary, str) else summary

                        if scraper == "fema":
                            data["flood_zone"] = (summary or {}).get("flood_zone")
                            data["flood_risk"] = (summary or {}).get("risk_level")
                            data["insurance_required"] = (summary or {}).get("insurance_required", False)
                        elif scraper == "permits":
                            data["permits_total"] = (summary or {}).get("total", 0)
                            data["permits_open"] = (summary or {}).get("open", 0)
                        elif scraper == "sunbiz":
                            data["sunbiz_entities"] = (summary or {}).get("found", 0)
                            data["sunbiz_active"] = (summary or {}).get("active", 0)
                        elif scraper == "realtor":
                            data["market_value"] = (summary or {}).get("price")
                            data["zestimate"] = (summary or {}).get("zestimate")
                except Exception as e:
                    logger.debug(f"Error getting {scraper} enrichment batch: {e}")
            conn.close()
        except Exception as e:
            logger.debug(f"Scraper DB not available: {e}")

    # Also get lien data from main DB in one query
    try:
        if folios_unique:
            conn = get_connection()
            placeholders = ",".join(["?"] * len(folios_unique))
            rows = conn.execute(f"""
                SELECT
                    folio,
                    COUNT(*) as total,
                    SUM(CASE WHEN survival_status = 'SURVIVED' THEN 1 ELSE 0 END) as surviving,
                    SUM(CASE WHEN survival_status = 'SURVIVED' THEN COALESCE(amount, 0) ELSE 0 END) as amount
                FROM encumbrances
                WHERE folio IN ({placeholders})
                GROUP BY folio
            """, folios_unique).fetchall()

            for folio, total, surviving, amount in rows:
                if folio not in result:
                    continue
                result[folio]["liens_surviving"] = surviving or 0
                result[folio]["liens_total_amount"] = amount or 0
                if total and total > 0:
                    result[folio]["has_enrichments"] = True

            conn.close()
    except Exception as e:
        logger.debug(f"Error getting liens batch: {e}")

    return result


def get_upcoming_auctions_with_enrichments(
    days_ahead: int = 60,
    auction_type: Optional[str] = None,
    sort_by: str = "auction_date",
    sort_order: str = "asc",
    limit: int = 24,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """
    Get upcoming auctions with enrichment data attached.
    Designed for card-grid view.
    """
    # Get base auctions
    auctions = get_upcoming_auctions(
        days_ahead=days_ahead,
        auction_type=auction_type,
        sort_by=sort_by,
        sort_order=sort_order,
        limit=limit,
        offset=offset
    )

    # Get enrichments for all folios
    folios = [str(a.get("folio")) for a in auctions if a.get("folio")]
    enrichments = get_bulk_enrichments(folios)

    # Attach enrichments to each auction
    photos = get_bulk_homeharvest_photos(folios)
    for auction in auctions:
        folio = auction.get("folio")
        if folio and folio in enrichments:
            auction["enrichments"] = enrichments[folio]
        else:
            auction["enrichments"] = {
                "has_enrichments": False,
                "flood_zone": None,
                "permits_open": 0,
                "liens_surviving": 0
                }
        auction["photo_url"] = photos.get(str(folio)) if folio else None

    return auctions


# -------------------------------------------------------------------------
# Failed HCPA Scrapes (Manual Review Queue)
# -------------------------------------------------------------------------

def get_failed_hcpa_scrapes(
    limit: int = 50,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """
    Get auctions where HCPA scrape failed and need manual review.

    Returns auctions with:
        - case_number
        - parcel_id (folio)
        - property_address
        - auction_date
        - hcpa_scrape_error
    """
    conn = get_connection()

    try:
        # Check if column exists first
        try:
            conn.execute("SELECT hcpa_scrape_failed FROM auctions LIMIT 1")
        except Exception:
            # Column doesn't exist yet
            return []

        query = """
            SELECT
                a.case_number,
                a.folio,
                a.parcel_id,
                a.property_address,
                a.auction_date,
                a.auction_type,
                a.hcpa_scrape_error,
                p.legal_description,
                p.raw_legal1,
                p.raw_legal2
            FROM auctions a
            LEFT JOIN parcels p ON a.parcel_id = p.folio
            WHERE a.hcpa_scrape_failed = TRUE
            ORDER BY a.auction_date ASC
            LIMIT ? OFFSET ?
        """
        results = conn.execute(query, [limit, offset]).fetchall()

        if results:
            columns = [desc[0] for desc in conn.description]
            return [dict(zip(columns, row, strict=True)) for row in results]
        return []

    except Exception as e:
        logger.error(f"Error fetching failed HCPA scrapes: {e}")
        return []
    finally:
        conn.close()


def get_failed_hcpa_count() -> int:
    """Get count of auctions with failed HCPA scrapes."""
    conn = get_connection()

    try:
        # Check if column exists
        try:
            conn.execute("SELECT hcpa_scrape_failed FROM auctions LIMIT 1")
        except Exception:
            return 0

        result = conn.execute("""
            SELECT COUNT(*) FROM auctions WHERE hcpa_scrape_failed = TRUE
        """).fetchone()
        return result[0] if result else 0

    except Exception as e:
        logger.error(f"Error counting failed HCPA scrapes: {e}")
        return 0
    finally:
        conn.close()


def mark_hcpa_reviewed(case_number: str, notes: str | None = None) -> bool:
    """
    Mark an auction as manually reviewed (clears the failed flag).

    Args:
        case_number: The auction case number
        notes: Optional notes about the review

    Returns:
        True if successful
    """
    # Need write connection for this
    try:
        conn = duckdb.connect(str(DB_PATH))  # Not read-only
        conn.execute("""
            UPDATE auctions SET
                hcpa_scrape_failed = FALSE,
                hcpa_scrape_error = ?
            WHERE case_number = ?
        """, [f"Reviewed: {notes}" if notes else "Reviewed", case_number])
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error marking {case_number} as reviewed: {e}")
        return False
