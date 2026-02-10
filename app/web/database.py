"""
Database queries for web interface.
Uses the existing PropertyDB from src/db/operations.py
"""
import sqlite3
import json
import os
import time
from contextlib import suppress, contextmanager
from datetime import UTC, date, datetime, timedelta
from typing import List, Dict, Any, Optional
from pathlib import Path
from loguru import logger
from src.utils.time import today_local


# =============================================================================
# Custom Exceptions
# =============================================================================

class DatabaseLockedError(Exception):
    """Raised when database is locked by another process."""


class DatabaseUnavailableError(Exception):
    """Raised when database file is missing or corrupted."""


# =============================================================================
# Database Connection Management
# =============================================================================

def _resolve_db_path() -> Path:
    """Resolve the SQLite database path."""
    data_dir = Path(__file__).resolve().parents[2] / "data"
    # Allow override via env for explicit control
    env_path = os.getenv("HILLS_SQLITE_DB")
    if env_path:
        return Path(env_path).expanduser().resolve()

    return (data_dir / "property_master_sqlite.db").resolve()


DB_PATH = _resolve_db_path()


def _is_lock_error(exc: Exception) -> bool:
    """Check if exception is a database lock error."""
    error_str = str(exc).lower()
    return any(phrase in error_str for phrase in [
        "could not set lock",
        "database is locked",
        "conflicting lock",
        "lock on file",
        "io error",
    ])


def _is_corruption_error(exc: Exception) -> bool:
    """Check if exception indicates database corruption."""
    error_str = str(exc).lower()
    return any(phrase in error_str for phrase in [
        "corrupt",
        "wal file",
        "invalid",
        "malformed",
    ])


def get_connection(retries: int = 2, retry_delay: float = 0.5) -> sqlite3.Connection:
    """
    Get a database connection with retry logic for transient lock errors.

    Args:
        retries: Number of retry attempts for lock errors
        retry_delay: Seconds to wait between retries

    Returns:
        SQLite connection

    Raises:
        DatabaseLockedError: If database is locked after all retries
        DatabaseUnavailableError: If database file is missing or corrupted
    """
    if not Path(DB_PATH).exists():
        raise DatabaseUnavailableError(f"Database file not found: {DB_PATH}")

    for attempt in range(retries + 1):
        try:
            conn = sqlite3.connect(str(DB_PATH))
            conn.row_factory = sqlite3.Row
            # Quick validation query
            conn.execute("SELECT 1").fetchone()
            return conn
        except Exception as e:
            if _is_corruption_error(e):
                raise DatabaseUnavailableError(
                    f"Database appears corrupted: {e}. "
                    "Try removing the WAL file or restoring from backup."
                ) from e

            if _is_lock_error(e):
                if attempt < retries:
                    logger.debug(f"Database locked, retry {attempt + 1}/{retries} in {retry_delay}s")
                    time.sleep(retry_delay)
                    continue
                raise DatabaseLockedError(
                    "Database is locked by another process (likely the pipeline). "
                    "Please wait and try again."
                ) from e

            # Unknown error - don't retry
            raise

    # Should not reach here, but satisfy the type checker
    raise DatabaseUnavailableError("Failed to connect to database")


def get_write_connection(retries: int = 3, retry_delay: float = 1.0) -> sqlite3.Connection:
    """
    Get a write-enabled database connection.

    Args:
        retries: Number of retry attempts for lock errors
        retry_delay: Seconds to wait between retries

    Returns:
        SQLite connection (read-write)

    Raises:
        DatabaseLockedError: If database is locked after all retries
        DatabaseUnavailableError: If database file is missing or corrupted
    """
    if not Path(DB_PATH).exists():
        raise DatabaseUnavailableError(f"Database file not found: {DB_PATH}")

    for attempt in range(retries + 1):
        try:
            conn = sqlite3.connect(str(DB_PATH))
            conn.row_factory = sqlite3.Row
            return conn
        except Exception as e:
            if _is_corruption_error(e):
                raise DatabaseUnavailableError(
                    f"Database appears corrupted: {e}"
                ) from e

            if _is_lock_error(e):
                if attempt < retries:
                    logger.debug(f"Database locked for write, retry {attempt + 1}/{retries} in {retry_delay}s")
                    time.sleep(retry_delay)
                    continue
                raise DatabaseLockedError(
                    "Cannot write to database - locked by another process."
                ) from e

            raise

    # Should not reach here, but satisfy the type checker
    raise DatabaseUnavailableError("Failed to connect to database for write")


@contextmanager
def safe_connection(for_write: bool = False):
    """
    Context manager for safe database connections.

    Usage:
        with safe_connection() as conn:
            results = conn.execute("SELECT ...").fetchall()
    """
    conn = get_write_connection() if for_write else get_connection()
    try:
        yield conn
    finally:
        try:
            if for_write:
                conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"safe_connection cleanup failed (for_write={for_write}): {e}")
            # Attempt final close, but suppress errors if connection is already closed
            with suppress(Exception):
                conn.close()


def check_database_health() -> Dict[str, Any]:
    """
    Check database health status.

    Returns:
        Dict with health info: available, locked, record_count, last_modified
    """
    result = {
        "available": False,
        "locked": False,
        "path": str(DB_PATH),
        "exists": Path(DB_PATH).exists(),
        "record_count": None,
        "last_modified": None,
        "error": None
    }

    if not result["exists"]:
        result["error"] = "Database file not found"
        return result

    try:
        # Get file modification time
        stat = Path(DB_PATH).stat()
        result["last_modified"] = datetime.fromtimestamp(stat.st_mtime, tz=UTC).isoformat()

        # Try to connect and query
        conn = get_connection(retries=0)
        try:
            row = conn.execute("SELECT COUNT(*) FROM auctions").fetchone()
            count = row[0] if row else 0
            result["record_count"] = count
            result["available"] = True
        finally:
            conn.close()

    except DatabaseLockedError:
        result["locked"] = True
        result["error"] = "Database is locked by another process"
    except DatabaseUnavailableError as e:
        result["error"] = str(e)
    except Exception as e:
        result["error"] = f"{type(e).__name__}: {e}"

    return result


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

    today = today_local()
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
        return [dict(row) for row in results]
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
        return [dict(row) for row in results]
    finally:
        conn.close()


def get_auction_map_points(days_ahead: int = 60) -> List[Dict[str, Any]]:
    """Return auctions with lat/lon for map display (with graceful fallbacks)."""
    conn = get_connection()
    try:
        today = today_local()
        end_date = today + timedelta(days=days_ahead)
        past_date = today - timedelta(days=180)

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
        upcoming_clause = "WHERE a.auction_date >= ? AND a.auction_date <= ?"
        results = conn.execute(base_query.format(where_clause=upcoming_clause), [str(today), str(end_date)]).fetchall()

        # Fallback: if no upcoming, pull recent/all
        if not results:
            recent_clause = "WHERE a.auction_date >= ?"
            results = conn.execute(base_query.format(where_clause=recent_clause), [str(past_date)]).fetchall()
        if not results:
            results = conn.execute(base_query.format(where_clause="")).fetchall()

        rows = [dict(row) for row in results]

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

    today = today_local()
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

        auction = dict(auction_result)

        # Parse extracted judgment JSON for templates
        try:
            if isinstance(auction.get("extracted_judgment_data"), str):
                auction["extracted_judgment_data"] = json.loads(auction["extracted_judgment_data"])
        except Exception as e:
            logger.debug(f"Could not parse judgment JSON for display: {e}")

        # Get bulk parcel data (join on strap, not folio)
        parcel = None
        try:
            parcel_query = "SELECT * FROM bulk_parcels WHERE strap = ?"
            parcel_result = conn.execute(parcel_query, [folio]).fetchone()
            if parcel_result:
                parcel = dict(parcel_result)
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
                liens = [dict(row) for row in liens_result]
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
                encumbrances = [dict(row) for row in enc_result]
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
                chain = [dict(row) for row in chain_result]
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
        except sqlite3.OperationalError as e:
            logger.debug(f"Table/column check failed: {e}")
            return []

        query = """
            SELECT * FROM property_sources
            WHERE folio = ?
            ORDER BY created_at DESC
        """
        results = conn.execute(query, [folio]).fetchall()
        if results:
            return [dict(row) for row in results]
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
            ORDER BY (recording_date IS NULL), recording_date DESC, created_at DESC
            """,
            [folio],
        ).fetchall()
        if not results:
            return []
        return [dict(row) for row in results]
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
            ORDER BY (issue_date IS NULL), issue_date DESC
            """,
            [folio],
        ).fetchall()
        if not results:
            return []
        return [dict(row) for row in results]
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
                a.property_address LIKE ?
                OR a.folio LIKE ?
                OR a.case_number LIKE ?
                OR bp.owner_name LIKE ?
            ORDER BY a.auction_date DESC
            LIMIT ?
        """
        results = conn.execute(sql, [search_term, search_term, search_term, search_term, limit]).fetchall()
        return [dict(row) for row in results]
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
        return [dict(row) for row in results]
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
        return [dict(row) for row in results]
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
        return [dict(row) for row in results]
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
            return [dict(row) for row in results]
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
        return dict(result) if result else None
    except Exception as e:
        logger.warning(f"Error fetching document by instrument {instrument_number}: {e}")
        return None
    finally:
        conn.close()


def get_dashboard_stats() -> Dict[str, Any]:
    """Get summary statistics for the dashboard."""
    conn = get_connection()

    today = today_local()

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
                WHERE auction_date >= ? AND is_toxic_title = 1
            """, [today]).fetchone()
            stats["toxic_flagged"] = toxic_row[0] if toxic_row else 0
        except sqlite3.OperationalError as e:
            logger.debug(f"is_toxic_title column check failed: {e}")
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
        liens = [dict(row) for row in rows]
        amounts = [_safe_float(lien.get("amount")) for lien in liens]
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
                homeharvest = dict(row)
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
                zillow = dict(z)
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
                realtor = dict(r)
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
    if folios_unique:
        try:
            conn = sqlite3.connect(str(DB_PATH))
            conn.row_factory = sqlite3.Row
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
                              AND extraction_success = 1
                        )
                        SELECT property_id, extracted_summary
                        FROM latest
                        WHERE rn = 1
                    """, [*folios_unique, scraper]).fetchall()

                    for row in rows:
                        property_id = row[0]
                        summary = row[1]
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
            logger.debug(f"Scraper outputs not available: {e}")

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
        except sqlite3.OperationalError as e:
            logger.debug(f"Table/column check failed: {e}")
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
                COALESCE(p.legal_description, bp.raw_legal1) as legal_description,
                bp.raw_legal1,
                bp.raw_legal2
            FROM auctions a
            LEFT JOIN parcels p ON a.folio = p.folio
            LEFT JOIN bulk_parcels bp ON a.folio = bp.strap
            WHERE a.hcpa_scrape_failed = 1
            ORDER BY a.auction_date ASC
            LIMIT ? OFFSET ?
        """
        results = conn.execute(query, [limit, offset]).fetchall()

        if results:
            return [dict(row) for row in results]
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
        except sqlite3.OperationalError as e:
            logger.debug(f"Table/column check failed: {e}")
            return 0

        result = conn.execute("""
            SELECT COUNT(*) FROM auctions WHERE hcpa_scrape_failed = 1
        """).fetchone()
        return result[0] if result else 0

    except Exception as e:
        logger.error(f"Error counting failed HCPA scrapes: {e}")
        return 0
    finally:
        conn.close()


def get_judgment_data(folio: str) -> Optional[Dict[str, Any]]:
    """
    Get extracted judgment data for a property.

    Returns the parsed JSON from extracted_judgment_data column.
    """
    conn = get_connection()
    try:
        query = """
            SELECT
                case_number,
                extracted_judgment_data,
                raw_judgment_text,
                judgment_extracted_at,
                final_judgment_amount,
                principal_amount,
                interest_amount,
                attorney_fees,
                court_costs,
                foreclosure_type,
                lis_pendens_date,
                plaintiff,
                defendant
            FROM auctions
            WHERE folio = ?
            ORDER BY auction_date DESC
            LIMIT 1
        """
        result = conn.execute(query, [folio]).fetchone()
        if not result:
            return None

        data = dict(result)

        # Parse JSON if string
        try:
            if isinstance(data.get("extracted_judgment_data"), str):
                data["extracted_judgment_data"] = json.loads(data["extracted_judgment_data"])
        except Exception as e:
            logger.debug(f"Could not parse judgment JSON for display: {e}")

        return data
    except Exception as e:
        logger.error(f"Error fetching judgment data for {folio}: {e}")
        return None
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

    Raises:
        DatabaseLockedError: If database is locked
        DatabaseUnavailableError: If database is unavailable
    """
    conn = get_write_connection()
    try:
        conn.execute("""
            UPDATE auctions SET
                hcpa_scrape_failed = 0,
                hcpa_scrape_error = ?
            WHERE case_number = ?
        """, [f"Reviewed: {notes}" if notes else "Reviewed", case_number])
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error marking {case_number} as reviewed: {e}")
        raise
    finally:
        conn.close()
