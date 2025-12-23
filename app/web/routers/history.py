from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
import duckdb
from pathlib import Path
from loguru import logger
from src.utils.time import ensure_duckdb_utc

router = APIRouter()

DB_PATH = Path("data/history.db")
SNAPSHOT_PATH = Path("data/history_web.db")


def _is_lock_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(phrase in msg for phrase in [
        "could not set lock",
        "database is locked",
        "conflicting lock",
        "lock on file",
        "io error",
    ])


def _get_conn() -> duckdb.DuckDBPyConnection | None:
    """Try primary history DB, fallback to snapshot if locked."""
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        ensure_duckdb_utc(conn)
        return conn
    except Exception as e:
        if _is_lock_error(e) and SNAPSHOT_PATH.exists():
            logger.warning("history.db locked; using snapshot {}", SNAPSHOT_PATH)
            conn = duckdb.connect(str(SNAPSHOT_PATH), read_only=True)
            ensure_duckdb_utc(conn)
            return conn
        raise

def get_history_stats():
    """Query history.db for dashboard stats."""
    # Connect in read_only mode to avoid locking issues with the scraper?
    # DuckDB read_only=True might still require lock if not WAL checkpointed.
    # We will try standard connect.
    conn = None
    try:
        conn = _get_conn()
        if conn is None:
            raise RuntimeError("Unable to open history DB")
        # 1. Total Volume
        row = conn.execute("SELECT COUNT(*) FROM auctions").fetchone()
        total_auctions = row[0] if row else 0
        
        # 2. Third Party Share
        tp_share = conn.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE buyer_type = 'Third Party') as tp_count,
                COUNT(*) as total
            FROM auctions
        """).fetchone()
        
        tp_pct = (tp_share[0] / tp_share[1] * 100) if tp_share and tp_share[1] > 0 else 0
        
        # 3. Success vs Failure (based on ROI)
        roi_stats = conn.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE gross_profit > 0) as profitable,
                COUNT(*) FILTER (WHERE gross_profit <= 0) as loss,
                AVG(gross_profit) as avg_profit,
                AVG(sale_price / NULLIF(winning_bid, 0)) as avg_roi
            FROM resales r
            JOIN auctions a ON r.auction_id = a.auction_id
        """).fetchone()

        # 4. Top Buyers
        top_buyers = conn.execute("""
            SELECT sold_to, COUNT(*) as buys,
                   COALESCE(SUM(winning_bid), 0) as volume,
                   COALESCE(AVG(winning_bid), 0) as avg_price
            FROM auctions
            WHERE buyer_type = 'Third Party'
            GROUP BY sold_to
            ORDER BY buys DESC
            LIMIT 10
        """).fetchall()
        
        return {
            "total_auctions": total_auctions,
            "tp_share_pct": round(tp_pct, 1),
            "profitable_flips": roi_stats[0] if roi_stats else 0,
            "loss_flips": roi_stats[1] if roi_stats else 0,
            "avg_profit": round(roi_stats[2], 0) if roi_stats and roi_stats[2] else 0,
            "avg_roi_mult": round(roi_stats[3], 2) if roi_stats and roi_stats[3] else 0,
            "top_buyers": top_buyers
        }
    except Exception as e:
        logger.error(f"Error querying history stats: {e}")
        return {
            "total_auctions": 0,
            "tp_share_pct": 0,
            "top_buyers": []
        }
    finally:
        if conn:
            conn.close()

@router.get("/history", response_class=HTMLResponse)
async def history_page(request: Request):
    """Render the historical analysis dashboard."""
    from app.web.main import templates
    
    stats = get_history_stats()
    return templates.TemplateResponse("history.html", {
        "request": request,
        "stats": stats,
        "active_tab": "history"
    })

@router.get("/history/data")
async def history_data(limit: int = 100):
    """Return JSON data for the history grid."""
    conn = None
    try:
        conn = _get_conn()
        if conn is None:
            raise RuntimeError("Unable to open history DB")
        
        query = """
            SELECT 
                a.auction_date, a.case_number, a.property_address, a.sold_to, 
                a.winning_bid, a.final_judgment_amount,
                r.sale_date, r.sale_price, r.gross_profit, r.hold_time_days,
                a.pdf_url
            FROM auctions a
            LEFT JOIN resales r ON a.auction_id = r.auction_id
            ORDER BY a.auction_date DESC
            LIMIT ?
        """
        rows = conn.execute(query, [limit]).fetchall()
        
        data = []
        for r in rows:
            data.append({
                "auction_date": str(r[0]),
                "case_number": r[1],
                "address": r[2],
                "buyer": r[3],
                "winning_bid": r[4],
                "debt": r[5],
                "resale_date": str(r[6]) if r[6] else None,
                "resale_price": r[7],
                "profit": r[8],
                "hold_time": r[9],
                "pdf_url": r[10]
            })
        return data
    except Exception as e:
        logger.error(f"Error fetching history data: {e}")
        return []
    finally:
        if conn:
            conn.close()
