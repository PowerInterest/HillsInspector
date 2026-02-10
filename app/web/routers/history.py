from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
import sqlite3
from pathlib import Path
from loguru import logger

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


def _get_conn() -> sqlite3.Connection | None:
    """Try primary history DB, fallback to snapshot if locked."""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        if _is_lock_error(e) and SNAPSHOT_PATH.exists():
            logger.warning("history.db locked; using snapshot {}", SNAPSHOT_PATH)
            conn = sqlite3.connect(str(SNAPSHOT_PATH))
            conn.row_factory = sqlite3.Row
            return conn
        raise

def get_history_stats():
    """Query history.db for dashboard stats."""
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
                SUM(CASE WHEN buyer_type = 'Third Party' THEN 1 ELSE 0 END) as tp_count,
                COUNT(*) as total
            FROM auctions
        """).fetchone()

        tp_pct = (tp_share[0] / tp_share[1] * 100) if tp_share and tp_share[1] > 0 else 0

        # 3. Success vs Failure (based on ROI)
        roi_stats = conn.execute("""
            SELECT
                SUM(CASE WHEN gross_profit > 0 THEN 1 ELSE 0 END) as profitable,
                SUM(CASE WHEN gross_profit <= 0 THEN 1 ELSE 0 END) as loss,
                AVG(gross_profit) as avg_profit,
                AVG(r.roi) as avg_roi
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
                r.sale_date, r.sale_price, r.gross_profit, r.roi, r.hold_time_days,
                a.status,
                a.pdf_url
            FROM auctions a
            LEFT JOIN resales r ON a.auction_id = r.auction_id
            ORDER BY
                CASE WHEN r.sale_date IS NULL THEN 1 ELSE 0 END,
                (r.sale_date IS NULL), r.sale_date DESC,
                a.auction_date DESC
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
                "roi": r[9],
                "hold_time": r[10],
                "status": r[11],
                "pdf_url": r[12]
            })
        return data
    except Exception as e:
        logger.error(f"Error fetching history data: {e}")
        return []
    finally:
        if conn:
            conn.close()
