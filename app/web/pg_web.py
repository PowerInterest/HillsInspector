"""
PG-only web query helpers for dashboard + API routes.

These functions mirror the old app.web.database interface used by routers,
but source data exclusively from PostgreSQL.
"""

from __future__ import annotations

import datetime as dt
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from loguru import logger
from sqlalchemy import text

from src.utils.time import today_local
from sunbiz.db import get_engine, resolve_pg_dsn


def _engine():
    return get_engine(resolve_pg_dsn())


def _normalize_auction_type(value: str | None) -> str | None:
    if not value:
        return None
    v = value.strip().lower()
    if v in {"foreclosure", "foreclosures"}:
        return "foreclosure"
    if v in {"tax_deed", "tax deed", "taxdeed", "tax-deed"}:
        return "tax_deed"
    return v


def _sort_sql(sort_by: str, sort_order: str) -> str:
    direction = "DESC" if (sort_order or "").lower() == "desc" else "ASC"
    allowed: dict[str, str] = {
        "auction_date": "f.auction_date",
        "property_address": "COALESCE(f.property_address, bp.property_address)",
        "assessed_value": "f.assessed_value",
        "final_judgment_amount": "f.final_judgment_amount",
        "net_equity": (
            "(COALESCE(f.market_value, bp.market_value, 0) - "
            "COALESCE(f.final_judgment_amount, 0) - "
            "COALESCE(enc.est_surviving_debt, 0))"
        ),
    }
    expr = allowed.get(sort_by, "f.auction_date")
    return f"{expr} {direction}, f.foreclosure_id DESC"


def _encumbrance_lateral_join(table_alias: str) -> str:
    """Lateral aggregate for encumbrance counts + estimated surviving debt."""
    return f"""
        LEFT JOIN LATERAL (
            SELECT
                COUNT(*) FILTER (
                    WHERE COALESCE(oe.is_satisfied, FALSE) = FALSE
                ) AS liens_total,
                COUNT(*) FILTER (
                    WHERE COALESCE(oe.is_satisfied, FALSE) = FALSE
                      AND UPPER(COALESCE(oe.survival_status, '')) = 'SURVIVED'
                ) AS liens_survived,
                COUNT(*) FILTER (
                    WHERE COALESCE(oe.is_satisfied, FALSE) = FALSE
                      AND UPPER(COALESCE(oe.survival_status, '')) = 'UNCERTAIN'
                ) AS liens_uncertain,
                COUNT(*) FILTER (
                    WHERE COALESCE(oe.is_satisfied, FALSE) = FALSE
                      AND UPPER(COALESCE(oe.survival_status, '')) IN ('SURVIVED', 'UNCERTAIN')
                ) AS liens_surviving,
                COALESCE(SUM(oe.amount) FILTER (
                    WHERE COALESCE(oe.is_satisfied, FALSE) = FALSE
                      AND UPPER(COALESCE(oe.survival_status, '')) IN ('SURVIVED', 'UNCERTAIN')
                ), 0)::numeric AS est_surviving_debt
            FROM ori_encumbrances oe
            WHERE ({table_alias}.strap IS NOT NULL AND oe.strap = {table_alias}.strap)
               OR ({table_alias}.folio IS NOT NULL AND oe.folio = {table_alias}.folio)
        ) enc ON TRUE
    """


def _jsonable(val: Any) -> Any:
    """Coerce PG types to JSON-safe Python types."""
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, (dt.date, dt.datetime)):
        return val.isoformat()
    return val


def _rows_to_dicts(rows: list[Any]) -> list[dict[str, Any]]:
    return [{k: _jsonable(v) for k, v in r._mapping.items()} for r in rows]  # noqa: SLF001


def get_upcoming_auctions(
    days_ahead: int = 60,
    auction_type: str | None = None,
    sort_by: str = "auction_date",
    sort_order: str = "asc",
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    start_date = today_local()
    end_date = start_date + timedelta(days=days_ahead)
    normalized_type = _normalize_auction_type(auction_type)
    order_sql = _sort_sql(sort_by, sort_order)
    type_clause = ""
    params: dict[str, Any] = {
        "start_date": start_date,
        "end_date": end_date,
        "lim": limit,
        "off": offset,
    }
    if normalized_type is not None:
        type_clause = "AND LOWER(f.auction_type) = :auction_type"
        params["auction_type"] = normalized_type

    sql = text(f"""
        SELECT
            f.foreclosure_id AS id,
            f.case_number_raw AS case_number,
            COALESCE(f.strap, f.folio) AS folio,
            UPPER(COALESCE(f.auction_type, 'foreclosure')) AS auction_type,
            f.auction_date,
            COALESCE(f.property_address, bp.property_address) AS property_address,
            f.assessed_value,
            f.final_judgment_amount,
            f.winning_bid AS opening_bid,
            f.auction_status AS status,
            NULL::numeric AS plaintiff_max_bid,
            COALESCE(f.owner_name, bp.owner_name) AS owner_name,
            COALESCE(f.beds, bp.beds) AS beds,
            COALESCE(f.baths, bp.baths) AS baths,
            COALESCE(f.heated_area, bp.heated_area) AS heated_area,
            COALESCE(f.year_built, bp.year_built) AS year_built,
            COALESCE(f.market_value, bp.market_value) AS hcpa_market_value,
            COALESCE(f.land_use, bp.land_use_desc) AS land_use_desc,
            COALESCE(enc.est_surviving_debt, 0)::numeric AS est_surviving_debt,
            (
                COALESCE(f.unsatisfied_encumbrance_count, 0) > 2
                OR COALESCE(enc.liens_surviving, 0) > 0
            ) AS is_toxic_title,
            (
                COALESCE(f.market_value, bp.market_value, 0)
                - COALESCE(f.final_judgment_amount, 0)
                - COALESCE(enc.est_surviving_debt, 0)
            ) AS net_equity,
            COALESCE(enc.liens_survived, 0)::integer AS liens_survived,
            COALESCE(enc.liens_uncertain, 0)::integer AS liens_uncertain,
            COALESCE(enc.liens_surviving, 0)::integer AS liens_surviving,
            COALESCE(enc.est_surviving_debt, 0)::numeric AS liens_total_amount,
            COALESCE(enc.liens_total, 0)::integer AS liens_total,
            COALESCE(f.latitude, bp.latitude) AS latitude,
            COALESCE(f.longitude, bp.longitude) AS longitude
        FROM foreclosures f
        LEFT JOIN LATERAL (
            SELECT
                bp2.property_address,
                bp2.owner_name,
                bp2.beds,
                bp2.baths,
                bp2.heated_area,
                bp2.year_built,
                bp2.market_value,
                bp2.land_use_desc,
                bp2.latitude,
                bp2.longitude
            FROM hcpa_bulk_parcels bp2
            WHERE (f.strap IS NOT NULL AND bp2.strap = f.strap)
               OR (f.folio IS NOT NULL AND bp2.folio = f.folio)
            ORDER BY bp2.source_file_id DESC NULLS LAST
            LIMIT 1
        ) bp ON TRUE
        {_encumbrance_lateral_join("f")}
        WHERE f.auction_date >= :start_date
          AND f.auction_date <= :end_date
          {type_clause}
        ORDER BY {order_sql}
        LIMIT :lim OFFSET :off
    """)

    try:
        with _engine().connect() as conn:
            rows = conn.execute(
                sql,
                params,
            ).fetchall()
            if not rows:
                fallback_where = ""
                if normalized_type is not None:
                    fallback_where = "WHERE LOWER(f.auction_type) = :auction_type"
                fallback_sql = text(f"""
                    SELECT
                        f.foreclosure_id AS id,
                        f.case_number_raw AS case_number,
                        COALESCE(f.strap, f.folio) AS folio,
                        UPPER(COALESCE(f.auction_type, 'foreclosure')) AS auction_type,
                        f.auction_date,
                        COALESCE(f.property_address, bp.property_address) AS property_address,
                        f.assessed_value,
                        f.final_judgment_amount,
                        f.winning_bid AS opening_bid,
                        f.auction_status AS status,
                        NULL::numeric AS plaintiff_max_bid,
                        COALESCE(f.owner_name, bp.owner_name) AS owner_name,
                        COALESCE(f.beds, bp.beds) AS beds,
                        COALESCE(f.baths, bp.baths) AS baths,
                        COALESCE(f.heated_area, bp.heated_area) AS heated_area,
                        COALESCE(f.year_built, bp.year_built) AS year_built,
                        COALESCE(f.market_value, bp.market_value) AS hcpa_market_value,
                        COALESCE(f.land_use, bp.land_use_desc) AS land_use_desc,
                        COALESCE(enc.est_surviving_debt, 0)::numeric AS est_surviving_debt,
                        (
                            COALESCE(f.unsatisfied_encumbrance_count, 0) > 2
                            OR COALESCE(enc.liens_surviving, 0) > 0
                        ) AS is_toxic_title,
                        (
                            COALESCE(f.market_value, bp.market_value, 0)
                            - COALESCE(f.final_judgment_amount, 0)
                            - COALESCE(enc.est_surviving_debt, 0)
                        ) AS net_equity,
                        COALESCE(enc.liens_survived, 0)::integer AS liens_survived,
                        COALESCE(enc.liens_uncertain, 0)::integer AS liens_uncertain,
                        COALESCE(enc.liens_surviving, 0)::integer AS liens_surviving,
                        COALESCE(enc.est_surviving_debt, 0)::numeric AS liens_total_amount,
                        COALESCE(enc.liens_total, 0)::integer AS liens_total,
                        COALESCE(f.latitude, bp.latitude) AS latitude,
                        COALESCE(f.longitude, bp.longitude) AS longitude
                    FROM foreclosures_history f
                    LEFT JOIN LATERAL (
                        SELECT
                            bp2.property_address,
                            bp2.owner_name,
                            bp2.beds,
                            bp2.baths,
                            bp2.heated_area,
                            bp2.year_built,
                            bp2.market_value,
                            bp2.land_use_desc,
                            bp2.latitude,
                            bp2.longitude
                        FROM hcpa_bulk_parcels bp2
                        WHERE (f.strap IS NOT NULL AND bp2.strap = f.strap)
                           OR (f.folio IS NOT NULL AND bp2.folio = f.folio)
                        ORDER BY bp2.source_file_id DESC NULLS LAST
                        LIMIT 1
                    ) bp ON TRUE
                    {_encumbrance_lateral_join("f")}
                    {fallback_where}
                    ORDER BY {order_sql}
                    LIMIT :lim OFFSET :off
                """)
                fallback_params = {"lim": limit, "off": offset}
                if normalized_type is not None:
                    fallback_params["auction_type"] = normalized_type
                rows = conn.execute(fallback_sql, fallback_params).fetchall()
            return _rows_to_dicts(rows)
    except Exception as e:
        logger.warning(f"get_upcoming_auctions failed: {e}")
        return []


def get_upcoming_auctions_with_enrichments(
    days_ahead: int = 60,
    auction_type: str | None = None,
    sort_by: str = "auction_date",
    sort_order: str = "asc",
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    auctions = get_upcoming_auctions(
        days_ahead=days_ahead,
        auction_type=auction_type,
        sort_by=sort_by,
        sort_order=sort_order,
        limit=limit,
        offset=offset,
    )
    if not auctions:
        return auctions

    foreclosure_ids = [a.get("id") for a in auctions if a.get("id") is not None]
    enrich_by_id: dict[int, dict[str, Any]] = {}
    for auction in auctions:
        foreclosure_id = int(auction.get("id") or 0)
        liens_survived = int(auction.get("liens_survived") or 0)
        liens_uncertain = int(auction.get("liens_uncertain") or 0)
        liens_surviving = int(auction.get("liens_surviving") or 0)
        liens_total_amount = float(auction.get("liens_total_amount") or 0)
        liens_total = int(auction.get("liens_total") or 0)
        enrich_by_id[foreclosure_id] = {
            "permits_total": 0,
            "permits_open": 0,
            "liens_survived": liens_survived,
            "liens_uncertain": liens_uncertain,
            "liens_surviving": liens_surviving,
            "liens_total_amount": liens_total_amount,
            "liens_total": liens_total,
            "flood_zone": None,
            "flood_risk": None,
            "insurance_required": False,
            "has_enrichments": liens_total > 0,
        }
    if foreclosure_ids:
        try:
            with _engine().connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT
                            e.foreclosure_id,
                            COUNT(*) FILTER (
                                WHERE e.event_source IN ('COUNTY_PERMIT', 'TAMPA_PERMIT')
                            ) AS permits_total,
                            COUNT(*) FILTER (
                                WHERE e.event_source IN ('COUNTY_PERMIT', 'TAMPA_PERMIT')
                                  AND e.description !~* '(closed|complete|final|expired)'
                            ) AS permits_open
                        FROM foreclosure_title_events e
                        WHERE e.foreclosure_id = ANY(:ids)
                        GROUP BY e.foreclosure_id
                    """),
                    {"ids": foreclosure_ids},
                ).fetchall()
                for r in rows:
                    m = dict(r._mapping)  # noqa: SLF001
                    foreclosure_id = int(m["foreclosure_id"])
                    base = enrich_by_id.get(
                        foreclosure_id,
                        {
                            "permits_total": 0,
                            "permits_open": 0,
                            "liens_survived": 0,
                            "liens_uncertain": 0,
                            "liens_surviving": 0,
                            "liens_total_amount": 0.0,
                            "liens_total": 0,
                            "flood_zone": None,
                            "flood_risk": None,
                            "insurance_required": False,
                            "has_enrichments": False,
                        },
                    )
                    permits_total = int(m.get("permits_total") or 0)
                    permits_open = int(m.get("permits_open") or 0)
                    base["permits_total"] = permits_total
                    base["permits_open"] = permits_open
                    base["has_enrichments"] = bool(
                        base.get("has_enrichments") or permits_total > 0
                    )
                    enrich_by_id[foreclosure_id] = base
        except Exception as e:
            logger.debug(f"get_upcoming_auctions_with_enrichments aggregation failed: {e}")

    for auction in auctions:
        auction["enrichments"] = enrich_by_id.get(
            int(auction.get("id") or 0),
            {
                "permits_total": 0,
                "permits_open": 0,
                "liens_survived": int(auction.get("liens_survived") or 0),
                "liens_uncertain": int(auction.get("liens_uncertain") or 0),
                "liens_surviving": int(auction.get("liens_surviving") or 0),
                "liens_total_amount": float(auction.get("liens_total_amount") or 0),
                "liens_total": int(auction.get("liens_total") or 0),
                "flood_zone": None,
                "flood_risk": None,
                "insurance_required": False,
                "has_enrichments": bool(auction.get("liens_total")),
            },
        )
    return auctions


def get_auction_count(days_ahead: int = 60, auction_type: str | None = None) -> int:
    start_date = today_local()
    end_date = start_date + timedelta(days=days_ahead)
    normalized_type = _normalize_auction_type(auction_type)
    type_clause = ""
    params: dict[str, Any] = {
        "start_date": start_date,
        "end_date": end_date,
    }
    if normalized_type is not None:
        type_clause = "AND LOWER(auction_type) = :auction_type"
        params["auction_type"] = normalized_type
    try:
        with _engine().connect() as conn:
            row = conn.execute(
                text(f"""
                    SELECT COUNT(*)
                    FROM foreclosures
                    WHERE auction_date >= :start_date
                      AND auction_date <= :end_date
                      {type_clause}
                """),
                params,
            ).fetchone()
            count = int(row[0]) if row and row[0] is not None else 0
            if count > 0:
                return count

            fallback_where = ""
            fallback_params: dict[str, Any] = {}
            if normalized_type is not None:
                fallback_where = "WHERE LOWER(auction_type) = :auction_type"
                fallback_params["auction_type"] = normalized_type

            row = conn.execute(
                text(f"""
                    SELECT COUNT(*)
                    FROM foreclosures_history
                    {fallback_where}
                """),
                fallback_params,
            ).fetchone()
            return int(row[0]) if row and row[0] is not None else 0
    except Exception as e:
        logger.warning(f"get_auction_count failed: {e}")
        return 0


def get_dashboard_stats() -> dict[str, Any]:
    today = today_local()
    week_end = today + timedelta(days=7)
    horizon_end = today + timedelta(days=60)
    try:
        with _engine().connect() as conn:
            row = conn.execute(
                text("""
                    SELECT
                        COUNT(*) FILTER (
                            WHERE auction_date >= :today
                              AND auction_date <= :horizon
                              AND LOWER(auction_type) = 'foreclosure'
                        ) AS foreclosures,
                        COUNT(*) FILTER (
                            WHERE auction_date >= :today
                              AND auction_date <= :horizon
                              AND LOWER(auction_type) = 'tax_deed'
                        ) AS tax_deeds,
                        COUNT(*) FILTER (
                            WHERE auction_date >= :today
                              AND auction_date <= :week_end
                        ) AS this_week,
                        COUNT(*) FILTER (
                            WHERE auction_date >= :today
                              AND auction_date <= :horizon
                              AND COALESCE(unsatisfied_encumbrance_count, 0) > 2
                        ) AS toxic_flagged
                    FROM foreclosures
                """),
                {"today": today, "week_end": week_end, "horizon": horizon_end},
            ).mappings().one()
            stats = dict(row)
            if (
                int(stats.get("foreclosures") or 0) == 0
                and int(stats.get("tax_deeds") or 0) == 0
                and int(stats.get("this_week") or 0) == 0
            ):
                fallback = conn.execute(
                    text("""
                        SELECT
                            COUNT(*) FILTER (
                                WHERE LOWER(auction_type) = 'foreclosure'
                            ) AS foreclosures,
                            COUNT(*) FILTER (
                                WHERE LOWER(auction_type) = 'tax_deed'
                            ) AS tax_deeds,
                            COUNT(*) FILTER (
                                WHERE auction_date >= (
                                    SELECT MAX(auction_date) - INTERVAL '7 days'
                                    FROM foreclosures_history
                                )
                            ) AS this_week,
                            COUNT(*) FILTER (
                                WHERE COALESCE(unsatisfied_encumbrance_count, 0) > 2
                            ) AS toxic_flagged
                        FROM foreclosures_history
                    """)
                ).mappings().one()
                stats = dict(fallback)
            return stats
    except Exception as e:
        logger.warning(f"get_dashboard_stats failed: {e}")
        return {
            "foreclosures": 0,
            "tax_deeds": 0,
            "this_week": 0,
            "toxic_flagged": 0,
        }


def get_auctions_by_date(auction_date: date) -> list[dict[str, Any]]:
    try:
        with _engine().connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT
                        f.foreclosure_id AS id,
                        f.case_number_raw AS case_number,
                        COALESCE(f.strap, f.folio) AS folio,
                        UPPER(COALESCE(f.auction_type, 'foreclosure')) AS auction_type,
                        f.auction_date,
                        COALESCE(f.property_address, bp.property_address) AS property_address,
                        f.assessed_value,
                        f.final_judgment_amount,
                        f.winning_bid AS opening_bid,
                        f.auction_status AS status,
                        COALESCE(f.owner_name, bp.owner_name) AS owner_name,
                        COALESCE(f.beds, bp.beds) AS beds,
                        COALESCE(f.baths, bp.baths) AS baths,
                        COALESCE(f.heated_area, bp.heated_area) AS heated_area,
                        COALESCE(f.year_built, bp.year_built) AS year_built,
                        COALESCE(f.market_value, bp.market_value) AS hcpa_market_value,
                        COALESCE(enc.liens_survived, 0)::integer AS liens_survived,
                        COALESCE(enc.liens_uncertain, 0)::integer AS liens_uncertain,
                        COALESCE(enc.est_surviving_debt, 0)::numeric AS est_surviving_debt,
                        COALESCE(enc.liens_surviving, 0)::integer AS liens_surviving,
                        COALESCE(enc.est_surviving_debt, 0)::numeric AS liens_total_amount,
                        COALESCE(enc.liens_total, 0)::integer AS liens_total,
                        (
                            COALESCE(f.unsatisfied_encumbrance_count, 0) > 2
                            OR COALESCE(enc.liens_surviving, 0) > 0
                        ) AS is_toxic_title,
                        (
                            COALESCE(f.market_value, bp.market_value, 0)
                            - COALESCE(f.final_judgment_amount, 0)
                            - COALESCE(enc.est_surviving_debt, 0)
                        ) AS net_equity
                    FROM foreclosures f
                    LEFT JOIN LATERAL (
                        SELECT
                            bp2.property_address,
                            bp2.owner_name,
                            bp2.beds,
                            bp2.baths,
                            bp2.heated_area,
                            bp2.year_built,
                            bp2.market_value
                        FROM hcpa_bulk_parcels bp2
                        WHERE (f.strap IS NOT NULL AND bp2.strap = f.strap)
                           OR (f.folio IS NOT NULL AND bp2.folio = f.folio)
                        ORDER BY bp2.source_file_id DESC NULLS LAST
                        LIMIT 1
                    ) bp ON TRUE
                    {_encumbrance_lateral_join("f")}
                    WHERE f.auction_date = :auction_date
                    ORDER BY f.case_number_raw
                """),
                {"auction_date": auction_date},
            ).fetchall()
            return _rows_to_dicts(rows)
    except Exception as e:
        logger.warning(f"get_auctions_by_date({auction_date}) failed: {e}")
        return []


def search_properties(query: str, limit: int = 20) -> list[dict[str, Any]]:
    if not query:
        return []
    q = f"%{query.strip()}%"
    try:
        with _engine().connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT
                        COALESCE(f.strap, f.folio) AS folio,
                        f.case_number_raw AS case_number,
                        COALESCE(f.property_address, bp.property_address) AS property_address,
                        f.auction_date,
                        UPPER(COALESCE(f.auction_type, 'foreclosure')) AS auction_type,
                        COALESCE(f.owner_name, bp.owner_name) AS owner_name
                    FROM foreclosures f
                    LEFT JOIN LATERAL (
                        SELECT bp2.property_address, bp2.owner_name
                        FROM hcpa_bulk_parcels bp2
                        WHERE (f.strap IS NOT NULL AND bp2.strap = f.strap)
                           OR (f.folio IS NOT NULL AND bp2.folio = f.folio)
                        ORDER BY bp2.source_file_id DESC NULLS LAST
                        LIMIT 1
                    ) bp ON TRUE
                    WHERE f.case_number_raw ILIKE :q
                       OR f.property_address ILIKE :q
                       OR f.folio ILIKE :q
                       OR f.strap ILIKE :q
                       OR COALESCE(f.owner_name, bp.owner_name, '') ILIKE :q
                    ORDER BY f.auction_date DESC, f.foreclosure_id DESC
                    LIMIT :lim
                """),
                {"q": q, "lim": limit},
            ).fetchall()
            return _rows_to_dicts(rows)
    except Exception as e:
        logger.warning(f"search_properties({query!r}) failed: {e}")
        return []


def get_auction_map_points(days_ahead: int = 60) -> list[dict[str, Any]]:
    start_date = today_local()
    end_date = start_date + timedelta(days=days_ahead)
    map_sql = text("""
        SELECT
            f.case_number_raw AS case_number,
            f.auction_date,
            UPPER(COALESCE(f.auction_type, 'foreclosure')) AS auction_type,
            COALESCE(f.property_address, bp.property_address) AS property_address,
            f.final_judgment_amount,
            COALESCE(f.latitude, bp.latitude) AS latitude,
            COALESCE(f.longitude, bp.longitude) AS longitude,
            COALESCE(f.strap, f.folio) AS folio
        FROM {table_name} f
        LEFT JOIN LATERAL (
            SELECT bp2.property_address, bp2.latitude, bp2.longitude
            FROM hcpa_bulk_parcels bp2
            WHERE (f.strap IS NOT NULL AND bp2.strap = f.strap)
               OR (f.folio IS NOT NULL AND bp2.folio = f.folio)
            ORDER BY bp2.source_file_id DESC NULLS LAST
            LIMIT 1
        ) bp ON TRUE
        {where_clause}
        ORDER BY f.auction_date DESC
        LIMIT 500
    """)
    try:
        with _engine().connect() as conn:
            upcoming_sql = str(map_sql).format(
                table_name="foreclosures",
                where_clause="WHERE f.auction_date >= :start_date AND f.auction_date <= :end_date",
            )
            rows = conn.execute(
                text(upcoming_sql),
                {"start_date": start_date, "end_date": end_date},
            ).fetchall()
            if not rows:
                history_sql = str(map_sql).format(
                    table_name="foreclosures_history",
                    where_clause="",
                )
                rows = conn.execute(text(history_sql)).fetchall()
            return _rows_to_dicts(rows)
    except Exception as e:
        logger.warning(f"get_auction_map_points failed: {e}")
        return []


def check_database_health() -> dict[str, Any]:
    status = {
        "available": False,
        "locked": False,
        "path": "postgresql://hills_sunbiz",
        "exists": True,
        "record_count": None,
        "last_modified": None,
        "error": None,
    }
    try:
        with _engine().connect() as conn:
            row = conn.execute(
                text("""
                    SELECT
                        COUNT(*)::bigint AS record_count,
                        MAX(updated_at) AS last_modified
                    FROM foreclosures
                """)
            ).mappings().one()
            status["available"] = True
            status["record_count"] = int(row.get("record_count") or 0)
            status["last_modified"] = (
                row.get("last_modified").isoformat()
                if row.get("last_modified") is not None
                else None
            )
    except Exception as e:
        status["error"] = str(e)
    return status
