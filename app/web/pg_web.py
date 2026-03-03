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
from sqlalchemy.exc import OperationalError

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
            WHERE (({table_alias}.strap IS NOT NULL AND oe.strap = {table_alias}.strap)
               OR ({table_alias}.folio IS NOT NULL AND oe.folio = {table_alias}.folio))
              AND oe.encumbrance_type != 'noc'
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
            COALESCE(f.longitude, bp.longitude) AS longitude,
            (pm.photo_cdn_urls->>0)::text AS photo_url
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
        LEFT JOIN LATERAL (
            SELECT pm2.photo_cdn_urls
            FROM property_market pm2
            WHERE (f.strap IS NOT NULL AND pm2.strap = f.strap)
               OR (f.folio IS NOT NULL AND pm2.folio = f.folio)
            ORDER BY pm2.updated_at DESC NULLS LAST
            LIMIT 1
        ) pm ON TRUE
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
                        COALESCE(f.longitude, bp.longitude) AS longitude,
                        (pm.photo_cdn_urls->>0)::text AS photo_url
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
                    LEFT JOIN LATERAL (
                        SELECT pm2.photo_cdn_urls
                        FROM property_market pm2
                        WHERE (f.strap IS NOT NULL AND pm2.strap = f.strap)
                           OR (f.folio IS NOT NULL AND pm2.folio = f.folio)
                        ORDER BY pm2.updated_at DESC NULLS LAST
                        LIMIT 1
                    ) pm ON TRUE
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
    except OperationalError:
        logger.exception("get_upcoming_auctions failed")
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
                    base["has_enrichments"] = bool(base.get("has_enrichments") or permits_total > 0)
                    enrich_by_id[foreclosure_id] = base
        except OperationalError:
            logger.exception("get_upcoming_auctions_with_enrichments aggregation failed")

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
    except OperationalError:
        logger.exception("get_auction_count failed")
        return 0


def get_dashboard_stats() -> dict[str, Any]:
    today = today_local()
    week_end = today + timedelta(days=7)
    horizon_end = today + timedelta(days=60)
    try:
        with _engine().connect() as conn:
            row = (
                conn.execute(
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
                )
                .mappings()
                .one()
            )
            stats = dict(row)
            if (
                int(stats.get("foreclosures") or 0) == 0
                and int(stats.get("tax_deeds") or 0) == 0
                and int(stats.get("this_week") or 0) == 0
            ):
                fallback = (
                    conn.execute(
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
                    )
                    .mappings()
                    .one()
                )
                stats = dict(fallback)
            return stats
    except OperationalError:
        logger.exception("get_dashboard_stats failed")
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
    except OperationalError:
        logger.exception(f"get_auctions_by_date({auction_date}) failed")
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
    except OperationalError:
        logger.exception(f"search_properties({query!r}) failed")
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
    except OperationalError:
        logger.exception("get_auction_map_points failed")
        return []


# ---------------------------------------------------------------------------
# Auction Intelligence helpers
# ---------------------------------------------------------------------------


def _next_auction_date(after_2pm: bool = False) -> date:
    """Compute the next auction target date.

    Rules:
    - If before 2 PM EST on a weekday, target = today
    - If after 2 PM EST on a weekday, target = next weekday
    - If weekend, target = next Monday
    Then walk forward until we find a date with auctions (up to 60 days).
    """
    import zoneinfo

    est = zoneinfo.ZoneInfo("America/New_York")
    now = dt.datetime.now(est)
    today = now.date()

    # Determine starting candidate
    if now.weekday() >= 5:  # Saturday or Sunday
        # Jump to next Monday
        days_to_monday = 7 - now.weekday()
        candidate = today + timedelta(days=days_to_monday)
    elif now.hour >= 14 or after_2pm:
        # After 2 PM: advance to next weekday
        candidate = today + timedelta(days=1)
        while candidate.weekday() >= 5:
            candidate += timedelta(days=1)
    else:
        candidate = today

    # Walk forward until we find a date with auctions
    try:
        with _engine().connect() as conn:
            for _ in range(60):
                if candidate.weekday() >= 5:
                    candidate += timedelta(days=1)
                    continue
                row = conn.execute(
                    text("""
                        SELECT COUNT(*) FROM foreclosures
                        WHERE auction_date = :d AND archived_at IS NULL
                    """),
                    {"d": candidate},
                ).fetchone()
                if row and int(row[0]) > 0:
                    return candidate
                candidate += timedelta(days=1)
    except OperationalError:
        logger.exception("_next_auction_date failed")

    return candidate


def _compute_intel_flags(auction: dict[str, Any]) -> dict[str, Any]:
    """Compute intelligence flags for an auction property."""
    flags: list[dict[str, str]] = []
    escrow = float(auction.get("escrow_amount") or 0)
    assessed = float(auction.get("hcpa_market_value") or 0)
    surviving_debt = float(auction.get("est_surviving_debt") or 0)

    # Predicted Max Bid (5% deposit rule => multiply by 20)
    predicted_max_bid = (escrow * 20) if escrow > 0 else None

    # 1. HIGH COMPETITION — multiple parties wired funds
    if auction.get("escrow_multiple_recipients"):
        flags.append({"tag": "HIGH COMPETITION", "color": "red", "icon": "⚔️"})

    # 2. TOXIC BID — surviving liens exceed 25% of predicted max bid
    if predicted_max_bid and surviving_debt > (predicted_max_bid * 0.25):
        flags.append({"tag": "TOXIC BID", "color": "orange", "icon": "☠️"})

    # 3. ANOMALOUS VALUATION — predicted max bid > 140% of assessed value
    if predicted_max_bid and assessed > 0:
        overpay_ratio = predicted_max_bid / assessed
        if overpay_ratio > 1.4:
            flags.append({
                "tag": f"ANOMALOUS VALUE {overpay_ratio:.0%}",
                "color": "purple",
                "icon": "📊",
            })

    # 4. 3RD PARTY INTEREST — escrow entity differs from auction plaintiff
    jd_plaintiff = (auction.get("jd_plaintiff") or "").upper().strip()
    escrow_plaintiff = (auction.get("escrow_plaintiff") or "").upper().strip()
    if escrow > 0 and escrow_plaintiff and jd_plaintiff:
        # Normalise for comparison: strip common suffixes
        jp_core = jd_plaintiff.replace(",", "").replace(".", "")[:30]
        ep_core = escrow_plaintiff.replace(",", "").replace(".", "")[:30]
        if jp_core not in ep_core and ep_core not in jp_core:
            flags.append({"tag": "3RD PARTY INTEREST", "color": "green", "icon": "🎯"})

    # 5. ESCROW SURGE — delta > 20% increase from prior report
    prev = float(auction.get("escrow_previous_amount") or 0)
    delta = float(auction.get("escrow_delta") or 0)
    if prev > 0 and delta > 0 and (delta / prev) > 0.20:
        flags.append({"tag": "ESCROW SURGE", "color": "blue", "icon": "📈"})

    # 6. NO ESCROW — no money deposited at all
    if escrow <= 0:
        flags.append({"tag": "NO ESCROW", "color": "gray", "icon": "💤"})

    auction["predicted_max_bid"] = predicted_max_bid
    auction["intel_flags"] = flags
    return auction


def get_auction_intel_for_date(
    target_date: date | None = None,
) -> tuple[date, list[dict[str, Any]], dict[str, Any]]:
    """Return (auction_date, list_of_enriched_auctions) for the intel dashboard."""
    if target_date is None:
        target_date = _next_auction_date()

    sql = text(f"""
        SELECT
            f.foreclosure_id AS id,
            f.case_number_raw,
            f.case_number_norm,
            COALESCE(f.strap, f.folio) AS folio,
            f.auction_date,
            COALESCE(f.property_address, bp.property_address) AS property_address,
            f.assessed_value,
            f.final_judgment_amount,
            f.winning_bid,
            f.auction_status,
            COALESCE(f.owner_name, bp.owner_name) AS owner_name,
            COALESCE(f.market_value, bp.market_value) AS hcpa_market_value,
            COALESCE(f.beds, bp.beds) AS beds,
            COALESCE(f.baths, bp.baths) AS baths,
            COALESCE(f.heated_area, bp.heated_area) AS heated_area,
            COALESCE(f.year_built, bp.year_built) AS year_built,
            f.judgment_data,
            -- Escrow intel (latest report)
            ta.amount AS escrow_amount,
            ta.in_escrow_since,
            ta.multiple_recipients AS escrow_multiple_recipients,
            ta.plaintiff_name AS escrow_plaintiff,
            ta.counterparty_type,
            ta.movement_type AS escrow_movement_type,
            ta.previous_amount AS escrow_previous_amount,
            ta.delta_amount AS escrow_delta,
            ta.report_date AS escrow_report_date,
            ta.winning_bid_match_count,
            ta.is_pre_auction_signal,
            -- Lien survival
            COALESCE(enc.liens_surviving, 0)::integer AS liens_surviving,
            COALESCE(enc.est_surviving_debt, 0)::numeric AS est_surviving_debt,
            COALESCE(enc.liens_total, 0)::integer AS liens_total,
            COALESCE(enc.liens_survived, 0)::integer AS liens_survived,
            COALESCE(enc.liens_uncertain, 0)::integer AS liens_uncertain,
            -- Net equity
            (
                COALESCE(f.market_value, bp.market_value, 0)
                - COALESCE(f.final_judgment_amount, 0)
                - COALESCE(enc.est_surviving_debt, 0)
            ) AS net_equity,
            -- Photo
            (pm.photo_cdn_urls->>0)::text AS photo_url
        FROM foreclosures f
        LEFT JOIN LATERAL (
            SELECT
                bp2.property_address, bp2.owner_name,
                bp2.beds, bp2.baths, bp2.heated_area,
                bp2.year_built, bp2.market_value,
                bp2.latitude, bp2.longitude
            FROM hcpa_bulk_parcels bp2
            WHERE (f.strap IS NOT NULL AND bp2.strap = f.strap)
               OR (f.folio IS NOT NULL AND bp2.folio = f.folio)
            ORDER BY bp2.source_file_id DESC NULLS LAST
            LIMIT 1
        ) bp ON TRUE
        LEFT JOIN LATERAL (
            SELECT pm2.photo_cdn_urls
            FROM property_market pm2
            WHERE (f.strap IS NOT NULL AND pm2.strap = f.strap)
               OR (f.folio IS NOT NULL AND pm2.folio = f.folio)
            ORDER BY pm2.updated_at DESC NULLS LAST
            LIMIT 1
        ) pm ON TRUE
        LEFT JOIN LATERAL (
            SELECT
                ta2.amount, ta2.in_escrow_since,
                ta2.multiple_recipients, ta2.plaintiff_name,
                ta2.counterparty_type, ta2.movement_type,
                ta2.previous_amount, ta2.delta_amount,
                ta2.report_date, ta2.winning_bid_match_count,
                ta2.is_pre_auction_signal
            FROM "TrustAccount" ta2
            WHERE ta2.source = 'real'
              AND ta2.case_number = f.case_number_norm
              AND ta2.movement_type != 'dropped'
            ORDER BY ta2.report_date DESC
            LIMIT 1
        ) ta ON TRUE
        {_encumbrance_lateral_join("f")}
        WHERE f.auction_date = :target_date
          AND f.archived_at IS NULL
        ORDER BY
            ta.multiple_recipients DESC NULLS LAST,
            ta.amount DESC NULLS LAST,
            f.case_number_raw
    """)

    try:
        with _engine().connect() as conn:
            rows = conn.execute(sql, {"target_date": target_date}).fetchall()
            auctions = _rows_to_dicts(rows)

            # Extract plaintiff from judgment_data JSON
            import json as _json

            for a in auctions:
                jd = a.get("judgment_data")
                if isinstance(jd, str):
                    try:
                        jd = _json.loads(jd)
                    except (ValueError, TypeError):
                        jd = {}
                elif not isinstance(jd, dict):
                    jd = {}
                a["jd_plaintiff"] = jd.get("plaintiff")
                a["jd_defendant"] = jd.get("defendant")
                # Remove raw JSON from template context
                a.pop("judgment_data", None)

            # Compute intelligence flags
            for a in auctions:
                _compute_intel_flags(a)

            # Compute aggregate stats
            total_escrow = sum(float(a.get("escrow_amount") or 0) for a in auctions)
            with_escrow = sum(1 for a in auctions if float(a.get("escrow_amount") or 0) > 0)
            high_competition = sum(1 for a in auctions if a.get("escrow_multiple_recipients"))

            return (
                target_date,
                auctions,
                {
                    "total_escrow": total_escrow,
                    "with_escrow": with_escrow,
                    "high_competition": high_competition,
                    "property_count": len(auctions),
                },
            )
    except OperationalError:
        logger.exception(f"get_auction_intel_for_date({target_date}) failed")
        return (
            target_date,
            [],
            {
                "total_escrow": 0,
                "with_escrow": 0,
                "high_competition": 0,
                "property_count": 0,
            },
        )


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
            row = (
                conn.execute(
                    text("""
                    SELECT
                        COUNT(*)::bigint AS record_count,
                        MAX(updated_at) AS last_modified
                    FROM foreclosures
                """)
                )
                .mappings()
                .one()
            )
            status["available"] = True
            status["record_count"] = int(row.get("record_count") or 0)
            status["last_modified"] = row.get("last_modified").isoformat() if row.get("last_modified") is not None else None
    except Exception as e:
        status["error"] = str(e)
    return status
