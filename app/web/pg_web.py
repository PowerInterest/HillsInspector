"""
PG-only web query helpers for dashboard + API routes.

These functions mirror the old app.web.database interface used by routers,
but source data exclusively from PostgreSQL.
"""

from __future__ import annotations

import datetime as dt
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import quote

from loguru import logger
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from src.utils.time import today_local
from sunbiz.db import get_engine, resolve_pg_dsn


def _engine():
    return get_engine(resolve_pg_dsn())


def _sql_placeholder_photo_condition(url_expr: str) -> str:
    return f"""
        LOWER(COALESCE({url_expr}, '')) LIKE '%redfin-logo%'
        OR LOWER(COALESCE({url_expr}, '')) LIKE '%/logos/%'
        OR LOWER(COALESCE({url_expr}, '')) LIKE '%no_image%'
        OR LOWER(COALESCE({url_expr}, '')) LIKE '%placeholder%'
        OR LOWER(COALESCE({url_expr}, '')) LIKE '%default_photo%'
        OR LOWER(COALESCE({url_expr}, '')) LIKE '%/images/logos/%'
        OR LOWER(COALESCE({url_expr}, '')) LIKE '%/static/images/%'
    """


def _sql_first_valid_photo(jsonb_expr: str) -> str:
    placeholder_sql = _sql_placeholder_photo_condition("photo_url")
    return f"""
        (
            SELECT photo_url
            FROM jsonb_array_elements_text(
                CASE
                    WHEN jsonb_typeof({jsonb_expr}) = 'array' THEN {jsonb_expr}
                    ELSE '[]'::jsonb
                END
            ) WITH ORDINALITY AS photos(photo_url, ord)
            WHERE NOT ({placeholder_sql})
            ORDER BY ord
            LIMIT 1
        )
    """


def _sql_first_local_photo_path(jsonb_expr: str) -> str:
    return f"""
        CASE
            WHEN jsonb_typeof({jsonb_expr}) = 'array'
                 AND jsonb_array_length({jsonb_expr}) > 0
            THEN {jsonb_expr}->>0
            ELSE NULL
        END
    """


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
    status_expr = "UPPER(COALESCE(fes.survival_status, oe.survival_status, ''))"
    return f"""
        LEFT JOIN LATERAL (
            SELECT
                COUNT(*) FILTER (
                    WHERE COALESCE(oe.is_satisfied, FALSE) = FALSE
                ) AS liens_total,
                COUNT(*) FILTER (
                    WHERE COALESCE(oe.is_satisfied, FALSE) = FALSE
                      AND {status_expr} = 'SURVIVED'
                ) AS liens_survived,
                COUNT(*) FILTER (
                    WHERE COALESCE(oe.is_satisfied, FALSE) = FALSE
                      AND {status_expr} = 'UNCERTAIN'
                ) AS liens_uncertain,
                COUNT(*) FILTER (
                    WHERE COALESCE(oe.is_satisfied, FALSE) = FALSE
                      AND {status_expr} IN ('SURVIVED', 'UNCERTAIN')
                ) AS liens_surviving,
                COALESCE(SUM(oe.amount) FILTER (
                    WHERE COALESCE(oe.is_satisfied, FALSE) = FALSE
                      AND {status_expr} IN ('SURVIVED', 'UNCERTAIN')
                ), 0)::numeric AS est_surviving_debt
            FROM ori_encumbrances oe
            LEFT JOIN foreclosure_encumbrance_survival fes
              ON fes.encumbrance_id = oe.id
             AND fes.foreclosure_id = {table_alias}.foreclosure_id
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
    dict_rows = [{k: _jsonable(v) for k, v in r._mapping.items()} for r in rows]  # noqa: SLF001
    for row in dict_rows:
        photo_local_path = str(row.get("photo_local_path") or "").strip()
        if photo_local_path and not row.get("photo_local_url"):
            filename = Path(photo_local_path).name
            path_parts = Path(photo_local_path).parts
            path_case_number = None
            if len(path_parts) >= 3 and path_parts[0] == "Foreclosure":
                path_case_number = path_parts[1]
            identifier = path_case_number or str(row.get("case_number") or "").strip()
            if filename and identifier:
                row["photo_local_url"] = (
                    f"/property/{quote(identifier)}/photos/{quote(filename)}"
                )
    return dict_rows


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
            {_sql_first_valid_photo("pm.photo_cdn_urls")} AS photo_url,
            {_sql_first_local_photo_path("pm.photo_local_paths")} AS photo_local_path
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
            SELECT pm2.photo_cdn_urls, pm2.photo_local_paths
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
                        {_sql_first_valid_photo("pm.photo_cdn_urls")} AS photo_url,
                        {_sql_first_local_photo_path("pm.photo_local_paths")} AS photo_local_path
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
                        SELECT pm2.photo_cdn_urls, pm2.photo_local_paths
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
                text(f"""
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


def _normalize_bank_name(name: str) -> str:
    """Normalize a bank/entity name for fuzzy matching across data sources."""
    if not name:
        return ""
    name = name.upper().strip()
    # Cut at trustee/DBA/capacity qualifiers — keep the core entity name
    for sep in (
        ", AS TRUSTEE", " AS TRUSTEE", ", NOT IN ITS",
        " D/B/A ", " DBA ", " F/K/A ", " FKA ",
        ", FORMERLY", ", AS SUCCESSOR", ", SUCCESSOR",
    ):
        idx = name.find(sep)
        if idx > 5:
            name = name[:idx]
    # Remove common entity suffixes
    for suffix in (
        ", N.A.", " N.A.", " NA", ", LLC", " LLC",
        ", INC.", " INC.", " INC", ", CORP.", " CORP.",
        " CORPORATION", " CORP", " COMPANY", " CO.",
        ", LTD", " LTD", ", LP", " LP",
    ):
        name = name.removesuffix(suffix)
    name = name.replace(",", "").replace(".", "").replace("'", "")
    return " ".join(name.split()).strip()


def _build_bank_escrow_profiles(conn: Any) -> dict[str, dict[str, Any]]:
    """Build escrow behavior profiles keyed by normalized bank name.

    For each bank that appears in TrustAccount data, computes:
    - How many cases they've deposited on (total, active, completed)
    - Median deposit amount
    - Historical win rate (escrow == winning bid = bank won)
    - Active deposits for upcoming auctions
    """
    from collections import defaultdict
    from statistics import median as _median

    # Latest trust account entry per case
    ta_rows = conn.execute(text("""
        SELECT DISTINCT ON (case_number)
            plaintiff_name, case_number, amount
        FROM "TrustAccount"
        WHERE source = 'real'
          AND movement_type <> 'dropped'
          AND amount > 0
          AND counterparty_type = 'bank'
        ORDER BY case_number, report_date DESC
    """)).fetchall()

    if not ta_rows:
        return {}

    # Historical outcomes for those cases
    case_numbers = list({r.case_number for r in ta_rows})
    hist_rows = conn.execute(text("""
        SELECT case_number_norm, winning_bid, final_judgment_amount, auction_status
        FROM foreclosures_history
        WHERE case_number_norm = ANY(:cases)
          AND winning_bid IS NOT NULL AND winning_bid > 0
    """), {"cases": case_numbers}).fetchall()
    hist_map = {r.case_number_norm: r for r in hist_rows}

    # Active foreclosure case numbers
    active_cases = {
        r[0]
        for r in conn.execute(
            text("SELECT case_number_norm FROM foreclosures WHERE archived_at IS NULL")
        ).fetchall()
    }

    # Group by normalized plaintiff name
    by_norm: dict[str, dict] = defaultdict(
        lambda: {"raw_names": set(), "deposits": [], "active": [], "completed": []}
    )

    for r in ta_rows:
        norm = _normalize_bank_name(r.plaintiff_name)
        if not norm or len(norm) < 3:
            continue
        entry = by_norm[norm]
        entry["raw_names"].add(r.plaintiff_name)
        entry["deposits"].append(float(r.amount))

        if r.case_number in active_cases:
            entry["active"].append(
                {"case_number": r.case_number, "amount": float(r.amount)}
            )

        outcome = hist_map.get(r.case_number)
        if outcome:
            entry["completed"].append(
                {
                    "case_number": r.case_number,
                    "escrow": float(r.amount),
                    "winning_bid": float(outcome.winning_bid),
                }
            )

    # Compute profile stats
    profiles: dict[str, dict[str, Any]] = {}
    for norm_name, data in by_norm.items():
        deposits = sorted(data["deposits"])
        completed = data["completed"]

        # Bank won = escrow within 5% of winning bid (no third party outbid)
        bank_wins = sum(
            1
            for c in completed
            if c["winning_bid"] > 0
            and 0.95 <= c["escrow"] / c["winning_bid"] <= 1.05
        )
        third_party_wins = sum(
            1
            for c in completed
            if c["winning_bid"] > 0
            and c["escrow"] / c["winning_bid"] < 0.5
        )

        profiles[norm_name] = {
            "display_name": min(data["raw_names"], key=len),
            "total_cases": len(deposits),
            "median_deposit": _median(deposits),
            "active_cases": data["active"],
            "active_case_count": len(data["active"]),
            "active_total": sum(c["amount"] for c in data["active"]),
            "completed_sold": len(completed),
            "bank_wins": bank_wins,
            "third_party_wins": third_party_wins,
        }

    return profiles


def _build_third_party_bidder_profiles(conn: Any) -> dict[str, dict[str, Any]]:
    """Build profiles for all non-bank trust account depositors.

    Queries TrustAccount for depositors with counterparty_type='unknown' (non-bank),
    deduplicates by (case_number, plaintiff_name) keeping the latest report,
    joins foreclosures_history for outcomes, and computes per-depositor stats.

    Returns dict keyed by normalized depositor name.
    """
    from collections import defaultdict
    from statistics import median as _median

    # Latest trust entry per (case, depositor) — non-bank depositors only
    ta_rows = conn.execute(text("""
        SELECT DISTINCT ON (case_number, plaintiff_name)
            plaintiff_name, case_number, amount, in_escrow_since, report_date
        FROM "TrustAccount"
        WHERE source = 'real'
          AND movement_type <> 'dropped'
          AND amount > 0
          AND counterparty_type = 'unknown'
        ORDER BY case_number, plaintiff_name, report_date DESC
    """)).fetchall()

    if not ta_rows:
        return {}

    # Historical outcomes
    case_numbers = list({r.case_number for r in ta_rows})
    hist_rows = conn.execute(text("""
        SELECT case_number_norm, winning_bid, sold_to, buyer_type,
               auction_status, auction_date
        FROM foreclosures_history
        WHERE case_number_norm = ANY(:cases)
          AND winning_bid IS NOT NULL AND winning_bid > 0
    """), {"cases": case_numbers}).fetchall()
    hist_map = {r.case_number_norm: r for r in hist_rows}

    # Active foreclosure case numbers
    active_cases = {
        r[0]
        for r in conn.execute(
            text("SELECT case_number_norm FROM foreclosures WHERE archived_at IS NULL")
        ).fetchall()
    }

    # Group by normalized depositor name
    by_norm: dict[str, dict] = defaultdict(
        lambda: {
            "raw_names": set(),
            "deposits": [],
            "cases": set(),
            "active_cases": [],
            "escrow_dates": [],
            "completed": [],
        }
    )

    for r in ta_rows:
        norm = _normalize_bank_name(r.plaintiff_name)
        if not norm or len(norm) < 3:
            continue
        entry = by_norm[norm]
        entry["raw_names"].add(r.plaintiff_name)
        entry["deposits"].append(float(r.amount))
        entry["cases"].add(r.case_number)
        if r.in_escrow_since:
            entry["escrow_dates"].append(r.in_escrow_since)

        if r.case_number in active_cases:
            entry["active_cases"].append({
                "case_number": r.case_number,
                "amount": float(r.amount),
                "in_escrow_since": str(r.in_escrow_since) if r.in_escrow_since else None,
            })

        outcome = hist_map.get(r.case_number)
        if outcome:
            entry["completed"].append({
                "case_number": r.case_number,
                "escrow": float(r.amount),
                "winning_bid": float(outcome.winning_bid),
            })

    # Compute profile stats
    profiles: dict[str, dict[str, Any]] = {}
    for norm_name, data in by_norm.items():
        deposits = sorted(data["deposits"])
        completed = data["completed"]

        cases_won = sum(
            1 for c in completed
            if c["winning_bid"] > 0
            and 0.95 <= c["escrow"] / c["winning_bid"] <= 1.05
        )
        cases_outbid = sum(
            1 for c in completed
            if c["winning_bid"] > 0
            and c["winning_bid"] > c["escrow"] * 1.5
        )
        cases_pending = len(data["active_cases"])
        escrow_dates = sorted(data["escrow_dates"])
        latest_deposit = deposits[-1] if deposits else 0

        profiles[norm_name] = {
            "display_name": min(data["raw_names"], key=len),
            "total_cases": len(data["cases"]),
            "total_deposits": sum(deposits),
            "median_deposit": _median(deposits) if deposits else 0,
            "oldest_deposit": str(escrow_dates[0]) if escrow_dates else None,
            "newest_deposit": str(escrow_dates[-1]) if escrow_dates else None,
            "cases_won": cases_won,
            "cases_outbid": cases_outbid,
            "cases_pending": cases_pending,
            "active_case_list": data["active_cases"],
            "max_bid_capacity": latest_deposit * 20,
        }

    return profiles


def _match_bank_profile(
    plaintiff: str, profiles: dict[str, dict[str, Any]]
) -> dict[str, Any] | None:
    """Match a foreclosure plaintiff to a bank escrow profile."""
    if not plaintiff:
        return None
    norm = _normalize_bank_name(plaintiff)
    if not norm or len(norm) < 5:
        return None

    # Direct normalized match
    if norm in profiles:
        return profiles[norm]

    # Substring match (either direction)
    for key, profile in profiles.items():
        if len(key) >= 5 and (norm in key or key in norm):
            return profile

    return None


def _compute_intel_flags(auction: dict[str, Any]) -> dict[str, Any]:
    """Compute intelligence flags for an auction property.

    Prioritizes data-driven flags from judgment, encumbrance, survival, and
    tax data (available for ~99% of properties) over escrow-based flags
    (available for ~1%).
    """
    flags: list[dict[str, str]] = []
    escrow = float(auction.get("escrow_amount") or 0)
    market_value = float(auction.get("hcpa_market_value") or 0)
    judgment = float(auction.get("final_judgment_amount") or 0)
    surviving_debt = float(auction.get("est_surviving_debt") or 0)
    net_equity = float(auction.get("net_equity") or 0)
    liens_surviving = int(auction.get("liens_surviving") or 0)
    liens_uncertain = int(auction.get("liens_uncertain") or 0)
    liens_total = int(auction.get("liens_total") or 0)
    open_permits = int(auction.get("open_permits") or 0)
    homestead = auction.get("tax_homestead")

    # ── Escrow analysis: plaintiff vs third-party deposit ────
    # Hillsborough requires third-party bidders to deposit 5% of their max bid.
    # Plaintiffs (banks) are exempt — they credit-bid the judgment amount.
    # So: plaintiff deposit = their actual bid; third-party deposit * 20 = max bid.
    jd_plaintiff_norm = _normalize_bank_name(auction.get("jd_plaintiff") or "")
    escrow_plaintiff_norm = _normalize_bank_name(
        auction.get("escrow_plaintiff") or ""
    )

    is_plaintiff_deposit = False
    is_third_party_deposit = False
    if escrow > 0 and escrow_plaintiff_norm and jd_plaintiff_norm:
        if (
            jd_plaintiff_norm in escrow_plaintiff_norm
            or escrow_plaintiff_norm in jd_plaintiff_norm
        ):
            is_plaintiff_deposit = True
        else:
            is_third_party_deposit = True
    elif escrow > 0:
        # Can't determine — assume plaintiff (more common)
        is_plaintiff_deposit = True

    if is_plaintiff_deposit:
        # Bank credit-bid: escrow IS their bid amount
        auction["bank_bid"] = escrow
        auction["third_party_max_bid"] = None
    elif is_third_party_deposit:
        # Third party: 5% deposit rule → max bid = deposit * 20
        auction["bank_bid"] = None
        auction["third_party_max_bid"] = escrow * 20
    else:
        auction["bank_bid"] = None
        auction["third_party_max_bid"] = None

    auction["is_plaintiff_deposit"] = is_plaintiff_deposit
    auction["is_third_party_deposit"] = is_third_party_deposit

    # ── Risk flags (red/orange) ─────────────────────────────
    # TOXIC TITLE — heavy surviving liens
    if liens_surviving >= 2 or (
        market_value > 0 and surviving_debt > market_value * 0.25
    ):
        flags.append({"tag": "TOXIC TITLE", "color": "red", "icon": "☠️"})

    # UNDERWATER — negative equity
    if market_value > 0 and net_equity < 0:
        flags.append({"tag": "UNDERWATER", "color": "orange", "icon": "🌊"})

    # HIGH JUDGMENT — judgment nearly equals or exceeds market value
    if market_value > 0 and judgment > market_value * 0.9:
        flags.append({"tag": "HIGH JUDGMENT", "color": "orange", "icon": "⚖️"})

    # UNCERTAIN LIENS — multiple unresolved lien statuses
    if liens_uncertain >= 2:
        flags.append({"tag": "UNCERTAIN LIENS", "color": "orange", "icon": "❓"})

    # OPEN PERMITS — permits with an explicit open-ish status.
    if open_permits > 0:
        flags.append({"tag": "OPEN PERMITS", "color": "orange", "icon": "🔨"})

    # ── Opportunity flags (green/blue) ──────────────────────
    # EQUITY SPREAD — significant positive equity
    if market_value > 0 and net_equity > market_value * 0.30:
        flags.append({"tag": "EQUITY SPREAD", "color": "green", "icon": "💰"})

    # NO KNOWN SURVIVING LIENS — conservative title signal from current data.
    if liens_surviving == 0 and liens_uncertain == 0 and liens_total > 0:
        flags.append({"tag": "NO KNOWN SURVIVING LIENS", "color": "green", "icon": "✅"})

    # HOMESTEAD — homestead exempt property
    if homestead:
        flags.append({"tag": "HOMESTEAD", "color": "blue", "icon": "🏠"})

    # ── Escrow / Bank profile flags ─────────────────────────
    # HIGH COMPETITION — multiple parties wired funds
    if auction.get("escrow_multiple_recipients"):
        flags.append({"tag": "HIGH COMPETITION", "color": "red", "icon": "⚔️"})

    # 3RD PARTY BIDDER — non-plaintiff depositors from trust account data
    tp_bidders = auction.get("third_party_bidders") or []
    if tp_bidders:
        tp_max = max(b["max_bid"] for b in tp_bidders)
        count = len(tp_bidders)
        label = (
            f"{count} 3RD PARTY BIDDERS (max ${tp_max:,.0f})"
            if count > 1
            else f"3RD PARTY BIDDER (max ${tp_max:,.0f})"
        )
        flags.append({"tag": label, "color": "red", "icon": "🎯"})
    elif is_third_party_deposit:
        # Fallback: old single-depositor logic from lateral join
        tp_max = escrow * 20
        flags.append({
            "tag": f"3RD PARTY BIDDER (max ${tp_max:,.0f})",
            "color": "red",
            "icon": "🎯",
        })

    # ESCROW SURGE — delta > 20% increase from prior report
    prev = float(auction.get("escrow_previous_amount") or 0)
    delta_amt = float(auction.get("escrow_delta") or 0)
    if prev > 0 and delta_amt > 0 and (delta_amt / prev) > 0.20:
        flags.append({"tag": "ESCROW SURGE", "color": "blue", "icon": "📈"})

    # PLAINTIFF DEPOSIT — bank has deposited for this case
    if is_plaintiff_deposit:
        flags.append({
            "tag": f"PLAINTIFF DEPOSIT (${escrow:,.0f})",
            "color": "purple",
            "icon": "🏦",
        })

    # BANK ACTIVE — plaintiff has deposits for OTHER upcoming auctions
    bank_active_other = int(auction.get("bank_active_other") or 0)
    if bank_active_other > 0:
        flags.append({
            "tag": f"BANK ACTIVE ({bank_active_other} other)",
            "color": "blue",
            "icon": "🏦",
        })

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
            f.auction_type,
            f.filing_date,
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
            bp.last_sale_price,
            bp.last_sale_date,
            bp.land_use_desc,
            bp.lot_size,
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
            -- Photo (exclude site logos / branding placeholders)
            {_sql_first_valid_photo("pm.photo_cdn_urls")} AS photo_url,
            {_sql_first_local_photo_path("pm.photo_local_paths")} AS photo_local_path,
            -- LP filing date
            lp.lp_filing_date,
            (CURRENT_DATE - lp.lp_filing_date)::integer AS days_in_foreclosure,
            (CURRENT_DATE - f.filing_date)::integer AS days_since_filing,
            -- Title chain quality
            ts.chain_status,
            ts.gap_count,
            ts.chain_years,
            -- Tax data
            tax.homestead_exempt AS tax_homestead,
            tax.estimated_annual_tax,
            -- Permits
            perm.open_permits
        FROM foreclosures f
        LEFT JOIN LATERAL (
            SELECT
                bp2.property_address, bp2.owner_name,
                bp2.beds, bp2.baths, bp2.heated_area,
                bp2.year_built, bp2.market_value,
                bp2.latitude, bp2.longitude,
                bp2.last_sale_price, bp2.last_sale_date,
                bp2.land_use_desc, bp2.lot_size
            FROM hcpa_bulk_parcels bp2
            WHERE (f.strap IS NOT NULL AND bp2.strap = f.strap)
               OR (f.folio IS NOT NULL AND bp2.folio = f.folio)
            ORDER BY bp2.source_file_id DESC NULLS LAST
            LIMIT 1
        ) bp ON TRUE
        LEFT JOIN LATERAL (
            SELECT pm2.photo_cdn_urls, pm2.photo_local_paths
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
        LEFT JOIN LATERAL (
            SELECT MIN(fte.event_date) AS lp_filing_date
            FROM foreclosure_title_events fte
            WHERE fte.foreclosure_id = f.foreclosure_id
              AND fte.event_subtype IN ('LP', 'LPR')
        ) lp ON TRUE
        LEFT JOIN LATERAL (
            SELECT fts.chain_status, fts.gap_count,
                   fts.years_covered AS chain_years
            FROM foreclosure_title_summary fts
            WHERE fts.foreclosure_id = f.foreclosure_id
        ) ts ON TRUE
        LEFT JOIN LATERAL (
            SELECT nal.homestead_exempt, nal.estimated_annual_tax
            FROM dor_nal_parcels nal
            WHERE (f.strap IS NOT NULL AND nal.strap = f.strap)
               OR (f.folio IS NOT NULL AND nal.folio = f.folio)
            ORDER BY nal.tax_year DESC
            LIMIT 1
        ) tax ON TRUE
        LEFT JOIN LATERAL (
            SELECT COUNT(*) FILTER (
                WHERE COALESCE(fte2.description, '') ~* 'status:\\s*(open|active|issued|pending|awaiting|review)'
            ) AS open_permits
            FROM foreclosure_title_events fte2
            WHERE fte2.foreclosure_id = f.foreclosure_id
              AND fte2.event_source IN ('COUNTY_PERMIT', 'TAMPA_PERMIT')
        ) perm ON TRUE
        WHERE f.auction_date = :target_date
          AND f.archived_at IS NULL
        ORDER BY
            (COALESCE(f.market_value, bp.market_value, 0)
             - COALESCE(f.final_judgment_amount, 0)
             - COALESCE(enc.est_surviving_debt, 0)) DESC,
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
                a["jd_foreclosure_type"] = jd.get("foreclosure_type")
                a["jd_original_mortgage_amount"] = (
                    jd.get("foreclosed_mortgage", {}) or {}
                ).get("original_amount")
                a["jd_lp_date"] = (
                    jd.get("lis_pendens", {}) or {}
                ).get("recording_date")
                a["jd_red_flags"] = jd.get("red_flags") or []
                # Remove raw JSON from template context
                a.pop("judgment_data", None)

            # Build bank escrow profiles and attach to each auction
            bank_profiles = _build_bank_escrow_profiles(conn)
            for a in auctions:
                plaintiff = a.get("jd_plaintiff") or ""
                profile = _match_bank_profile(plaintiff, bank_profiles)
                if profile:
                    own_case = a.get("case_number_norm") or a.get("case_number_raw")
                    other_active = [
                        c
                        for c in profile["active_cases"]
                        if c["case_number"] != own_case
                    ]
                    a["bank_name"] = profile["display_name"]
                    a["bank_total_cases"] = profile["total_cases"]
                    a["bank_median_deposit"] = profile["median_deposit"]
                    a["bank_completed_sold"] = profile["completed_sold"]
                    a["bank_wins"] = profile["bank_wins"]
                    a["bank_third_party_wins"] = profile["third_party_wins"]
                    a["bank_active_other"] = len(other_active)
                    a["bank_active_other_total"] = sum(
                        c["amount"] for c in other_active
                    )

            # Build third-party bidder profiles (system-wide)
            tp_profiles = _build_third_party_bidder_profiles(conn)

            # Fetch ALL depositors for displayed auction cases
            case_numbers = [
                a.get("case_number_norm") or a.get("case_number_raw")
                for a in auctions
            ]
            case_numbers = [c for c in case_numbers if c]
            tp_deposits_by_case: dict[str, list] = {}
            if case_numbers:
                tp_rows = conn.execute(text("""
                    SELECT ta.case_number, ta.plaintiff_name, ta.amount,
                           ta.in_escrow_since, ta.counterparty_type, ta.report_date
                    FROM "TrustAccount" ta
                    WHERE ta.source = 'real'
                      AND ta.movement_type != 'dropped'
                      AND ta.amount > 0
                      AND ta.case_number = ANY(:case_list)
                      AND ta.report_date = (
                          SELECT MAX(ta2.report_date)
                          FROM "TrustAccount" ta2
                          WHERE ta2.case_number = ta.case_number
                            AND ta2.plaintiff_name = ta.plaintiff_name
                            AND ta2.source = 'real'
                            AND ta2.movement_type != 'dropped'
                      )
                    ORDER BY ta.case_number, ta.amount DESC
                """), {"case_list": case_numbers}).fetchall()

                for r in tp_rows:
                    tp_deposits_by_case.setdefault(r.case_number, []).append({
                        "name": r.plaintiff_name,
                        "amount": float(r.amount),
                        "in_escrow_since": str(r.in_escrow_since) if r.in_escrow_since else None,
                        "counterparty_type": r.counterparty_type,
                        "report_date": str(r.report_date) if r.report_date else None,
                    })

            # Attach third-party bidders to each auction
            _today = today_local()
            for a in auctions:
                case = a.get("case_number_norm") or a.get("case_number_raw")
                all_depositors = tp_deposits_by_case.get(case, []) if case else []
                jd_plaintiff_norm = _normalize_bank_name(
                    a.get("jd_plaintiff") or ""
                )
                third_party_bidders = []
                for dep in all_depositors:
                    dep_norm = _normalize_bank_name(dep["name"])
                    # Skip if depositor matches the judgment plaintiff (the bank)
                    is_plaintiff = bool(
                        jd_plaintiff_norm
                        and dep_norm
                        and len(dep_norm) >= 5
                        and (jd_plaintiff_norm in dep_norm or dep_norm in jd_plaintiff_norm)
                    )
                    if is_plaintiff:
                        continue
                    # Also skip known bank counterparty types
                    if dep.get("counterparty_type") == "bank":
                        continue
                    days_in_escrow = None
                    if dep["in_escrow_since"]:
                        try:
                            esc_date = date.fromisoformat(dep["in_escrow_since"])
                            days_in_escrow = (_today - esc_date).days
                        except (ValueError, TypeError):
                            pass
                    bidder_entry = {
                        "name": dep["name"],
                        "amount": dep["amount"],
                        "max_bid": dep["amount"] * 20,
                        "in_escrow_since": dep["in_escrow_since"],
                        "days_in_escrow": days_in_escrow,
                    }
                    # Attach profile data if available
                    profile = tp_profiles.get(dep_norm)
                    if profile:
                        bidder_entry["total_cases"] = profile["total_cases"]
                        bidder_entry["cases_won"] = profile["cases_won"]
                        bidder_entry["cases_outbid"] = profile["cases_outbid"]
                        bidder_entry["median_deposit"] = profile["median_deposit"]
                    third_party_bidders.append(bidder_entry)
                a["third_party_bidders"] = third_party_bidders

            # Compute intelligence flags
            for a in auctions:
                _compute_intel_flags(a)

            # Compute aggregate stats
            from statistics import median as _median

            equity_vals = [
                float(a.get("net_equity") or 0)
                for a in auctions
                if a.get("hcpa_market_value")
            ]
            median_equity = _median(equity_vals) if equity_vals else 0

            no_known_surviving_liens = sum(
                1
                for a in auctions
                if int(a.get("liens_surviving") or 0) == 0
                and int(a.get("liens_uncertain") or 0) == 0
                and int(a.get("liens_total") or 0) > 0
            )
            title_issues = sum(
                1
                for a in auctions
                if int(a.get("liens_surviving") or 0) >= 2
                or (
                    float(a.get("hcpa_market_value") or 0) > 0
                    and float(a.get("est_surviving_debt") or 0)
                    > float(a.get("hcpa_market_value") or 0) * 0.25
                )
            )
            with_escrow = sum(
                1
                for a in auctions
                if float(a.get("escrow_amount") or 0) > 0
            )
            with_third_party = sum(
                1 for a in auctions if a.get("third_party_bidders")
            )
            total_judgment = sum(
                float(a.get("final_judgment_amount") or 0) for a in auctions
            )

            # Active market bidders: tp_profiles with newest_deposit in last 90 days
            cutoff = str(_today - timedelta(days=90))
            active_bidders = sorted(
                [
                    p for p in tp_profiles.values()
                    if p.get("newest_deposit") and p["newest_deposit"] >= cutoff
                ],
                key=lambda p: p["total_deposits"],
                reverse=True,
            )

            return (
                target_date,
                auctions,
                {
                    "property_count": len(auctions),
                    "median_equity": median_equity,
                    "no_known_surviving_liens": no_known_surviving_liens,
                    "title_issues": title_issues,
                    "with_escrow": with_escrow,
                    "with_third_party_bidders": with_third_party,
                    "total_judgment": total_judgment,
                    "active_bidders": active_bidders,
                },
            )
    except OperationalError:
        logger.exception(f"get_auction_intel_for_date({target_date}) failed")
        return (
            target_date,
            [],
            {
                "property_count": 0,
                "median_equity": 0,
                "no_known_surviving_liens": 0,
                "title_issues": 0,
                "with_escrow": 0,
                "with_third_party_bidders": 0,
                "total_judgment": 0,
                "active_bidders": [],
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
        logger.exception("check_database_health failed")
        status["error"] = str(e)
    return status
