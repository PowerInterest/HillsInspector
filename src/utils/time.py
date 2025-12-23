from __future__ import annotations

from contextlib import suppress
from datetime import UTC, date, datetime
from typing import Any
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("America/New_York")


def now_utc() -> datetime:
    """Return timezone-aware UTC timestamp."""
    return datetime.now(tz=UTC)


def now_utc_naive() -> datetime:
    """Return naive UTC timestamp (for TIMESTAMP columns without tz)."""
    return now_utc().replace(tzinfo=None)


def today_local() -> date:
    """Return today's date in the local (auction) timezone."""
    return datetime.now(tz=LOCAL_TZ).date()


def parse_date(value: Any) -> date | None:
    """Parse common date formats to a date object."""
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        with suppress(ValueError):
            return datetime.fromisoformat(raw).date()
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%Y %H:%M:%S", "%Y%m%d"):
            with suppress(ValueError):
                return datetime.strptime(raw, fmt).date()
    return None


def coerce_datetime_utc(value: Any) -> datetime | None:
    """Coerce datetime or string to timezone-aware UTC datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time(), tzinfo=UTC)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        with suppress(ValueError):
            dt = datetime.fromisoformat(raw)
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%Y %H:%M:%S", "%Y%m%d"):
            with suppress(ValueError):
                dt = datetime.strptime(raw, fmt)
                return dt.replace(tzinfo=UTC)
    return None


def ensure_duckdb_utc(conn) -> None:
    """Set DuckDB session timezone to UTC for consistent timestamps."""
    with suppress(Exception):
        conn.execute("SET TimeZone='UTC'")
