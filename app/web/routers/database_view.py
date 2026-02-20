"""Database workspace — read-only PG query tool + CloudBeaver embed."""

from __future__ import annotations

import os
import re

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from loguru import logger
from sqlalchemy import text

from sunbiz.db import get_engine, resolve_pg_dsn

router = APIRouter()

BLOCKED_SQL_KEYWORDS_RE = re.compile(
    r"\b("
    r"insert|update|delete|drop|alter|create|replace|truncate|"
    r"vacuum|reindex|transaction|begin|commit|rollback|grant|revoke"
    r")\b",
    re.IGNORECASE,
)


def _normalize_external_url(value: str) -> str:
    candidate = value.strip()
    if not candidate:
        return ""
    if not candidate.startswith(("http://", "https://")):
        candidate = f"http://{candidate}"
    return candidate.rstrip("/")


def _is_truthy(value: str | None, default: bool = True) -> bool:
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "off", "no"}


def _load_pg_tables() -> list[str]:
    """List user tables from PG information_schema."""
    try:
        engine = get_engine(resolve_pg_dsn())
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)
            ).fetchall()
            return [str(row[0]) for row in rows]
    except Exception as e:
        logger.warning(f"Failed to list PG tables: {e}")
        return []


def _validate_read_query(raw_sql: str) -> tuple[str, str | None]:
    sql_text = raw_sql.strip()
    if not sql_text:
        return "", "Query is empty."

    if ";" in sql_text.rstrip(";"):
        return "", "Only a single SELECT/CTE statement is allowed."

    lowered = sql_text.lower()
    if not lowered.startswith(("select ", "with ")):
        return "", "Only read-only SELECT queries are allowed."

    if BLOCKED_SQL_KEYWORDS_RE.search(sql_text):
        return "", "Write/admin SQL keywords are blocked."

    return sql_text.rstrip(";"), None


def _run_pg_query(
    sql_text: str,
    max_rows: int,
) -> tuple[list[str], list[list[object]], str | None]:
    validated, error = _validate_read_query(sql_text)
    if error:
        return [], [], error

    wrapped = f"SELECT * FROM ({validated}) AS q LIMIT {max_rows}"
    try:
        engine = get_engine(resolve_pg_dsn())
        with engine.connect() as conn:
            result = conn.execute(text(wrapped))
            columns = list(result.keys())
            rows = [list(row) for row in result.fetchall()]
            return columns, rows, None
    except Exception as exc:
        return [], [], str(exc)


def _safe_default_table_query(table_name: str, max_rows: int) -> str:
    escaped = table_name.replace('"', '""')
    return f'SELECT * FROM "{escaped}" LIMIT {max_rows}'


@router.get("/database", response_class=HTMLResponse)
async def database_page(
    request: Request,
    table: str | None = None,
    sql: str | None = None,
    max_rows: int = 200,
):
    """Database workspace: PG read-only query view + optional CloudBeaver embed."""
    from app.web.main import templates

    max_rows = max(1, min(max_rows, 1000))

    cloudbeaver_pg_url = _normalize_external_url(
        os.getenv("CLOUDBEAVER_PG_URL")
        or os.getenv("CLOUDBEAVER_URL", "http://localhost:8978")
    )
    embed_enabled = _is_truthy(os.getenv("CLOUDBEAVER_EMBED"), default=True)

    pg_tables = _load_pg_tables()
    selected_table = table if table in pg_tables else (pg_tables[0] if pg_tables else "")
    query = (sql or "").strip()
    columns: list[str] = []
    rows: list[list[object]] = []
    query_error: str | None = None

    table_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    if query:
        columns, rows, query_error = _run_pg_query(query, max_rows)
    elif selected_table and table_re.match(selected_table):
        auto_query = _safe_default_table_query(selected_table, max_rows)
        columns, rows, query_error = _run_pg_query(auto_query, max_rows)
        query = auto_query
    else:
        query_error = "No table available to preview."

    return templates.TemplateResponse(
        "database.html",
        {
            "request": request,
            "selected_backend": "postgres",
            "cloudbeaver_pg_url": cloudbeaver_pg_url,
            "cloudbeaver_sqlite_url": "",
            "cloudbeaver_embed_enabled": embed_enabled,
            "sqlite_db_path": "",
            "sqlite_tables": pg_tables,
            "sqlite_table": selected_table,
            "sqlite_query": query,
            "sqlite_columns": columns,
            "sqlite_rows": rows,
            "sqlite_error": query_error,
            "sqlite_row_count": len(rows),
            "max_rows": max_rows,
        },
    )
