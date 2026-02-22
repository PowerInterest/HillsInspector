"""Database workspace — CloudBeaver embed."""

from __future__ import annotations

import os

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()


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


@router.get("/database", response_class=HTMLResponse)
async def database_page(request: Request):
    """Database workspace: CloudBeaver embed."""
    from app.web.main import templates

    cloudbeaver_pg_url = _normalize_external_url(
        os.getenv("CLOUDBEAVER_PG_URL") or os.getenv("CLOUDBEAVER_URL", "http://localhost:8978")
    )
    embed_enabled = _is_truthy(os.getenv("CLOUDBEAVER_EMBED"), default=True)

    return templates.TemplateResponse(
        "database.html",
        {
            "request": request,
            "cloudbeaver_pg_url": cloudbeaver_pg_url,
            "cloudbeaver_embed_enabled": embed_enabled,
        },
    )
