"""File-based cache for PAV CustomQuery/KeywordSearch API responses.

Caches responses keyed by a hash of the full request payload (query_id,
keywords, date range).  Responses are stored as gzip-compressed JSON in
``data/cache/pav_api/``.

TTL is 7 days by default — ORI document metadata rarely changes, and the
pipeline runs frequently enough that stale hits are acceptable.  A force
flag can bypass the cache entirely.

Usage::

    from src.services.pav_cache import pav_cache_get, pav_cache_put

    cached = pav_cache_get(payload)
    if cached is not None:
        return cached
    data = _actual_api_call(payload)
    pav_cache_put(payload, data)
"""

from __future__ import annotations

import contextlib
import gzip
import hashlib
import json
import time
from pathlib import Path
from typing import Any

from loguru import logger

_CACHE_DIR = Path("data/cache/pav_api")
_TTL_SECONDS = 7 * 24 * 3600  # 7 days


def _cache_key(payload: dict[str, Any]) -> str:
    """Deterministic hash of the PAV request payload."""
    # Normalize: sort keys, strip whitespace from keyword values
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:24]


def pav_cache_get(payload: dict[str, Any]) -> dict[str, Any] | None:
    """Return cached PAV response or None if miss/expired."""
    key = _cache_key(payload)
    path = _CACHE_DIR / f"{key}.json.gz"
    if not path.exists():
        return None
    # Check TTL
    age = time.time() - path.stat().st_mtime
    if age > _TTL_SECONDS:
        with contextlib.suppress(OSError):
            path.unlink()
        return None
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        logger.warning("PAV cache read error for {}: {}", path.name, exc)
        return None


def pav_cache_put(payload: dict[str, Any], data: dict[str, Any]) -> None:
    """Write PAV response to cache."""
    try:
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        key = _cache_key(payload)
        path = _CACHE_DIR / f"{key}.json.gz"
        with gzip.open(path, "wt", encoding="utf-8") as f:
            json.dump(data, f, separators=(",", ":"))
    except Exception as exc:
        logger.debug("PAV cache write error: {}", exc)


def pav_cache_stats() -> dict[str, Any]:
    """Return basic cache statistics."""
    if not _CACHE_DIR.exists():
        return {"entries": 0, "size_mb": 0.0}
    files = list(_CACHE_DIR.glob("*.json.gz"))
    total_bytes = sum(f.stat().st_size for f in files)
    expired = sum(1 for f in files if (time.time() - f.stat().st_mtime) > _TTL_SECONDS)
    return {
        "entries": len(files),
        "expired": expired,
        "size_mb": round(total_bytes / (1024 * 1024), 2),
    }
