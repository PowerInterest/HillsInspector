"""
Simple geocoder using Nominatim (OpenStreetMap) with on-disk caching.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Optional, Tuple
from urllib import parse, request, error

from loguru import logger

CACHE_PATH = Path("data/geocode_cache.json")
USER_AGENT = "HillsInspector/1.0 (contact: offline)"


def _load_cache() -> dict:
    if CACHE_PATH.exists():
        try:
            return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
    return {}


def _save_cache(cache: dict) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, indent=2), encoding="utf-8")


def geocode_address(address: str) -> Optional[Tuple[float, float]]:
    """Return (lat, lon) for an address, cached to avoid repeat lookups."""
    if not address:
        return None
    cache = _load_cache()
    if address in cache:
        return tuple(cache[address])  # type: ignore[arg-type]

    url = f"https://nominatim.openstreetmap.org/search?format=json&limit=1&q={parse.quote(address)}"
    req = request.Request(url, headers={"User-Agent": USER_AGENT})  # noqa: S310
    try:
        with request.urlopen(req, timeout=10) as resp:  # noqa: S310
            data = json.loads(resp.read().decode("utf-8"))
            if not data:
                logger.warning("Geocode: no result for {addr}", addr=address)
                return None
            lat = float(data[0]["lat"])
            lon = float(data[0]["lon"])
            cache[address] = [lat, lon]
            _save_cache(cache)
            # Respect Nominatim usage policy
            time.sleep(1.0)
            return lat, lon
    except error.URLError as exc:
        logger.error("Geocode failed for {addr}: {err}", addr=address, err=exc)
        return None
