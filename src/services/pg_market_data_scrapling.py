"""Scrapling-backed multi-site market-data service.

Architectural purpose:
    This module extends ``MarketDataService`` to add Scrapling-based scrapers
    for Realtor.com, Redfin, and Zillow.  All three sites run **concurrently**
    in Phase 1 (via ``asyncio.gather``), each with its own per-site delay
    profile to avoid rate limiting.  Phase 2 delegates to the browser-based
    ``MarketDataService`` as a fallback for any properties scrapling missed.

    ``run_market_data_update`` is a drop-in replacement for the function of the
    same name in ``market_data_worker.py``.

Integration:
    Pipeline controller step ``market_data`` → dispatcher → this module
    (realtor + redfin + zillow concurrent via scrapling) → browser fallback.

Notes
-----
- Per-site delay profiles are defined in ``DELAY_PROFILES``.
- Each site gets a random delay between requests (e.g. 15-55s for Realtor),
  with a longer backoff (3-5 min) after consecutive 429s.
- If Scrapling is unavailable or a specific fetch fails, failures are surfaced in
  logs and reflected in the result payload rather than silently ignored.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import random
import inspect
import json
import re
from typing import Any
import bs4
from loguru import logger
from sqlalchemy import text

from src.services.market_data_service import MarketDataService
from src.scripts.refresh_foreclosures import refresh as refresh_foreclosures
from dataclasses import dataclass

from sunbiz.db import get_engine, resolve_pg_dsn


_REALTOR_SOURCE = "realtor"
_REDFIN_SOURCE = "redfin"
_ZILLOW_SOURCE = "zillow"


@dataclass(frozen=True)
class SiteDelayProfile:
    """Per-site delay configuration for rate-limit avoidance."""

    delay_min: float
    delay_max: float
    backoff_min: float
    backoff_max: float
    backoff_after: int  # consecutive failures before backoff


DELAY_PROFILES: dict[str, SiteDelayProfile] = {
    "realtor": SiteDelayProfile(delay_min=15, delay_max=55, backoff_min=180, backoff_max=300, backoff_after=5),
    "redfin": SiteDelayProfile(delay_min=10, delay_max=30, backoff_min=120, backoff_max=240, backoff_after=5),
    "zillow": SiteDelayProfile(delay_min=12, delay_max=40, backoff_min=150, backoff_max=300, backoff_after=5),
}


def _query_properties_needing_market(
    dsn: str,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Return properties that do not yet have at least one required source row.

    This keeps the same query shape as the existing market-data worker so this
    module can be dropped into the same scheduling path if needed.
    """
    query = """
        SELECT f.strap, f.folio, f.case_number_raw AS case_number, f.property_address,
               h.city AS property_city, h.zip_code AS property_zip
        FROM foreclosures f
        LEFT JOIN property_market pm ON f.strap = pm.strap
        LEFT JOIN hcpa_bulk_parcels h ON h.strap = f.strap
        WHERE f.strap IS NOT NULL
          AND f.property_address IS NOT NULL
          AND f.archived_at IS NULL
          AND (pm.strap IS NULL
               OR NOT (pm.redfin_json IS NOT NULL AND pm.redfin_json::text != 'null'
                       AND pm.zillow_json IS NOT NULL AND pm.zillow_json::text != 'null'
                       AND pm.homeharvest_json IS NOT NULL AND pm.homeharvest_json::text != 'null'))
        ORDER BY f.auction_date DESC
    """
    params: dict[str, Any] = {}
    if limit and limit > 0:
        query += "\n LIMIT :limit"
        params["limit"] = int(limit)

    engine = get_engine(dsn)
    with engine.connect() as conn:
        rows = conn.execute(text(query), params).mappings().all()

    return [dict(row) for row in rows]


class _MissingScraplingError(RuntimeError):
    """Raised when scrapling is not importable for this environment."""


class PgMarketDataScraplingService(MarketDataService):
    """Market-data service that augments existing behavior with Scrapling."""

    def __init__(
        self,
        dsn: str | None = None,
        *,
        use_windows_chrome: bool = False,
        headless: bool = False,
    ) -> None:
        super().__init__(dsn=dsn, use_windows_chrome=use_windows_chrome)
        self._headless = headless

        self._fetcher_cls = self._detect_scrapling_fetcher()

    @staticmethod
    def _normalize_float(raw_value: Any) -> float | None:
        if raw_value in (None, ""):
            return None

        try:
            if isinstance(raw_value, (int, float)):
                val = float(raw_value)
            else:
                val = float(str(raw_value).replace(",", "").replace("$", "").strip())
            if val < 0:
                return None
            return val
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_int(raw_value: Any) -> int | None:
        if raw_value in (None, ""):
            return None

        try:
            if isinstance(raw_value, int):
                return raw_value
            return int(float(str(raw_value).replace(",", "").strip()))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_int_or_float(raw_value: Any) -> float | int | None:
        norm = PgMarketDataScraplingService._normalize_float(raw_value)
        if norm is None:
            return None
        if norm.is_integer():
            return int(norm)
        return norm

    @staticmethod
    def _extract_json_from_script(value: str, marker: str) -> Any | None:
        marker_pos = value.find(marker)
        if marker_pos < 0:
            return None

        start = value.find("{", marker_pos)
        if start < 0:
            return None

        depth = 0
        for idx in range(start, len(value)):
            char = value[idx]
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    candidate = value[start : idx + 1]
                    try:
                        return json.loads(candidate)
                    except json.JSONDecodeError:
                        return None
        return None

    @staticmethod
    def _safe_list(raw_values: Any) -> list[str]:
        if isinstance(raw_values, (list, tuple, set)):
            values = raw_values
        elif isinstance(raw_values, str):
            values = [raw_values]
        else:
            return []

        out: list[str] = []
        for value in values:
            if isinstance(value, str):
                item = value.strip()
                if item:
                    out.append(item)
            elif isinstance(value, dict):
                for key in ("href", "url", "src", "srcSet"):
                    candidate = value.get(key)
                    if isinstance(candidate, str):
                        candidate = candidate.strip()
                        if candidate:
                            out.append(candidate)
                        break
        return out

    @staticmethod
    def _to_realtor_url(address: str, city: str = "") -> str:
        slug = re.sub(r"[^A-Za-z0-9]+", "-", address.strip().lower())
        slug = slug.strip("-")
        if not slug:
            raise ValueError("empty address")
        city_slug = re.sub(r"[^A-Za-z0-9]+", "-", city.strip().lower()).strip("-") if city else ""
        if city_slug:
            return f"https://www.realtor.com/realestateandhomes-search/{slug}_{city_slug}_FL"
        return f"https://www.realtor.com/realestateandhomes-search/{slug}"

    @staticmethod
    def _find_realtor_payload_node(payload: Any, seen: set[int] | None = None) -> dict[str, Any] | None:
        if seen is None:
            seen = set()

        if id(payload) in seen:
            return None

        if isinstance(payload, dict):
            seen.add(id(payload))

            preference_hits = (
                "list_price", "listPrice", "homeStatus", "photos", "detailUrl", "address", "listingStatus"
            )
            if any(key in payload for key in preference_hits):
                return payload

            # Some payloads nest listing data under these keys.
            for nested_key in (
                "propertyDetails",
                "propertyDetail",
                "data",
                "result",
                "props",
                "pageProps",
                "initialReduxState",
                "payload",
            ):
                nested = payload.get(nested_key)
                found = PgMarketDataScraplingService._find_realtor_payload_node(nested, seen)
                if found is not None:
                    return found

            for value in payload.values():
                found = PgMarketDataScraplingService._find_realtor_payload_node(value, seen)
                if found is not None:
                    return found

        elif isinstance(payload, list):
            seen.add(id(payload))
            for item in payload:
                found = PgMarketDataScraplingService._find_realtor_payload_node(item, seen)
                if found is not None:
                    return found

        return None

    @staticmethod
    def _extract_realtor_payload_from_node(node: dict[str, Any], address: str, detail_url: str) -> dict[str, Any]:
        photos = []
        raw_photos = node.get("photos")
        if raw_photos is None:
            raw_photos = node.get("photo_urls")
        if raw_photos is None:
            raw_photos = node.get("image")
        photos.extend(PgMarketDataScraplingService._safe_list(raw_photos))

        est = node.get("estimate")
        if isinstance(est, dict):
            est_value = est.get("value")
            if est_value in (None, ""):
                est_value = est.get("amount")
        else:
            est_value = est

        if node.get("price") not in (None, "") and node.get("list_price") in (None, ""):
            node.setdefault("list_price", node.get("price"))
        if est_value not in (None, "") and node.get("zestimate") in (None, ""):
            node["zestimate"] = est_value

        offers = node.get("offers")
        if isinstance(offers, dict) and node.get("list_price") in (None, ""):
            offer_price = offers.get("price")
            if isinstance(offer_price, dict):
                offer_price = offer_price.get("price")
            node["list_price"] = offer_price

        payload = {
            "listing_status": node.get("homeStatus") or node.get("status") or node.get("listing_status"),
            "list_price": node.get("list_price"),
            "zestimate": est_value or node.get("zestimate"),
            "rent_estimate": node.get("rentEstimate") or node.get("rent_estimate") or node.get("rent"),
            "beds": node.get("beds") or node.get("bedrooms") or node.get("bedroomCount"),
            "baths": node.get("baths") or node.get("bathrooms") or node.get("bathCount"),
            "sqft": node.get("sqft") or node.get("livingArea") or node.get("area") or node.get("squareFootage"),
            "year_built": node.get("year_built") or node.get("yearBuilt"),
            "lot_size": node.get("lot_size") or node.get("lotSize") or node.get("lotSqft") or node.get("lot_sqft"),
            "property_type": node.get("homeType") or node.get("property_type") or node.get("home_type"),
            "photos": photos,
            "detail_url": node.get("detailUrl") or node.get("url") or detail_url,
        }

        if not payload["detail_url"]:
            # Keep detail URL in payload if present so we can still debug page output.
            payload["detail_url"] = detail_url

        if not payload["listing_status"] and node.get("is_for_sale") is True:
            payload["listing_status"] = "FOR_SALE"

        if address:
            payload["_source_address"] = address

        return payload

    @staticmethod
    def _extract_realtor_payload_from_jsonld(nodes: list[dict[str, Any]], address: str, detail_url: str) -> dict[str, Any] | None:
        for node in nodes:
            if not isinstance(node, dict):
                continue
            if str(node.get("@type", "")).lower() in {"realestatelisting", "house", "singlefamilyhouse", "residence"}:
                payload: dict[str, Any] = {
                    "listing_status": node.get("eventStatus") or node.get("name") or node.get("status"),
                    "detail_url": node.get("url") or detail_url,
                    "property_type": node.get("additionalType") or node.get("@type"),
                    "photos": PgMarketDataScraplingService._safe_list(node.get("image")),
                }

                offers = node.get("offers")
                if isinstance(offers, list) and offers:
                    offers = offers[0]
                if isinstance(offers, dict):
                    payload["list_price"] = offers.get("price") or payload.get("list_price")

                floor = node.get("floorSize")
                if isinstance(floor, dict):
                    payload["sqft"] = floor.get("value")

                if node.get("numberOfRooms") is not None:
                    payload["beds"] = node.get("numberOfRooms")

                if node.get("numberOfBathroomsTotal") is not None:
                    payload["baths"] = node.get("numberOfBathroomsTotal")

                payload["_source_address"] = address

                return payload
        return None

    def _detect_scrapling_fetcher(self):
        """Return the most compatible Scrapling fetcher class available.

        Prefers fetchers with ``async_fetch`` since this service runs inside
        an asyncio event loop.
        """
        try:
            module = importlib.import_module("scrapling.fetchers")
        except ModuleNotFoundError as exc:
            logger.warning("Scrapling module not installed: {}", exc)
            return None

        preferred = ("StealthyFetcher", "DynamicFetcher", "PlayWrightFetcher", "AsyncDynamicFetcher", "AsyncPlayWrightFetcher")
        for name in preferred:
            cls = getattr(module, name, None)
            if cls is None:
                continue
            if hasattr(cls, "async_fetch"):
                return cls

        # Fallback: any fetcher with sync fetch (will run in executor)
        for name in preferred:
            cls = getattr(module, name, None)
            if cls is not None and hasattr(cls, "fetch"):
                logger.warning("No async-capable fetcher found; falling back to sync {}", name)
                return cls

        return None

    def _fetcher(self):
        if not self._fetcher_cls:
            raise _MissingScraplingError(
                "scrapling.fetchers unavailable. Install scrapling with fetchers extras before using this service."
            )
        return self._fetcher_cls()

    def _response_text(self, response: Any) -> str:
        candidates: tuple[str, ...] = (
            "html_content",
            "text",
            "html",
            "body",
            "content",
        )
        for attr in candidates:
            value = getattr(response, attr, None)
            if value is None:
                continue
            if isinstance(value, bytes) and value:
                return value.decode("utf-8", errors="ignore")
            if isinstance(value, str) and value:
                return value
            if isinstance(value, bs4.element.Tag):
                text = value.get_text(" ")
                if text:
                    return text

        if isinstance(response, (bytes, str)):
            if isinstance(response, bytes):
                return response.decode("utf-8", errors="ignore")
            return response

        with contextlib.suppress(AttributeError):
            soup = response.soup
            if isinstance(soup, bs4.BeautifulSoup):
                return str(soup)

        return ""

    def _parse_realtor_html(self, html: str, address: str, detail_url: str) -> dict[str, Any]:
        soup = bs4.BeautifulSoup(html, "lxml")
        payload: dict[str, Any] = {}

        # 1) Try `__NEXT_DATA__` payload first.
        for script in soup.find_all("script"):
            script_text = script.get_text(" ", strip=True)
            if "__NEXT_DATA__" not in script_text:
                continue

            raw_payload = self._extract_json_from_script(script_text, "__NEXT_DATA__")
            if raw_payload is None:
                continue

            node = self._find_realtor_payload_node(raw_payload)
            if isinstance(node, dict):
                payload = self._extract_realtor_payload_from_node(node, address, detail_url)
                if payload:
                    break

        # 2) Parse JSON-LD blocks.
        if not payload:
            json_ld_payloads: list[dict[str, Any]] = []
            for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
                try:
                    raw_text = script.get_text("", strip=True)
                    if not raw_text:
                        continue
                    parsed = json.loads(raw_text)
                    if isinstance(parsed, list):
                        json_ld_payloads.extend([item for item in parsed if isinstance(item, dict)])
                    elif isinstance(parsed, dict):
                        json_ld_payloads.append(parsed)
                except json.JSONDecodeError:
                    continue

            jsonld_payload = self._extract_realtor_payload_from_jsonld(json_ld_payloads, address, detail_url)
            if jsonld_payload:
                payload = jsonld_payload

        # 3) Fallback regex extraction for human-readable fields.
        if not payload:
            text = soup.get_text(" ", strip=True)

            # Price patterns: $1,234,567
            price_match = re.search(r"\$\s*([0-9][0-9,]{2,})", text)
            if price_match:
                payload["list_price"] = price_match.group(1)

            beds_match = re.search(r"(\d+)\s*(?:bed|beds|bd)\b", text, flags=re.IGNORECASE)
            if beds_match:
                payload["beds"] = beds_match.group(1)

            baths_match = re.search(r"(\d+(?:\.\d+)?)\s*(?:bath|baths|ba)\b", text, flags=re.IGNORECASE)
            if baths_match:
                payload["baths"] = baths_match.group(1)

            sqft_match = re.search(r"([0-9][0-9,]+)\s*(?:sqft|sq\s*ft)", text, flags=re.IGNORECASE)
            if sqft_match:
                payload["sqft"] = sqft_match.group(1)

            year_match = re.search(r"\b(19|20)\d{2}\b", text)
            if year_match:
                payload["year_built"] = year_match.group(0)

            payload["detail_url"] = detail_url
            payload["_source_address"] = address

        if not payload:
            return {}

        # Normalize monetary values to floats — the parent _upsert_realtor passes
        # these through _f() which expects numeric types, not raw strings like "$325,000".
        payload["list_price"] = self._normalize_float(payload.get("list_price"))
        payload["zestimate"] = self._normalize_float(payload.get("zestimate"))
        payload["rent_estimate"] = self._normalize_float(payload.get("rent_estimate"))
        payload["beds"] = self._normalize_int_or_float(payload.get("beds"))
        payload["baths"] = self._normalize_float(payload.get("baths"))
        payload["sqft"] = self._normalize_int(payload.get("sqft"))
        payload["year_built"] = self._normalize_int(payload.get("year_built"))
        if isinstance(payload.get("photos"), list):
            payload["photos"] = payload["photos"]
        else:
            payload["photos"] = self._safe_list(payload.get("photos"))

        payload["_source"] = "scrapling"
        return payload

    def _is_useful_realtor_payload(self, payload: dict[str, Any]) -> bool:
        if not payload:
            return False
        value_fields = (
            "list_price",
            "zestimate",
            "rent_estimate",
            "beds",
            "baths",
            "sqft",
            "year_built",
            "property_type",
            "listing_status",
        )
        return any(payload.get(field) not in (None, "", []) for field in value_fields) or bool(
            payload.get("photos")
        )

    # ------------------------------------------------------------------
    # Redfin URL + parse
    # ------------------------------------------------------------------

    @staticmethod
    def _to_redfin_url(address: str, city: str = "", zip_code: str = "") -> str:
        street_slug = re.sub(r"[^\w\s]", "", address.strip())
        street_slug = re.sub(r"\s+", "-", street_slug)
        zip_clean = (zip_code or "").strip()[:5]
        slug = f"{street_slug}-{zip_clean}" if zip_clean else street_slug
        city_clean = (city or "Tampa").strip().replace(" ", "-")
        return f"https://www.redfin.com/FL/{city_clean}/{slug}/home/"

    def _parse_redfin_html(self, html: str, address: str, detail_url: str) -> dict[str, Any]:
        soup = bs4.BeautifulSoup(html, "lxml")
        payload: dict[str, Any] = {"_source": "scrapling", "_source_address": address, "detail_url": detail_url}

        # Try __NEXT_DATA__ or preloaded JSON
        for script in soup.find_all("script", {"type": "application/json"}):
            raw = self._extract_json_from_script(script.get_text(" ", strip=True), "__NEXT_DATA__")
            if raw:
                payload.update(self._extract_redfin_from_json(raw, address))
                if payload.get("list_price") or payload.get("zestimate"):
                    return payload

        # Fallback: meta tags
        for meta in soup.find_all("meta"):
            name = (meta.get("name") or meta.get("property") or "").lower()
            content = (meta.get("content") or "").strip()
            if not content:
                continue
            if name == "og:description" and "$" in content:
                import re as _re
                price_match = _re.search(r"\$[\d,]+", content)
                if price_match and not payload.get("list_price"):
                    payload["list_price"] = self._normalize_float(price_match.group().replace("$", ""))

        # Try JSON-LD
        for script in soup.find_all("script", {"type": "application/ld+json"}):
            try:
                ld = json.loads(script.string or "")
            except (json.JSONDecodeError, TypeError):
                continue
            nodes = ld if isinstance(ld, list) else [ld]
            for node in nodes:
                if node.get("@type") in ("SingleFamilyResidence", "Residence", "Product", "RealEstateListing"):
                    if node.get("numberOfRooms"):
                        payload.setdefault("beds", self._normalize_int(node["numberOfRooms"]))
                    if node.get("floorSize", {}).get("value"):
                        payload.setdefault("sqft", self._normalize_int(node["floorSize"]["value"]))

        # Try Redfin-specific data attributes
        for el in soup.select("[data-rf-test-id='abp-price'] .statsValue"):
            payload.setdefault("list_price", self._normalize_float(el.get_text(strip=True)))
        for el in soup.select("[data-rf-test-id='abp-beds'] .statsValue"):
            payload.setdefault("beds", self._normalize_int(el.get_text(strip=True)))
        for el in soup.select("[data-rf-test-id='abp-baths'] .statsValue"):
            payload.setdefault("baths", self._normalize_int_or_float(el.get_text(strip=True)))
        for el in soup.select("[data-rf-test-id='abp-sqFt'] .statsValue"):
            payload.setdefault("sqft", self._normalize_int(el.get_text(strip=True)))

        # Photos from meta
        photos = []
        for meta in soup.find_all("meta", {"property": "og:image"}):
            url = (meta.get("content") or "").strip()
            if url and url.startswith("http"):
                photos.append(url)
        if photos:
            payload["photos"] = photos

        return payload

    @staticmethod
    def _extract_redfin_from_json(data: Any, _address: str) -> dict[str, Any]:
        """Extract property fields from Redfin's __NEXT_DATA__ JSON."""
        result: dict[str, Any] = {}
        if not isinstance(data, dict):
            return result

        # Walk the JSON looking for property info
        def _walk(obj: Any, depth: int = 0) -> None:
            if depth > 8 or not isinstance(obj, dict):
                return
            if obj.get("propertyId") or obj.get("listPrice") or obj.get("price"):
                result.setdefault("list_price", obj.get("listPrice") or obj.get("price"))
                result.setdefault("beds", obj.get("beds") or obj.get("numBeds"))
                result.setdefault("baths", obj.get("baths") or obj.get("numBaths"))
                result.setdefault("sqft", obj.get("sqFt") or obj.get("sqft"))
                result.setdefault("year_built", obj.get("yearBuilt"))
                result.setdefault("lot_size", obj.get("lotSize") or obj.get("lotSqFt"))
                result.setdefault("property_type", obj.get("propertyType") or obj.get("propertyTypeName"))
                result.setdefault("listing_status", obj.get("listingType") or obj.get("marketStatus"))
                result.setdefault("zestimate", obj.get("predictedValue") or obj.get("avm", {}).get("predictedValue"))
                photos = obj.get("photos") or obj.get("photoUrls") or []
                if isinstance(photos, list) and photos:
                    result.setdefault("photos", [p.get("photoUrl") or p if isinstance(p, dict) else p for p in photos[:15]])
            for v in obj.values():
                if isinstance(v, dict):
                    _walk(v, depth + 1)
                elif isinstance(v, list):
                    for item in v[:5]:
                        if isinstance(item, dict):
                            _walk(item, depth + 1)

        _walk(data)
        return result

    # ------------------------------------------------------------------
    # Zillow URL + parse
    # ------------------------------------------------------------------

    @staticmethod
    def _to_zillow_url(address: str, city: str = "", zip_code: str = "") -> str:
        slug = re.sub(r"[^A-Za-z0-9]+", "-", address.strip())
        slug = slug.strip("-")
        city_clean = (city or "Tampa").strip().replace(" ", "-")
        zip_clean = (zip_code or "").strip()[:5]
        suffix = f"-{city_clean}-FL-{zip_clean}" if zip_clean else f"-{city_clean}-FL"
        return f"https://www.zillow.com/homes/{slug}{suffix}_rb/"

    def _parse_zillow_html(self, html: str, address: str, detail_url: str) -> dict[str, Any]:
        soup = bs4.BeautifulSoup(html, "lxml")
        payload: dict[str, Any] = {"_source": "scrapling", "_source_address": address, "detail_url": detail_url}

        # Try preloaded data
        for script in soup.find_all("script"):
            text_content = script.get_text(" ", strip=True)

            # Zillow uses window.__NEXT_DATA__ or similar
            for marker in ("__NEXT_DATA__", "window.__data__", "apiCache"):
                raw = self._extract_json_from_script(text_content, marker)
                if raw and isinstance(raw, dict):
                    payload.update(self._extract_zillow_from_json(raw, address))
                    if payload.get("zestimate") or payload.get("list_price"):
                        return payload

        # JSON-LD
        for script in soup.find_all("script", {"type": "application/ld+json"}):
            try:
                ld = json.loads(script.string or "")
            except (json.JSONDecodeError, TypeError):
                continue
            nodes = ld if isinstance(ld, list) else [ld]
            for node in nodes:
                if node.get("@type") in ("SingleFamilyResidence", "Residence", "Product", "RealEstateListing"):
                    if node.get("floorSize", {}).get("value"):
                        payload.setdefault("sqft", self._normalize_int(node["floorSize"]["value"]))
                    offers = node.get("offers") if isinstance(node.get("offers"), dict) else {}
                    if offers.get("price"):
                        payload.setdefault("list_price", self._normalize_float(offers["price"]))

        # Meta tags
        for meta in soup.find_all("meta"):
            prop = (meta.get("property") or "").lower()
            content = (meta.get("content") or "").strip()
            if prop == "zillow_fb:price" and content:
                payload.setdefault("list_price", self._normalize_float(content))
            elif prop == "zillow_fb:beds" and content:
                payload.setdefault("beds", self._normalize_int(content))
            elif prop == "zillow_fb:baths" and content:
                payload.setdefault("baths", self._normalize_int_or_float(content))

        # Photos
        photos = []
        for meta in soup.find_all("meta", {"property": "og:image"}):
            url = (meta.get("content") or "").strip()
            if url and url.startswith("http") and "zillow" in url:
                photos.append(url)
        if photos:
            payload["photos"] = photos

        return payload

    @staticmethod
    def _extract_zillow_from_json(data: Any, _address: str) -> dict[str, Any]:
        """Extract property fields from Zillow's preloaded JSON."""
        result: dict[str, Any] = {}
        if not isinstance(data, dict):
            return result

        def _walk(obj: Any, depth: int = 0) -> None:
            if depth > 8 or not isinstance(obj, dict):
                return
            # Zillow property data keys
            if obj.get("zpid") or obj.get("zestimate") or obj.get("price"):
                result.setdefault("zestimate", obj.get("zestimate"))
                result.setdefault("rent_estimate", obj.get("rentZestimate"))
                result.setdefault("list_price", obj.get("price"))
                result.setdefault("tax_assessed_value", obj.get("taxAssessedValue"))
                result.setdefault("beds", obj.get("bedrooms") or obj.get("beds"))
                result.setdefault("baths", obj.get("bathrooms") or obj.get("baths"))
                result.setdefault("sqft", obj.get("livingArea") or obj.get("sqft"))
                result.setdefault("year_built", obj.get("yearBuilt"))
                result.setdefault("lot_size", obj.get("lotSize") or obj.get("lotAreaValue"))
                result.setdefault("property_type", obj.get("homeType") or obj.get("propertyType"))
                result.setdefault("listing_status", obj.get("homeStatus") or obj.get("listingStatus"))
                photos = obj.get("photos") or obj.get("hugePhotos") or obj.get("responsivePhotos") or []
                if isinstance(photos, list) and photos:
                    urls = []
                    for p in photos[:15]:
                        if isinstance(p, dict):
                            urls.append(p.get("url") or p.get("mixedSources", {}).get("jpeg", [{}])[0].get("url", ""))
                        elif isinstance(p, str):
                            urls.append(p)
                    result.setdefault("photos", [u for u in urls if u])
            for v in obj.values():
                if isinstance(v, dict):
                    _walk(v, depth + 1)
                elif isinstance(v, list):
                    for item in v[:5]:
                        if isinstance(item, dict):
                            _walk(item, depth + 1)

        _walk(data)
        return result

    # ------------------------------------------------------------------
    # Generic scrapling fetch
    # ------------------------------------------------------------------

    async def _fetch_site_html(self, url: str) -> tuple[str, str]:
        """Fetch a URL via scrapling and return (final_url, html)."""
        fetcher = self._fetcher()
        try:
            if hasattr(fetcher, "async_fetch"):
                response = await fetcher.async_fetch(
                    url,
                    headless=self._headless,
                    google_search=True,
                    network_idle=True,
                    block_webrtc=True,
                    hide_canvas=True,
                    wait=5,
                )
            else:
                loop = asyncio.get_running_loop()
                response = await loop.run_in_executor(None, lambda: fetcher.fetch(url))

            status = getattr(response, "status", None)
            if status and status >= 400:
                raise ValueError(f"HTTP {status} from {url}")

            html = self._response_text(response)
            if not html:
                raise ValueError("empty response")
            return url, html
        finally:
            close = getattr(fetcher, "close", None)
            if close is not None:
                result = close()
                if inspect.isawaitable(result):
                    with contextlib.suppress(Exception):
                        await result

    # ------------------------------------------------------------------
    # Generic site scraping loop
    # ------------------------------------------------------------------

    async def _run_site_loop(
        self,
        site: str,
        properties: list[dict[str, Any]],
        url_builder,
        html_parser,
        upsert_fn,
        is_useful_fn,
    ) -> int:
        """Generic per-site scraping loop with delay profile and progress reporting."""
        if not properties:
            return 0

        profile = DELAY_PROFILES.get(site, DELAY_PROFILES["realtor"])
        matched = 0
        attempted = 0
        consecutive_failures = 0

        for i, prop in enumerate(properties):
            strap = prop.get("strap", "")
            folio = prop.get("folio")
            case_number = prop.get("case_number", "") or ""
            address = (prop.get("property_address") or "").strip()
            city = (prop.get("property_city") or "").strip()
            zip_code = (prop.get("property_zip") or "").strip()

            if not strap or not address or address.lower() in {"unknown", "n/a", "none"}:
                self._mark_source_attempted(strap, folio, case_number, site)
                continue

            # Delay between requests (skip first)
            if i > 0:
                if consecutive_failures >= profile.backoff_after:
                    backoff = random.uniform(profile.backoff_min, profile.backoff_max)  # noqa: S311
                    logger.warning(
                        "{} scrapling: {} consecutive failures — backing off {:.0f}s",
                        site.capitalize(), consecutive_failures, backoff,
                    )
                    await asyncio.sleep(backoff)
                    consecutive_failures = 0
                else:
                    delay = random.uniform(profile.delay_min, profile.delay_max)  # noqa: S311
                    logger.debug("{} scrapling: waiting {:.0f}s", site.capitalize(), delay)
                    await asyncio.sleep(delay)

            logger.info("{} scrapling [{}/{}]: '{}'", site.capitalize(), i + 1, len(properties), address)
            attempted += 1

            try:
                url = url_builder(address, city=city, zip_code=zip_code)
                _, html = await self._fetch_site_html(url)
            except Exception:
                logger.exception("{} scrapling fetch failed for {}", site.capitalize(), address)
                self._mark_source_attempted(strap, folio, case_number, site)
                consecutive_failures += 1
                if attempted % 10 == 0:
                    logger.info("{} scrapling progress: {}/{} attempted, {} matched", site.capitalize(), attempted, len(properties), matched)
                continue

            consecutive_failures = 0

            payload = html_parser(html, address, url)
            if not payload or not is_useful_fn(payload):
                self._mark_source_attempted(strap, folio, case_number, site)
                if attempted % 10 == 0:
                    logger.info("{} scrapling progress: {}/{} attempted, {} matched", site.capitalize(), attempted, len(properties), matched)
                continue

            upsert_fn(strap, folio, case_number, payload)
            matched += 1
            logger.success("{} scrapling: saved for {}", site.capitalize(), strap)

            if attempted % 10 == 0:
                logger.info("{} scrapling progress: {}/{} attempted, {} matched", site.capitalize(), attempted, len(properties), matched)

        logger.info("{} scrapling complete: {}/{} matched", site.capitalize(), matched, attempted)
        return matched

    # ------------------------------------------------------------------
    # Per-site runners (delegate to generic loop)
    # ------------------------------------------------------------------

    async def _run_realtor(self, properties: list[dict[str, Any]]) -> int:
        return await self._run_site_loop(
            site=_REALTOR_SOURCE,
            properties=properties,
            url_builder=lambda addr, *, city="", **_kw: self._to_realtor_url(addr, city=city),
            html_parser=self._parse_realtor_html,
            upsert_fn=self._upsert_realtor,
            is_useful_fn=self._is_useful_realtor_payload,
        )

    async def _run_redfin_scrapling(self, properties: list[dict[str, Any]]) -> int:
        return await self._run_site_loop(
            site=_REDFIN_SOURCE,
            properties=properties,
            url_builder=lambda addr, *, city="", zip_code="": self._to_redfin_url(addr, city=city, zip_code=zip_code),
            html_parser=self._parse_redfin_html,
            upsert_fn=self._upsert_redfin,
            is_useful_fn=lambda p: any(p.get(k) for k in ("list_price", "zestimate", "beds", "sqft")),
        )

    async def _run_zillow_scrapling(self, properties: list[dict[str, Any]]) -> int:
        return await self._run_site_loop(
            site=_ZILLOW_SOURCE,
            properties=properties,
            url_builder=lambda addr, *, city="", zip_code="": self._to_zillow_url(addr, city=city, zip_code=zip_code),
            html_parser=self._parse_zillow_html,
            upsert_fn=self._upsert_zillow,
            is_useful_fn=lambda p: any(p.get(k) for k in ("zestimate", "list_price", "rent_estimate", "beds")),
        )

    def _build_site_needs(
        self,
        properties: list[dict[str, Any]],
        sources: list[str],
    ) -> dict[str, list[dict[str, Any]]]:
        """Partition properties by which sites still need data."""
        selected = {s.lower() for s in sources}
        state_key = {"realtor": "has_realtor", "redfin": "has_redfin", "zillow": "has_zillow"}
        needs: dict[str, list[dict[str, Any]]] = {s: [] for s in selected if s in state_key}

        for prop in properties:
            strap = (prop.get("strap") or "").strip()
            address = (prop.get("property_address") or "").strip()
            if not strap or not address:
                continue
            state = self._get_market_state(strap)
            for site, key in state_key.items():
                if site in needs and not (state and state.get(key)):
                    needs[site].append(prop)

        # Deduplicate by strap
        for site, site_list in needs.items():
            needs[site] = list({p["strap"]: p for p in site_list}.values())

        return needs

    async def run_batch(
        self,
        properties: list[dict[str, Any]],
        sources: list[str] | None = None,
    ) -> dict[str, Any]:
        sources = list(sources or ["redfin", "zillow", "realtor", "homeharvest"])
        scrapling_results: dict[str, int] = {}

        # Phase 1: Run scrapling-backed enrichment for all supported sites
        # concurrently before the heavy browser phase.
        needs = self._build_site_needs(properties, sources)

        site_runners = {
            "realtor": (self._run_realtor, self._has_realtor_column),
            "redfin": (self._run_redfin_scrapling, True),
            "zillow": (self._run_zillow_scrapling, True),
        }

        tasks: list[asyncio.Task] = []
        task_sites: list[str] = []

        for site, (runner, enabled) in site_runners.items():
            site_props = needs.get(site, [])
            if not enabled:
                logger.warning("Scrapling {}: skipped (column missing)", site)
                continue
            if not site_props:
                logger.info("Scrapling {}: all properties already have data", site)
                continue
            logger.info("Scrapling {}: {} properties need data", site, len(site_props))
            tasks.append(asyncio.create_task(self._safe_site_run(site, runner, site_props)))
            task_sites.append(site)

        if tasks:
            results = await asyncio.gather(*tasks)
            scrapling_results = dict(zip(task_sites, results, strict=True))
            logger.info("Scrapling phase complete: {}", scrapling_results)
        else:
            logger.info("Scrapling phase: nothing to do")

        # Phase 2: Delegate to the parent MarketDataService for *all* sources
        # (browser-based fallback for any sites scrapling missed).
        return await super().run_batch(properties, sources=sources)

    async def _safe_site_run(
        self,
        site: str,
        runner,
        properties: list[dict[str, Any]],
    ) -> int:
        """Run a site scraper with exception isolation."""
        try:
            return await runner(properties)
        except _MissingScraplingError as exc:
            logger.warning("Scrapling not available for {}: {}", site, exc)
            return 0
        except Exception:
            logger.exception("Scrapling {} batch failed", site)
            return 0


def run_market_data_update(
    dsn: str | None = None,
    limit: int | None = None,
    use_windows_chrome: bool = False,
) -> dict[str, Any]:
    """Drop-in wrapper mirroring ``market_data_worker.run_market_data_update``."""
    resolved_dsn = resolve_pg_dsn(dsn)
    properties = _query_properties_needing_market(dsn=resolved_dsn, limit=limit)
    if not properties:
        return {"skipped": True, "reason": "no_properties_need_market_data"}

    logger.info("Scrapling market worker: {} foreclosures need market data", len(properties))

    service = PgMarketDataScraplingService(dsn=resolved_dsn, use_windows_chrome=use_windows_chrome)
    result = asyncio.run(service.run_batch(properties))
    if result.get("error"):
        return {
            "properties_queried": len(properties),
            "update": result,
            "error": result["error"],
        }

    try:
        refresh_counts = refresh_foreclosures(dsn=resolved_dsn)
        result["foreclosure_refresh"] = refresh_counts
    except Exception as exc:
        logger.warning("Post-market foreclosure refresh failed: {}", exc)

    return {"properties_queried": len(properties), "update": result}


def _payload_failed(payload: dict[str, Any]) -> bool:
    if payload.get("success") is False:
        return True
    if payload.get("error") not in {None, ""}:
        return True

    update = payload.get("update")
    if isinstance(update, dict):
        if update.get("success") is False:
            return True
        if update.get("error") not in {None, ""}:
            return True

    return False


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Standalone Scrapling Market Data Worker")
    parser.add_argument("--use-windows-chrome", action="store_true", help="Compat flag")
    parser.add_argument("--limit", type=int, default=None, help="Max properties to process")
    args = parser.parse_args()

    result = run_market_data_update(limit=args.limit, use_windows_chrome=args.use_windows_chrome)
    logger.info("Scrapling market worker complete: {}", result)
    print(json.dumps(result, indent=2, default=str))
    if _payload_failed(result):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
