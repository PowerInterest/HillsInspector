#!/usr/bin/env python3
"""
Zillow alternatives bakeoff runner.

Runs a 10-property comparison across:
- HomeHarvest
- Auction.com
- RealtyTrac
- HillsForeclosures
- Redfin county foreclosures (real Chrome, surfaced, with debugger artifacts)
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import math
import re
import sqlite3
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from html import unescape
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlsplit, urlunsplit

from homeharvest import scrape_property
from jsonschema import Draft202012Validator
from loguru import logger
from playwright.async_api import (
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)
from playwright_stealth import Stealth

DEFAULT_LIMIT = 10
DEFAULT_TIMEOUT_MS = 60000
DEFAULT_SCHEMA_PATH = Path("config/market_photo_first_schema.json")
REDFIN_URL = "https://www.redfin.com/county/464/FL/Hillsborough-County/foreclosures"
SOURCE_ORDER = [
    "homeharvest",
    "auction_com",
    "realtytrac",
    "hillsforeclosures",
    "redfin_county",
]
BLOCK_MARKERS = [
    "are you a robot",
    "usage behavior algorithm",
    "captcha",
    "security check",
    "access denied",
    "rate limit",
    "forbidden",
    "incapsula",
    "cloudflare",
    "verify you are human",
]
NON_LISTING_IMAGE_MARKERS = [
    "logo",
    "icon",
    "sprite",
    "favicon",
    "doubleclick",
    "googletag",
    "google-analytics",
    "facebook.com/tr",
]


@dataclass
class PropertyRow:
    case_number: str
    folio: str
    address: str
    auction_date: Optional[str]
    final_judgment_amount: Optional[float]


@dataclass
class AttemptResult:
    source: str
    case_number: str
    folio: str
    address: str
    request_url: str
    final_url: Optional[str] = None
    page_title: Optional[str] = None
    http_status: Optional[int] = None
    result_status: str = "error"
    failure_reason: Optional[str] = None
    canonical_address: Optional[str] = None
    list_price: Optional[float] = None
    est_value: Optional[float] = None
    beds: Optional[float] = None
    baths: Optional[float] = None
    sqft: Optional[int] = None
    listing_status: Optional[str] = None
    photo_count: Optional[int] = None
    elapsed_sec: Optional[float] = None
    screenshot_path: Optional[str] = None
    page_html_path: Optional[str] = None
    page_text_path: Optional[str] = None
    debugger_network_path: Optional[str] = None
    debugger_console_path: Optional[str] = None
    timestamp_utc: str = ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Zillow alternatives bakeoff")
    parser.add_argument("--db-path", type=Path, default=None, help="SQLite DB path")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Number of properties to test")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("logs/market_bakeoff"),
        help="Output directory for results and artifacts",
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=SOURCE_ORDER,
        default=SOURCE_ORDER,
        help="Sources to test",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=DEFAULT_TIMEOUT_MS,
        help="Playwright navigation timeout in milliseconds",
    )
    parser.add_argument(
        "--redfin-interactive-seconds",
        type=int,
        default=8,
        help="Wait time after Redfin navigation/search before capture",
    )
    parser.add_argument(
        "--profile-dir",
        type=Path,
        default=Path("data/browser_profiles/market_bakeoff_chrome"),
        help="Chrome user profile directory inside this project",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logger level (DEBUG/INFO/WARNING/ERROR)",
    )
    parser.add_argument(
        "--schema-path",
        type=Path,
        default=DEFAULT_SCHEMA_PATH,
        help="JSON schema path for photo-first output validation",
    )
    return parser.parse_args()


def resolve_db_path(cli_path: Optional[Path]) -> Path:
    candidates = []
    if cli_path is not None:
        candidates.append(cli_path)
    candidates.extend(
        [
            Path("data/property_master_sqlite.db"),
            Path("datav2/property_master_sqlite.db"),
        ]
    )

    for path in candidates:
        if path.exists() and path.stat().st_size > 0:
            return path

    raise FileNotFoundError(
        "No non-empty SQLite DB found. Checked: "
        + ", ".join(str(path) for path in candidates)
    )


def fetch_properties(db_path: Path, limit: int) -> list[PropertyRow]:
    query = """
        SELECT
          case_number,
          COALESCE(parcel_id, folio) AS folio,
          property_address,
          auction_date,
          final_judgment_amount
        FROM auctions
        WHERE property_address IS NOT NULL
          AND TRIM(property_address) <> ''
          AND COALESCE(parcel_id, folio) IS NOT NULL
          AND TRIM(COALESCE(parcel_id, folio)) <> ''
        ORDER BY auction_date DESC, case_number
        LIMIT ?
    """
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(query, [limit]).fetchall()
    finally:
        conn.close()

    return [
        PropertyRow(
            case_number=str(row[0]),
            folio=str(row[1]),
            address=str(row[2]),
            auction_date=str(row[3]) if row[3] else None,
            final_judgment_amount=float(row[4]) if row[4] is not None else None,
        )
        for row in rows
    ]


def now_utc() -> str:
    return datetime.now(UTC).isoformat()


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def build_source_url(source: str, address: str) -> str:
    encoded = quote(address, safe="")
    if source == "auction_com":
        return f"https://www.auction.com/residential/search?searchVal={encoded}"
    if source == "realtytrac":
        return f"https://www.realtytrac.com/search/?q={encoded}"
    if source == "hillsforeclosures":
        return f"https://www.hillsforeclosures.com/?s={encoded}"
    if source == "redfin_county":
        return REDFIN_URL
    if source == "homeharvest":
        return "homeharvest://scrape_property"
    raise ValueError(f"Unsupported source: {source}")


def detect_blocked(text: str) -> bool:
    lowered = text.lower()
    return any(marker in lowered for marker in BLOCK_MARKERS)


def parse_numeric_fields(text: str) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[int]]:
    price_matches = re.findall(r"\$([0-9]{2,3}(?:,[0-9]{3})+)", text)
    list_price = float(price_matches[0].replace(",", "")) if price_matches else None
    est_value = float(price_matches[1].replace(",", "")) if len(price_matches) > 1 else None

    beds = None
    baths = None
    sqft = None
    bed_match = re.search(r"(\d+(?:\.\d+)?)\s+beds?", text, flags=re.IGNORECASE)
    bath_match = re.search(r"(\d+(?:\.\d+)?)\s+baths?", text, flags=re.IGNORECASE)
    sqft_match = re.search(r"([0-9,]{3,})\s+sq\.?\s?ft", text, flags=re.IGNORECASE)
    if bed_match:
        beds = float(bed_match.group(1))
    if bath_match:
        baths = float(bath_match.group(1))
    if sqft_match:
        sqft = int(sqft_match.group(1).replace(",", ""))

    return list_price, est_value, beds, baths, sqft


def parse_listing_status(text: str) -> Optional[str]:
    lowered = text.lower()
    for token in ["for sale", "sold", "pending", "active", "foreclosure", "auction"]:
        if token in lowered:
            return token
    return None


def maybe_canonical_address(address: str, body_text: str) -> Optional[str]:
    normalized = re.sub(r"[^a-z0-9]+", " ", address.lower()).strip()
    haystack = re.sub(r"[^a-z0-9]+", " ", body_text.lower())
    return address if normalized and normalized in haystack else None


async def apply_stealth(page: Page) -> None:
    await Stealth().apply_stealth_async(page)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def ensure_attempt_artifact_paths(attempt: AttemptResult, out_dir: Path) -> tuple[Path, Path]:
    source_dir = out_dir / attempt.source
    ensure_dir(source_dir)
    stem = f"{attempt.case_number}_{slugify(attempt.folio)}"

    html_path = Path(attempt.page_html_path) if attempt.page_html_path else source_dir / f"{stem}.html"
    text_path = Path(attempt.page_text_path) if attempt.page_text_path else source_dir / f"{stem}.txt"
    ensure_dir(html_path.parent)
    ensure_dir(text_path.parent)

    if not html_path.exists():
        fallback_html = (
            "<html><body>"
            f"<h1>{attempt.source}</h1>"
            f"<p>status={attempt.result_status}</p>"
            f"<p>reason={attempt.failure_reason or ''}</p>"
            "</body></html>"
        )
        write_text(html_path, fallback_html)
    if not text_path.exists():
        write_text(text_path, attempt.failure_reason or "")
    return html_path, text_path


def _ordered_unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            ordered.append(value)
    return ordered


def _looks_like_listing_image(url: str) -> bool:
    lowered = url.lower()
    if any(marker in lowered for marker in NON_LISTING_IMAGE_MARKERS):
        return False
    return (
        any(lowered.endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"])
        or "photo" in lowered
        or "images" in lowered
        or "listing" in lowered
    )


def normalize_photo_url(raw_url: str, base_url: str) -> Optional[str]:
    value = unescape((raw_url or "").strip())
    if not value or value.startswith("data:"):
        return None
    if value.startswith("//"):
        value = f"https:{value}"
    elif value.startswith("/"):
        value = urljoin(base_url, value)
    parts = urlsplit(value)
    cleaned = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    return cleaned if cleaned else None


def upscale_photo_url(url: str) -> str:
    upgraded = url
    upgraded = re.sub(r"([_-])(small|thumb|thumbnail)([._-])", r"\1large\3", upgraded, flags=re.IGNORECASE)
    split = urlsplit(upgraded)
    if split.query:
        query = dict(parse_qsl(split.query, keep_blank_values=True))
        touched = False
        for key in ["w", "h", "width", "height"]:
            if key in query:
                query[key] = "2048"
                touched = True
        if touched:
            upgraded = urlunsplit((split.scheme, split.netloc, split.path, urlencode(query), ""))
    return upgraded


def extract_photo_urls(html: str, base_url: str) -> tuple[list[str], list[str], list[str]]:
    attrs = re.findall(
        r"""(?:src|data-src|data-lazy-src)\s*=\s*["']([^"']+)["']""",
        html,
        flags=re.IGNORECASE,
    )
    direct = re.findall(
        r"""https?://[^\s"'<>]+(?:jpg|jpeg|png|webp|gif)(?:\?[^\s"'<>]*)?""",
        html,
        flags=re.IGNORECASE,
    )
    raw_urls = [url for url in (attrs + direct) if url]

    normalized_candidates: list[str] = []
    for raw in raw_urls:
        normalized = normalize_photo_url(raw, base_url)
        if not normalized:
            continue
        if not _looks_like_listing_image(normalized):
            continue
        normalized_candidates.append(normalized)

    normalized_urls = _ordered_unique(normalized_candidates)
    high_res_urls = _ordered_unique([upscale_photo_url(url) for url in normalized_urls])
    return raw_urls, normalized_urls, high_res_urls


def extract_expected_photo_count(text: str, html: str) -> Optional[int]:
    candidates: list[int] = []
    patterns = [
        r"Photos?\s*Count\s*[:=]\s*(\d+)",
        r"(\d+)\s+Photos?\b",
        r'"photosCount"\s*:\s*(\d+)',
        r'"photos_count"\s*:\s*(\d+)',
    ]
    for pattern in patterns:
        for match in re.findall(pattern, f"{text}\n{html}", flags=re.IGNORECASE):
            try:
                candidates.append(int(match))
            except (TypeError, ValueError):
                continue
    return max(candidates) if candidates else None


def parse_address_components(address: str) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    raw = (address or "").strip()
    if not raw:
        return None, None, None, None
    line = raw
    city = None
    state = None
    zip_code = None

    parts = [part.strip() for part in raw.split(",") if part.strip()]
    if parts:
        line = parts[0]
    if len(parts) >= 2:
        city = parts[-2].strip() if len(parts) >= 3 else None

    sz_match = re.search(r"\b([A-Z]{2})\s*[- ]\s*(\d{5})(?:-\d{4})?\b", raw.upper())
    if sz_match:
        state = sz_match.group(1)
        zip_code = sz_match.group(2)
    return line, city, state, zip_code


def build_photo_first_record(attempt: AttemptResult, out_dir: Path) -> dict[str, Any]:
    html_path, text_path = ensure_attempt_artifact_paths(attempt, out_dir)
    html = html_path.read_text(encoding="utf-8", errors="ignore")
    text = text_path.read_text(encoding="utf-8", errors="ignore")

    base_url = attempt.final_url or attempt.request_url
    photo_urls_raw, photo_urls_normalized, photo_urls_high_res = extract_photo_urls(html, base_url)
    expected = extract_expected_photo_count(text, html)
    if expected is None:
        expected = len(photo_urls_normalized)
    captured = len(photo_urls_normalized)
    rate = (captured / expected) if expected > 0 else 0.0
    missing = captured < expected
    primary = photo_urls_high_res[0] if photo_urls_high_res else None
    photo_order = list(range(captured))
    photo_hashes = [hashlib.sha1(url.encode("utf-8")).hexdigest() for url in photo_urls_normalized]

    address, city, state, zip_code = parse_address_components(attempt.address)
    state_val = state if state and len(state) == 2 else None

    record: dict[str, Any] = {
        "source": attempt.source,
        "source_url": attempt.request_url,
        "captured_at_utc": attempt.timestamp_utc,
        "case_number": attempt.case_number,
        "folio": attempt.folio,
        "http_status": attempt.http_status,
        "result_status": attempt.result_status,
        "failure_reason": attempt.failure_reason,
        "final_url": attempt.final_url,
        "page_title": attempt.page_title,
        "elapsed_sec": attempt.elapsed_sec,
        "page_html_path": str(html_path),
        "page_text_path": str(text_path),
        "debugger_network_path": attempt.debugger_network_path,
        "debugger_console_path": attempt.debugger_console_path,
        "photos_count_expected": expected,
        "photo_urls_raw": photo_urls_raw,
        "photo_urls_normalized": photo_urls_normalized,
        "photo_urls_high_res": photo_urls_high_res,
        "primary_photo_url": primary,
        "photo_order": photo_order,
        "photo_capture_success_rate": round(rate, 4),
        "photo_missing_flag": missing,
        "photo_hashes": photo_hashes,
        "gallery_source": "img_tag_scan",
        "address": address,
        "city": city,
        "state": state_val,
        "zip_code": zip_code,
        "parcel_number": attempt.folio,
        "property_type": None,
        "property_sub_type": None,
        "beds": attempt.beds,
        "baths_full": None,
        "baths_half": None,
        "baths_total": attempt.baths,
        "living_area_sqft": attempt.sqft,
        "lot_size_sqft": None,
        "year_built": None,
        "list_price": attempt.list_price,
        "listing_id": None,
        "listing_status": attempt.listing_status,
        "on_market_date": None,
        "latitude": None,
        "longitude": None,
        "hoa_fee": None,
        "tax_annual_amount": None,
        "tax_year": None,
    }
    return record


def save_photo_first_results(
    out_dir: Path,
    attempts: list[AttemptResult],
    schema_path: Path,
) -> tuple[int, int]:
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)
    valid_records: list[dict[str, Any]] = []
    invalid_records: list[dict[str, Any]] = []

    for attempt in attempts:
        record = build_photo_first_record(attempt, out_dir)
        errors = sorted(validator.iter_errors(record), key=lambda err: err.path)
        if errors:
            invalid_records.append(
                {
                    "source": attempt.source,
                    "case_number": attempt.case_number,
                    "folio": attempt.folio,
                    "error": errors[0].message,
                    "path": "/".join(str(part) for part in errors[0].path),
                    "record": record,
                }
            )
        else:
            valid_records.append(record)

    write_json(out_dir / "photo_first_results.json", valid_records)
    write_json(out_dir / "photo_first_failures.json", invalid_records)
    return len(valid_records), len(invalid_records)


async def run_homeharvest_attempt(
    prop: PropertyRow,
    out_dir: Path,
) -> AttemptResult:
    start = time.perf_counter()
    result = AttemptResult(
        source="homeharvest",
        case_number=prop.case_number,
        folio=prop.folio,
        address=prop.address,
        request_url=build_source_url("homeharvest", prop.address),
        timestamp_utc=now_utc(),
    )

    try:
        df = scrape_property(
            location=prop.address,
            listing_type="sold",
            past_days=3650,
            parallel=False,
        )
        result.elapsed_sec = round(time.perf_counter() - start, 3)
        if df is None or df.empty:
            result.result_status = "not_found"
            result.failure_reason = "homeharvest returned empty dataframe"
            return result

        row = df.iloc[0].to_dict()
        result.result_status = "success"
        result.canonical_address = str(
            row.get("formatted_address") or row.get("street") or prop.address
        )
        result.list_price = to_float(row.get("list_price"))
        result.est_value = to_float(row.get("estimated_value"))
        result.beds = to_float(row.get("beds"))
        result.baths = to_float(row.get("full_baths"))
        result.sqft = to_int(row.get("sqft"))
        result.listing_status = str(row.get("status")) if row.get("status") else None

        photos = row.get("photos")
        if isinstance(photos, list):
            result.photo_count = len(photos)
        elif isinstance(row.get("primary_photo"), str) and row.get("primary_photo"):
            result.photo_count = 1
        else:
            result.photo_count = 0

        artifact_path = out_dir / f"homeharvest_{prop.case_number}_{prop.folio}.json"
        write_json(artifact_path, row)
        return result
    except Exception as exc:
        result.elapsed_sec = round(time.perf_counter() - start, 3)
        message = str(exc).lower()
        result.result_status = "blocked" if detect_blocked(message) else "error"
        result.failure_reason = str(exc)
        return result


def to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    except Exception:
        return None


def to_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(float(value))
    except Exception:
        return None


async def run_web_attempt(
    source: str,
    prop: PropertyRow,
    page: Page,
    out_dir: Path,
    timeout_ms: int,
) -> AttemptResult:
    start = time.perf_counter()
    url = build_source_url(source, prop.address)
    result = AttemptResult(
        source=source,
        case_number=prop.case_number,
        folio=prop.folio,
        address=prop.address,
        request_url=url,
        timestamp_utc=now_utc(),
    )

    try:
        response = await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(3500)

        # Auction.com often needs manual search after initial load.
        if source == "auction_com":
            search_box = page.locator("input[placeholder*='Search'], input[type='search']").first
            if await search_box.count():
                await search_box.click()
                await search_box.fill(prop.address)
                await search_box.press("Enter")
                await page.wait_for_timeout(3000)

        body = await page.inner_text("body")
        html = await page.content()
        title = await page.title()
        final_url = page.url
        photo_count = await page.locator("img").count()
        html_rel = f"{source}/{prop.case_number}_{slugify(prop.folio)}.html"
        text_rel = f"{source}/{prop.case_number}_{slugify(prop.folio)}.txt"
        html_path = out_dir / html_rel
        text_path = out_dir / text_rel
        ensure_dir(html_path.parent)
        write_text(html_path, html)
        write_text(text_path, body)

        status = response.status if response else None
        blocked = detect_blocked(f"{title}\n{body}\n{final_url}")
        list_price, est_value, beds, baths, sqft = parse_numeric_fields(body)

        result.http_status = status
        result.page_title = title
        result.final_url = final_url
        result.photo_count = int(photo_count)
        result.page_html_path = str(html_path)
        result.page_text_path = str(text_path)
        result.canonical_address = maybe_canonical_address(prop.address, body)
        result.list_price = list_price
        result.est_value = est_value
        result.beds = beds
        result.baths = baths
        result.sqft = sqft
        result.listing_status = parse_listing_status(body)
        result.elapsed_sec = round(time.perf_counter() - start, 3)

        if blocked:
            result.result_status = "blocked"
            result.failure_reason = "challenge/captcha/robot marker detected"
        elif status is None:
            result.result_status = "error"
            result.failure_reason = "no http response"
        elif status == 404:
            result.result_status = "not_found"
            result.failure_reason = "http 404"
        elif status >= 400:
            result.result_status = "error"
            result.failure_reason = f"http {status}"
        else:
            result.result_status = "success"
    except PlaywrightTimeoutError:
        result.elapsed_sec = round(time.perf_counter() - start, 3)
        result.result_status = "timeout"
        result.failure_reason = "navigation timeout"
    except Exception as exc:
        result.elapsed_sec = round(time.perf_counter() - start, 3)
        result.result_status = "error"
        result.failure_reason = str(exc)

    return result


async def run_redfin_attempt(
    prop: PropertyRow,
    page: Page,
    out_dir: Path,
    timeout_ms: int,
    interactive_seconds: int,
) -> AttemptResult:
    start = time.perf_counter()
    result = AttemptResult(
        source="redfin_county",
        case_number=prop.case_number,
        folio=prop.folio,
        address=prop.address,
        request_url=REDFIN_URL,
        timestamp_utc=now_utc(),
    )

    network_events: list[dict[str, Any]] = []
    console_events: list[dict[str, Any]] = []

    def on_response(resp) -> None:
        network_events.append(
            {
                "url": resp.url,
                "status": resp.status,
                "resource_type": resp.request.resource_type,
                "method": resp.request.method,
            }
        )

    def on_console(msg) -> None:
        console_events.append(
            {
                "type": msg.type,
                "text": msg.text,
            }
        )

    page.on("response", on_response)
    page.on("console", on_console)

    try:
        response = await page.goto(REDFIN_URL, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(3000)

        # Attempt search for property relevance from county page.
        search_box = page.locator("#search-box-input, input[type='search'], input[placeholder*='Address']").first
        if await search_box.count():
            await search_box.click()
            await search_box.fill(prop.address)
            await search_box.press("Enter")

        await page.wait_for_timeout(max(0, interactive_seconds) * 1000)

        body = await page.inner_text("body")
        html = await page.content()
        title = await page.title()
        final_url = page.url
        photo_count = await page.locator("img").count()

        html_rel = f"redfin_county/{prop.case_number}_{slugify(prop.folio)}.html"
        text_rel = f"redfin_county/{prop.case_number}_{slugify(prop.folio)}.txt"
        html_path = out_dir / html_rel
        text_path = out_dir / text_rel
        ensure_dir(html_path.parent)
        write_text(html_path, html)
        write_text(text_path, body)

        network_rel = f"redfin_county/{prop.case_number}_{slugify(prop.folio)}_network.json"
        console_rel = f"redfin_county/{prop.case_number}_{slugify(prop.folio)}_console.json"
        network_path = out_dir / network_rel
        console_path = out_dir / console_rel
        write_json(network_path, network_events)
        write_json(console_path, console_events)

        status = response.status if response else None
        blocked = detect_blocked(f"{title}\n{body}\n{final_url}")
        list_price, est_value, beds, baths, sqft = parse_numeric_fields(body)

        result.http_status = status
        result.page_title = title
        result.final_url = final_url
        result.photo_count = int(photo_count)
        result.page_html_path = str(html_path)
        result.page_text_path = str(text_path)
        result.debugger_network_path = str(network_path)
        result.debugger_console_path = str(console_path)
        result.canonical_address = maybe_canonical_address(prop.address, body)
        result.list_price = list_price
        result.est_value = est_value
        result.beds = beds
        result.baths = baths
        result.sqft = sqft
        result.listing_status = parse_listing_status(body)
        result.elapsed_sec = round(time.perf_counter() - start, 3)

        if blocked:
            result.result_status = "blocked"
            result.failure_reason = "challenge/captcha/robot marker detected"
        elif status is None:
            result.result_status = "error"
            result.failure_reason = "no http response"
        elif status == 404:
            result.result_status = "not_found"
            result.failure_reason = "http 404"
        elif status >= 400:
            result.result_status = "error"
            result.failure_reason = f"http {status}"
        else:
            result.result_status = "success"

    except PlaywrightTimeoutError:
        result.elapsed_sec = round(time.perf_counter() - start, 3)
        result.result_status = "timeout"
        result.failure_reason = "navigation timeout"
    except Exception as exc:
        result.elapsed_sec = round(time.perf_counter() - start, 3)
        result.result_status = "error"
        result.failure_reason = str(exc)
    finally:
        page.remove_listener("response", on_response)
        page.remove_listener("console", on_console)

    return result


def compute_summary(results: list[AttemptResult]) -> dict[str, Any]:
    summary: dict[str, Any] = {"overall": {"attempts": len(results)}, "sources": {}}
    for source in sorted({row.source for row in results}):
        subset = [row for row in results if row.source == source]
        attempts = len(subset)
        if attempts == 0:
            continue
        success = [row for row in subset if row.result_status == "success"]
        blocked = [row for row in subset if row.result_status == "blocked"]
        timeouts = [row for row in subset if row.result_status == "timeout"]
        not_found = [row for row in subset if row.result_status == "not_found"]
        errors = [row for row in subset if row.result_status == "error"]
        with_address = [row for row in subset if row.canonical_address]
        with_price = [row for row in subset if row.list_price is not None or row.est_value is not None]
        with_specs = [
            row
            for row in subset
            if sum(v is not None for v in [row.beds, row.baths, row.sqft]) >= 2
        ]
        with_photo = [row for row in subset if (row.photo_count or 0) > 0]
        elapsed_vals = [row.elapsed_sec for row in subset if row.elapsed_sec is not None]

        summary["sources"][source] = {
            "attempts": attempts,
            "success_rate": round(len(success) / attempts, 3),
            "blocked_rate": round(len(blocked) / attempts, 3),
            "timeout_rate": round(len(timeouts) / attempts, 3),
            "not_found_rate": round(len(not_found) / attempts, 3),
            "error_rate": round(len(errors) / attempts, 3),
            "address_coverage": round(len(with_address) / attempts, 3),
            "price_coverage": round(len(with_price) / attempts, 3),
            "specs_coverage": round(len(with_specs) / attempts, 3),
            "photo_coverage": round(len(with_photo) / attempts, 3),
            "median_elapsed_sec": round(sorted(elapsed_vals)[len(elapsed_vals) // 2], 3) if elapsed_vals else None,
        }
    return summary


def save_results(out_dir: Path, results: list[AttemptResult], summary: dict[str, Any]) -> None:
    ensure_dir(out_dir)
    raw_json = out_dir / "results.json"
    write_json(raw_json, [asdict(row) for row in results])

    csv_path = out_dir / "results.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        fieldnames = list(asdict(results[0]).keys()) if results else list(AttemptResult.__dataclass_fields__.keys())
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow(asdict(row))

    write_json(out_dir / "summary.json", summary)


async def run_browser_sources(
    playwright: Playwright,
    properties: list[PropertyRow],
    sources: list[str],
    out_dir: Path,
    timeout_ms: int,
    profile_dir: Path,
    redfin_interactive_seconds: int,
) -> list[AttemptResult]:
    results: list[AttemptResult] = []
    browser_sources = [source for source in sources if source != "homeharvest"]
    if not browser_sources:
        return results

    ensure_dir(profile_dir)
    context = await playwright.chromium.launch_persistent_context(
        user_data_dir=str(profile_dir),
        channel="chrome",
        headless=False,
        devtools=True,
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1440, "height": 900},
        locale="en-US",
        timezone_id="America/New_York",
        args=["--disable-blink-features=AutomationControlled"],
    )
    await context.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
    )

    source_tabs: dict[str, Page] = {}
    try:
        for source in browser_sources:
            page = await context.new_page()
            await apply_stealth(page)
            await page.goto("about:blank")
            source_tabs[source] = page
            logger.info(f"Opened tab for source: {source}")

        for prop in properties:
            for source in browser_sources:
                logger.info(f"[{source}] {prop.case_number} | {prop.address}")
                page = source_tabs[source]
                if source == "redfin_county":
                    attempt = await run_redfin_attempt(
                        prop=prop,
                        page=page,
                        out_dir=out_dir,
                        timeout_ms=timeout_ms,
                        interactive_seconds=redfin_interactive_seconds,
                    )
                else:
                    attempt = await run_web_attempt(
                        source=source,
                        prop=prop,
                        page=page,
                        out_dir=out_dir,
                        timeout_ms=timeout_ms,
                    )
                results.append(attempt)
    finally:
        for page in source_tabs.values():
            await page.close()
        await context.close()

    return results


async def run_all(args: argparse.Namespace) -> int:
    logger.remove()
    logger.add(lambda msg: print(msg, end=""), level=args.log_level.upper())

    db_path = resolve_db_path(args.db_path)
    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_dir / run_stamp
    ensure_dir(out_dir)

    logger.info(f"Using DB: {db_path}\n")
    properties = fetch_properties(db_path, args.limit)
    if not properties:
        logger.error("No eligible properties found in auctions table.\n")
        return 1

    manifest_path = out_dir / "manifest.json"
    write_json(manifest_path, [asdict(prop) for prop in properties])
    logger.info(f"Saved manifest: {manifest_path}\n")

    all_results: list[AttemptResult] = []

    if "homeharvest" in args.sources:
        hh_dir = out_dir / "homeharvest"
        ensure_dir(hh_dir)
        for prop in properties:
            logger.info(f"[homeharvest] {prop.case_number} | {prop.address}\n")
            all_results.append(await run_homeharvest_attempt(prop, hh_dir))

    browser_sources = [s for s in args.sources if s != "homeharvest"]
    if browser_sources:
        async with async_playwright() as p:
            browser_results = await run_browser_sources(
                playwright=p,
                properties=properties,
                sources=browser_sources,
                out_dir=out_dir,
                timeout_ms=args.timeout_ms,
                profile_dir=args.profile_dir,
                redfin_interactive_seconds=args.redfin_interactive_seconds,
            )
            all_results.extend(browser_results)

    summary = compute_summary(all_results)
    save_results(out_dir, all_results, summary)
    valid_count, invalid_count = save_photo_first_results(
        out_dir=out_dir,
        attempts=all_results,
        schema_path=args.schema_path,
    )

    logger.info(f"Completed bakeoff. Artifacts: {out_dir}\n")
    logger.info(
        f"Photo-first schema validation: valid={valid_count}, invalid={invalid_count}\n"
    )
    logger.info(json.dumps(summary, indent=2) + "\n")
    return 0


def main() -> None:
    args = parse_args()
    raise SystemExit(asyncio.run(run_all(args)))


if __name__ == "__main__":
    main()
