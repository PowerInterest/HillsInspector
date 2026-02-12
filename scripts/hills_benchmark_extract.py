#!/usr/bin/env python3
"""
Crawl HillsForeclosures and build a data benchmark dataset.

Output:
- listing records (JSONL + CSV)
- saved HTML/text per property page
- downloaded property photos
- comparison report versus local auctions DB
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sqlite3
from collections import deque
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qsl, urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup
from loguru import logger
from playwright.sync_api import (
    BrowserContext,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)

SITE_ROOT = "https://www.hillsforeclosures.com"
SEED_URLS = [
    f"{SITE_ROOT}/",
    f"{SITE_ROOT}/featured-upcoming.html",
    f"{SITE_ROOT}/featured-results.html",
    f"{SITE_ROOT}/tax-deed-sales.html",
]
NAVIGATION_TIMEOUT_MS = 90000
REQUEST_TIMEOUT_SEC = 30
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
CHALLENGE_MARKERS = [
    "just a moment",
    "enable javascript and cookies to continue",
    "cf-chl",
]
IMAGE_FILTER_MARKERS = [
    "logo",
    "icon",
    "favicon",
    "sprite",
    "doubleclick",
    "google-analytics",
    "googletagmanager",
    "maps.gstatic",
    "mapbox",
    "facebook.com/tr",
]
ALLOWED_LIST_PATHS = [
    "/foreclosure-auctions/",
    "/featured-upcoming.html",
    "/featured-results.html",
    "/tax-deed-sales.html",
]


@dataclass
class ListingRecord:
    property_id: str
    slug: str
    url: str
    crawled_at_utc: str
    title: Optional[str]
    full_address: Optional[str]
    city: Optional[str]
    state: Optional[str]
    zip_code: Optional[str]
    county: Optional[str]
    bedrooms: Optional[float]
    bathrooms: Optional[float]
    sqft_under_air: Optional[int]
    lot_size_sqft: Optional[int]
    year_built: Optional[int]
    property_type: Optional[str]
    auction_status: Optional[str]
    auction_type: Optional[str]
    auction_date: Optional[str]
    case_number: Optional[str]
    final_judgment_ref: Optional[str]
    number_of_bids: Optional[str]
    winning_bid: Optional[float]
    winner_name: Optional[str]
    previous_sale_price: Optional[float]
    previous_sale_date: Optional[str]
    appraised_value: Optional[float]
    taxes_previous_year: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    photo_count: int
    photo_urls: list[str]
    html_path: str
    text_path: str
    section_data: dict[str, dict[str, str]]
    data_sources: list[str]
    matched_case_number: Optional[str] = None
    matched_folio: Optional[str] = None
    matched_address: Optional[str] = None
    home_harvest_photo_count: Optional[int] = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build HillsForeclosures benchmark dataset")
    parser.add_argument(
        "--profile-dir",
        type=Path,
        default=Path("data/browser_profiles/hills_benchmark"),
        help="Chrome profile path inside the project",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/temp/hills_benchmark"),
        help="Output root directory",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("datav2/property_master_sqlite.db"),
        help="SQLite DB path for comparison",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="Max pages to crawl (0 = no cap)",
    )
    parser.add_argument(
        "--max-properties",
        type=int,
        default=0,
        help="Max property pages to extract (0 = no cap)",
    )
    parser.add_argument(
        "--download-photos",
        action="store_true",
        help="Download discovered photos to local output folder",
    )
    return parser.parse_args()


def now_utc() -> str:
    return datetime.now(UTC).isoformat()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def normalize_url(url: str) -> str:
    parts = urlsplit(url)
    query = parse_qsl(parts.query, keep_blank_values=True)
    normalized_query = "&".join(f"{k}={v}" for k, v in sorted(query))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, normalized_query, ""))


def is_hills_url(url: str) -> bool:
    return url.startswith(f"{SITE_ROOT}/")


def is_property_url(url: str) -> bool:
    return "/property-info/" in url


def is_list_url(url: str) -> bool:
    path = urlsplit(url).path
    return any(path.startswith(prefix) for prefix in ALLOWED_LIST_PATHS)


def is_challenge_page(title: str, text: str) -> bool:
    blob = f"{title}\n{text}".lower()
    return any(marker in blob for marker in CHALLENGE_MARKERS)


def parse_amount(value: str) -> Optional[float]:
    if not value:
        return None
    match = re.search(r"\$([0-9,]+(?:\.\d+)?)", value)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", ""))
    except ValueError:
        return None


def parse_int(value: str) -> Optional[int]:
    if not value:
        return None
    match = re.search(r"([0-9][0-9,]*)", value)
    if not match:
        return None
    try:
        return int(match.group(1).replace(",", ""))
    except ValueError:
        return None


def parse_float(value: str) -> Optional[float]:
    if not value:
        return None
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)", value)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def normalize_photo_url(raw: str, base_url: str) -> Optional[str]:
    value = (raw or "").strip()
    if not value or value.startswith("data:"):
        return None
    if value.startswith("//"):
        value = f"https:{value}"
    elif value.startswith("/"):
        value = urljoin(base_url, value)
    parts = urlsplit(value)
    normalized = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    return normalized if normalized else None


def looks_like_property_photo(url: str) -> bool:
    lowered = url.lower()
    if any(marker in lowered for marker in IMAGE_FILTER_MARKERS):
        return False
    if lowered.endswith((".jpg", ".jpeg", ".png", ".webp", ".gif")):
        return True
    return "photo" in lowered or "property" in lowered or "listing" in lowered


def normalize_address(value: str) -> str:
    text = re.sub(r"[^A-Z0-9 ]+", " ", value.upper())
    text = re.sub(r"\s+", " ", text).strip()
    replacements = {
        " STREET": " ST",
        " AVENUE": " AVE",
        " ROAD": " RD",
        " DRIVE": " DR",
        " COURT": " CT",
        " LANE": " LN",
        " PLACE": " PL",
        " BOULEVARD": " BLVD",
        " TERRACE": " TER",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def parse_city_state_zip(full_address: Optional[str]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    if not full_address:
        return None, None, None
    match = re.search(r",\s*([^,]+),\s*([A-Z]{2})[- ]+(\d{5})", full_address.upper())
    if not match:
        return None, None, None
    city = match.group(1).title()
    state = match.group(2)
    zip_code = match.group(3)
    return city, state, zip_code


def extract_section_data(text: str) -> dict[str, dict[str, str]]:
    sections: dict[str, dict[str, str]] = {}
    current: Optional[str] = None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line in {"Property Information", "Auction Details"}:
            current = line
            sections.setdefault(current, {})
            continue
        if line in {"Your Gallery", "Your Documents", "Your Notes", "Similar Foreclosure Listings"}:
            current = None
            continue
        if current and ":" in line:
            key, value = line.split(":", 1)
            sections[current][key.strip()] = value.strip()
    return sections


def extract_coordinates(html: str) -> tuple[Optional[float], Optional[float]]:
    match = re.search(r"L\.marker\(\[([0-9.\-]+),\s*([0-9.\-]+)\]", html)
    if not match:
        return None, None
    try:
        return float(match.group(1)), float(match.group(2))
    except ValueError:
        return None, None


def extract_links(page: Page) -> list[str]:
    hrefs = page.eval_on_selector_all("a[href]", "els => els.map(a => a.href)")
    deduped: list[str] = []
    seen: set[str] = set()
    for href in hrefs:
        if not isinstance(href, str):
            continue
        url = normalize_url(href)
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def extract_record_from_page(
    url: str,
    title: str,
    html: str,
    text: str,
    html_path: Path,
    text_path: Path,
) -> ListingRecord:
    prop_match = re.search(r"/property-info/([0-9]+)/([^/?#]+)", url)
    property_id = prop_match.group(1) if prop_match else ""
    slug = prop_match.group(2) if prop_match else ""

    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    full_address = h1.get_text(" ", strip=True).replace(" Foreclosure Information", "") if h1 else None
    city, state, zip_code = parse_city_state_zip(full_address)

    section_data = extract_section_data(text)
    prop_info = section_data.get("Property Information", {})
    auction = section_data.get("Auction Details", {})

    beds = parse_float(prop_info.get("Bed / Bath", "").split("/")[0]) if "Bed / Bath" in prop_info else None
    baths = parse_float(prop_info.get("Bed / Bath", "").split("/")[1]) if "Bed / Bath" in prop_info and "/" in prop_info.get("Bed / Bath", "") else None

    photo_urls_raw = [
        img.get("src", "")
        for img in soup.find_all("img")
    ]
    photo_urls: list[str] = []
    seen: set[str] = set()
    for raw in photo_urls_raw:
        normalized = normalize_photo_url(raw, url)
        if not normalized:
            continue
        if not looks_like_property_photo(normalized):
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        photo_urls.append(normalized)

    latitude, longitude = extract_coordinates(html)

    prev_sale = prop_info.get("Previous Sale Price")
    prev_sale_amount = parse_amount(prev_sale or "")
    prev_sale_date = None
    if prev_sale:
        date_match = re.search(r"in\s+([0-9/]+)", prev_sale)
        if date_match:
            prev_sale_date = date_match.group(1)

    return ListingRecord(
        property_id=property_id,
        slug=slug,
        url=url,
        crawled_at_utc=now_utc(),
        title=title,
        full_address=full_address,
        city=city,
        state=state,
        zip_code=zip_code,
        county=prop_info.get("County"),
        bedrooms=beds,
        bathrooms=baths,
        sqft_under_air=parse_int(prop_info.get("SQFT Under air", "")),
        lot_size_sqft=parse_int(prop_info.get("SQFT Total", "")),
        year_built=parse_int(prop_info.get("Year Built", "")),
        property_type=prop_info.get("Type Of Property"),
        auction_status=auction.get("Auction Status"),
        auction_type=auction.get("Auction Type"),
        auction_date=auction.get("Date Of Auction"),
        case_number=auction.get("Case #"),
        final_judgment_ref=auction.get("Fnl Judg"),
        number_of_bids=auction.get("Number Of Bids"),
        winning_bid=parse_amount(auction.get("Winning Bid", "")),
        winner_name=auction.get("Winners Name"),
        previous_sale_price=prev_sale_amount,
        previous_sale_date=prev_sale_date,
        appraised_value=parse_amount(prop_info.get("Appraised", "")),
        taxes_previous_year=prop_info.get("Previous Year Taxes"),
        latitude=latitude,
        longitude=longitude,
        photo_count=len(photo_urls),
        photo_urls=photo_urls,
        html_path=str(html_path),
        text_path=str(text_path),
        section_data=section_data,
        data_sources=["property-info-page"],
    )


def save_record_jsonl(path: Path, records: list[ListingRecord]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(asdict(record), ensure_ascii=False) + "\n")


def save_record_csv(path: Path, records: list[ListingRecord]) -> None:
    if not records:
        return
    rows: list[dict[str, Any]] = []
    for record in records:
        row = asdict(record)
        row["photo_urls"] = json.dumps(row["photo_urls"])
        row["section_data"] = json.dumps(row["section_data"])
        row["data_sources"] = json.dumps(row["data_sources"])
        rows.append(row)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def download_photos(records: list[ListingRecord], photos_dir: Path) -> dict[str, Any]:
    ensure_dir(photos_dir)
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    downloaded = 0
    failed = 0
    for record in records:
        if not record.photo_urls:
            continue
        record_dir = photos_dir / (record.property_id or slugify(record.slug) or "unknown")
        ensure_dir(record_dir)
        for idx, url in enumerate(record.photo_urls):
            ext = Path(urlsplit(url).path).suffix.lower()
            if ext not in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
                ext = ".jpg"
            digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]
            target = record_dir / f"{idx:03d}_{digest}{ext}"
            if target.exists():
                continue
            try:
                response = session.get(url, timeout=REQUEST_TIMEOUT_SEC)
                ctype = response.headers.get("Content-Type", "").lower()
                if response.status_code == 200 and "image" in ctype:
                    target.write_bytes(response.content)
                    downloaded += 1
                else:
                    failed += 1
            except requests.RequestException:
                failed += 1
    return {"downloaded": downloaded, "failed": failed}


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def load_auctions(db_path: Path) -> list[tuple[str, Optional[str], Optional[str], Optional[str]]]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT
                case_number,
                COALESCE(parcel_id, folio) AS folio,
                property_address,
                auction_date
            FROM auctions
            WHERE property_address IS NOT NULL AND TRIM(property_address) <> ''
            """
        ).fetchall()
        return [(str(r[0]), r[1], r[2], r[3]) for r in rows]
    finally:
        conn.close()


def load_home_harvest_photo_counts(db_path: Path) -> dict[str, int]:
    if not db_path.exists():
        return {}
    conn = sqlite3.connect(db_path)
    try:
        try:
            rows = conn.execute("SELECT folio, photos, primary_photo FROM home_harvest").fetchall()
        except sqlite3.DatabaseError:
            return {}
        counts: dict[str, int] = {}
        for folio, photos, primary_photo in rows:
            count = 0
            if photos:
                try:
                    parsed = json.loads(photos)
                    if isinstance(parsed, list):
                        count = len(parsed)
                except (TypeError, ValueError, json.JSONDecodeError):
                    count = 0
            if count == 0 and primary_photo:
                count = 1
            counts[str(folio)] = max(counts.get(str(folio), 0), count)
        return counts
    finally:
        conn.close()


def compare_with_local_db(records: list[ListingRecord], db_path: Path) -> dict[str, Any]:
    auctions = load_auctions(db_path)
    home_harvest_counts = load_home_harvest_photo_counts(db_path)

    by_address: dict[str, tuple[str, Optional[str], Optional[str], Optional[str]]] = {}
    for case_number, folio, address, auction_date in auctions:
        key = normalize_address(address or "")
        if key and key not in by_address:
            by_address[key] = (case_number, folio, address, auction_date)

    matched = 0
    external_with_photos = 0
    matched_with_external_photos = 0
    matched_with_home_harvest_photos = 0

    for record in records:
        if record.photo_count > 0:
            external_with_photos += 1
        key = normalize_address(record.full_address or "")
        match = by_address.get(key)
        if match:
            matched += 1
            case_number, folio, address, _auction_date = match
            record.matched_case_number = case_number
            record.matched_folio = folio
            record.matched_address = address
            if record.photo_count > 0:
                matched_with_external_photos += 1
            if folio and folio in home_harvest_counts:
                count = home_harvest_counts[folio]
                record.home_harvest_photo_count = count
                if count > 0:
                    matched_with_home_harvest_photos += 1

    return {
        "external_records": len(records),
        "matched_to_local_auctions": matched,
        "match_rate": round((matched / len(records)), 4) if records else 0.0,
        "external_with_photos": external_with_photos,
        "external_photo_rate": round((external_with_photos / len(records)), 4) if records else 0.0,
        "matched_with_external_photos": matched_with_external_photos,
        "matched_with_home_harvest_photos": matched_with_home_harvest_photos,
    }


def write_markdown_report(path: Path, crawl_stats: dict[str, Any], compare_stats: dict[str, Any]) -> None:
    lines = [
        "# HillsForeclosures Benchmark Report",
        "",
        f"- Generated: `{now_utc()}`",
        "",
        "## Crawl Stats",
        "",
        f"- Pages visited: `{crawl_stats['pages_visited']}`",
        f"- Listing pages visited: `{crawl_stats['list_pages_visited']}`",
        f"- Property pages visited: `{crawl_stats['property_pages_visited']}`",
        f"- Unique property URLs discovered: `{crawl_stats['property_urls_discovered']}`",
        f"- Records extracted: `{crawl_stats['records_extracted']}`",
        "",
        "## Photo Download",
        "",
        f"- Downloaded: `{crawl_stats['photos_downloaded']}`",
        f"- Failed: `{crawl_stats['photos_failed']}`",
        "",
        "## Compare to Local DB",
        "",
        f"- External records: `{compare_stats['external_records']}`",
        f"- Matched to local auctions: `{compare_stats['matched_to_local_auctions']}`",
        f"- Match rate: `{compare_stats['match_rate']}`",
        f"- External records with photos: `{compare_stats['external_with_photos']}`",
        f"- External photo rate: `{compare_stats['external_photo_rate']}`",
        f"- Matched records with external photos: `{compare_stats['matched_with_external_photos']}`",
        f"- Matched records with HomeHarvest photos: `{compare_stats['matched_with_home_harvest_photos']}`",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def crawl_all(
    context: BrowserContext,
    out_dir: Path,
    max_pages: int,
    max_properties: int,
) -> tuple[list[ListingRecord], dict[str, Any]]:
    ensure_dir(out_dir)
    raw_pages_dir = out_dir / "pages"
    ensure_dir(raw_pages_dir)

    queue = deque([normalize_url(url) for url in SEED_URLS])
    visited: set[str] = set()
    discovered_property_urls: set[str] = set()
    records: list[ListingRecord] = []

    pages_visited = 0
    list_pages_visited = 0
    property_pages_visited = 0

    page = context.new_page()
    page.set_default_timeout(NAVIGATION_TIMEOUT_MS)

    while queue:
        if max_pages > 0 and pages_visited >= max_pages:
            break
        url = queue.popleft()
        if url in visited:
            continue
        visited.add(url)

        response = None
        title = ""
        text = ""
        html = ""
        current = url

        for attempt in range(3):
            try:
                response = page.goto(url, wait_until="domcontentloaded")
                page.wait_for_timeout(2000)
                title = page.title()
                text = page.inner_text("body")
                html = page.content()
                current = normalize_url(page.url)
                if is_challenge_page(title, text):
                    logger.warning(f"Challenge page at {url}, retry {attempt + 1}/3")
                    page.wait_for_timeout(6000)
                    continue
                break
            except PlaywrightTimeoutError:
                logger.warning(f"Timeout loading {url}, retry {attempt + 1}/3")
                page.wait_for_timeout(3000)
        else:
            logger.error(f"Failed to load after retries: {url}")
            continue

        pages_visited += 1
        status = response.status if response else None
        logger.info(f"[{pages_visited}] {status} {current}")

        links = extract_links(page)
        for link in links:
            if not is_hills_url(link):
                continue
            if is_property_url(link):
                if link not in discovered_property_urls:
                    discovered_property_urls.add(link)
                if (
                    link not in visited
                    and link not in queue
                    and (max_properties <= 0 or len(discovered_property_urls) <= max_properties)
                ):
                    queue.append(link)
            elif is_list_url(link):
                if link not in visited and link not in queue:
                    queue.append(link)

        if is_property_url(current):
            property_pages_visited += 1
            prop_match = re.search(r"/property-info/([0-9]+)/([^/?#]+)", current)
            slug = prop_match.group(2) if prop_match else slugify(current)
            pid = prop_match.group(1) if prop_match else "unknown"
            html_path = raw_pages_dir / f"{pid}_{slug}.html"
            text_path = raw_pages_dir / f"{pid}_{slug}.txt"
            html_path.write_text(html, encoding="utf-8")
            text_path.write_text(text, encoding="utf-8")
            record = extract_record_from_page(current, title, html, text, html_path, text_path)
            records.append(record)
        elif is_list_url(current):
            list_pages_visited += 1

        page.wait_for_timeout(1200)

    page.close()
    stats = {
        "pages_visited": pages_visited,
        "list_pages_visited": list_pages_visited,
        "property_pages_visited": property_pages_visited,
        "property_urls_discovered": len(discovered_property_urls),
        "records_extracted": len(records),
    }
    return records, stats


def main() -> None:
    args = parse_args()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = args.output_dir / timestamp
    ensure_dir(run_dir)
    logger.remove()
    logger.add(lambda message: print(message, end=""), level="INFO")

    logger.info(f"Output dir: {run_dir}\n")
    logger.info("Starting HillsForeclosures crawl (real Chrome, non-headless, persistent profile)\n")

    with sync_playwright() as playwright:
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(args.profile_dir),
            channel="chrome",
            headless=False,
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            timezone_id="America/New_York",
            user_agent=USER_AGENT,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        records, stats = crawl_all(
            context=context,
            out_dir=run_dir,
            max_pages=args.max_pages,
            max_properties=args.max_properties,
        )
        context.close()

    photo_stats = {"downloaded": 0, "failed": 0}
    if args.download_photos:
        logger.info("Downloading property photos...\n")
        photo_stats = download_photos(records, run_dir / "photos")

    compare_stats = compare_with_local_db(records, args.db_path)

    # Save outputs
    save_record_jsonl(run_dir / "hills_listings.jsonl", records)
    save_record_csv(run_dir / "hills_listings.csv", records)

    crawl_stats = {
        **stats,
        "photos_downloaded": photo_stats["downloaded"],
        "photos_failed": photo_stats["failed"],
    }
    (run_dir / "crawl_stats.json").write_text(json.dumps(crawl_stats, indent=2), encoding="utf-8")
    (run_dir / "compare_stats.json").write_text(json.dumps(compare_stats, indent=2), encoding="utf-8")
    write_markdown_report(run_dir / "comparison_report.md", crawl_stats, compare_stats)

    logger.info(f"Crawl complete. Records: {len(records)}\n")
    logger.info(f"Run artifacts: {run_dir}\n")
    logger.info(json.dumps({"crawl_stats": crawl_stats, "compare_stats": compare_stats}, indent=2) + "\n")


if __name__ == "__main__":
    main()
