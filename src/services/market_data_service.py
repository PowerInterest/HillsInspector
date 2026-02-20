"""
Market Data Service — standalone service for property web data.

Orchestrates Redfin, Zillow, and HomeHarvest into a single batch operation.
Owns the Chrome browser lifecycle (one session, shared across Redfin + Zillow).
Writes directly to PG ``property_market`` table — no SQLite intermediate.

Can run independently: `uv run python -m src.services.market_data_service`
"""
import asyncio
import contextlib
import json
import random
import re
from pathlib import Path
from typing import Any

from loguru import logger
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.scrapers.redfin_scraper import (
    RedfinScraper,
    normalize_address_for_match,
)
from sunbiz.db import get_engine, resolve_pg_dsn
from sunbiz.models import Base, PropertyMarket

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PROFILE_DIR = PROJECT_ROOT / "data" / "browser_profiles" / "user_chrome"


class MarketDataService:
    """Batch market data fetcher: Redfin → Zillow → HomeHarvest → PG."""

    REDFIN_DETAIL_DELAY = (5.0, 10.0)
    ZILLOW_SEARCH_DELAY = (8.0, 14.0)
    ZILLOW_BLOCK_LIMIT = 3      # consecutive CAPTCHAs → stop Zillow
    REDFIN_FAIL_LIMIT = 5       # consecutive Tier 2 failures → stop

    def __init__(self, dsn: str | None = None):
        self._dsn = resolve_pg_dsn(dsn)
        self._engine = get_engine(self._dsn)
        self._ensure_pg_table()

    def _ensure_pg_table(self) -> None:
        """Create the property_market table if it doesn't exist."""
        try:
            Base.metadata.create_all(bind=self._engine, tables=[
                Base.metadata.tables["property_market"],
            ])
        except Exception as e:
            logger.warning(f"Failed to ensure property_market table: {e}")

    async def run_batch(
        self,
        properties: list[dict],
        sources: list[str] | None = None,
    ) -> dict:
        """Run all market data sources for the given properties.

        Each dict needs: strap, folio, case_number, property_address.
        Properties come from PG foreclosures table.
        Returns summary: {redfin: N, zillow: N, homeharvest: N, photos: N}
        """
        sources = sources or ["redfin", "zillow", "homeharvest"]
        summary = {"redfin": 0, "zillow": 0, "homeharvest": 0, "photos": 0}
        matched_straps: set[str] = set()

        # Filter out properties that already have market data in PG
        need_market = []
        for prop in properties:
            strap = prop.get("strap", "")
            addr = (prop.get("property_address") or "").strip()
            if not addr or addr.lower() in ("unknown", "n/a", "none", ""):
                continue
            if not strap:
                continue
            if self._has_market_data(strap):
                matched_straps.add(strap)
                continue
            need_market.append(prop)

        logger.info(
            f"MarketDataService: {len(need_market)} properties need data "
            f"({len(matched_straps)} already have it)"
        )
        if not need_market:
            return summary

        # --- Browser-based sources (Redfin + Zillow) ---
        run_redfin = "redfin" in sources
        run_zillow = "zillow" in sources
        if run_redfin or run_zillow:
            pw = None
            context = None
            try:
                pw, context, page, cdp = await self._launch_browser()

                if run_redfin:
                    redfin_matched = await self._run_redfin(
                        context, page, need_market, matched_straps,
                    )
                    summary["redfin"] = len(redfin_matched)
                    matched_straps.update(redfin_matched)
                    logger.info(f"Redfin done: {summary['redfin']} properties matched")

                if run_zillow:
                    remaining = [
                        p for p in need_market
                        if p.get("strap", "") not in matched_straps
                    ]
                    if remaining:
                        # Navigate to Zillow homepage before starting
                        await page.goto(
                            "https://www.zillow.com",
                            wait_until="domcontentloaded",
                            timeout=60000,
                        )
                        await page.wait_for_timeout(3000)

                        zillow_matched = await self._run_zillow(
                            page, cdp, remaining, matched_straps,
                        )
                        summary["zillow"] = len(zillow_matched)
                        matched_straps.update(zillow_matched)
                        logger.info(f"Zillow done: {summary['zillow']} properties matched")

            except Exception as exc:
                logger.error(f"MarketDataService browser phase failed: {exc}")
            finally:
                if context:
                    with contextlib.suppress(Exception):
                        await context.close()
                if pw:
                    with contextlib.suppress(Exception):
                        await pw.stop()

        # --- HomeHarvest (no browser) ---
        if "homeharvest" in sources:
            still_remaining = [
                p for p in need_market
                if p.get("strap", "") not in matched_straps
            ]
            if still_remaining:
                hh_count = await self._run_homeharvest(still_remaining)
                summary["homeharvest"] = hh_count
                logger.info(f"HomeHarvest done: {hh_count} properties matched")

        # --- Photo download ---
        try:
            photo_count = self._download_all_photos(need_market)
            summary["photos"] = photo_count
        except Exception as exc:
            logger.error(f"Photo download failed: {exc}")

        logger.success(f"MarketDataService complete: {summary}")
        return summary

    # ------------------------------------------------------------------
    # PG helpers
    # ------------------------------------------------------------------

    def _has_market_data(self, strap: str) -> bool:
        """Check if PG property_market already has data for this strap."""
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT zestimate, list_price, redfin_json, zillow_json "
                        "FROM property_market WHERE strap = :strap"
                    ),
                    {"strap": strap},
                ).fetchone()
                if not row:
                    return False
                # Has data if any valuation or raw JSON is present
                return any(row[i] is not None for i in range(4))
        except Exception as exc:
            logger.debug(f"Market existing-data check failed for strap={strap}: {exc}")
            return False

    def _upsert_redfin(self, strap: str, folio: str | None, case_number: str, payload: dict) -> None:
        """Upsert Redfin data into PG property_market.

        Redfin is highest priority for: list_price, listing_status.
        Lower priority for: zestimate (Zillow > Redfin).
        """
        row = {
            "strap": strap,
            "folio": folio,
            "case_number": case_number,
            "list_price": payload.get("list_price"),
            "zestimate": payload.get("zestimate"),
            "listing_status": payload.get("listing_status"),
            "beds": _i(payload.get("beds")),
            "baths": _f(payload.get("baths")),
            "sqft": _i(payload.get("sqft")),
            "year_built": _i(payload.get("year_built")),
            "lot_size": str(payload.get("lot_size") or "") or None,
            "property_type": payload.get("property_type"),
            "detail_url": payload.get("detail_url"),
            "photo_cdn_urls": payload.get("photos") or [],
            "redfin_json": payload,
            "primary_source": "redfin",
        }

        stmt = pg_insert(PropertyMarket).values([row])
        stmt = stmt.on_conflict_do_update(
            index_elements=["strap"],
            set_={
                "folio": text("COALESCE(EXCLUDED.folio, property_market.folio)"),
                "case_number": text("COALESCE(EXCLUDED.case_number, property_market.case_number)"),
                # Redfin wins for list_price, listing_status
                "list_price": text("COALESCE(EXCLUDED.list_price, property_market.list_price)"),
                "listing_status": text("COALESCE(EXCLUDED.listing_status, property_market.listing_status)"),
                # Zillow > Redfin for zestimate — keep existing if present
                "zestimate": text("COALESCE(property_market.zestimate, EXCLUDED.zestimate)"),
                # Specs: keep existing (HomeHarvest > Redfin)
                "beds": text("COALESCE(property_market.beds, EXCLUDED.beds)"),
                "baths": text("COALESCE(property_market.baths, EXCLUDED.baths)"),
                "sqft": text("COALESCE(property_market.sqft, EXCLUDED.sqft)"),
                "year_built": text("COALESCE(property_market.year_built, EXCLUDED.year_built)"),
                "lot_size": text("COALESCE(property_market.lot_size, EXCLUDED.lot_size)"),
                "property_type": text("COALESCE(property_market.property_type, EXCLUDED.property_type)"),
                # Redfin wins for detail_url
                "detail_url": text("COALESCE(EXCLUDED.detail_url, property_market.detail_url)"),
                # Always update photo CDN URLs and raw JSON
                "photo_cdn_urls": text("COALESCE(EXCLUDED.photo_cdn_urls, property_market.photo_cdn_urls)"),
                "redfin_json": stmt.excluded.redfin_json,
                "primary_source": text("COALESCE(property_market.primary_source, EXCLUDED.primary_source)"),
                "updated_at": text("NOW()"),
            },
        )
        try:
            with self._engine.begin() as conn:
                conn.execute(stmt)
        except Exception as e:
            logger.error(f"PG Redfin upsert failed for {strap}: {e}")

    def _upsert_zillow(self, strap: str, folio: str | None, case_number: str, payload: dict) -> None:
        """Upsert Zillow data into PG property_market.

        Zillow is highest priority for: zestimate, rent_zestimate.
        Lower priority for: list_price (Redfin > Zillow).
        """
        row = {
            "strap": strap,
            "folio": folio,
            "case_number": case_number,
            "zestimate": payload.get("zestimate"),
            "rent_zestimate": payload.get("rent_zestimate") or payload.get("rent_estimate"),
            "list_price": payload.get("list_price"),
            "tax_assessed_value": payload.get("tax_assessed_value"),
            "listing_status": payload.get("listing_status"),
            "beds": _i(payload.get("beds")),
            "baths": _f(payload.get("baths")),
            "sqft": _i(payload.get("sqft")),
            "year_built": _i(payload.get("year_built")),
            "lot_size": str(payload.get("lot_size") or "") or None,
            "property_type": payload.get("property_type"),
            "detail_url": payload.get("detail_url"),
            "photo_cdn_urls": payload.get("photos") or [],
            "zillow_json": payload,
            "primary_source": "zillow",
        }

        stmt = pg_insert(PropertyMarket).values([row])
        stmt = stmt.on_conflict_do_update(
            index_elements=["strap"],
            set_={
                "folio": text("COALESCE(EXCLUDED.folio, property_market.folio)"),
                "case_number": text("COALESCE(EXCLUDED.case_number, property_market.case_number)"),
                # Zillow wins for zestimate, rent_zestimate
                "zestimate": text("COALESCE(EXCLUDED.zestimate, property_market.zestimate)"),
                "rent_zestimate": text("COALESCE(EXCLUDED.rent_zestimate, property_market.rent_zestimate)"),
                # Redfin > Zillow for list_price — keep existing if present
                "list_price": text("COALESCE(property_market.list_price, EXCLUDED.list_price)"),
                "tax_assessed_value": text("COALESCE(EXCLUDED.tax_assessed_value, property_market.tax_assessed_value)"),
                # Keep existing listing_status (Redfin > Zillow)
                "listing_status": text("COALESCE(property_market.listing_status, EXCLUDED.listing_status)"),
                # Specs: keep existing (HomeHarvest > Zillow)
                "beds": text("COALESCE(property_market.beds, EXCLUDED.beds)"),
                "baths": text("COALESCE(property_market.baths, EXCLUDED.baths)"),
                "sqft": text("COALESCE(property_market.sqft, EXCLUDED.sqft)"),
                "year_built": text("COALESCE(property_market.year_built, EXCLUDED.year_built)"),
                "lot_size": text("COALESCE(property_market.lot_size, EXCLUDED.lot_size)"),
                "property_type": text("COALESCE(property_market.property_type, EXCLUDED.property_type)"),
                # Keep existing detail_url (Redfin > Zillow)
                "detail_url": text("COALESCE(property_market.detail_url, EXCLUDED.detail_url)"),
                # Update photo CDN URLs if we don't have any yet
                "photo_cdn_urls": text("COALESCE(property_market.photo_cdn_urls, EXCLUDED.photo_cdn_urls)"),
                "zillow_json": stmt.excluded.zillow_json,
                "primary_source": text(
                    "CASE WHEN property_market.primary_source IS NULL THEN 'zillow' "
                    "ELSE property_market.primary_source END"
                ),
                "updated_at": text("NOW()"),
            },
        )
        try:
            with self._engine.begin() as conn:
                conn.execute(stmt)
        except Exception as e:
            logger.error(f"PG Zillow upsert failed for {strap}: {e}")

    def _upsert_homeharvest(self, strap: str, folio: str | None, case_number: str, payload: dict) -> None:
        """Upsert HomeHarvest data into PG property_market.

        HomeHarvest is highest priority for: beds, baths, sqft, year_built.
        Lowest priority for: zestimate, list_price.
        """
        row = {
            "strap": strap,
            "folio": folio,
            "case_number": case_number,
            "zestimate": _f(payload.get("estimated_value")),
            "rent_zestimate": None,
            "list_price": _f(payload.get("list_price")),
            "tax_assessed_value": None,
            "beds": _i(payload.get("beds")),
            "baths": _f(payload.get("full_baths")),
            "sqft": _i(payload.get("sqft")),
            "year_built": _i(payload.get("year_built")),
            "lot_size": str(payload.get("lot_sqft") or "") or None,
            "property_type": payload.get("style"),
            "listing_status": payload.get("status"),
            "detail_url": payload.get("property_url"),
            "photo_cdn_urls": _extract_hh_photos(payload),
            "homeharvest_json": payload,
            "primary_source": "homeharvest",
        }

        stmt = pg_insert(PropertyMarket).values([row])
        stmt = stmt.on_conflict_do_update(
            index_elements=["strap"],
            set_={
                "folio": text("COALESCE(EXCLUDED.folio, property_market.folio)"),
                "case_number": text("COALESCE(EXCLUDED.case_number, property_market.case_number)"),
                # HomeHarvest is lowest priority for valuations
                "zestimate": text("COALESCE(property_market.zestimate, EXCLUDED.zestimate)"),
                "rent_zestimate": text("COALESCE(property_market.rent_zestimate, EXCLUDED.rent_zestimate)"),
                "list_price": text("COALESCE(property_market.list_price, EXCLUDED.list_price)"),
                "tax_assessed_value": text("COALESCE(property_market.tax_assessed_value, EXCLUDED.tax_assessed_value)"),
                "listing_status": text("COALESCE(property_market.listing_status, EXCLUDED.listing_status)"),
                # HomeHarvest wins for specs
                "beds": text("COALESCE(EXCLUDED.beds, property_market.beds)"),
                "baths": text("COALESCE(EXCLUDED.baths, property_market.baths)"),
                "sqft": text("COALESCE(EXCLUDED.sqft, property_market.sqft)"),
                "year_built": text("COALESCE(EXCLUDED.year_built, property_market.year_built)"),
                "lot_size": text("COALESCE(EXCLUDED.lot_size, property_market.lot_size)"),
                "property_type": text("COALESCE(EXCLUDED.property_type, property_market.property_type)"),
                # Keep existing detail_url (Redfin/Zillow > HomeHarvest)
                "detail_url": text("COALESCE(property_market.detail_url, EXCLUDED.detail_url)"),
                # Update photo CDN URLs if we don't have any yet
                "photo_cdn_urls": text("COALESCE(property_market.photo_cdn_urls, EXCLUDED.photo_cdn_urls)"),
                "homeharvest_json": stmt.excluded.homeharvest_json,
                "primary_source": text(
                    "CASE WHEN property_market.primary_source IS NULL THEN 'homeharvest' "
                    "ELSE property_market.primary_source END"
                ),
                "updated_at": text("NOW()"),
            },
        )
        try:
            with self._engine.begin() as conn:
                conn.execute(stmt)
        except Exception as e:
            logger.error(f"PG HomeHarvest upsert failed for {strap}: {e}")

    # ------------------------------------------------------------------
    # Photo download
    # ------------------------------------------------------------------

    def _download_all_photos(self, properties: list[dict]) -> int:
        """Download photos for all properties that have CDN URLs in PG."""
        import hashlib
        import time as _time

        import requests

        MAX_PHOTOS = 15
        DOWNLOAD_TIMEOUT = 15
        IMAGE_CONTENT_TYPES = frozenset({
            "image/jpeg", "image/png", "image/webp", "image/gif",
        })
        EXT_MAP = {
            "image/jpeg": ".jpg",
            "image/png": ".png",
            "image/webp": ".webp",
            "image/gif": ".gif",
        }

        session = requests.Session()
        session.headers["User-Agent"] = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )

        straps = [p["strap"] for p in properties if p.get("strap")]
        if not straps:
            return 0

        # Fetch CDN URLs and case_number from PG
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT strap, case_number, photo_cdn_urls "
                        "FROM property_market "
                        "WHERE strap = ANY(:straps) "
                        "  AND photo_cdn_urls IS NOT NULL "
                        "  AND jsonb_array_length(photo_cdn_urls) > 0"
                    ),
                    {"straps": straps},
                ).fetchall()
        except Exception as e:
            logger.error(f"Failed to fetch photo CDN URLs from PG: {e}")
            return 0

        total_downloaded = 0
        for row in rows:
            strap, case_number, cdn_urls = row[0], row[1], row[2]
            if not case_number or not cdn_urls:
                continue

            urls = cdn_urls if isinstance(cdn_urls, list) else []
            if not urls:
                continue

            photos_dir = PROJECT_ROOT / "data" / "Foreclosure" / case_number / "photos"
            photos_dir.mkdir(parents=True, exist_ok=True)

            local_paths: list[str] = []
            for idx, url in enumerate(urls[:MAX_PHOTOS]):
                try:
                    url_hash = hashlib.sha1(url.encode()).hexdigest()[:12]
                    existing = list(photos_dir.glob(f"{idx:03d}_{url_hash}.*"))
                    if existing:
                        rel = str(existing[0].relative_to(PROJECT_ROOT / "data"))
                        local_paths.append(rel)
                        continue

                    resp = session.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True)
                    if resp.status_code != 200:
                        continue

                    content_type = resp.headers.get("Content-Type", "").split(";")[0].strip().lower()
                    if content_type not in IMAGE_CONTENT_TYPES:
                        continue

                    ext = EXT_MAP.get(content_type, ".jpg")
                    filename = f"{idx:03d}_{url_hash}{ext}"
                    filepath = photos_dir / filename
                    filepath.write_bytes(resp.content)

                    rel = str(filepath.relative_to(PROJECT_ROOT / "data"))
                    local_paths.append(rel)
                    total_downloaded += 1

                    if idx < len(urls[:MAX_PHOTOS]) - 1:
                        _time.sleep(0.5)

                except Exception as dl_err:
                    logger.debug(f"Photo {idx} download failed: {dl_err}")
                    continue

            # Update PG with local paths
            if local_paths:
                try:
                    with self._engine.begin() as conn:
                        conn.execute(
                            text(
                                "UPDATE property_market SET photo_local_paths = CAST(:paths AS jsonb), "
                                "updated_at = NOW() WHERE strap = :strap"
                            ),
                            {"paths": json.dumps(local_paths), "strap": strap},
                        )
                except Exception as e:
                    logger.error(f"Failed to update photo paths for {strap}: {e}")

        return total_downloaded

    # ------------------------------------------------------------------
    # Browser lifecycle
    # ------------------------------------------------------------------

    async def _launch_browser(self):
        """Launch real Chrome with user_chrome profile, stealth, CDP."""
        PROFILE_DIR.mkdir(parents=True, exist_ok=True)
        pw = await async_playwright().start()
        context = await pw.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
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
        page = context.pages[0] if context.pages else await context.new_page()
        await Stealth().apply_stealth_async(page)
        cdp = await context.new_cdp_session(page)
        logger.info("MarketDataService: Chrome launched with user_chrome profile")
        return pw, context, page, cdp

    # ------------------------------------------------------------------
    # Redfin
    # ------------------------------------------------------------------

    async def _run_redfin(
        self,
        context,
        page,
        properties: list[dict],
        already_matched: set[str],
    ) -> set[str]:
        """Run Redfin Tier 1 + Tier 2. Returns set of newly matched straps."""
        matched: set[str] = set()

        # Build normalized address → property lookup
        addr_to_props: dict[str, list[dict]] = {}
        for row in properties:
            addr = (row.get("property_address") or "").strip()
            norm = normalize_address_for_match(addr)
            if norm:
                addr_to_props.setdefault(norm, []).append(row)

        async with RedfinScraper(context=context, page=page) as scraper:
            # ---- Tier 1: Foreclosure listings page ----
            listings = await scraper.scrape_foreclosure_listings()
            logger.info(f"Redfin Tier 1: {len(listings)} foreclosure listings")

            for card in listings:
                card_addr = normalize_address_for_match(card.get("address", ""))
                if not card_addr:
                    continue
                matching = addr_to_props.get(card_addr)
                if not matching:
                    continue

                detail_url = card.get("url", "")
                if not detail_url:
                    continue

                logger.info(f"Redfin Tier 1 match: {card.get('address')} → {detail_url}")
                listing = await scraper.scrape_detail_page(detail_url)
                if listing:
                    payload = RedfinScraper.listing_to_market_payload(listing)
                    for prop in matching:
                        strap = prop.get("strap", "")
                        folio = prop.get("folio")
                        case = prop.get("case_number", "")
                        if strap and strap not in matched and strap not in already_matched:
                            self._upsert_redfin(strap, folio, case, payload)
                            matched.add(strap)
                            logger.success(f"Redfin Tier 1: saved for {strap}")

                await scraper.delay(scraper.DETAIL_PAGE_DELAY)

            # ---- Tier 2: Direct URL for unmatched ----
            unmatched = [
                row
                for rows in addr_to_props.values()
                for row in rows
                if row.get("strap", "") not in matched
                and row.get("strap", "") not in already_matched
            ]
            logger.info(f"Redfin Tier 2: {len(unmatched)} unmatched for direct URL")

            consecutive_failures = 0
            for row in unmatched:
                if consecutive_failures >= self.REDFIN_FAIL_LIMIT:
                    logger.warning(
                        f"Redfin Tier 2: {self.REDFIN_FAIL_LIMIT} consecutive "
                        f"failures — stopping"
                    )
                    break

                addr = (row.get("property_address") or "").strip()
                strap = row.get("strap", "")
                folio = row.get("folio")
                case = row.get("case_number", "")
                # Extract city/zip from address or use defaults
                city = (row.get("city") or "Tampa").strip()
                zip_code = (row.get("zip_code") or "").strip()

                street = addr.split(",")[0].strip()
                url = RedfinScraper.build_detail_url(street, city, "FL", zip_code)
                logger.info(f"Redfin Tier 2: trying {url}")

                listing = await scraper.scrape_detail_page(url)
                if listing and (listing.list_price or listing.redfin_estimate):
                    payload = RedfinScraper.listing_to_market_payload(listing)
                    self._upsert_redfin(strap, folio, case, payload)
                    matched.add(strap)
                    consecutive_failures = 0
                    logger.success(f"Redfin Tier 2: saved for {strap}")
                else:
                    consecutive_failures += 1
                    logger.debug(f"Redfin Tier 2: no data for {addr}")

                await scraper.delay(scraper.DETAIL_PAGE_DELAY)

        return matched

    # ------------------------------------------------------------------
    # Zillow
    # ------------------------------------------------------------------

    async def _run_zillow(
        self,
        page,
        cdp,
        properties: list[dict],
        already_matched: set[str],
    ) -> set[str]:
        """Run Zillow searches via CDP. Returns set of newly matched straps."""
        from src.scrapers.zillow_scraper import (
            search_property,
            listing_to_market_payload,
            _is_blocked,
        )

        matched: set[str] = set()
        consecutive_blocks = 0

        # Check if homepage is already blocked
        if await _is_blocked(page):
            logger.warning("Zillow: blocked on homepage — skipping all searches")
            return matched

        for i, prop in enumerate(properties):
            if consecutive_blocks >= self.ZILLOW_BLOCK_LIMIT:
                logger.warning(
                    f"Zillow: {self.ZILLOW_BLOCK_LIMIT} consecutive blocks — stopping"
                )
                break

            strap = prop.get("strap", "")
            folio = prop.get("folio")
            case = prop.get("case_number", "")
            addr = (prop.get("property_address") or "").strip()

            if strap in already_matched or strap in matched:
                continue

            logger.info(f"Zillow [{i+1}/{len(properties)}]: searching '{addr}'")

            listing = await search_property(page, cdp, addr)

            if listing is None:
                # None = blocked
                consecutive_blocks += 1
                logger.warning(
                    f"Zillow: block #{consecutive_blocks} on '{addr}'"
                )
                # Try to recover by going back to homepage
                await page.wait_for_timeout(5000)
                try:
                    await page.goto(
                        "https://www.zillow.com",
                        wait_until="domcontentloaded",
                        timeout=30000,
                    )
                    await page.wait_for_timeout(3000)
                except Exception as nav_err:
                    logger.debug(f"Zillow: homepage recovery failed: {nav_err}")
                continue

            # Empty listing (not found, not blocked) — don't count as block
            consecutive_blocks = 0

            if listing.zestimate or listing.price or listing.rent_zestimate:
                payload = listing_to_market_payload(listing)
                self._upsert_zillow(strap, folio, case, payload)
                matched.add(strap)
                logger.success(
                    f"Zillow: saved for {strap} "
                    f"(zest={listing.zestimate}, photos={len(listing.photos)})"
                )
            else:
                logger.debug(f"Zillow: no useful data for '{addr}'")

            # Delay between searches
            delay = random.uniform(*self.ZILLOW_SEARCH_DELAY)  # noqa: S311
            await page.wait_for_timeout(int(delay * 1000))

            # Navigate back to homepage for next search
            try:
                await page.goto(
                    "https://www.zillow.com",
                    wait_until="domcontentloaded",
                    timeout=30000,
                )
                await page.wait_for_timeout(2000)
            except Exception as nav_err:
                logger.debug(f"Zillow: homepage navigation failed: {nav_err}")

        return matched

    # ------------------------------------------------------------------
    # HomeHarvest
    # ------------------------------------------------------------------

    async def _run_homeharvest(self, properties: list[dict]) -> int:
        """Run HomeHarvest API for remaining properties. Returns count matched."""
        from homeharvest import scrape_property

        count = 0

        for i, prop in enumerate(properties):
            strap = prop.get("strap", "")
            folio = prop.get("folio")
            case = prop.get("case_number", "")
            addr = (prop.get("property_address") or "").strip()

            # Build location string
            if re.search(r"FL\s+\d{5}", addr):
                location = addr
            else:
                parts = [addr]
                if "FL" not in addr.upper():
                    parts.append("FL")
                location = ", ".join(parts).replace("FL, ", "FL ")

            try:
                kwargs: dict[str, Any] = {
                    "location": location,
                    "listing_type": "sold",
                    "past_days": 3650,
                    "parallel": False,
                }
                df = scrape_property(**kwargs)

                if df is None or df.empty:
                    logger.debug(f"HomeHarvest: no data for '{addr}'")
                else:
                    # Convert pandas row to dict at boundary
                    row_data = df.iloc[0].to_dict()
                    hh_payload = _build_homeharvest_payload(folio or strap, row_data)
                    self._upsert_homeharvest(strap, folio, case, hh_payload)
                    count += 1
                    logger.success(f"HomeHarvest: saved for {strap}")

            except SystemExit:
                logger.warning("HomeHarvest: upgrade triggered — stopping batch")
                break
            except Exception as exc:
                error_str = str(exc).lower()
                if any(x in error_str for x in ("403", "blocked", "retryerror", "429")):
                    logger.warning(f"HomeHarvest: blocked — stopping ({exc})")
                    break
                logger.error(f"HomeHarvest failed for {strap}: {exc}")

            # Delay between requests
            if i < len(properties) - 1:
                delay = random.uniform(15.0, 30.0)  # noqa: S311
                await asyncio.sleep(delay)

        return count


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _f(val: Any) -> float | None:
    """Safely convert to float, returning None for non-positive/invalid."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def _i(val: Any) -> int | None:
    """Safely convert to int, returning None for non-positive/invalid."""
    if val is None:
        return None
    try:
        v = int(float(val))
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def _safe_json(value: Any) -> Any:
    """Parse a JSON string or return the value unchanged."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return None
    return None


def _extract_hh_photos(payload: dict) -> list[str]:
    """Extract photo URLs from HomeHarvest payload."""
    urls: list[str] = []
    primary = (payload.get("primary_photo") or "").strip()
    if primary:
        urls.append(primary)
    for field in ("photos", "alt_photos"):
        raw = _safe_json(payload.get(field))
        if isinstance(raw, list):
            for u in raw:
                if isinstance(u, str) and u.strip():
                    urls.append(u.strip())
    return list(dict.fromkeys(urls))  # dedupe, preserve order


def _build_homeharvest_payload(folio: str, row: dict) -> dict:
    """Build a flat dict from a HomeHarvest pandas row for PG upsert."""
    import pandas as pd

    def _is_na(v: Any) -> bool:
        if v is None:
            return True
        try:
            return bool(pd.isna(v))
        except (TypeError, ValueError):
            return False

    def val(col: str, dtype: str = "str") -> Any:
        if col not in row:
            return None
        v = row[col]
        if _is_na(v):
            return None
        try:
            if dtype == "json":
                return json.dumps(v, default=str)
            if dtype == "bool":
                return bool(v)
            if dtype == "int":
                return int(v)
            if dtype == "float":
                return float(v)
            return str(v)
        except (TypeError, ValueError):
            return None

    return {
        "folio": folio,
        "property_url": val("property_url"),
        "status": val("status"),
        "style": val("style"),
        "beds": val("beds", "float"),
        "full_baths": val("full_baths", "float"),
        "half_baths": val("half_baths", "float"),
        "sqft": val("sqft", "float"),
        "year_built": val("year_built", "int"),
        "lot_sqft": val("lot_sqft", "float"),
        "list_price": val("list_price", "float"),
        "sold_price": val("sold_price", "float"),
        "estimated_value": val("estimated_value", "float"),
        "primary_photo": val("primary_photo"),
        "photos": val("photos", "json"),
        "alt_photos": val("alt_photos", "json"),
        "property_type": val("style"),
        "estimated_monthly_rental": val("estimated_monthly_rental", "float"),
    }


# ---------------------------------------------------------------------------
# Standalone CLI
# ---------------------------------------------------------------------------

def _query_properties_needing_market(dsn: str | None = None, limit: int = 0) -> list[dict]:
    """Query properties needing market data from PG foreclosures."""
    engine = get_engine(resolve_pg_dsn(dsn))
    query = """
        SELECT f.strap, f.folio, f.case_number_raw AS case_number, f.property_address
        FROM foreclosures f
        LEFT JOIN property_market pm ON f.strap = pm.strap
        WHERE f.strap IS NOT NULL
          AND f.property_address IS NOT NULL
          AND (pm.strap IS NULL OR pm.zestimate IS NULL)
        ORDER BY f.auction_date DESC
    """
    if limit:
        query += f" LIMIT {limit}"
    with engine.connect() as conn:
        rows = conn.execute(text(query)).fetchall()
    return [dict(r._mapping) for r in rows]  # noqa: SLF001


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch market data for foreclosure properties (PG-only)",
    )
    parser.add_argument("--limit", type=int, default=0, help="Max properties")
    parser.add_argument("--dsn", help="PostgreSQL DSN override")
    parser.add_argument(
        "--source",
        choices=["all", "redfin", "zillow", "homeharvest"],
        default="all",
    )
    args = parser.parse_args()

    props = _query_properties_needing_market(dsn=args.dsn, limit=args.limit)
    logger.info(f"Found {len(props)} properties for market data")

    service = MarketDataService(dsn=args.dsn)
    src_list = [args.source] if args.source != "all" else None
    asyncio.run(service.run_batch(props, sources=src_list))
