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
    ZILLOW_BLOCK_LIMIT = 3  # consecutive CAPTCHAs → stop Zillow
    REDFIN_FAIL_LIMIT = 5  # consecutive Tier 2 failures → stop

    def __init__(self, dsn: str | None = None, use_windows_chrome: bool = False):
        self._dsn = resolve_pg_dsn(dsn)
        self._engine = get_engine(self._dsn)
        self._use_windows_chrome = use_windows_chrome
        self._ensure_pg_table()
        self._has_realtor_column = self._column_exists("property_market", "realtor_json")
        if not self._has_realtor_column:
            logger.warning(
                "property_market.realtor_json column missing; Realtor source disabled. "
                "Apply add_realtor_cols migration to enable Realtor persistence."
            )

    def _column_exists(self, table_name: str, column_name: str) -> bool:
        """Return True when a column exists on the current schema table."""
        try:
            with self._engine.connect() as conn:
                exists = conn.execute(
                    text(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.columns
                            WHERE table_schema = current_schema()
                              AND table_name = :table_name
                              AND column_name = :column_name
                        )
                        """
                    ),
                    {"table_name": table_name, "column_name": column_name},
                ).scalar()
                return bool(exists)
        except Exception as exc:
            logger.warning(f"Failed to check schema column {table_name}.{column_name}: {exc}")
            return False

    def _ensure_pg_table(self) -> None:
        """Create the property_market table if it doesn't exist."""
        try:
            Base.metadata.create_all(
                bind=self._engine,
                tables=[
                    Base.metadata.tables["property_market"],
                ],
            )
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
        sources = list(sources or ["redfin", "zillow", "realtor", "homeharvest"])
        if not self._has_realtor_column and "realtor" in sources:
            logger.warning("Ignoring Realtor source because property_market.realtor_json is not available.")
            sources = [s for s in sources if s != "realtor"]
        summary = {"redfin": 0, "zillow": 0, "realtor": 0, "homeharvest": 0, "photos": 0}
        browser_phase_error: str | None = None

        # Check existing PG state per-property to decide which sources to run.
        # A property is "done" only when all configured sources have been attempted.
        need_redfin: list[dict] = []
        need_zillow: list[dict] = []
        need_realtor: list[dict] = []
        need_hh: list[dict] = []
        already_complete = 0
        selected_redfin = "redfin" in sources
        selected_zillow = "zillow" in sources
        selected_realtor = "realtor" in sources and self._has_realtor_column
        selected_hh = "homeharvest" in sources

        for prop in properties:
            strap = prop.get("strap", "")
            addr = (prop.get("property_address") or "").strip()
            if not addr or addr.lower() in ("unknown", "n/a", "none", ""):
                continue
            if not strap:
                continue

            state = self._get_market_state(strap)
            has_realtor = state["has_realtor"] if state else False
            complete_for_selected_sources = True
            if selected_redfin:
                complete_for_selected_sources = complete_for_selected_sources and bool(state and state["has_redfin"])
            if selected_zillow:
                complete_for_selected_sources = complete_for_selected_sources and bool(state and state["has_zillow"])
            if selected_realtor:
                complete_for_selected_sources = complete_for_selected_sources and bool(state and has_realtor)
            if selected_hh:
                complete_for_selected_sources = complete_for_selected_sources and bool(state and state["has_hh"])

            if complete_for_selected_sources:
                already_complete += 1
                continue

            if selected_redfin and (not state or not state["has_redfin"]):
                need_redfin.append(prop)
            if selected_zillow and (not state or not state["has_zillow"]):
                need_zillow.append(prop)
            if selected_realtor and (not state or not state["has_realtor"]):
                need_realtor.append(prop)
            if selected_hh and (not state or not state["has_hh"]):
                need_hh.append(prop)

        total_need = len({p["strap"] for p in need_redfin + need_zillow + need_realtor + need_hh})
        logger.info(
            f"MarketDataService: {total_need} properties need data "
            f"({already_complete} already complete across all sources), "
            f"redfin={len(need_redfin)}, zillow={len(need_zillow)}, realtor={len(need_realtor)}, hh={len(need_hh)}"
        )
        if not total_need:
            return summary

        # Track straps that got new data this run (for photo download scope)
        all_need = {p["strap"]: p for p in need_redfin + need_zillow + need_realtor + need_hh}
        need_market = list(all_need.values())

        # --- Browser-based sources (Redfin + Zillow + Realtor) ---
        run_redfin = "redfin" in sources and need_redfin
        run_zillow = "zillow" in sources and need_zillow
        run_realtor = "realtor" in sources and need_realtor
        redfin_matched: set[str] = set()

        if run_redfin or run_zillow or run_realtor:
            pw = None
            context = None
            try:
                pw, context, page, cdp = await self._launch_browser()

                # IMPORTANT: Open tabs upfront and wait for the user to solve any Captchas manually.
                logger.warning("\n=======================================================")
                logger.warning("Opening target tabs! You have 60 seconds to solve Captchas.")
                logger.warning("=======================================================\n")

                zillow_page = await context.new_page() if run_zillow else None
                redfin_page = await context.new_page() if run_redfin else None
                realtor_page = await context.new_page() if run_realtor else None

                if zillow_page:
                    await zillow_page.goto("https://www.zillow.com")
                if redfin_page:
                    await redfin_page.goto("https://www.redfin.com")
                if realtor_page:
                    await realtor_page.goto("https://www.realtor.com")
                # We must also get the CDP sessions for these specific new tabs
                zillow_cdp = await context.new_cdp_session(zillow_page) if run_zillow else cdp
                redfin_cdp = await context.new_cdp_session(redfin_page) if run_redfin else cdp
                realtor_cdp = await context.new_cdp_session(realtor_page) if run_realtor else cdp

                # Wait for captcha overlay if present on any of the loaded tabs
                for _cw in range(24):  # 120 seconds max
                    cf = False
                    if zillow_page:
                        try:
                            await zillow_page.wait_for_load_state("domcontentloaded", timeout=5000)
                            cf = cf or await zillow_page.query_selector('iframe#px-captcha-modal, iframe[id*="px-captcha"]')
                        except Exception:
                            # Page navigated (anti-bot redirect); wait for it to settle and retry
                            logger.warning("Zillow page navigated during captcha check — waiting for load")
                            try:
                                await zillow_page.wait_for_load_state("domcontentloaded", timeout=10000)
                            except Exception:
                                logger.warning("Zillow page failed to stabilize — may be blocked")
                            continue
                    # Optionally check redfin/realtor for their specific captchas here if known

                    if not cf:
                        break

                    if _cw == 0:
                        logger.warning("Captcha overlay detected! Waiting up to 120s for user to solve...")
                    await asyncio.sleep(5)

                # Keep the extra preview tabs open as requested by user

                if run_redfin:
                    redfin_page_ref = redfin_page or page
                    logger.info(f"Redfin: using tab {redfin_page_ref.url}")

                    redfin_matched = await self._run_redfin(
                        context,
                        redfin_page_ref,
                        redfin_cdp,
                        need_redfin,
                        set(),
                    )
                    summary["redfin"] = len(redfin_matched)
                    logger.info(f"Redfin done: {summary['redfin']} properties matched")

                if run_zillow:
                    already_have_zillow = set()
                    z_page = zillow_page or page
                    logger.info(f"Zillow: using tab {z_page.url}")

                    zillow_matched = await self._run_zillow(
                        z_page,
                        zillow_cdp,
                        need_zillow,
                        already_have_zillow,
                    )
                    summary["zillow"] = len(zillow_matched)
                    logger.info(f"Zillow done: {summary['zillow']} properties matched")

                if run_realtor:
                    already_have_realtor = set()
                    r_page = realtor_page or page
                    logger.info(f"Realtor: using tab {r_page.url}")

                    realtor_matched = await self._run_realtor(r_page, realtor_cdp, need_realtor, already_have_realtor)
                    summary["realtor"] = len(realtor_matched)
                    logger.info(f"Realtor done: {summary['realtor']} properties matched")

            except Exception as exc:
                browser_phase_error = str(exc)
                logger.exception("MarketDataService browser phase failed")
            finally:
                if context:
                    with contextlib.suppress(Exception):
                        if self._use_windows_chrome:
                            # CDP contexts belong to the user's running browser instance; don't close them!
                            pass
                        else:
                            await context.close()
                if pw:
                    with contextlib.suppress(Exception):
                        await pw.stop()

        # --- HomeHarvest (no browser) ---
        if "homeharvest" in sources and need_hh:
            hh_count = await self._run_homeharvest(need_hh)
            summary["homeharvest"] = hh_count
            logger.info(f"HomeHarvest done: {hh_count} properties matched")

        # --- Photo download ---
        try:
            photo_count = self._download_all_photos(need_market)
            summary["photos"] = photo_count
        except Exception as exc:
            logger.error(f"Photo download failed: {exc}")

        if browser_phase_error:
            summary["error"] = f"browser_phase_failed:{browser_phase_error}"

        logger.success(f"MarketDataService complete: {summary}")
        return summary

    # ------------------------------------------------------------------
    # PG helpers
    # ------------------------------------------------------------------

    def _get_market_state(self, strap: str) -> dict | None:
        """Check what market data exists in PG for this strap.

        Returns None if no row, or dict with source flags.
        """
        try:
            with self._engine.connect() as conn:
                if self._has_realtor_column:
                    row = conn.execute(
                        text(
                            "SELECT redfin_json IS NOT NULL AND redfin_json::text != 'null' AS has_redfin, "
                            "       zillow_json IS NOT NULL AND zillow_json::text != 'null' AS has_zillow, "
                            "       realtor_json IS NOT NULL AND realtor_json::text != 'null' AS has_realtor, "
                            "       homeharvest_json IS NOT NULL AND homeharvest_json::text != 'null' AS has_hh "
                            "FROM property_market WHERE strap = :strap"
                        ),
                        {"strap": strap},
                    ).fetchone()
                else:
                    row = conn.execute(
                        text(
                            "SELECT redfin_json IS NOT NULL AND redfin_json::text != 'null' AS has_redfin, "
                            "       zillow_json IS NOT NULL AND zillow_json::text != 'null' AS has_zillow, "
                            "       homeharvest_json IS NOT NULL AND homeharvest_json::text != 'null' AS has_hh "
                            "FROM property_market WHERE strap = :strap"
                        ),
                        {"strap": strap},
                    ).fetchone()
                if not row:
                    return None
                return {
                    "has_redfin": row.has_redfin,
                    "has_zillow": row.has_zillow,
                    "has_hh": row.has_hh,
                    "has_realtor": row.has_realtor if self._has_realtor_column else False,
                }
        except Exception as exc:
            msg = str(exc).lower()
            if self._has_realtor_column and "realtor_json" in msg and "column" in msg:
                logger.warning("property_market.realtor_json is missing at runtime; disabling Realtor source.")
                self._has_realtor_column = False
                return self._get_market_state(strap)
            logger.debug(f"Market state check failed for strap={strap}: {exc}")
            return None

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
            "photo_cdn_urls": _filter_photos(payload.get("photos") or []),
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
                "photo_cdn_urls": text(_photo_cdn_urls_upsert_sql()),
                "photo_local_paths": text(_photo_local_paths_reset_sql()),
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
            "photo_cdn_urls": _filter_photos(payload.get("photos") or []),
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
                # Update photo CDN URLs: keep whichever array has more photos
                "photo_cdn_urls": text(_photo_cdn_urls_upsert_sql()),
                "photo_local_paths": text(_photo_local_paths_reset_sql()),
                "zillow_json": stmt.excluded.zillow_json,
                "primary_source": text(
                    "CASE WHEN property_market.primary_source IS NULL THEN 'zillow' ELSE property_market.primary_source END"
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
                # Update photo CDN URLs: keep whichever array has more photos
                "photo_cdn_urls": text(_photo_cdn_urls_upsert_sql()),
                "photo_local_paths": text(_photo_local_paths_reset_sql()),
                "homeharvest_json": stmt.excluded.homeharvest_json,
                "primary_source": text(
                    "CASE WHEN property_market.primary_source IS NULL THEN 'homeharvest' ELSE property_market.primary_source END"
                ),
                "updated_at": text("NOW()"),
            },
        )
        try:
            with self._engine.begin() as conn:
                conn.execute(stmt)
        except Exception as e:
            logger.error(f"PG HomeHarvest upsert failed for {strap}: {e}")

    def _upsert_realtor(self, strap: str, folio: str | None, case_number: str, payload: dict) -> None:
        """Upsert Realtor.com data into PG property_market."""
        if not self._has_realtor_column:
            return
        row = {
            "strap": strap,
            "folio": folio,
            "case_number": case_number,
            "zestimate": _f(payload.get("zestimate")),  # Realtor "estimate" maps to zestimate column
            "rent_zestimate": _f(payload.get("rent_estimate")),
            "list_price": _f(payload.get("list_price")),
            "listing_status": payload.get("listing_status"),
            "beds": _i(payload.get("beds")),
            "baths": _f(payload.get("baths")),
            "sqft": _i(payload.get("sqft")),
            "year_built": _i(payload.get("year_built")),
            "lot_size": str(payload.get("lot_size") or "") or None,
            "property_type": payload.get("property_type"),
            "detail_url": payload.get("detail_url"),
            "photo_cdn_urls": _filter_photos(payload.get("photos") or []),
            "realtor_json": payload,
            # We treat realtor as backup if zillow/redfin are present
            "primary_source": "realtor",
        }

        stmt = pg_insert(PropertyMarket).values([row])
        stmt = stmt.on_conflict_do_update(
            index_elements=["strap"],
            set_={
                "folio": text("COALESCE(EXCLUDED.folio, property_market.folio)"),
                "case_number": text("COALESCE(EXCLUDED.case_number, property_market.case_number)"),
                "zestimate": text("COALESCE(property_market.zestimate, EXCLUDED.zestimate)"),
                "rent_zestimate": text("COALESCE(property_market.rent_zestimate, EXCLUDED.rent_zestimate)"),
                "list_price": text("COALESCE(property_market.list_price, EXCLUDED.list_price)"),
                "listing_status": text("COALESCE(property_market.listing_status, EXCLUDED.listing_status)"),
                # Realtor is backup for specs — preserve existing Zillow/Redfin/HomeHarvest values
                "beds": text("COALESCE(property_market.beds, EXCLUDED.beds)"),
                "baths": text("COALESCE(property_market.baths, EXCLUDED.baths)"),
                "sqft": text("COALESCE(property_market.sqft, EXCLUDED.sqft)"),
                "year_built": text("COALESCE(property_market.year_built, EXCLUDED.year_built)"),
                "lot_size": text("COALESCE(property_market.lot_size, EXCLUDED.lot_size)"),
                "property_type": text("COALESCE(property_market.property_type, EXCLUDED.property_type)"),
                "detail_url": text("COALESCE(property_market.detail_url, EXCLUDED.detail_url)"),
                "photo_cdn_urls": text(_photo_cdn_urls_upsert_sql()),
                "photo_local_paths": text(_photo_local_paths_reset_sql()),
                "realtor_json": stmt.excluded.realtor_json,
                "primary_source": text(
                    "CASE WHEN property_market.primary_source IS NULL THEN 'realtor' ELSE property_market.primary_source END"
                ),
                "updated_at": text("NOW()"),
            },
        )
        try:
            with self._engine.begin() as conn:
                conn.execute(stmt)
        except Exception as e:
            logger.error(f"PG Realtor upsert failed for {strap}: {e}")

    def _mark_source_attempted(
        self,
        strap: str,
        folio: str | None,
        case_number: str,
        source: str,
    ) -> None:
        """Save a tombstone so we don't re-scrape a source that found nothing."""
        col = {
            "redfin": "redfin_json",
            "zillow": "zillow_json",
            "homeharvest": "homeharvest_json",
            "realtor": "realtor_json",
        }.get(source)
        if source == "realtor" and not self._has_realtor_column:
            return
        if not col:
            return
        row = {"strap": strap, "folio": folio, "case_number": case_number, col: {"_attempted": True, "_found": False}}
        stmt = pg_insert(PropertyMarket).values([row])
        stmt = stmt.on_conflict_do_update(
            index_elements=["strap"],
            set_={
                col: text(f"COALESCE(property_market.{col}, EXCLUDED.{col})"),
                "updated_at": text("NOW()"),
            },
        )
        try:
            with self._engine.begin() as conn:
                conn.execute(stmt)
        except Exception as e:
            logger.debug(f"PG tombstone upsert failed for {strap}/{source}: {e}")

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
            "image/jpeg",
            "image/png",
            "image/webp",
            "image/gif",
        })
        EXT_MAP = {
            "image/jpeg": ".jpg",
            "image/png": ".png",
            "image/webp": ".webp",
            "image/gif": ".gif",
        }

        session = requests.Session()
        session.headers["User-Agent"] = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
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
                    logger.warning(
                        "Photo download failed for strap={} case={} idx={} url={}: {}",
                        strap,
                        case_number,
                        idx,
                        url,
                        dl_err,
                    )
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
        import os

        pw = await async_playwright().start()

        if self._use_windows_chrome:
            # Connect to an existing Windows Chrome instance over CDP
            host = os.getenv("WSL_HOST_IP", "127.0.0.1")
            port = os.getenv("CHROME_CDP_PORT", "9222")
            endpoint = f"http://{host}:{port}"

            logger.info(f"MarketDataService: Connecting to Windows Chrome CDP at {endpoint}")
            try:
                browser = await pw.chromium.connect_over_cdp(endpoint)
                context = browser.contexts[0] if browser.contexts else await browser.new_context()
                page = context.pages[0] if context.pages else await context.new_page()
                await Stealth().apply_stealth_async(page)
                cdp = await context.new_cdp_session(page)
                return pw, context, page, cdp
            except Exception as e:
                logger.error(f"Failed to connect to Windows Chrome CDP at {endpoint}: {e}")
                logger.error(
                    "Please ensure you launched Windows Chrome with --remote-debugging-port=9222 and --remote-allow-origins=*"
                )
                # Fall back to launching local headless if desired, or raise
                raise

        # Original isolated launch for WSL
        PROFILE_DIR.mkdir(parents=True, exist_ok=True)
        context = await pw.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            channel="chrome",
            headless=False,
            devtools=True,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            timezone_id="America/New_York",
            args=["--disable-blink-features=AutomationControlled"],
        )
        logger.info("MarketDataService: Chrome launched with user_chrome profile")

        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        page = context.pages[0] if context.pages else await context.new_page()
        await Stealth().apply_stealth_async(page)
        cdp = await context.new_cdp_session(page)
        return pw, context, page, cdp

    # ------------------------------------------------------------------
    # Redfin
    # ------------------------------------------------------------------

    async def _run_redfin(
        self,
        context,
        page,
        cdp,
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
                try:
                    nav_status, listing = await scraper.scrape_detail_page(detail_url)
                except Exception as detail_exc:
                    logger.warning(f"Redfin Tier 1: scrape_detail_page raised {type(detail_exc).__name__}: {detail_exc}")
                    nav_status, listing = "blocked", None
                if nav_status == "ok" and listing:
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
                if row.get("strap", "") not in matched and row.get("strap", "") not in already_matched
            ]
            logger.info(f"Redfin Tier 2: {len(unmatched)} unmatched for direct URL")

            consecutive_failures = 0
            for row in unmatched:
                if consecutive_failures >= self.REDFIN_FAIL_LIMIT:
                    logger.warning(f"Redfin Tier 2: {self.REDFIN_FAIL_LIMIT} consecutive failures — stopping")
                    break

                addr = (row.get("property_address") or "").strip()
                strap = row.get("strap", "")
                folio = row.get("folio")
                case = row.get("case_number", "")

                logger.info(f"Redfin Tier 2: typing '{addr}' via CDP")

                try:
                    nav_status, listing = await scraper.search_property(cdp, addr)
                except Exception as search_exc:
                    logger.warning(f"Redfin Tier 2: search_property raised {type(search_exc).__name__}: {search_exc}")
                    nav_status, listing = "blocked", None

                if nav_status == "ok" and listing and (listing.list_price or listing.redfin_estimate):
                    payload = RedfinScraper.listing_to_market_payload(listing)
                    self._upsert_redfin(strap, folio, case, payload)
                    matched.add(strap)
                    consecutive_failures = 0
                    logger.success(f"Redfin Tier 2: saved for {strap}")
                elif nav_status == "not_found":
                    # Property doesn't exist in Redfin — not a block
                    self._mark_source_attempted(strap, folio, case, "redfin")
                    consecutive_failures = 0
                    logger.info(f"Redfin Tier 2: not_found for '{addr}'")
                else:
                    consecutive_failures += 1
                    self._mark_source_attempted(strap, folio, case, "redfin")
                    has_price = bool(listing and (listing.list_price or listing.redfin_estimate)) if listing else False
                    logger.warning(
                        f"Redfin Tier 2: fail #{consecutive_failures} — "
                        f"status={nav_status}, has_listing={listing is not None}, has_price={has_price} for '{addr}'"
                    )

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
                logger.warning(f"Zillow: {self.ZILLOW_BLOCK_LIMIT} consecutive blocks — stopping")
                break

            strap = prop.get("strap", "")
            folio = prop.get("folio")
            case = prop.get("case_number", "")
            addr = (prop.get("property_address") or "").strip()

            if strap in already_matched or strap in matched:
                continue

            logger.info(f"Zillow [{i + 1}/{len(properties)}]: searching '{addr}'")

            try:
                listing = await search_property(page, cdp, addr)
            except Exception as search_exc:
                # Captcha iframe or DOM detach — treat as a block, don't crash
                err_str = str(search_exc)
                if "Execution context was destroyed" in err_str:
                    logger.warning(
                        "Zillow Blocked: 'Execution context was destroyed'. "
                        "This usually happens when Zillow hits the bot with a Captcha overlay "
                        "or maliciously redirects the page to block automated interaction."
                    )
                else:
                    logger.warning(f"Zillow: search_property raised {type(search_exc).__name__}: {search_exc}")
                listing = None

            if listing is None:
                # None = blocked
                consecutive_blocks += 1
                logger.warning(f"Zillow: block #{consecutive_blocks} on '{addr}'")
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
                logger.success(f"Zillow: saved for {strap} (zest={listing.zestimate}, photos={len(listing.photos)})")
            else:
                self._mark_source_attempted(strap, folio, case, "zillow")
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
    # Realtor
    # ------------------------------------------------------------------

    async def _run_realtor(
        self,
        page,
        cdp,
        properties: list[dict],
        already_matched: set[str],
    ) -> set[str]:
        """Run Realtor.com searches via CDP."""
        from src.scrapers.realtor_scraper import (
            search_property,
            listing_to_market_payload,
            _is_blocked,
        )

        matched: set[str] = set()
        consecutive_blocks = 0

        if await _is_blocked(page):
            logger.warning("Realtor: blocked on homepage — skipping all searches")
            return matched

        for i, prop in enumerate(properties):
            if consecutive_blocks >= self.ZILLOW_BLOCK_LIMIT:  # Reusing block limit parameter
                logger.warning(f"Realtor: {self.ZILLOW_BLOCK_LIMIT} consecutive blocks — stopping")
                break

            strap = prop.get("strap", "")
            folio = prop.get("folio")
            case = prop.get("case_number", "")
            addr = (prop.get("property_address") or "").strip()

            if strap in already_matched or strap in matched:
                continue

            logger.info(f"Realtor [{i + 1}/{len(properties)}]: searching '{addr}'")

            try:
                listing = await search_property(page, cdp, addr)
            except Exception as search_exc:
                logger.warning(f"Realtor: search_property raised {type(search_exc).__name__}: {search_exc}")
                listing = None

            if listing is None:
                consecutive_blocks += 1
                logger.warning(f"Realtor: block #{consecutive_blocks} on '{addr}'")
                await page.wait_for_timeout(5000)
                try:
                    await page.goto(
                        "https://www.realtor.com",
                        wait_until="domcontentloaded",
                        timeout=30000,
                    )
                    await page.wait_for_timeout(3000)
                except Exception as nav_err:
                    logger.debug(f"Realtor: homepage recovery failed: {nav_err}")
                continue

            consecutive_blocks = 0

            if listing.estimate or listing.price or listing.beds or listing.photos:
                payload = listing_to_market_payload(listing)
                self._upsert_realtor(strap, folio, case, payload)
                matched.add(strap)
                logger.success(f"Realtor: saved for {strap} (est={listing.estimate}, photos={len(listing.photos)})")
            else:
                self._mark_source_attempted(strap, folio, case, "realtor")
                logger.debug(f"Realtor: no useful data for '{addr}'")

            delay = random.uniform(*self.ZILLOW_SEARCH_DELAY)  # noqa: S311
            await page.wait_for_timeout(int(delay * 1000))

            try:
                await page.goto(
                    "https://www.realtor.com",
                    wait_until="domcontentloaded",
                    timeout=30000,
                )
                await page.wait_for_timeout(2000)
            except Exception as nav_err:
                logger.debug(f"Realtor: homepage navigation failed: {nav_err}")

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
                    self._mark_source_attempted(strap, folio, case, "homeharvest")
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


def _sql_placeholder_photo_condition(url_expr: str) -> str:
    """Return SQL that treats logos/placeholders as invalid property photos."""
    return f"""
        LOWER(COALESCE({url_expr}, '')) LIKE '%redfin-logo%'
        OR LOWER(COALESCE({url_expr}, '')) LIKE '%/logos/%'
        OR LOWER(COALESCE({url_expr}, '')) LIKE '%no_image%'
        OR LOWER(COALESCE({url_expr}, '')) LIKE '%placeholder%'
        OR LOWER(COALESCE({url_expr}, '')) LIKE '%default_photo%'
        OR LOWER(COALESCE({url_expr}, '')) LIKE '%/images/logos/%'
        OR LOWER(COALESCE({url_expr}, '')) LIKE '%/static/images/%'
    """


def _sql_first_photo_placeholder_condition(jsonb_expr: str) -> str:
    """Return SQL that checks whether the first JSONB photo entry is invalid."""
    return _sql_placeholder_photo_condition(f"{jsonb_expr}->>0")


def _photo_cdn_urls_upsert_sql() -> str:
    """Prefer longer valid arrays, but allow placeholder-only rows to heal."""
    existing_placeholder = _sql_first_photo_placeholder_condition(
        "property_market.photo_cdn_urls"
    )
    return f"""
        CASE
            WHEN property_market.photo_cdn_urls IS NULL THEN EXCLUDED.photo_cdn_urls
            WHEN jsonb_typeof(property_market.photo_cdn_urls) != 'array' THEN EXCLUDED.photo_cdn_urls
            WHEN EXCLUDED.photo_cdn_urls IS NULL THEN property_market.photo_cdn_urls
            WHEN jsonb_typeof(EXCLUDED.photo_cdn_urls) != 'array' THEN property_market.photo_cdn_urls
            WHEN ({existing_placeholder}) THEN EXCLUDED.photo_cdn_urls
            WHEN jsonb_array_length(EXCLUDED.photo_cdn_urls) > jsonb_array_length(property_market.photo_cdn_urls) THEN EXCLUDED.photo_cdn_urls
            ELSE property_market.photo_cdn_urls
        END
    """


def _photo_local_paths_reset_sql() -> str:
    """Clear cached local photos when replacing a placeholder-only CDN array."""
    existing_placeholder = _sql_first_photo_placeholder_condition(
        "property_market.photo_cdn_urls"
    )
    return f"""
        CASE
            WHEN ({existing_placeholder})
              AND EXCLUDED.photo_cdn_urls IS NOT NULL
              AND jsonb_typeof(EXCLUDED.photo_cdn_urls) = 'array'
              AND EXCLUDED.photo_cdn_urls IS DISTINCT FROM property_market.photo_cdn_urls
            THEN NULL
            ELSE property_market.photo_local_paths
        END
    """


def _is_placeholder_photo(url: str) -> bool:
    """Return True if *url* is a site logo, placeholder, or generic fallback image.

    Redfin returns its square logo (``redfin-logo-square-red-1200.png``) for
    properties that have no listing photos.  Zillow and Realtor have similar
    fallback images.  These must not be stored as property photos because they
    cause many unrelated properties to display the same picture on the
    dashboard.
    """
    if not url:
        return True
    lower = url.lower()
    # Redfin logo / generic branding images
    if "redfin-logo" in lower or "/logos/" in lower:
        return True
    # Zillow/Trulia/Realtor generic placeholders
    if "no_image" in lower or "placeholder" in lower or "default_photo" in lower:
        return True
    # Generic static asset paths (not a property photo)
    return "/images/logos/" in lower or "/static/images/" in lower


def _filter_photos(photos: list[str]) -> list[str]:
    """Remove placeholder/logo URLs from a photo list."""
    return [u for u in photos if not _is_placeholder_photo(u)]


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
    return _filter_photos(list(dict.fromkeys(urls)))  # dedupe, filter, preserve order


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
    first_photo_placeholder = _sql_first_photo_placeholder_condition("pm.photo_cdn_urls")
    query = f"""
        SELECT f.strap, f.folio, f.case_number_raw AS case_number, f.property_address
        FROM foreclosures f
        LEFT JOIN property_market pm ON f.strap = pm.strap
        WHERE f.strap IS NOT NULL
          AND f.property_address IS NOT NULL
          AND (
              pm.strap IS NULL 
              OR pm.zestimate IS NULL
              OR pm.zillow_json IS NULL OR pm.zillow_json::text = 'null'
              OR pm.redfin_json IS NULL OR pm.redfin_json::text = 'null'
              OR (
                  pm.photo_cdn_urls IS NOT NULL
                  AND jsonb_typeof(pm.photo_cdn_urls) = 'array'
                  AND (
                      jsonb_array_length(pm.photo_cdn_urls) = 0
                      OR ({first_photo_placeholder})
                      OR pm.photo_local_paths IS NULL
                      OR jsonb_typeof(pm.photo_local_paths) != 'array'
                      OR jsonb_array_length(pm.photo_local_paths) = 0
                      OR (
                          jsonb_array_length(pm.photo_local_paths) < 15
                          AND jsonb_array_length(pm.photo_local_paths) < jsonb_array_length(pm.photo_cdn_urls)
                      )
                  )
              )
          )
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
        choices=["all", "redfin", "zillow", "realtor", "homeharvest"],
        default="all",
    )
    args = parser.parse_args()

    props = _query_properties_needing_market(dsn=args.dsn, limit=args.limit)
    logger.info(f"Found {len(props)} properties for market data")

    service = MarketDataService(dsn=args.dsn)
    src_list = [args.source] if args.source != "all" else None
    asyncio.run(service.run_batch(props, sources=src_list))
