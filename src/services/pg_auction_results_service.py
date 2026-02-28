"""Phase B auction-result refresh service for the PG foreclosure hub.

This service fills the operational gap between:
1) upcoming auction inventory ingestion (`PgAuctionService`), and
2) post-sale analytics that depend on terminal outcomes.

`PgAuctionService` loads case/date/property context into `foreclosures`, but
it does not currently persist intraday auction outcomes such as:
- `auction_status` (e.g. "Auction Sold", "Canceled per County")
- `winning_bid`
- `sold_to`
- `buyer_type`

`PgAuctionResultsService` is designed to run on a schedule (for example hourly
from cron). It re-scrapes active, near-term auction dates from
`hillsborough.realforeclose.com`, parses the status ribbon data from each
auction card, and updates matching `foreclosures` rows by
`(case_number_raw, auction_date)`.

Design goals:
- Idempotent: repeated runs update the same rows safely.
- Incremental: targets only relevant dates (`auction_date <= CURRENT_DATE` and
  recent lookback window).
- Safe for cron: bounded date/page scan and explicit summary metrics.
"""

from __future__ import annotations

import asyncio
import re
from typing import TYPE_CHECKING, Any

from loguru import logger
from playwright.async_api import async_playwright
from sqlalchemy import text

from src.scrapers.auction_scraper import USER_AGENT_DESKTOP, apply_stealth
from sunbiz.db import get_engine, resolve_pg_dsn

if TYPE_CHECKING:
    from datetime import date

_MONEY_RE = re.compile(r"\$([0-9,]+(?:\.\d+)?)")
_SOLD_TO_RE = re.compile(r"Sold To\s+(.+?)\s+Auction Type", re.IGNORECASE | re.DOTALL)


def _parse_amount(value: str | None) -> float | None:
    if not value:
        return None
    match = _MONEY_RE.search(value)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", ""))
    except ValueError:
        return None


def _classify_buyer(sold_to: str | None) -> str | None:
    if not sold_to:
        return None
    normalized = sold_to.strip().lower()
    if not normalized:
        return None
    if "3rd party" in normalized or "third party" in normalized:
        return "Third Party"
    if "plaintiff" in normalized:
        return "Plaintiff"
    return "Third Party"


def _is_terminal_outcome(row: dict[str, Any]) -> bool:
    auction_status = str(row.get("auction_status") or "").strip().lower()
    if auction_status == "auction sold":
        return True
    if auction_status.startswith(("canceled", "cancelled")):
        return True
    return bool(row.get("winning_bid") is not None or row.get("sold_to"))


class PgAuctionResultsService:
    """Refresh sold/canceled outcomes for active foreclosure rows."""

    BASE_URL = "https://hillsborough.realforeclose.com"

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = resolve_pg_dsn(dsn)
        self.engine = get_engine(self.dsn)

    def run(
        self,
        *,
        date_limit: int | None = None,
        lookback_days: int = 3,
    ) -> dict[str, Any]:
        """Scrape auction outcomes and persist them into PG foreclosures."""
        target_dates = self._target_dates(
            date_limit=date_limit,
            lookback_days=max(0, int(lookback_days)),
        )
        if not target_dates:
            return {"skipped": True, "reason": "no_dates_need_result_refresh"}

        scraped_dates = 0
        outcomes_found = 0
        rows_updated = 0
        not_found_in_pg = 0
        failures: list[dict[str, Any]] = []

        for target_date in target_dates:
            try:
                outcomes = asyncio.run(self._scrape_date(target_date))
                scraped_dates += 1
            except Exception as exc:
                logger.error("Auction results scrape failed for {}: {}", target_date, exc)
                failures.append({"date": target_date.isoformat(), "error": str(exc)})
                continue

            outcomes_found += len(outcomes)
            upd_count, missing_count = self._save_outcomes(target_date, outcomes)
            rows_updated += upd_count
            not_found_in_pg += missing_count

        result: dict[str, Any] = {
            "dates_targeted": len(target_dates),
            "dates_scraped": scraped_dates,
            "outcomes_found": outcomes_found,
            "rows_updated": rows_updated,
            "not_found_in_pg": not_found_in_pg,
            "target_dates": [d.isoformat() for d in target_dates],
        }
        if failures:
            result["failures"] = failures
            if scraped_dates == 0:
                result["success"] = False
                result["error"] = "all_date_scrapes_failed"
        return result

    def _target_dates(
        self,
        *,
        date_limit: int | None,
        lookback_days: int,
    ) -> list[date]:
        limit = int(date_limit) if date_limit and date_limit > 0 else 30
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT DISTINCT auction_date::date
                    FROM foreclosures
                    WHERE archived_at IS NULL
                      AND auction_date <= CURRENT_DATE
                      AND auction_date >= (CURRENT_DATE - :lookback_days)
                      AND (
                            auction_status IS NULL
                         OR auction_date = CURRENT_DATE
                      )
                    ORDER BY auction_date
                    LIMIT :limit
                    """
                ),
                {"lookback_days": lookback_days, "limit": limit},
            ).fetchall()
        return [row[0] for row in rows if row[0] is not None]

    async def _scrape_date(self, target_date: date) -> list[dict[str, Any]]:
        date_str = target_date.strftime("%m/%d/%Y")
        url = (
            f"{self.BASE_URL}/index.cfm?zaction=AUCTION"
            f"&Zmethod=PREVIEW&AUCTIONDATE={date_str}"
        )
        outcomes: list[dict[str, Any]] = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=USER_AGENT_DESKTOP,
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
                timezone_id="America/New_York",
            )
            page = await context.new_page()
            await apply_stealth(page)

            try:
                logger.info("Auction results scrape: {} ({})", target_date, url)
                await page.goto(url, timeout=90000)
                await page.wait_for_load_state("networkidle")
                await page.wait_for_timeout(1200)

                content = await page.content()
                if "No auctions found" in content:
                    return []

                page_num = 1
                while True:
                    items = page.locator("div.AUCTION_ITEM")
                    count = await items.count()
                    if count <= 0:
                        break

                    for i in range(count):
                        parsed = await self._parse_item(items.nth(i), target_date)
                        if parsed:
                            outcomes.append(parsed)

                    if page_num >= 10:
                        break

                    next_btn = page.locator(".PageRight_W").first
                    if not (await next_btn.count() > 0 and await next_btn.is_visible()):
                        break
                    try:
                        await next_btn.click(timeout=5000)
                        await page.wait_for_load_state("networkidle")
                        await page.wait_for_timeout(1200)
                        page_num += 1
                    except Exception:
                        break
            finally:
                await browser.close()

        return outcomes

    async def _parse_item(
        self,
        item: Any,
        auction_date: date,
    ) -> dict[str, Any] | None:
        # Resolve case number first (primary key with auction_date for PG update).
        case_link = item.locator("table.ad_tab tr:has-text('Case #:') a")
        if await case_link.count() == 0:
            return None
        case_number = (await case_link.first.inner_text()).strip()
        if not case_number:
            return None

        labels = await self._texts(item, ".ASTAT_LBL")
        banners = await self._texts(item, ".ASTAT_MSGB")
        details = await self._texts(item, ".ASTAT_MSGD")

        detail_by_label: dict[str, str] = {}
        detail_idx = 0
        for label in labels:
            normalized = label.strip().lower()
            if normalized in {"auction sold", "auction status", "auction starts"}:
                continue
            if detail_idx >= len(details):
                break
            detail_by_label[label.strip()] = details[detail_idx]
            detail_idx += 1

        auction_status: str | None = None
        winning_bid: float | None = None
        sold_to: str | None = None
        buyer_type: str | None = None

        if any("auction sold" in lbl.lower() for lbl in labels):
            auction_status = "Auction Sold"
            winning_bid = _parse_amount(detail_by_label.get("Amount"))
            sold_to = detail_by_label.get("Sold To")
            if not sold_to:
                full_text = " ".join((await item.inner_text()).split())
                match = _SOLD_TO_RE.search(full_text)
                if match:
                    sold_to = match.group(1).strip()
            buyer_type = _classify_buyer(sold_to)
        else:
            canceled_banner = next(
                (
                    banner
                    for banner in banners
                    if "canceled" in banner.lower() or "cancelled" in banner.lower()
                ),
                None,
            )
            if canceled_banner:
                auction_status = canceled_banner.strip()
            elif labels and banners and any("auction status" in lbl.lower() for lbl in labels):
                auction_status = banners[0].strip()

        if (
            auction_status is None
            and winning_bid is None
            and not sold_to
            and buyer_type is None
        ):
            return None

        return {
            "case_number_raw": case_number,
            "auction_date": auction_date,
            "auction_status": auction_status,
            "winning_bid": winning_bid,
            "sold_to": sold_to,
            "buyer_type": buyer_type,
        }

    @staticmethod
    async def _texts(item: Any, selector: str) -> list[str]:
        loc = item.locator(selector)
        count = await loc.count()
        texts: list[str] = []
        for i in range(count):
            value = (await loc.nth(i).inner_text()).strip()
            if value:
                texts.append(value)
        return texts

    def _save_outcomes(
        self,
        target_date: date,
        outcomes: list[dict[str, Any]],
    ) -> tuple[int, int]:
        if not outcomes:
            return 0, 0

        updated = 0
        missing = 0
        with self.engine.begin() as conn:
            for row in outcomes:
                result = conn.execute(
                    text(
                        """
                        UPDATE foreclosures
                        SET auction_status = COALESCE(:auction_status, auction_status),
                            winning_bid = COALESCE(:winning_bid, winning_bid),
                            sold_to = COALESCE(NULLIF(:sold_to, ''), sold_to),
                            buyer_type = COALESCE(NULLIF(:buyer_type, ''), buyer_type),
                            archived_at = CASE
                                WHEN :archive_now THEN COALESCE(archived_at, now())
                                ELSE archived_at
                            END
                        WHERE case_number_raw = :case_number_raw
                          AND auction_date = :auction_date
                        """
                    ),
                    {
                        "auction_status": row.get("auction_status"),
                        "winning_bid": row.get("winning_bid"),
                        "sold_to": row.get("sold_to"),
                        "buyer_type": row.get("buyer_type"),
                        "archive_now": _is_terminal_outcome(row),
                        "case_number_raw": row["case_number_raw"],
                        "auction_date": target_date,
                    },
                )
                if result.rowcount and result.rowcount > 0:
                    updated += result.rowcount
                else:
                    missing += 1

        return updated, missing
