"""Phase B Step 1: Scrape upcoming auctions → PG foreclosures.

Scrapes the Hillsborough County auction website for upcoming foreclosure
and tax-deed auctions, downloads Final Judgment PDFs, and writes the auction
data directly to the PG ``foreclosures`` table.  The PG trigger
``normalize_foreclosure()`` handles case-number normalization and
strap↔folio cross-fill automatically.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from datetime import date, timedelta
from typing import Any

from loguru import logger
from sqlalchemy import text

from sunbiz.db import get_engine, resolve_pg_dsn


def _scrape_window() -> tuple[date, date]:
    """Return (start, end) dates for upcoming auctions (today → 45 days out)."""
    today = dt.datetime.now(dt.UTC).date()
    return today, today + timedelta(days=45)


class PgAuctionService:
    """Scrape upcoming auctions and save directly to PG."""

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = resolve_pg_dsn(dsn)
        self.engine = get_engine(self.dsn)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, *, limit: int | None = None) -> dict[str, Any]:
        """Scrape upcoming auctions and save to PG.  Returns summary dict."""
        start, end = _scrape_window()

        # Find dates that already have auctions in PG
        existing_dates = self._dates_with_auctions(start, end)

        return asyncio.run(self._scrape_range(start, end, existing_dates, limit))

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _dates_with_auctions(self, start: date, end: date) -> set[date]:
        """Return set of auction_dates in PG foreclosures within range."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT DISTINCT auction_date::date
                    FROM foreclosures
                    WHERE auction_date BETWEEN :start AND :end
                """),
                {"start": start, "end": end},
            ).fetchall()
        return {row[0] for row in rows}

    async def _scrape_range(
        self,
        start: date,
        end: date,
        existing_dates: set[date],
        limit: int | None,
    ) -> dict[str, Any]:
        from src.scrapers.auction_scraper import AuctionScraper

        scraper = AuctionScraper(process_final_judgments=False)

        all_props: list[Any] = []
        dates_scraped = 0
        dates_skipped = 0
        current = start

        while current <= end:
            if current.weekday() >= 5:  # Skip weekends
                current += timedelta(days=1)
                continue

            if current in existing_dates:
                dates_skipped += 1
                current += timedelta(days=1)
                continue

            logger.info(f"Scraping auctions for {current}")
            try:
                props = await scraper.scrape_date(
                    current,
                    fast_fail=True,
                    max_properties=limit,
                )
                all_props.extend(props)
                dates_scraped += 1
                logger.info(f"Scraped {len(props)} auctions for {current}")
            except Exception as exc:
                logger.error(f"Scrape failed for {current}: {exc}")

            current += timedelta(days=1)

        saved = self._save_to_pg(all_props)

        return {
            "dates_scraped": dates_scraped,
            "dates_skipped": dates_skipped,
            "auctions_found": len(all_props),
            "auctions_saved": saved,
        }

    def _save_to_pg(self, properties: list[Any]) -> int:
        """UPSERT scraped Property objects into PG foreclosures."""
        if not properties:
            return 0

        saved = 0
        with self.engine.begin() as conn:
            for prop in properties:
                # Build partial judgment_data with plaintiff/defendant
                # (full extraction happens in Step B2)
                jdata: dict[str, Any] | None = None
                plaintiff = getattr(prop, "plaintiff", None)
                defendant = getattr(prop, "defendant", None)
                if plaintiff or defendant:
                    jdata = {}
                    if plaintiff:
                        jdata["plaintiff"] = plaintiff
                    if defendant:
                        jdata["defendant"] = defendant

                conn.execute(text("SAVEPOINT auction_row"))
                try:
                    conn.execute(
                        text("""
                            INSERT INTO foreclosures (
                                case_number_raw, auction_date, auction_type,
                                strap, property_address,
                                final_judgment_amount, appraised_value,
                                judgment_data
                            ) VALUES (
                                :case_number,
                                CAST(:auction_date AS DATE),
                                COALESCE(:auction_type, 'foreclosure'),
                                NULLIF(:strap, ''),
                                :address,
                                :fja, :av,
                                CAST(:jdata AS JSONB)
                            )
                            ON CONFLICT (case_number_raw, auction_date) DO UPDATE SET
                                property_address = COALESCE(
                                    EXCLUDED.property_address,
                                    foreclosures.property_address
                                ),
                                strap = COALESCE(EXCLUDED.strap, foreclosures.strap),
                                final_judgment_amount = COALESCE(
                                    EXCLUDED.final_judgment_amount,
                                    foreclosures.final_judgment_amount
                                ),
                                appraised_value = COALESCE(
                                    EXCLUDED.appraised_value,
                                    foreclosures.appraised_value
                                ),
                                judgment_data = COALESCE(
                                    foreclosures.judgment_data,
                                    EXCLUDED.judgment_data
                                )
                        """),
                        {
                            "case_number": prop.case_number,
                            "auction_date": str(prop.auction_date) if prop.auction_date else None,
                            "auction_type": (prop.auction_type or "foreclosure").lower(),
                            "strap": prop.parcel_id or "",
                            "address": prop.address,
                            "fja": prop.final_judgment_amount,
                            "av": prop.assessed_value,
                            "jdata": json.dumps(jdata) if jdata else None,
                        },
                    )
                    conn.execute(text("RELEASE SAVEPOINT auction_row"))
                    saved += 1
                except Exception as exc:
                    conn.execute(text("ROLLBACK TO SAVEPOINT auction_row"))
                    logger.warning(
                        f"Skip auction {prop.case_number}: {exc}"
                    )

        logger.info(f"Saved {saved}/{len(properties)} auctions to PG")
        return saved
