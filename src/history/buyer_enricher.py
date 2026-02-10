import asyncio
import sqlite3
from datetime import datetime, date
from pathlib import Path
from loguru import logger
from src.models.property import Property
from src.history.hcpa_history_scraper import HistoricalHCPAScraper
from src.history.db_init import ensure_history_schema
from src.db.sqlite_paths import resolve_sqlite_db_path_str
from src.utils.logging_config import configure_logger

configure_logger(log_file="history_pipeline.log")

DB_PATH = resolve_sqlite_db_path_str()

PLACEHOLDER_BUYERS = {
    "3rd party bidder",
    "third party bidder",
    "3rd party",
    "third party",
    "unknown",
    "",
}
INVALID_PARCELS = {"property appraiser", "n/a", "none", "unknown"}


class BuyerNameEnricher:
    def __init__(
        self,
        db_path: str = DB_PATH,
        headless: bool = True,
        max_concurrent: int = 3,
        max_days_after: int = 365,
    ):
        self.db_path = db_path
        ensure_history_schema(self.db_path)
        self.scraper = HistoricalHCPAScraper(headless=headless)
        self.max_concurrent = max_concurrent
        self.max_days_after = max_days_after

    def get_targets(self, limit: int = 25):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            query = """
                SELECT auction_id, auction_date, case_number, parcel_id, property_address, sold_to, buyer_type
                FROM history_auctions
                WHERE parcel_id IS NOT NULL
                  AND parcel_id != ''
                  AND lower(parcel_id) NOT IN ({invalids})
                  AND buyer_type = 'Third Party'
                  AND (sold_to IS NULL OR lower(sold_to) IN ({placeholders}))
                  AND (status IS NULL OR lower(status) NOT LIKE '%walked away%')
                ORDER BY auction_date ASC
                LIMIT ?
            """.format(
                placeholders=",".join([f"'{p}'" for p in PLACEHOLDER_BUYERS]),
                invalids=",".join([f"'{p}'" for p in INVALID_PARCELS]),
            )
            return conn.execute(query, [limit]).fetchall()
        finally:
            conn.close()

    async def _process_single(self, row, semaphore: asyncio.Semaphore):
        async with semaphore:
            auction_id = row["auction_id"]
            auction_date_val = row["auction_date"]
            case_number = row["case_number"]
            parcel_id = row["parcel_id"]
            address = row["property_address"]
            sold_to = row["sold_to"]
            buyer_type = row["buyer_type"]

            if isinstance(auction_date_val, str):
                auction_date = datetime.strptime(auction_date_val, "%Y-%m-%d").date()
            else:
                auction_date = auction_date_val

            prop = Property(case_number=case_number, parcel_id=parcel_id, address=address)
            prop = await self.scraper.enrich_property(prop)

            buyer_name, sale_date, sale_price = self._pick_auction_sale(prop.sales_history, auction_date)
            if not buyer_name:
                return None

            buyer_type = self._classify_buyer(buyer_name) or buyer_type
            return {
                "auction_id": auction_id,
                "sold_to": buyer_name,
                "buyer_type": buyer_type,
                "sale_date": sale_date,
                "sale_price": sale_price,
            }

    def _parse_date(self, value: str) -> date | None:
        if not value:
            return None
        value = value.strip()
        for fmt in ("%m/%d/%Y", "%m/%Y", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(value, fmt)
                if fmt == "%m/%Y":
                    return date(parsed.year, parsed.month, 1)
                return parsed.date()
            except ValueError:
                continue
        return None

    def _parse_price(self, value: str | None) -> float | None:
        if not value:
            return None
        try:
            return float(value.replace("$", "").replace(",", "").strip())
        except ValueError:
            return None

    def _pick_auction_sale(self, sales: list[dict], auction_date: date) -> tuple[str | None, date | None, float | None]:
        if not sales:
            return None, None, None

        candidates = []
        for sale in sales:
            sale_date = self._parse_date(sale.get("date"))
            if not sale_date:
                continue
            if sale_date < auction_date:
                continue
            delta_days = (sale_date - auction_date).days
            if delta_days > self.max_days_after:
                continue
            grantee = sale.get("grantee") or sale.get("buyer") or ""
            grantee = grantee.strip()
            if not grantee:
                continue
            price = self._parse_price(sale.get("price") or sale.get("sale_price"))
            candidates.append((sale_date, grantee, price))

        if not candidates:
            return None, None, None

        candidates.sort(key=lambda x: x[0])
        sale_date, grantee, price = candidates[0]
        return grantee, sale_date, price

    def _classify_buyer(self, name: str) -> str | None:
        if not name:
            return None
        normalized = name.lower()
        if any(term in normalized for term in ("plaintiff", "bank", "mortgage", "financial", "lending", "loan")):
            return "Plaintiff"
        if "3rd party" in normalized or "third party" in normalized:
            return "Third Party"
        return "Third Party"

    def _update_buyers(self, updates: list[dict]):
        if not updates:
            return
        conn = sqlite3.connect(self.db_path)
        try:
            for upd in updates:
                conn.execute(
                    """
                    UPDATE history_auctions
                    SET sold_to = ?,
                        buyer_normalized = ?,
                        buyer_type = ?
                    WHERE auction_id = ?
                    """,
                    [
                        upd["sold_to"],
                        upd["sold_to"].upper().strip() if upd["sold_to"] else None,
                        upd["buyer_type"],
                        upd["auction_id"],
                    ],
                )
            conn.commit()
        finally:
            conn.close()

    async def enrich_batch(self, batch_size: int = 25):
        targets = self.get_targets(batch_size)
        if not targets:
            logger.info("No third-party buyer placeholders to enrich.")
            return 0

        logger.info(f"Enriching buyer names for {len(targets)} auctions via HCPA sales history...")
        sem = asyncio.Semaphore(self.max_concurrent)
        tasks = [self._process_single(row, sem) for row in targets]
        results = await asyncio.gather(*tasks)
        updates = [r for r in results if r]
        self._update_buyers(updates)
        logger.info(f"Updated {len(updates)} buyer names from HCPA.")
        return len(updates)

    async def enrich_all_pending(
        self,
        batch_size: int = 25,
        max_batches: int | None = None,
    ) -> int:
        """Process all pending buyer placeholders in batches."""
        total_updates = 0
        batches = 0
        while True:
            if max_batches is not None and batches >= max_batches:
                logger.info("Reached max buyer enrichment batches (%d).", max_batches)
                break
            updates = await self.enrich_batch(batch_size)
            if updates == 0:
                break
            total_updates += updates
            batches += 1
        return total_updates


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Enrich buyer names from HCPA sales history.")
    parser.add_argument("--batch", type=int, default=25, help="Number of auctions to enrich per run")
    parser.add_argument("--concurrency", type=int, default=3, help="Concurrent HCPA scrapes")
    parser.add_argument("--max-days", type=int, default=365, help="Max days after auction to consider a sale")
    parser.add_argument("--headed", action="store_true", help="Run browser in headed mode")
    args = parser.parse_args()

    enricher = BuyerNameEnricher(
        headless=not args.headed,
        max_concurrent=args.concurrency,
        max_days_after=args.max_days,
    )
    asyncio.run(enricher.enrich_batch(args.batch))
