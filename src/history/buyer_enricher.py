import asyncio
from datetime import datetime, date
from pathlib import Path
from loguru import logger
import duckdb
from src.models.property import Property
from src.history.hcpa_history_scraper import HistoricalHCPAScraper
from src.utils.time import ensure_duckdb_utc
from src.utils.logging_config import configure_logger

configure_logger(log_file="history_pipeline.log")

DB_PATH = Path("data/history.db")

PLACEHOLDER_BUYERS = {
    "3rd party bidder",
    "third party bidder",
    "3rd party",
    "third party",
    "unknown",
    "",
}


class BuyerNameEnricher:
    def __init__(
        self,
        db_path: Path = DB_PATH,
        headless: bool = True,
        max_concurrent: int = 3,
        max_days_after: int = 365,
    ):
        self.db_path = db_path
        self.scraper = HistoricalHCPAScraper(headless=headless)
        self.max_concurrent = max_concurrent
        self.max_days_after = max_days_after

    def get_targets(self, limit: int = 25):
        conn = duckdb.connect(str(self.db_path), read_only=True)
        ensure_duckdb_utc(conn)
        try:
            query = """
                SELECT auction_id, auction_date, case_number, parcel_id, property_address, sold_to, buyer_type
                FROM auctions
                WHERE parcel_id IS NOT NULL
                  AND parcel_id != ''
                  AND buyer_type = 'Third Party'
                  AND (sold_to IS NULL OR lower(sold_to) IN ({placeholders}))
                  AND (status IS NULL OR lower(status) NOT LIKE '%walked away%')
                ORDER BY auction_date ASC
                LIMIT ?
            """.format(
                placeholders=",".join([f"'{p}'" for p in PLACEHOLDER_BUYERS])
            )
            return conn.execute(query, [limit]).fetchall()
        finally:
            conn.close()

    async def _process_single(self, row, semaphore: asyncio.Semaphore):
        async with semaphore:
            (
                auction_id,
                auction_date_val,
                case_number,
                parcel_id,
                address,
                sold_to,
                buyer_type,
            ) = row

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
        conn = duckdb.connect(str(self.db_path))
        try:
            for upd in updates:
                conn.execute(
                    """
                    UPDATE auctions
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
        finally:
            conn.close()

    async def enrich_batch(self, batch_size: int = 25):
        targets = self.get_targets(batch_size)
        if not targets:
            logger.info("No third-party buyer placeholders to enrich.")
            return

        logger.info(f"Enriching buyer names for {len(targets)} auctions via HCPA sales history...")
        sem = asyncio.Semaphore(self.max_concurrent)
        tasks = [self._process_single(row, sem) for row in targets]
        results = await asyncio.gather(*tasks)
        updates = [r for r in results if r]
        self._update_buyers(updates)
        logger.info(f"Updated {len(updates)} buyer names from HCPA.")


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
