import asyncio
import calendar
import duckdb
import polars as pl
from datetime import datetime, date
from pathlib import Path
from loguru import logger
from rapidfuzz import fuzz
from src.utils.time import ensure_duckdb_utc

from src.models.property import Property
from src.history.hcpa_history_scraper import HistoricalHCPAScraper
from src.history.db_init import ensure_history_schema
from src.utils.logging_config import configure_logger

configure_logger(log_file="history_pipeline.log")

DB_PATH = Path("data/history.db")
INVALID_PARCELS = {"property appraiser", "n/a", "none", "unknown"}
PLACEHOLDER_BUYERS = {
    "3rd party bidder",
    "third party bidder",
    "3rd party",
    "third party",
    "unknown",
    "",
}

class ResaleScanner:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        ensure_history_schema(self.db_path)
        self.scraper = HistoricalHCPAScraper(headless=True)
        
    def get_pending_scans(self, limit: int = 50):
        """Get 3rd party purchases that haven't been scanned for resale recently."""
        conn = duckdb.connect(str(self.db_path), read_only=True)
        ensure_duckdb_utc(conn)
        try:
            # Logic: 
            # 1. Must be Third Party buyer
            # 2. Must have minimal fields to query HCPA
            # 3. Has NOT been scanned yet (or scanned long ago)
            query = """
                SELECT auction_id, auction_date, case_number, parcel_id, property_address, sold_to, winning_bid
                FROM auctions
                WHERE buyer_type = 'Third Party' 
                AND parcel_id IS NOT NULL 
                AND parcel_id != ''
                AND lower(parcel_id) NOT IN ({invalids})
                AND (last_resale_scan_at IS NULL OR last_resale_scan_at < now() - INTERVAL 30 DAY)
                ORDER BY last_resale_scan_at ASC NULLS FIRST
                LIMIT ?
            """.format(
                invalids=",".join([f"'{p}'" for p in INVALID_PARCELS])
            )
            return conn.execute(query, [limit]).fetchall()
        finally:
            conn.close()

    def update_scan_timestamp(self, auction_ids: list[str]):
        """Update last_resale_scan_at for a batch of auctions."""
        if not auction_ids:
            return
        
        conn = duckdb.connect(str(self.db_path))
        try:
            # Use DuckDB executemany for efficiency
            ids = [(aid,) for aid in auction_ids]
            conn.executemany(
                "UPDATE auctions SET last_resale_scan_at = now() WHERE auction_id = ?",
                ids
            )
        except Exception as e:
            logger.error(f"Failed to update scan timestamps: {e}")
        finally:
            conn.close()

    def save_batch_resales(self, resales_data: list[dict]):
        """Insert batch of confirmed resales into database using Polars."""
        if not resales_data:
            return

        # Create Polars DataFrame
        df = pl.DataFrame(resales_data)
        
        conn = duckdb.connect(str(self.db_path))
        ensure_duckdb_utc(conn)
        try:
            logger.info(f"Writing {len(resales_data)} resales to DB via Polars...")
            conn.execute("ALTER TABLE resales ADD COLUMN IF NOT EXISTS roi DOUBLE")
            
            # Register the DataFrame as a view
            conn.register('df_batch', df)
            
            # Insert using SQL
            conn.execute("""
                INSERT INTO resales (
                    resale_id, parcel_id, auction_id,
                    sale_date, sale_price, sale_type,
                    hold_time_days, gross_profit, roi, source
                ) SELECT 
                    resale_id, parcel_id, auction_id,
                    sale_date, sale_price, sale_type,
                    hold_time_days, gross_profit, roi, source
                FROM df_batch
                ON CONFLICT (resale_id) DO UPDATE SET
                    sale_date = EXCLUDED.sale_date,
                    sale_price = EXCLUDED.sale_price,
                    sale_type = EXCLUDED.sale_type,
                    hold_time_days = EXCLUDED.hold_time_days,
                    gross_profit = EXCLUDED.gross_profit,
                    roi = EXCLUDED.roi,
                    source = EXCLUDED.source
            """)
        except Exception as e:
            logger.error(f"Failed to save batch: {e}")
        finally:
            conn.close()

    def _parse_date(self, date_str: str) -> date | None:
        if not date_str:
            return None
        date_str = date_str.strip()
        for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y", "%m-%d-%y", "%Y/%m/%d"):
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        try:
            month = datetime.strptime(date_str, "%m/%Y").month
            year = datetime.strptime(date_str, "%m/%Y").year
            last_day = calendar.monthrange(year, month)[1]
            return date(year, month, last_day)
        except ValueError:
            return None

    def check_match(self, auction_buyer: str, resale_grantor: str) -> bool:
        """Fuzzy match buyer to grantor."""
        if not auction_buyer or not resale_grantor:
            return False
        
        # Simple normalization
        a = auction_buyer.upper().replace(" LLC", "").replace(" INC", "").replace(" TRUST", "").strip()
        b = resale_grantor.upper().replace(" LLC", "").replace(" INC", "").replace(" TRUST", "").strip()
        
        ratio = fuzz.ratio(a, b)
        return ratio > 75 # Lowered threshold slightly

    def _is_placeholder_buyer(self, buyer: str | None) -> bool:
        if buyer is None:
            return True
        return buyer.strip().lower() in PLACEHOLDER_BUYERS

    def _is_transfer_deed(self, deed_type: str | None) -> bool:
        if not deed_type:
            return False
        normalized = deed_type.upper().strip()
        return "CERTIFICATE" in normalized or normalized.startswith("CT")

    async def _process_single_auction(self, row, semaphore: asyncio.Semaphore) -> dict | None:
        """Process a single auction row under semaphore."""
        async with semaphore:
            auction_id, auction_date_val, case_num, parcel_id, addr, sold_to, winning_bid = row
            
            # Map valid date (handle duckdb date vs text)
            if isinstance(auction_date_val, str):
                auction_date = datetime.strptime(auction_date_val, "%Y-%m-%d").date()
            else:
                auction_date = auction_date_val

            # Create dummy property object for scraper
            prop = Property(
                case_number=case_num,
                parcel_id=parcel_id,
                address=addr
            )
            
            try:
                # Scrape HCPA (No DB write involved)
                prop = await self.scraper.enrich_property(prop)
                
                if prop.sales_history:
                    candidates = []
                    for sale in prop.sales_history:
                        sale_date_val = sale.get("date")
                        if not sale_date_val:
                            continue
                        sale_date = self._parse_date(sale_date_val)
                        if not sale_date or sale_date <= auction_date:
                            continue
                        price_val = sale.get("price", 0)
                        if isinstance(price_val, str):
                            try:
                                price_val = float(price_val.replace("$", "").replace(",", ""))
                            except ValueError:
                                price_val = 0
                        deed_type = sale.get("deed_type") or sale.get("type") or "Unknown"
                        grantor = sale.get("grantor", "")
                        candidates.append((sale_date, price_val, deed_type, grantor))

                    if not candidates:
                        return None

                    # Sort candidates by sale date (earliest first) to ensure we pick
                    # the first qualifying sale after auction, not an arbitrary one
                    candidates.sort(key=lambda x: x[0])

                    # Prefer verified grantor match.
                    for sale_date, price_val, deed_type, grantor in candidates:
                        if self.check_match(sold_to, grantor):
                            return self._build_resale(
                                auction_id,
                                parcel_id,
                                sale_date,
                                price_val,
                                deed_type,
                                auction_date,
                                winning_bid,
                                "HCPA",
                                sold_to,
                            )

                    # Fallback: if buyer is placeholder, use first non-transfer sale.
                    if self._is_placeholder_buyer(sold_to):
                        for sale_date, price_val, deed_type, _grantor in candidates:
                            if self._is_transfer_deed(deed_type):
                                continue
                            if price_val <= 0:
                                continue
                            return self._build_resale(
                                auction_id,
                                parcel_id,
                                sale_date,
                                price_val,
                                deed_type,
                                auction_date,
                                winning_bid,
                                "HCPA_UNVERIFIED",
                                sold_to,
                            )
                            
            except Exception as e:
                logger.error(f"Error scanning {parcel_id}: {e}")
            
            return None

    def _build_resale(
        self,
        auction_id: str,
        parcel_id: str,
        sale_date: date,
        price_val: float,
        deed_type: str,
        auction_date: date,
        winning_bid: float | None,
        source: str,
        sold_to: str | None,
    ) -> dict:
        hold_time = (sale_date - auction_date).days
        base_bid = winning_bid if winning_bid and winning_bid > 0 else None
        profit = price_val - base_bid if base_bid is not None else None
        roi = (profit / base_bid) if base_bid else None
        if source == "HCPA":
            logger.success(
                "FOUND FLIP! {} ({}) -> Sold on {} for ${} (Profit: ${})",
                auction_id,
                sold_to or "Unknown",
                sale_date,
                f"{price_val:,.0f}",
                f"{profit:,.0f}" if profit is not None else "N/A",
            )
        else:
            logger.info(
                "Resale candidate {} -> Sold on {} for ${} (unverified grantor)",
                auction_id,
                sale_date,
                f"{price_val:,.0f}",
            )
        return {
            "resale_id": f"{parcel_id}_{sale_date.strftime('%Y%m%d')}",
            "parcel_id": parcel_id,
            "auction_id": auction_id,
            "sale_date": sale_date,
            "sale_price": price_val,
            "sale_type": deed_type or "Unknown",
            "hold_time_days": hold_time,
            "gross_profit": profit,
            "roi": roi,
            "source": source,
        }

    async def scan_batch(self, batch_size: int = 50) -> int:
        """Process a batch of auctions concurrently."""
        targets = self.get_pending_scans(batch_size)
        if not targets:
            logger.info("No pending auctions to scan.")
            return 0

        logger.info(f"Scanning {len(targets)} auctions for resales...")
        
        # Concurrency Control
        sem = asyncio.Semaphore(5) # 5 concurrent browsers
        
        tasks = [self._process_single_auction(row, sem) for row in targets]
        results = await asyncio.gather(*tasks)
        
        # Filter None
        resales_to_save = [r for r in results if r]
        
        # Batch Write Resales
        if resales_to_save:
            self.save_batch_resales(resales_to_save)
        else:
            logger.info("No flips found in this batch.")

        # Batch Update Scan Timestamps (for ALL attempted auctions)
        auction_ids = [row[0] for row in targets]
        self.update_scan_timestamp(auction_ids)
        return len(targets)

    async def scan_all_pending(
        self,
        batch_size: int = 50,
        max_batches: int | None = None,
    ) -> int:
        """Scan all pending auctions in successive batches."""
        total_scanned = 0
        batches = 0
        while True:
            if max_batches is not None and batches >= max_batches:
                logger.info("Reached max resale scan batches (%d).", max_batches)
                break
            scanned = await self.scan_batch(batch_size)
            if scanned == 0:
                break
            total_scanned += scanned
            batches += 1
        return total_scanned

if __name__ == "__main__":
    asyncio.run(ResaleScanner().scan_batch(50))
