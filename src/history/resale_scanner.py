import asyncio
import duckdb
import polars as pl
from datetime import datetime, date
from pathlib import Path
from loguru import logger
from rapidfuzz import fuzz
from src.utils.time import ensure_duckdb_utc

from src.models.property import Property
from src.history.hcpa_history_scraper import HistoricalHCPAScraper
from src.utils.logging_config import configure_logger

configure_logger(log_file="history_pipeline.log")

DB_PATH = Path("data/history.db")

class ResaleScanner:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
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
                AND (last_resale_scan_at IS NULL OR last_resale_scan_at < now() - INTERVAL 30 DAY)
                ORDER BY last_resale_scan_at ASC NULLS FIRST
                LIMIT ?
            """
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
                    sale_price = EXCLUDED.sale_price,
                    gross_profit = EXCLUDED.gross_profit,
                    roi = EXCLUDED.roi
            """)
        except Exception as e:
            logger.error(f"Failed to save batch: {e}")
        finally:
            conn.close()

    def _parse_date(self, date_str: str) -> date | None:
        try:
            return datetime.strptime(date_str, "%m/%d/%Y").date()
        except ValueError:
            try:
                return datetime.strptime(date_str, "%Y-%m-%d").date()
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
                    for sale in prop.sales_history:
                        # 1. Date Check
                        sale_date_val = sale.get('date')
                        if not sale_date_val: continue
                        sale_date = self._parse_date(sale_date_val)
                        if not sale_date or sale_date <= auction_date:
                            continue
                            
                        # 2. Grantor Match Check
                        grantor = sale.get('grantor', '')
                        if self.check_match(sold_to, grantor):
                            # Found a flip!
                            price_val = sale.get('price', 0)
                            if isinstance(price_val, str):
                                try:
                                    price_val = float(price_val.replace('$', '').replace(',', ''))
                                except ValueError:
                                    price_val = 0
                            
                            hold_time = (sale_date - auction_date).days
                            base_bid = winning_bid or 0
                            profit = price_val - base_bid
                            roi = None
                            if base_bid > 0:
                                roi = profit / base_bid
                            
                            logger.success(f"FOUND FLIP! {auction_id} ({sold_to}) -> Sold on {sale_date} for ${price_val:,.0f} (Profit: ${profit:,.0f})")
                            
                            return {
                                "resale_id": f"{parcel_id}_{sale_date.strftime('%Y%m%d')}",
                                "parcel_id": parcel_id,
                                "auction_id": auction_id,
                                "sale_date": sale_date,
                                "sale_price": price_val,
                                "sale_type": sale.get('deed_type', 'Unknown'),
                                "hold_time_days": hold_time,
                                "gross_profit": profit,
                                "roi": roi,
                                "source": "HCPA"
                            }
                            
            except Exception as e:
                logger.error(f"Error scanning {parcel_id}: {e}")
            
            return None

    async def scan_batch(self, batch_size: int = 50):
        """Process a batch of auctions concurrently."""
        targets = self.get_pending_scans(batch_size)
        if not targets:
            logger.info("No pending auctions to scan.")
            return

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

if __name__ == "__main__":
    asyncio.run(ResaleScanner().scan_batch(50))
