import asyncio
import sys
import duckdb
import re
import random
from contextlib import suppress
from datetime import date, timedelta
from pathlib import Path
from loguru import logger
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from src.utils.time import today_local
from src.utils.time import ensure_duckdb_utc, now_utc_naive
from src.utils.logging_config import configure_logger

configure_logger(log_file="history_pipeline.log")

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

DB_PATH = Path("data/history.db")
BASE_URL = "https://hillsborough.realforeclose.com"

class HistoricalScraper:
    def __init__(self, db_path: Path = DB_PATH, max_concurrent: int = 1):
        self.db_path = db_path
        self._scraped_dates = set()
        self._load_scraped_dates()
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # Persistent playwright objects
        self.playwright = None
        self.browser = None
        self.context = None
        self.session_initialized = False

    def _load_scraped_dates(self):
        """Load fully processed dates from scraped_dates table."""
        if not self.db_path.exists():
            return
        conn = None
        try:
            conn = duckdb.connect(str(self.db_path))
            # Load from the tracking table instead of DISTINCT auctions
            rows = conn.execute("SELECT auction_date FROM scraped_dates WHERE status != 'Failed'").fetchall()
            self._scraped_dates = {r[0].date() if hasattr(r[0], 'date') else r[0] for r in rows}
            logger.info(f"Loaded {len(self._scraped_dates)} fully processed dates.")
        except Exception as e:
            logger.warning(f"Could not load scraped dates: {e}")
        finally:
            if conn:
                conn.close()

    async def setup_browser(self, headless: bool = True):
        """Initialize a persistent browser and establish a session."""
        if self.browser:
            return
            
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=headless, 
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        self.context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        # Establish session by visiting home page
        page = await self.context.new_page()
        await Stealth().apply_stealth_async(page)
        logger.info("Establishing session on home page...")
        await page.goto(f"{BASE_URL}/index.cfm", wait_until="networkidle", timeout=60000)
        await asyncio.sleep(2)
        await page.close()
        self.session_initialized = True
        logger.success("Session established.")

    async def close_browser(self):
        """Close playwright resources."""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        self.context = None
        self.browser = None
        self.playwright = None
        self.session_initialized = False

    def mark_date_done(self, target_date: date, status: str = 'Success'):
        """Record that a date has been fully processed."""
        conn = None
        try:
            conn = duckdb.connect(str(self.db_path))
            conn.execute("""
                INSERT INTO scraped_dates (auction_date, status) 
                VALUES (?, ?)
                ON CONFLICT (auction_date) DO UPDATE SET 
                    status = EXCLUDED.status,
                    scraped_at = now()
            """, [target_date, status])
        except Exception as e:
            logger.error(f"Failed to mark date {target_date} as done: {e}")
        finally:
            if conn:
                conn.close()

    def save_batch(self, auctions: list[dict]):
        if not auctions:
            return
            
        logger.info(f"Saving batch of {len(auctions)} auctions...")
        
        insert_sql = """
            INSERT INTO auctions (
                auction_id, auction_date, case_number, parcel_id, property_address,
                winning_bid, final_judgment_amount, assessed_value,
                sold_to, buyer_normalized, buyer_type,
                auction_url, pdf_url, status, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (auction_id) DO UPDATE SET
                winning_bid = EXCLUDED.winning_bid,
                sold_to = EXCLUDED.sold_to,
                buyer_type = EXCLUDED.buyer_type,
                status = EXCLUDED.status,
                pdf_url = EXCLUDED.pdf_url,
                scraped_at = EXCLUDED.scraped_at
        """
        
        data = []
        now = now_utc_naive()
        for a in auctions:
            # Generate ID
            auction_id = f"{a['case_number']}_{a['auction_date'].strftime('%Y%m%d')}"
            
            data.append((
                auction_id,
                a['auction_date'], 
                a['case_number'], 
                a['parcel_id'], 
                a['property_address'],
                a['winning_bid'], 
                a['final_judgment_amount'], 
                a['assessed_value'],
                a['sold_to'], 
                a['sold_to'].upper() if a['sold_to'] else None, # Simple normalization for now
                a['buyer_type'],
                a['auction_url'], 
                a.get('pdf_url'),
                a.get('status'),
                now
            ))

        conn = None  
        try:
            conn = duckdb.connect(str(self.db_path))
            ensure_duckdb_utc(conn)
            conn.executemany(insert_sql, data)
        except Exception as e:
            logger.error(f"Failed to save batch: {e}")
        finally:
            if conn:
                conn.close()

    async def scrape_date_range(self, start_date: date, end_date: date):
        """Iterate through all dates and scrape with concurrency control."""
        self._load_scraped_dates()
        
        await self.setup_browser()
        
        current = start_date
        while current <= end_date:
            # Group into small chunks just for reporting/batching
            chunk_end = min(current + timedelta(days=5), end_date)
            
            tasks = []
            d = current
            while d < chunk_end:
                if d.weekday() < 5: # Skip weekends
                    if d not in self._scraped_dates:
                        tasks.append(self.scrape_with_semaphore(d))
                d += timedelta(days=1)
            
            if tasks:
                results = await asyncio.gather(*tasks)
                
                # 'tasks' was built from dates that weren't in self._scraped_dates
                # We need the actual dates back to mark them done.
                # Let's adjust tasks to return (date, results)
                for target_date, daily_auctions in results:
                    if daily_auctions is not None:
                        if daily_auctions:
                            self.save_batch(daily_auctions)
                            self.mark_date_done(target_date, 'Success')
                        else:
                            self.mark_date_done(target_date, 'Empty')
            
            current = chunk_end
            
    async def scrape_with_semaphore(self, target_date: date):
        """Wrapper for scrape_single_date with semaphore and jitter."""
        async with self.semaphore:
            # Human-like jitter
            delay = random.uniform(1.0, 3.5)
            await asyncio.sleep(delay)
            
            auctions = await self.scrape_single_date(target_date)
            return (target_date, auctions)

    async def scrape_single_date(self, target_date: date) -> list[dict] | None:
        """Scrape a specific date using the shared context."""
        if not self.session_initialized:
            await self.setup_browser()
            
        date_str = target_date.strftime("%m/%d/%Y")
        url = f"{BASE_URL}/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE={date_str}"
        
        auctions = []
        page = await self.context.new_page()
        await Stealth().apply_stealth_async(page)
        
        try:
            max_retries = 2
            for attempt in range(max_retries + 1):
                try:
                    logger.info(f"Scraping {date_str} (Attempt {attempt+1})...")
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    
                    # Capture early failure (Forbidden)
                    content = await page.content()
                    if "Forbidden" in content or "403" in content:
                        logger.error(f"Access Denied (Forbidden) for {date_str}. Retrying in 30s...")
                        await asyncio.sleep(30)
                        continue

                    # Wait for items logic
                    with suppress(Exception):
                        await page.wait_for_selector(".AUCTION_ITEM", timeout=20000)

                    items = page.locator(".AUCTION_ITEM")
                    count = await items.count()
                    
                    if count == 0:
                        empty_indicators = [
                            "No auctions found",
                            "There are no cases",
                            "no cases currently being auctioned",
                            "is not a scheduled auction date"
                        ]
                        is_empty = any(ind.lower() in content.lower() for ind in empty_indicators)
                        
                        if is_empty:
                             logger.info(f"Confirmed no auctions for {date_str}")
                             # Important: break to return empty list [].
                             # We break the retry loop and it will return the 'auctions' list (empty).
                             break 
                        else:
                             if attempt < max_retries:
                                 logger.warning(f"Empty results (likely loading issue) for {date_str}, retrying...")
                                 await asyncio.sleep(random.uniform(5, 10))
                                 continue
                             logger.warning(f"Final timeout waiting for items on {date_str}")
                        return None

                    # If we got here, we have items
                    for i in range(count):
                        item = items.nth(i)
                        try:
                            text = await item.inner_text()
                            
                            # --- extraction ---
                            sold_to = None
                            status = "Unknown"
                            
                            if "Sold To" in text:
                                status = "Sold"
                                sold_row = item.locator("tr:has-text('Sold To:')")
                                if await sold_row.count() > 0:
                                    val = await sold_row.locator("td").nth(1).inner_text()
                                    sold_to = val.strip()
                                else:
                                    # Try DIV selector (observed in 2024 HTML)
                                    sold_div = item.locator(".ASTAT_MSG_SOLDTO_MSG")
                                    if await sold_div.count() > 0:
                                        sold_to = (await sold_div.inner_text()).strip()
                            elif "Cancelled" in text:
                                status = "Cancelled"
                            
                            winning_bid = None
                            bid_row = item.locator("tr:has-text('Winning Bid:')")
                            if await bid_row.count() > 0:
                                 val = await bid_row.locator("td").nth(1).inner_text()
                                 winning_bid = self._parse_money(val)
                            else:
                                 # Regex fallback for Winning Bid
                                 match = re.search(r"Winning Bid:\s*(\$[\d,]+\.?\d*)", text)
                                 if match:
                                     winning_bid = self._parse_money(match.group(1))
                                 
                            # Buyer Type Logic
                            buyer_type = "Unknown" 
                            if sold_to:
                                normalized = sold_to.lower()
                                if "plaintiff" in normalized or "bank" in normalized:
                                    buyer_type = "Plaintiff"
                                elif "3rd party" in normalized or "third party" in normalized:
                                    buyer_type = "Third Party"
                                else:
                                    buyer_type = "Third Party" # Default assumption if named entity
                            


                            if status == "Cancelled":
                                 buyer_type = "N/A"

                            case_row = item.locator("tr:has-text('Case #:')")
                            case_link = case_row.locator("a")
                            case_number = (await case_link.inner_text()).strip()
                            case_url = await case_link.get_attribute("href")
                            if case_url and not case_url.startswith("http"):
                                 case_url = f"{BASE_URL}{case_url}"
                            
                            parcel_row = item.locator("tr:has-text('Parcel ID:')")
                            parcel_id = ""
                            if await parcel_row.count() > 0:
                                 parcel_a = parcel_row.locator("a")
                                 if await parcel_a.count() > 0:
                                    parcel_id = (await parcel_a.inner_text()).strip()
                            
                            addr_row = item.locator("tr:has-text('Property Address:')")
                            address = ""
                            if await addr_row.count() > 0:
                                address = (await addr_row.locator("td").nth(1).inner_text()).strip()

                            judg_row = item.locator("tr:has-text('Final Judgment Amount:')")
                            judgment_amount = None
                            if await judg_row.count() > 0:
                                 val = await judg_row.locator("td").nth(1).inner_text()
                                 judgment_amount = self._parse_money(val)
                                 
                            assessed_row = item.locator("tr:has-text('Assessed Value:')")
                            assessed_value = None
                            if await assessed_row.count() > 0:
                                 val = await assessed_row.locator("td").nth(1).inner_text()
                                 assessed_value = self._parse_money(val)

                            if status == "Sold":
                                # logger.info(f"debug: {case_number} -> Sold To: {sold_to} Type: {buyer_type} Bid: {winning_bid}")
                                pass

                            auctions.append({
                                "auction_date": target_date,
                                "case_number": case_number,
                                "parcel_id": parcel_id,
                                "status": status,
                                "winning_bid": winning_bid,
                                "final_judgment_amount": judgment_amount,
                                "assessed_value": assessed_value,
                                "sold_to": sold_to,
                                "buyer_type": buyer_type,
                                "property_address": address,
                                "auction_url": url,
                                "pdf_url": case_url
                            })
                            
                        except Exception as e:
                            logger.error(f"Error parsing item on {date_str}: {e}")
                            continue
                            
                    # If we successfully parsed the page, break the retry loop
                    break

                except Exception as e:
                    if attempt < max_retries:
                        logger.warning(f"Error on {date_str} (Attempt {attempt+1}): {e}. Retrying...")
                        await asyncio.sleep(random.uniform(2, 5))
                        continue
                    logger.error(f"Failed page {date_str} after {max_retries+1} attempts: {e}")
            
        finally:
            await page.close()
                
        return auctions

    def _parse_money(self, text: str) -> float | None:
        if not text: return None
        try:
            clean = text.replace("$", "").replace(",", "").strip()
            return float(clean)
        except ValueError:
            return None

async def run_scrape():
    # Full Phase 2 Range: June 2023 to Present
    start = date(2023, 6, 1)
    end = today_local()
    
    scraper = HistoricalScraper(max_concurrent=1) # One at a time for maximum stealth
    try:
        await scraper.scrape_date_range(start, end)
    finally:
        await scraper.close_browser()

if __name__ == "__main__":
    asyncio.run(run_scrape())
