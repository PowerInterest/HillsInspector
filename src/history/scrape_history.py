import asyncio
import sys
import duckdb
import re
import random
import time
from contextlib import suppress
from datetime import date, timedelta
from pathlib import Path
from loguru import logger
import polars as pl
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
CALENDAR_URL = f"{BASE_URL}/index.cfm?zaction=user&zmethod=calendar"
USER_AGENT_DESKTOP = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
HISTORY_PARQUET = Path("data/web/history.parquet")

USER_AGENTS = [
    USER_AGENT_DESKTOP,
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
]

VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1680, "height": 1050},
    {"width": 1366, "height": 768},
]

SOLD_TO_PATTERNS = (
    r"Sold To:\s*([^\n]+)",
    r"Sold To\s*([^\n]+)",
)
WINNING_BID_PATTERNS = (
    r"Winning Bid:\s*(\$\d[\d,]*\.?\d*)",
    r"Sale Amount:\s*(\$\d[\d,]*\.?\d*)",
    r"Sold Amount:\s*(\$\d[\d,]*\.?\d*)",
    r"Winning Bid\s*(\$\d[\d,]*\.?\d*)",
    r"Amount:\s*(\$\d[\d,]*\.?\d*)",
    r"Amount\s*(\$\d[\d,]*\.?\d*)",
)


def _extract_text_field(patterns: tuple[str, ...], text: str) -> str | None:
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            value = match.group(1).strip()
            if value:
                return value
    return None


class BlockedError(RuntimeError):
    pass

class HistoricalScraper:
    def __init__(
        self,
        db_path: Path = DB_PATH,
        max_concurrent: int = 1,
        headless: bool = True,
        browser_names: list[str] | None = None,
    ):
        self.db_path = db_path
        self._scraped_dates = set()
        self._load_scraped_dates()
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.headless = headless
        self.user_agent_index = 0
        self.viewport_index = 0
        self.browser_names = browser_names or ["chromium"]
        self.browser_index = 0
        self.active_browser_name = None
        self.consecutive_blocks = 0
        self.max_consecutive_blocks = 3
        self.backoff_seconds = 30
        self.backoff_max_seconds = 300
        
        # Persistent playwright objects
        self.playwright = None
        self.browser = None
        self.context = None
        self.session_initialized = False

        self.history_df = self._load_history_df()
        self.last_history_flush = time.monotonic()
        self.flush_interval_seconds = 600  # 10 minutes

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
        await self._launch_browser(headless=headless)
        self.context = await self.browser.new_context(
            viewport=VIEWPORTS[self.viewport_index],
            user_agent=USER_AGENTS[self.user_agent_index],
            locale="en-US",
            timezone_id="America/New_York",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        # Establish session by visiting home page
        page = await self.context.new_page()
        await Stealth().apply_stealth_async(page)
        await page.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9"})
        logger.info("Establishing session on home page...")
        await page.goto(f"{BASE_URL}/index.cfm", wait_until="networkidle", timeout=60000)
        logger.info(f"Session landing URL: {page.url}")
        await asyncio.sleep(2)
        await self._bypass_splash(page)
        await page.close()
        self.session_initialized = True
        logger.success("Session established.")

    async def _launch_browser(self, headless: bool) -> None:
        if self.playwright is None:
            self.playwright = await async_playwright().start()
        attempts = len(self.browser_names)
        last_error = None
        for _ in range(attempts):
            name = self.browser_names[self.browser_index]
            browser_type = getattr(self.playwright, name, None)
            if browser_type is None:
                logger.warning(f"Unknown browser type '{name}', skipping.")
                self.browser_index = (self.browser_index + 1) % len(self.browser_names)
                continue
            logger.info(
                "Launching %s (headless=%s, UA=%s, viewport=%s)",
                name,
                headless,
                USER_AGENTS[self.user_agent_index],
                VIEWPORTS[self.viewport_index],
            )
            try:
                self.browser = await browser_type.launch(
                    headless=headless,
                    args=["--no-sandbox", "--disable-dev-shm-usage"],
                )
                self.active_browser_name = name
                return
            except Exception as exc:
                last_error = exc
                logger.warning(f"Failed to launch {name}: {exc}")
                self.browser_index = (self.browser_index + 1) % len(self.browser_names)
        raise RuntimeError(f"Unable to launch any browser: {last_error}")

    async def close_browser(self, force_flush: bool = True):
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
        if force_flush:
            self._maybe_flush_history(force=True)

    async def _rotate_browser_profile(self) -> None:
        """Rotate user agent + viewport when blocked and re-establish session."""
        self.user_agent_index = (self.user_agent_index + 1) % len(USER_AGENTS)
        self.viewport_index = (self.viewport_index + 1) % len(VIEWPORTS)
        self.browser_index = (self.browser_index + 1) % len(self.browser_names)
        logger.warning(
            "Rotating browser profile to UA index %d, viewport index %d, browser index %d",
            self.user_agent_index,
            self.viewport_index,
            self.browser_index,
        )
        await self.close_browser(force_flush=False)
        await self.setup_browser(headless=self.headless)

    async def _backoff_and_maybe_stop(self, reason: str, attempt: int) -> None:
        self.consecutive_blocks += 1
        sleep_for = min(
            self.backoff_seconds * (2 ** min(self.consecutive_blocks - 1, 4)),
            self.backoff_max_seconds,
        )
        sleep_for += random.uniform(0, self.backoff_seconds)
        logger.warning(
            "Blocked (%s). Backing off %.1fs (attempt %d, consecutive %d/%d).",
            reason,
            sleep_for,
            attempt + 1,
            self.consecutive_blocks,
            self.max_consecutive_blocks,
        )
        await asyncio.sleep(sleep_for)
        if self.consecutive_blocks >= self.max_consecutive_blocks:
            raise BlockedError(f"Blocked {self.consecutive_blocks} consecutive times: {reason}")

    def _reset_block_counter(self) -> None:
        self.consecutive_blocks = 0

    def _load_history_df(self) -> pl.DataFrame:
        if HISTORY_PARQUET.exists():
            try:
                return pl.read_parquet(HISTORY_PARQUET)
            except Exception as exc:
                logger.warning(f"Failed to load history parquet: {exc}")
        return pl.DataFrame()

    def _append_history(self, auctions: list[dict]) -> None:
        if not auctions:
            return
        batch = pl.DataFrame(auctions)
        if self.history_df.is_empty():
            self.history_df = batch
        else:
            self.history_df = pl.concat([self.history_df, batch], how="vertical_relaxed")
        self._maybe_flush_history()

    def _maybe_flush_history(self, force: bool = False) -> None:
        if self.history_df.is_empty():
            return
        now = time.monotonic()
        if not force and (now - self.last_history_flush) < self.flush_interval_seconds:
            return
        if "auction_id" in self.history_df.columns:
            self.history_df = self.history_df.unique(subset=["auction_id"], keep="last")
        HISTORY_PARQUET.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = HISTORY_PARQUET.with_name(HISTORY_PARQUET.name + ".tmp")
        self.history_df.write_parquet(tmp_path)
        tmp_path.replace(HISTORY_PARQUET)
        self.last_history_flush = now
        logger.info(f"History parquet flushed to {HISTORY_PARQUET}")

    async def _bypass_splash(self, page) -> None:
        """If the splash page is shown, click through to set session cookies."""
        try:
            if await page.locator("#splashContainer").count() == 0:
                return
            logger.info("Splash page detected; clicking Auction Calendar...")
            with suppress(Exception):
                await page.click("#splashMenuBottom", timeout=5000)
            with suppress(Exception):
                await page.context.add_cookies([{"name": "bypassPage", "value": "1", "url": BASE_URL}])
            with suppress(Exception):
                await page.evaluate(
                    "() => { try { localStorage.setItem('bypassPage','1'); "
                    "sessionStorage.setItem('bypassPage','1'); } catch (e) {} }"
                )
            with suppress(Exception):
                await page.goto(CALENDAR_URL, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(1500)
            logger.info(f"After splash bypass URL: {page.url}")
        except Exception as exc:
            logger.warning(f"Failed to bypass splash page: {exc}")

    async def _navigate_calendar_to_date(self, page, target_date: date) -> bool:
        """Click a calendar day cell to navigate to the auction day page."""
        date_str = target_date.strftime("%m/%d/%Y")
        # Ensure month view is correct
        await page.goto(f"{CALENDAR_URL}&selCalDate={date_str}", wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(1500)
        logger.info(f"Calendar URL for {date_str}: {page.url}")
        with suppress(Exception):
            await page.evaluate("CALMODE = 'PREVIEW'")

        day_cell = page.locator(f"div.CALBOX[dayid='{date_str}']")
        if await day_cell.count() == 0:
            logger.warning(f"Calendar day cell not found for {date_str}")
            return False

        logger.info(f"Calendar page detected; clicking day {date_str}")
        try:
            await day_cell.first.click(timeout=5000)
            await page.wait_for_load_state("domcontentloaded", timeout=30000)
            await page.wait_for_timeout(1500)
            logger.info(f"After calendar click URL: {page.url}")
            return True
        except Exception as exc:
            logger.warning(f"Failed to click calendar day {date_str}: {exc}")
            return False

    async def _extract_stats_map(self, item) -> dict[str, str]:
        """Extract label/value pairs from the AUCTION_STATS block."""
        stats = item.locator(".AUCTION_STATS")
        if await stats.count() == 0:
            return {}

        labels = stats.locator(".ASTAT_LBL")
        data = stats.locator(".Astat_DATA")
        label_count = await labels.count()
        data_count = await data.count()
        stats_map: dict[str, str] = {}

        for i in range(label_count):
            label_text = (await labels.nth(i).inner_text()).strip()
            if not label_text:
                continue
            value = await labels.nth(i).evaluate(
                "el => el.nextElementSibling && el.nextElementSibling.innerText ? el.nextElementSibling.innerText : ''"
            )
            if not value and i < data_count:
                value = (await data.nth(i).inner_text()).strip()
            stats_map[label_text.lower().rstrip(":").strip()] = str(value).strip()

        return stats_map

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
                winning_bid = COALESCE(EXCLUDED.winning_bid, auctions.winning_bid),
                sold_to = COALESCE(NULLIF(EXCLUDED.sold_to, ''), auctions.sold_to),
                buyer_type = CASE
                    WHEN EXCLUDED.buyer_type IS NULL OR EXCLUDED.buyer_type = 'Unknown'
                        THEN auctions.buyer_type
                    ELSE EXCLUDED.buyer_type
                END,
                status = EXCLUDED.status,
                pdf_url = COALESCE(EXCLUDED.pdf_url, auctions.pdf_url),
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
        
        await self.setup_browser(headless=self.headless)
        
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
                try:
                    results = await asyncio.gather(*tasks)
                except BlockedError as exc:
                    logger.error(f"Stopping history scrape to avoid escalating blocks: {exc}")
                    break
                
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
        """Scrape a specific date using the shared context.

        Returns None when the page fails to load (so the date can be retried).
        """
        if not self.session_initialized:
            await self.setup_browser(headless=self.headless)
            
        date_str = target_date.strftime("%m/%d/%Y")
        # PREVIEW view contains the sold/walked-away status and sold-to/amount info.
        url = f"{BASE_URL}/index.cfm?zaction=AUCTION&zmethod=PREVIEW&AuctionDate={date_str}"
        
        auctions = []
        page = await self.context.new_page()
        await Stealth().apply_stealth_async(page)
        await page.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9"})
        
        try:
            max_retries = 2
            for attempt in range(max_retries + 1):
                try:
                    logger.info(f"Scraping {date_str} (Attempt {attempt+1})...")
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    await page.wait_for_timeout(1500)
                    await self._bypass_splash(page)
                    logger.info(f"Post-goto URL: {page.url}")

                    # Capture early failure (Forbidden)
                    content = await page.content()
                    if "Forbidden" in content or "403" in content:
                        logger.error(f"Access Denied (Forbidden) for {date_str}.")
                        await self._backoff_and_maybe_stop("403 Forbidden", attempt)
                        await self._rotate_browser_profile()
                        continue
                    if "Splash Page" in content or "splashContainer" in content:
                        await self._bypass_splash(page)
                        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                        await page.wait_for_timeout(1500)
                        logger.info(f"Post-splash retry URL: {page.url}")
                        content = await page.content()
                        if "Splash Page" in content or "splashContainer" in content:
                            await self._backoff_and_maybe_stop("splash-loop", attempt)
                            await self._rotate_browser_profile()
                            continue

                    # If we landed on the calendar, click the target day
                    if "Auction Calendar" in content or "CALDAYBOX" in content:
                        # Retry direct PREVIEW link (calendar JS uses PREVIEW mode)
                        fallback_url = f"{BASE_URL}/index.cfm?zaction=AUCTION&zmethod=PREVIEW&AuctionDate={date_str}"
                        await page.goto(fallback_url, wait_until="domcontentloaded", timeout=60000)
                        await page.wait_for_timeout(1500)
                        logger.info(f"Fallback PREVIEW URL: {page.url}")
                        content = await page.content()

                    if "Auction Calendar" in content or "CALDAYBOX" in content:
                        if not await self._navigate_calendar_to_date(page, target_date):
                            content = await page.content()
                            if "Forbidden" in content or "403" in content:
                                await self._backoff_and_maybe_stop("403 Forbidden (calendar)", attempt)
                                await self._rotate_browser_profile()
                                continue
                            # If the day isn't present on the calendar, treat as no auctions for that date.
                            logger.info(f"No calendar entry for {date_str}; treating as empty auction day.")
                            return []
                        content = await page.content()

                    # Wait for items logic (multiple layouts)
                    with suppress(Exception):
                        await page.wait_for_selector(
                            "div.AUCTION_ITEM, #Area_C div.AUCTION_ITEM, div.AUCTION_DETAILS, div.AUCTION_STATS",
                            timeout=20000,
                        )

                    items = page.locator("div.AUCTION_ITEM")
                    count = await items.count()
                    if count == 0:
                        items = page.locator("div.AUCTION_DETAILS").locator("xpath=..")
                        count = await items.count()
                    if count == 0:
                        items = page.locator("div.AUCTION_STATS").locator("xpath=..")
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
                                debug_dir = Path("logs/history_debug")
                                debug_dir.mkdir(parents=True, exist_ok=True)
                                html_path = debug_dir / f"history_{target_date.strftime('%Y%m%d')}_attempt{attempt+1}.html"
                                screenshot_path = debug_dir / f"history_{target_date.strftime('%Y%m%d')}_attempt{attempt+1}.png"
                                with suppress(Exception):
                                    html_path.write_text(content, encoding="utf-8")
                                with suppress(Exception):
                                    await page.screenshot(path=str(screenshot_path), full_page=True)
                                await self._backoff_and_maybe_stop("empty page", attempt)
                                await self._rotate_browser_profile()
                                logger.warning(f"Empty results (likely loading issue) for {date_str}, retrying...")
                                continue
                            logger.warning(f"Final timeout waiting for items on {date_str}")
                        return None

                    # If we got here, we have items
                    self._reset_block_counter()
                    for i in range(count):
                        item = items.nth(i)
                        try:
                            text = await item.inner_text()
                            
                            # --- extraction ---
                            sold_to = None
                            status = "Unknown"

                            details = item.locator("table.ad_tab")
                            stats_map = await self._extract_stats_map(item)

                            async def cell_after(label: str) -> str:
                                row = details.locator(f"tr:has-text('{label}')")
                                if await row.count() == 0:
                                    return ""
                                return (await row.locator("td").nth(1).inner_text()).strip()

                            status_text = (
                                await cell_after("Status:")
                                or await cell_after("Auction Status:")
                                or stats_map.get("auction status", "")
                                or stats_map.get("status", "")
                            )
                            if not status_text and "auction sold" in stats_map:
                                status_text = "Sold"
                            if status_text:
                                status = status_text
                            elif "Cancelled" in text:
                                status = "Cancelled"
                            elif "Redeemed" in text:
                                status = "Redeemed"

                            sold_to = await cell_after("Sold To:")
                            if not sold_to:
                                sold_to = await cell_after("Sold To")
                            if not sold_to:
                                sold_to = stats_map.get("sold to", "")
                            if not sold_to:
                                sold_div = item.locator(".ASTAT_MSG_SOLDTO_MSG, .ASTAT_MSG_SOLDTO")
                                if await sold_div.count() > 0:
                                    sold_to = (await sold_div.first.inner_text()).strip()
                            if not sold_to:
                                sold_to = _extract_text_field(SOLD_TO_PATTERNS, text)
                            if sold_to and sold_to.lower().startswith("sold to"):
                                sold_to = sold_to.split(":", 1)[-1].strip()

                            if sold_to and status == "Unknown":
                                status = "Sold"
                            
                            winning_bid = None
                            bid_text = await cell_after("Winning Bid:")
                            if not bid_text:
                                bid_text = await cell_after("Sale Amount:")
                            if not bid_text:
                                bid_text = await cell_after("Sold Amount:")
                            if not bid_text:
                                for key in ("winning bid", "sale amount", "sold amount", "amount"):
                                    if stats_map.get(key):
                                        bid_text = stats_map[key]
                                        break
                            if not bid_text:
                                bid_div = item.locator(".ASTAT_MSG_WINNING_BID, .ASTAT_MSG_WINNING_BID_MSG")
                                if await bid_div.count() > 0:
                                    bid_text = (await bid_div.first.inner_text()).strip()
                            if bid_text:
                                if "bid" in bid_text.lower() or "sale" in bid_text.lower():
                                    bid_match = _extract_text_field(WINNING_BID_PATTERNS, bid_text)
                                    winning_bid = self._parse_money(bid_match) if bid_match else self._parse_money(bid_text)
                                else:
                                    winning_bid = self._parse_money(bid_text)
                            else:
                                bid_match = _extract_text_field(WINNING_BID_PATTERNS, text)
                                if bid_match:
                                    winning_bid = self._parse_money(bid_match)
                                 
                            # Buyer Type Logic
                            buyer_type = "Unknown" 
                            if sold_to:
                                normalized = sold_to.lower()
                                if any(term in normalized for term in ("plaintiff", "bank", "mortgage", "financial", "lending", "loan")):
                                    buyer_type = "Plaintiff"
                                elif "3rd party" in normalized or "third party" in normalized:
                                    buyer_type = "Third Party"
                                else:
                                    buyer_type = "Third Party" # Default assumption if named entity

                            if "walked away" in status.lower():
                                buyer_type = "Walked Away"

                            if status == "Cancelled":
                                buyer_type = "N/A"

                            case_row = details.locator("tr:has-text('Case #:')")
                            case_link = case_row.locator("a")
                            case_number = (await case_link.inner_text()).strip() if await case_link.count() else ""
                            case_url = await case_link.get_attribute("href") if await case_link.count() else None
                            if case_url and not case_url.startswith("http"):
                                case_url = f"{BASE_URL}{case_url}"
                            
                            parcel_row = details.locator("tr:has-text('Parcel ID:')")
                            parcel_id = ""
                            if await parcel_row.count() > 0:
                                parcel_a = parcel_row.locator("a")
                                if await parcel_a.count() > 0:
                                    parcel_id = (await parcel_a.inner_text()).strip()
                                else:
                                    parcel_id = (await parcel_row.locator("td").nth(1).inner_text()).strip()

                            if parcel_id and parcel_id.lower() in ("property appraiser", "n/a", "none"):
                                parcel_id = ""
                            
                            addr_row = details.locator("tr:has-text('Property Address:')")
                            address = ""
                            if await addr_row.count() > 0:
                                address = (await addr_row.locator("td").nth(1).inner_text()).strip()
                                city_row = addr_row.locator("xpath=./following-sibling::tr[1]")
                                if await city_row.count() > 0:
                                    city_val = (await city_row.locator("td").nth(1).inner_text()).strip()
                                    if city_val and city_val not in address:
                                        address = f"{address}, {city_val}"

                            judg_row = details.locator("tr:has-text('Final Judgment Amount:')")
                            judgment_amount = None
                            if await judg_row.count() > 0:
                                val = await judg_row.locator("td").nth(1).inner_text()
                                judgment_amount = self._parse_money(val)
                                 
                            assessed_row = details.locator("tr:has-text('Assessed Value:')")
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
                                "pdf_url": case_url,
                                "auction_id": f"{case_number}_{target_date.strftime('%Y%m%d')}",
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
            with suppress(Exception):
                await page.close()

        if auctions:
            self._append_history(auctions)
        return auctions

    def _parse_money(self, text: str) -> float | None:
        if not text:
            return None
        try:
            match = re.search(r"\$[\d,]+\.?\d*", text)
            raw = match.group(0) if match else text
            clean = raw.replace("$", "").replace(",", "").strip()
            return float(clean)
        except ValueError:
            return None

async def run_scrape():
    # Full Phase 2 Range: June 2023 to Present
    start = date(2023, 6, 1)
    end = today_local()
    
    scraper = HistoricalScraper(
        max_concurrent=1,
        headless=False,
        browser_names=["chromium", "firefox", "webkit"],
    )  # One at a time for maximum stealth
    try:
        await scraper.scrape_date_range(start, end)
    finally:
        await scraper.close_browser()

if __name__ == "__main__":
    asyncio.run(run_scrape())
