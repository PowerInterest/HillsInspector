import asyncio
import concurrent.futures
import random
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests
from loguru import logger
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from src.utils.time import now_utc, today_local
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from src.utils.logging_utils import log_search, Timer


async def apply_stealth(page):
    """Apply stealth settings to a page to avoid bot detection."""
    await Stealth().apply_stealth_async(page)


# Anti-detection: User agent rotation pool
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0",
]

# Anti-detection: Viewport sizes (common desktop resolutions)
VIEWPORT_SIZES = [
    {"width": 1920, "height": 1080},
    {"width": 1536, "height": 864},
    {"width": 1440, "height": 900},
    {"width": 1366, "height": 768},
    {"width": 2560, "height": 1440},
    {"width": 1680, "height": 1050},
]

# Anti-detection: Timezones (US-based for Florida site)
TIMEZONES = [
    "America/New_York",
    "America/Chicago",
    "America/Denver",
    "America/Los_Angeles",
]

# Anti-detection: Locales
LOCALES = ["en-US", "en-GB", "en-CA"]


def get_random_browser_config() -> dict:
    """Get randomized browser configuration for anti-detection."""
    return {
        "user_agent": random.choice(USER_AGENTS),  # noqa: S311
        "viewport": random.choice(VIEWPORT_SIZES),  # noqa: S311
        "timezone_id": random.choice(TIMEZONES),  # noqa: S311
        "locale": random.choice(LOCALES),  # noqa: S311
    }


def random_delay(min_seconds: float = 2.0, max_seconds: float = 6.0):
    """Add a random delay to avoid detection."""
    delay = random.uniform(min_seconds, max_seconds)  # noqa: S311
    logger.debug(f"Waiting {delay:.1f}s...")
    time.sleep(delay)


async def random_delay_async(min_seconds: float = 2.0, max_seconds: float = 6.0):
    """Async version of random delay."""
    delay = random.uniform(min_seconds, max_seconds)  # noqa: S311
    logger.debug(f"Waiting {delay:.1f}s...")
    await asyncio.sleep(delay)


class ORIApiScraper:
    """
    Scraper for Hillsborough County Official Records Index (ORI) using the hidden API.
    """

    SEARCH_URL = "https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search"
    PDF_URL = "https://publicaccess.hillsclerk.com/Public/ORIUtilities/OverlayWatermark/api/Watermark"

    HEADERS = {
        "Content-Type": "application/json; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Origin": "https://publicaccess.hillsclerk.com",
        "Referer": "https://publicaccess.hillsclerk.com/oripublicaccess/",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": random.choice(USER_AGENTS),  # Randomize on init  # noqa: S311
    }

    TITLE_DOC_TYPES = [
        "(MTG) MORTGAGE",
        "(MTGREV) MORTGAGE REVERSE",
        "(MTGNT) MORTGAGE EXEMPT TAXES",
        "(MTGNIT) MORTGAGE NO INTANGIBLE TAXES",
        "(LN) LIEN",
        "(MEDLN) MEDICAID LIEN",
        "(LNCORPTX) CORP TAX LIEN FOR STATE OF FLORIDA",
        "(LP) LIS PENDENS",
        "(RELLP) RELEASE LIS PENDENS",
        "(JUD) JUDGMENT",
        "(CCJ) CERTIFIED COPY OF A COURT JUDGMENT",
        "(D) DEED",
        "(ASG) ASSIGNMENT",
        "(TAXDEED) TAX DEED",
        "(SATCORPTX) SATISFACTION CORP TAX FOR STATE OF FL",
        "(SAT) SATISFACTION",
        "(REL) RELEASE",
        "(PR) PARTIAL RELEASE",
        "(NOC) NOTICE OF COMMENCEMENT",
        "(MOD) MODIFICATION",
        "(ASGT) ASSIGNMENT/TAXES",
        "(AFF) AFFIDAVIT",
        "(FNLJ) FINAL JUDGMENT",
        "(COURT) COURT",
        "(COR) CORRECTIVE",
    ]

    def __init__(self):
        self.session = requests.Session()
        # Initialize session cookies
        try:
            self.session.get("https://publicaccess.hillsclerk.com/oripublicaccess/", timeout=10)
        except Exception as e:
            logger.warning(f"Failed to initialize ORI session: {e}")

        # Persistent browser for scraping
        self.playwright = None
        self.browser = None
        self.context = None
        # Track API stability to decide backoff/fallback timing
        self.consecutive_api_errors = 0
        self.last_api_error_ts: float = 0.0
        # Cool-down mode: if True, skip API entirely and use browser-only
        self.api_cooled_down = False
        self.api_cooldown_until: float = 0.0
        # Threshold for entering cool-down mode
        self.API_COOLDOWN_THRESHOLD = 5  # After 5 consecutive 400s, cool down
        self.API_COOLDOWN_DURATION = 300  # 5 minutes cool-down

    def search_by_legal(
        self,
        legal_description: str,
        start_date: str = "01/01/1900",
        end_date: str | None = None,
    ) -> List[Dict[str, Any]]:
        """
        Search for documents by legal description.

        Args:
            legal_description: Text to search in legal description (e.g. subdivision name)
            start_date: Start date for search (MM/DD/YYYY)
            end_date: End date for search (MM/DD/YYYY), defaults to today

        Returns:
            List of document dictionaries
        """
        # Strip wildcards - API uses CONTAINS which doesn't support wildcards
        clean_legal = legal_description.rstrip("*").strip()

        payload = {
            "DocType": self.TITLE_DOC_TYPES,
            "RecordDateBegin": start_date,
            "RecordDateEnd": end_date or today_local().strftime("%m/%d/%Y"),
            "Legal": ["CONTAINS", clean_legal],
        }
        with Timer() as t:
            results = self._execute_search(payload)
        log_search(
            source="ORI_API",
            query=f"legal:{clean_legal}",
            results_raw=len(results),
            duration_ms=t.ms,
        )
        return results

    def search_by_legal_parallel(
        self,
        search_terms: List[str],
        start_date: str = "01/01/1900",
        max_workers: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Search for documents using multiple legal description terms in parallel.

        Fires all search terms concurrently and returns the union of all results,
        deduplicated by instrument number.

        Args:
            search_terms: List of legal description search terms
            start_date: Start date for search (MM/DD/YYYY)
            max_workers: Maximum concurrent API requests (default 5)

        Returns:
            List of unique document dictionaries (deduplicated by Instrument)
        """
        if not search_terms:
            return []

        def search_single(term: str) -> tuple[str, List[Dict[str, Any]]]:
            """Search a single term and return (term, results)."""
            try:
                with Timer() as t:
                    results = self.search_by_legal(term, start_date)
                log_search(
                    source="ORI_API",
                    query=f"legal:{term}",
                    results_raw=len(results),
                    duration_ms=t.ms,
                )
                return (term, results)
            except Exception as e:
                logger.debug(f"Parallel search failed for '{term}': {e}")
                return (term, [])

        logger.info(f"Parallel ORI search: {len(search_terms)} terms with {max_workers} workers")

        all_results: List[Dict[str, Any]] = []
        successful_terms: List[str] = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(search_single, term): term for term in search_terms}
            for future in concurrent.futures.as_completed(futures):
                term, results = future.result()
                if results:
                    all_results.extend(results)
                    successful_terms.append(term)
                    logger.debug(f"  {term}: {len(results)} results")

        # Deduplicate by Instrument number
        seen_instruments: set[str] = set()
        unique_results: List[Dict[str, Any]] = []
        for doc in all_results:
            instrument = doc.get("Instrument", "")
            if instrument and instrument not in seen_instruments:
                seen_instruments.add(instrument)
                unique_results.append(doc)

        log_search(
            source="ORI_API",
            query=f"legal_parallel:{len(search_terms)} terms",
            results_raw=len(unique_results),
            results_kept=len(unique_results),
        )
        return unique_results

    def search_by_party(
        self,
        party_name: str,
        start_date: str = "01/01/1900",
        end_date: str | None = None,
    ) -> List[Dict[str, Any]]:
        """
        Search for documents by party name.

        Args:
            party_name: Name of party (Last First Middle or Company Name)
            start_date: Start date for search (MM/DD/YYYY)
            end_date: End date for search (MM/DD/YYYY), defaults to today
        """
        payload = {
            "DocType": self.TITLE_DOC_TYPES,
            "RecordDateBegin": start_date,
            "RecordDateEnd": end_date or today_local().strftime("%m/%d/%Y"),
            "Party": party_name,
        }
        return self._execute_search(payload)

    def search_by_instrument(self, instrument: str, include_doc_types: bool = False) -> List[Dict[str, Any]]:
        """
        Search for documents by instrument number.

        Args:
            instrument: Instrument number (e.g., "2024478600")
            include_doc_types: If True, filter by TITLE_DOC_TYPES. If False, search all doc types.

        Returns:
            List of document dictionaries (usually 0 or 1)
        """
        payload = {
            "RecordDateBegin": "01/01/1900",
            "RecordDateEnd": today_local().strftime("%m/%d/%Y"),
            "Instrument": instrument,
        }
        # Only add doc type filter if requested (can cause 400 errors on some searches)
        if include_doc_types:
            payload["DocType"] = self.TITLE_DOC_TYPES
        return self._execute_search(payload)

    async def search_by_book_page_browser(
        self, book: str, page: str, book_type: str = "OR", headless: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Search ORI by book and page number using browser-based CQID=319 endpoint.

        This is the most accurate search method - returns exact document.

        Known OBKey parameters for CQID 319:
        - OBKey__573_1: Book number
        - OBKey__1049_1: Page number
        - OBKey__1530_1: Book type (OR = Official Records)

        Args:
            book: Book number (e.g., "12345")
            page: Page number (e.g., "0001")
            book_type: Book type, usually "OR" for Official Records
            headless: Run browser in headless mode

        Returns:
            List of document records (usually 0 or 1)
        """
        # Use CQID=319 for book/page search
        url = (
            f"https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html"
            f"?CQID=319&OBKey__573_1={quote(book)}&OBKey__1049_1={quote(page)}"
            f"&OBKey__1530_1={quote(book_type)}"
        )
        logger.info(f"Searching ORI by book/page: Book={book}, Page={page}")

        # Ensure browser is running
        await self._ensure_browser(headless)

        context = await self._create_isolated_context()

        try:
            page_obj = await context.new_page()
            await apply_stealth(page_obj)

            logger.debug(f"ORI Browser GET: {url}")
            with Timer() as t:
                await page_obj.goto(url, timeout=60000)
            await page_obj.wait_for_load_state("domcontentloaded", timeout=30000)
            await asyncio.sleep(2)

            # Get column headers
            headers = await page_obj.query_selector_all("table thead th")
            header_names = [await h.inner_text() for h in headers]

            # Get all data rows
            rows = await page_obj.query_selector_all("table tbody tr")

            if not rows:
                logger.debug(f"No results for book/page: {book}/{page}")
                return []

            results = []
            for row in rows:
                cells = await row.query_selector_all("td")
                if len(cells) >= 4:
                    data = {}
                    for i, cell in enumerate(cells):
                        if i < len(header_names):
                            data[header_names[i]] = (await cell.inner_text()).strip()

                    # Normalize to standard field names
                    normalized = {
                        "person_type": data.get("ORI - Person Type", ""),
                        "name": data.get("Name", ""),
                        "record_date": data.get("Recording Date Time", ""),
                        "doc_type": data.get("ORI - Doc Type", ""),
                        "book_type": data.get("Book Type", ""),
                        "book_num": data.get("Book #", ""),
                        "page_num": data.get("Page #", ""),
                        "legal": data.get("Legal Description", ""),
                        "instrument": data.get("Instrument #", ""),
                    }
                    results.append(normalized)

            log_search(
                source="ORI_CQID",
                query=f"book_page:{book}/{page}",
                results_raw=len(results),
                duration_ms=t.ms,
            )
            return results

        except Exception as e:
            logger.error(f"Error searching ORI by book/page {book}/{page}: {e}")
            return []
        finally:
            await context.close()

    def search_by_book_page_sync(
        self, book: str, page: str, book_type: str = "OR", headless: bool = True, timeout: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Synchronous wrapper for search_by_book_page_browser.

        Args:
            book: Book number
            page: Page number
            book_type: Book type (default "OR")
            headless: Run browser in headless mode
            timeout: Timeout in seconds

        Returns:
            List of document records
        """
        import asyncio as aio

        async def _search():
            return await self.search_by_book_page_browser(book, page, book_type, headless)

        try:
            loop = aio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            # We're in an async context, need to use a thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(lambda: aio.run(_search()))
                return future.result(timeout=timeout)
        else:
            return aio.run(_search())

    def _execute_search(self, payload: Dict) -> List[Dict[str, Any]]:
        try:
            # Check if we're in cool-down mode
            if self.api_cooled_down:
                if time.time() < self.api_cooldown_until:
                    remaining = int(self.api_cooldown_until - time.time())
                    logger.debug(f"API in cool-down mode ({remaining}s remaining), skipping request")
                    return []
                # Cool-down expired, reset
                logger.info("API cool-down period expired, resuming API requests")
                self.api_cooled_down = False
                self.consecutive_api_errors = 0

            # Back off on ANY error (threshold lowered to 1) with exponential backoff
            if self.consecutive_api_errors >= 1 and (time.time() - self.last_api_error_ts) < 300:
                # Exponential backoff: 2^errors seconds, capped at 30s
                backoff = min(30, 2 ** self.consecutive_api_errors)
                # Add jitter (±25%)
                jitter = backoff * random.uniform(-0.25, 0.25)  # noqa: S311
                backoff = backoff + jitter
                logger.warning(f"Exponential backoff {backoff:.1f}s due to ORI API errors ({self.consecutive_api_errors} consecutive)")
                time.sleep(backoff)

            # Log the API URL and payload
            logger.info(f"ORI API POST: {self.SEARCH_URL}")
            logger.debug(f"ORI API payload: {payload}")

            response = self.session.post(
                self.SEARCH_URL,
                headers=self.HEADERS,
                json=payload,
                timeout=60
            )
            response.raise_for_status()
            data = response.json()
            # Reset error counters on success
            self.consecutive_api_errors = 0
            self.last_api_error_ts = 0.0
            return data.get("ResultList", [])
        except requests.HTTPError as e:
            status = e.response.status_code if e.response else None
            self.consecutive_api_errors += 1
            self.last_api_error_ts = time.time()

            # Enter cool-down mode after threshold
            if self.consecutive_api_errors >= self.API_COOLDOWN_THRESHOLD:
                self.api_cooled_down = True
                self.api_cooldown_until = time.time() + self.API_COOLDOWN_DURATION
                logger.warning(f"ORI API entering cool-down mode for {self.API_COOLDOWN_DURATION}s after {self.consecutive_api_errors} consecutive errors")

            # Rotate UA to reduce fingerprinting issues
            self.HEADERS["User-Agent"] = random.choice(USER_AGENTS)  # noqa: S311
            logger.error(f"Error searching ORI (status={status}, consecutive={self.consecutive_api_errors}): {e}")
            return []
        except Exception as e:
            self.consecutive_api_errors += 1
            self.last_api_error_ts = time.time()

            # Enter cool-down mode after threshold
            if self.consecutive_api_errors >= self.API_COOLDOWN_THRESHOLD:
                self.api_cooled_down = True
                self.api_cooldown_until = time.time() + self.API_COOLDOWN_DURATION
                logger.warning(f"ORI API entering cool-down mode for {self.API_COOLDOWN_DURATION}s after {self.consecutive_api_errors} consecutive errors")

            logger.error(f"Error searching ORI (consecutive={self.consecutive_api_errors}): {e}")
            return []

    def download_pdf(self, doc: Dict, output_dir: Path, prefer_browser: bool = False) -> Optional[Path]:
        """
        Download PDF for a document.

        Args:
            doc: Document dictionary. Can have ID field from API or just Instrument from browser.
            output_dir: Directory to save PDF
            prefer_browser: If True, skip API lookup and use browser-based download directly.
                           Useful when docs came from browser search or API is in cooldown.

        Returns:
            Path to downloaded PDF or None if failed
        """
        doc_id = doc.get("ID")
        instrument = doc.get("Instrument") or doc.get("instrument", "unknown")
        doc_type = (doc.get("DocType") or doc.get("doc_type") or "UNKNOWN")

        # Helper to format record_date for filename
        def format_record_date(raw_date) -> str:
            if not raw_date:
                return ""
            try:
                if isinstance(raw_date, (int, float)):
                    return datetime.fromtimestamp(raw_date, tz=UTC).strftime("%Y%m%d")
                if isinstance(raw_date, str) and raw_date:
                    try:
                        parsed = datetime.strptime(raw_date.split()[0], "%m/%d/%Y")
                        return parsed.strftime("%Y%m%d")
                    except Exception:
                        return raw_date.replace("/", "").replace(" ", "")[:8]
            except Exception as exc:
                logger.debug("Could not format record date %s: %s", raw_date, exc)
            return ""

        record_date = format_record_date(doc.get("RecordDate") or doc.get("record_date"))

        # If API is in cooldown or prefer_browser is set, go directly to browser download
        if (prefer_browser or self.api_cooled_down) and instrument and instrument != "unknown":
            logger.debug(f"Using browser download for {instrument} (prefer_browser={prefer_browser}, api_cooled_down={self.api_cooled_down})")
            return self.download_pdf_browser_sync(instrument, output_dir, doc_type, headless=True, record_date=record_date)

        # If no ID, try to look it up via API using instrument number
        if not doc_id and instrument and instrument != "unknown":
            logger.debug(f"Looking up document ID for instrument {instrument}")
            api_results = self.search_by_instrument(instrument)
            if api_results:
                doc_id = api_results[0].get("ID")
                # Also get the record date from API result for filename
                if not doc.get("RecordDate") and api_results[0].get("RecordDate"):
                    doc["RecordDate"] = api_results[0]["RecordDate"]
                logger.debug(f"Found document ID {doc_id} for instrument {instrument}")

        if not doc_id:
            # Try to get ID by fetching document viewer page directly
            doc_id = self._fetch_document_id_via_viewer(instrument)

        # If still no doc_id, fall back to browser download
        if not doc_id:
            logger.debug(f"Could not find document ID for {instrument} via API, trying browser download")
            # Re-format record_date in case it was updated from API lookup
            record_date = format_record_date(doc.get("RecordDate") or doc.get("record_date"))
            return self.download_pdf_browser_sync(instrument, output_dir, doc_type, headless=True, record_date=record_date)

        # Support both uppercase (API) and lowercase (browser) field names
        doc_type = (doc.get("DocType") or doc.get("doc_type") or "UNKNOWN")
        doc_type = doc_type.replace("(", "").replace(")", "").replace(" ", "_")

        try:
            # Handle both API format (timestamp) and browser format (string)
            raw_date = doc.get("RecordDate") or doc.get("record_date")
            if isinstance(raw_date, (int, float)):
                record_date = datetime.fromtimestamp(raw_date, tz=UTC).strftime("%Y%m%d")
            elif isinstance(raw_date, str) and raw_date:
                # Browser format is like "11/25/2024 12:03 PM"
                try:
                    parsed = datetime.strptime(raw_date.split()[0], "%m/%d/%Y")
                    record_date = parsed.strftime("%Y%m%d")
                except Exception:
                    record_date = raw_date.replace("/", "").replace(" ", "")[:8]
            else:
                record_date = "unknown"
        except Exception as exc:
            logger.debug("Could not parse record date %s: %s", raw_date, exc)
            record_date = "unknown"

        pdf_url = f"{self.PDF_URL}/{quote(str(doc_id))}"
        filename = f"{record_date}_{doc_type}_{instrument}.pdf"
        filepath = output_dir / filename

        if filepath.exists():
            return filepath

        headers = self.HEADERS.copy()
        headers["Accept"] = "application/pdf,*/*"

        try:
            response = self.session.get(pdf_url, headers=headers, timeout=30)
            if response.status_code == 200 and response.content[:4] == b"%PDF":
                with open(filepath, "wb") as f:
                    f.write(response.content)
                return filepath
        except Exception as e:
            logger.error(f"Error downloading PDF {doc_id}: {e}")

        return None

    def _fetch_document_id_via_viewer(self, instrument: str) -> Optional[str]:
        """
        Fetch document ID by searching via API with a legal description that matches.

        Since the PAVDirectSearch page requires JavaScript execution, we instead
        use the API's legal description search to find documents with IDs.

        Args:
            instrument: The instrument number

        Returns:
            Document ID string if found, None otherwise
        """
        # First try a broader API approach: search using instrument number pattern
        # The API doesn't support direct instrument search, but we can search
        # with minimal criteria and filter by instrument

        try:
            # Try to find via book/page if we have it (from prior DB lookup)
            # For now, just try a generic legal search that might catch it
            # This is a fallback - ideally we'd have the legal description

            # Parse year from instrument (first 4 digits are typically year)
            year = instrument[:4] if len(instrument) >= 4 else "2024"

            # Try searching with a very broad date range for that year
            payload = {
                "RecordDateBegin": f"01/01/{year}",
                "RecordDateEnd": f"12/31/{year}",
                # No Legal filter - just search by date range
                # This will hit the 25 result limit but might find our doc
            }

            results = self._execute_search(payload)

            # Filter by instrument number
            for result in results:
                if str(result.get("Instrument")) == str(instrument):
                    doc_id = result.get("ID")
                    if doc_id:
                        logger.debug(f"Found document ID via broad search: {doc_id}")
                        return doc_id

            return None
        except Exception as e:
            logger.debug(f"Error fetching document ID for {instrument}: {e}")
            return None

    async def _ensure_browser(self, headless: bool = True):
        """Ensure browser is initialized and running.

        Only ensures the browser process is started. Contexts are created
        per-search via _create_isolated_context() for thread safety.

        Args:
            headless: Run in headless mode
        """
        if self.browser is None:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=headless,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-web-security',
                    '--disable-features=IsolateOrigins,site-per-process'
                ]
            )
            logger.info("Browser launched")

    async def _create_isolated_context(self):
        """Create an isolated browser context for a single search operation.

        Each concurrent search gets its own context, preventing race conditions
        where one search's timeout/retry would invalidate another's pages.

        Returns:
            A new BrowserContext with randomized fingerprint

        Raises:
            RuntimeError: If browser is not initialized
        """
        if self.browser is None:
            raise RuntimeError("Browser not initialized. Call _ensure_browser first.")

        # Get randomized browser fingerprint for this context
        config = get_random_browser_config()
        logger.debug(f"Creating isolated context: UA={config['user_agent'][:50]}..., viewport={config['viewport']}")

        return await self.browser.new_context(
            user_agent=config['user_agent'],
            viewport=config['viewport'],
            locale=config['locale'],
            timezone_id=config['timezone_id']
        )

    async def close_browser(self):
        """Close the persistent browser."""
        if self.browser:
            await self.browser.close()
            self.browser = None
        if self.playwright:
            await self.playwright.stop()
            self.playwright = None
        if self.context:
            self.context = None

    async def search_by_legal_browser(self, legal_desc: str, headless: bool = True, force_new_context: bool = False) -> List[Dict[str, Any]]:
        """
        Search ORI by legal description using browser-based CQID=321 endpoint.
        This returns ALL results without the 25-record API limit.

        Args:
            legal_desc: Legal description to search (e.g., "L 198 TUSCANY*" with wildcard)
            headless: Run browser in headless mode
            force_new_context: Deprecated, ignored (each search now uses isolated context)

        Returns:
            List of document records with full metadata
        """
        # Use CQID=321 for legal description search
        url = f"https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=321&OBKey__1011_1={quote(legal_desc)}"
        logger.info(f"Searching ORI by legal: {legal_desc}")

        # Ensure browser is running (shared across searches)
        await self._ensure_browser(headless)

        max_attempts = 2
        for attempt in range(1, max_attempts + 1):
            # Create isolated context for this search attempt
            context = await self._create_isolated_context()
            page = None

            debug_dir = Path("logs/ori_browser_debug")
            debug_dir.mkdir(parents=True, exist_ok=True)
            safe_term = "".join(ch if ch.isalnum() else "_" for ch in legal_desc)[:50]
            timestamp = now_utc().strftime("%Y%m%d_%H%M%S")
            screenshot_path = debug_dir / f"legal_{safe_term}_{timestamp}.png"

            try:
                # Create new page for this search with stealth
                page = await context.new_page()
                await apply_stealth(page)

                logger.info(f"ORI Browser GET: {url}")
                await page.goto(url, timeout=60000)
                # Use domcontentloaded instead of networkidle (faster, less prone to hanging)
                await page.wait_for_load_state("domcontentloaded", timeout=30000)
                await page.wait_for_selector("table tbody tr", timeout=30000)
                await asyncio.sleep(2)  # Give table time to render fully

                # Get column headers
                headers = await page.query_selector_all("table thead th")
                header_names = [await h.inner_text() for h in headers]

                # Get all data rows
                rows = await page.query_selector_all("table tbody tr")

                if not rows:
                    await page.screenshot(path=str(screenshot_path), full_page=True)
                    logger.warning(f"No table rows found for legal '{legal_desc}'. Screenshot saved to {screenshot_path}")
                    return []

                results = []
                for row in rows:
                    cells = await row.query_selector_all("td")
                    if len(cells) >= 4:
                        data = {}
                        for i, cell in enumerate(cells):
                            if i < len(header_names):
                                data[header_names[i]] = (await cell.inner_text()).strip()

                        # Normalize to standard field names
                        normalized = {
                            "person_type": data.get("ORI - Person Type", ""),
                            "name": data.get("Name", ""),
                            "record_date": data.get("Recording Date Time", ""),
                            "doc_type": data.get("ORI - Doc Type", ""),
                            "book_type": data.get("Book Type", ""),
                            "book_num": data.get("Book #", ""),
                            "page_num": data.get("Page #", ""),
                            "legal": data.get("Legal Description", ""),
                            "instrument": data.get("Instrument #", ""),
                        }
                        results.append(normalized)

                logger.info(f"Found {len(results)} records for legal: {legal_desc}")
                return results

            except PlaywrightTimeoutError as e:
                if page:
                    await page.screenshot(path=str(screenshot_path), full_page=True)
                logger.warning(
                    f"Timeout while searching ORI by legal '{legal_desc}' (attempt {attempt}/{max_attempts}): "
                    f"{e}. Screenshot: {screenshot_path}"
                )
                if attempt < max_attempts:
                    continue
                # Fallback to API search (limited results but better than none)
                try:
                    fallback = self.search_by_legal(legal_desc)
                    if fallback:
                        logger.warning(
                            f"ORI browser timed out; fallback API returned {len(fallback)} results for {legal_desc}"
                        )
                    return fallback or []
                except Exception as api_exc:
                    logger.warning(f"ORI API fallback failed for {legal_desc}: {api_exc}")
                    return []
            except Exception as e:
                if page:
                    await page.screenshot(path=str(screenshot_path), full_page=True)
                logger.warning(f"Error searching ORI by legal '{legal_desc}': {e}. Screenshot: {screenshot_path}")
                if attempt < max_attempts:
                    continue
                return []
            finally:
                # Close the isolated context (and its page) - won't affect other concurrent searches
                await context.close()

        # Should not reach here, but satisfy type checker
        return []

    def search_by_legal_sync(self, legal_desc: str, headless: bool = True, timeout: int = 60) -> List[Dict[str, Any]]:
        """Synchronous wrapper for search_by_legal_browser with timeout protection.

        Uses subprocess isolation to ensure browser cleanup on timeout.
        """
        import subprocess
        import json
        import sys

        # Use subprocess for true isolation - if it times out, we can kill it
        script = f'''
import asyncio
import json
import sys
sys.path.insert(0, "{Path(__file__).parent.parent.parent}")
from src.scrapers.ori_api_scraper import ORIApiScraper

async def main():
    scraper = ORIApiScraper()
    try:
        results = await scraper.search_by_legal_browser({legal_desc!r}, {headless})
        print(json.dumps(results))
    finally:
        await scraper.close_browser()

asyncio.run(main())
'''
        try:
            result = subprocess.run(
                [sys.executable, "-c", script],
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=str(Path(__file__).parent.parent.parent),
                check=False,
            )
            if result.returncode == 0 and result.stdout.strip():
                return json.loads(result.stdout.strip())
            if result.stderr:
                logger.warning(f"Browser search stderr: {result.stderr[:500]}")
            return []
        except subprocess.TimeoutExpired:
            logger.error(f"Browser search subprocess timed out after {timeout}s for: {legal_desc}")
            # Subprocess is automatically killed on timeout
            return []
        except json.JSONDecodeError as exc:
            logger.warning(f"Failed to parse browser search result: {exc}")
            return []
        except Exception as exc:
            logger.error(f"Browser search failed for {legal_desc}: {exc}")
            return []

    async def search_by_party_browser(self, party_name: str, headless: bool = True) -> List[Dict[str, Any]]:
        """
        Search ORI by party name using browser-based CQID=326 endpoint.
        This returns ALL results without the 25-record API limit.

        Unlike legal description search, this returns documents where the party
        appears as EITHER Party 1 (grantor) OR Party 2 (grantee).

        Args:
            party_name: Party name to search (e.g., "BARGAMIN KRISTEN*" with wildcard)
            headless: Run browser in headless mode

        Returns:
            List of document records with party type information
        """
        # Use CQID=326 for party name search
        url = f"https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=326&OBKey__486_1={quote(party_name)}"
        logger.info(f"Searching ORI by party name: {party_name}")

        # Ensure browser is running (shared across searches)
        await self._ensure_browser(headless)

        # Create isolated context for this search
        context = await self._create_isolated_context()

        try:
            # Create new page for this search with stealth
            page = await context.new_page()
            await apply_stealth(page)

            logger.info(f"ORI Browser GET: {url}")
            await page.goto(url, timeout=60000)
            # Use domcontentloaded instead of networkidle (faster, less prone to hanging)
            await page.wait_for_load_state("domcontentloaded", timeout=30000)
            await asyncio.sleep(3)  # Give table time to render

            # Get column headers
            headers = await page.query_selector_all("table thead th")
            header_names = [await h.inner_text() for h in headers]

            # Get all data rows
            rows = await page.query_selector_all("table tbody tr")

            results = []
            for row in rows:
                cells = await row.query_selector_all("td")
                if len(cells) >= 4:
                    data = {}
                    for i, cell in enumerate(cells):
                        if i < len(header_names):
                            data[header_names[i]] = (await cell.inner_text()).strip()

                    # Normalize to standard field names
                    normalized = {
                        "person_type": data.get("ORI - Person Type", ""),
                        "name": data.get("Name", ""),
                        "record_date": data.get("Recording Date Time", ""),
                        "doc_type": data.get("ORI - Doc Type", ""),
                        "book_type": data.get("Book Type", ""),
                        "book_num": data.get("Book #", ""),
                        "page_num": data.get("Page #", ""),
                        "legal": data.get("Legal Description", ""),
                        "instrument": data.get("Instrument #", ""),
                    }
                    results.append(normalized)

            logger.info(f"Found {len(results)} records for party: {party_name}")
            return results

        except Exception as e:
            logger.error(f"Error searching ORI by party: {e}")
            return []
        finally:
            # Close the isolated context - won't affect other concurrent searches
            await context.close()

    def search_by_party_browser_sync(self, party_name: str, headless: bool = True, timeout: int = 60) -> List[Dict[str, Any]]:
        """Synchronous wrapper for search_by_party_browser with timeout protection.

        Uses subprocess isolation to ensure browser cleanup on timeout.
        """
        import subprocess
        import json
        import sys

        script = f'''
import asyncio
import json
import sys
sys.path.insert(0, "{Path(__file__).parent.parent.parent}")
from src.scrapers.ori_api_scraper import ORIApiScraper

async def main():
    scraper = ORIApiScraper()
    try:
        results = await scraper.search_by_party_browser({party_name!r}, {headless})
        print(json.dumps(results))
    finally:
        await scraper.close_browser()

asyncio.run(main())
'''
        try:
            result = subprocess.run(
                [sys.executable, "-c", script],
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=str(Path(__file__).parent.parent.parent),
                check=False,
            )
            if result.returncode == 0 and result.stdout.strip():
                return json.loads(result.stdout.strip())
            if result.stderr:
                logger.warning(f"Party browser search stderr: {result.stderr[:500]}")
            return []
        except subprocess.TimeoutExpired:
            logger.error(f"Party browser search subprocess timed out after {timeout}s for: {party_name}")
            return []
        except json.JSONDecodeError as exc:
            logger.warning(f"Failed to parse party browser search result: {exc}")
            return []
        except Exception as exc:
            logger.error(f"Party browser search failed for {party_name}: {exc}")
            return []

    async def search_by_party_and_instrument_browser(self, party_name: str, instrument: str,
                                                       headless: bool = True) -> List[Dict[str, Any]]:
        """
        Search ORI by party name AND instrument number using combined OBKey parameters.

        This is more targeted than searching by party alone - it filters to a specific
        instrument while using the party name search interface.

        Known OBKey parameters:
        - OBKey__486_1: Party name (CQID 326)
        - OBKey__1006_1: Instrument number (CQID 320)
        - OBKey__1011_1: Legal description text (CQID 321)
        - OBKey__573_1: Book (CQID 319)
        - OBKey__1049_1: Page (CQID 319)
        - OBKey__1530_1: Book type flag (CQID 319)

        Args:
            party_name: Party name to search
            instrument: Instrument number to filter by
            headless: Run browser in headless mode

        Returns:
            List of document records
        """
        # Use CQID=326 with both party name and instrument number
        url = (f"https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html"
               f"?CQID=326&OBKey__486_1={quote(party_name)}&OBKey__1006_1={quote(instrument)}")
        logger.info(f"Searching ORI by party+instrument: {party_name} / {instrument}")

        # Ensure browser is running (shared across searches)
        await self._ensure_browser(headless)

        # Create isolated context for this search
        context = await self._create_isolated_context()

        try:
            # Create new page for this search with stealth
            page = await context.new_page()
            await apply_stealth(page)

            logger.info(f"ORI Browser GET: {url}")
            await page.goto(url, timeout=60000)
            await page.wait_for_load_state("domcontentloaded", timeout=30000)
            await asyncio.sleep(3)

            # Get column headers
            headers = await page.query_selector_all("table thead th")
            header_names = [await h.inner_text() for h in headers]

            # Get all data rows
            rows = await page.query_selector_all("table tbody tr")

            results = []
            for row in rows:
                cells = await row.query_selector_all("td")
                if len(cells) >= 4:
                    data = {}
                    for i, cell in enumerate(cells):
                        if i < len(header_names):
                            data[header_names[i]] = (await cell.inner_text()).strip()

                    normalized = {
                        "person_type": data.get("ORI - Person Type", ""),
                        "name": data.get("Name", ""),
                        "record_date": data.get("Recording Date Time", ""),
                        "doc_type": data.get("ORI - Doc Type", ""),
                        "book_type": data.get("Book Type", ""),
                        "book_num": data.get("Book #", ""),
                        "page_num": data.get("Page #", ""),
                        "legal": data.get("Legal Description", ""),
                        "instrument": data.get("Instrument #", ""),
                    }
                    results.append(normalized)

            logger.info(f"Found {len(results)} records for party+instrument: {party_name} / {instrument}")
            return results

        except Exception as e:
            logger.error(f"Error searching ORI by party+instrument: {e}")
            return []
        finally:
            # Close the isolated context - won't affect other concurrent searches
            await context.close()

    async def find_party2_for_instrument_async(self, grantor_name: str, instrument: str) -> Optional[str]:
        """
        Search CQID 326 to find Party 2 (grantee) for a specific instrument.

        First tries combined party+instrument search (more targeted).
        Falls back to full party search if combined search returns no results.

        Args:
            grantor_name: Name of Party 1 (grantor) to search
            instrument: Target instrument number

        Returns:
            Party 2 name if found, None if not indexed
        """
        # Add wildcard for partial matches
        search_name = grantor_name.strip()
        if not search_name.endswith("*"):
            search_name += "*"

        # Try combined party+instrument search first (more targeted)
        try:
            results = await self.search_by_party_and_instrument_browser(search_name, instrument)
            for row in results:
                if "PARTY 2" in row.get("person_type", ""):
                    logger.info(f"Found Party 2 via combined search: {row.get('name')}")
                    return row.get("name")
        except Exception as e:
            logger.warning(f"Combined party+instrument search failed: {e}")

        # Fall back to full party search
        try:
            results = await self.search_by_party_browser(search_name)
            for row in results:
                if row.get("instrument") == instrument and "PARTY 2" in row.get("person_type", ""):
                    return row.get("name")
        except Exception as e:
            logger.warning(f"Full party search failed: {e}")

        return None

    def find_party2_for_instrument(self, grantor_name: str, instrument: str) -> Optional[str]:
        """Synchronous wrapper for find_party2_for_instrument_async."""
        try:
            asyncio.get_running_loop()
            # Already in async context - use thread
            import concurrent.futures

            def run_in_new_loop():
                return asyncio.run(self.find_party2_for_instrument_async(grantor_name, instrument))

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_in_new_loop)
                return future.result()
        except RuntimeError:
            # No event loop running
            return asyncio.run(self.find_party2_for_instrument_async(grantor_name, instrument))

    def get_property_documents(self, folio: str, legal1: str, legal2: str) -> List[Dict[str, Any]]:
        """
        Get all ORI documents for a property using its legal description.

        Args:
            folio: Property folio number
            legal1: First part of legal (e.g., "TUSCANY SUBDIVISION AT TAMPA PALMS")
            legal2: Second part of legal (e.g., "LOT 198")

        Returns:
            List of document records grouped by unique instruments
        """
        # Convert LOT X to L X format used by ORI
        lot_part = legal2.replace("LOT ", "L ").replace("Lot ", "L ")

        # Extract subdivision short name (first few words)
        subdiv = " ".join(legal1.split()[:2])  # e.g., "TUSCANY SUBDIVISION" -> "TUSCANY SUBDIVISION"
        if "SUBDIVISION" in subdiv.upper():
            subdiv = subdiv.split()[0]  # Just use first word like "TUSCANY"

        # Build search term with wildcard
        search_term = f"{lot_part} {subdiv}*"

        logger.info(f"Searching ORI for folio {folio} using: {search_term}")

        # Search using browser method
        results = self.search_by_legal_sync(search_term, headless=True)

        # Group by instrument number to get unique documents
        by_instrument = {}
        for r in results:
            inst = r.get("instrument", "")
            if inst and inst not in by_instrument:
                by_instrument[inst] = {
                    "instrument": inst,
                    "doc_type": r.get("doc_type", ""),
                    "record_date": r.get("record_date", ""),
                    "legal": r.get("legal", ""),
                    "book": r.get("book_num", ""),
                    "page": r.get("page_num", ""),
                    "parties": []
                }
            if inst:
                by_instrument[inst]["parties"].append({
                    "type": r.get("person_type", ""),
                    "name": r.get("name", "")
                })

        return list(by_instrument.values())

    async def download_pdf_browser(self, instrument: str, output_dir: Path, doc_type: str = "UNKNOWN", headless: bool = True, fresh_context: bool = False, record_date: str = "") -> Optional[Path]:
        """
        Download PDF for a document using browser-based approach.

        Uses CQID=320 instrument search to find the document, then downloads via the Document API.

        Args:
            instrument: Instrument number to download
            output_dir: Directory to save PDF
            doc_type: Document type for filename
            headless: Run browser in headless mode
            fresh_context: Deprecated, ignored (each download now uses isolated context)
            record_date: Recording date for filename (YYYYMMDD format)

        Returns:
            Path to downloaded PDF or None if failed
        """
        import urllib.parse

        # Clean up doc type for filename
        doc_type_clean = doc_type.replace("(", "").replace(")", "").replace(" ", "_").replace("/", "_").replace("\\", "_")
        date_prefix = record_date if record_date else "unknown"
        filename = f"{date_prefix}_{doc_type_clean}_{instrument}.pdf"
        filepath = output_dir / filename

        # Check if already exists
        if filepath.exists():
            logger.debug(f"PDF already exists: {filepath}")
            return filepath

        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)

        # Anti-detection: Random delay before request
        await random_delay_async(2.0, 5.0)

        # Ensure browser is running (shared across downloads)
        await self._ensure_browser(headless)

        # Create isolated context for this download
        context = await self._create_isolated_context()

        # Use CQID=320 for instrument search (OBKey__1006_1 is the instrument number parameter)
        url = f"https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=320&OBKey__1006_1={instrument}"
        logger.info(f"Downloading PDF for instrument {instrument}...")

        try:
            page = await context.new_page()
            await apply_stealth(page)

            # Set up response listener to capture document ID
            doc_id_future = asyncio.get_event_loop().create_future()

            async def handle_response(response):
                if "KeywordSearch" in response.url and not doc_id_future.done():
                    try:
                        json_data = await response.json()
                        if "Data" in json_data and len(json_data["Data"]) > 0:
                            doc_id = json_data["Data"][0].get("ID")
                            if doc_id:
                                doc_id_future.set_result(doc_id)
                    except Exception as exc:
                        logger.debug("Failed to parse OnBase response for %s: %s", instrument, exc)

            page.on("response", handle_response)

            # Navigate to instrument search
            logger.info(f"ORI Browser GET: {url}")
            await page.goto(url, timeout=60000)
            await page.wait_for_load_state("domcontentloaded", timeout=30000)

            # Wait for document ID from API response
            try:
                doc_id = await asyncio.wait_for(doc_id_future, timeout=10.0)
            except TimeoutError:
                logger.warning(f"Could not find Document ID for instrument {instrument}")
                return None

            logger.debug(f"Found document ID: {doc_id}")

            # Download the PDF
            encoded_id = urllib.parse.quote(str(doc_id))
            download_url = f"https://publicaccess.hillsclerk.com/PAVDirectSearch/api/Document/{encoded_id}/?OverlayMode=View"
            logger.info(f"ORI Browser PDF download: {download_url}")

            async with page.expect_download(timeout=60000) as download_info:
                await page.evaluate(f"window.location.href = '{download_url}'")

            download = await download_info.value

            # Save the PDF
            temp_path = await download.path()
            with open(temp_path, "rb") as f:
                pdf_bytes = f.read()

            # Verify it's a valid PDF
            if pdf_bytes[:4] != b"%PDF":
                logger.warning(f"Downloaded file is not a valid PDF for {instrument}")
                return None

            with open(filepath, "wb") as f:
                f.write(pdf_bytes)

            logger.success(f"Downloaded PDF: {filepath.name} ({len(pdf_bytes)} bytes)")

            # Anti-detection: Random delay after successful download
            await random_delay_async(1.0, 3.0)

            return filepath

        except Exception as e:
            logger.error(f"Error downloading PDF for {instrument}: {e}")
            return None
        finally:
            # Close the isolated context - won't affect other concurrent operations
            await context.close()

    def download_pdf_browser_sync(self, instrument: str, output_dir: Path, doc_type: str = "UNKNOWN", headless: bool = True, fresh_context: bool = False, timeout: int = 90, record_date: str = "") -> Optional[Path]:
        """Synchronous wrapper for download_pdf_browser with timeout protection.

        Uses subprocess isolation to ensure browser cleanup on timeout.

        Args:
            instrument: Instrument number to download
            output_dir: Directory to save PDF
            doc_type: Document type for filename
            headless: Run browser in headless mode
            fresh_context: If True, create new browser context with fresh fingerprint
            timeout: Timeout in seconds (default 90s for PDF downloads)
            record_date: Recording date for filename (YYYYMMDD format)
        """
        import subprocess
        import sys

        # Ensure output_dir exists
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Use subprocess for true isolation - if it times out, we can kill it
        script = f'''
import asyncio
import sys
from pathlib import Path
sys.path.insert(0, "{Path(__file__).parent.parent.parent}")
from src.scrapers.ori_api_scraper import ORIApiScraper

async def main():
    scraper = ORIApiScraper()
    try:
        result = await scraper.download_pdf_browser({instrument!r}, Path({str(output_dir)!r}), {doc_type!r}, {headless}, {fresh_context}, {record_date!r})
        if result:
            print(str(result))
        else:
            print("")
    finally:
        await scraper.close_browser()

asyncio.run(main())
'''
        try:
            result = subprocess.run(
                [sys.executable, "-c", script],
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=str(Path(__file__).parent.parent.parent),
                check=False,
            )
            if result.returncode == 0 and result.stdout.strip():
                return Path(result.stdout.strip())
            if result.stderr:
                logger.warning(f"PDF download stderr: {result.stderr[:500]}")
            return None
        except subprocess.TimeoutExpired:
            logger.error(f"PDF download subprocess timed out after {timeout}s for: {instrument}")
            # Subprocess is automatically killed on timeout
            return None
        except Exception as exc:
            logger.error(f"PDF download failed for {instrument}: {exc}")
            return None
