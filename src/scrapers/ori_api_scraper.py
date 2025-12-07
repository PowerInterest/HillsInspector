import requests
import asyncio
from datetime import datetime
from typing import List, Dict, Optional, Any
from pathlib import Path
from urllib.parse import quote
from loguru import logger
from playwright.async_api import async_playwright

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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
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

    def search_by_legal(self, legal_description: str, start_date: str = "01/01/1900") -> List[Dict[str, Any]]:
        """
        Search for documents by legal description.

        Args:
            legal_description: Text to search in legal description (e.g. subdivision name)
            start_date: Start date for search (MM/DD/YYYY)

        Returns:
            List of document dictionaries
        """
        payload = {
            "DocType": self.TITLE_DOC_TYPES,
            "RecordDateBegin": start_date,
            "RecordDateEnd": datetime.now().strftime("%m/%d/%Y"),
            "Legal": ["CONTAINS", legal_description],
        }
        return self._execute_search(payload)

    def search_by_party(self, party_name: str, start_date: str = "01/01/1900") -> List[Dict[str, Any]]:
        """
        Search for documents by party name.

        Args:
            party_name: Name of party (Last First Middle or Company Name)
            start_date: Start date for search (MM/DD/YYYY)
        """
        payload = {
            "DocType": self.TITLE_DOC_TYPES,
            "RecordDateBegin": start_date,
            "RecordDateEnd": datetime.now().strftime("%m/%d/%Y"),
            "Party": party_name,
        }
        return self._execute_search(payload)

    def search_by_instrument(self, instrument: str) -> List[Dict[str, Any]]:
        """
        Search for documents by instrument number.

        Args:
            instrument: Instrument number (e.g., "2024478600")

        Returns:
            List of document dictionaries (usually 0 or 1)
        """
        payload = {
            "DocType": self.TITLE_DOC_TYPES,
            "RecordDateBegin": "01/01/1900",
            "RecordDateEnd": datetime.now().strftime("%m/%d/%Y"),
            "Instrument": instrument,
        }
        return self._execute_search(payload)

    def _execute_search(self, payload: Dict) -> List[Dict[str, Any]]:
        try:
            response = self.session.post(
                self.SEARCH_URL,
                headers=self.HEADERS,
                json=payload,
                timeout=60
            )
            response.raise_for_status()
            data = response.json()
            return data.get("ResultList", [])
        except Exception as e:
            logger.error(f"Error searching ORI: {e}")
            return []

    def download_pdf(self, doc: Dict, output_dir: Path) -> Optional[Path]:
        """
        Download PDF for a document.

        Args:
            doc: Document dictionary with ID field from API search results
            output_dir: Directory to save PDF

        Returns:
            Path to downloaded PDF or None if failed
        """
        doc_id = doc.get("ID")
        if not doc_id:
            return None

        instrument = doc.get("Instrument", "unknown")
        doc_type = doc.get("DocType", "UNKNOWN").replace("(", "").replace(")", "").replace(" ", "_")

        try:
            record_date = datetime.fromtimestamp(doc.get("RecordDate", 0)).strftime("%Y%m%d")
        except:
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

    async def _ensure_browser(self, headless: bool = True):
        """Ensure browser is initialized and running."""
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
            self.context = await self.browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
                timezone_id='America/New_York'
            )
            logger.info("Browser initialized and ready")

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

    async def search_by_legal_browser(self, legal_desc: str, headless: bool = True) -> List[Dict[str, Any]]:
        """
        Search ORI by legal description using browser-based CQID=321 endpoint.
        This returns ALL results without the 25-record API limit.

        Args:
            legal_desc: Legal description to search (e.g., "L 198 TUSCANY*" with wildcard)
            headless: Run browser in headless mode

        Returns:
            List of document records with full metadata
        """
        # Use CQID=321 for legal description search
        url = f"https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=321&OBKey__1011_1={quote(legal_desc)}"
        logger.info(f"Searching ORI by legal: {legal_desc}")

        # Ensure browser is running
        await self._ensure_browser(headless)

        # Create new page for this search
        page = await self.context.new_page()

        try:
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

            logger.info(f"Found {len(results)} records for legal: {legal_desc}")
            return results

        except Exception as e:
            logger.error(f"Error searching ORI by legal: {e}")
            return []
        finally:
            await page.close()  # Close the page, but keep browser alive

    def search_by_legal_sync(self, legal_desc: str, headless: bool = True) -> List[Dict[str, Any]]:
        """Synchronous wrapper for search_by_legal_browser."""
        try:
            # Check if there's already a running event loop
            asyncio.get_running_loop()
            # If we're in an async context, we need to run in a new thread
            import concurrent.futures

            def run_in_new_loop():
                return asyncio.run(self.search_by_legal_browser(legal_desc, headless))

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_in_new_loop)
                return future.result()
        except RuntimeError:
            # No event loop running, safe to use asyncio.run()
            return asyncio.run(self.search_by_legal_browser(legal_desc, headless))

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

        # Ensure browser is running
        await self._ensure_browser(headless)

        # Create new page for this search
        page = await self.context.new_page()

        try:
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
            await page.close()  # Close the page, but keep browser alive

    def search_by_party_browser_sync(self, party_name: str, headless: bool = True) -> List[Dict[str, Any]]:
        """Synchronous wrapper for search_by_party_browser."""
        try:
            # Check if there's already a running event loop
            asyncio.get_running_loop()
            # If we're in an async context, we need to run in a new thread
            import concurrent.futures

            def run_in_new_loop():
                return asyncio.run(self.search_by_party_browser(party_name, headless))

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_in_new_loop)
                return future.result()
        except RuntimeError:
            # No event loop running, safe to use asyncio.run()
            return asyncio.run(self.search_by_party_browser(party_name, headless))

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

        # Ensure browser is running
        await self._ensure_browser(headless)

        # Create new page for this search
        page = await self.context.new_page()

        try:
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
            await page.close()

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
