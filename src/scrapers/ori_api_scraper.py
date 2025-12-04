import requests
import json
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

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            page = await browser.new_page()

            try:
                await page.goto(url, timeout=60000)
                await page.wait_for_load_state("networkidle")
                await asyncio.sleep(2)

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
                await browser.close()

    def search_by_legal_sync(self, legal_desc: str, headless: bool = True) -> List[Dict[str, Any]]:
        """Synchronous wrapper for search_by_legal_browser."""
        return asyncio.run(self.search_by_legal_browser(legal_desc, headless))

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
