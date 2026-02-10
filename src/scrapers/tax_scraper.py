
import asyncio
import re
import time
import urllib.parse
from typing import List, Optional
from dataclasses import dataclass
from loguru import logger
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from src.services.scraper_storage import ScraperStorage
from src.services.vision_service import VisionService
from src.models.property import TaxStatus, TaxCertificate
from src.utils.logging_utils import log_search


@dataclass
class TaxSearchResult:
    """Result from tax search."""
    account_number: str
    owner_name: str
    address: str
    city: str
    state: str
    zip_code: str


class TaxScraper:
    """
    Scraper for Hillsborough County tax information.

    Uses county-taxes.net (Grant Street Group) which doesn't have Cloudflare protection,
    unlike the official hillsborough.county-taxes.com which has Turnstile challenges.

    Also checks lienhub.com for tax lien certificate information.
    """
    # county-taxes.net works without Cloudflare challenges
    BASE_URL = "https://county-taxes.net/hillsborough/property-tax"
    LIENHUB_URL = "https://lienhub.com/county/hillsborough"

    def __init__(self, storage: Optional[ScraperStorage] = None):
        self.storage = storage or ScraperStorage()
        self.vision = VisionService()

    async def scrape_tax_status(self, parcel_id: str, property_address: Optional[str] = None) -> TaxStatus:
        """
        Scrape tax status including liens, amount due, and payment history.
        Returns a TaxStatus Pydantic model.
        """
        if not property_address:
            logger.warning(f"No property address provided for {parcel_id}, cannot search taxes")
            return TaxStatus()

        logger.info(f"Searching Tax Collector for: {property_address} (Parcel: {parcel_id})")
        search_started = time.perf_counter()
        result_found = False
        tax_status = TaxStatus(situs=property_address)

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(viewport={'width': 1280, 'height': 720})
            page = await context.new_page()

            # Apply stealth
            stealth = Stealth()
            await stealth.apply_stealth_async(page)

            try:
                # Try multiple search strategies in order
                search_queries = []

                # Strategy 1: Street address only (most common)
                search_address = self._normalize_address(property_address)
                if search_address:
                    search_queries.append(("address", search_address))

                # Strategy 2: Parcel ID / Folio (more reliable if available)
                if parcel_id and len(parcel_id) > 5:
                    search_queries.append(("parcel", parcel_id))

                found_results = False
                for search_type, search_term in search_queries:
                    encoded_term = urllib.parse.quote(search_term)
                    search_url = f"{self.BASE_URL}?search_query={encoded_term}"

                    logger.info(f"Navigating to {search_url} (search by {search_type})")
                    await page.goto(search_url, timeout=60000)
                    await page.wait_for_load_state("domcontentloaded")

                    # Wait for search results
                    try:
                        await page.wait_for_selector("button:has-text('View'), text=No bills or accounts matched, text=Page", timeout=15000)
                    except Exception:
                        logger.debug("Timeout waiting for search results selector, using fallback wait")
                        await asyncio.sleep(8)

                    # Check for "No results"
                    text = await page.inner_text("body")
                    if "No bills or accounts matched your search" not in text:
                        found_results = True
                        break
                    logger.debug(f"No results with {search_type} search: {search_term}")

                if not found_results:
                    logger.info(f"No tax records found for {property_address} (tried address and parcel ID)")
                    duration_ms = (time.perf_counter() - search_started) * 1000
                    log_search(
                        source="TAX",
                        query=property_address,
                        results_raw=0,
                        results_kept=0,
                        duration_ms=duration_ms,
                        parcel_id=parcel_id,
                    )
                    return tax_status

                # Wait for JavaScript redirect to complete (site auto-redirects to detail page on unique match)
                await asyncio.sleep(3)

                # Re-check URL and page content after redirect
                page_url = page.url
                text = await page.inner_text("body")

                # Check if already on detail page (the site redirects directly when there's a unique match)
                is_search_page = "search_query=" in page_url
                # URL pattern for detail page: /property-tax/{base64_id}
                is_detail_url = "/property-tax/" in page_url and len(page_url.split("/")[-1]) > 20

                # Check for content markers indicating we're on detail page
                detail_markers = ["Account Summary", "Real Estate Account", "Amount due", "Account history", "Your account is"]
                has_detail_markers = any(marker in text for marker in detail_markers)

                already_on_detail = is_detail_url or (has_detail_markers and not is_search_page)

                if already_on_detail:
                    logger.debug(f"Already on detail page for {property_address}")

                view_links = []
                if not already_on_detail:
                    # Broaden selectors: look for "View" buttons, "View" links, or account numbers in the results list
                    view_selectors = [
                        'button:has-text("View")',
                        'a:has-text("View")',
                        '.btn-primary:has-text("View")',
                        'tr.search-result a',
                        '.account-link'
                    ]
                    
                    for selector in view_selectors:
                        try:
                            view_buttons = await page.query_selector_all(selector)
                            if len(view_buttons) > 0:
                                view_links = view_buttons
                                break
                        except Exception as exc:
                            logger.debug(f"View selector failed ({selector}): {exc}")
                            continue

                    if len(view_links) == 0:
                        await asyncio.sleep(5)
                        for selector in view_selectors:
                            try:
                                view_buttons = await page.query_selector_all(selector)
                                if len(view_buttons) > 0:
                                    view_links = view_buttons
                                    break
                            except Exception as exc:
                                logger.debug(f"View selector retry failed ({selector}): {exc}")
                                continue

                if len(view_links) == 0 and not already_on_detail:
                    # Final fallback: Look for ANY button or link that might be the view button in a results table
                    try:
                        results_table_links = await page.query_selector_all('table tr td a')
                        if len(results_table_links) > 0:
                             view_links = results_table_links
                        else:
                            logger.warning(f"No View buttons or result links found for {property_address}")
                            duration_ms = (time.perf_counter() - search_started) * 1000
                            log_search(
                                source="TAX",
                                query=property_address,
                                results_raw=0,
                                results_kept=0,
                                duration_ms=duration_ms,
                                parcel_id=parcel_id,
                            )
                            return tax_status
                    except Exception:
                        logger.warning(f"No View buttons found for {property_address}")
                        duration_ms = (time.perf_counter() - search_started) * 1000
                        log_search(
                            source="TAX",
                            query=property_address,
                            results_raw=0,
                            results_kept=0,
                            duration_ms=duration_ms,
                            parcel_id=parcel_id,
                        )
                        return tax_status

                # Navigate to detail page if needed
                if not already_on_detail and len(view_links) > 0:
                    try:
                        async with page.expect_navigation(timeout=30000):
                            await view_links[0].click()
                        
                        # Wait for content
                        try:
                            await page.wait_for_selector("h2:has-text('Amount Due'), h3:has-text('Amount Due'), text=Your account is", timeout=20000)
                        except Exception as e:
                            logger.debug(f"Tax detail page selector not found, falling back: {e}")
                            await asyncio.sleep(5)
                            
                        await asyncio.sleep(3)
                        already_on_detail = True
                    except Exception as e:
                        logger.warning(f"Navigation to detail failed: {e}")
                        duration_ms = (time.perf_counter() - search_started) * 1000
                        log_search(
                            source="TAX",
                            query=property_address,
                            results_raw=0,
                            results_kept=0,
                            duration_ms=duration_ms,
                            parcel_id=parcel_id,
                        )
                        # Fallback parsing of search results logic inside 'finally' or here?
                        # Using search result parsing fallback
                        amount_due = self._parse_amount_due(text)
                        if amount_due is not None:
                             tax_status.amount_due = amount_due
                             if amount_due == 0:
                                 tax_status.paid_in_full = True

                if already_on_detail:
                     # Scroll to top
                     await page.evaluate("window.scrollTo(0, 0)")
                     await asyncio.sleep(1)

                     # Screenshot
                     screenshot_bytes = await page.screenshot(full_page=True)
                     screenshot_path = self.storage.save_screenshot(
                         property_id=parcel_id,
                         scraper="tax_collector",
                         image_data=screenshot_bytes,
                         context="detail_page"
                     )
                     tax_status.screenshot_path = screenshot_path
                     full_screenshot_path = self.storage.get_full_path(parcel_id, screenshot_path)

                     # Extract Data
                     tax_data = await self._extract_tax_from_aria(page)
                     
                     # Vision Fallback
                     if not tax_data.get("account_number") and not tax_data.get("paid_in_full"):
                         vision_data = await self._extract_tax_from_screenshot(str(full_screenshot_path))
                         if vision_data.get("account_number") or vision_data.get("paid_in_full"):
                             tax_data = vision_data
                     
                     # Populate Model
                     tax_status.account_number = tax_data.get("account_number")
                     tax_status.owner = tax_data.get("owner")
                     tax_status.situs = tax_data.get("situs")
                     tax_status.amount_due = tax_data.get("amount_due", 0.0)
                     tax_status.paid_in_full = tax_data.get("paid_in_full", False)
                     tax_status.last_payment = tax_data.get("last_payment")

                     if tax_data.get("certificates"):
                         for cert in tax_data["certificates"]:
                             tax_status.certificates.append(TaxCertificate(
                                 certificate_number=str(cert.get("certificate_number")),
                                 face_value=float(cert.get("face_value", 0.0))
                             ))
                     result_found = True
            except Exception as e:
                logger.exception(f"Error scraping Tax site: {e}")
                # Re-raise so orchestrator can properly mark as failed with actual error
                raise
            finally:
                await browser.close()

        duration_ms = (time.perf_counter() - search_started) * 1000
        log_search(
            source="TAX",
            query=property_address,
            results_raw=1 if result_found else 0,
            results_kept=len(tax_status.certificates) + (1 if tax_status.amount_due else 0),
            duration_ms=duration_ms,
            parcel_id=parcel_id,
        )
        return tax_status

    async def get_tax_liens(self, parcel_id: str, property_address: Optional[str] = None) -> List[dict]:
        """Legacy wrapper for backward compatibility."""
        status = await self.scrape_tax_status(parcel_id, property_address)
        # Convert TaxStatus to list of lien dicts as expected by legacy pipeline
        results = []
        # Add basic tax lien info if amount due > 0
        if status.amount_due > 0:
            results.append({
                 "document_type": "TAX",
                 "recording_date": None,
                 "amount": status.amount_due,
                 "grantor": parcel_id,
                 "grantee": "Hillsborough County Tax Collector",
                 "description": f"Unpaid property taxes: ${status.amount_due:,.2f}",
                 "tax_account": status.account_number
            })
        # Add historic certificates
        for cert in status.certificates:
            results.append({
                "document_type": "TAX_CERTIFICATE",
                "recording_date": None,
                "amount": cert.face_value,
                "grantor": parcel_id,
                "grantee": "Tax Certificate Holder",
                "description": f"Tax Certificate #{cert.certificate_number}",
                "certificate_number": cert.certificate_number
            })
        return results

    async def check_lienhub(self, parcel_id: str, property_address: Optional[str] = None) -> List[dict]:
        """
        Check lienhub.com for tax lien certificates.

        Note: Lienhub has Cloudflare protection, so this may not always work.

        Args:
            parcel_id: The folio/parcel ID
            property_address: The property address

        Returns:
            List of lien dicts from lienhub
        """
        if not property_address:
            logger.debug(f"No property address for lienhub search: {parcel_id}")
            return []

        liens = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)  # Non-headless may help with Cloudflare
            context = await browser.new_context(viewport={'width': 1280, 'height': 720})
            page = await context.new_page()

            stealth = Stealth()
            await stealth.apply_stealth_async(page)

            try:
                logger.info(f"Checking lienhub.com for {property_address}")
                await page.goto(self.LIENHUB_URL, timeout=60000)
                await page.wait_for_load_state("domcontentloaded")
                await asyncio.sleep(5)  # Wait for Cloudflare challenge

                # Check if we got past Cloudflare
                content = await page.content()
                if "cf-turnstile" in content or "Verifying you are human" in content:
                    logger.warning("Lienhub blocked by Cloudflare Turnstile")
                    return []

                # Look for search input
                search_input = page.locator("input[type='text'], input[type='search']").first
                if await search_input.count() > 0:
                    await search_input.fill(property_address)
                    await search_input.press("Enter")
                    await asyncio.sleep(3)
                    # Parse lienhub results...
                    # TODO: Implement lienhub parsing when we can get past Cloudflare

            except Exception as e:
                logger.debug(f"Lienhub check failed: {e}")
            finally:
                await browser.close()

        return liens

    def _parse_tax_detail_page(self, text: str) -> dict:
        """
        Parse the tax detail page to extract structured data.

        Returns dict with:
        - account_number: str
        - owner: str
        - situs: str
        - amount_due: float
        - paid_in_full: bool
        - last_payment: str
        - certificates: list of tax certificate info
        """
        result = {
            "account_number": None,
            "owner": None,
            "situs": None,
            "amount_due": 0.0,
            "paid_in_full": False,
            "last_payment": None,
            "certificates": [],
        }

        if not text:
            return result

        # Extract account number - pattern: "Real Estate Account #A0380975910"
        account_patterns = [
            r"Account\s*#([A-Z0-9]+)",
            r"Real Estate Account\s*#([A-Z0-9]+)",
            r"Account[:\s]+([A-Z0-9]+)",
        ]
        for pattern in account_patterns:
            account_match = re.search(pattern, text, re.IGNORECASE)
            if account_match:
                result["account_number"] = account_match.group(1)
                break

        # Check if paid in full - multiple patterns
        paid_patterns = [
            "paid in full",
            "nothing due at this time",
            "there is nothing due",
        ]
        for pattern in paid_patterns:
            if pattern in text.lower():
                result["paid_in_full"] = True
                break

        # Extract last payment info - pattern: "most recent payment was made on 11/26/2025 for $4,804.20"
        payment_patterns = [
            r"most recent payment.*?on\s+(\d{1,2}/\d{1,2}/\d{4}).*?for\s+\$([\d,]+\.?\d*)",
            r"most recent payment.*?(\d{1,2}/\d{1,2}/\d{4}).*?\$([\d,]+\.?\d*)",
            r"last payment.*?(\d{1,2}/\d{1,2}/\d{4}).*?\$([\d,]+\.?\d*)",
        ]
        for pattern in payment_patterns:
            payment_match = re.search(pattern, text, re.IGNORECASE)
            if payment_match:
                result["last_payment"] = f"{payment_match.group(1)} - ${payment_match.group(2)}"
                break

        # Check for amount due (if not paid in full)
        if not result["paid_in_full"]:
            # Look for "Total Amount Due" or similar
            due_patterns = [
                r"Total\s+Amount\s+Due[:\s]*\$([\d,]+\.?\d*)",
                r"Amount\s+Due[:\s]*\$([\d,]+\.?\d*)",
                r"Balance\s+Due[:\s]*\$([\d,]+\.?\d*)",
                r"Total Amount Due\s*\$([\d,]+\.?\d*)",
            ]
            for pattern in due_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    try:
                        amount = float(match.group(1).replace(",", ""))
                        if amount > 0:
                            result["amount_due"] = amount
                            break
                    except ValueError:
                        continue

        # Look for tax certificates (liens that were sold) - pattern: "Certificate #300551" ... "Face $1,288.79"
        cert_patterns = [
            r"Certificate\s*#(\d+).*?Face\s+\$([\d,]+\.?\d*)",
            r"Certificate\s*#?\s*(\d+).*?(?:Face|Amount)[:\s]*\$([\d,]+\.?\d*)",
        ]
        for pattern in cert_patterns:
            cert_matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
            if cert_matches:
                for cert_num, amount in cert_matches:
                    result["certificates"].append({
                        "certificate_number": cert_num,
                        "face_value": float(amount.replace(",", "")) if amount else 0
                    })
                break

        # Extract owner name - pattern after "Owner:" or "Owner"
        owner_patterns = [
            r"Owner[:\s]+([A-Z][A-Z\s,]+?)(?:\n|Situs|$)",
            r"Owner\s+([A-Z][A-Z\s]+)\s+([A-Z][A-Z\s]+)\s+Situs",
        ]
        for pattern in owner_patterns:
            owner_match = re.search(pattern, text)
            if owner_match:
                result["owner"] = owner_match.group(1).strip()
                break

        # Extract situs address - pattern after "Situs:"
        situs_patterns = [
            r"Situs[:\s]+(.+?)(?:\n|Parcel|GIS|Property|$)",
            r"Situs\s+(\d+\s+[A-Z\s]+(?:CT|ST|AVE|DR|RD|LN|BLVD|WAY|PL|CIR))\s+",
        ]
        for pattern in situs_patterns:
            situs_match = re.search(pattern, text, re.IGNORECASE)
            if situs_match:
                result["situs"] = situs_match.group(1).strip()
                break

        return result



    def _normalize_address(self, address: str) -> str:
        """
        Normalize address for search.

        Extracts just the street address (number + street name) without city/state/zip.
        This improves matching on the tax collector site.
        """
        if not address:
            return ""

        # Remove common separators and city/state/zip
        # Pattern: "123 MAIN ST, TAMPA, FL 33602" -> "123 MAIN ST"
        # Also handles: "123 MAIN ST, TAMPA, FL- 33602" (note the dash)
        address = address.upper().strip()

        # Split on comma and take just the street portion
        if ',' in address:
            address = address.split(',')[0].strip()

        # Remove any remaining state/zip at the end (e.g., "FL 33602" or "FL- 33602")
        address = re.sub(r'\s+FL[-\s]*\d{5}.*$', '', address, flags=re.IGNORECASE)

        return address.strip()

    @staticmethod
    def _parse_amount_due(text: str) -> Optional[float]:
        """
        Try to extract an "Amount Due" dollar value from page text.
        Returns None if nothing obvious is found.
        """
        if not text:
            return None

        patterns = [
            r"Amount\s+Due[:\s]*\$([\d,]+\.?\d*)",
            r"Total\s+Due[:\s]*\$([\d,]+\.?\d*)",
            r"Balance\s+Due[:\s]*\$([\d,]+\.?\d*)",
            r"Taxes\s+Due[:\s]*\$([\d,]+\.?\d*)",
            r"Due[:\s]*\$([\d,]+\.?\d*)",
        ]

        for pat in patterns:
            match = re.search(pat, text, flags=re.IGNORECASE)
            if match:
                try:
                    amount_str = match.group(1).replace(",", "")
                    if "." not in amount_str:
                        amount_str += ".00"
                    return float(amount_str)
                except ValueError:
                    continue

        # Check for $0.00 or "paid" indicators
        if re.search(r"\$0\.00", text) or "paid in full" in text.lower():
            return 0.0

        return None

    async def _extract_tax_from_aria(self, page) -> dict:
        """
        Extract tax data from page using accessibility tree.

        This is a fallback when vision service is unavailable.
        Playwright's accessibility snapshot can extract text from JS-rendered content.
        """
        try:
            # Get accessibility snapshot which includes all rendered text
            snapshot = await page.accessibility.snapshot()

            def extract_text(node, texts=None):
                if texts is None:
                    texts = []
                if node:
                    node_name = node.get("name")
                    if node_name:
                        texts.append(node_name)
                    for child in node.get("children", []):
                        extract_text(child, texts)
                return texts

            all_texts = extract_text(snapshot)
            full_text = ' '.join(all_texts)
            logger.info(f"Accessibility text length: {len(full_text)}")
            logger.debug(f"Accessibility text (first 1000): {full_text[:1000]}")

            # Parse using existing method
            return self._parse_tax_detail_page(full_text)
        except Exception as e:
            logger.error(f"Accessibility extraction failed: {e}")
            return {
                "account_number": None,
                "owner": None,
                "situs": None,
                "paid_in_full": False,
                "amount_due": 0.0,
                "last_payment": None,
                "certificates": [],
            }

    async def _extract_tax_from_screenshot(self, screenshot_path: str) -> dict:
        """
        Use vision model to extract tax data from screenshot.

        Args:
            screenshot_path: Path to the screenshot file

        Returns dict with tax information extracted from the screenshot.
        """
        prompt = """Extract the following information from this tax collector screenshot:

1. Account Number (e.g., A0380975910)
2. Owner Name(s)
3. Property Address (Situs)
4. Is the account "paid in full"? (yes/no)
5. Amount Due (if any unpaid amount)
6. Most recent payment date and amount (if shown)
7. Any tax certificates (certificate numbers and amounts)

Return ONLY a JSON object with these fields:
{
  "account_number": "string or null",
  "owner": "string or null",
  "situs": "string or null",
  "paid_in_full": true/false,
  "amount_due": 0.0,
  "last_payment_date": "string or null",
  "last_payment_amount": 0.0,
  "certificates": []
}

If the page shows "paid in full" or "nothing due", set paid_in_full to true and amount_due to 0.
If there's an amount due, set paid_in_full to false and provide the amount.
"""
        try:
            result = await self.vision.process_async(self.vision.analyze_image, screenshot_path, prompt)
            logger.info(f"Vision extraction result: {result}")

            if not result:
                logger.warning("Vision returned no data for tax screenshot {}", screenshot_path)
                return {
                    "account_number": None,
                    "owner": None,
                    "situs": None,
                    "paid_in_full": False,
                    "amount_due": 0.0,
                    "last_payment": None,
                    "certificates": [],
                }

            # Parse the JSON response
            import json
            # Try direct JSON parse first (vision model often returns clean JSON)
            try:
                # Clean up markdown code blocks if present
                clean_result = result.strip()
                if clean_result.startswith("```"):
                    clean_result = re.sub(r'^```(?:json)?\s*', '', clean_result)
                    clean_result = re.sub(r'\s*```$', '', clean_result)
                data = json.loads(clean_result)
                return {
                    "account_number": data.get("account_number"),
                    "owner": data.get("owner"),
                    "situs": data.get("situs"),
                    "paid_in_full": data.get("paid_in_full", False),
                    "amount_due": float(data.get("amount_due", 0) or 0),
                    "last_payment": f"{data.get('last_payment_date', '')} - ${data.get('last_payment_amount', 0)}" if data.get('last_payment_date') else None,
                    "certificates": data.get("certificates", []),
                }
            except json.JSONDecodeError:
                logger.warning(f"Could not parse JSON from vision response: {result[:200]}...")

            # Fallback: try to parse text response
            result_lower = result.lower()
            return {
                "account_number": None,
                "owner": None,
                "situs": None,
                "paid_in_full": "paid in full" in result_lower or "nothing due" in result_lower,
                "amount_due": 0.0,
                "last_payment": None,
                "certificates": [],
            }
        except Exception as e:
            logger.error(f"Vision extraction failed: {e}")
            return {
                "account_number": None,
                "owner": None,
                "situs": None,
                "paid_in_full": False,
                "amount_due": 0.0,
                "last_payment": None,
                "certificates": [],
            }


if __name__ == "__main__":
    scraper = TaxScraper()
    # Test with an address
    result = asyncio.run(scraper.get_tax_liens(
        "1928231J1000005J00000T",
        "7713 SUMTER CT"
    ))
    print(f"Results: {result}")
