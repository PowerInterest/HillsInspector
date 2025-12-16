
import asyncio
import re
import urllib.parse
from typing import List, Optional
from dataclasses import dataclass
from loguru import logger
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from src.services.scraper_storage import ScraperStorage
from src.services.vision_service import VisionService


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

    async def get_tax_liens(self, parcel_id: str, property_address: Optional[str] = None) -> List[dict]:
        """
        Searches for unpaid property taxes by address.

        Args:
            parcel_id: The folio/parcel ID (used for logging and storage)
            property_address: The property address to search for

        Returns:
            List of lien-like dicts (document_type='TAX').
        """
        if not property_address:
            logger.warning(f"No property address provided for {parcel_id}, cannot search taxes")
            return []

        logger.info(f"Searching Tax Collector for: {property_address} (Parcel: {parcel_id})")
        liens = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(viewport={'width': 1280, 'height': 720})
            page = await context.new_page()

            # Apply stealth
            stealth = Stealth()
            await stealth.apply_stealth_async(page)

            try:
                # Build search URL - use street address without city/zip for better matching
                # Extract just the street number and name
                search_address = self._normalize_address(property_address)
                encoded_address = urllib.parse.quote(search_address)
                search_url = f"{self.BASE_URL}?search_query={encoded_address}"

                logger.info(f"Navigating to {search_url}")
                await page.goto(search_url, timeout=60000)
                await page.wait_for_load_state("domcontentloaded")

                # Wait for search results to load (Angular app fetches data async)
                # Look for either results (View button) or "No bills" message or Page indicator
                try:
                    await page.wait_for_selector("button:has-text('View'), text=No bills or accounts matched, text=Page", timeout=15000)
                except Exception:
                    logger.debug("Timeout waiting for search results selector, using fallback wait")
                    await asyncio.sleep(8)  # Fallback wait for slow loads

                # Check for "No results" message
                text = await page.inner_text("body")
                if "No bills or accounts matched your search" in text:
                    logger.info(f"No tax records found for {property_address}")
                    return []

                # Check if we're already on the detail page (site auto-navigates for single results)
                # The detail page URL contains a base64 encoded account ID, not "search_query"
                page_url = page.url
                is_search_page = "search_query=" in page_url
                already_on_detail = (
                    "Account Summary" in text or
                    "Real Estate Account" in text or
                    "Amount Due" in text or
                    (not is_search_page and "/property-tax/" in page_url and len(page_url.split("/")[-1]) > 20)  # Long base64 ID
                )
                logger.info(f"Page URL: {page_url}, Is search page: {is_search_page}, Already on detail: {already_on_detail}")

                if already_on_detail:
                    logger.info("Already on account detail page - extracting data directly")
                    # Skip looking for View buttons, we're already on the detail page
                    view_links = []
                else:
                    # Look for View buttons - they appear in search results as dark blue buttons
                    view_buttons = await page.query_selector_all('button:has-text("View")')
                    logger.info(f"Found {len(view_buttons)} View buttons")

                    if len(view_buttons) == 0:
                        # Wait a bit more and retry
                        await asyncio.sleep(5)
                        view_buttons = await page.query_selector_all('button:has-text("View")')
                        logger.info(f"After retry: Found {len(view_buttons)} View buttons")

                    view_links = view_buttons

                if len(view_links) == 0 and not already_on_detail:
                    logger.warning(f"No View buttons found on tax search page for {property_address}")
                    # Save screenshot for debugging
                    screenshot_bytes = await page.screenshot()
                    self.storage.save_screenshot(
                        property_id=parcel_id,
                        scraper="tax_collector",
                        image_data=screenshot_bytes,
                        context="no_results"
                    )
                    return []

                # Handle detail page data extraction
                if already_on_detail:
                    # We're already on the detail page - extract data directly
                    logger.info("Already on detail page - extracting tax data...")
                    # Wait briefly for any remaining content to load
                    await asyncio.sleep(3)

                    # Save screenshot for records
                    screenshot_bytes = await page.screenshot(full_page=True)
                    screenshot_path = self.storage.save_screenshot(
                        property_id=parcel_id,
                        scraper="tax_collector",
                        image_data=screenshot_bytes,
                        context="detail_page"
                    )
                    # Convert to full path for VisionService
                    full_screenshot_path = self.storage.get_full_path(parcel_id, screenshot_path)
                    logger.info(f"Saved screenshot to {full_screenshot_path}")

                    # Try accessibility tree first (faster, no network needed)
                    tax_data = await self._extract_tax_from_aria(page)
                    logger.info(f"Accessibility extracted tax data: {tax_data}")

                    # If accessibility extraction didn't find key data, try vision extraction
                    if not tax_data.get("account_number") and not tax_data.get("paid_in_full"):
                        logger.info(f"Text extraction incomplete, trying vision extraction...")
                        vision_data = self._extract_tax_from_screenshot(str(full_screenshot_path))
                        if vision_data.get("account_number") or vision_data.get("paid_in_full"):
                            logger.info(f"Vision extracted tax data: {vision_data}")
                            tax_data = vision_data
                        else:
                            logger.info(f"Vision extraction also incomplete, screenshot saved to {full_screenshot_path}")

                    # Always return tax status data (not just liens)
                    tax_record = {
                        "document_type": "TAX",
                        "recording_date": None,
                        "amount": tax_data.get("amount_due", 0),
                        "grantor": parcel_id,
                        "grantee": "Hillsborough County Tax Collector",
                        "tax_account": tax_data.get("account_number"),
                        "owner": tax_data.get("owner"),
                        "situs": tax_data.get("situs"),
                        "paid_in_full": tax_data.get("paid_in_full", False),
                        "last_payment": tax_data.get("last_payment"),
                        "certificates": tax_data.get("certificates", []),
                    }

                    if tax_data.get("amount_due", 0) > 0:
                        tax_record["description"] = f"Unpaid property taxes: ${tax_data['amount_due']:,.2f}"
                        logger.success(f"Detected unpaid taxes for {parcel_id}: ${tax_data['amount_due']:,.2f}")
                    elif tax_data.get("paid_in_full"):
                        tax_record["description"] = "Taxes paid in full"
                        logger.info(f"Taxes PAID IN FULL for {property_address}")
                        if tax_data.get("last_payment"):
                            logger.info(f"  Last payment: {tax_data['last_payment']}")
                    else:
                        tax_record["description"] = "Tax status unknown"
                        logger.info(f"Tax status unknown for {property_address}, screenshot saved at {screenshot_path}")

                    # Log certificates if any exist (historical tax liens that were sold)
                    if tax_data.get("certificates"):
                        logger.info(f"  Found {len(tax_data['certificates'])} historical tax certificates")

                    liens.append(tax_record)

                elif len(view_links) > 0:
                    try:
                        logger.info("Clicking View to get tax details...")

                        # Click and wait for navigation
                        async with page.expect_navigation(timeout=30000):
                            await view_links[0].click()

                        # Wait for Angular app to finish loading - look for "paid in full" or "Amount Due" heading
                        # The page shows loading spinners initially, then content
                        try:
                            # Wait for the "Amount Due" section header which appears on all accounts
                            await page.wait_for_selector("h2:has-text('Amount Due'), h3:has-text('Amount Due'), text=Your account is", timeout=20000)
                        except Exception as wait_err:
                            logger.debug(f"Wait for Amount Due failed: {wait_err}")
                            # Last resort - longer wait for slow loads
                            await asyncio.sleep(10)

                        await asyncio.sleep(3)  # Additional safety buffer for rendering

                        # Scroll to top to ensure content is visible
                        await page.evaluate("window.scrollTo(0, 0)")
                        await asyncio.sleep(1)

                        # Save screenshot for records
                        screenshot_bytes = await page.screenshot(full_page=True)
                        screenshot_path = self.storage.save_screenshot(
                            property_id=parcel_id,
                            scraper="tax_collector",
                            image_data=screenshot_bytes,
                            context="detail_page"
                        )
                        # Convert to full path for VisionService
                        full_screenshot_path = self.storage.get_full_path(parcel_id, screenshot_path)
                        logger.info(f"Saved screenshot to {full_screenshot_path}")

                        # Try accessibility tree first (faster, no network needed)
                        tax_data = await self._extract_tax_from_aria(page)
                        logger.info(f"Accessibility extracted tax data: {tax_data}")

                        # If accessibility extraction didn't find key data, try vision extraction
                        if not tax_data.get("account_number") and not tax_data.get("paid_in_full"):
                            logger.info(f"Text extraction incomplete, trying vision extraction...")
                            vision_data = self._extract_tax_from_screenshot(str(full_screenshot_path))
                            if vision_data.get("account_number") or vision_data.get("paid_in_full"):
                                logger.info(f"Vision extracted tax data: {vision_data}")
                                tax_data = vision_data
                            else:
                                logger.info(f"Vision extraction also incomplete, screenshot saved to {full_screenshot_path}")

                        # Always return tax status data (not just liens)
                        tax_record = {
                            "document_type": "TAX",
                            "recording_date": None,
                            "amount": tax_data.get("amount_due", 0),
                            "grantor": parcel_id,
                            "grantee": "Hillsborough County Tax Collector",
                            "tax_account": tax_data.get("account_number"),
                            "owner": tax_data.get("owner"),
                            "situs": tax_data.get("situs"),
                            "paid_in_full": tax_data.get("paid_in_full", False),
                            "last_payment": tax_data.get("last_payment"),
                            "certificates": tax_data.get("certificates", []),
                        }

                        if tax_data.get("amount_due", 0) > 0:
                            tax_record["description"] = f"Unpaid property taxes: ${tax_data['amount_due']:,.2f}"
                            logger.success(f"Detected unpaid taxes for {parcel_id}: ${tax_data['amount_due']:,.2f}")
                        elif tax_data.get("paid_in_full"):
                            tax_record["description"] = "Taxes paid in full"
                            logger.info(f"Taxes PAID IN FULL for {property_address}")
                            if tax_data.get("last_payment"):
                                logger.info(f"  Last payment: {tax_data['last_payment']}")
                        else:
                            tax_record["description"] = "Tax status unknown"
                            logger.info(f"Tax status unknown for {property_address}, screenshot saved at {screenshot_path}")

                        # Log certificates if any exist (historical tax liens that were sold)
                        if tax_data.get("certificates"):
                            logger.info(f"  Found {len(tax_data['certificates'])} historical tax certificates")

                        liens.append(tax_record)

                    except Exception as e:
                        logger.warning(f"Could not get tax details: {e}")
                        # Fall back to search results parsing
                        amount_due = self._parse_amount_due(text)
                        if amount_due is not None and amount_due > 0:
                            liens.append({
                                "document_type": "TAX",
                                "recording_date": None,
                                "amount": amount_due,
                                "grantor": parcel_id,
                                "grantee": "Hillsborough County Tax Collector",
                                "description": f"Unpaid property taxes: ${amount_due:,.2f}",
                            })
                else:
                    # No view links - parse search results
                    amount_due = self._parse_amount_due(text)
                    if amount_due is not None and amount_due > 0:
                        liens.append({
                            "document_type": "TAX",
                            "recording_date": None,
                            "amount": amount_due,
                            "grantor": parcel_id,
                            "grantee": "Hillsborough County Tax Collector",
                            "description": f"Unpaid property taxes: ${amount_due:,.2f}",
                        })

                # Save screenshot for records
                screenshot_bytes = await page.screenshot()
                screenshot_path = self.storage.save_screenshot(
                    property_id=parcel_id,
                    scraper="tax_collector",
                    image_data=screenshot_bytes,
                    context="search_results"
                )
                logger.debug(f"Screenshot saved to {screenshot_path}")

            except Exception as e:
                logger.error(f"Error scraping Tax site: {e}")
            finally:
                await browser.close()

        return liens

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

        Removes common suffixes like DR, ST, AVE, etc. to improve matching.
        """
        if not address:
            return ""

        # Keep only the street number and first part of street name
        # This helps match variations like "123 MAIN ST" vs "123 MAIN STREET"
        parts = address.upper().strip().split()

        # Build normalized address - include suffix for uniqueness but don't over-filter
        return " ".join(parts)

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

    def _extract_tax_from_screenshot(self, screenshot_path: str) -> dict:
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
            result = self.vision.analyze_image(screenshot_path, prompt)
            logger.info(f"Vision extraction result: {result}")

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
