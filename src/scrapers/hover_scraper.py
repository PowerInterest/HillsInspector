import asyncio
import random
import re
from typing import Optional, List, Dict
from loguru import logger
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.services.scraper_storage import ScraperStorage

class HoverScraper:
    BASE_URL = "https://hover.hillsclerk.com"
    
    def __init__(self, storage: Optional[ScraperStorage] = None):
        self.storage = storage or ScraperStorage()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((PlaywrightTimeoutError, Exception)),
        reraise=True
    )
    async def get_case_documents(self, case_number: str) -> List[Dict[str, str]]:
        """
        Searches for a case and returns the list of documents, specifically looking for Final Judgment.
        Returns a list of dicts with 'title' and 'url' (or 'content' if downloaded).
        """
        parsed = self._parse_case_number(case_number)
        if not parsed:
            logger.error(f"Invalid case number format: {case_number}")
            return []
            
        year, case_type, seq = parsed
        logger.info(f"Searching HOVER for Case: Year={year}, Type={case_type}, Seq={seq}")

        documents = []

        async with async_playwright() as p:
            # User requested to surface browser
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={'width': 1280, 'height': 720}
            )
            page = await context.new_page()
            
            try:
                # Navigate directly to case search page
                search_url = f"{self.BASE_URL}/html/case/caseSearch.html"
                logger.info(f"HOVER GET: {search_url}")
                await page.goto(search_url, timeout=60000)
                await page.wait_for_load_state("networkidle")
                
                # Wait for form fields
                # The page likely has inputs for Year, Type, Sequence
                # We'll wait for the Year input specifically if possible, or just any input
                await page.wait_for_selector("input", timeout=10000)
                
                # Fill the form
                # We need to identify the inputs. 
                # Strategy: Look for labels or placeholders.
                # "Case Year" (4 digits), "Court Type" (Dropdown), "Sequence #" (digits)
                
                # Try to find inputs by label text if possible, or by proximity
                # Assuming standard layout: Year, Type, Seq
                
                # Year
                await page.get_by_label("Case Year").fill(year)
                
                # Type
                # This is likely a select. We need to match 'CA' to the option.
                # 'CA' usually maps to 'Circuit Civil' or similar code.
                # If the value is 'CA', select_option(value='CA') works.
                try:
                    await page.get_by_label("Court Type").select_option(value=case_type)
                except Exception as e:
                    options_sample = []
                    try:
                        options_sample = await page.get_by_label("Court Type").locator("option").all_inner_texts()
                    except Exception as opt_err:
                        logger.warning(f"Could not inspect Court Type options for {case_number}: {opt_err}")
                    logger.warning(
                        f"Could not select Court Type '{case_type}' for case {case_number}: {e}. "
                        f"Available options sample={options_sample[:10]}. Continuing with best-effort form submission."
                    )
                
                # Sequence
                await page.get_by_label("Sequence #").fill(seq)
                
                # Search
                await asyncio.sleep(random.uniform(1, 2))  # noqa: S311
                await page.click("button:has-text('Search')")
                
                # Wait for results
                # It might go to a list or directly to the case.
                await page.wait_for_load_state("networkidle")
                await page.wait_for_timeout(2000)
                
                # Check if we have results
                # Look for a link with the case number
                case_link = page.locator(f"a:has-text('{seq}')")
                if await case_link.count() > 0:
                    await asyncio.sleep(random.uniform(1, 2))  # noqa: S311
                    await case_link.first.click()
                    await page.wait_for_load_state("networkidle")
                    
                    # Now we are on the case detail page.
                    # Look for "Dockets" or "Events" tab/section.
                    # We want to find "Final Judgment".
                    
                    # Strategy: Get all rows that contain "Judgment"
                    judgment_rows = page.locator("tr:has-text('Judgment')")
                    count = await judgment_rows.count()
                    logger.info(f"Found {count} rows with 'Judgment'")
                    
                    for i in range(count):
                        row = judgment_rows.nth(i)
                        text = await row.inner_text()
                        if "Final Judgment" in text or "FINAL JUDGMENT" in text:
                            logger.info(f"Found Final Judgment: {text}")
                            # Look for a view/download link/button in this row
                            # It might be an image icon or "View" text
                            view_btn = row.locator("a, button, img[title='View']")
                            if await view_btn.count() > 0:
                                # This might open a popup.
                                async with context.expect_page() as new_page_info:
                                    await view_btn.first.click()
                                    
                                new_page = await new_page_info.value
                                await new_page.wait_for_load_state()
                                logger.info(f"Opened document page: {new_page.url}")
                                
                                # Check if it's a PDF or a viewer
                                if new_page.url.lower().endswith('.pdf'):
                                    # Check if already downloaded
                                    existing_path = self.storage.document_exists(
                                        property_id=case_number,
                                        doc_type="final_judgment",
                                        doc_id=case_number,
                                        extension="pdf"
                                    )
                                    if existing_path:
                                        logger.debug(f"PDF already exists for {case_number}: {existing_path}")
                                        documents.append({
                                            "title": "Final Judgment",
                                            "url": new_page.url,
                                            "local_path": str(existing_path),
                                            "doc_type": "Final Judgment"
                                        })
                                        await new_page.close()
                                        break

                                    logger.info(f"Downloading PDF from {new_page.url}...")
                                    response = await new_page.request.get(new_page.url)
                                    if response.status == 200:
                                        pdf_bytes = await response.body()

                                        # Save using ScraperStorage
                                        saved_path = self.storage.save_document(
                                            property_id=case_number,
                                            file_data=pdf_bytes,
                                            doc_type="final_judgment",
                                            doc_id=case_number,
                                            extension="pdf"
                                        )
                                        logger.info(f"Saved to {saved_path}")
                                        
                                        documents.append({
                                            "title": "Final Judgment",
                                            "url": new_page.url,
                                            "local_path": saved_path,
                                            "doc_type": "Final Judgment"
                                        })
                                    else:
                                        logger.error(f"Failed to download PDF: {response.status}")
                                else:
                                    # It might be a viewer. Try to find a download button or print button
                                    # For now, just save the URL
                                    logger.warning(f"Document is not a direct PDF: {new_page.url}")
                                    documents.append({
                                        "title": "Final Judgment",
                                        "url": new_page.url,
                                        "doc_type": "Final Judgment"
                                    })

                                await new_page.close()
                                break # Found it
                            
                else:
                    logger.warning("Case not found in search results.")

            except Exception as e:
                logger.error(f"Error searching HOVER: {e}")
                # await page.screenshot(path=f"error_hover_{case_number}.png")
                
            # finally:
            #     await browser.close()
            logger.info("Browser left open for user inspection.")
                
        return documents

    def _parse_case_number(self, case_number: str):
        """
        Parses case number into Year, Type, Sequence.
        Supports formats:
        - 292023CA013924A001HC (Full)
        - 23-CA-013924 (Short)
        """
        case_number = case_number.strip().upper()
        
        # Format: 23-CA-013924
        match_short = re.match(r'(\d{2})[- ]*([A-Z]{2})[- ]*(\d+)', case_number)
        if match_short:
            year_short = match_short.group(1)
            year = f"20{year_short}" if int(year_short) < 50 else f"19{year_short}"
            case_type = match_short.group(2)
            seq = match_short.group(3)
            return year, case_type, seq
            
        # Format: 292023CA013924...
        # 29 (County) + 2023 (Year) + CA (Type) + 013924 (Seq)
        if len(case_number) >= 14:
            # Assuming starts with 29 for Hillsborough
            # But let's just grab the year position
            # 29 2023 CA ...
            year = case_number[2:6]
            case_type = case_number[6:8]
            seq = case_number[8:14]
            return year, case_type, seq
            
        return None

if __name__ == "__main__":
    scraper = HoverScraper()
    # Test with the case number found in previous steps or a dummy
    # 292023CA013924A001HC -> 2023, CA, 013924
    asyncio.run(scraper.get_case_documents("292023CA013924A001HC"))
