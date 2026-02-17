import json
import os
from contextlib import suppress
from pathlib import Path
from typing import Optional, Dict, Any
from loguru import logger
import fitz  # PyMuPDF
from src.services.vision_service import VisionService


class FinalJudgmentProcessor:
    """
    Service for processing Final Judgment PDFs and extracting structured data.
    """
    
    def __init__(self):
        self.vision_service = VisionService()
        self.temp_dir = Path("data/temp/doc_images")
        self.temp_dir.mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def _json_cache_path(pdf_path: str) -> Path:
        """Return the path to the JSON extraction cache file next to the PDF."""
        p = Path(pdf_path)
        return p.parent / f"{p.stem}_extracted.json"

    def process_pdf(
        self,
        pdf_path: str,
        case_number: str,
        *,
        force: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Process a Final Judgment PDF and extract structured data.

        Args:
            pdf_path: Path to the Final Judgment document (PDF, etc.)
            case_number: Case number for logging/tracking
            force: If True, ignore cached JSON and re-extract via Vision

        Returns:
            Dict with extracted data (including raw_text for debugging) or None if processing failed
        """
        if not os.path.exists(pdf_path):
            logger.error(f"PDF not found: {pdf_path}")
            return None

        # Check for cached extraction JSON next to the PDF
        cache_path = self._json_cache_path(pdf_path)
        if not force and cache_path.exists():
            try:
                cached = json.loads(cache_path.read_text(encoding="utf-8"))
                if cached and isinstance(cached, dict):
                    logger.info(f"Loaded cached extraction for {case_number} from {cache_path.name}")
                    return cached
            except Exception as e:
                logger.warning(f"Bad cache file {cache_path}, re-extracting: {e}")

        page_images: list[str] = []
        try:
            logger.info(f"Processing Final Judgment PDF for case {case_number}...")

            # Open PDF with PyMuPDF
            doc = fitz.open(pdf_path)
            total_pages = len(doc)
            num_pages = total_pages  # Process all pages; chunked extraction avoids context issues

            logger.info(f"PDF has {total_pages} pages, rendering all pages")

            # Render all pages to images
            for page_num in range(num_pages):
                page = doc[page_num]
                temp_image_path = self.temp_dir / f"{case_number}_page_{page_num + 1}.png"
                pix = page.get_pixmap(dpi=150)
                pix.save(str(temp_image_path))
                page_images.append(str(temp_image_path))

            doc.close()

            merged_json: Optional[Dict[str, Any]] = None
            strategies: list[str] = []

            # First pass: prioritize first 3 pages + last 5 pages (often contains Exhibit A)
            priority_images = self._select_priority_pages(page_images)
            if priority_images:
                strategies.append("priority_pages")
                logger.info(
                    f"Extracting from {len(priority_images)} prioritized pages..."
                )
                merged_json = self._extract_in_batches(priority_images, batch_size=3)

            # Second pass: chunk the entire document if critical fields are missing
            if self._needs_full_pass(merged_json):
                strategies.append("chunked_full")
                logger.info(
                    f"Running chunked extraction across {len(page_images)} pages..."
                )
                full_result = self._extract_in_batches(page_images, batch_size=3)
                if merged_json and full_result:
                    merged_json = self._merge_page_data([merged_json, full_result])
                else:
                    merged_json = merged_json or full_result

            # Final fallback: per-page extraction if still missing critical fields
            if self._needs_full_pass(merged_json):
                strategies.append("per_page_fallback")
                logger.info("Running per-page extraction fallback...")
                page_data_list: list[Dict[str, Any]] = []
                if merged_json:
                    page_data_list.append(merged_json)
                for image_path in page_images:
                    page_result = self.vision_service.extract_final_judgment(image_path)
                    if page_result:
                        page_data_list.append(page_result)
                merged_json = (
                    self._merge_page_data(page_data_list)
                    if page_data_list
                    else merged_json
                )

            if not merged_json:
                logger.warning(
                    "No structured data extracted for case {} ({} pages)",
                    case_number,
                    total_pages,
                )
                return None

            # Save raw OCR text to disk for troubleshooting
            raw_text = merged_json.get("raw_text", "")
            if raw_text and case_number:
                try:
                    docs_dir = Path(f"data/Foreclosure/{case_number}/documents")
                    docs_dir.mkdir(parents=True, exist_ok=True)
                    ocr_path = docs_dir / f"{case_number}_raw_ocr.txt"
                    ocr_path.write_text(raw_text, encoding="utf-8")
                except Exception as exc:
                    # Non-fatal debug artifact: extraction should continue.
                    logger.debug(f"Could not write OCR debug text for {case_number}: {exc}")

            merged_json['_metadata'] = {
                'case_number': case_number,
                'pages_processed': num_pages,
                'total_pages': total_pages,
                'extraction_strategies': strategies,
            }

            # Save extraction to JSON cache next to the PDF
            try:
                cache_path.write_text(
                    json.dumps(merged_json, indent=2, default=str),
                    encoding="utf-8",
                )
                logger.info(f"Saved extraction cache: {cache_path.name}")
            except Exception as e:
                logger.warning(f"Failed to save extraction cache: {e}")

            logger.info(f"Successfully processed Final Judgment for case {case_number}")
            return merged_json

        except Exception as e:
            logger.error(f"Error processing PDF for case {case_number}: {e}")
            return None
        finally:
            # Clean up temp images
            for img_path in page_images:
                with suppress(Exception):
                    Path(img_path).unlink()
    
    def _merge_page_data(self, page_data_list: list) -> Dict[str, Any]:
        """
        Merge data extracted from multiple pages, prioritizing non-null values.
        
        Args:
            page_data_list: List of dicts from each page
            
        Returns:
            Merged dict with most complete data
        """
        if not page_data_list:
            return {}
        
        # Start with first page as base
        merged = page_data_list[0].copy()
        
        # Merge in data from subsequent pages (only if value is missing in merged)
        for page_data in page_data_list[1:]:
            for key, value in page_data.items():
                # Only update if current value is None/empty and new value exists
                if not merged.get(key) and value:
                    merged[key] = value
        
        return merged

    def _extract_in_batches(
        self,
        image_paths: list[str],
        batch_size: int = 3,
    ) -> Optional[Dict[str, Any]]:
        """Extract final judgment data in smaller batches to reduce timeouts."""
        if not image_paths:
            return None
        page_data_list: list[Dict[str, Any]] = []
        for i in range(0, len(image_paths), batch_size):
            batch = image_paths[i:i + batch_size]
            batch_result = self.vision_service.extract_final_judgment_multi(batch)
            if batch_result:
                page_data_list.append(batch_result)
        return self._merge_page_data(page_data_list) if page_data_list else None

    def _select_priority_pages(self, page_images: list[str]) -> list[str]:
        """
        Select priority pages: first 3 pages + last 5 pages (often contains Exhibit A).
        Deduplicates if the document is short.
        """
        if not page_images:
            return []
        total = len(page_images)
        head_count = min(3, total)
        tail_count = min(5, max(0, total - head_count))
        selected = page_images[:head_count]
        if tail_count:
            selected += page_images[-tail_count:]
        # Deduplicate while preserving order
        seen = set()
        deduped = []
        for path in selected:
            if path in seen:
                continue
            seen.add(path)
            deduped.append(path)
        return deduped

    def _needs_full_pass(self, extracted_data: Optional[Dict[str, Any]]) -> bool:
        """Determine if we need a deeper pass to capture critical fields."""
        if not extracted_data:
            return True
        defendants = extracted_data.get('defendants') or []
        has_defendants = any(d.get('name') for d in defendants)
        legal_description = (extracted_data.get('legal_description') or "").strip()
        mortgage = extracted_data.get('foreclosed_mortgage') or {}
        has_mortgage_ref = any(
            mortgage.get(key)
            for key in ('instrument_number', 'recording_book', 'recording_page')
        )
        has_amount = bool(
            extracted_data.get('total_judgment_amount')
            or extracted_data.get('principal_amount')
        )
        return not (has_defendants and legal_description and (has_mortgage_ref or has_amount))
    
    @staticmethod
    def is_thin_extraction(result: Optional[Dict[str, Any]]) -> bool:
        """
        Check if extraction result is missing critical foreclosure fields.

        A "thin" extraction means the PDF probably isn't the real Final Judgment
        (e.g. a fee order from a CC case).  Recovery should be attempted.
        """
        if not result:
            return True
        legal_desc = (result.get("legal_description") or "").strip()
        mortgage = result.get("foreclosed_mortgage") or {}
        has_mortgage_ref = any(
            mortgage.get(k)
            for k in ("instrument_number", "recording_book", "recording_page")
        )
        return not legal_desc and not has_mortgage_ref

    @staticmethod
    def dump_pdf_text(pdf_path: str, case_number: str) -> Optional[str]:
        """
        Extract full text from PDF via PyMuPDF and dump to a debug file.

        Returns the path to the dump file, or None on failure.
        """
        try:
            dump_dir = Path("data/Foreclosure") / case_number / "debug"
            dump_dir.mkdir(parents=True, exist_ok=True)
            dump_path = dump_dir / "pdf_full_text.txt"

            doc = fitz.open(pdf_path)
            lines = []
            for page_num in range(len(doc)):
                page = doc[page_num]
                text = page.get_text("text")
                lines.append(f"--- PAGE {page_num + 1} ---")
                lines.append(text)
            doc.close()

            full_text = "\n".join(lines)
            dump_path.write_text(full_text, encoding="utf-8")
            logger.info(f"Dumped PDF text ({len(full_text)} chars) to {dump_path}")
            return str(dump_path)
        except Exception as e:
            logger.warning(f"Failed to dump PDF text for {case_number}: {e}")
            return None

    def _clean_amount(self, amount_str: Optional[str]) -> Optional[float]:
        """
        Clean and parse dollar amount strings.
        
        Args:
            amount_str: String like "$123,456.78" or "123456.78", or a number
            
        Returns:
            Float value or None if parsing fails
        """
        if not amount_str:
            return None
        
        # If already a number, return it
        if isinstance(amount_str, (int, float)):
            return float(amount_str) if amount_str != 0 else None
        
        try:
            # Remove $, commas, and whitespace
            cleaned = str(amount_str).replace('$', '').replace(',', '').strip()
            value = float(cleaned)
            return value if value != 0 else None
        except (ValueError, AttributeError):
            return None
    
    def extract_key_amounts(self, extracted_data: Dict[str, Any]) -> Dict[str, Optional[float]]:
        """
        Extract and clean key dollar amounts from the extracted data.

        Args:
            extracted_data: Raw data dict from vision service

        Returns:
            Dict with cleaned float amounts
        """
        # Handle nested foreclosed_mortgage structure
        mortgage_data = extracted_data.get('foreclosed_mortgage', {}) or {}

        return {
            'total_judgment_amount': self._clean_amount(extracted_data.get('total_judgment_amount')),
            'principal_amount': self._clean_amount(extracted_data.get('principal_amount')),
            'interest_amount': self._clean_amount(extracted_data.get('interest_amount')),
            'attorney_fees': self._clean_amount(extracted_data.get('attorney_fees')),
            'court_costs': self._clean_amount(extracted_data.get('court_costs')),
            'original_mortgage_amount': self._clean_amount(mortgage_data.get('original_amount')),
            'monthly_payment': self._clean_amount(extracted_data.get('monthly_payment')),
            'escrow_advances': self._clean_amount(extracted_data.get('escrow_advances')),
            'late_charges': self._clean_amount(extracted_data.get('late_charges')),
            'per_diem_rate': self._clean_amount(extracted_data.get('per_diem_rate')),
        }

    def extract_dates(self, extracted_data: Dict[str, Any]) -> Dict[str, Optional[str]]:
        """
        Extract key dates from the extracted data.

        Args:
            extracted_data: Raw data dict from vision service

        Returns:
            Dict with date strings in YYYY-MM-DD format
        """
        mortgage_data = extracted_data.get('foreclosed_mortgage', {}) or {}
        lis_pendens_data = extracted_data.get('lis_pendens', {}) or {}

        return {
            'judgment_date': extracted_data.get('judgment_date'),
            'foreclosure_sale_date': extracted_data.get('foreclosure_sale_date'),
            'default_date': extracted_data.get('default_date'),
            'original_mortgage_date': mortgage_data.get('original_date'),
            'lis_pendens_date': lis_pendens_data.get('recording_date'),
            'interest_through_date': extracted_data.get('interest_through_date'),
        }

    def extract_parties(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract party information from the extracted data.

        Args:
            extracted_data: Raw data dict from vision service

        Returns:
            Dict with plaintiff, defendant(s), and party analysis
        """
        defendants = extracted_data.get('defendants', []) or []

        # Flatten defendants list to string for storage
        defendant_names = [d.get('name', '') for d in defendants if d.get('name')]

        # Check for federal entities
        has_federal = any(d.get('is_federal_entity', False) for d in defendants)
        federal_defendants = [d.get('name') for d in defendants if d.get('is_federal_entity')]

        # Check for deceased borrowers
        has_deceased = any(d.get('is_deceased', False) for d in defendants)

        return {
            'plaintiff': extracted_data.get('plaintiff'),
            'plaintiff_type': extracted_data.get('plaintiff_type'),
            'defendant': ', '.join(defendant_names),
            'defendants_list': defendants,
            'has_federal_defendant': has_federal,
            'federal_defendants': federal_defendants,
            'has_deceased_borrower': has_deceased,
        }

    def extract_property_info(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract property information from the extracted data.

        Args:
            extracted_data: Raw data dict from vision service

        Returns:
            Dict with property details including legal description
        """
        return {
            'property_address': extracted_data.get('property_address'),
            'legal_description': extracted_data.get('legal_description'),
            'parcel_id': extracted_data.get('parcel_id'),
            'subdivision': extracted_data.get('subdivision'),
            'lot': extracted_data.get('lot'),
            'block': extracted_data.get('block'),
            'unit': extracted_data.get('unit'),
            'plat_book': extracted_data.get('plat_book'),
            'plat_page': extracted_data.get('plat_page'),
            'is_condo': extracted_data.get('is_condo', False),
        }

    def extract_recording_refs(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract recording references from the extracted data.

        Args:
            extracted_data: Raw data dict from vision service

        Returns:
            Dict with book/page and instrument references
        """
        mortgage_data = extracted_data.get('foreclosed_mortgage', {}) or {}
        lis_pendens_data = extracted_data.get('lis_pendens', {}) or {}

        return {
            'mortgage_book': mortgage_data.get('recording_book'),
            'mortgage_page': mortgage_data.get('recording_page'),
            'mortgage_instrument': mortgage_data.get('instrument_number'),
            'lis_pendens_book': lis_pendens_data.get('recording_book'),
            'lis_pendens_page': lis_pendens_data.get('recording_page'),
            'lis_pendens_instrument': lis_pendens_data.get('instrument_number'),
        }

    def extract_red_flags(self, extracted_data: Dict[str, Any]) -> list:
        """
        Extract red flags from the extracted data.

        Args:
            extracted_data: Raw data dict from vision service

        Returns:
            List of red flag dictionaries
        """
        return extracted_data.get('red_flags', []) or []

    def get_foreclosure_type(self, extracted_data: Dict[str, Any]) -> Optional[str]:
        """
        Get the foreclosure type from the extracted data.

        Args:
            extracted_data: Raw data dict from vision service

        Returns:
            Foreclosure type string
        """
        fc_type = extracted_data.get('foreclosure_type')
        if fc_type:
            # Normalize to our expected values
            fc_type = fc_type.upper().strip()
            if 'FIRST' in fc_type or 'PRIMARY' in fc_type:
                return 'FIRST MORTGAGE'
            if 'SECOND' in fc_type or 'JUNIOR' in fc_type or 'HELOC' in fc_type:
                return 'SECOND MORTGAGE'
            if 'HOA' in fc_type or 'CONDO' in fc_type or 'ASSOCIATION' in fc_type:
                return 'HOA'
            if 'TAX' in fc_type:
                return 'TAX'
            return fc_type
        return None


if __name__ == "__main__":
    # Test the processor
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python -m src.services.final_judgment_processor <pdf_path>")
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    case_number = Path(pdf_path).stem.replace('_final_judgment', '')
    
    processor = FinalJudgmentProcessor()
    result = processor.process_pdf(pdf_path, case_number)
    
    if result:
        print("\n=== Extracted Data ===")
        import json
        print(json.dumps(result, indent=2))
        
        print("\n=== Cleaned Amounts ===")
        amounts = processor.extract_key_amounts(result)
        print(json.dumps(amounts, indent=2))
    else:
        print("Failed to extract data from PDF")
