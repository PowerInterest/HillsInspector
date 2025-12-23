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
    
    def process_pdf(self, pdf_path: str, case_number: str) -> Optional[Dict[str, Any]]:
        """
        Process a Final Judgment PDF and extract structured data.
        
        Args:
            pdf_path: Path to the Final Judgment document (PDF, etc.)
            case_number: Case number for logging/tracking
            
        Returns:
            Dict with extracted data (including raw_text for debugging) or None if processing failed
        """
        if not os.path.exists(pdf_path):
            logger.error(f"PDF not found: {pdf_path}")
            return None
        
        try:
            logger.info(f"Processing Final Judgment PDF for case {case_number}...")
            
            # Open PDF with PyMuPDF
            doc = fitz.open(pdf_path)
            total_pages = len(doc)
            num_pages = total_pages  # Process all pages - server has 262k context

            logger.info(f"PDF has {total_pages} pages, processing all pages")
            
            # Render all pages to images
            page_images = []
            for page_num in range(num_pages):
                page = doc[page_num]
                temp_image_path = self.temp_dir / f"{case_number}_page_{page_num + 1}.png"
                pix = page.get_pixmap(dpi=200)
                pix.save(str(temp_image_path))
                page_images.append(str(temp_image_path))

            doc.close()

            # Single multi-image call for structured extraction
            logger.info(f"Sending {len(page_images)} pages to vision service in one batch...")
            merged_json = self.vision_service.extract_final_judgment_multi(page_images)

            # Also collect raw text per page (separate calls to keep transcript)
            all_text = []
            for idx, img_path in enumerate(page_images, start=1):
                page_text = self.vision_service.extract_text(img_path)
                if page_text:
                    all_text.append(f"=== PAGE {idx} ===\n{page_text}")

            # Clean up temp images
            for img_path in page_images:
                with suppress(Exception):
                    Path(img_path).unlink()

            if not merged_json:
                pages_with_text = len(all_text)
                logger.warning(
                    "No structured data extracted for case {} (OCR text on {}/{} pages)",
                    case_number,
                    pages_with_text,
                    total_pages,
                )
                return None

            merged_json['raw_text'] = "\n\n".join(all_text)
            merged_json['_metadata'] = {
                'case_number': case_number,
                'pages_processed': num_pages,
                'total_pages': total_pages
            }

            logger.info(f"Successfully processed Final Judgment for case {case_number}")
            return merged_json
            
        except Exception as e:
            logger.error(f"Error processing PDF for case {case_number}: {e}")
            return None
    
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
