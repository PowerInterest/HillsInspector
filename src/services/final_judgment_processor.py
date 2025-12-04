import os
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
            num_pages = total_pages  # Process all pages to capture full judgment details
            
            logger.info(f"PDF has {total_pages} pages, processing all pages")
            
            # Process each page and merge results
            all_data = []
            all_text = []  # Store raw OCR text for debugging
            
            for page_num in range(num_pages):
                page = doc[page_num]
                temp_image_path = self.temp_dir / f"{case_number}_page_{page_num + 1}.png"
                
                # Render page to image (200 DPI)
                pix = page.get_pixmap(dpi=200)
                pix.save(str(temp_image_path))
                
                logger.info(f"Processing page {page_num + 1}...")
                
                # Extract structured data from this page
                page_data = self.vision_service.extract_final_judgment(str(temp_image_path))
                
                # Also extract raw text for debugging
                page_text = self.vision_service.extract_text(str(temp_image_path))
                
                if page_data:
                    all_data.append(page_data)
                    logger.info(f"Extracted structured data from page {page_num + 1}")
                
                if page_text:
                    all_text.append(f"=== PAGE {page_num + 1} ===\n{page_text}")
                    logger.info(f"Extracted raw text from page {page_num + 1} ({len(page_text)} chars)")
                
                # Clean up temp image
                temp_image_path.unlink()
            
            doc.close()
            
            if not all_data:
                logger.warning(f"No data extracted from PDF for case {case_number}")
                return None
            
            # Merge data from all pages (first page usually has most complete info)
            merged_data = self._merge_page_data(all_data)
            
            # Add raw text for debugging
            merged_data['raw_text'] = "\n\n".join(all_text)
            merged_data['_metadata'] = {
                'case_number': case_number,
                'pages_processed': num_pages,
                'total_pages': total_pages
            }
            
            logger.info(f"Successfully processed Final Judgment for case {case_number}")
            return merged_data
            
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
        return {
            'total_judgment_amount': self._clean_amount(extracted_data.get('total_judgment_amount')),
            'principal_amount': self._clean_amount(extracted_data.get('principal_amount')),
            'interest_amount': self._clean_amount(extracted_data.get('interest_amount')),
            'attorney_fees': self._clean_amount(extracted_data.get('attorney_fees')),
            'court_costs': self._clean_amount(extracted_data.get('court_costs')),
            'original_mortgage_amount': self._clean_amount(extracted_data.get('original_mortgage_amount')),
            'monthly_payment': self._clean_amount(extracted_data.get('monthly_payment'))
        }


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
