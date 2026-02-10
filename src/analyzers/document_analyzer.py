import fitz  # PyMuPDF
import easyocr
import os
from contextlib import suppress
from loguru import logger

class DocumentAnalyzer:
    def __init__(self):
        self.reader = None

    def _init_reader(self):
        if not self.reader:
            print("Initializing EasyOCR for Documents...")
            self.reader = easyocr.Reader(['en'])

    def extract_text_from_pdf(self, pdf_path: str, max_pages: int = 3) -> str:
        """
        Extracts text from a PDF using OCR.
        Converts pages to images first to handle scanned documents.
        """
        if not os.path.exists(pdf_path):
            return ""

        # self._init_reader() # EasyOCR no longer primary
        full_text = []
        
        from src.services.vision_service import VisionService
        vision = VisionService()

        try:
            doc = fitz.open(pdf_path)

            for i in range(min(len(doc), max_pages)):
                page = doc[i]
                    
                # Render page to image (pixmap)
                # Zoom = 2 for better resolution
                pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                
                # Save temp image
                temp_img = f"temp_page_{i}.png"
                pix.save(temp_img)
                
                try:
                    # Use Vision API
                    print(f"  Vision API processing page {i+1} of {pdf_path}...")
                    page_text = vision.extract_text(temp_img)
                    
                    if not page_text:
                        print("  Vision API returned empty text, falling back to EasyOCR...")
                        self._init_reader()
                        result = self.reader.readtext(temp_img, detail=0)
                        page_text = " ".join(result)
                        
                    full_text.append(f"--- Page {i+1} ---\n{page_text}")
                finally:
                    # Cleanup temp image
                    if os.path.exists(temp_img):
                        os.remove(temp_img)
                        
            doc.close()
            
        except Exception as e:
            logger.error(f"Error processing PDF {pdf_path}: {e}")
            return f"Error extracting text: {e}"

        return "\n\n".join(full_text)

    def parse_judgment_details(self, text: str) -> dict:
        """
        Attempts to extract specific details from the judgment text.
        """
        import re
        details = {}
        
        # Normalize text
        text = text.replace('\n', ' ').replace('  ', ' ')
        
        # 1. Find Money Amounts (e.g., $123,456.78)
        # We look for the largest amount, which is usually the total judgment
        amounts = re.findall(r'\$[\d,]+\.\d{2}', text)
        if amounts:
            # Clean and convert to float
            valid_amounts = []
            for amt in amounts:
                with suppress(Exception):
                    val = float(amt.replace('$', '').replace(',', ''))
                    valid_amounts.append(val)
            
            if valid_amounts:
                details['judgment_amount'] = max(valid_amounts)
                
        # 2. Find Interest Rate (e.g., 4.75% or 4.75 %)
            interest = re.search(r'(\d+\.?\d*)\s*%', text)
            if interest:
                with suppress(Exception):
                    details['interest_rate'] = float(interest.group(1))
                
        # 3. Find Case Number (e.g., 20-CA-1234)
        # This is hard because OCR might mess it up, but let's try standard formats
        case_match = re.search(r'\d{2,4}[- ]*[A-Za-z]{2}[- ]*\d+', text)
        if case_match:
            details['case_number_ref'] = case_match.group(0)
            
        return details
