"""
Service for extracting dollar amounts from encumbrance documents (mortgages, liens, etc.)
by downloading PDFs from ORI and analyzing them with vLLM vision.
"""
import os
from contextlib import suppress
from pathlib import Path
from typing import Optional, Dict, Any, List
from loguru import logger
import fitz  # PyMuPDF
import duckdb

from src.services.vision_service import VisionService
from src.scrapers.ori_api_scraper import ORIApiScraper


class EncumbranceAmountExtractor:
    """
    Downloads and analyzes encumbrance documents to extract dollar amounts.
    """

    def __init__(self, db_path: str = "data/property_master.db"):
        self.db_path = db_path
        self.vision_service = VisionService()
        self.ori_scraper = ORIApiScraper()
        self.temp_dir = Path("data/temp/encumbrance_images")
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def get_encumbrances_missing_amounts(self, limit: int = 100) -> List[Dict]:
        """
        Get encumbrances that are missing amounts.

        Args:
            limit: Maximum number of records to return

        Returns:
            List of encumbrance records with document info
        """
        conn = duckdb.connect(self.db_path, read_only=True)

        query = """
            SELECT DISTINCT
                e.id as encumbrance_id,
                e.folio,
                e.encumbrance_type,
                e.instrument,
                e.recording_date,
                e.creditor,
                d.id as document_id,
                d.extracted_data
            FROM encumbrances e
            LEFT JOIN documents d ON e.folio = d.folio
                AND e.instrument = d.instrument_number
            WHERE (e.amount IS NULL OR e.amount = 0)
              AND e.encumbrance_type LIKE '%MTG%'
              AND e.instrument IS NOT NULL
            ORDER BY e.recording_date DESC
            LIMIT ?
        """

        results = conn.execute(query, [limit]).fetchall()
        cols = ['encumbrance_id', 'folio', 'encumbrance_type', 'instrument',
                'recording_date', 'creditor', 'document_id', 'extracted_data']

        encumbrances = []
        for row in results:
            encumbrances.append(dict(zip(cols, row, strict=True)))

        conn.close()
        return encumbrances

    def download_and_extract_amount(self, instrument: str, folio: str,
                                    doc_type: str = "MTG") -> Optional[Dict[str, Any]]:
        """
        Download a document PDF and extract the dollar amount.

        Args:
            instrument: Instrument number
            folio: Folio/parcel ID for organizing files
            doc_type: Document type code

        Returns:
            Dict with amount data or None if failed
        """
        # Create output directory for this property
        output_dir = Path(f"data/properties/{folio}/documents")
        output_dir.mkdir(parents=True, exist_ok=True)

        # Build doc dict for download (need to search for the ID first)
        # The ORI API scraper needs the document ID, not just instrument
        doc = self._find_document_by_instrument(instrument)
        if not doc:
            logger.warning(f"Could not find document for instrument {instrument}")
            return None

        # Download PDF
        pdf_path = self.ori_scraper.download_pdf(doc, output_dir)
        if not pdf_path or not pdf_path.exists():
            logger.warning(f"Failed to download PDF for instrument {instrument}")
            return None

        logger.info(f"Downloaded PDF: {pdf_path}")

        # Extract amount from PDF
        return self.extract_amount_from_pdf(str(pdf_path), instrument)

    def _find_document_by_instrument(self, instrument: str) -> Optional[Dict]:
        """
        Search ORI API to find document by instrument number.

        Args:
            instrument: Instrument number

        Returns:
            Document dict with ID or None
        """
        try:
            # Search by instrument number
            results = self.ori_scraper.search_by_instrument(instrument)
            if results:
                return results[0]
        except Exception as e:
            logger.error(f"Error searching for instrument {instrument}: {e}")

        return None

    def extract_amount_from_pdf(self, pdf_path: str, instrument: str) -> Optional[Dict[str, Any]]:
        """
        Extract dollar amount from a PDF document using vLLM vision.

        Args:
            pdf_path: Path to the PDF file
            instrument: Instrument number for logging

        Returns:
            Dict with amount data or None if failed
        """
        if not os.path.exists(pdf_path):
            logger.error(f"PDF not found: {pdf_path}")
            return None

        try:
            # Open PDF
            doc = fitz.open(pdf_path)
            num_pages = min(len(doc), 3)  # Only need first 1-3 pages for amount

            # Render first page(s) to images
            page_images = []
            for page_num in range(num_pages):
                page = doc[page_num]
                temp_image_path = self.temp_dir / f"{instrument}_page_{page_num + 1}.png"
                pix = page.get_pixmap(dpi=200)
                pix.save(str(temp_image_path))
                page_images.append(str(temp_image_path))

            doc.close()

            # Extract amount using vision service
            if len(page_images) == 1:
                result = self.vision_service.extract_encumbrance_amount(page_images[0])
            else:
                result = self.vision_service.extract_encumbrance_amount_multi(page_images)

            # Clean up temp images
            for img_path in page_images:
                with suppress(Exception):
                    Path(img_path).unlink()

            if result:
                result['instrument'] = instrument
                result['pdf_path'] = pdf_path
                logger.info(f"Extracted amount for {instrument}: {result.get('amount')} "
                           f"(confidence: {result.get('confidence')})")

            return result

        except Exception as e:
            logger.error(f"Error extracting amount from {pdf_path}: {e}")
            return None

    def update_encumbrance_amount(
        self,
        encumbrance_id: int,
        amount: float,
        confidence: str,
        source_phrase: str | None = None,
    ) -> bool:
        """
        Update an encumbrance record with the extracted amount.

        Args:
            encumbrance_id: ID of the encumbrance record
            amount: Dollar amount to set
            confidence: Confidence level (high/medium/low)
            source_phrase: The text where amount was found

        Returns:
            True if updated successfully
        """
        try:
            conn = duckdb.connect(self.db_path)

            conn.execute("""
                UPDATE encumbrances
                SET amount = ?,
                    amount_confidence = ?,
                    amount_flags = ?
                WHERE id = ?
            """, [amount, confidence.upper(), source_phrase or '', encumbrance_id])

            conn.close()
            logger.info(f"Updated encumbrance {encumbrance_id} with amount ${amount:,.2f}")
            return True

        except Exception as e:
            logger.error(f"Error updating encumbrance {encumbrance_id}: {e}")
            return False

    def process_missing_amounts(self, limit: int = 50) -> Dict[str, int]:
        """
        Process encumbrances missing amounts - download PDFs and extract amounts.

        Args:
            limit: Maximum number to process

        Returns:
            Summary stats dict
        """
        stats = {
            'processed': 0,
            'success': 0,
            'failed': 0,
            'no_pdf': 0,
            'low_confidence': 0
        }

        encumbrances = self.get_encumbrances_missing_amounts(limit)
        logger.info(f"Found {len(encumbrances)} encumbrances missing amounts")

        for enc in encumbrances:
            stats['processed'] += 1
            instrument = enc['instrument']
            folio = enc['folio']
            enc_id = enc['encumbrance_id']

            logger.info(f"Processing {instrument} for folio {folio}")

            result = self.download_and_extract_amount(instrument, folio)

            if not result:
                stats['no_pdf'] += 1
                continue

            amount = result.get('amount')
            confidence = result.get('confidence', 'low')

            if amount is None:
                stats['failed'] += 1
                continue

            if confidence == 'low':
                stats['low_confidence'] += 1
                # Still update but flag it
                logger.warning(f"Low confidence amount for {instrument}: ${amount:,.2f}")

            # Update database
            if self.update_encumbrance_amount(
                enc_id, amount, confidence, result.get('source_phrase')
            ):
                stats['success'] += 1
            else:
                stats['failed'] += 1

        return stats


if __name__ == "__main__":
    import sys

    extractor = EncumbranceAmountExtractor()

    if len(sys.argv) > 1:
        # Process specific instrument
        instrument = sys.argv[1]
        folio = sys.argv[2] if len(sys.argv) > 2 else "test"
        result = extractor.download_and_extract_amount(instrument, folio)
        if result:
            import json
            print(json.dumps(result, indent=2))
        else:
            print("Failed to extract amount")
    else:
        # Process batch
        stats = extractor.process_missing_amounts(limit=10)
        print("\n=== Processing Complete ===")
        for key, value in stats.items():
            print(f"  {key}: {value}")
