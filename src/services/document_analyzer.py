"""
Unified document analysis service for extracting structured data from ORI documents.
Downloads PDFs, converts to images, and uses vLLM for intelligent extraction.
"""
import os
from contextlib import suppress
from pathlib import Path
from typing import Optional, Dict, Any, List
from loguru import logger
import fitz  # PyMuPDF
import sqlite3
import json

from src.services.vision_service import VisionService
from src.scrapers.ori_api_scraper import ORIApiScraper
from src.db.sqlite_paths import resolve_sqlite_db_path_str


class DocumentAnalyzer:
    """
    Downloads and analyzes recorded documents from ORI to extract structured data
    including amounts, legal descriptions, party information, and red flags.
    """

    # Document type mappings for categorization
    DEED_TYPES = {'D', 'WD', 'QC', 'SWD', 'TD', 'CD', 'PRD', 'CT'}
    MORTGAGE_TYPES = {'MTG', 'MTGNT', 'MTGNIT', 'DOT', 'HELOC'}
    LIEN_TYPES = {'LN', 'LIEN', 'JUD', 'TL', 'ML', 'HOA', 'COD', 'MECH'}
    SATISFACTION_TYPES = {'SAT', 'REL', 'SATMTG', 'RELMTG'}
    ASSIGNMENT_TYPES = {'ASG', 'ASGN', 'ASGNMTG', 'ASSIGN', 'ASINT'}
    LIS_PENDENS_TYPES = {'LP', 'LISPEN'}
    NOC_TYPES = {'NOC'}
    AFFIDAVIT_TYPES = {'AFF', 'AFFD'}
    FINAL_JUDGMENT_TYPES = {'FJ'}

    def __init__(self, db_path: str | None = None):
        self.db_path = db_path or resolve_sqlite_db_path_str()
        self.vision_service = VisionService()
        self.ori_scraper = ORIApiScraper()
        self.temp_dir = Path("data/temp/doc_images")
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def _normalize_doc_type(self, doc_type: str) -> str:
        """Normalize document type code for routing."""
        # Handle format like "(MTG) MORTGAGE" -> "MTG"
        doc_type = doc_type.upper().replace('(', '').replace(')', '')
        # Get just the code part
        if ' ' in doc_type:
            doc_type = doc_type.split()[0]
        return doc_type

    def _get_doc_category(self, doc_type: str) -> str:
        """Categorize document type code."""
        doc_type = self._normalize_doc_type(doc_type)

        if doc_type in self.DEED_TYPES:
            return 'deed'
        if doc_type in self.MORTGAGE_TYPES:
            return 'mortgage'
        if doc_type in self.LIEN_TYPES:
            return 'lien'
        if doc_type in self.SATISFACTION_TYPES:
            return 'satisfaction'
        if doc_type in self.ASSIGNMENT_TYPES:
            return 'assignment'
        if doc_type in self.LIS_PENDENS_TYPES:
            return 'lis_pendens'
        if doc_type in self.NOC_TYPES:
            return 'noc'
        if doc_type in self.AFFIDAVIT_TYPES:
            return 'affidavit'
        if doc_type in self.FINAL_JUDGMENT_TYPES:
            return 'final_judgment'
        return 'other'

    def analyze_document(
        self,
        pdf_path: str,
        doc_type: str,
        instrument: str | None = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Analyze a PDF document and extract structured data based on type.

        Args:
            pdf_path: Path to the PDF file
            doc_type: Document type code (e.g., 'MTG', 'D', 'LN')
            instrument: Instrument number for logging

        Returns:
            Extracted data dict or None if failed
        """
        if not os.path.exists(pdf_path):
            logger.error(f"PDF not found: {pdf_path}")
            return None

        try:
            # Convert PDF to images
            page_images = self._pdf_to_images(pdf_path, instrument or 'doc')

            if not page_images:
                logger.error(f"Failed to convert PDF to images: {pdf_path}")
                return None

            # Use the unified document type routing method
            normalized_type = self._normalize_doc_type(doc_type)
            category = self._get_doc_category(doc_type)

            # Route to appropriate extraction method
            if len(page_images) == 1:
                analysis_strategy = "single_page"
                result = self.vision_service.extract_document_by_type(
                    page_images[0], normalized_type
                )
            else:
                analysis_strategy = "multi"
                result = self.vision_service.extract_document_by_type_multi(
                    page_images, normalized_type
                )

            # If multi-image extraction failed, fall back to per-page extraction
            if not result and analysis_strategy != "single_page":
                analysis_strategy = "per_page"
                logger.warning(
                    f"Document extraction returned no data for {doc_type} {instrument}; "
                    "falling back to per-page extraction"
                )
                page_results: list[dict[str, Any]] = []
                for image_path in page_images:
                    page_result = self.vision_service.extract_document_by_type(
                        image_path, normalized_type
                    )
                    if page_result:
                        page_results.append(page_result)

                if page_results:
                    merged = page_results[0]
                    for page_result in page_results[1:]:
                        merged = self._merge_extracted_data(merged, page_result)
                    result = merged

            # Add category to result
            if result:
                result['_category'] = category

            # Clean up temp images
            self._cleanup_images(page_images)

            if result:
                result['_analysis_metadata'] = {
                    'pdf_path': pdf_path,
                    'doc_type': doc_type,
                    'instrument': instrument,
                    'category': category,
                    'pages_analyzed': len(page_images),
                    'analysis_strategy': analysis_strategy,
                }

            return result

        except Exception as e:
            logger.error(f"Error analyzing document {pdf_path}: {e}")
            return None

    def _pdf_to_images(
        self,
        pdf_path: str,
        prefix: str,
        max_pages: int = 1,
        dpi: int = 120,
    ) -> List[str]:
        """
        Convert PDF pages to images.

        Args:
            pdf_path: Path to PDF
            prefix: Filename prefix for temp images
            max_pages: Maximum pages to convert (most docs only need first few)
            dpi: Image resolution

        Returns:
            List of image paths
        """
        try:
            doc = fitz.open(pdf_path)
            num_pages = min(len(doc), max_pages)

            page_images = []
            
            # Ensure temp dir exists (in case it was deleted)
            self.temp_dir.mkdir(parents=True, exist_ok=True)
            
            for i in range(num_pages):
                page = doc[i]
                img_path = self.temp_dir / f"{prefix}_p{i+1}.png"
                pix = page.get_pixmap(dpi=dpi)
                pix.save(str(img_path))
                page_images.append(str(img_path))

            doc.close()
            return page_images

        except Exception as e:
            logger.error(f"Error converting PDF to images: {e}")
            return []

    def _cleanup_images(self, image_paths: List[str]):
        """Remove temporary image files."""
        for path in image_paths:
            with suppress(Exception):
                Path(path).unlink(missing_ok=True)

    def _merge_extracted_data(self, base: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
        """Merge extracted data, filling missing fields from the update dict."""
        merged = dict(base)
        for key, value in update.items():
            if value is None:
                continue
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = self._merge_extracted_data(merged[key], value)
                continue
            if not merged.get(key):
                merged[key] = value
        return merged

    def download_and_analyze(
        self,
        instrument: str,
        folio: str,
        doc_type: str | None = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Download a document from ORI and analyze it.

        Args:
            instrument: Instrument number
            folio: Folio for organizing files
            doc_type: Document type (if known)

        Returns:
            Extracted data dict or None
        """
        # Search for document
        docs = self.ori_scraper.search_by_instrument(instrument)
        if not docs:
            logger.warning(f"No document found for instrument {instrument}")
            return None

        doc = docs[0]
        doc_type = doc_type or doc.get('DocType', 'UNKNOWN')

        # Create output directory
        output_dir = Path(f"data/properties/{folio}/documents")
        output_dir.mkdir(parents=True, exist_ok=True)

        # Download PDF
        pdf_path = self.ori_scraper.download_pdf(doc, output_dir)
        if not pdf_path:
            logger.warning(f"Failed to download PDF for {instrument}")
            return None

        # Analyze
        return self.analyze_document(str(pdf_path), doc_type, instrument)

    def process_documents_for_folio(
        self,
        folio: str,
        doc_types: List[str] | None = None,
    ) -> Dict[str, Any]:
        """
        Process all or specific document types for a folio.

        Args:
            folio: Property folio
            doc_types: List of document types to process (None = all)

        Returns:
            Summary of processing results
        """
        results = {
            'folio': folio,
            'processed': 0,
            'success': 0,
            'failed': 0,
            'documents': []
        }

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        # Get documents for this folio
        query = """
            SELECT instrument_number, document_type, recording_date
            FROM documents
            WHERE folio = ?
              AND instrument_number IS NOT NULL
        """
        params = [folio]

        if doc_types:
            placeholders = ','.join(['?' for _ in doc_types])
            query += f" AND document_type IN ({placeholders})"
            params.extend(doc_types)

        docs = conn.execute(query, params).fetchall()
        conn.close()

        for instrument, doc_type, rec_date in docs:
            results['processed'] += 1

            try:
                data = self.download_and_analyze(instrument, folio, doc_type)

                if data:
                    results['success'] += 1
                    results['documents'].append({
                        'instrument': instrument,
                        'doc_type': doc_type,
                        'recording_date': str(rec_date),
                        'extracted_data': data
                    })
                else:
                    results['failed'] += 1

            except Exception as e:
                logger.error(f"Error processing {instrument}: {e}")
                results['failed'] += 1

        return results

    def update_encumbrance_from_analysis(self, encumbrance_id: int,
                                          analysis: Dict[str, Any]) -> bool:
        """
        Update an encumbrance record with analyzed data.

        Args:
            encumbrance_id: Database ID of encumbrance
            analysis: Extracted data from document analysis

        Returns:
            True if updated successfully
        """
        try:
            conn = sqlite3.connect(self.db_path)

            # Extract relevant fields based on document type
            amount = None
            creditor = None
            confidence = analysis.get('confidence', 'low')

            if 'principal_amount' in analysis:
                # Mortgage
                amount = analysis.get('principal_amount')
                creditor = analysis.get('lender')
            elif 'amount' in analysis:
                # Lien or generic
                amount = analysis.get('amount')
                creditor = analysis.get('creditor')

            # Build update
            updates = []
            params = []

            if amount and amount > 0:
                updates.append("amount = ?")
                params.append(amount)
                updates.append("amount_confidence = ?")
                params.append(confidence.upper())

            if creditor:
                updates.append("creditor = ?")
                params.append(creditor)

            if not updates:
                conn.close()
                return False

            params.append(encumbrance_id)
            query = f"UPDATE encumbrances SET {', '.join(updates)} WHERE id = ?"

            conn.execute(query, params)
            conn.commit()
            conn.close()

            logger.info(f"Updated encumbrance {encumbrance_id}: amount=${amount}, creditor={creditor}")
            return True

        except Exception as e:
            logger.error(f"Error updating encumbrance {encumbrance_id}: {e}")
            return False

    def update_chain_from_deed_analysis(self, chain_id: int,
                                         analysis: Dict[str, Any]) -> bool:
        """
        Update a chain_of_title record with deed analysis data.

        Args:
            chain_id: Database ID of chain record
            analysis: Extracted deed data

        Returns:
            True if updated successfully
        """
        try:
            conn = sqlite3.connect(self.db_path)

            updates = []
            params = []

            # Owner name (grantee)
            if analysis.get('grantee'):
                updates.append("owner_name = ?")
                params.append(analysis['grantee'])

            # Acquisition price (consideration)
            if analysis.get('consideration') and analysis['consideration'] > 10:
                updates.append("acquisition_price = ?")
                params.append(analysis['consideration'])

            if not updates:
                conn.close()
                return False

            params.append(chain_id)
            query = f"UPDATE chain_of_title SET {', '.join(updates)} WHERE id = ?"

            conn.execute(query, params)
            conn.commit()
            conn.close()

            logger.info(f"Updated chain {chain_id} with deed data")
            return True

        except Exception as e:
            logger.error(f"Error updating chain {chain_id}: {e}")
            return False


def process_missing_amounts(limit: int = 50) -> Dict[str, int]:
    """
    Process encumbrances missing amounts.

    Args:
        limit: Maximum number to process

    Returns:
        Summary stats
    """
    analyzer = DocumentAnalyzer()
    stats = {'processed': 0, 'success': 0, 'failed': 0}

    conn = sqlite3.connect(analyzer.db_path)
    conn.row_factory = sqlite3.Row

    # Get encumbrances missing amounts
    query = """
        SELECT e.id, e.folio, e.instrument, e.encumbrance_type
        FROM encumbrances e
        WHERE (e.amount IS NULL OR e.amount = 0)
          AND e.instrument IS NOT NULL
          AND e.encumbrance_type LIKE '%MTG%'
        LIMIT ?
    """

    encumbrances = conn.execute(query, [limit]).fetchall()
    conn.close()

    logger.info(f"Processing {len(encumbrances)} encumbrances missing amounts")

    for enc_id, folio, instrument, enc_type in encumbrances:
        stats['processed'] += 1
        logger.info(f"[{stats['processed']}/{len(encumbrances)}] Processing {instrument}")

        try:
            analysis = analyzer.download_and_analyze(instrument, folio, enc_type)

            if analysis and analyzer.update_encumbrance_from_analysis(enc_id, analysis):
                stats['success'] += 1
            else:
                stats['failed'] += 1

        except Exception as e:
            logger.error(f"Error: {e}")
            stats['failed'] += 1

    return stats


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        if sys.argv[1] == '--batch':
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
            stats = process_missing_amounts(limit)
            print(f"\n=== Results ===")
            for k, v in stats.items():
                print(f"  {k}: {v}")
        else:
            # Analyze single document
            pdf_path = sys.argv[1]
            doc_type = sys.argv[2] if len(sys.argv) > 2 else 'D'

            analyzer = DocumentAnalyzer()
            result = analyzer.analyze_document(pdf_path, doc_type)

            if result:
                print(json.dumps(result, indent=2))
            else:
                print("Failed to analyze document")
    else:
        print("Usage:")
        print("  python -m src.services.document_analyzer <pdf_path> [doc_type]")
        print("  python -m src.services.document_analyzer --batch [limit]")
