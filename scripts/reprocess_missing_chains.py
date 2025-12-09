"""
Reprocess documents for folios that have documents but no chain of title.

This script:
1. Gets all documents for folios missing chain of title
2. Downloads PDFs from ORI (if not already downloaded) using browser-based approach
3. Runs vision extraction to get party data (grantor/grantee)
4. Updates the documents table with extracted party data
5. Rebuilds the chain of title
"""

import duckdb
import asyncio
from pathlib import Path
from loguru import logger
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.scrapers.ori_api_scraper import ORIApiScraper
from src.services.vision_service import VisionService
from src.services.title_chain_service import TitleChainService
from src.db.operations import PropertyDB


def get_folios_missing_chain(conn) -> list[str]:
    """Get folios that have documents but no chain of title."""
    result = conn.execute('''
        SELECT DISTINCT d.folio
        FROM documents d
        LEFT JOIN chain_of_title c ON d.folio = c.folio
        WHERE c.folio IS NULL
    ''').fetchall()
    return [row[0] for row in result]


def get_documents_for_folio(conn, folio: str) -> list[dict]:
    """Get all documents for a folio."""
    result = conn.execute('''
        SELECT id, folio, document_type, instrument_number, recording_date,
               party1, party2, book, page, file_path
        FROM documents
        WHERE folio = ?
        ORDER BY recording_date
    ''', [folio]).fetchdf()
    return result.to_dict('records')


async def download_pdf_if_needed(ori_scraper: ORIApiScraper, doc: dict, folio: str, use_fresh_context: bool = False) -> Path | None:
    """Download PDF if not already present using browser-based approach with anti-detection."""
    # Check if already downloaded
    if doc.get('file_path') and Path(doc['file_path']).exists():
        logger.debug(f"PDF already exists: {doc['file_path']}")
        return Path(doc['file_path'])

    instrument = doc.get('instrument_number')
    if not instrument:
        logger.warning(f"No instrument number for document {doc.get('id')}")
        return None

    # Skip negative/old instrument numbers (pre-digital era)
    if str(instrument).startswith('-'):
        logger.debug(f"Skipping pre-digital instrument: {instrument}")
        return None

    # Create output directory
    output_dir = Path(f"data/properties/{folio}/documents")
    output_dir.mkdir(parents=True, exist_ok=True)

    doc_type = doc.get('document_type', 'UNKNOWN')

    try:
        # Use browser-based download with anti-detection
        pdf_path = await ori_scraper.download_pdf_browser(
            str(instrument),
            output_dir,
            doc_type,
            headless=True,
            fresh_context=use_fresh_context
        )
        if pdf_path and pdf_path.exists():
            size = pdf_path.stat().st_size
            logger.info(f"Downloaded: {pdf_path.name} ({size} bytes)")
            return pdf_path
        else:
            logger.warning(f"Failed to download PDF for instrument {instrument}")
            return None
    except Exception as e:
        logger.error(f"Error downloading PDF for {instrument}: {e}")
        return None


def extract_parties_from_pdf(vision_service: VisionService, pdf_path: Path, doc_type: str) -> dict | None:
    """Extract party data from PDF using vision service."""
    try:
        import fitz  # PyMuPDF

        # Open PDF and convert pages to images
        doc = fitz.open(str(pdf_path))
        total_pages = len(doc)
        num_pages = min(3, total_pages)  # Process first 3 pages max

        if total_pages == 0:
            logger.warning(f"PDF has no pages: {pdf_path}")
            doc.close()
            return None

        # Save temp images
        temp_dir = pdf_path.parent / "temp_images"
        temp_dir.mkdir(exist_ok=True)

        image_paths = []
        for i in range(num_pages):
            page = doc[i]
            img_path = temp_dir / f"{pdf_path.stem}_page_{i+1}.png"
            pix = page.get_pixmap(dpi=150)
            pix.save(str(img_path))
            image_paths.append(str(img_path))

        doc.close()

        # Extract using appropriate method based on doc type
        doc_type_upper = doc_type.upper() if doc_type else ''

        # Use multi-page extraction for better accuracy
        if len(image_paths) > 1:
            result = vision_service.extract_document_by_type_multi(image_paths, doc_type_upper)
        else:
            result = vision_service.extract_document_by_type(image_paths[0], doc_type_upper)

        # Clean up temp images
        for img_path in image_paths:
            try:
                Path(img_path).unlink()
            except:
                pass
        try:
            temp_dir.rmdir()
        except:
            pass

        return result

    except Exception as e:
        logger.error(f"Error extracting from PDF {pdf_path}: {e}")
        return None


def update_document_parties(conn, doc_id: int, party1: str | None, party2: str | None, file_path: str | None):
    """Update document with extracted party data."""
    updates = []
    params = []

    if party1:
        updates.append("party1 = ?")
        params.append(party1)
    if party2:
        updates.append("party2 = ?")
        params.append(party2)
    if file_path:
        updates.append("file_path = ?")
        params.append(file_path)

    if updates:
        params.append(doc_id)
        sql = f"UPDATE documents SET {', '.join(updates)} WHERE id = ?"
        conn.execute(sql, params)
        conn.commit()


def build_chain_for_folio(conn, folio: str, chain_service: TitleChainService):
    """Build and save chain of title for a folio."""
    # Get all documents
    docs = conn.execute('''
        SELECT id, folio, document_type, instrument_number, recording_date,
               party1, party2, book, page, file_path
        FROM documents
        WHERE folio = ?
        ORDER BY recording_date
    ''', [folio]).fetchdf().to_dict('records')

    if not docs:
        logger.warning(f"No documents for folio {folio}")
        return

    # Map to format expected by chain service
    mapped_docs = []
    for doc in docs:
        mapped_docs.append({
            'document_type': doc.get('document_type'),
            'doc_type': doc.get('document_type'),  # Chain service uses doc_type
            'recording_date': str(doc.get('recording_date')) if doc.get('recording_date') else None,
            'party1': doc.get('party1'),
            'party2': doc.get('party2'),
            'book': doc.get('book'),
            'page': doc.get('page'),
            'instrument_number': doc.get('instrument_number'),
        })

    # Build chain
    analysis = chain_service.build_chain_and_analyze(mapped_docs)

    chain = analysis.get('chain', [])
    encumbrances = analysis.get('encumbrances', [])

    logger.info(f"Built chain for {folio}: {len(chain)} ownership periods, {len(encumbrances)} encumbrances")

    if not chain:
        logger.warning(f"No chain periods built for {folio}")
        return

    # Save to chain_of_title table
    for period in chain:
        conn.execute('''
            INSERT INTO chain_of_title (folio, owner_name, acquired_from, acquisition_date,
                                        disposition_date, acquisition_doc_type)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', [
            folio,
            period.get('grantee'),
            period.get('grantor'),
            period.get('date'),
            None,  # disposition_date - would need to calculate from next period
            period.get('doc_type')
        ])

    conn.commit()
    logger.success(f"Saved {len(chain)} chain periods for {folio}")


async def process_folio(folio: str, conn, ori_scraper: ORIApiScraper,
                        vision_service: VisionService, chain_service: TitleChainService,
                        is_first_folio: bool = False):
    """Process all documents for a single folio."""
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing folio: {folio}")
    logger.info(f"{'='*60}")

    docs = get_documents_for_folio(conn, folio)
    logger.info(f"Found {len(docs)} documents")

    updated_count = 0
    downloaded_count = 0

    for i, doc in enumerate(docs):
        doc_id = doc['id']
        doc_type = doc.get('document_type', '')
        instrument = doc.get('instrument_number', '')
        party1 = doc.get('party1')
        party2 = doc.get('party2')

        logger.info(f"  [{doc_id}] {doc_type} - Inst: {instrument}")
        logger.info(f"       Party1: {party1 or '(missing)'}")
        logger.info(f"       Party2: {party2 or '(missing)'}")

        # Download PDF if needed (use fresh context for first doc of first folio)
        use_fresh = is_first_folio and i == 0
        pdf_path = await download_pdf_if_needed(ori_scraper, doc, folio, use_fresh_context=use_fresh)

        if pdf_path:
            downloaded_count += 1

            # Update file_path in DB if it was just downloaded
            if not doc.get('file_path'):
                update_document_parties(conn, doc_id, None, None, str(pdf_path))

            # Only extract if party2 is missing (needed for chain)
            if not party2 or party2.strip() == '':
                logger.info(f"       Extracting parties from PDF...")
                extracted = extract_parties_from_pdf(vision_service, pdf_path, doc_type)

                if extracted:
                    # Get party names based on document type
                    new_party1 = None
                    new_party2 = None

                    doc_type_upper = (doc_type or '').upper()

                    if 'DEED' in doc_type_upper or doc_type_upper in ['D', 'WD', 'QC']:
                        new_party1 = extracted.get('grantor')
                        new_party2 = extracted.get('grantee')
                    elif 'MTG' in doc_type_upper or 'MORTGAGE' in doc_type_upper:
                        new_party1 = extracted.get('borrower')
                        new_party2 = extracted.get('lender')
                    elif 'LIEN' in doc_type_upper or 'LN' in doc_type_upper:
                        new_party1 = extracted.get('creditor')
                        new_party2 = extracted.get('debtor')
                    elif 'SAT' in doc_type_upper or 'REL' in doc_type_upper:
                        new_party1 = extracted.get('releasing_party')
                        new_party2 = extracted.get('property_owner')

                    # Update if we got new data
                    if new_party1 or new_party2:
                        update_document_parties(conn, doc_id,
                                               new_party1 if not party1 else None,
                                               new_party2 if not party2 else None,
                                               None)
                        if new_party2:
                            logger.success(f"       Extracted party2: {new_party2}")
                            updated_count += 1
                else:
                    logger.warning(f"       Vision extraction returned no data")

    logger.info(f"Downloaded: {downloaded_count}, Updated: {updated_count}")

    # Rebuild chain of title
    logger.info(f"Building chain of title...")
    build_chain_for_folio(conn, folio, chain_service)


async def main():
    logger.info("Starting reprocessing of folios with missing chain of title")

    # Initialize services
    conn = duckdb.connect('data/property_master.db', read_only=False)
    ori_scraper = ORIApiScraper()
    vision_service = VisionService()
    chain_service = TitleChainService()

    # Check vision service
    if not vision_service.check_server():
        logger.error("Vision service is not available!")
        return
    logger.success("Vision service is available")

    # Get folios to process
    folios = get_folios_missing_chain(conn)
    logger.info(f"Found {len(folios)} folios with documents but no chain")

    # Process each folio
    for i, folio in enumerate(folios):
        logger.info(f"\n[{i+1}/{len(folios)}] Processing {folio}")
        try:
            is_first = (i == 0)
            await process_folio(folio, conn, ori_scraper, vision_service, chain_service, is_first_folio=is_first)
        except Exception as e:
            logger.error(f"Error processing {folio}: {e}")
            continue

    # Close browser to clean up
    await ori_scraper.close_browser()

    conn.close()
    logger.success("Reprocessing complete!")


if __name__ == "__main__":
    asyncio.run(main())
