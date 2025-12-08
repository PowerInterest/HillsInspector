"""
Batch extract Final Judgment data from existing PDFs.

Processes all Final Judgment PDFs in data/pdfs/final_judgments/
and stores the extracted data in the database.
"""

import asyncio
import json
import argparse
from pathlib import Path
from datetime import datetime
from loguru import logger
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
from src.services.final_judgment_processor import FinalJudgmentProcessor

logger.remove()
logger.add(sys.stderr, level="INFO")


def get_database_connection():
    """Get connection to the database."""
    db_path = Path("data/property_master.db")
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        return None
    return duckdb.connect(str(db_path))


def store_judgment_data(conn, case_number: str, extracted_data: dict):
    """Store extracted judgment data in the database."""
    
    # Get cleaned amounts
    processor = FinalJudgmentProcessor()
    amounts = processor.extract_key_amounts(extracted_data)

    def _parse_date(value):
        """Normalize various date formats to ISO YYYY-MM-DD or return None."""
        if not value:
            return None
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
            # Support MM/DD/YYYY from Vision output and already-ISO strings
            for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
                try:
                    return datetime.strptime(value, fmt).date().isoformat()
                except ValueError:
                    continue
        logger.debug(f"Unrecognized date format for case {case_number}: {value!r}")
        return None
    
    # Prepare data for database
    update_data = {
        'plaintiff': extracted_data.get('plaintiff'),
        'defendant': extracted_data.get('defendant'),
        'foreclosure_type': extracted_data.get('foreclosure_type'),
        'judgment_date': _parse_date(extracted_data.get('judgment_date')),
        'lis_pendens_date': _parse_date(extracted_data.get('lis_pendens_date')),
        'foreclosure_sale_date': _parse_date(extracted_data.get('foreclosure_sale_date')),
        'total_judgment_amount': amounts.get('total_judgment_amount'),
        'principal_amount': amounts.get('principal_amount'),
        'interest_amount': amounts.get('interest_amount'),
        'attorney_fees': amounts.get('attorney_fees'),
        'court_costs': amounts.get('court_costs'),
        'original_mortgage_amount': amounts.get('original_mortgage_amount'),
        'original_mortgage_date': _parse_date(extracted_data.get('original_mortgage_date')),
        'monthly_payment': amounts.get('monthly_payment'),
        'default_date': _parse_date(extracted_data.get('default_date')),
        'extracted_judgment_data': json.dumps(extracted_data),
        'raw_judgment_text': extracted_data.get('raw_text', ''),
        'judgment_extracted_at': datetime.now().isoformat(),
        'case_number': case_number
    }
    
    # Update the auction record
    try:
        # Build SET clause dynamically
        set_clauses = []
        params = []
        
        for key, value in update_data.items():
            if key != 'case_number' and value is not None:
                set_clauses.append(f"{key} = ?")
                params.append(value)
        
        if not set_clauses:
            logger.warning(f"No data to update for case {case_number}")
            return False
        
        # Add case_number for WHERE clause
        params.append(case_number)
        
        sql = f"""
            UPDATE auctions 
            SET {', '.join(set_clauses)}
            WHERE case_number = ?
        """
        
        conn.execute(sql, params)
        logger.success(f"✓ Stored data for case {case_number}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to store data for case {case_number}: {e}")
        return False


def batch_extract_judgments(force: bool = False):
    """Extract data from all Final Judgment PDFs."""
    
    pdf_dir = Path("data/pdfs/final_judgments")
    
    if not pdf_dir.exists():
        logger.error(f"PDF directory not found: {pdf_dir}")
        logger.info("Run the auction scraper first to download PDFs")
        return False
    
    # Get all PDFs
    pdfs = list(pdf_dir.glob("*.pdf"))
    
    if not pdfs:
        logger.warning("No PDFs found in directory")
        return False
    
    logger.info("=" * 60)
    logger.info(f"BATCH FINAL JUDGMENT EXTRACTION")
    logger.info("=" * 60)
    logger.info(f"Found {len(pdfs)} Final Judgment PDFs")
    logger.info(f"Force Re-extraction: {force}")
    logger.info("")
    
    # Connect to database
    conn = get_database_connection()
    if not conn:
        return False
    
    # Initialize processor
    processor = FinalJudgmentProcessor()
    
    # Process each PDF
    results = {
        'success': 0,
        'failed': 0,
        'skipped': 0
    }
    
    for i, pdf_path in enumerate(pdfs, 1):
        # Extract case number from filename
        case_number = pdf_path.stem.replace('_final_judgment', '')
        
        logger.info(f"\n[{i}/{len(pdfs)}] Processing: {pdf_path.name}")
        logger.info(f"Case Number: {case_number}")
        
        # Check if already extracted
        if not force:
            existing = conn.execute(
                "SELECT judgment_extracted_at FROM auctions WHERE case_number = ?",
                [case_number]
            ).fetchone()
            
            if existing and existing[0]:
                logger.info(f"Already extracted on {existing[0]}, skipping...")
                results['skipped'] += 1
                continue
        
        # Extract data
        try:
            extracted_data = processor.process_pdf(str(pdf_path), case_number)
            
            if extracted_data:
                # Store in database
                if store_judgment_data(conn, case_number, extracted_data):
                    results['success'] += 1
                    
                    # Show key extracted fields
                    logger.info(f"  Foreclosure Type: {extracted_data.get('foreclosure_type')}")
                    logger.info(f"  Plaintiff: {extracted_data.get('plaintiff')}")
                    amounts = processor.extract_key_amounts(extracted_data)
                    if amounts.get('total_judgment_amount'):
                        logger.info(f"  Total Judgment: ${amounts['total_judgment_amount']:,.2f}")
                else:
                    results['failed'] += 1
            else:
                logger.error(f"Failed to extract data from {pdf_path.name}")
                results['failed'] += 1
                
        except Exception as e:
            logger.error(f"Error processing {pdf_path.name}: {e}")
            results['failed'] += 1
    
    conn.close()
    
    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("BATCH EXTRACTION COMPLETE")
    logger.info("=" * 60)
    logger.success(f"Successfully extracted: {results['success']}")
    logger.info(f"Skipped (already done): {results['skipped']}")
    logger.error(f"Failed: {results['failed']}")
    logger.info(f"Total processed: {len(pdfs)}")
    logger.info("=" * 60)
    
    return results['failed'] == 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Batch extract Final Judgment data.')
    parser.add_argument('--force', action='store_true', help='Force re-extraction of existing data')
    args = parser.parse_args()
    
    success = batch_extract_judgments(force=args.force)
    sys.exit(0 if success else 1)
