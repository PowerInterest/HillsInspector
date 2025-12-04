import asyncio
import json
from pathlib import Path
from src.services.final_judgment_processor import FinalJudgmentProcessor
from loguru import logger
import sys

# Configure logger
logger.remove()
logger.add(sys.stderr, level="INFO")

async def test_final_judgment_extraction():
    """
    Test Final Judgment PDF extraction on downloaded PDFs.
    """
    pdf_dir = Path("data/pdfs/final_judgments")
    
    if not pdf_dir.exists():
        logger.error(f"PDF directory not found: {pdf_dir}")
        logger.info("Run the auction scraper first to download PDFs")
        return
    
    # Get all PDFs
    pdfs = list(pdf_dir.glob("*.pdf"))
    
    if not pdfs:
        logger.error("No PDFs found in directory")
        return
    
    logger.info(f"Found {len(pdfs)} Final Judgment PDFs")
    
    processor = FinalJudgmentProcessor()
    
    # Process first PDF as a test
    test_pdf = pdfs[0]
    case_number = test_pdf.stem.replace('_final_judgment', '')
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Testing extraction on: {test_pdf.name}")
    logger.info(f"Case Number: {case_number}")
    logger.info(f"{'='*60}\n")
    
    result = processor.process_pdf(str(test_pdf), case_number)
    
    if result:
        logger.info("\n=== EXTRACTED DATA ===")
        # Don't print raw_text in main output (too long)
        display_result = {k: v for k, v in result.items() if k not in ['raw_text', '_metadata']}
        print(json.dumps(display_result, indent=2))
        
        logger.info("\n=== CLEANED AMOUNTS ===")
        amounts = processor.extract_key_amounts(result)
        print(json.dumps(amounts, indent=2))
        
        # Highlight key fields for lien analysis
        logger.info("\n=== KEY FIELDS FOR LIEN ANALYSIS ===")
        key_fields = {
            'Foreclosure Type': result.get('foreclosure_type'),
            'Plaintiff': result.get('plaintiff'),
            'Total Judgment': amounts.get('total_judgment_amount'),
            'Original Mortgage Amount': amounts.get('original_mortgage_amount'),
            'Lis Pendens Date': result.get('lis_pendens_date'),
            'Judgment Date': result.get('judgment_date')
        }
        print(json.dumps(key_fields, indent=2))
        
        # Show raw text for debugging
        logger.info("\n=== RAW OCR TEXT (first 500 chars) ===")
        raw_text = result.get('raw_text', '')
        print(raw_text[:500] if raw_text else "No raw text extracted")
        
        # Show metadata
        logger.info("\n=== METADATA ===")
        print(json.dumps(result.get('_metadata', {}), indent=2))
        
    else:
        logger.error("Failed to extract data from PDF")

if __name__ == "__main__":
    asyncio.run(test_final_judgment_extraction())
