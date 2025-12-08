"""Test Vision API connection and Final Judgment extraction."""
import sys
from pathlib import Path
from loguru import logger
from src.services.vision_service import VisionService

# Configure detailed logging
logger.remove()
logger.add(sys.stderr, level="DEBUG")

def test_connection():
    """Test basic connection to Vision API."""
    logger.info("Testing Vision API connection...")
    vs = VisionService()
    
    # Test server availability
    logger.info(f"API URL: {vs.API_URL}")
    is_available = vs.check_server()
    logger.info(f"Server available: {is_available}")
    
    if not is_available:
        logger.error("Vision API server is not responding!")
        return False
    
    logger.success("✓ Vision API server is responding")
    return True

def test_simple_extraction():
    """Test simple text extraction."""
    logger.info("\nTesting simple image analysis...")
    vs = VisionService()
    
    # Find any image to test with
    test_images = list(Path("data/temp/pdf_images").glob("*.png")) if Path("data/temp/pdf_images").exists() else []
    
    if not test_images:
        logger.warning("No test images found. Creating one from a PDF...")
        import fitz
        pdf_dir = Path("data/pdfs/final_judgments")
        pdfs = list(pdf_dir.glob("*.pdf"))
        
        if not pdfs:
            logger.error("No PDFs found to test with")
            return False
        
        # Convert first page of first PDF
        doc = fitz.open(str(pdfs[0]))
        page = doc[0]
        temp_dir = Path("data/temp/pdf_images")
        temp_dir.mkdir(parents=True, exist_ok=True)
        temp_image = temp_dir / "test_page.png"
        pix = page.get_pixmap(dpi=200)
        pix.save(str(temp_image))
        doc.close()
        logger.info(f"Created test image: {temp_image}")
        test_images = [temp_image]
    
    test_image = test_images[0]
    logger.info(f"Using test image: {test_image}")
    
    # Try simple text extraction
    result = vs.extract_text(str(test_image))
    
    if result:
        logger.success(f"✓ Successfully extracted text ({len(result)} characters)")
        logger.info(f"First 200 chars: {result[:200]}")
        return True
    else:
        logger.error("Failed to extract text from image")
        return False

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Vision API Diagnostic Test")
    logger.info("=" * 60)
    
    # Test 1: Connection
    if not test_connection():
        logger.error("\n❌ Connection test failed. Cannot proceed.")
        sys.exit(1)
    
    # Test 2: Simple extraction
    if not test_simple_extraction():
        logger.error("\n❌ Extraction test failed.")
        sys.exit(1)
    
    logger.success("\n✓ All tests passed! Vision API is working correctly.")
