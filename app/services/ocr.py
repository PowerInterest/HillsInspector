"""
DEPRECATED: This module is no longer used.

All OCR and text extraction is now handled by VisionService (src/services/vision_service.py)
which uses the Qwen-VL API at http://10.10.1.5:6969

For PDF text extraction, use:
    from src.services.vision_service import VisionService

    vision = VisionService()
    text = vision.extract_text(image_path)

For structured data extraction:
    result = vision.extract_json(image_path, prompt)
"""

raise ImportError(
    "OCRProcessor is deprecated. Use VisionService from src/services/vision_service.py instead. "
    "See docs/PLACES.md for the new PDF extraction pipeline."
)
