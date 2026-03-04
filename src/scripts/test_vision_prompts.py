import argparse
import json
import random
from pathlib import Path

from loguru import logger
from dotenv import load_dotenv

from src.services.vision_service import VisionService

load_dotenv()

DOC_TYPE_DIRS = {
    "D": "D",  # Deeds
    "MTG": "MTG",  # Mortgages
    "JUD": "JUD",  # Judgments
    "LN": "LN",  # Liens
    "LP": "LP",  # Lis Pendens
}


def main():
    parser = argparse.ArgumentParser(description="Test OCR vision prompts against sample documents")
    parser.add_argument("--type", type=str, choices=list(DOC_TYPE_DIRS.keys()), help="Specific document type to test")
    parser.add_argument("--random-seed", type=int, default=42, help="Random seed for repeatable tests")
    parser.add_argument("--samples", type=int, default=1, help="Number of samples to test per type")
    parser.add_argument("--out", type=str, default="tmp_research/vision_results.json", help="Output JSON file for results")
    args = parser.parse_args()

    random.seed(args.random_seed)

    docs_dir = Path("docs/example_docs")
    types_to_test = [args.type] if args.type else list(DOC_TYPE_DIRS.keys())

    logger.info("Initializing VisionService...")
    vision_service = VisionService()

    # Check endpoints
    healthy = vision_service.health_check_endpoints()
    if not healthy:
        logger.error("No healthy vision endpoints. Exiting.")
        return

    results = {}

    for doc_type in types_to_test:
        dir_name = DOC_TYPE_DIRS[doc_type]
        type_dir = docs_dir / dir_name

        if not type_dir.exists():
            logger.warning(f"Directory {type_dir} not found. Skipping.")
            continue

        # Get all PDFs in directory
        pdfs = list(type_dir.glob("*.pdf"))
        if not pdfs:
            logger.warning(f"No PDFs found in {type_dir}. Skipping.")
            continue

        sample_pdfs = random.sample(pdfs, min(args.samples, len(pdfs)))

        logger.info(f"Testing {len(sample_pdfs)} sample(s) for type {doc_type}...")
        results[doc_type] = []

        for pdf_path in sample_pdfs:
            logger.info(f"Processing {pdf_path.name}...")
            # For this test, we just use extract_document_by_type which grabs the first page.
            # Multipage extraction requires splitting the PDF first, which we can add if needed.
            try:
                extracted_data = vision_service.extract_document_by_type(str(pdf_path), doc_type)
                results[doc_type].append({"file": pdf_path.name, "extracted_data": extracted_data})
                logger.success(f"Successfully processed {pdf_path.name}")
            except Exception as e:
                logger.error(f"Failed to process {pdf_path.name}: {e}")
                results[doc_type].append({"file": pdf_path.name, "error": str(e)})

    # Save results
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Results saved to {out_path}")


if __name__ == "__main__":
    main()
