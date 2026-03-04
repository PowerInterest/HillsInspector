import argparse
import json
import random
import re
from pathlib import Path

from loguru import logger
from dotenv import load_dotenv

from src.services.vision_service import VisionService

load_dotenv()


# Extract prompts from DOC_TYPES.md
def get_prompts_from_md():
    md_path = Path("docs/domain/DOC_TYPES.md")
    content = md_path.read_text(encoding="utf-8")

    prompts = {}

    # Simple regex to find all ```text blocks under "*   **OCR Vision Prompt:**"
    # We will map them based on the text contents
    matches = re.finditer(r"\*\s+\*\*OCR Vision Prompt:\*\*\s*```text\s*(.*?)\s*```", content, re.DOTALL | re.IGNORECASE)

    for match in matches:
        prompt_text = match.group(1).strip()
        if (
            "Deeds" in content[: match.start()]
            and "instrument_number and book_page" in prompt_text
            and "party_1 (Grantor/Seller)" in prompt_text
        ):
            prompts["D"] = prompt_text
        elif "party_1 (Mortgagor/Borrower)" in prompt_text:
            prompts["MTG"] = prompt_text
        elif (
            "civil_case_number" in prompt_text
            and "foreclosed_instrument" in prompt_text
            and "Lis Pendens" not in prompt_text
            and "Plaintiff" in prompt_text
            and "List of ALL Adjudicated Defendants" in prompt_text
        ):
            prompts["JUD"] = prompt_text
        elif "municipal_case_number" in prompt_text:
            prompts["LN"] = prompt_text
        elif "List of ALL Defendants and any other parties being notified" in prompt_text:
            prompts["LP"] = prompt_text
        elif "Assignor/Old Lender" in prompt_text:
            prompts["ASG"] = prompt_text
        elif "Releasor/Creditor" in prompt_text:
            prompts["SAT"] = prompt_text
        elif "General Contractor" in prompt_text:
            prompts["NOC"] = prompt_text

    # For judgement, the prompt contains "civil_case_number".
    # LP also has civil_case_number but "List of ALL Defendants"

    return prompts


def main():
    parser = argparse.ArgumentParser(description="Test OCR vision prompts from DOC_TYPES.md")
    parser.add_argument("--type", type=str, help="Specific document type to test")
    parser.add_argument("--random-seed", type=int, default=42, help="Random seed for repeatable tests")
    parser.add_argument("--samples", type=int, default=1, help="Number of samples to test per type")
    parser.add_argument("--out", type=str, default="tmp_research/doc_types_results.json")
    args = parser.parse_args()

    random.seed(args.random_seed)

    docs_dir = Path("docs/example_docs")
    prompts = get_prompts_from_md()

    if not prompts:
        logger.error("Could not extract any prompts from DOC_TYPES.md")
        return

    logger.info("Initializing VisionService...")
    vision_service = VisionService()

    results = {}

    types_to_test = [args.type] if args.type else list(prompts.keys())

    for doc_type in types_to_test:
        if doc_type not in prompts:
            logger.warning(f"No prompt found for {doc_type}. Skipping.")
            continue
        prompt_text = prompts[doc_type]
        # Map some doc types to their directories if they differ slightly
        dir_name = doc_type
        type_dir = docs_dir / dir_name

        if not type_dir.exists():
            logger.warning(f"Directory {type_dir} not found. Skipping.")
            continue

        pdfs = list(type_dir.glob("*.pdf"))
        if not pdfs:
            logger.warning(f"No PDFs found in {type_dir}. Skipping.")
            continue

        sample_pdfs = random.sample(pdfs, min(args.samples, len(pdfs)))

        logger.info(f"Testing {len(sample_pdfs)} sample(s) for type {doc_type} with DOC_TYPES.md prompt...")
        results[doc_type] = []

        for pdf_path in sample_pdfs:
            logger.info(f"Processing {pdf_path.name}...")
            try:
                # Use extract_json directly with the prompt text
                full_prompt = (
                    f"{prompt_text}\n\nRespond ONLY with a valid JSON object. Do not include markdown formatting like ```json."
                )
                extracted_data = vision_service.extract_json(str(pdf_path), full_prompt)

                results[doc_type].append({"file": pdf_path.name, "extracted_data": extracted_data})
                logger.success(f"Successfully processed {pdf_path.name}")
            except Exception as e:
                logger.error(f"Failed to process {pdf_path.name}: {e}")
                results[doc_type].append({"file": pdf_path.name, "error": str(e)})

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Results saved to {out_path}")


if __name__ == "__main__":
    main()
