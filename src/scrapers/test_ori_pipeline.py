"""
Test script for the ORI document pipeline.

This script demonstrates:
1. Searching ORI API by legal description
2. Downloading PDFs for all matching documents
3. Extracting text from PDFs using VisionService (Qwen-VL)

Usage:
    uv run python -m src.scrapers.test_ori_pipeline
"""

import json
import time
import tempfile
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote

import requests

import fitz  # PyMuPDF

from src.services.vision_service import VisionService


# Configuration
ORI_SEARCH_URL = "https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search"
ORI_PDF_URL = "https://publicaccess.hillsclerk.com/Public/ORIUtilities/OverlayWatermark/api/Watermark"

HEADERS = {
    "Content-Type": "application/json; charset=UTF-8",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Origin": "https://publicaccess.hillsclerk.com",
    "Referer": "https://publicaccess.hillsclerk.com/oripublicaccess/",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

# Document types for title search
TITLE_DOC_TYPES = [
    "(MTG) MORTGAGE",
    "(MTGREV) MORTGAGE REVERSE",
    "(MTGNT) MORTGAGE EXEMPT TAXES",
    "(MTGNIT) MORTGAGE NO INTANGIBLE TAXES",
    "(LN) LIEN",
    "(MEDLN) MEDICAID LIEN",
    "(LNCORPTX) CORP TAX LIEN FOR STATE OF FLORIDA",
    "(LP) LIS PENDENS",
    "(RELLP) RELEASE LIS PENDENS",
    "(JUD) JUDGMENT",
    "(CCJ) CERTIFIED COPY OF A COURT JUDGMENT",
    "(D) DEED",
    "(ASG) ASSIGNMENT",
    "(TAXDEED) TAX DEED",
    "(SATCORPTX) SATISFACTION CORP TAX FOR STATE OF FL",
    "(SAT) SATISFACTION",
    "(REL) RELEASE",
    "(PR) PARTIAL RELEASE",
    "(NOC) NOTICE OF COMMENCEMENT",
    "(MOD) MODIFICATION",
    "(ASGT) ASSIGNMENT/TAXES",
]


def search_ori_by_legal(legal_description: str, session: requests.Session) -> list:
    """Search ORI API by legal description."""
    payload = {
        "DocType": TITLE_DOC_TYPES,
        "RecordDateBegin": "01/01/1900",
        "RecordDateEnd": datetime.now().strftime("%m/%d/%Y"),
        "Legal": ["CONTAINS", legal_description],
    }

    response = session.post(ORI_SEARCH_URL, headers=HEADERS, json=payload, timeout=60)
    response.raise_for_status()

    data = response.json()
    return data.get("ResultList", [])


def download_pdf(doc: dict, output_dir: Path, session: requests.Session) -> Path | None:
    """Download PDF for a document."""
    doc_id = doc.get("ID")
    if not doc_id:
        return None

    instrument = doc.get("Instrument", "unknown")
    doc_type = doc.get("DocType", "UNKNOWN").replace("(", "").replace(")", "").replace(" ", "_")
    try:
        record_date = datetime.fromtimestamp(doc.get("RecordDate", 0), tz=UTC).strftime("%Y%m%d")
    except (OSError, ValueError):
        record_date = "unknown"

    pdf_url = f"{ORI_PDF_URL}/{quote(doc_id)}"
    filename = f"{record_date}_{doc_type}_{instrument}.pdf"
    filepath = output_dir / filename

    # Skip if already downloaded
    if filepath.exists():
        return filepath

    headers = {
        "Accept": "application/pdf,*/*",
        "Referer": "https://publicaccess.hillsclerk.com/oripublicaccess/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }

    response = session.get(pdf_url, headers=headers, timeout=30)

    if response.status_code == 200 and response.content[:4] == b"%PDF":
        with open(filepath, "wb") as f:
            f.write(response.content)
        return filepath

    return None


def extract_text_from_pdf(pdf_path: Path, vision_service: VisionService, output_dir: Path) -> str:
    """Extract text from PDF using VisionService (Qwen-VL)."""
    text_file = output_dir / f"{pdf_path.stem}.txt"

    # Check if already extracted
    if text_file.exists():
        return text_file.read_text(encoding="utf-8")

    full_text = []
    temp_files = []

    try:
        # Open PDF with PyMuPDF
        doc = fitz.open(str(pdf_path))
        print(f"   Converting {len(doc)} page(s) to images...")

        for i, page in enumerate(doc):
            # Render page to image (150 DPI)
            mat = fitz.Matrix(150 / 72, 150 / 72)
            pix = page.get_pixmap(matrix=mat)

            # Save image to temp directory
            tmp_path = Path(tempfile.gettempdir()) / f"ori_page_{pdf_path.stem}_{i}.png"
            pix.save(str(tmp_path))
            temp_files.append(tmp_path)

            # Use VisionService to extract text
            prompt = """Extract ALL text from this document image exactly as it appears.
This is a legal document (deed, mortgage, lien, etc.) from Hillsborough County Florida.
Include all names, dates, amounts, legal descriptions, and any other text visible.
Do not summarize - transcribe the complete text."""

            page_text = vision_service.analyze_image(str(tmp_path), prompt, max_tokens=4096)
            if page_text:
                full_text.append(f"--- Page {i + 1} ---\n{page_text}")
                print(f"   Page {i + 1}: Extracted {len(page_text)} chars")
            else:
                print(f"   Page {i + 1}: No text extracted")

        doc.close()

    except Exception as e:
        print(f"   Error extracting text: {e}")
    finally:
        # Clean up temp files
        for tmp_path in temp_files:
            with suppress(Exception):
                tmp_path.unlink(missing_ok=True)

    combined_text = "\n\n".join(full_text)

    # Save extracted text (use utf-8 encoding for unicode support)
    if combined_text:
        text_file.write_text(combined_text, encoding="utf-8")

    return combined_text


def main():
    """Run the ORI pipeline test."""
    # Output directory for 205 W Amelia
    output_base = Path("data/test_205_amelia")
    pdf_dir = output_base / "pdfs"
    text_dir = output_base / "extracted"
    pdf_dir.mkdir(parents=True, exist_ok=True)
    text_dir.mkdir(parents=True, exist_ok=True)

    # Initialize VisionService and check server availability
    vision_service = VisionService()
    print(f"Checking Qwen Vision API server at {vision_service.API_URL}...")
    if vision_service.check_server():
        print("✓ Qwen Vision API server is UP")
    else:
        print("✗ Qwen Vision API server is DOWN - text extraction will be skipped")
        vision_service = None

    # Initialize ORI session
    session = requests.Session()
    session.get("https://publicaccess.hillsclerk.com/oripublicaccess/")

    # Legal description for 205 W Amelia Ave, Tampa
    # From bulk data: MUNRO AND MC INTOSH'S ADDITION LOT 9 BLOCK 12
    # The apostrophe causes issues, so search by subdivision name
    legal_description = "MUNRO AND MC INTOSH"

    print(f"Searching ORI for: {legal_description}")
    print("=" * 60)

    documents = search_ori_by_legal(legal_description, session)
    print(f"Found {len(documents)} total documents")

    # Save all search results
    with open(output_base / "all_search_results.json", "w") as f:
        json.dump(documents, f, indent=2)

    # Filter to title-relevant document types (exclude NOCs)
    title_types = ["DEED", "MORTGAGE", "LIEN", "TAXDEED", "JUDGMENT", "LIS PENDENS", "ASSIGNMENT", "SATISFACTION", "RELEASE"]
    filtered_docs = [
        doc
        for doc in documents
        if any(t in doc.get("DocType", "").upper() for t in title_types)
    ]

    print(f"Filtered to {len(filtered_docs)} title-relevant documents")
    print()

    # Save filtered results
    with open(output_base / "filtered_docs.json", "w") as f:
        json.dump(filtered_docs, f, indent=2)

    # Process each document
    for i, doc in enumerate(filtered_docs):
        doc_type = doc.get("DocType", "UNKNOWN")
        instrument = doc.get("Instrument", "unknown")
        parties_one = ", ".join(doc.get("PartiesOne", []))
        parties_two = ", ".join(doc.get("PartiesTwo", []))
        try:
            record_date = datetime.fromtimestamp(doc.get("RecordDate", 0), tz=UTC).strftime("%Y-%m-%d")
        except (OSError, ValueError):
            record_date = "unknown"

        print(f"{i + 1}. {doc_type}")
        print(f"   Date: {record_date} | Instrument: {instrument}")
        print(f"   From: {parties_one}")
        print(f"   To: {parties_two}")

        # Download PDF
        pdf_path = download_pdf(doc, pdf_dir, session)
        if not pdf_path:
            print("   Failed to download PDF")
            print()
            continue
        print(f"   Downloaded: {pdf_path.name} ({pdf_path.stat().st_size:,} bytes)")

        # Extract text using VisionService (if available)
        if vision_service:
            print("   Extracting text with Qwen-VL...")
            extracted_text = extract_text_from_pdf(pdf_path, vision_service, text_dir)
            if extracted_text:
                print(f"   Extracted {len(extracted_text)} total characters")
            else:
                print("   No text extracted")
        else:
            print("   Skipping text extraction (Qwen server unavailable)")

        time.sleep(0.5)  # Be nice to server
        print()

    print("=" * 60)
    print("Pipeline complete!")
    print(f"PDFs saved to: {pdf_dir}")
    print(f"Extracted text saved to: {text_dir}")


if __name__ == "__main__":
    main()
