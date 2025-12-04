# HOVER Scraper

## Overview
The `HoverScraper` searches the Hillsborough Online Viewing of Electronic Records (HOVER) system for court case documents, specifically Final Judgments.

## Source
- **URL**: `https://hover.hillsclerk.com`
- **Type**: Web Scraping (Playwright)

## Inputs
- **Case Number**: Full case number (e.g., `292023CA013924A001HC`) or short format (`23-CA-013924`).

## Outputs
- **Document List**: List of found documents with URLs.
- **Files Stored via ScraperStorage**:
    - **Final Judgment PDFs**: Downloaded and saved to `data/properties/{case_number}/documents/final_judgment_{case_number}.pdf`

## Key Methods
- `get_case_documents(case_number)`: Searches for the case, identifies "Final Judgment" entries, and downloads the PDF.
