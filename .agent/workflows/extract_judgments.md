---
description: Batch extract data from Final Judgment PDFs
---

# Batch Extract Judgments

This workflow explains how to extract structured data from downloaded Final Judgment PDFs using the Vision API.

## Steps

1.  **Ensure PDFs exist**:
    Check `data/pdfs/final_judgments/` for PDF files.

2.  **Run Extraction**:
    ```bash
    uv run python scripts/batch_extract_judgments.py
    ```

3.  **Force Re-extraction**:
    To force re-processing of all PDFs (ignoring already extracted ones):
    ```bash
    uv run python scripts/batch_extract_judgments.py --force
    ```

## Output

-   Updates the `auctions` table in `data/property_master.db` with:
    -   `extracted_judgment_data` (JSON)
    -   `judgment_date`
    -   `total_judgment_amount`
    -   `foreclosure_type`
    -   `plaintiff`
    -   And other extracted fields.
