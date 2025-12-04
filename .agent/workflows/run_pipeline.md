---
description: Run the full property analysis pipeline
---

# Run Full Pipeline

This workflow describes how to run the end-to-end property analysis pipeline, which includes scraping, extracting Final Judgment data, and analyzing lien survival.

## Prerequisites

1.  **Database**: Ensure `data/property_master.db` exists.
2.  **PDFs**: Ensure Final Judgment PDFs are in `data/pdfs/final_judgments/` (downloaded via `AuctionScraper`).
3.  **ORI Data**: Ensure ORI documents are scraped and stored in the `documents` table (currently manual or via `ORIScraper`).

## Steps

1.  **Run the Pipeline Script**:
    ```bash
    uv run python run_full_pipeline.py
    ```

2.  **View Extracted Judgment Data**:
    To see the data extracted from Final Judgments:
    ```bash
    uv run python view_judgment_data.py
    ```

3.  **Batch Extract Judgments (Optional)**:
    If you need to re-process PDFs or process new ones manually:
    ```bash
    uv run python scripts/batch_extract_judgments.py --force
    ```

## Output

-   **Console**: The script logs progress to the console.
-   **Database**:
    -   `auctions` table: Updated with `extracted_judgment_data` and analysis status.
    -   `liens` table: Populated with identified liens and their survival status.
