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
    uv run python controller.py
    ```

