"""PG-first entrypoint controller — single pipeline for the entire project.

Phase A (bulk data refresh — idempotent, no per-property scraping):
  1. HCPA suite (parcels, sales, subdivisions)
  2. Clerk bulk (cases, events)
  3. DOR NAL (tax data)
  4. Sunbiz UCC (SFTP filings)
  5. Sunbiz entities
  6. County permits (API)
  7. Tampa permits (Accela scrape)
  8. Foreclosure refresh (join all bulk → hub table)
  9. Trust accounts
  10. Title chain (PG chain builder)

Phase B (per-auction enrichment — scraping + analysis):
  11. Scrape upcoming auctions (Playwright → clerk website → PG)
  12. Extract judgment PDFs (VisionService → disk JSON → PG)
  13. ORI document search (Playwright → ORI website → PG ori_encumbrances)
  14. Lien survival analysis (pure computation → PG ori_encumbrances)
  15. Final refresh (pick up Phase B data)
  16. Market data (Redfin/Zillow/HomeHarvest; background by default)

Usage:
  uv run Controller.py                           # Full pipeline
  uv run Controller.py --skip-hcpa --skip-nal    # Skip bulk steps
  uv run Controller.py --skip-auction-scrape     # Skip Phase B scraping
  uv run Controller.py --ori-limit 10            # Limit ORI to 10 properties
"""

from __future__ import annotations

import json
from dataclasses import asdict

from dotenv import load_dotenv
from loguru import logger

from src.services.pg_pipeline_controller import PgPipelineController, parse_args

load_dotenv()


def main() -> None:
    settings = parse_args()
    logger.info(f"PG controller start: {asdict(settings)}")

    controller = PgPipelineController(settings)
    result = controller.run()

    logger.info("PG controller complete")
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
