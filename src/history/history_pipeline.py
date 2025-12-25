import asyncio
import sys
from pathlib import Path
from loguru import logger
from datetime import date, timedelta

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.history.scrape_history import run_scrape
from src.history.buyer_enricher import BuyerNameEnricher
from src.history.resale_scanner import ResaleScanner
from src.history.judgment_pipeline import JudgmentPipeline
from src.utils.time import today_local
from src.utils.db_snapshot import DatabaseSnapshotError, refresh_web_snapshot
from src.utils.logging_config import configure_logger

# Configure a separate log file for history
configure_logger(log_file="history_pipeline.log")

async def run_history_pipeline():
    logger.info("Starting Historical Auction Analysis Pipeline...")
    
    # Phase 2: Skeleton Scrape
    logger.info("Phase 2: Running Skeleton Scrape...")
    await run_scrape()

    # Phase 3: Buyer Name Enrichment (HCPA Sales History)
    logger.info("Phase 3: Enriching buyer names from HCPA sales history...")
    buyer_enricher = BuyerNameEnricher(headless=True)
    await buyer_enricher.enrich_batch(25)
    
    # Phase 4: Final Judgment processing (SKIPPED for speed)
    # logger.info("Phase 4: Running Judgment Pipeline...")
    # judgment_pipe = JudgmentPipeline()
    # await judgment_pipe.process_batch(20)

    # Phase 5: Flip Analysis (Resale Scanning)
    logger.info("Phase 5: Running Resale Scanner (Flip Analysis)...")
    scanner = ResaleScanner()
    await scanner.scan_batch(50)
    
    logger.info("Historical Pipeline completed successfully.")
    try:
        refresh_web_snapshot(Path("data/history.db"), snapshot_name="history_web.db")
    except DatabaseSnapshotError as exc:
        logger.warning(f"History snapshot refresh failed: {exc}")


if __name__ == "__main__":
    asyncio.run(run_history_pipeline())
