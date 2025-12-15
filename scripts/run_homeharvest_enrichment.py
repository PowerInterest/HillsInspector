from src.services.homeharvest_service import HomeHarvestService
from loguru import logger
import argparse

def main():
    parser = argparse.ArgumentParser(description="Run HomeHarvest enrichment")
    parser.add_argument("--limit", type=int, default=10, help="Number of properties to process")
    args = parser.parse_args()

    service = HomeHarvestService()
    
    logger.info("Checking for properties needing market data...")
    props = service.get_pending_properties(limit=args.limit)
    
    if not props:
        logger.info("No properties found needing enrichment.")
        return

    logger.info(f"Found {len(props)} properties. Starting enrichment...")
    service.fetch_and_save(props)
    logger.success("Batch complete.")

if __name__ == "__main__":
    main()
