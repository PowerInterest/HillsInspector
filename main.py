"""
Main entry point for HillsInspector.
Supports modes:
  --test: Run pipeline for next auction data (small set)
  --new: Create new database (renaming old one)
  --update: Run full pipeline for next 60 days
  --web: Start web server
"""
import argparse
import asyncio
import sys
import os
import shutil
from datetime import datetime
from pathlib import Path
from loguru import logger

# Import modules
from src.db.new import create_database
from src.pipeline import run_full_pipeline

# Configure logging
logger.remove()
logger.add(sys.stderr, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}")

DB_PATH = Path("data/property_master.db")
DEBUG_DB_PATH = Path("data/debug.db")

def handle_new():
    """Create a new database, archiving the old one."""
    if DB_PATH.exists():
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_path = DB_PATH.parent / f"property_master_{timestamp}.db"
        logger.info(f"Archiving existing database to {archive_path}")
        shutil.move(str(DB_PATH), str(archive_path))
    
    logger.info("Creating new database...")
    create_database(str(DB_PATH))
    logger.success("New database created successfully.")

async def handle_test():
    """Run pipeline for next auction data (small set)."""
    logger.info("Running TEST pipeline (small batch)...")
    # Run full pipeline with limited auctions
    await run_full_pipeline(max_auctions=5)

async def handle_update():
    """Run full update for next 60 days."""
    logger.info("Running FULL UPDATE pipeline...")

    # 1. Run main pipeline (Scrape -> Extract -> Ingest -> Analyze -> Enrich)
    await run_full_pipeline(max_auctions=1000)

    logger.success("Full update complete.")

async def handle_debug():
    """Debug run: process a single auction property end-to-end."""
    logger.info("Running DEBUG pipeline (single property)...")
    # Isolate to a clean debug database
    DEBUG_DB_PATH.unlink(missing_ok=True)
    DEBUG_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    os.environ["HILLS_DB_PATH"] = str(DEBUG_DB_PATH)
    create_database(str(DEBUG_DB_PATH))
    await run_full_pipeline(max_auctions=1, property_limit=1)
    logger.success("Debug run complete.")

def handle_web():
    """Start the web server."""
    logger.info("Starting Web Server...")
    from nicegui import ui
    import app.ui as ui_module
    
    # Initialize UI
    ui_module.init_ui()
    
    # Run UI
    ui.run(title='HillsInspector', storage_secret='secret', port=8089, reload=False)

def main():
    parser = argparse.ArgumentParser(description="HillsInspector Main Controller")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--test", action="store_true", help="Run pipeline for next auction data (small set)")
    group.add_argument("--debug", action="store_true", help="Run pipeline for a single auction property")
    group.add_argument("--new", action="store_true", help="Create new database (renaming old one)")
    group.add_argument("--update", action="store_true", help="Run full update for next 60 days")
    group.add_argument("--web", action="store_true", help="Start web server")
    
    args = parser.parse_args()
    
    if args.new:
        handle_new()
    elif args.test:
        asyncio.run(handle_test())
    elif args.debug:
        asyncio.run(handle_debug())
    elif args.update:
        asyncio.run(handle_update())
    elif args.web:
        handle_web()

if __name__ == "__main__":
    main()
