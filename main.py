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
import logging
from datetime import datetime, date
from pathlib import Path
from loguru import logger


class InterceptHandler(logging.Handler):
    """Intercept standard logging and redirect to loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message
        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )

# Import modules
from src.db.new import create_database
from src.pipeline import run_full_pipeline

# Configure logging - both console and file
logger.remove()
logger.add(
    sys.stderr,
    level="INFO",
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
    backtrace=True,
    diagnose=True,
    enqueue=True,
)
logger.add(
    "logs/hills_inspector_{time:YYYY-MM-DD}.log",
    rotation="00:00",  # Rotate at midnight daily
    retention="30 days",
    level="INFO",
    format="{time:HH:mm:ss} | {level: <8} | {module}:{function}:{line} | {extra} - {message}\n{exception}",
    backtrace=True,
    diagnose=True,
    enqueue=True,
)

# Intercept standard logging (Playwright, httpx, etc.) and route to loguru
logging.basicConfig(handlers=[InterceptHandler()], level=logging.INFO, force=True)
for logger_name in ["playwright", "httpx", "httpcore", "asyncio", "urllib3"]:
    logging.getLogger(logger_name).handlers = [InterceptHandler()]
    logging.getLogger(logger_name).propagate = False

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

async def handle_update(start_date: date | None = None, start_step: int = 1):
    """Run full update for next 60 days."""
    logger.info(f"Running FULL UPDATE pipeline (from step {start_step})...")

    # 1. Run main pipeline (Scrape -> Extract -> Ingest -> Analyze -> Enrich)
    await run_full_pipeline(max_auctions=10, start_date=start_date, start_step=start_step)

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

def handle_web(port: int):
    """Start the FastAPI web server (app/web)."""
    import uvicorn
    import socket

    def get_ip():
        """Get local IP address."""
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # doesn't even have to be reachable
            s.connect(('10.255.255.255', 1))
            IP = s.getsockname()[0]
        except Exception:
            IP = '127.0.0.1'
        finally:
            s.close()
        return IP

    ip_addr = get_ip()
    logger.info(f"Starting FastAPI Web Server (app/web) on port {port}...")
    logger.info(f"Local Access: http://localhost:{port}")
    if ip_addr != '127.0.0.1':
        logger.info(f"Network Access (WSL): http://{ip_addr}:{port}")

    uvicorn.run(
        "app.web.main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="debug",
    )

def main():
    parser = argparse.ArgumentParser(description="HillsInspector Main Controller")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--test", action="store_true", help="Run pipeline for next auction data (small set)")
    group.add_argument("--debug", action="store_true", help="Run pipeline for a single auction property")
    group.add_argument("--new", action="store_true", help="Create new database (renaming old one)")
    group.add_argument("--update", action="store_true", help="Run full update for next 60 days")
    group.add_argument("--web", action="store_true", help="Start web server")
    parser.add_argument("--port", type=int, default=int(os.getenv("WEB_PORT", 8080)),
                        help="Port for web server (default 8080 or WEB_PORT env var)")
    parser.add_argument("--start-date", type=str, default=None,
                        help="Start date for --update (YYYY-MM-DD). Defaults to tomorrow.")
    parser.add_argument("--start-step", type=int, default=1,
                        help="Step number to start from (1-13). Use to resume after failures.")

    args = parser.parse_args()

    # Parse start date if provided
    start_date = None
    if args.start_date:
        try:
            start_date = date.fromisoformat(args.start_date)
        except ValueError:
            logger.error(f"Invalid date format: {args.start_date}. Use YYYY-MM-DD.")
            sys.exit(1)

    if args.new:
        handle_new()
    elif args.test:
        asyncio.run(handle_test())
    elif args.debug:
        asyncio.run(handle_debug())
    elif args.update:
        asyncio.run(handle_update(start_date=start_date, start_step=args.start_step))
    elif args.web:
        handle_web(args.port)

if __name__ == "__main__":
    main()
