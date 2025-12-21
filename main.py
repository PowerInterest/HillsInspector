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
import logging
import os
import shutil
import sys
from datetime import UTC, date, datetime
from pathlib import Path

from loguru import logger

from src.db.new import create_database
from src.ingest.bulk_parcel_ingest import download_and_ingest
from src.pipeline import run_full_pipeline


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
    format="{time:HH:mm:ss} | {level: <8} | {module}:{function}:{line} | {extra} - {message}{exception}",
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
    
    # Auto-download and ingest bulk data
    try:
        logger.info("Starting initial bulk data ingestion...")
        download_and_ingest(db_path=str(DB_PATH), force=False)
        logger.success("Initial bulk data ingestion complete.")
    except Exception as e:
        logger.error(f"Failed during initial bulk data ingestion: {e}")
        logger.warning("Database created but bulk data missing. Run: uv run python -m src.ingest.bulk_parcel_ingest --download")

async def handle_test():
    """Run pipeline for next auction data (small set)."""
    logger.info("Running TEST pipeline (small batch)...")
    # Run full pipeline with limited auctions
    await run_full_pipeline(max_auctions=5, geocode_missing_parcels=False)

async def handle_update(
    start_date: date | None = None,
    end_date: date | None = None,
    start_step: int = 1,
    geocode_missing_parcels: bool = True,
    geocode_limit: int | None = 25,
):
    """Run full update for next 60 days using PipelineOrchestrator.

    Runs pre-processing steps (auction scrape, judgment extraction, bulk enrichment)
    before calling the orchestrator for parallel property enrichment.
    """
    from datetime import timedelta
    from src.orchestrator import PipelineOrchestrator
    from src.db.writer import DatabaseWriter
    from src.db.operations import PropertyDB
    from src.scrapers.auction_scraper import AuctionScraper
    from src.scrapers.tax_deed_scraper import TaxDeedScraper
    from src.services.final_judgment_processor import FinalJudgmentProcessor
    from src.services.scraper_storage import ScraperStorage
    from src.ingest.bulk_parcel_ingest import enrich_auctions_from_bulk
    import json

    logger.info(f"Running FULL UPDATE pipeline (Optimized)...")

    # Defaults
    if not start_date:
        start_date = datetime.now(tz=UTC).date()
    if not end_date:
        end_date = start_date + timedelta(days=60)

    logger.info(f"Date Range: {start_date} to {end_date}")

    # Initialize Components
    db = PropertyDB()
    storage = ScraperStorage()

    # =========================================================================
    # STEP 1 & 1.5: Scrape Auctions (if start_step <= 1)
    # =========================================================================
    if start_step <= 1:
        logger.info("=" * 60)
        logger.info("STEP 1: SCRAPING FORECLOSURE AUCTIONS")
        logger.info("=" * 60)

        try:
            foreclosure_scraper = AuctionScraper()
            # Check which dates need scraping
            from datetime import timedelta as td
            current = start_date
            while current <= end_date:
                if current.weekday() < 5:  # Skip weekends
                    count = db.get_auction_count_by_date(current)
                    if count == 0:
                        logger.info(f"Scraping foreclosures for {current}...")
                        props = await foreclosure_scraper.scrape_date(current, fast_fail=True)
                        for p in props:
                            db.upsert_auction(p)
                        logger.success(f"Scraped {len(props)} auctions for {current}")
                    else:
                        logger.debug(f"Skipping {current}: {count} auctions already in DB")
                current += td(days=1)
        except Exception as e:
            logger.error(f"Foreclosure scrape failed: {e}")

        logger.info("=" * 60)
        logger.info("STEP 1.5: SCRAPING TAX DEED AUCTIONS")
        logger.info("=" * 60)

        try:
            tax_deed_scraper = TaxDeedScraper()
            tax_props = await tax_deed_scraper.scrape_all(start_date, end_date)
            for p in tax_props:
                db.upsert_auction(p)
            logger.success(f"Scraped {len(tax_props)} tax deed auctions")
        except Exception as e:
            logger.error(f"Tax deed scrape failed: {e}")

    # =========================================================================
    # STEP 2: Judgment Extraction (if start_step <= 2)
    # =========================================================================
    if start_step <= 2:
        logger.info("=" * 60)
        logger.info("STEP 2: DOWNLOADING & EXTRACTING FINAL JUDGMENT DATA")
        logger.info("=" * 60)

        try:
            judgment_processor = FinalJudgmentProcessor()
            auctions = db.execute_query(
                "SELECT * FROM auctions WHERE needs_judgment_extraction = TRUE AND parcel_id IS NOT NULL"
            )
            logger.info(f"Found {len(auctions)} auctions needing judgment extraction")

            extracted_count = 0
            for auction in auctions:
                case_number = auction["case_number"]
                parcel_id = auction.get("parcel_id", "")

                # Check if PDF exists
                sanitized_folio = parcel_id.replace("/", "_").replace("\\", "_").replace(":", "_")
                base_dir = Path("data/properties") / sanitized_folio / "documents"
                pdf_paths = list(base_dir.glob("final_judgment*.pdf")) if base_dir.exists() else []

                if not pdf_paths:
                    # Try legacy path
                    legacy_path = Path(f"data/pdfs/final_judgments/{case_number}_final_judgment.pdf")
                    if legacy_path.exists():
                        pdf_paths = [legacy_path]

                if pdf_paths:
                    pdf_path = pdf_paths[0]
                    logger.info(f"Processing judgment from {pdf_path.name}...")
                    try:
                        result = judgment_processor.process_pdf(str(pdf_path), case_number)
                        if result:
                            amounts = judgment_processor.extract_key_amounts(result)
                            payload = {
                                **result,
                                **amounts,
                                "extracted_judgment_data": json.dumps(result),
                                "raw_judgment_text": result.get("raw_text", ""),
                            }
                            db.update_judgment_data(case_number, payload)
                            db.mark_step_complete(case_number, "needs_judgment_extraction")
                            extracted_count += 1
                    except Exception as e:
                        logger.warning(f"Failed to process judgment for {case_number}: {e}")

            logger.success(f"Extracted data from {extracted_count} Final Judgments")
        except Exception as e:
            logger.error(f"Judgment extraction failed: {e}")

    # =========================================================================
    # STEP 3: Bulk Data Enrichment (if start_step <= 3)
    # =========================================================================
    if start_step <= 3:
        logger.info("=" * 60)
        logger.info("STEP 3: BULK DATA ENRICHMENT")
        logger.info("=" * 60)

        try:
            enrichment_stats = enrich_auctions_from_bulk()
            logger.success(f"Bulk enrichment: {enrichment_stats}")
        except Exception as e:
            logger.error(f"Bulk enrichment failed: {e}")

        # Update legal descriptions from judgment extractions
        try:
            auctions_with_judgment = db.execute_query(
                """SELECT parcel_id, extracted_judgment_data FROM auctions
                   WHERE parcel_id IS NOT NULL AND extracted_judgment_data IS NOT NULL"""
            )
            for row in auctions_with_judgment:
                folio = row["parcel_id"]
                try:
                    judgment_data = json.loads(row["extracted_judgment_data"])
                    legal_desc = judgment_data.get("legal_description")
                    if legal_desc:
                        conn = db.connect()
                        conn.execute("""
                            UPDATE parcels SET
                                judgment_legal_description = ?,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE folio = ?
                        """, [legal_desc, folio])
                except Exception as exc:
                    logger.debug(f"Could not update judgment legal for {folio}: {exc}")
        except Exception as e:
            logger.warning(f"Could not update judgment legal descriptions: {e}")

    # =========================================================================
    # STEP 3.5: HomeHarvest Enrichment (Property Photos & MLS Data)
    # =========================================================================
    logger.info("=" * 60)
    logger.info("STEP 3.5: HOMEHARVEST ENRICHMENT (Photos & MLS Data)")
    logger.info("=" * 60)

    try:
        from src.services.homeharvest_service import HomeHarvestService

        hh_service = HomeHarvestService()
        hh_props = hh_service.get_pending_properties(limit=100, auction_date=start_date)
        logger.info(f"Found {len(hh_props)} properties needing HomeHarvest enrichment")

        if hh_props:
            for prop_data in hh_props:
                folio = prop_data["folio"]
                case_number = prop_data["case_number"]
                location = prop_data["location"]

                try:
                    success = hh_service.process_single_property(folio, location)
                    if success:
                        db.mark_step_complete(case_number, "needs_homeharvest_enrichment")
                        logger.success(f"HomeHarvest enriched: {folio}")
                    else:
                        logger.warning(f"HomeHarvest failed for {folio}")
                except SystemExit:
                    # HomeHarvest upgraded and spawned subprocess
                    logger.info("HomeHarvest upgraded - subprocess will continue enrichment")
                    break
                except Exception as e:
                    logger.warning(f"HomeHarvest error for {folio}: {e}")

                # Rate limiting delay
                import secrets
                delay = 15.0 + (secrets.randbelow(1501) / 100.0)
                await asyncio.sleep(delay)

            logger.success("HomeHarvest enrichment complete")
        else:
            logger.info("No properties need HomeHarvest enrichment")

    except Exception as e:
        logger.error(f"HomeHarvest enrichment failed: {e}")

    # =========================================================================
    # STEPS 4+: Parallel Property Enrichment via Orchestrator
    # =========================================================================
    logger.info("=" * 60)
    logger.info("STEPS 4+: PARALLEL PROPERTY ENRICHMENT (Orchestrator)")
    logger.info("=" * 60)

    writer = DatabaseWriter(Path(db.db_path))
    orchestrator = PipelineOrchestrator(db_writer=writer, db=db, storage=storage)

    await writer.start()
    try:
        await orchestrator.process_auctions(start_date, end_date)
    finally:
        await writer.stop()

    logger.success("Full update complete.")

async def handle_debug():
    """Debug run: process a single auction property end-to-end."""
    logger.info("Running DEBUG pipeline (single property)...")
    # Isolate to a clean debug database
    DEBUG_DB_PATH.unlink(missing_ok=True)
    DEBUG_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    os.environ["HILLS_DB_PATH"] = str(DEBUG_DB_PATH)
    create_database(str(DEBUG_DB_PATH))
    await run_full_pipeline(max_auctions=1, property_limit=1, geocode_missing_parcels=False)
    logger.success("Debug run complete.")

def handle_web(port: int, use_ngrok: bool = False):
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

    # Start ngrok tunnel if requested
    ngrok_tunnel = None
    if use_ngrok:
        try:
            from pyngrok import ngrok

            # Configure ngrok (uses ~/.ngrok2/ngrok.yml for auth token)
            logger.info("Starting ngrok tunnel...")

            # Create tunnel
            ngrok_tunnel = ngrok.connect(str(port), "http")
            public_url = ngrok_tunnel.public_url

            logger.success(f"ngrok tunnel established!")
            logger.info(f"Public URL: {public_url}")
            logger.info(f"Share this URL to access from anywhere (phone, remote computer)")
            print()
            print("=" * 60)
            print(f"  PUBLIC URL: {public_url}")
            print("=" * 60)
            print()

        except ImportError:
            logger.error("pyngrok not installed. Run: uv add pyngrok")
            use_ngrok = False
        except Exception as e:
            logger.error(f"Failed to start ngrok: {e}")
            logger.info("Make sure ngrok is configured: ngrok config add-authtoken YOUR_TOKEN")
            use_ngrok = False

    try:
        uvicorn.run(
            "app.web.main:app",
            host="0.0.0.0",
            port=port,
            reload=False,
            log_level="debug",
        )
    finally:
        # Clean up ngrok tunnel on shutdown
        public_url = ngrok_tunnel.public_url if ngrok_tunnel else None
        if public_url:
            logger.info("Closing ngrok tunnel...")
            from pyngrok import ngrok
            ngrok.disconnect(public_url)

def main():
    parser = argparse.ArgumentParser(description="HillsInspector Main Controller")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--test", action="store_true", help="Run pipeline for next auction data (small set)")
    group.add_argument("--debug", action="store_true", help="Run pipeline for a single auction property")
    group.add_argument("--new", action="store_true", help="Create new database (renaming old one)")
    group.add_argument("--update", action="store_true", help="Run full update for next 60 days")
    group.add_argument("--web", action="store_true", help="Start web server")
    parser.add_argument("--port", type=int, default=int(os.getenv("WEB_PORT", "8080")),
                        help="Port for web server (default 8080 or WEB_PORT env var)")
    parser.add_argument("--ngrok", action="store_true",
                        help="Start ngrok tunnel for remote access (requires ngrok auth token)")
    parser.add_argument("--start-date", type=str, default=None,
                        help="Start date for --update (YYYY-MM-DD). Defaults to tomorrow.")
    parser.add_argument("--end-date", type=str, default=None,
                        help="End date for --update (YYYY-MM-DD). If not specified, defaults to 30 days after start.")
    parser.add_argument("--start-step", type=int, default=1,
                        help="Step number to start from (1-15). Use to resume after failures.")
    parser.add_argument(
        "--no-geocode",
        action="store_true",
        help="Disable final geocoding of parcels missing latitude/longitude.",
    )
    parser.add_argument(
        "--geocode-limit",
        type=int,
        default=25,
        help="Max parcels to geocode at pipeline end (default 25, use 0 for no geocoding).",
    )

    args = parser.parse_args()

    # Parse start date if provided
    start_date = None
    if args.start_date:
        try:
            start_date = date.fromisoformat(args.start_date)
        except ValueError:
            logger.error(f"Invalid date format: {args.start_date}. Use YYYY-MM-DD.")
            sys.exit(1)

    # Parse end date if provided
    end_date = None
    if args.end_date:
        try:
            end_date = date.fromisoformat(args.end_date)
        except ValueError:
            logger.error(f"Invalid date format: {args.end_date}. Use YYYY-MM-DD.")
            sys.exit(1)

    if args.new:
        handle_new()
    elif args.test:
        asyncio.run(handle_test())
    elif args.debug:
        asyncio.run(handle_debug())
    elif args.update:
        geocode_missing = not args.no_geocode and args.geocode_limit != 0
        geocode_limit = None if args.geocode_limit == 0 else args.geocode_limit
        asyncio.run(
            handle_update(
                start_date=start_date,
                end_date=end_date,
                start_step=args.start_step,
                geocode_missing_parcels=geocode_missing,
                geocode_limit=geocode_limit,
            )
        )
    elif args.web:
        handle_web(args.port, use_ngrok=args.ngrok)

if __name__ == "__main__":
    main()
