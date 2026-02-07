"""
Main entry point for HillsInspector.
Supports modes:
  --new: Create new databases (v1 and v2), archiving old ones
  --update: Run full pipeline for next 40 days
  --status: Show pipeline status summary
  --verify: Verify status against stored data
  --web: Start web server
"""
import argparse
import asyncio
import logging
import os
import shutil
import signal
import sqlite3
import sys
from datetime import date
from pathlib import Path

from loguru import logger

from src.db.migrations.create_sqlite_database import create_sqlite_database
from src.ingest.bulk_parcel_ingest import download_and_ingest
from src.utils.db_lock import DatabaseLockError, exclusive_db_lock
from src.utils.db_snapshot import DatabaseSnapshotError, refresh_web_snapshot
from src.utils.time import now_utc
from src.utils.logging_utils import env_log_level, add_optional_sinks


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
_level = env_log_level("INFO")
logger.add(
    sys.stderr,
    level=_level,
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
    backtrace=True,
    diagnose=True,
    enqueue=False,
)
logger.add(
    "logs/hills_inspector_{time:YYYY-MM-DD}.log",
    rotation="00:00",  # Rotate at midnight daily
    retention="30 days",
    level=_level,
    format="{time:HH:mm:ss} | {level: <8} | {module}:{function}:{line} | {extra} - {message}{exception}",
    backtrace=True,
    diagnose=True,
    enqueue=False,
)
add_optional_sinks()

# Intercept standard logging (Playwright, httpx, etc.) and route to loguru
logging.basicConfig(handlers=[InterceptHandler()], level=logging.INFO, force=True)
for logger_name in ["playwright", "httpx", "httpcore", "asyncio", "urllib3"]:
    logging.getLogger(logger_name).handlers = [InterceptHandler()]
    logging.getLogger(logger_name).propagate = False

DB_PATH = Path("data/property_master_sqlite.db")

# Global shutdown flag for signal handlers
_shutdown_requested = False


def _checkpoint_and_cleanup(reason: str = "shutdown") -> None:
    """Checkpoint the SQLite database to flush WAL and preserve data."""
    try:
        if DB_PATH.exists():
            conn = sqlite3.connect(str(DB_PATH), timeout=30.0)
            try:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                logger.info(f"Database checkpointed on {reason} - data preserved")
            finally:
                conn.close()
    except Exception as e:
        logger.warning(f"Checkpoint on {reason} failed (data may still be in WAL): {e}")


def _is_wal_replay_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    if "wal" not in msg and "write-ahead" not in msg and "write ahead" not in msg:
        return False
    replay_markers = (
        "replay",
        "checksum",
        "corrupt",
        "corrupted",
        "invalid",
        "failed to read",
        "could not read",
        "truncated",
    )
    return any(marker in msg for marker in replay_markers)


def _cleanup_wal_files(db_path: Path) -> None:
    """Delete WAL only when SQLite reports a WAL replay error on open."""
    wal_path = Path(str(db_path) + "-wal")
    if not wal_path.exists():
        return
    try:
        conn = sqlite3.connect(str(db_path), timeout=30.0)
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            conn.close()
    except Exception as exc:
        if _is_wal_replay_error(exc):
            try:
                wal_path.unlink()
                logger.warning(f"Deleted WAL file {wal_path} after WAL replay error: {exc}")
            except Exception as del_exc:
                logger.error(f"Failed to delete WAL file {wal_path}: {del_exc}")
        else:
            logger.info(f"WAL cleanup skipped for {wal_path}: {exc}")

def handle_new():
    """Create new SQLite databases, archiving the old ones."""
    timestamp = now_utc().strftime("%Y%m%d_%H%M%S")

    # Archive existing SQLite database if it exists
    if DB_PATH.exists():
        archive_path = DB_PATH.parent / f"property_master_sqlite_{timestamp}.db"
        logger.info(f"Archiving existing SQLite database to {archive_path}")
        shutil.move(str(DB_PATH), str(archive_path))
        # Also archive WAL files
        for ext in ["-wal", "-shm"]:
            wal_file = Path(str(DB_PATH) + ext)
            if wal_file.exists():
                shutil.move(str(wal_file), str(archive_path) + ext)

    logger.info("Creating new SQLite database with WAL mode...")
    create_sqlite_database()
    logger.success("New SQLite database created successfully.")

    # Auto-download and ingest bulk data
    try:
        logger.info("Starting initial bulk data ingestion...")
        download_and_ingest(db_path=str(DB_PATH), force=False)
        logger.success("Initial bulk data ingestion complete.")
    except Exception as e:
        logger.error(f"Failed during initial bulk data ingestion: {e}")
        logger.warning("Database created but bulk data missing. Run: uv run python -m src.ingest.bulk_parcel_ingest --download")

async def handle_update(
    start_date: date | None = None,
    end_date: date | None = None,
    start_step: int = 1,
    geocode_missing_parcels: bool = True,
    geocode_limit: int | None = 25,
    skip_tax_deeds: bool = False,
    auction_limit: int | None = None,
    retry_failed: bool = False,
    max_retries: int = 3,
):
    """Run full update via orchestrator."""
    global _shutdown_requested
    from src.orchestrator import run_full_update

    lock_path = DB_PATH.with_suffix(DB_PATH.suffix + ".lock")
    snapshot_interval = int(os.getenv("WEB_SNAPSHOT_INTERVAL", "300"))
    stop_event = asyncio.Event()
    snapshot_task = None
    update_task = None

    def _signal_handler(signum, frame):
        """Handle Ctrl+C gracefully."""
        global _shutdown_requested
        sig_name = signal.Signals(signum).name
        if _shutdown_requested:
            logger.warning(f"Second {sig_name} received - forcing exit...")
            _checkpoint_and_cleanup("forced shutdown")
            sys.exit(1)
        _shutdown_requested = True
        logger.warning(f"{sig_name} received - shutting down gracefully (Ctrl+C again to force)...")
        stop_event.set()
        if update_task and not update_task.done():
            update_task.cancel()

    async def _snapshot_loop() -> None:
        if snapshot_interval <= 0:
            return
        while not _shutdown_requested:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=snapshot_interval)
                break
            except TimeoutError:
                try:
                    # skip_lock=True because we're already inside exclusive_db_lock
                    refresh_web_snapshot(DB_PATH, skip_lock=True)
                except DatabaseSnapshotError as exc:
                    logger.warning(f"Web snapshot refresh failed: {exc}")

    # Install signal handlers
    original_sigint = signal.signal(signal.SIGINT, _signal_handler)
    original_sigterm = signal.signal(signal.SIGTERM, _signal_handler)

    try:
        with exclusive_db_lock(lock_path, wait_seconds=10):
            snapshot_task = asyncio.create_task(_snapshot_loop())
            update_task = asyncio.create_task(run_full_update(
                start_date=start_date,
                end_date=end_date,
                start_step=start_step,
                geocode_missing_parcels=geocode_missing_parcels,
                geocode_limit=geocode_limit,
                skip_tax_deeds=skip_tax_deeds,
                auction_limit=auction_limit,
                retry_failed=retry_failed,
                max_retries=max_retries,
            ))
            try:
                await update_task
            except asyncio.CancelledError:
                logger.warning("Update cancelled by user - checkpointing database...")
                _checkpoint_and_cleanup("user interrupt")
                raise
            try:
                refresh_web_snapshot(DB_PATH, skip_lock=True)
            except DatabaseSnapshotError as exc:
                logger.warning(f"Web snapshot refresh failed after update: {exc}")
    except DatabaseLockError as exc:
        logger.error(str(exc))
        logger.error("Another process is holding the DB lock; stop it and retry.")
    except asyncio.CancelledError:
        logger.info("Shutdown complete - data has been preserved")
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt caught - checkpointing...")
        _checkpoint_and_cleanup("keyboard interrupt")
    finally:
        stop_event.set()
        if snapshot_task:
            snapshot_task.cancel()
            try:
                await snapshot_task
            except asyncio.CancelledError:
                pass
        # Restore original signal handlers
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)
        # Final checkpoint to ensure all data is saved
        if _shutdown_requested:
            _checkpoint_and_cleanup("final cleanup")


def handle_status(start_date: date | None = None, end_date: date | None = None) -> None:
    """Show pipeline status summary."""
    from src.orchestrator import show_status_summary

    show_status_summary(start_date=start_date, end_date=end_date)


def handle_verify(start_date: date | None = None, end_date: date | None = None) -> None:
    """Verify status against stored data and files."""
    from src.orchestrator import verify_status

    verify_status(start_date=start_date, end_date=end_date)


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

    try:
        refresh_web_snapshot(DB_PATH)
    except DatabaseSnapshotError as exc:
        logger.warning(f"Web snapshot refresh skipped: {exc}")
    try:
        refresh_web_snapshot(Path("data/history.db"), snapshot_name="history_web.db")
    except DatabaseSnapshotError as exc:
        logger.warning(f"History snapshot refresh skipped: {exc}")

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
    _cleanup_wal_files(DB_PATH)
    parser = argparse.ArgumentParser(description="HillsInspector Main Controller")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--new", action="store_true", help="Create new databases (v1 and v2), archiving old ones")
    group.add_argument("--update", action="store_true", help="Run full update for next 40 days")
    group.add_argument("--status", action="store_true", help="Show pipeline status summary")
    group.add_argument("--verify", action="store_true", help="Verify pipeline status against data")
    group.add_argument("--web", action="store_true", help="Start web server")
    parser.add_argument("--port", type=int, default=int(os.getenv("WEB_PORT", "8080")),
                        help="Port for web server (default 8080 or WEB_PORT env var)")
    parser.add_argument("--ngrok", action="store_true",
                        help="Start ngrok tunnel for remote access (requires ngrok auth token)")
    parser.add_argument("--start-date", type=str, default=None,
                        help="Start date for --update/--status/--verify (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--end-date", type=str, default=None,
                        help="End date for --update/--status/--verify (YYYY-MM-DD). Defaults to 40 days after start.")
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
    parser.add_argument(
        "--skip-tax-deeds",
        action="store_true",
        help="Skip tax deed auction scraping (default behavior).",
    )
    parser.add_argument(
        "--include-tax-deeds",
        action="store_true",
        help="Include tax deed auction scraping.",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry failed cases during --update.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Max retries for failed cases (default 3).",
    )
    parser.add_argument(
        "--auction-limit",
        type=int,
        default=None,
        help="Max auctions to scrape per date (for testing).",
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

    skip_tax_deeds = True
    env_skip_tax_deeds = os.getenv("SKIP_TAX_DEEDS")
    if env_skip_tax_deeds is not None:
        skip_tax_deeds = env_skip_tax_deeds.strip().lower() in {
            "1",
            "true",
            "yes",
            "y",
            "on",
        }
    if args.include_tax_deeds:
        skip_tax_deeds = False
    if args.skip_tax_deeds:
        skip_tax_deeds = True

    if args.retry_failed and not args.update:
        logger.error("--retry-failed can only be used with --update.")
        sys.exit(1)

    if args.new:
        handle_new()
    elif args.status:
        handle_status(start_date=start_date, end_date=end_date)
    elif args.verify:
        handle_verify(start_date=start_date, end_date=end_date)
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
                skip_tax_deeds=skip_tax_deeds,
                retry_failed=args.retry_failed,
                max_retries=args.max_retries,
                auction_limit=args.auction_limit,
            )
        )
    elif args.web:
        handle_web(args.port, use_ngrok=args.ngrok)

if __name__ == "__main__":
    main()
