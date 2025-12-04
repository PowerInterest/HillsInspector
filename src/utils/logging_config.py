import sys
from pathlib import Path
from loguru import logger

def configure_logger(log_file: str = "hills_inspector.log", level: str = "INFO"):
    """
    Configure loguru logger for the entire project.
    """
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Remove default handler to avoid duplicate logs
    logger.remove()
    
    # Add console handler
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=level
    )
    
    # Add file handler
    logger.add(
        log_dir / log_file,
        rotation="10 MB",
        retention="10 days",
        level=level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        backtrace=True,
        diagnose=True
    )

# Create a default configuration instance
# This ensures that simply importing this module (via src/__init__.py) sets up logging
_configured = False

def setup_default_logging():
    global _configured
    if not _configured:
        configure_logger()
        _configured = True
