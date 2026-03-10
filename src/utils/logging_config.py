"""Shared Loguru configuration for the repository.

Most modules import ``logger`` directly from Loguru, so this module is the single
place that defines how logs are routed and formatted. The controller can
reconfigure the logger at runtime to add a dedicated per-run log file without
breaking the default shared sink in ``logs/hills_inspector.log``.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Sequence

from loguru import logger


def _resolve_log_path(log_dir: Path, log_file: str | Path) -> Path:
    path = Path(log_file)
    if not path.is_absolute():
        path = log_dir / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def configure_logger(
    log_file: str | Path = "hills_inspector.log",
    level: str = "INFO",
    *,
    extra_log_files: Sequence[str | Path] | None = None,
) -> None:
    """Configure Loguru for console output plus one or more file sinks."""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    logger.remove()
    logger.configure(extra={"run_id": "-"})

    console_format = (
        "<green>{time:MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    )
    file_format = (
        "{time:MM-DD HH:mm:ss} | {level: <8} | "
        "{name}:{function}:{line} - {message}"
    )

    logger.add(
        sys.stderr,
        format=console_format,
        level=level,
    )

    sink_paths: list[Path] = [_resolve_log_path(log_dir, log_file)]
    for extra_file in extra_log_files or ():
        path = _resolve_log_path(log_dir, extra_file)
        if path not in sink_paths:
            sink_paths.append(path)

    for path in sink_paths:
        logger.add(
            path,
            rotation="10 MB",
            retention="10 days",
            level=level,
            format=file_format,
            backtrace=True,
            diagnose=True,
        )

# Create a default configuration instance
# This ensures that simply importing this module (via src/__init__.py) sets up logging
_configured = False


def setup_default_logging():
    global _configured
    if not _configured:
        configure_logger()
        _configured = True
