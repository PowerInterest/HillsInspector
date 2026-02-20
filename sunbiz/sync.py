#!/usr/bin/env python3
# ruff: noqa: E402
"""Compatibility wrapper for Sunbiz SFTP sync service.

Primary implementation now lives at `src/services/sunbiz_sync_service.py`.
"""

from pathlib import Path
import sys

# Ensure repository root is importable when this file is executed directly.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.sunbiz_sync_service import DEFAULT_DATA_DIR
from src.services.sunbiz_sync_service import DEFAULT_DAILY_DIR
from src.services.sunbiz_sync_service import DEFAULT_HOST
from src.services.sunbiz_sync_service import DEFAULT_MANIFEST
from src.services.sunbiz_sync_service import DEFAULT_PASSWORD
from src.services.sunbiz_sync_service import DEFAULT_PORT
from src.services.sunbiz_sync_service import DEFAULT_QUARTERLY_DIR
from src.services.sunbiz_sync_service import DEFAULT_USER
from src.services.sunbiz_sync_service import SunbizMirror
from src.services.sunbiz_sync_service import build_parser
from src.services.sunbiz_sync_service import main

__all__ = [
    "DEFAULT_DAILY_DIR",
    "DEFAULT_DATA_DIR",
    "DEFAULT_HOST",
    "DEFAULT_MANIFEST",
    "DEFAULT_PASSWORD",
    "DEFAULT_PORT",
    "DEFAULT_QUARTERLY_DIR",
    "DEFAULT_USER",
    "SunbizMirror",
    "build_parser",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
