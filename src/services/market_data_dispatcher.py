"""Background dispatch for the market-data worker process."""

from __future__ import annotations

import contextlib
import datetime as dt
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from loguru import logger


def dispatch_market_data_worker(dsn: str, *, logs_dir: Path | None = None) -> dict[str, Any]:
    target_logs_dir = logs_dir or Path("logs")
    target_logs_dir.mkdir(parents=True, exist_ok=True)
    pid_path = target_logs_dir / "market_data_worker.pid"

    existing_pid = read_pid_file(pid_path)
    if existing_pid and pid_is_alive(existing_pid):
        logger.info(f"Market data worker already running (pid={existing_pid})")
        return {
            "skipped": True,
            "reason": "market_data_worker_already_running",
            "pid": existing_pid,
        }

    if existing_pid:
        logger.warning(f"Found stale market-data pid file ({existing_pid}); replacing")
        with contextlib.suppress(OSError):
            pid_path.unlink()

    timestamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d_%H%M%S")
    log_path = target_logs_dir / f"market_data_worker_{timestamp}.log"
    command = [sys.executable, "-m", "src.services.market_data_worker"]
    env = os.environ.copy()
    env["SUNBIZ_PG_DSN"] = dsn

    try:
        with log_path.open("ab") as log_file:
            process = subprocess.Popen(
                command,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                env=env,
            )
        pid_path.write_text(f"{process.pid}\n", encoding="utf-8")
    except Exception as exc:
        logger.error(f"Failed to start market data worker: {exc}")
        return {
            "skipped": True,
            "reason": "market_data_worker_launch_failed",
            "error": str(exc),
        }

    exit_code = process.poll()
    if exit_code is not None and exit_code != 0:
        with contextlib.suppress(OSError):
            pid_path.unlink()
        logger.error("Market data worker exited immediately (code={})", exit_code)
        return {
            "error": f"market_data_worker_exited_immediately:{exit_code}",
            "reason": "market_data_worker_startup_failed",
            "log_path": str(log_path),
        }

    logger.info(f"Market data worker dispatched (pid={process.pid})")
    return {
        "skipped": True,
        "reason": "market_data_worker_dispatched_background",
        "dispatched": True,
        "pid": process.pid,
        "log_path": str(log_path),
    }


def read_pid_file(path: Path) -> int | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not raw:
        return None
    try:
        pid = int(raw)
    except ValueError:
        return None
    return pid if pid > 0 else None


def pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True
