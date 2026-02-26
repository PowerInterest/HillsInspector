"""Background dispatch for non-blocking controller steps."""

from __future__ import annotations

import contextlib
import datetime as dt
import os
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from loguru import logger


def dispatch_controller_step(
    step_name: str,
    dsn: str,
    *,
    force_all: bool = False,
    logs_dir: Path | None = None,
) -> dict[str, Any]:
    target_logs_dir = logs_dir or Path("logs") / "step_workers"
    target_logs_dir.mkdir(parents=True, exist_ok=True)

    pid_path = target_logs_dir / f"{step_name}.pid"
    lock_path = target_logs_dir / f"{step_name}.lock"
    with _dispatch_lock(lock_path):
        existing_pid = read_pid_file(pid_path)
        if existing_pid and pid_is_alive(existing_pid):
            if _pid_matches_worker(existing_pid, "src.services.bulk_step_worker"):
                logger.info(f"Step worker already running for {step_name} (pid={existing_pid})")
                return {
                    "skipped": True,
                    "reason": "step_worker_already_running",
                    "step_name": step_name,
                    "pid": existing_pid,
                }
            logger.warning(
                "PID {} for step {} is alive but does not match worker command; replacing stale pid file",
                existing_pid,
                step_name,
            )
            with contextlib.suppress(OSError):
                pid_path.unlink()
        elif existing_pid:
            logger.warning(
                f"Found stale step-worker pid for {step_name} ({existing_pid}); replacing"
            )
            with contextlib.suppress(OSError):
                pid_path.unlink()

        timestamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d_%H%M%S")
        log_path = target_logs_dir / f"{step_name}_{timestamp}.log"
        command = [sys.executable, "-m", "src.services.bulk_step_worker"]

        env = os.environ.copy()
        env["SUNBIZ_PG_DSN"] = dsn
        env["HI_BULK_STEP_NAME"] = step_name
        env["HI_FORCE_ALL"] = "1" if force_all else "0"

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
            logger.error(f"Failed to start step worker for {step_name}: {exc}")
            return {
                "error": str(exc),
                "reason": "step_worker_launch_failed",
                "step_name": step_name,
            }

        # Detect immediate startup failures (import errors, env mistakes) so the
        # controller can mark the step failed instead of reporting a false success.
        exit_code = process.poll()
        if exit_code is not None and exit_code != 0:
            with contextlib.suppress(OSError):
                pid_path.unlink()
            logger.error(
                "Step worker for {} exited immediately (code={})",
                step_name,
                exit_code,
            )
            return {
                "error": f"step_worker_exited_immediately:{exit_code}",
                "reason": "step_worker_startup_failed",
                "step_name": step_name,
                "log_path": str(log_path),
            }

        logger.info(f"Step worker dispatched for {step_name} (pid={process.pid})")
        return {
            "skipped": True,
            "reason": "step_worker_dispatched_background",
            "dispatched": True,
            "step_name": step_name,
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


def _pid_matches_worker(pid: int, module_name: str) -> bool:
    cmdline = _read_cmdline(pid)
    if not cmdline:
        return True
    return module_name in cmdline


def _read_cmdline(pid: int) -> list[str] | None:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return None
    if not raw:
        return None
    return [token for token in raw.decode(errors="ignore").split("\x00") if token]


@contextmanager
def _dispatch_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        lock_mod = None
        try:
            import fcntl as _fcntl  # type: ignore

            lock_mod = _fcntl
            lock_mod.flock(lock_file.fileno(), lock_mod.LOCK_EX)
        except Exception:
            lock_mod = None
        try:
            yield
        finally:
            if lock_mod is not None:
                with contextlib.suppress(Exception):
                    lock_mod.flock(lock_file.fileno(), lock_mod.LOCK_UN)
