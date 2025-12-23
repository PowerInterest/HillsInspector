from __future__ import annotations

import os
import time
from contextlib import contextmanager
from pathlib import Path

try:
    import fcntl
except ImportError as exc:  # pragma: no cover - non-POSIX platforms
    fcntl = None  # type: ignore[assignment]


class DatabaseLockError(RuntimeError):
    """Raised when an exclusive database lock cannot be acquired."""


@contextmanager
def exclusive_db_lock(lock_path: Path, wait_seconds: float = 0) -> None:
    """
    Acquire an exclusive, cross-process lock for the main database.

    Args:
        lock_path: File path for the lock (e.g., data/property_master.db.lock).
        wait_seconds: How long to wait before giving up (0 = fail fast).
    """
    if fcntl is None:  # pragma: no cover - non-POSIX platforms
        raise DatabaseLockError("File locking is not supported on this platform.")

    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR)
    start = time.monotonic()

    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except BlockingIOError:
            if wait_seconds and (time.monotonic() - start) < wait_seconds:
                time.sleep(0.25)
                continue
            raise DatabaseLockError(
                f"Database lock already held. Try stopping the other process or wait "
                f"for it to finish. Lock file: {lock_path}"
            )

    try:
        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode("ascii", errors="ignore"))
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
            # Clean up lock file on exit
            try:
                lock_path.unlink(missing_ok=True)
            except OSError:
                pass  # Best effort cleanup
