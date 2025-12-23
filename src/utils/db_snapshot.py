from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
from loguru import logger

from src.utils.db_lock import DatabaseLockError, exclusive_db_lock
from src.utils.time import now_utc


class DatabaseSnapshotError(RuntimeError):
    """Raised when a web snapshot could not be created."""


def refresh_web_snapshot(
    source_db: Path,
    snapshot_name: str = "property_master_web.db",
    wait_seconds: float = 0,
    skip_lock: bool = False,
) -> Path:
    """
    Create or refresh the web snapshot DB.

    This writes a single snapshot file (no history). If the main DB is locked by
    an update, the snapshot refresh is skipped unless wait_seconds > 0.

    Args:
        source_db: Path to the source database.
        snapshot_name: Name of the snapshot file.
        wait_seconds: How long to wait for lock (0 = fail fast).
        skip_lock: If True, skip lock acquisition (use when caller already holds lock).
    """
    source_db = Path(source_db)
    if not source_db.exists():
        raise DatabaseSnapshotError(f"Source DB not found: {source_db}")

    snapshot_path = source_db.parent / snapshot_name
    lock_path = source_db.with_suffix(source_db.suffix + ".lock")

    def _do_snapshot() -> Path:
        # Ensure WAL is flushed before snapshot copy.
        conn = duckdb.connect(str(source_db))
        try:
            conn.execute("CHECKPOINT")
        finally:
            conn.close()

        tmp_path = snapshot_path.with_suffix(snapshot_path.suffix + ".tmp")
        shutil.copy2(source_db, tmp_path)
        tmp_path.replace(snapshot_path)
        logger.info(f"Web snapshot refreshed at {snapshot_path} ({now_utc().isoformat()})")
        return snapshot_path

    try:
        if skip_lock:
            return _do_snapshot()
        else:
            with exclusive_db_lock(lock_path, wait_seconds=wait_seconds):
                return _do_snapshot()
    except DatabaseLockError as exc:
        raise DatabaseSnapshotError(str(exc)) from exc
    except Exception as exc:
        raise DatabaseSnapshotError(f"Failed to refresh snapshot: {exc}") from exc
