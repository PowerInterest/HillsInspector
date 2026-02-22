"""SQLite path resolution helpers."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency in some contexts
    def load_dotenv(*_args: object, **_kwargs: object) -> bool:
        return False


DEFAULT_SQLITE_PATH = Path("data/property_master_sqlite.db")


@lru_cache(maxsize=1)
def resolve_sqlite_db_path(env_var: str = "HILLS_SQLITE_DB") -> Path:
    """Return the configured SQLite DB path (env override) as an absolute Path."""
    load_dotenv()
    env_path = os.getenv(env_var)
    if env_path:
        return Path(env_path).expanduser().resolve()
    return DEFAULT_SQLITE_PATH.resolve()


def resolve_sqlite_db_path_str(env_var: str = "HILLS_SQLITE_DB") -> str:
    """Return the configured SQLite DB path (env override) as a string."""
    return str(resolve_sqlite_db_path(env_var=env_var))
