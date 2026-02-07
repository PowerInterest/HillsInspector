"""Shared logging utilities."""

from __future__ import annotations

import os
import time
from typing import Any

from loguru import logger


def env_log_level(default: str = "INFO") -> str:
    """Return log level string from LOG_LEVEL env (fallback to ``default``)."""
    return os.getenv("LOG_LEVEL", default).upper()


def add_optional_sinks() -> None:
    """Attach optional sinks controlled by env vars.

    - ``LOG_DEBUG_FILE``: path for a DEBUG sink (serialize=False).
    - ``LOG_JSON`` ("1"/"true"): write structured JSON to ``logs/hills_inspector_{time}.jsonl``.
    """

    debug_file = os.getenv("LOG_DEBUG_FILE")
    if debug_file:
        logger.add(debug_file, level="DEBUG", backtrace=True, diagnose=True, enqueue=False)

    if os.getenv("LOG_JSON", "0").lower() in {"1", "true", "yes", "on"}:
        os.makedirs("logs", exist_ok=True)
        logger.add(
            "logs/hills_inspector_{time}.jsonl",
            level="DEBUG",
            serialize=True,
            backtrace=True,
            diagnose=True,
            enqueue=False,
        )


def log_search(
    *,
    source: str,
    query: Any,
    results_raw: int,
    results_kept: int | None = None,
    duration_ms: float | None = None,
    **context: Any,
) -> None:
    """Standardized search/result log line.

    Args:
        source: external system ("ORI", "Tax", "Permit", etc.).
        query: search term / address / date descriptor.
        results_raw: count returned before filtering.
        results_kept: count after filtering (optional).
        duration_ms: elapsed milliseconds (optional).
        context: extra key/values (case_number, parcel_id, step, etc.).
    """

    payload = {
        "source": source,
        "query": query,
        "results_raw": results_raw,
    }
    if results_kept is not None:
        payload["results_kept"] = results_kept
    if duration_ms is not None:
        payload["duration_ms"] = round(duration_ms, 1)
    payload.update(context)
    logger.info("search", **payload)


class Timer:
    """Lightweight context timer for logging durations."""

    def __enter__(self) -> "Timer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, *exc: object) -> None:
        self.elapsed_ms = (time.perf_counter() - self._start) * 1000

    @property
    def ms(self) -> float:
        return (time.perf_counter() - self._start) * 1000

