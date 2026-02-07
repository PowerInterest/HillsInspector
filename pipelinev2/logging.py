"""Lightweight logging helpers for the prototype pipeline."""

from __future__ import annotations

from typing import Any
from loguru import logger


def bind_context(**kwargs: Any):
    """Return a logger with bound contextual fields (step/run/case)."""
    return logger.bind(**{k: v for k, v in kwargs.items() if v is not None})


def step_started(step: str, run_id: str | None = None):
    bind_context(step=step, run_id=run_id).info("step_start")


def step_finished(step: str, summary: dict[str, Any], run_id: str | None = None):
    bind_context(step=step, run_id=run_id).info("step_end", **summary)


def search_log(source: str, query: Any, results_raw: int, results_kept: int | None = None, duration_ms: float | None = None, **ctx: Any):
    payload = {
        "source": source,
        "query": query,
        "results_raw": results_raw,
    }
    if results_kept is not None:
        payload["results_kept"] = results_kept
    if duration_ms is not None:
        payload["duration_ms"] = round(duration_ms, 1)
    payload.update({k: v for k, v in ctx.items() if v is not None})
    logger.info("search", **payload)
