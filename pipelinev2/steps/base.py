"""Common helpers for prototype steps."""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Callable

from pipelinev2.state import StepResult, RunContext


@contextmanager
def timed_step(step: str):
    start = time.perf_counter()
    try:
        yield lambda: (time.perf_counter() - start) * 1000
    finally:
        pass


def empty_result(step: str, duration_ms: float = 0.0) -> StepResult:
    return StepResult(step=step, duration_ms=duration_ms)


def noop_step(step: str) -> Callable[[RunContext], StepResult]:
    """Factory for steps that are intentionally skipped."""

    def _run(context: RunContext) -> StepResult:
        with timed_step(step) as elapsed:
            return StepResult(step=step, duration_ms=elapsed(), skipped=1)

    return _run
