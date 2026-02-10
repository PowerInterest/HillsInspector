"""Shared state objects for the pipeline v2 prototype."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional


@dataclass(slots=True)
class StepError:
    step: str
    message: str
    case_number: str | None = None
    parcel_id: str | None = None
    exception: Exception | None = None
    traceback: str | None = None


@dataclass(slots=True)
class StepResult:
    step: str
    duration_ms: float = 0.0
    processed: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    retried: int = 0
    artifacts: Dict[str, Any] = field(default_factory=dict)
    errors: List[StepError] = field(default_factory=list)


@dataclass(slots=True)
class RunContext:
    start_date: date
    end_date: date
    start_step: int = 1
    skip_tax_deeds: bool = False
    retry_failed: bool = False
    max_retries: int = 3
    geocode_missing_parcels: bool = True
    geocode_limit: int | None = 25
    auction_limit: int | None = None
    run_id: str = ""
    data_dir: Path = field(default_factory=lambda: Path("datav2"))
    # Allows callers to inject prebuilt services; keep optional to avoid side-effects in prototype.
    services: Dict[str, Any] = field(default_factory=dict)


# Type alias for step call signatures
StepFn = Callable[[RunContext], StepResult]
