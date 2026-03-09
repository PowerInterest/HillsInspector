"""Structured pipeline step results for consistent orchestration reporting.

Every ``_run_*`` method in ``PgPipelineController`` returns a ``StepResult``
instead of an ad-hoc dict.  This gives the controller a uniform contract for
logging, failure detection, and ``pipeline_job_runs.summary_json`` storage.

Status semantics:
    - ``success`` — step ran and changed data (``inserted + updated > 0``)
    - ``noop`` — step ran but found nothing to change
    - ``skipped`` — step was not attempted (service unavailable, flag disabled,
      data fresh)
    - ``failed`` — step hit errors (exception or explicit failure signal)
    - ``degraded`` — step partially succeeded (some rows failed, some succeeded)

The module also exports ``is_failed_payload()``, a shared helper that replaces
the three duplicate ``_payload_failed()`` functions formerly in
``bulk_step_worker.py``, ``market_data_worker.py``, and
``pg_market_data_scrapling.py``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass(slots=True)
class StepResult:
    """Structured result from a pipeline step."""

    step_name: str
    status: Literal["success", "skipped", "failed", "noop", "degraded"]
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    errors: int = 0
    duration_ms: int = 0
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def is_failure(self) -> bool:
        return self.status == "failed"

    @property
    def changed_rows(self) -> int:
        return self.inserted + self.updated

    def to_summary_dict(self) -> dict[str, Any]:
        """Serialize for ``pipeline_job_runs.summary_json``."""
        d: dict[str, Any] = {
            "name": self.step_name,
            "status": self.status,
            "inserted": self.inserted,
            "updated": self.updated,
            "skipped": self.skipped,
            "errors": self.errors,
            "duration_ms": self.duration_ms,
        }
        if self.details:
            d["details"] = self.details
        return d

    def log_line(self) -> str:
        """One-line summary for structured logging."""
        counts = (
            f"inserted={self.inserted}, updated={self.updated}, "
            f"skipped={self.skipped}, errors={self.errors}"
        )
        return (
            f"STEP {self.step_name}: {self.status} "
            f"({counts}) {self.duration_ms / 1000:.1f}s"
        )


def is_failed_payload(payload: dict[str, Any]) -> bool:
    """Check whether a raw service-result dict signals failure.

    This is the single source of truth replacing the three identical
    ``_payload_failed()`` functions that were duplicated across worker modules.
    """
    if payload.get("success") is False:
        return True
    if payload.get("error") not in {None, ""}:
        return True

    update = payload.get("update")
    if isinstance(update, dict):
        if update.get("success") is False:
            return True
        if update.get("error") not in {None, ""}:
            return True
    return False
