from __future__ import annotations

import datetime as dt
from pathlib import Path

from src.utils.step_result import StepResult, is_failed_payload


def test_to_summary_dict_makes_details_json_safe() -> None:
    result = StepResult(
        step_name="demo",
        status="success",
        details={
            "when": dt.datetime(2026, 3, 9, 12, 0, tzinfo=dt.UTC),
            "path": Path("demo.txt"),
            "nested": {"day": dt.date(2026, 3, 9)},
        },
    )

    summary = result.to_summary_dict()

    assert summary["details"]["when"] == "2026-03-09T12:00:00+00:00"
    assert summary["details"]["path"] == "demo.txt"
    assert summary["details"]["nested"]["day"] == "2026-03-09"


def test_is_failed_payload_respects_step_result_summary_status() -> None:
    assert is_failed_payload({"status": "failed"}) is True
    assert is_failed_payload({"status": "degraded"}) is False
