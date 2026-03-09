from __future__ import annotations

from typing import Any

import pytest

from src.services import bulk_step_worker
from src.utils.step_result import StepResult, is_failed_payload


class _FakeController:
    def __init__(self, settings: Any) -> None:
        self.settings = settings

    def _run_hcpa_suite(self) -> StepResult:
        return StepResult(
            step_name="hcpa_suite",
            status="success",
            updated=1,
            details={
                "dsn": self.settings.dsn,
                "force_all": self.settings.force_all,
            },
        )


def test_run_bulk_step_returns_error_for_unknown_step() -> None:
    result = bulk_step_worker.run_bulk_step("not_a_step")

    assert result["success"] is False
    assert "unknown_bulk_step" in result["error"]


def test_run_bulk_step_calls_mapped_controller_method(monkeypatch: Any) -> None:
    monkeypatch.setattr(bulk_step_worker, "PgPipelineController", _FakeController)

    result = bulk_step_worker.run_bulk_step(
        "hcpa_suite",
        dsn="postgresql://user:pw@host:5432/db",
        force_all=True,
    )

    assert result["name"] == "hcpa_suite"
    assert result["status"] == "success"
    assert result["updated"] == 1
    assert result["details"]["dsn"] == "postgresql://user:pw@host:5432/db"
    assert result["details"]["force_all"] is True


def test_payload_failed_detects_nested_update_error() -> None:
    assert not is_failed_payload({"update": {"rows": 1}})
    assert is_failed_payload({"update": {"error": "boom"}})
    assert is_failed_payload({"update": {"success": False}})
    assert is_failed_payload({"status": "failed"})


def test_main_exits_nonzero_when_missing_step_env(monkeypatch: Any) -> None:
    monkeypatch.delenv("HI_BULK_STEP_NAME", raising=False)
    monkeypatch.delenv("HI_FORCE_ALL", raising=False)
    monkeypatch.delenv("SUNBIZ_PG_DSN", raising=False)

    with pytest.raises(SystemExit) as exc:
        bulk_step_worker.main()

    assert exc.value.code == 1


def test_main_exits_nonzero_when_step_summary_failed(monkeypatch: Any) -> None:
    monkeypatch.setenv("HI_BULK_STEP_NAME", "hcpa_suite")
    monkeypatch.delenv("HI_FORCE_ALL", raising=False)
    monkeypatch.delenv("SUNBIZ_PG_DSN", raising=False)
    monkeypatch.setattr(
        bulk_step_worker,
        "run_bulk_step",
        lambda *_args, **_kwargs: {"name": "hcpa_suite", "status": "failed"},
    )

    with pytest.raises(SystemExit) as exc:
        bulk_step_worker.main()

    assert exc.value.code == 1
