from __future__ import annotations

from typing import Any

import pytest

from src.services import bulk_step_worker


class _FakeController:
    def __init__(self, settings: Any) -> None:
        self.settings = settings

    def _run_hcpa_suite(self) -> dict[str, Any]:
        return {
            "ok": True,
            "dsn": self.settings.dsn,
            "force_all": self.settings.force_all,
        }


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

    assert result["ok"] is True
    assert result["dsn"] == "postgresql://user:pw@host:5432/db"
    assert result["force_all"] is True


def test_payload_failed_detects_nested_update_error() -> None:
    assert not bulk_step_worker._payload_failed({"update": {"rows": 1}})  # noqa: SLF001
    assert bulk_step_worker._payload_failed({"update": {"error": "boom"}})  # noqa: SLF001
    assert bulk_step_worker._payload_failed({"update": {"success": False}})  # noqa: SLF001


def test_main_exits_nonzero_when_missing_step_env(monkeypatch: Any) -> None:
    monkeypatch.delenv("HI_BULK_STEP_NAME", raising=False)
    monkeypatch.delenv("HI_FORCE_ALL", raising=False)
    monkeypatch.delenv("SUNBIZ_PG_DSN", raising=False)

    with pytest.raises(SystemExit) as exc:
        bulk_step_worker.main()

    assert exc.value.code == 1
