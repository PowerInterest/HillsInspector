from __future__ import annotations

from typing import Any

from src.services import controller_step_dispatcher


def test_dispatch_controller_step_skips_if_worker_running(
    monkeypatch: Any,
    tmp_path: Any,
) -> None:
    logs_dir = tmp_path / "logs"
    monkeypatch.setattr(controller_step_dispatcher, "read_pid_file", lambda _path: 4321)
    monkeypatch.setattr(controller_step_dispatcher, "pid_is_alive", lambda _pid: True)

    result = controller_step_dispatcher.dispatch_controller_step(
        "hcpa_suite",
        "postgresql://user:pw@host:5432/db",
        logs_dir=logs_dir,
    )

    assert result["skipped"] is True
    assert result["reason"] == "step_worker_already_running"
    assert result["step_name"] == "hcpa_suite"


def test_dispatch_controller_step_launches_controller_process(
    monkeypatch: Any,
    tmp_path: Any,
) -> None:
    logs_dir = tmp_path / "logs"
    monkeypatch.setattr(controller_step_dispatcher, "read_pid_file", lambda _path: None)

    captured: dict[str, Any] = {}

    class _FakeProcess:
        pid = 2468

        @staticmethod
        def poll() -> None:
            return None

    def _fake_popen(*args: Any, **kwargs: Any) -> _FakeProcess:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return _FakeProcess()

    monkeypatch.setattr(controller_step_dispatcher.subprocess, "Popen", _fake_popen)

    result = controller_step_dispatcher.dispatch_controller_step(
        "hcpa_suite",
        "postgresql://user:pw@host:5432/db",
        force_all=True,
        logs_dir=logs_dir,
    )

    assert result["skipped"] is True
    assert result["reason"] == "step_worker_dispatched_background"
    assert result["dispatched"] is True
    assert result["step_name"] == "hcpa_suite"
    assert result["pid"] == 2468

    pid_path = logs_dir / "hcpa_suite.pid"
    assert pid_path.read_text(encoding="utf-8").strip() == "2468"

    command = captured["args"][0]
    assert command[0] == controller_step_dispatcher.sys.executable
    assert command[1:] == ["-m", "src.services.bulk_step_worker"]
    assert captured["kwargs"]["env"]["HI_BULK_STEP_NAME"] == "hcpa_suite"
    assert captured["kwargs"]["env"]["HI_FORCE_ALL"] == "1"
    assert captured["kwargs"]["env"]["SUNBIZ_PG_DSN"] == "postgresql://user:pw@host:5432/db"


def test_dispatch_controller_step_fails_if_worker_exits_immediately(
    monkeypatch: Any,
    tmp_path: Any,
) -> None:
    logs_dir = tmp_path / "logs"
    monkeypatch.setattr(controller_step_dispatcher, "read_pid_file", lambda _path: None)

    class _FakeProcess:
        pid = 4321

        @staticmethod
        def poll() -> int:
            return 2

    monkeypatch.setattr(
        controller_step_dispatcher.subprocess,
        "Popen",
        lambda *_args, **_kwargs: _FakeProcess(),
    )

    result = controller_step_dispatcher.dispatch_controller_step(
        "hcpa_suite",
        "postgresql://user:pw@host:5432/db",
        logs_dir=logs_dir,
    )

    assert result["reason"] == "step_worker_startup_failed"
    assert "step_worker_exited_immediately:2" in result["error"]
    assert not (logs_dir / "hcpa_suite.pid").exists()
