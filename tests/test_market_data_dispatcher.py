from __future__ import annotations

from typing import Any

from src.services import market_data_dispatcher


def test_dispatch_skips_when_worker_is_already_running(
    monkeypatch: Any,
    tmp_path: Any,
) -> None:
    logs_dir = tmp_path / "logs"
    monkeypatch.setattr(market_data_dispatcher, "read_pid_file", lambda _path: 321)
    monkeypatch.setattr(market_data_dispatcher, "pid_is_alive", lambda _pid: True)

    result = market_data_dispatcher.dispatch_market_data_worker(
        "postgresql://user:pw@host:5432/db",
        logs_dir=logs_dir,
    )

    assert result["skipped"] is True
    assert result["reason"] == "market_data_worker_already_running"
    assert result["pid"] == 321


def test_dispatch_starts_detached_worker_and_writes_pid(
    monkeypatch: Any,
    tmp_path: Any,
) -> None:
    logs_dir = tmp_path / "logs"
    monkeypatch.setattr(market_data_dispatcher, "read_pid_file", lambda _path: None)

    captured: dict[str, Any] = {}

    class _FakeProcess:
        pid = 9876

        @staticmethod
        def poll() -> None:
            return None

    def _fake_popen(*args: Any, **kwargs: Any) -> _FakeProcess:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return _FakeProcess()

    monkeypatch.setattr(market_data_dispatcher.subprocess, "Popen", _fake_popen)

    result = market_data_dispatcher.dispatch_market_data_worker(
        "postgresql://user:pw@host:5432/db",
        logs_dir=logs_dir,
    )

    assert result["skipped"] is True
    assert result["reason"] == "market_data_worker_dispatched_background"
    assert result["dispatched"] is True
    assert result["pid"] == 9876

    pid_path = logs_dir / "market_data_worker.pid"
    assert pid_path.read_text(encoding="utf-8").strip() == "9876"

    command = captured["args"][0]
    assert command[0] == market_data_dispatcher.sys.executable
    assert command[1:] == ["-m", "src.services.market_data_worker"]
    assert captured["kwargs"]["start_new_session"] is True
    assert captured["kwargs"]["env"]["SUNBIZ_PG_DSN"] == "postgresql://user:pw@host:5432/db"


def test_dispatch_market_worker_fails_if_exits_immediately(
    monkeypatch: Any,
    tmp_path: Any,
) -> None:
    logs_dir = tmp_path / "logs"
    monkeypatch.setattr(market_data_dispatcher, "read_pid_file", lambda _path: None)

    class _FakeProcess:
        pid = 1111

        @staticmethod
        def poll() -> int:
            return 1

    monkeypatch.setattr(
        market_data_dispatcher.subprocess,
        "Popen",
        lambda *_args, **_kwargs: _FakeProcess(),
    )

    result = market_data_dispatcher.dispatch_market_data_worker(
        "postgresql://user:pw@host:5432/db",
        logs_dir=logs_dir,
    )

    assert result["reason"] == "market_data_worker_startup_failed"
    assert "market_data_worker_exited_immediately:1" in result["error"]
    assert not (logs_dir / "market_data_worker.pid").exists()
