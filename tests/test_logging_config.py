from __future__ import annotations

from loguru import logger

from src.utils.logging_config import configure_logger


def test_configure_logger_writes_to_shared_and_extra_log_files(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)

    configure_logger(extra_log_files=["controller_runs/controller-test.log"])

    with logger.contextualize(run_id="test-run"):
        logger.info("controller log entry")

    shared_log = tmp_path / "logs" / "hills_inspector.log"
    run_log = tmp_path / "logs" / "controller_runs" / "controller-test.log"

    shared_text = shared_log.read_text(encoding="utf-8")
    run_text = run_log.read_text(encoding="utf-8")

    assert "controller log entry" in shared_text
    assert "run=test-run" in shared_text
    assert "controller log entry" in run_text
    assert "run=test-run" in run_text
