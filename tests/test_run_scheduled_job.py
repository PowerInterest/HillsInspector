from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from src.tools import run_scheduled_job
from src.utils.step_result import StepResult


def test_sunbiz_daily_raises_when_no_remote_files_match(monkeypatch: Any) -> None:
    class _FakeMirror:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def sync(self, **_kwargs: Any) -> dict[str, int]:
            return {"candidate_files": 0}

    monkeypatch.setattr(run_scheduled_job, "SunbizMirror", _FakeMirror)
    monkeypatch.setattr(
        run_scheduled_job,
        "load_sunbiz_raw",
        lambda **_kwargs: pytest.fail("daily loader should not run for zero remote matches"),
    )

    with pytest.raises(RuntimeError, match="matched no remote files"):
        run_scheduled_job.JOB_DEFINITIONS["sunbiz_daily"].handler("postgresql://db", {})


def test_sunbiz_daily_loads_raw_records_from_non_quarterly_feed(monkeypatch: Any) -> None:
    captured: dict[str, dict[str, Any]] = {}

    class _FakeMirror:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def sync(self, **kwargs: Any) -> dict[str, int]:
            captured["sync"] = kwargs
            return {"candidate_files": 4, "downloaded": 2, "skipped": 2}

    def _fake_load_sunbiz_raw(**kwargs: Any) -> dict[str, int]:
        captured["load"] = kwargs
        return {"files_discovered": 4, "files_loaded": 2, "files_skipped": 2}

    monkeypatch.setattr(run_scheduled_job, "SunbizMirror", _FakeMirror)
    monkeypatch.setattr(run_scheduled_job, "load_sunbiz_raw", _fake_load_sunbiz_raw)

    result = run_scheduled_job.JOB_DEFINITIONS["sunbiz_daily"].handler("postgresql://db", {})

    assert not isinstance(result, StepResult)
    assert isinstance(result, dict)
    assert result["success"] is True
    assert captured["sync"]["exclude"] == r"/quarterly/"
    assert captured["sync"]["dataset_profile"] is None
    assert captured["load"]["root"] == run_scheduled_job.DEFAULT_DATA_DIR / "public/doc"
    assert captured["load"]["pattern"] == r"^(?!quarterly/).+"


def test_sunbiz_entity_quarterly_loads_only_supported_quarterly_files(
    monkeypatch: Any,
) -> None:
    captured: dict[str, dict[str, Any]] = {}

    class _FakeMirror:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def sync(self, **kwargs: Any) -> dict[str, int]:
            captured["sync"] = kwargs
            return {"candidate_files": 2, "downloaded": 1, "skipped": 1}

    def _fake_load_sunbiz_entity(**kwargs: Any) -> dict[str, int]:
        captured["load"] = kwargs
        return {"files_scanned": 2}

    monkeypatch.setattr(run_scheduled_job, "SunbizMirror", _FakeMirror)
    monkeypatch.setattr(run_scheduled_job, "load_sunbiz_entity", _fake_load_sunbiz_entity)

    result = run_scheduled_job.JOB_DEFINITIONS["sunbiz_entity_quarterly"].handler(
        "postgresql://db",
        {},
    )

    assert not isinstance(result, StepResult)
    assert isinstance(result, dict)
    assert result["success"] is True
    assert captured["sync"]["dataset_profile"] == "entity-quarterly"
    assert captured["load"]["root"] == run_scheduled_job.DEFAULT_DATA_DIR / "public/doc/quarterly"
    assert captured["load"]["pattern"] == r"(?i)^(cor|gen)/(cordata|corevt|genfile|genevt)\.zip$"


def test_sunbiz_entity_quarterly_raises_when_loader_scans_nothing(
    monkeypatch: Any,
) -> None:
    class _FakeMirror:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def sync(self, **_kwargs: Any) -> dict[str, int]:
            return {"candidate_files": 1, "downloaded": 1, "skipped": 0}

    monkeypatch.setattr(run_scheduled_job, "SunbizMirror", _FakeMirror)
    monkeypatch.setattr(
        run_scheduled_job,
        "load_sunbiz_entity",
        lambda **_kwargs: {"files_scanned": 0},
    )

    with pytest.raises(RuntimeError, match="scanned no entity files"):
        run_scheduled_job.JOB_DEFINITIONS["sunbiz_entity_quarterly"].handler(
            "postgresql://db",
            {},
        )


def test_hcpa_bulk_uses_pg_loader_suite(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def _fake_load_hcpa_suite(**kwargs: Any) -> dict[str, int]:
        captured.update(kwargs)
        return {"parcels_loaded": 10}

    monkeypatch.setattr(run_scheduled_job, "load_hcpa_suite", _fake_load_hcpa_suite)

    result = run_scheduled_job.JOB_DEFINITIONS["hcpa_bulk"].handler(
        "postgresql://db",
        {"skip_latlon": "true", "force_sync": "1", "batch_size": "250"},
    )

    assert not isinstance(result, StepResult)
    assert isinstance(result, dict)
    assert result["success"] is True
    assert result["update"]["parcels_loaded"] == 10
    assert captured["downloads_dir"] == Path("data/bulk_data/hcpa")
    assert captured["include_latlon"] is False
    assert captured["force_sync"] is True
    assert captured["batch_size"] == 250
