from __future__ import annotations

from typing import Any
from typing import Self

import pytest

from sunbiz import pg_loader


class _FakeSession:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> bool:
        return False

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def execute(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("empty FLR loads should not attempt row inserts")


def test_load_sunbiz_flr_marks_zero_row_files_failed(
    monkeypatch: Any,
    tmp_path: Any,
) -> None:
    empty_zip = tmp_path / "flrf.zip"
    empty_zip.write_bytes(b"placeholder")

    session = _FakeSession()
    marked: list[dict[str, Any]] = []

    monkeypatch.setattr(pg_loader, "_collect_input_files", lambda **_kwargs: [empty_zip])
    monkeypatch.setattr(pg_loader, "get_session_factory", lambda _dsn: lambda: session)
    monkeypatch.setattr(pg_loader, "_compute_sha256", lambda _path: "sha256")
    monkeypatch.setattr(pg_loader, "_upsert_ingest_file", lambda **_kwargs: 11)
    monkeypatch.setattr(
        pg_loader,
        "_iter_text_records",
        lambda _path: iter([("unsupported_member.txt", 1, "raw line")]),
    )

    def _mark_ingest_file(**kwargs: Any) -> None:
        marked.append(kwargs)

    monkeypatch.setattr(pg_loader, "_mark_ingest_file", _mark_ingest_file)

    with pytest.raises(RuntimeError, match="No FLR records parsed"):
        pg_loader.load_sunbiz_flr(
            dsn="postgresql://db",
            root=tmp_path,
            pattern=None,
            limit_files=None,
            limit_lines=None,
            batch_size=100,
        )

    assert session.rollbacks == 1
    assert marked == [
        {
            "session": session,
            "file_id": 11,
            "status": "failed",
            "row_count": None,
            "error_message": "No FLR records parsed from flrf.zip; refusing to mark empty load as current",
        }
    ]
