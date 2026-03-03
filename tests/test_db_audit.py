from __future__ import annotations

from src.tools.db_audit import _count_existing_paths


def test_count_existing_paths_counts_only_files_present(tmp_path) -> None:
    present = tmp_path / "judgment.pdf"
    present.write_text("pdf", encoding="utf-8")

    missing = tmp_path / "missing.pdf"

    assert _count_existing_paths([str(present), str(missing), None, ""]) == 1
