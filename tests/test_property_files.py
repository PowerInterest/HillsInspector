from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from app.web.routers import properties


def test_normalize_serve_path_rejects_traversal() -> None:
    assert properties._normalize_serve_path("CASE123/documents/judgment.pdf") == "CASE123/documents/judgment.pdf"  # noqa: SLF001
    assert properties._normalize_serve_path(r"props\STRAP123\documents\deed.json") == "props/STRAP123/documents/deed.json"  # noqa: SLF001
    assert properties._normalize_serve_path("../secret.txt") is None  # noqa: SLF001
    assert properties._normalize_serve_path("CASE123/../secret.txt") is None  # noqa: SLF001


def test_property_file_browser_excludes_html() -> None:
    assert ".html" not in properties._VIEWABLE_EXTENSIONS  # noqa: SLF001


def test_resolve_property_file_path_only_serves_listed_files(monkeypatch: Any) -> None:
    project_root = Path(properties.__file__).resolve().parents[3]
    case_dir = project_root / "data" / "__pytest_property_files__" / "CASE123" / "documents"
    case_dir.mkdir(parents=True, exist_ok=True)
    allowed_file = case_dir / "judgment.pdf"
    allowed_file.write_text("pdf-placeholder", encoding="utf-8")

    try:
        rel_path = str(allowed_file.resolve().relative_to(project_root))
        monkeypatch.setattr(
            properties,
            "_pg_all_files_for_property",
            lambda _identifier: {
                "documents": [
                    {
                        "serve_path": "__pytest_property_files__/CASE123/documents/judgment.pdf",
                        "file_path": rel_path,
                    }
                ]
            },
        )

        resolved = properties._resolve_property_file_path(  # noqa: SLF001
            "FOLIO123",
            "__pytest_property_files__/CASE123/documents/judgment.pdf",
        )

        assert resolved == allowed_file.resolve()
        assert (
            properties._resolve_property_file_path(  # noqa: SLF001
                "FOLIO123",
                "__pytest_property_files__/OTHER/documents/judgment.pdf",
            )
            is None
        )
        assert (
            properties._resolve_property_file_path(  # noqa: SLF001
                "FOLIO123",
                "../__pytest_property_files__/CASE123/documents/judgment.pdf",
            )
            is None
        )
    finally:
        shutil.rmtree(project_root / "data" / "__pytest_property_files__", ignore_errors=True)
