from __future__ import annotations

from src.services.sunbiz_sync_service import SunbizMirror


def test_entity_quarterly_profile_excludes_nonprofit_archives() -> None:
    assert (
        SunbizMirror._matches_dataset_profile(  # noqa: SLF001
            "/public/doc/quarterly/cor/cordata.zip",
            "entity-quarterly",
        )
        is True
    )
    assert (
        SunbizMirror._matches_dataset_profile(  # noqa: SLF001
            "/public/doc/quarterly/gen/genevt.zip",
            "entity-quarterly",
        )
        is True
    )
    assert (
        SunbizMirror._matches_dataset_profile(  # noqa: SLF001
            "/public/doc/quarterly/non-profit/npcordata.zip",
            "entity-quarterly",
        )
        is False
    )
