from __future__ import annotations

from typing import Any

from src.services import pg_pipeline_controller


class _DummyEngine:
    pass


def _build_controller(monkeypatch: Any) -> pg_pipeline_controller.PgPipelineController:
    monkeypatch.setattr(
        pg_pipeline_controller,
        "resolve_pg_dsn",
        lambda _dsn: "postgresql://user:pw@host:5432/db",
    )
    monkeypatch.setattr(
        pg_pipeline_controller,
        "get_engine",
        lambda _dsn: _DummyEngine(),
    )
    return pg_pipeline_controller.PgPipelineController(
        pg_pipeline_controller.ControllerSettings(),
    )


def test_controller_reads_nested_bulk_loader_metrics(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)

    class _FakeBulkSvc:
        available = True

        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == controller.dsn

        def get_current_state(self) -> dict[str, Any]:
            return {"cases_count": 0, "latest_loaded_at": None}

        def update(self, force_download: bool = False) -> dict[str, Any]:
            assert force_download is False
            return {
                "cases": {"rows_upserted": 5},
                "events": {"rows_inserted": 7},
                "parties": {"rows_upserted": 11},
                "disposed": {"rows_upserted": 13},
                "garnishment": {"rows_inserted": 17},
                "official_records": {"rows_upserted": 19},
            }

    class _FakeCriminalSvc:
        available = True

        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == controller.dsn

        def update(self, force_download: bool = False) -> dict[str, Any]:
            assert force_download is False
            return {"load": {"rows_inserted": 23}}

    class _FakeCivilAlphaSvc:
        available = True

        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == controller.dsn

        def update(self, force_download: bool = False) -> dict[str, Any]:
            assert force_download is False
            return {"load": {"cases_upserted": 29, "parties_upserted": 29}}

    class _FakeNalSvc:
        available = True

        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == controller.dsn

        def get_current_state(self) -> dict[str, Any]:
            return {"latest_tax_year": None, "latest_loaded_at": None}

        def update(self, force_download: bool = False) -> dict[str, Any]:
            assert force_download is False
            return {"load_stats": {"parcels_upserted": 31}}

    class _FakeFlrSvc:
        available = True

        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == controller.dsn

        def get_current_state(self) -> dict[str, Any]:
            return {"filings_count": 0, "latest_loaded_at": None}

        def update(self, skip_sftp: bool = False, force_download: bool = False) -> dict[str, Any]:
            assert skip_sftp is False
            assert force_download is False
            return {
                "load_stats": {
                    "filings_upserted": 37,
                    "parties_inserted": 41,
                    "events_inserted": 43,
                }
            }

    class _FakeTrustSvc:
        available = True
        unavailable_reason = None

        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == controller.dsn

        def run(self, force_reprocess: bool = False) -> dict[str, Any]:
            assert force_reprocess is False
            return {
                "rows_upserted": 47,
                "rows_deleted": 3,
                "summary_rows_written": 5,
            }

    monkeypatch.setattr(pg_pipeline_controller, "PgClerkBulkService", _FakeBulkSvc)
    monkeypatch.setattr(pg_pipeline_controller, "PgClerkCriminalService", _FakeCriminalSvc)
    monkeypatch.setattr(pg_pipeline_controller, "PgClerkCivilAlphaService", _FakeCivilAlphaSvc)
    monkeypatch.setattr(pg_pipeline_controller, "PgNalService", _FakeNalSvc)
    monkeypatch.setattr(pg_pipeline_controller, "PgFlrService", _FakeFlrSvc)
    monkeypatch.setattr(
        "src.services.pg_trust_accounts.PgTrustAccountsService",
        lambda **_kwargs: _FakeTrustSvc(controller.dsn),
    )

    assert controller._run_clerk_bulk().inserted == 72  # noqa: SLF001
    assert controller._run_clerk_criminal().inserted == 23  # noqa: SLF001
    assert controller._run_clerk_civil_alpha().inserted == 29  # noqa: SLF001
    assert controller._run_nal().inserted == 31  # noqa: SLF001
    assert controller._run_flr().inserted == 121  # noqa: SLF001
    assert controller._run_trust_accounts().updated == 55  # noqa: SLF001


def test_controller_reads_phase_b_service_metrics(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)

    class _FakeAuctionSvc:
        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == controller.dsn

        def run(self, limit: int | None = None) -> dict[str, Any]:
            assert limit is None
            return {"auctions_saved": 11}

    class _FakeJudgmentSvc:
        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == controller.dsn

        def run(self, limit: int | None = None) -> dict[str, Any]:
            assert limit is None
            return {"pdfs_extracted": 3, "judgments_loaded_to_pg": 5}

    class _FakeIdentifierSvc:
        available = True

        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == controller.dsn

        def run(self, limit: int | None = None) -> dict[str, Any]:
            assert limit is None
            return {"rows_updated": 7}

    class _FakeOriSvc:
        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == controller.dsn

        def run(self, limit: int | None = None) -> dict[str, Any]:
            assert limit is None
            return {
                "encumbrances_saved": 13,
                "inferred_saved": 2,
                "satisfactions_linked": 1,
                "errors": 0,
            }

    class _FakeMunicipalSvc:
        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == controller.dsn

        def run_phase0(self, **_kwargs: Any) -> dict[str, Any]:
            return {"findings_written": 17}

    class _FakeMortgageSvc:
        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == controller.dsn

        def run(self, limit: int | None = None) -> dict[str, Any]:
            assert limit is None
            return {"mortgages_extracted": 19}

    monkeypatch.setattr("src.services.pg_auction_service.PgAuctionService", _FakeAuctionSvc)
    monkeypatch.setattr("src.services.pg_judgment_service.PgJudgmentService", _FakeJudgmentSvc)
    monkeypatch.setattr(
        "src.services.pg_foreclosure_identifier_recovery_service.PgForeclosureIdentifierRecoveryService",
        _FakeIdentifierSvc,
    )
    monkeypatch.setattr("src.services.pg_ori_service.PgOriService", _FakeOriSvc)
    monkeypatch.setattr("src.services.pg_municipal_lien_service.PgMunicipalLienService", _FakeMunicipalSvc)
    monkeypatch.setattr("src.services.pg_mortgage_extraction_service.PgMortgageExtractionService", _FakeMortgageSvc)

    assert controller._run_auction_scrape().inserted == 11  # noqa: SLF001
    assert controller._run_judgment_extract().updated == 8  # noqa: SLF001
    assert controller._run_identifier_recovery().updated == 7  # noqa: SLF001
    assert controller._run_ori_search().updated == 16  # noqa: SLF001
    assert controller._run_municipal_liens_phase0().inserted == 17  # noqa: SLF001
    assert controller._run_mortgage_extract().updated == 19  # noqa: SLF001


def test_controller_reads_refresh_metrics_from_refresh_script(monkeypatch: Any) -> None:
    controller = _build_controller(monkeypatch)

    class _FakeForeclosureSvc:
        available = True

        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == controller.dsn

        def refresh(self) -> dict[str, int]:
            return {
                "enriched": 5,
                "strap_resolved": 3,
                "coords_enriched": 2,
                "resale": 7,
                "events_inserted": 11,
                "encumbrances": 13,
                "archived": 17,
                "judgments": 19,
                "rescheduled_reused": 23,
            }

    monkeypatch.setattr(pg_pipeline_controller, "PgForeclosureService", _FakeForeclosureSvc)
    monkeypatch.setattr(
        "src.scripts.refresh_foreclosures.refresh",
        lambda **_kwargs: {
            "enriched": 2,
            "strap_resolved": 3,
            "coords_enriched": 5,
            "resale": 7,
            "events_inserted": 11,
            "encumbrances": 13,
            "archived": 17,
            "judgments": 19,
            "rescheduled_reused": 23,
        },
    )

    assert controller._run_foreclosure_refresh().updated == 100  # noqa: SLF001
    assert controller._run_final_refresh().updated == 100  # noqa: SLF001
