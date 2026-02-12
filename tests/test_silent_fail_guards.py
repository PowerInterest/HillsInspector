import asyncio
import sqlite3
from types import SimpleNamespace
from typing import Any, cast

import pytest

from src.db.operations import PropertyDB
from src.models.property import Property
from src.orchestrator import PipelineOrchestrator
from src.scrapers.ori_api_scraper import ORIApiScraper
from src.services.ingestion_service import IngestionService


class DummyORIScraper(ORIApiScraper):
    def __init__(self):
        pass


def test_survival_parse_failure_returns_error_dict():
    prop = Property(case_number="CASE-1", parcel_id="1234567890", address="123 MAIN ST")
    fake_db = SimpleNamespace(
        get_auction_by_case=lambda _: {"extracted_judgment_data": "{not-json"},
    )
    fake_orch: Any = SimpleNamespace(
        db=fake_db,
        get_encumbrances_by_folio=lambda _: [],
        get_chain_of_title=lambda _: None,
    )

    result = PipelineOrchestrator._gather_and_analyze_survival(  # noqa: SLF001
        cast("PipelineOrchestrator", fake_orch), prop
    )

    assert "error" in result
    assert "Invalid extracted_judgment_data JSON" in result["error"]
    assert result["case_number"] == "CASE-1"


def test_survival_v2_parse_failure_returns_error_dict(monkeypatch):
    import src.services.step4v2.chain_builder as chain_builder_mod

    class FakeBuilder:
        def __init__(self, _conn):
            pass

        def get_chain(self, _folio):
            return []

        def get_encumbrances(self, _folio):
            return []

    monkeypatch.setattr(chain_builder_mod, "ChainBuilder", FakeBuilder)

    prop = Property(case_number="CASE-2", parcel_id="2234567890", address="456 OAK ST")
    fake_db = SimpleNamespace(connect=lambda: object())
    fake_orch: Any = SimpleNamespace(db=fake_db)
    auction = {"extracted_judgment_data": "{still-not-json"}

    result = PipelineOrchestrator._gather_and_analyze_survival_v2(  # noqa: SLF001
        cast("PipelineOrchestrator", fake_orch), prop, auction
    )

    assert "error" in result
    assert "Invalid extracted_judgment_data JSON" in result["error"]
    assert result["case_number"] == "CASE-2"


def test_ingest_property_raises_when_no_legal_search_terms():
    ori_scraper = DummyORIScraper()
    service = IngestionService(ori_scraper=ori_scraper, analyze_pdfs=False)
    prop = Property(case_number="CASE-3", parcel_id="3234567890", address="789 PINE ST")

    with pytest.raises(ValueError, match="No valid legal description/search terms"):
        service.ingest_property(prop)


def test_ingest_property_async_raises_when_no_legal_search_terms():
    ori_scraper = DummyORIScraper()
    service = IngestionService(ori_scraper=ori_scraper, analyze_pdfs=False)
    prop = Property(case_number="CASE-4", parcel_id="4234567890", address="101 MAPLE ST")

    with pytest.raises(ValueError, match="No valid legal description/search terms"):
        asyncio.run(service.ingest_property_async(prop))


def test_backfill_status_steps_raises_when_table_probe_fails(monkeypatch):
    db = PropertyDB(":memory:")

    class ExplodingConn:
        def execute(self, *_args, **_kwargs):
            raise sqlite3.OperationalError("boom")

    monkeypatch.setattr(db, "ensure_status_table", lambda: None)
    monkeypatch.setattr(db, "connect", lambda: ExplodingConn())

    with pytest.raises(sqlite3.OperationalError, match="boom"):
        db.backfill_status_steps()
