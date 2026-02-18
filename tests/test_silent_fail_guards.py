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


def test_invalid_folio_fallback_does_not_mark_skipped_when_ori_incomplete():
    class FakeWriter:
        def __init__(self):
            self.calls: list[tuple[str, dict[str, Any]]] = []

        async def enqueue(self, op: str, payload: dict[str, Any]) -> None:
            self.calls.append((op, payload))

        async def execute_with_result(self, func, *args, **kwargs):
            return func(*args, **kwargs)

    class FakeDB:
        def get_status_state(self, _case_number: str) -> str:
            return "processing"

        def get_parcel_by_folio(self, _folio: str):
            return None

        def set_surrogate_folio(self, _case_number: str, _surrogate: str) -> None:
            return None

        def mark_ori_party_fallback_used(self, _case_number: str, _note: str) -> None:
            return None

        def mark_step_complete(self, _case_number: str, _step: str) -> None:
            return None

        def mark_status_skipped(self, _case_number: str, _reason: str) -> None:
            return None

        def mark_status_retriable_error(
            self, _case_number: str, _error_message: str, _error_step: int | None = None
        ) -> None:
            return None

        def checkpoint(self) -> None:
            return None

        def is_status_step_complete(self, _case_number: str, _step_column: str) -> bool:
            return False

    async def _no_op_ori(_case_number: str, _prop: Property, fallback_mode: bool = False):
        assert fallback_mode is True

    async def _run() -> list[str]:
        writer = FakeWriter()
        fake_orch: Any = SimpleNamespace(
            db=FakeDB(),
            db_writer=writer,
            _run_ori_ingestion=_no_op_ori,
        )
        auction = {
            "case_number": "CASE-INV-1",
            "parcel_id": "PROPERTY APPRAISER",
            "address": "UNKNOWN",
            "plaintiff": "SAMPLE HOA, INC.",
            "defendant": "JOHN DOE",
        }
        await PipelineOrchestrator._enrich_property(  # noqa: SLF001
            cast("PipelineOrchestrator", fake_orch), auction
        )
        return [
            getattr(payload.get("func"), "__name__", str(payload.get("func")))
            for _, payload in writer.calls
        ]

    func_names = asyncio.run(_run())
    assert "mark_status_retriable_error" in func_names
    assert "mark_status_skipped" not in func_names
