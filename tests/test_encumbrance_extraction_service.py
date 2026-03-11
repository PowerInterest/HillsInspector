"""Tests for pg_encumbrance_extraction_service dispatch and cache logic."""

import asyncio
import json
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock

import fitz
import pytest

from src.models.deed_extraction import DeedExtraction
from src.models.mortgage_extraction import MortgageExtraction
from src.models.lien_extraction import LienExtraction
from src.models.lis_pendens_extraction import LisPendensExtraction
from src.models.satisfaction_extraction import SatisfactionExtraction
from src.models.assignment_extraction import AssignmentExtraction
from src.models.noc_extraction import NOCExtraction

if TYPE_CHECKING:
    from playwright.async_api import Page


class TestDispatchTable:
    def test_import_service(self):
        from src.services.pg_encumbrance_extraction_service import EXTRACTION_DISPATCH
        assert isinstance(EXTRACTION_DISPATCH, dict)

    @pytest.mark.parametrize(
        ("enc_type", "expected_model"),
        [
            ("mortgage", MortgageExtraction),
            ("lis_pendens", LisPendensExtraction),
            ("lien", LienExtraction),
            ("satisfaction", SatisfactionExtraction),
            ("release", SatisfactionExtraction),
            ("assignment", AssignmentExtraction),
            ("noc", NOCExtraction),
            ("easement", DeedExtraction),
            ("other", DeedExtraction),
        ],
    )
    def test_dispatch_maps_type_to_model(self, enc_type, expected_model):
        from src.services.pg_encumbrance_extraction_service import EXTRACTION_DISPATCH
        prompt, model_cls = EXTRACTION_DISPATCH[enc_type]
        assert model_cls is expected_model
        assert isinstance(prompt, str)
        assert len(prompt) > 50, f"Prompt for {enc_type} looks too short to be a real prompt"

    def test_all_dispatch_prompts_contain_instructions(self):
        from src.services.pg_encumbrance_extraction_service import EXTRACTION_DISPATCH
        for enc_type, (prompt, _) in EXTRACTION_DISPATCH.items():
            lower = prompt.lower()
            assert "extract" in lower or "analyz" in lower, (
                f"Prompt for {enc_type} missing extraction/analysis instructions"
            )

    def test_dispatch_response_format_uses_model_schema(self):
        from src.services.pg_encumbrance_extraction_service import EXTRACTION_DISPATCH

        prompt, model_cls = EXTRACTION_DISPATCH["mortgage"]

        assert isinstance(prompt, str)
        schema = model_cls.model_json_schema()
        assert schema["additionalProperties"] is False
        assert "mortgage_type" in schema["required"]

    def test_strip_legacy_output_format_removes_stale_json_examples(self):
        from src.services.pg_encumbrance_extraction_service import (
            _strip_legacy_output_format,
        )

        prompt = """
## DOCUMENT PURPOSE
Keep this.

## OUTPUT FORMAT
Return ONLY valid JSON:
{"debtor": "old"}

## CRITICAL
Keep this too.
"""

        cleaned = _strip_legacy_output_format(prompt)

        assert "debtor" not in cleaned
        assert "## DOCUMENT PURPOSE" in cleaned
        assert "## CRITICAL" in cleaned

    def test_schema_contract_uses_live_lien_field_names(self):
        from src.services.pg_encumbrance_extraction_service import _schema_contract_text

        contract = _schema_contract_text(LienExtraction)

        assert "lienor" in contract
        assert "lienee" in contract
        assert "confidence_score" in contract
        assert '"debtor"' not in contract
        assert '"creditor"' not in contract

    def test_schema_contract_uses_live_assignment_field_names(self):
        from src.services.pg_encumbrance_extraction_service import _schema_contract_text

        contract = _schema_contract_text(AssignmentExtraction)

        assert "assignor" in contract
        assert "assignee" in contract
        assert "parent_instrument" in contract
        assert "confidence_score" in contract
        assert '"original_mortgage"' not in contract


class TestCacheLogic:
    def test_cache_path_from_pdf(self):
        from src.services.pg_encumbrance_extraction_service import _cache_path_for
        pdf = Path("/tmp/data/mortgage_12345.pdf")  # noqa: S108
        assert _cache_path_for(pdf) == Path("/tmp/data/mortgage_12345_extracted.json")  # noqa: S108

    def test_load_cache_returns_dict_when_exists(self):
        from src.services.pg_encumbrance_extraction_service import _load_cache, _cache_path_for
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            pdf = Path(f.name)
        cache = _cache_path_for(pdf)
        cache.write_text(json.dumps({"principal_amount": 100000}))
        try:
            result = _load_cache(pdf)
            assert result == {"principal_amount": 100000}
        finally:
            cache.unlink(missing_ok=True)
            pdf.unlink(missing_ok=True)

    def test_load_cache_returns_none_when_missing(self):
        from src.services.pg_encumbrance_extraction_service import _load_cache
        result = _load_cache(Path("/tmp/nonexistent_abc123.pdf"))  # noqa: S108
        assert result is None

    def test_find_unextracted_filters_to_rows_missing_pg_payload(self) -> None:
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        captured: dict[str, Any] = {}

        class _FakeResult:
            def mappings(self) -> "_FakeResult":
                return self

            def all(self) -> list[dict[str, Any]]:
                return []

        class _FakeConn:
            def __enter__(self) -> "_FakeConn":
                return self

            def __exit__(
                self,
                _exc_type: object,
                _exc: object,
                _tb: object,
            ) -> None:
                return None

            def execute(self, sql: Any, params: dict[str, Any]) -> _FakeResult:
                captured["sql"] = str(sql)
                captured["params"] = params
                return _FakeResult()

        class _FakeEngine:
            def connect(self) -> _FakeConn:
                return _FakeConn()

        svc = PgEncumbranceExtractionService()
        svc.engine = _FakeEngine()

        rows = svc._find_unextracted(  # noqa: SLF001
            limit=5,
            straps=["123"],
            enc_types=["mortgage"],
        )

        assert rows == []
        assert "oe.extracted_data IS NULL" in captured["sql"]
        assert "!= 'release'" not in captured["sql"], "release should no longer be excluded"
        assert captured["params"] == {
            "straps": ["123"],
            "enc_types": ["mortgage"],
            "lim": 5,
        }

    def test_backfill_missing_ori_ids_uses_exact_instrument_lookup(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from src.services.pg_encumbrance_extraction_service import (
            EXTRACTION_DISPATCH,
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
        svc.dsn = "postgresql://db"

        class _FakeOriService:
            def __init__(self, dsn: str | None = None) -> None:
                assert dsn == "postgresql://db"

            def backfill_missing_ori_ids(self, **kwargs: Any) -> dict[str, Any]:
                assert kwargs == {
                    "limit": None,
                    "straps": None,
                    "enc_types": sorted(EXTRACTION_DISPATCH),
                    "only_unextracted": True,
                }
                return {"resolved": 1, "api_calls": 3}

        monkeypatch.setattr("src.services.pg_ori_service.PgOriService", _FakeOriService)

        result = svc._backfill_missing_ori_ids()  # noqa: SLF001

        assert result["updated"] == 1
        assert result["api_calls"] == 3

    def test_backfill_missing_ori_ids_preserves_explicit_type_scope(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
        svc.dsn = "postgresql://db"

        class _FakeOriService:
            def __init__(self, dsn: str | None = None) -> None:
                assert dsn == "postgresql://db"

            def backfill_missing_ori_ids(self, **kwargs: Any) -> dict[str, Any]:
                assert kwargs == {
                    "limit": 5,
                    "straps": ["strap-1"],
                    "enc_types": ["assignment", "mortgage"],
                    "only_unextracted": True,
                }
                return {"resolved": 2, "api_calls": 4}

        monkeypatch.setattr("src.services.pg_ori_service.PgOriService", _FakeOriService)

        result = svc._backfill_missing_ori_ids(  # noqa: SLF001
            limit=5,
            straps=["strap-1"],
            enc_types=["mortgage", "assignment"],
        )

        assert result["updated"] == 2
        assert result["api_calls"] == 4

    def test_process_one_ignores_stale_mortgage_cache_and_falls_through_to_download(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
            _cache_path_for,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
        pdf_path = tmp_path / "mortgage_2024000123.pdf"
        cache_path = _cache_path_for(pdf_path)
        cache_path.write_text(json.dumps({"borrower": "OLD NAME", "lender": "OLD BANK"}))

        monkeypatch.setattr(
            PgEncumbranceExtractionService,
            "_pdf_path_for",
            staticmethod(lambda _row: pdf_path),
        )

        async def _fake_download(_page: Any, _row: dict[str, Any]) -> Path | None:
            return None

        monkeypatch.setattr(svc, "_download_pdf", _fake_download)

        row = {
            "id": 17,
            "encumbrance_type": "mortgage",
            "instrument_number": "2024000123",
            "ori_id": "ori-17",
        }

        result = asyncio.run(svc._process_one(cast("Page", None), row))  # noqa: SLF001

        assert result == {"_status": "error", "_reason": "download_failed"}
        assert not cache_path.exists()

    def test_process_one_keeps_enriched_cached_lp_without_reextract(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
            _cache_path_for,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
        pdf_path = tmp_path / "lis_pendens_2024000777.pdf"
        cache_path = _cache_path_for(pdf_path)
        cache_path.write_text(json.dumps({
            "instrument_number": "2024000777",
            "recording_book": None,
            "recording_page": None,
            "recording_date": "2024-01-15",
            "execution_date": "2024-01-10",
            "property_address": None,
            "legal_description": "LEGAL",
            "parcel_id": None,
            "confidence_score": 0.8,
            "unclear_sections": [],
            "plaintiff": "BANK",
            "defendants": ["OWNER"],
            "civil_case_number": None,
            "foreclosed_instrument": None,
            "is_release": False,
        }))

        monkeypatch.setattr(
            PgEncumbranceExtractionService,
            "_pdf_path_for",
            staticmethod(lambda _row: pdf_path),
        )
        monkeypatch.setattr(
            svc,
            "_enrich_from_metadata",
            lambda validated, _enc_type, _row: {
                **validated,
                "property_address": "123 MAIN ST",
                "civil_case_number": "25-CA-000777",
            },
        )
        monkeypatch.setattr(svc, "_address_resolves", lambda address: address == "123 MAIN ST")

        saved: dict[str, Any] = {}
        monkeypatch.setattr(
            svc,
            "_save_to_pg",
            lambda encumbrance_id, data: saved.update({"id": encumbrance_id, "data": data}),
        )

        async def _unexpected_download(_page: Any, _row: dict[str, Any]) -> Path | None:
            raise AssertionError("cache hit should not re-download")

        monkeypatch.setattr(svc, "_download_pdf", _unexpected_download)

        row = {
            "id": 77,
            "encumbrance_type": "lis_pendens",
            "instrument_number": "2024000777",
            "case_number": "292025CA000777A001HC",
            "ori_id": "ori-77",
            "strap": "strap-77",
            "folio": "folio-77",
        }

        result = asyncio.run(svc._process_one(cast("Page", None), row))  # noqa: SLF001

        assert result is not None
        assert result["_status"] == "cached"
        assert saved["id"] == 77
        assert saved["data"]["property_address"] == "123 MAIN ST"
        assert saved["data"]["civil_case_number"] == "25-CA-000777"


class TestEndToEnd:
    """Integration: query -> cache miss -> extract -> validate -> save."""

    def test_run_with_no_unextracted_returns_zeros(self):
        """When DB has no unextracted rows, run() returns all-zero stats."""
        from unittest.mock import patch

        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService()
        with (
            patch.object(svc, "_backfill_missing_ori_ids", return_value={"updated": 0, "api_calls": 0}),
            patch.object(svc, "_find_unextracted", return_value=[]),
        ):
            result = svc.run()
        assert result["extracted"] == 0
        assert result["errors"] == 0

    def test_validate_accepts_valid_mortgage(self):
        """Pydantic validation passes for a well-formed mortgage dict."""
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        data = {
            # BaseDocumentExtraction fields
            "instrument_number": "2024-0012345",
            "recording_book": None,
            "recording_page": None,
            "recording_date": "2024-01-15",
            "execution_date": "2024-01-10",
            "property_address": "123 Main St, Tampa, FL 33601",
            "legal_description": "LOT 5, BLOCK 3, TAMPA PALMS UNIT 1",
            "parcel_id": "1929084000",
            "confidence_score": 0.9,
            "unclear_sections": [],
            # MortgageExtraction fields
            "mortgage_type": "MTG",
            "mortgagor": "JOHN SMITH",
            "mortgagee": "WELLS FARGO BANK",
            "principal_amount": 250000.0,
            "interest_rate": 6.5,
            "maturity_date": "2054-01-15",
            "is_adjustable_rate": False,
            "mers_min": None,
            "is_mers_nominee": False,
            "association_name": None,
            "has_pud_rider": False,
            "has_condo_rider": False,
        }

        result, messages = PgEncumbranceExtractionService._validate(data, "mortgage")  # noqa: SLF001
        assert messages == []
        assert result is not None
        assert result["principal_amount"] == 250000.0
        assert result["mortgagee"] == "WELLS FARGO BANK"
        assert result["mortgagor"] == "JOHN SMITH"

    def test_validate_rejects_prompt_shaped_payload(self):
        """Validation must fail closed when the model returns the wrong contract."""
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        prompt_shaped = {
            "borrower": "JOHN SMITH",
            "lender": "WELLS FARGO BANK",
            "principal_amount": 250000.0,
            "confidence": "high",
        }

        result, messages = PgEncumbranceExtractionService._validate(prompt_shaped, "mortgage")  # noqa: SLF001

        assert result is None
        assert messages

    def test_validate_repairs_partial_lien_payload_and_drops_unknown_keys(self):
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        partial = {
            "instrument_number": "2006029847",
            "recording_book": "16010",
            "recording_page": "1385",
            "recording_date": "2006-01-19",
            "execution_date": "2006-01-13",
            "property_address": None,
            "legal_description": "Test legal description",
            "WITH": "spurious OCR spill",
            "follows": "more OCR spill",
        }

        result, messages = PgEncumbranceExtractionService._validate(  # noqa: SLF001
            partial,
            "lien",
            row_context={"id": 137024, "instrument_number": "2006029847"},
            source="fresh extraction",
        )

        assert messages == []
        assert result is not None
        assert result["lien_type"] is None
        assert result["lienor"] is None
        assert result["lienee"] is None
        assert result["unclear_sections"] == []
        assert "WITH" not in result
        assert "follows" not in result

    def test_validate_repairs_assignment_using_ori_party_metadata(self):
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        partial = {
            "instrument_number": "2011326920",
            "recording_book": "20743",
            "recording_page": "317",
            "recording_date": "2011-10-06",
            "execution_date": "2011-10-01",
            "property_address": None,
            "legal_description": "Test legal description",
            "parcel_id": None,
            "confidence_score": 0.7,
        }

        result, messages = PgEncumbranceExtractionService._validate(  # noqa: SLF001
            partial,
            "assignment",
            row_context={
                "id": 138531,
                "instrument_number": "2011326920",
                "party1": "REDUS PROPERTIES INC, REDUS TRG LLC",
                "party2": "DREF II FL I LLC",
            },
            source="fresh extraction",
        )

        assert messages == []
        assert result is not None
        assert result["assignor"] == "REDUS PROPERTIES INC, REDUS TRG LLC"
        assert result["assignee"] == "DREF II FL I LLC"
        assert result["unclear_sections"] == []
        assert result["assignment_type"] is None
        assert result["parent_instrument"] is None

    def test_extract_from_ocr_text_passes_json_schema_response_format(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService()
        captured: dict[str, Any] = {}

        def _fake_analyze_text(
            prompt: str,
            *,
            max_tokens: int,
            response_format: dict[str, Any] | None = None,
        ) -> str:
            captured["prompt"] = prompt
            captured["max_tokens"] = max_tokens
            captured["response_format"] = response_format
            return json.dumps(
                {
                    "instrument_number": "2024-0012345",
                    "recording_book": None,
                    "recording_page": None,
                    "recording_date": "2024-01-15",
                    "execution_date": "2024-01-10",
                    "property_address": "123 Main St, Tampa, FL 33601",
                    "legal_description": "LOT 5, BLOCK 3, TAMPA PALMS UNIT 1",
                    "parcel_id": "1929084000",
                    "confidence_score": 0.9,
                    "unclear_sections": [],
                    "mortgage_type": "MTG",
                    "mortgagor": "JOHN SMITH",
                    "mortgagee": "WELLS FARGO BANK",
                    "principal_amount": 250000.0,
                    "interest_rate": 6.5,
                    "maturity_date": "2054-01-15",
                    "is_adjustable_rate": False,
                    "mers_min": None,
                    "is_mers_nominee": False,
                    "association_name": None,
                    "has_pud_rider": False,
                    "has_condo_rider": False,
                }
            )

        monkeypatch.setattr(svc.vision, "analyze_text", _fake_analyze_text)

        result = svc._extract_from_ocr_text("--- PAGE 1 ---\nMortgage text", "mortgage")  # noqa: SLF001

        assert result is not None
        assert result["raw_text"].startswith("--- PAGE 1 ---")
        assert captured["max_tokens"] == 4000
        assert captured["response_format"]["type"] == "json_schema"
        assert captured["response_format"]["json_schema"]["name"] == "mortgage_extraction"
        assert "schema" in captured["response_format"]["json_schema"]
        assert "## JSON CONTRACT" in captured["prompt"]
        assert "mortgagor" in captured["prompt"]
        assert "mortgagee" in captured["prompt"]
        assert '"borrower"' not in captured["prompt"]
        assert '"lender"' not in captured["prompt"]

    def test_extract_from_ocr_text_strips_stale_easement_prompt_keys(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService()
        captured: dict[str, Any] = {}

        def _fake_analyze_text(
            prompt: str,
            *,
            max_tokens: int,
            response_format: dict[str, Any] | None = None,
        ) -> str:
            captured["prompt"] = prompt
            captured["response_format"] = response_format
            return json.dumps(
                {
                    "instrument_number": "2002449822",
                    "recording_book": None,
                    "recording_page": None,
                    "recording_date": "2002-01-02",
                    "execution_date": "2001-12-20",
                    "property_address": "123 Main St, Tampa, FL 33601",
                    "legal_description": "LOT 1, BLOCK 2, TEST SUBDIVISION",
                    "parcel_id": None,
                    "confidence_score": 0.85,
                    "unclear_sections": [],
                    "deed_type": "OTHER",
                    "grantor": "A",
                    "grantee": "B",
                    "consideration": None,
                    "documentary_stamps": None,
                    "assumed_encumbrances": None,
                    "assumed_encumbrance_refs": [],
                    "related_case_number": None,
                }
            )

        monkeypatch.setattr(svc.vision, "analyze_text", _fake_analyze_text)

        result = svc._extract_from_ocr_text("--- PAGE 1 ---\nEasement text", "easement")  # noqa: SLF001

        assert result is not None
        assert "grantor" in captured["prompt"]
        assert "grantee" in captured["prompt"]
        assert "deed_type" in captured["prompt"]
        assert '"party_1"' not in captured["prompt"]
        assert '"party_2"' not in captured["prompt"]

    def test_process_one_valid_cache_skips_vision_call(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
            _cache_path_for,
        )

        svc = PgEncumbranceExtractionService()
        pdf_path = tmp_path / "mortgage_2024-0012345.pdf"
        pdf_path.write_bytes(b"%PDF-1.4\n")
        cache_path = _cache_path_for(pdf_path)
        cache_path.write_text(
            json.dumps(
                {
                    "instrument_number": "2024-0012345",
                    "recording_book": None,
                    "recording_page": None,
                    "recording_date": "2024-01-15",
                    "execution_date": "2024-01-10",
                    "property_address": "123 Main St, Tampa, FL 33601",
                    "legal_description": "LOT 5, BLOCK 3, TAMPA PALMS UNIT 1",
                    "parcel_id": "1929084000",
                    "confidence_score": 0.9,
                    "unclear_sections": [],
                    "mortgage_type": "MTG",
                    "mortgagor": "JOHN SMITH",
                    "mortgagee": "WELLS FARGO BANK",
                    "principal_amount": 250000.0,
                    "interest_rate": 6.5,
                    "maturity_date": "2054-01-15",
                    "is_adjustable_rate": False,
                    "mers_min": None,
                    "is_mers_nominee": False,
                    "association_name": None,
                    "has_pud_rider": False,
                    "has_condo_rider": False,
                }
            ),
            encoding="utf-8",
        )

        monkeypatch.setattr(
            PgEncumbranceExtractionService,
            "_pdf_path_for",
            staticmethod(lambda _row: pdf_path),
        )

        def _fail_analyze_text(*_args: Any, **_kwargs: Any) -> str:
            raise AssertionError("vision should not run on cache hit")

        monkeypatch.setattr(
            svc.vision,
            "analyze_text",
            _fail_analyze_text,
        )

        saved: dict[str, Any] = {}

        def _fake_save(encumbrance_id: int, data: dict[str, Any]) -> None:
            saved["id"] = encumbrance_id
            saved["data"] = data

        monkeypatch.setattr(svc, "_save_to_pg", _fake_save)

        # Mock _address_resolves so cache path doesn't hit real DB
        monkeypatch.setattr(svc, "_address_resolves", lambda _addr: True)

        row = {
            "id": 99,
            "encumbrance_type": "mortgage",
            "instrument_number": "2024-0012345",
            "raw_document_type": "(MTG) MORTGAGE",
            "case_number": "TESTCASE",
        }

        result = asyncio.run(svc._process_one(cast("Page", None), row))  # noqa: SLF001

        assert result is not None
        assert result["_status"] == "cached"
        assert saved["id"] == 99
        assert saved["data"]["mortgagee"] == "WELLS FARGO BANK"

    def test_render_pages_renders_entire_document(self) -> None:
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            pdf_path = Path(tmp.name)

        doc = fitz.open()
        try:
            for idx in range(4):
                page = doc.new_page()
                page.insert_text((72, 72), f"Page {idx + 1}")
            doc.save(str(pdf_path))
        finally:
            doc.close()

        image_paths = PgEncumbranceExtractionService._render_pages(pdf_path)  # noqa: SLF001
        try:
            assert len(image_paths) == 4
            for image_path in image_paths:
                assert Path(image_path).exists()
        finally:
            for image_path in image_paths:
                Path(image_path).unlink(missing_ok=True)
            pdf_path.unlink(missing_ok=True)

    def test_tally_result_counts_errors_separately(self) -> None:
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        stats = {"extracted": 0, "cached": 0, "errors": 0, "skipped": 0}

        PgEncumbranceExtractionService._tally_result(stats, {"_status": "error"})  # noqa: SLF001
        PgEncumbranceExtractionService._tally_result(stats, {"_status": "cached"})  # noqa: SLF001
        PgEncumbranceExtractionService._tally_result(stats, {"_status": "extracted"})  # noqa: SLF001
        PgEncumbranceExtractionService._tally_result(stats, None)  # noqa: SLF001

        assert stats == {"extracted": 1, "cached": 1, "errors": 1, "skipped": 1}


@pytest.fixture
def mock_engine():
    return MagicMock()


class TestAddressResolves:
    def test_address_resolves_returns_false_for_non_hillsborough(self, mock_engine):
        """Out-of-county addresses should not resolve."""
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        svc.engine = mock_engine

        assert svc._address_resolves("951 Yamato Road, Suite 175, Boca Raton, FL 33431") is False  # noqa: SLF001

    def test_address_resolves_returns_true_for_matching_hcpa(self, mock_engine):
        """Known HCPA address should resolve."""
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (1,)
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        svc.engine = mock_engine

        assert svc._address_resolves("1202 E 15TH AVE, TAMPA, FL 33605") is True  # noqa: SLF001

    def test_address_resolves_returns_false_for_null(self):
        """Null and empty addresses should not resolve."""
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
        assert svc._address_resolves(None) is False  # noqa: SLF001
        assert svc._address_resolves("") is False  # noqa: SLF001


class TestRepairErrorDescription:
    def test_repair_error_description_detects_non_hillsborough_zip(self):
        """Non-HC zip code should be flagged in the error description."""
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
        desc = svc._build_repair_error_description("951 Yamato Road, Boca Raton, FL 33431")  # noqa: SLF001
        assert "33431" in desc
        assert "not in Hillsborough County" in desc

    def test_repair_error_description_handles_none(self):
        """Null address should produce a 'No property address' error."""
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
        desc = svc._build_repair_error_description(None)  # noqa: SLF001
        assert "No property address" in desc

    def test_repair_error_description_handles_hcpa_mismatch(self):
        """HC zip but no HCPA match should mention OCR errors."""
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
        desc = svc._build_repair_error_description("999 FAKE ST, TAMPA, FL 33601")  # noqa: SLF001
        assert "does not match any known parcel" in desc


class TestAttemptRepair:
    def test_attempt_repair_returns_repaired_data_on_success(self):
        """When vision returns valid corrected JSON, _attempt_repair returns it."""
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
        mock_vision = MagicMock()
        # Return a valid mortgage JSON from the repair prompt
        mock_vision.analyze_text.return_value = json.dumps({
            "instrument_number": "2024-0012345",
            "recording_book": None,
            "recording_page": None,
            "recording_date": "2024-01-15",
            "execution_date": "2024-01-10",
            "property_address": "1202 E 15TH AVE, TAMPA, FL 33605",
            "legal_description": "LOT 5, BLOCK 3, TAMPA PALMS UNIT 1",
            "parcel_id": "1929084000",
            "confidence_score": 0.9,
            "unclear_sections": [],
            "mortgage_type": "MTG",
            "mortgagor": "JOHN SMITH",
            "mortgagee": "WELLS FARGO BANK",
            "principal_amount": 250000.0,
            "interest_rate": 6.5,
            "maturity_date": "2054-01-15",
            "is_adjustable_rate": False,
            "mers_min": None,
            "is_mers_nominee": False,
            "association_name": None,
            "has_pud_rider": False,
            "has_condo_rider": False,
        })
        svc.vision = mock_vision

        original = {
            "instrument_number": "2024-0012345",
            "property_address": "951 Yamato Road, Boca Raton, FL 33431",
            "mortgagor": "JOHN SMITH",
            "mortgagee": "WELLS FARGO BANK",
        }

        result = svc._attempt_repair("--- PAGE 1 ---\nMortgage text", original, "mortgage")  # noqa: SLF001

        assert result is not None
        assert result["property_address"] == "1202 E 15TH AVE, TAMPA, FL 33605"
        mock_vision.analyze_text.assert_called_once()
        # Verify the repair prompt contains expected content
        call_args = mock_vision.analyze_text.call_args
        prompt = call_args[0][0]
        assert "951 Yamato Road" in prompt
        assert "not in Hillsborough County" in prompt

    def test_attempt_repair_returns_none_on_empty_vision_response(self):
        """When vision returns nothing, _attempt_repair returns None."""
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
        mock_vision = MagicMock()
        mock_vision.analyze_text.return_value = None
        svc.vision = mock_vision

        result = svc._attempt_repair(  # noqa: SLF001
            "--- PAGE 1 ---\nMortgage text",
            {"property_address": "bad address"},
            "mortgage",
        )

        assert result is None

    def test_attempt_repair_returns_none_on_invalid_json(self):
        """When vision returns garbage, _attempt_repair returns None."""
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
        mock_vision = MagicMock()
        mock_vision.analyze_text.return_value = "not valid json at all"
        svc.vision = mock_vision

        result = svc._attempt_repair(  # noqa: SLF001
            "--- PAGE 1 ---\nMortgage text",
            {"property_address": "bad address"},
            "mortgage",
        )

        assert result is None


class TestRepairIntegration:
    """End-to-end repair wiring: _process_one triggers repair when address doesn't resolve."""

    def _make_mortgage_payload(self, address: str) -> dict[str, Any]:
        return {
            "instrument_number": "2024-0012345",
            "recording_book": None,
            "recording_page": None,
            "recording_date": "2024-01-15",
            "execution_date": "2024-01-10",
            "property_address": address,
            "legal_description": "LOT 5, BLOCK 3, TAMPA PALMS UNIT 1",
            "parcel_id": "1929084000",
            "confidence_score": 0.9,
            "unclear_sections": [],
            "mortgage_type": "MTG",
            "mortgagor": "JOHN SMITH",
            "mortgagee": "WELLS FARGO BANK",
            "principal_amount": 250000.0,
            "interest_rate": 6.5,
            "maturity_date": "2054-01-15",
            "is_adjustable_rate": False,
            "mers_min": None,
            "is_mers_nominee": False,
            "association_name": None,
            "has_pud_rider": False,
            "has_condo_rider": False,
        }

    def test_process_one_triggers_repair_and_uses_repaired_address(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """When initial extraction has bad address but repair fixes it, the
        repaired data is what gets cached and saved."""
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
            _cache_path_for,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)

        # Create a minimal valid PDF
        import fitz as _fitz

        pdf_path = tmp_path / "mortgage_2024-0012345.pdf"
        doc = _fitz.open()
        pg = doc.new_page()
        pg.insert_text((72, 72), "MORTGAGE DOCUMENT TEXT HERE")
        doc.save(str(pdf_path))
        doc.close()

        monkeypatch.setattr(
            PgEncumbranceExtractionService,
            "_pdf_path_for",
            staticmethod(lambda _row: pdf_path),
        )

        # Mock _save_raw_to_pg to be a no-op
        monkeypatch.setattr(svc, "_save_raw_to_pg", lambda _id, _text: None)

        # Track what gets saved
        saved: dict[str, Any] = {}

        def _fake_save(encumbrance_id: int, data: dict[str, Any]) -> None:
            saved["id"] = encumbrance_id
            saved["data"] = data

        monkeypatch.setattr(svc, "_save_to_pg", _fake_save)

        # Vision call counter: first call returns bad address, second (repair) returns good
        bad_address = "951 Yamato Road, Boca Raton, FL 33431"
        good_address = "1202 E 15TH AVE, TAMPA, FL 33605"
        call_count = {"n": 0}

        def _fake_analyze_text(
            prompt: str,
            *,
            max_tokens: int = 4000,
            response_format: dict[str, Any] | None = None,
        ) -> str:
            call_count["n"] += 1
            if call_count["n"] == 1:
                return json.dumps(self._make_mortgage_payload(bad_address))
            return json.dumps(self._make_mortgage_payload(good_address))

        mock_vision = MagicMock()
        mock_vision.analyze_text = _fake_analyze_text
        svc.vision = mock_vision

        # Mock _address_resolves: only good_address resolves
        def _fake_address_resolves(addr: str | None) -> bool:
            if not addr:
                return False
            return "1202 E 15TH AVE" in addr.upper()

        monkeypatch.setattr(svc, "_address_resolves", _fake_address_resolves)

        # Mock engine so it doesn't hit a real DB
        svc.engine = MagicMock()

        row = {
            "id": 140408,
            "encumbrance_type": "mortgage",
            "instrument_number": "2024-0012345",
            "raw_document_type": "(MTG) MORTGAGE",
            "case_number": "TESTCASE",
            "book": None,
            "page": None,
            "recording_date": "2024-01-15",
            "party1": "JOHN SMITH",
            "party2": "WELLS FARGO BANK",
            "strap": "1929084000",
            "folio": None,
            "ori_id": "test-ori-id",
            "ori_uuid": None,
        }

        result = asyncio.run(svc._process_one(cast("Page", None), row))  # noqa: SLF001

        assert result is not None
        assert result["_status"] == "extracted"
        # The repaired address should be what was saved
        assert saved["data"]["property_address"] == good_address
        assert result["property_address"] == good_address
        # Vision was called twice: initial extraction + repair
        assert call_count["n"] == 2
        # Cache should have the repaired address
        cache_path = _cache_path_for(pdf_path)
        assert cache_path.exists()
        cached = json.loads(cache_path.read_text())
        assert cached["property_address"] == good_address

    def test_process_one_keeps_original_when_repair_fails(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """When repair doesn't improve the address, original extraction is kept."""
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)

        import fitz as _fitz

        pdf_path = tmp_path / "mortgage_2024-0099999.pdf"
        doc = _fitz.open()
        pg = doc.new_page()
        pg.insert_text((72, 72), "MORTGAGE DOCUMENT TEXT HERE")
        doc.save(str(pdf_path))
        doc.close()

        monkeypatch.setattr(
            PgEncumbranceExtractionService,
            "_pdf_path_for",
            staticmethod(lambda _row: pdf_path),
        )
        monkeypatch.setattr(svc, "_save_raw_to_pg", lambda _id, _text: None)

        saved: dict[str, Any] = {}

        def _fake_save(encumbrance_id: int, data: dict[str, Any]) -> None:
            saved["id"] = encumbrance_id
            saved["data"] = data

        monkeypatch.setattr(svc, "_save_to_pg", _fake_save)

        bad_address = "951 Yamato Road, Boca Raton, FL 33431"

        def _fake_analyze_text(
            prompt: str,
            *,
            max_tokens: int = 4000,
            response_format: dict[str, Any] | None = None,
        ) -> str:
            # Both initial and repair return the same bad address
            return json.dumps(self._make_mortgage_payload(bad_address))

        mock_vision = MagicMock()
        mock_vision.analyze_text = _fake_analyze_text
        svc.vision = mock_vision

        # Address never resolves
        monkeypatch.setattr(svc, "_address_resolves", lambda _addr: False)

        svc.engine = MagicMock()

        row = {
            "id": 138560,
            "encumbrance_type": "mortgage",
            "instrument_number": "2024-0099999",
            "raw_document_type": "(MTG) MORTGAGE",
            "case_number": "TESTCASE2",
            "book": None,
            "page": None,
            "recording_date": "2024-01-15",
            "party1": "JANE DOE",
            "party2": "CHASE BANK",
            "strap": "9999999999",
            "folio": None,
            "ori_id": "test-ori-id-2",
            "ori_uuid": None,
        }

        result = asyncio.run(svc._process_one(cast("Page", None), row))  # noqa: SLF001

        assert result is not None
        assert result["_status"] == "extracted"
        # Original (bad) address is kept since repair didn't improve it
        assert saved["data"]["property_address"] == bad_address

    def test_process_one_persists_raw_ocr_before_llm_failure(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        class _CaptureConn:
            def __init__(self) -> None:
                self.calls: list[tuple[str, dict[str, Any]]] = []

            def execute(self, sql: Any, params: dict[str, Any]) -> None:
                self.calls.append((str(sql), params))

        class _BeginCtx:
            def __init__(self, conn: _CaptureConn) -> None:
                self._conn = conn

            def __enter__(self) -> _CaptureConn:
                return self._conn

            def __exit__(
                self,
                _exc_type: object,
                _exc: object,
                _tb: object,
            ) -> bool:
                return False

        class _CaptureEngine:
            def __init__(self, conn: _CaptureConn) -> None:
                self._conn = conn

            def begin(self) -> _BeginCtx:
                return _BeginCtx(self._conn)

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
        capture_conn = _CaptureConn()
        svc.engine = _CaptureEngine(capture_conn)

        pdf_path = tmp_path / "mortgage_2024-0012345.pdf"
        pdf_path.write_bytes(b"%PDF-1.4\n")

        monkeypatch.setattr(
            PgEncumbranceExtractionService,
            "_pdf_path_for",
            staticmethod(lambda _row: pdf_path),
        )

        async def _fake_download(_page: Any, _row: dict[str, Any]) -> Path:
            return pdf_path

        monkeypatch.setattr(svc, "_download_pdf", _fake_download)
        monkeypatch.setattr(svc, "_render_pages", lambda _pdf: [tmp_path / "page-1.png"])
        monkeypatch.setattr(
            svc,
            "_ocr_images_to_text",
            lambda _images: ("--- PAGE 1 ---\nRAW OCR TEXT", []),
        )

        def _fake_extract(ocr_text: str, enc_type: str) -> None:
            assert capture_conn.calls, "raw OCR must be persisted before LLM extraction"
            assert ocr_text == "--- PAGE 1 ---\nRAW OCR TEXT"
            assert enc_type == "mortgage"

        monkeypatch.setattr(svc, "_extract_from_ocr_text", _fake_extract)

        row = {
            "id": 140409,
            "encumbrance_type": "mortgage",
            "instrument_number": "2024-0012345",
            "raw_document_type": "(MTG) MORTGAGE",
            "case_number": "TESTCASE",
        }

        result = asyncio.run(svc._process_one(cast("Page", None), row))  # noqa: SLF001

        assert result == {
            "_status": "error",
            "_reason": "llm_no_structured_output",
        }
        assert len(capture_conn.calls) == 1
        sql, params = capture_conn.calls[0]
        assert "UPDATE ori_encumbrances" in sql
        assert "SET raw = :ocr_text" in sql
        assert params == {
            "ocr_text": "--- PAGE 1 ---\nRAW OCR TEXT",
            "id": 140409,
        }


class TestRemapLegacyKeys:
    """Tests for _remap_legacy_keys — converting old cache field names to current schema."""

    def test_mortgage_remap_borrower_lender(self):
        from src.services.pg_encumbrance_extraction_service import _remap_legacy_keys

        legacy = {
            "borrower": "JOHN SMITH",
            "lender": "WELLS FARGO BANK",
            "book": "12345",
            "page": "678",
            "is_mers": True,
            "confidence": "high",
            "document_type": "MTG",
            "red_flags": ["some flag"],
            "prior_assignments": [{"assignee": "X"}],
            "instrument_number": "2024-0012345",
        }

        result = _remap_legacy_keys(legacy, "mortgage")

        assert result["mortgagor"] == "JOHN SMITH"
        assert result["mortgagee"] == "WELLS FARGO BANK"
        assert result["recording_book"] == "12345"
        assert result["recording_page"] == "678"
        assert result["is_mers_nominee"] is True
        assert result["confidence_score"] == 0.95
        assert "document_type" not in result
        assert "red_flags" not in result
        assert "prior_assignments" not in result
        assert result["instrument_number"] == "2024-0012345"

    def test_mortgage_remap_confidence_medium(self):
        from src.services.pg_encumbrance_extraction_service import _remap_legacy_keys

        legacy = {"confidence": "medium", "instrument_number": "X"}
        result = _remap_legacy_keys(legacy, "mortgage")
        assert result["confidence_score"] == 0.80

    def test_mortgage_remap_confidence_low(self):
        from src.services.pg_encumbrance_extraction_service import _remap_legacy_keys

        legacy = {"confidence": "low", "instrument_number": "X"}
        result = _remap_legacy_keys(legacy, "mortgage")
        assert result["confidence_score"] == 0.60

    def test_mortgage_remap_idempotent_when_already_new_format(self):
        from src.services.pg_encumbrance_extraction_service import _remap_legacy_keys

        new_format = {
            "mortgagor": "JOHN SMITH",
            "mortgagee": "WELLS FARGO BANK",
            "recording_book": "12345",
            "recording_page": "678",
            "is_mers_nominee": True,
            "confidence_score": 0.9,
            "instrument_number": "2024-0012345",
        }

        result = _remap_legacy_keys(new_format, "mortgage")

        assert result == new_format

    def test_assignment_remap_original_mortgage_to_parent_instrument(self):
        from src.services.pg_encumbrance_extraction_service import _remap_legacy_keys

        legacy = {
            "original_mortgage": {
                "book": "15000",
                "page": "200",
                "original_amount": 250000,
                "instrument_number": "2020-001",
            },
            "confidence": "high",
            "red_flags": ["flag"],
            "assignor": "OLD BANK",
            "assignee": "NEW BANK",
        }

        result = _remap_legacy_keys(legacy, "assignment")

        assert result["confidence_score"] == 0.95
        assert "red_flags" not in result
        assert result["parent_instrument"]["recording_book"] == "15000"
        assert result["parent_instrument"]["recording_page"] == "200"
        assert result["parent_instrument"]["instrument_number"] == "2020-001"
        assert "original_amount" not in result["parent_instrument"]
        assert result["assignor"] == "OLD BANK"
        assert result["assignee"] == "NEW BANK"

    def test_assignment_remap_idempotent_parent_instrument(self):
        from src.services.pg_encumbrance_extraction_service import _remap_legacy_keys

        new_format = {
            "parent_instrument": {"recording_book": "15000", "recording_page": "200"},
            "confidence_score": 0.9,
            "assignor": "OLD BANK",
            "assignee": "NEW BANK",
        }

        result = _remap_legacy_keys(new_format, "assignment")

        assert result["parent_instrument"] == {"recording_book": "15000", "recording_page": "200"}
        assert result["confidence_score"] == 0.9

    def test_lien_remap_creditor_debtor(self):
        from src.services.pg_encumbrance_extraction_service import _remap_legacy_keys

        legacy = {
            "creditor": "CITY OF TAMPA",
            "debtor": "JOHN DOE",
            "amount": 5000.0,
            "document_type": "CEL",
            "confidence": "medium",
            "instrument_number": "2023-999",
        }

        result = _remap_legacy_keys(legacy, "lien")

        assert result["lienor"] == "CITY OF TAMPA"
        assert result["lienee"] == "JOHN DOE"
        assert result["lien_amount"] == 5000.0
        assert result["lien_type"] == "CEL"
        assert result["confidence_score"] == 0.80
        assert result["instrument_number"] == "2023-999"

    def test_no_remap_for_unknown_type(self):
        from src.services.pg_encumbrance_extraction_service import _remap_legacy_keys

        payload = {"grantor": "A", "grantee": "B"}
        result = _remap_legacy_keys(payload, "easement")

        assert result is payload  # exact same object, no copy

    def test_no_remap_for_satisfaction(self):
        from src.services.pg_encumbrance_extraction_service import _remap_legacy_keys

        payload = {"releasor": "X", "releasee": "Y"}
        result = _remap_legacy_keys(payload, "satisfaction")

        assert result is payload


class TestNormalizeCaseNumber:
    """Tests for _normalize_case_number — ORI clerk format to standard."""

    def test_standard_ca_case(self):
        from src.services.pg_encumbrance_extraction_service import _normalize_case_number

        assert _normalize_case_number("292025CA006599A001HC") == "25-CA-006599"

    def test_cc_case(self):
        from src.services.pg_encumbrance_extraction_service import _normalize_case_number

        assert _normalize_case_number("292024CC012345A001HC") == "24-CC-012345"

    def test_returns_none_for_short_input(self):
        from src.services.pg_encumbrance_extraction_service import _normalize_case_number

        assert _normalize_case_number("29") is None
        assert _normalize_case_number("") is None

    def test_returns_none_for_non_matching(self):
        from src.services.pg_encumbrance_extraction_service import _normalize_case_number

        assert _normalize_case_number("ABCDEF1234567890") is None

    def test_returns_none_for_none(self):
        from src.services.pg_encumbrance_extraction_service import _normalize_case_number

        assert _normalize_case_number("") is None


class TestEnrichFromMetadata:
    """Tests for _enrich_from_metadata — backfilling from HCPA and ORI metadata."""

    def test_enriches_property_address_from_hcpa(self, mock_engine):
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = ("1202 E 15TH AVE",)
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        svc.engine = mock_engine

        validated: dict[str, Any] = {"property_address": None, "parcel_id": "1929084000"}
        row: dict[str, Any] = {"id": 100, "strap": "1929084NUB00000000040A"}

        result = svc._enrich_from_metadata(validated, "mortgage", row)  # noqa: SLF001

        assert result["property_address"] == "1202 E 15TH AVE"

    def test_does_not_overwrite_existing_address(self, mock_engine):
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
        svc.engine = mock_engine

        validated: dict[str, Any] = {"property_address": "EXISTING ADDRESS"}
        row: dict[str, Any] = {"id": 101, "strap": "1929084NUB00000000040A"}

        result = svc._enrich_from_metadata(validated, "mortgage", row)  # noqa: SLF001

        assert result["property_address"] == "EXISTING ADDRESS"
        # Should NOT have queried the DB since address already present
        mock_engine.connect.assert_not_called()

    def test_no_enrichment_when_no_strap(self, mock_engine):
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
        svc.engine = mock_engine

        validated: dict[str, Any] = {"property_address": None}
        row: dict[str, Any] = {"id": 102}

        result = svc._enrich_from_metadata(validated, "mortgage", row)  # noqa: SLF001

        assert result["property_address"] is None
        mock_engine.connect.assert_not_called()

    def test_enriches_civil_case_number_for_lis_pendens(self):
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
        svc.engine = MagicMock()

        validated: dict[str, Any] = {
            "property_address": "123 MAIN ST",
            "civil_case_number": None,
        }
        row: dict[str, Any] = {
            "id": 103,
            "case_number": "292025CA006599A001HC",
        }

        result = svc._enrich_from_metadata(validated, "lis_pendens", row)  # noqa: SLF001

        assert result["civil_case_number"] == "25-CA-006599"

    def test_does_not_overwrite_existing_case_number(self):
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
        svc.engine = MagicMock()

        validated: dict[str, Any] = {
            "property_address": "123 MAIN ST",
            "civil_case_number": "25-CA-000001",
        }
        row: dict[str, Any] = {
            "id": 104,
            "case_number": "292025CA006599A001HC",
        }

        result = svc._enrich_from_metadata(validated, "lis_pendens", row)  # noqa: SLF001

        assert result["civil_case_number"] == "25-CA-000001"

    def test_no_case_enrichment_for_non_lis_pendens(self):
        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
        svc.engine = MagicMock()

        validated: dict[str, Any] = {"property_address": "123 MAIN ST"}
        row: dict[str, Any] = {
            "id": 105,
            "case_number": "292025CA006599A001HC",
        }

        result = svc._enrich_from_metadata(validated, "mortgage", row)  # noqa: SLF001

        assert "civil_case_number" not in result
