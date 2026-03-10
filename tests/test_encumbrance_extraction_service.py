"""Tests for pg_encumbrance_extraction_service dispatch and cache logic."""

import json
import tempfile
from pathlib import Path

import pytest

from src.models.mortgage_extraction import MortgageExtraction
from src.models.deed_extraction import DeedExtraction
from src.models.lien_extraction import LienExtraction
from src.models.lis_pendens_extraction import LisPendensExtraction
from src.models.satisfaction_extraction import SatisfactionExtraction
from src.models.assignment_extraction import AssignmentExtraction
from src.models.noc_extraction import NOCExtraction


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


class TestEndToEnd:
    """Integration: query -> cache miss -> extract -> validate -> save."""

    def test_run_with_no_unextracted_returns_zeros(self):
        """When DB has no unextracted rows, run() returns all-zero stats."""
        from unittest.mock import patch

        from src.services.pg_encumbrance_extraction_service import (
            PgEncumbranceExtractionService,
        )

        svc = PgEncumbranceExtractionService()
        with patch.object(svc, "_find_unextracted", return_value=[]):
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

        result = PgEncumbranceExtractionService._validate(data, "mortgage")  # noqa: SLF001
        assert result is not None
        assert result["principal_amount"] == 250000.0
        assert result["mortgagee"] == "WELLS FARGO BANK"
        assert result["mortgagor"] == "JOHN SMITH"
