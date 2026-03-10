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
        vision_method, model_cls = EXTRACTION_DISPATCH[enc_type]
        assert model_cls is expected_model
        assert isinstance(vision_method, str)

    def test_all_dispatch_vision_methods_exist_on_vision_service(self):
        from src.services.pg_encumbrance_extraction_service import EXTRACTION_DISPATCH
        from src.services.vision_service import VisionService
        for enc_type, (method_name, _) in EXTRACTION_DISPATCH.items():
            assert hasattr(VisionService, method_name), (
                f"VisionService missing {method_name} for {enc_type}"
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
