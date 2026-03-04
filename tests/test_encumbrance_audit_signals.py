"""Tests for LP-to-judgment delta signal extraction.

Tests the pure extraction functions in ``src.services.encumbrance_audit_signals``
as well as the ``AuditSignalExtractor`` class with mocked PG access.  No real
database connection is required.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Self

from src.services.encumbrance_audit_signals import (
    AuditSignal,
    AuditSignalExtractor,
    extract_judgment_instrument_gap,
    extract_judgment_joined_party_gap,
    extract_long_case_interim_risk,
    extract_lp_to_judgment_party_expansion,
    extract_lp_to_judgment_plaintiff_change,
    extract_lp_to_judgment_property_change,
    names_match,
    normalize_name,
)


# -----------------------------------------------------------------------
# Name normalisation helpers
# -----------------------------------------------------------------------

class TestNormalizeName:
    def test_strips_punctuation_and_uppercases(self) -> None:
        assert normalize_name("Wells Fargo, N.A.") == "WELLS FARGO N A"

    def test_collapses_whitespace(self) -> None:
        assert normalize_name("  JOHN    DOE  ") == "JOHN DOE"

    def test_empty(self) -> None:
        assert normalize_name("") == ""


class TestNamesMatch:
    def test_exact_match(self) -> None:
        assert names_match("WELLS FARGO BANK NA", "WELLS FARGO BANK NA") is True

    def test_suffix_variation(self) -> None:
        # LLC vs Inc is stripped as noise; core tokens still overlap
        assert names_match(
            "NATIONSTAR MORTGAGE LLC",
            "NATIONSTAR MORTGAGE INC",
        ) is True

    def test_completely_different(self) -> None:
        assert names_match("WELLS FARGO BANK NA", "JAMES SMITH") is False

    def test_empty_strings(self) -> None:
        assert names_match("", "WELLS FARGO") is False
        assert names_match("WELLS FARGO", "") is False
        assert names_match("", "") is False

    def test_partial_overlap_accepted(self) -> None:
        # "BANK" + "AMERICA" overlap out of "BANK OF AMERICA NA" tokens
        assert names_match(
            "BANK OF AMERICA NA",
            "BANK OF AMERICA NATIONAL ASSOCIATION",
        ) is True

    def test_trustee_variation(self) -> None:
        assert names_match(
            "US BANK NATIONAL ASSOCIATION AS TRUSTEE",
            "US BANK NA TRUSTEE FOR XYZ TRUST",
        ) is True


# -----------------------------------------------------------------------
# Signal 1: judgment_joined_party_gap
# -----------------------------------------------------------------------

class TestJudgmentJoinedPartyGap:
    def test_no_defendants(self) -> None:
        signals = extract_judgment_joined_party_gap(
            foreclosure_id=1,
            judgment_data={},
            encumbrance_parties=["BANK OF AMERICA"],
        )
        assert signals == []

    def test_borrower_skipped(self) -> None:
        """Borrower/co-borrower/spouse defendants should not trigger a gap."""
        jd = {
            "defendants": [
                {"name": "JOHN DOE", "party_type": "borrower"},
                {"name": "JANE DOE", "party_type": "co_borrower"},
            ],
        }
        signals = extract_judgment_joined_party_gap(1, jd, ["BANK OF AMERICA"])
        assert signals == []

    def test_unknown_tenant_skipped(self) -> None:
        jd = {"defendants": [{"name": "UNKNOWN TENANT", "party_type": "tenant"}]}
        assert extract_judgment_joined_party_gap(1, jd, []) == []

    def test_lienholder_not_in_encumbrances(self) -> None:
        jd = {
            "defendants": [
                {
                    "name": "CAPITAL ONE BANK",
                    "party_type": "second_mortgage_holder",
                    "is_federal_entity": False,
                },
            ],
        }
        enc_parties = ["WELLS FARGO BANK NA", "JOHN DOE"]
        signals = extract_judgment_joined_party_gap(1, jd, enc_parties)
        assert len(signals) == 1
        assert signals[0].signal_type == "judgment_joined_party_gap"
        assert signals[0].severity == "high"
        assert signals[0].detail["party_name"] == "CAPITAL ONE BANK"

    def test_lienholder_present_in_encumbrances(self) -> None:
        jd = {
            "defendants": [
                {"name": "CAPITAL ONE BANK NA", "party_type": "second_mortgage_holder"},
            ],
        }
        enc_parties = ["CAPITAL ONE BANK, NA"]
        signals = extract_judgment_joined_party_gap(1, jd, enc_parties)
        assert signals == []

    def test_federal_entity_flagged(self) -> None:
        jd = {
            "defendants": [
                {
                    "name": "UNITED STATES OF AMERICA",
                    "party_type": "federal_agency",
                    "is_federal_entity": True,
                },
            ],
        }
        signals = extract_judgment_joined_party_gap(1, jd, [])
        assert len(signals) == 1
        assert signals[0].detail["is_federal"] is True
        assert signals[0].severity == "high"

    def test_hoa_gap(self) -> None:
        jd = {
            "defendants": [
                {
                    "name": "SUNSET LAKES HOMEOWNERS ASSOCIATION",
                    "party_type": "hoa",
                },
            ],
        }
        signals = extract_judgment_joined_party_gap(1, jd, ["WELLS FARGO"])
        assert len(signals) == 1
        assert signals[0].detail["party_type"] == "hoa"


# -----------------------------------------------------------------------
# Signal 2: judgment_instrument_gap
# -----------------------------------------------------------------------

class TestJudgmentInstrumentGap:
    def test_no_instrument_in_judgment(self) -> None:
        jd = {"foreclosed_mortgage": {}}
        signals = extract_judgment_instrument_gap(1, jd, [])
        assert signals == []

    def test_instrument_matched(self) -> None:
        jd = {
            "foreclosed_mortgage": {
                "instrument_number": "2019123456",
                "recording_book": None,
                "recording_page": None,
            },
        }
        enc = [{"instrument_number": "2019123456", "book": None, "page": None}]
        signals = extract_judgment_instrument_gap(1, jd, enc)
        assert signals == []

    def test_instrument_missing_from_encumbrances(self) -> None:
        jd = {
            "foreclosed_mortgage": {
                "instrument_number": "2019123456",
                "recording_book": "12345",
                "recording_page": "678",
            },
        }
        enc = [{"instrument_number": "9999999", "book": "11111", "page": "222"}]
        signals = extract_judgment_instrument_gap(1, jd, enc)
        assert len(signals) == 1
        assert signals[0].signal_type == "judgment_instrument_gap"
        assert signals[0].severity == "high"
        assert signals[0].detail["instrument_number"] == "2019123456"

    def test_book_page_matched(self) -> None:
        jd = {
            "foreclosed_mortgage": {
                "instrument_number": None,
                "recording_book": "12345",
                "recording_page": "678",
            },
        }
        enc = [{"instrument_number": None, "book": "12345", "page": "678"}]
        signals = extract_judgment_instrument_gap(1, jd, enc)
        assert signals == []

    def test_lis_pendens_instrument_gap(self) -> None:
        jd = {
            "lis_pendens": {
                "instrument_number": "2020555555",
                "recording_book": None,
                "recording_page": None,
            },
        }
        enc = [{"instrument_number": "9999999", "book": None, "page": None}]
        signals = extract_judgment_instrument_gap(1, jd, enc)
        assert len(signals) == 1
        assert signals[0].severity == "medium"
        assert signals[0].detail["reference_source"] == "lis_pendens_recording"


# -----------------------------------------------------------------------
# Signal 3: lp_to_judgment_plaintiff_change
# -----------------------------------------------------------------------

class TestLpToJudgmentPlaintiffChange:
    def test_same_plaintiff(self) -> None:
        jd = {"plaintiff": "WELLS FARGO BANK NA"}
        signals = extract_lp_to_judgment_plaintiff_change(1, jd, "WELLS FARGO BANK NA")
        assert signals == []

    def test_different_plaintiff(self) -> None:
        jd = {"plaintiff": "NATIONSTAR MORTGAGE LLC"}
        signals = extract_lp_to_judgment_plaintiff_change(1, jd, "WELLS FARGO BANK NA")
        assert len(signals) == 1
        assert signals[0].signal_type == "lp_to_judgment_plaintiff_change"
        assert signals[0].severity == "high"
        assert signals[0].detail["lp_plaintiff"] == "WELLS FARGO BANK NA"
        assert signals[0].detail["judgment_plaintiff"] == "NATIONSTAR MORTGAGE LLC"

    def test_missing_lp_plaintiff(self) -> None:
        jd = {"plaintiff": "WELLS FARGO"}
        signals = extract_lp_to_judgment_plaintiff_change(1, jd, None)
        assert signals == []

    def test_missing_judgment_plaintiff(self) -> None:
        signals = extract_lp_to_judgment_plaintiff_change(1, {}, "WELLS FARGO")
        assert signals == []

    def test_close_name_variants(self) -> None:
        """Suffix differences should not trigger a change signal."""
        jd = {"plaintiff": "US BANK NATIONAL ASSOCIATION"}
        signals = extract_lp_to_judgment_plaintiff_change(
            1, jd, "US BANK NA AS TRUSTEE",
        )
        assert signals == []


# -----------------------------------------------------------------------
# Signal 4: lp_to_judgment_party_expansion
# -----------------------------------------------------------------------

class TestLpToJudgmentPartyExpansion:
    def test_no_new_parties(self) -> None:
        jd = {"defendants": [{"name": "JOHN DOE", "party_type": "borrower"}]}
        signals = extract_lp_to_judgment_party_expansion(1, jd, ["JOHN DOE"])
        assert signals == []

    def test_new_party_detected(self) -> None:
        jd = {
            "defendants": [
                {"name": "JOHN DOE", "party_type": "borrower"},
                {"name": "CAPITAL ONE BANK", "party_type": "judgment_creditor"},
            ],
        }
        signals = extract_lp_to_judgment_party_expansion(1, jd, ["JOHN DOE", "WELLS FARGO"])
        assert len(signals) == 1
        assert signals[0].signal_type == "lp_to_judgment_party_expansion"
        d = signals[0].detail
        assert d["new_party_count"] == 1
        assert d["new_parties"][0]["name"] == "CAPITAL ONE BANK"

    def test_generic_party_ignored(self) -> None:
        jd = {"defendants": [{"name": "UNKNOWN TENANT", "party_type": "tenant"}]}
        signals = extract_lp_to_judgment_party_expansion(1, jd, ["JOHN DOE"])
        assert signals == []

    def test_empty_lp_parties(self) -> None:
        jd = {"defendants": [{"name": "CAPITAL ONE", "party_type": "judgment_creditor"}]}
        signals = extract_lp_to_judgment_party_expansion(1, jd, [])
        assert signals == []

    def test_severity_escalation_for_federal(self) -> None:
        jd = {
            "defendants": [
                {"name": "INTERNAL REVENUE SERVICE", "party_type": "irs"},
            ],
        }
        signals = extract_lp_to_judgment_party_expansion(1, jd, ["JOHN DOE"])
        assert len(signals) == 1
        assert signals[0].severity == "high"


# -----------------------------------------------------------------------
# Signal 5: lp_to_judgment_property_change
# -----------------------------------------------------------------------

class TestLpToJudgmentPropertyChange:
    def test_same_legal(self) -> None:
        jd = {"legal_description": "LOT 5 BLOCK 2 SUNSET LAKES UNIT 3"}
        signals = extract_lp_to_judgment_property_change(
            1, jd,
            lp_legal_description="LOT 5 BLOCK 2 SUNSET LAKES UNIT 3",
            lp_property_address=None,
        )
        assert signals == []

    def test_materially_different_legal(self) -> None:
        jd = {"legal_description": "LOT 99 BLOCK 7 PALM HARBOR ESTATES"}
        signals = extract_lp_to_judgment_property_change(
            1, jd,
            lp_legal_description="LOT 5 BLOCK 2 SUNSET LAKES UNIT 3",
            lp_property_address=None,
        )
        assert len(signals) == 1
        assert signals[0].signal_type == "lp_to_judgment_property_change"
        changes = signals[0].detail["changes"]
        assert any(c["field"] == "legal_description" for c in changes)

    def test_address_change(self) -> None:
        jd = {
            "legal_description": "",
            "property_address": "5678 OAK STREET",
        }
        signals = extract_lp_to_judgment_property_change(
            1, jd,
            lp_legal_description=None,
            lp_property_address="1234 PINE AVENUE",
        )
        assert len(signals) == 1
        changes = signals[0].detail["changes"]
        assert any(c["field"] == "property_address" for c in changes)

    def test_missing_data_no_signal(self) -> None:
        signals = extract_lp_to_judgment_property_change(
            1, {},
            lp_legal_description=None,
            lp_property_address=None,
        )
        assert signals == []


# -----------------------------------------------------------------------
# Signal 6: long_case_interim_risk
# -----------------------------------------------------------------------

class TestLongCaseInterimRisk:
    def test_short_gap_no_signal(self) -> None:
        signals = extract_long_case_interim_risk(
            1,
            lp_filing_date=date(2023, 1, 1),
            judgment_date=date(2024, 6, 1),
            lifecycle_encumbrance_count=0,
        )
        assert signals == []

    def test_long_gap_triggers_signal(self) -> None:
        signals = extract_long_case_interim_risk(
            1,
            lp_filing_date=date(2018, 1, 1),
            judgment_date=date(2025, 6, 1),
            lifecycle_encumbrance_count=0,
        )
        assert len(signals) == 1
        assert signals[0].signal_type == "long_case_interim_risk"
        assert signals[0].severity == "high"
        assert signals[0].detail["gap_years"] >= 5.0

    def test_lifecycle_evidence_suppresses_signal(self) -> None:
        signals = extract_long_case_interim_risk(
            1,
            lp_filing_date=date(2018, 1, 1),
            judgment_date=date(2025, 6, 1),
            lifecycle_encumbrance_count=2,
        )
        assert signals == []

    def test_medium_gap(self) -> None:
        signals = extract_long_case_interim_risk(
            1,
            lp_filing_date=date(2020, 1, 1),
            judgment_date=date(2024, 6, 1),
            lifecycle_encumbrance_count=0,
            threshold_years=3,
        )
        assert len(signals) == 1
        assert signals[0].severity == "medium"

    def test_missing_dates(self) -> None:
        signals = extract_long_case_interim_risk(1, None, None, 0)
        assert signals == []

    def test_string_dates(self) -> None:
        signals = extract_long_case_interim_risk(
            1,
            lp_filing_date="2017-03-15",
            judgment_date="2025-01-10",
            lifecycle_encumbrance_count=0,
        )
        assert len(signals) == 1
        assert signals[0].detail["gap_years"] >= 7.0


# -----------------------------------------------------------------------
# AuditSignal dataclass
# -----------------------------------------------------------------------

class TestAuditSignal:
    def test_to_dict(self) -> None:
        s = AuditSignal(
            foreclosure_id=42,
            signal_type="judgment_joined_party_gap",
            severity="high",
            detail={"party_name": "CAPITAL ONE"},
        )
        d = s.to_dict()
        assert d["foreclosure_id"] == 42
        assert d["signal_type"] == "judgment_joined_party_gap"
        assert d["party_name"] == "CAPITAL ONE"


# -----------------------------------------------------------------------
# AuditSignalExtractor (with mocked DB)
# -----------------------------------------------------------------------

class _FakeResult:
    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self._rows = rows or []

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self, results: dict[str, list[tuple[Any, ...]]]) -> None:
        self._results = results
        self._call_idx = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        pass

    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> _FakeResult:
        sql = str(stmt).strip()
        # Return canned results based on query patterns
        for key, rows in self._results.items():
            if key in sql:
                return _FakeResult(rows)
        return _FakeResult()


class _FakeEngine:
    def __init__(self, results: dict[str, list[tuple[Any, ...]]]) -> None:
        self._results = results

    def connect(self) -> _FakeConn:
        return _FakeConn(self._results)


class TestAuditSignalExtractorIntegration:
    """Integration test with fake DB results."""

    def _build_extractor(self, results: dict[str, list[tuple[Any, ...]]]) -> AuditSignalExtractor:
        engine = _FakeEngine(results)
        return AuditSignalExtractor(engine=engine)

    def test_no_foreclosure_found(self) -> None:
        extractor = self._build_extractor({})
        signals = extractor.extract_signals_for(999)
        assert signals == []

    def test_full_signal_extraction(self) -> None:
        """Run all six signal families with canned foreclosure + enc data."""
        import json

        judgment_data = json.dumps({
            "plaintiff": "NATIONSTAR MORTGAGE LLC",
            "defendants": [
                {"name": "JOHN DOE", "party_type": "borrower"},
                {"name": "CAPITAL ONE BANK", "party_type": "second_mortgage_holder"},
            ],
            "foreclosed_mortgage": {
                "instrument_number": "2019555555",
                "recording_book": "22222",
                "recording_page": "333",
            },
            "legal_description": "LOT 5 BLOCK 2 SUNSET LAKES",
            "judgment_date": "2025-06-01",
        })

        results = {
            # _load_foreclosure query
            "FROM foreclosures": [
                (1, "292020CA001234A001HC", "20-CA-001234", "STRAP123", "FOLIO123",
                 judgment_data, date(2018, 3, 15), date(2025, 6, 1)),
            ],
            # _load_encumbrances query
            "FROM ori_encumbrances": [
                # LP row
                (100, "lis_pendens", "WELLS FARGO BANK NA", "JOHN DOE",
                 "2018111111", "11111", "222", date(2018, 3, 15),
                 "292020CA001234A001HC",
                 "LOT 5 BLOCK 2 SUNSET LAKES", "(LP) LIS PENDENS"),
            ],
            # clerk_civil_parties plaintiff query
            "party_type ILIKE 'Plaintiff%'": [
                ("WELLS FARGO BANK NA",),
            ],
            # clerk_civil_parties all query
            "COALESCE(NULLIF(name, ''), NULLIF(business_name, ''))": [
                ("WELLS FARGO BANK NA",),
                ("JOHN DOE",),
            ],
        }

        extractor = self._build_extractor(results)
        signals = extractor.extract_signals_for(1)

        signal_types = {s.signal_type for s in signals}

        # Should detect plaintiff change (Wells Fargo -> Nationstar)
        assert "lp_to_judgment_plaintiff_change" in signal_types

        # Should detect CAPITAL ONE as a joined party not in encumbrances
        assert "judgment_joined_party_gap" in signal_types

        # Should detect foreclosed mortgage instrument gap
        assert "judgment_instrument_gap" in signal_types

        # Should detect CAPITAL ONE as a new party expansion
        assert "lp_to_judgment_party_expansion" in signal_types

        # Should detect long case interim risk (2018 -> 2025 = 7 years)
        assert "long_case_interim_risk" in signal_types

    def test_lp_recording_date_drives_long_case_signal(self) -> None:
        """Use LP recording date, not foreclosure filing date, for the gap."""
        import json

        judgment_data = json.dumps({
            "plaintiff": "WELLS FARGO BANK NA",
            "defendants": [],
            "judgment_date": "2025-06-01",
        })

        results = {
            "FROM foreclosures": [
                (1, "292020CA001234A001HC", "20-CA-001234", "STRAP123", "FOLIO123",
                 judgment_data, date(2018, 3, 15), date(2025, 6, 1)),
            ],
            "FROM ori_encumbrances": [
                (100, "lis_pendens", "WELLS FARGO BANK NA", "JOHN DOE",
                 "2018111111", "11111", "222", date(2024, 3, 15),
                 "292020CA001234A001HC",
                 "LOT 5 BLOCK 2 SUNSET LAKES", "(LP) LIS PENDENS"),
            ],
            "party_type ILIKE 'Plaintiff%'": [
                ("WELLS FARGO BANK NA",),
            ],
            "COALESCE(NULLIF(name, ''), NULLIF(business_name, ''))": [
                ("WELLS FARGO BANK NA",),
                ("JOHN DOE",),
            ],
        }

        extractor = self._build_extractor(results)
        signals = extractor.extract_signals_for(1)
        signal_types = {s.signal_type for s in signals}
        assert "long_case_interim_risk" not in signal_types

    def test_static_helpers(self) -> None:
        """Test the static helper methods of AuditSignalExtractor."""
        enc_rows = [
            {
                "id": 1,
                "encumbrance_type": "lis_pendens",
                "party1": "BANK A, BANK B",
                "party2": "JOHN DOE",
                "instrument_number": "123",
                "book": "456",
                "page": "789",
                "recording_date": date(2020, 1, 1),
                "case_number": "CA001",
                "legal_description": "LOT 5",
                "raw_document_type": "LP",
            },
            {
                "id": 2,
                "encumbrance_type": "assignment",
                "party1": "BANK A",
                "party2": "BANK C",
                "instrument_number": "555",
                "book": "111",
                "page": "222",
                "recording_date": date(2022, 6, 1),
                "case_number": "CA001",
                "legal_description": "",
                "raw_document_type": "ASGN",
            },
        ]

        # find_lp_row
        lp = AuditSignalExtractor._find_lp_row(enc_rows)  # noqa: SLF001
        assert lp is not None
        assert lp["id"] == 1

        # collect_encumbrance_parties
        parties = AuditSignalExtractor._collect_encumbrance_parties(enc_rows)  # noqa: SLF001
        assert "BANK A" in parties
        assert "BANK B" in parties
        assert "JOHN DOE" in parties
        assert "BANK C" in parties

        # collect_encumbrance_instruments
        instruments = AuditSignalExtractor._collect_encumbrance_instruments(enc_rows)  # noqa: SLF001
        assert len(instruments) == 2
        assert instruments[0]["instrument_number"] == "123"

        # count_lifecycle_encumbrances
        count = AuditSignalExtractor._count_lifecycle_encumbrances(  # noqa: SLF001
            enc_rows,
            lp_date=date(2019, 1, 1),
            judgment_date=date(2025, 1, 1),
        )
        assert count == 1  # the assignment row is in range

        count_none = AuditSignalExtractor._count_lifecycle_encumbrances(  # noqa: SLF001
            enc_rows,
            lp_date=None,
            judgment_date=None,
        )
        assert count_none == 0
