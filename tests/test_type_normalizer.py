from src.db.type_normalizer import normalize_document_type
from src.db.type_normalizer import normalize_encumbrance_type


def test_lifecycle_document_types_normalize_to_other() -> None:
    assert normalize_encumbrance_type(normalize_document_type("(MOD) MODIFICATION")) == "other"
    assert normalize_encumbrance_type(normalize_document_type("(SUB) SUBORDINATION")) == "other"
    assert normalize_encumbrance_type(
        normalize_document_type("(NCL) NOTICE OF CONTEST OF LIEN")
    ) == "other"
    assert normalize_encumbrance_type(
        normalize_document_type("(CTF) CERTIFIED COPY OF COURT JUDGMENT")
    ) == "other"


def test_satmtg_classified_as_satisfaction_not_mortgage() -> None:
    """SATMTG (satisfaction of mortgage) must NOT be classified as mortgage.

    This was a confirmed bug where the MTG substring check ran before the SAT
    check, causing satisfaction-of-mortgage documents to inflate survived debt.
    """
    # Via normalize_document_type (the ORI code path)
    assert normalize_document_type("(SATMTG) SATISFACTION OF MORTGAGE") == "satisfaction"
    # Via normalize_encumbrance_type with raw codes
    assert normalize_encumbrance_type("SATMTG") == "satisfaction"
    assert normalize_encumbrance_type("(SATMTG) SATISFACTION OF MORTGAGE") == "satisfaction"


def test_relmtg_classified_as_release_not_mortgage() -> None:
    """RELMTG (release of mortgage) must NOT be classified as mortgage."""
    assert normalize_document_type("(RELMTG) RELEASE OF MORTGAGE") == "satisfaction"
    assert normalize_encumbrance_type("RELMTG") == "release"
    assert normalize_encumbrance_type("(RELMTG) RELEASE OF MORTGAGE") == "release"


def test_asgnmtg_classified_as_assignment_not_mortgage() -> None:
    """ASGNMTG (assignment of mortgage) must NOT be classified as mortgage."""
    assert normalize_document_type("(ASGNMTG) ASSIGNMENT OF MORTGAGE") == "assignment"
    assert normalize_encumbrance_type("ASGNMTG") == "assignment"
    assert normalize_encumbrance_type("(ASGNMTG) ASSIGNMENT OF MORTGAGE") == "assignment"


def test_plain_mortgage_still_classified_as_mortgage() -> None:
    """Ensure plain mortgage codes are still correctly classified after reorder."""
    assert normalize_encumbrance_type("MTG") == "mortgage"
    assert normalize_encumbrance_type("MORTGAGE") == "mortgage"
    assert normalize_encumbrance_type("(MTG) MORTGAGE") == "mortgage"
    assert normalize_encumbrance_type("DOT") == "mortgage"
    assert normalize_encumbrance_type("HELOC") == "mortgage"


def test_satisfaction_and_release_basic_codes() -> None:
    """Verify basic SAT/REL codes still work correctly."""
    assert normalize_encumbrance_type("SAT") == "satisfaction"
    assert normalize_encumbrance_type("SATISFACTION") == "satisfaction"
    assert normalize_encumbrance_type("REL") == "release"
    assert normalize_encumbrance_type("RELEASE") == "release"
    assert normalize_encumbrance_type("TER") == "release"
    assert normalize_encumbrance_type("TERMINATION") == "release"


def test_assignment_basic_codes() -> None:
    """Verify basic assignment codes still work correctly."""
    assert normalize_encumbrance_type("ASG") == "assignment"
    assert normalize_encumbrance_type("ASSIGNMENT") == "assignment"
