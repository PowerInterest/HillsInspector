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
