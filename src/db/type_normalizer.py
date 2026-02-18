"""Canonical type normalization for all DB-bound values.

Every type string that enters the database MUST pass through these functions.
SQLite triggers enforce this at the DB layer as a safety net.
"""

import re

# --- Allowed encumbrance types (enforced by SQLite trigger) ---
ALLOWED_ENCUMBRANCE_TYPES = frozenset({
    "mortgage", "judgment", "lis_pendens", "lien", "easement",
    "satisfaction", "release", "assignment", "other",
})

# --- Classification sets (canonical, post-normalization) ---
CANONICAL_ENCUMBRANCE_TYPES = frozenset({"mortgage", "judgment", "lis_pendens", "lien", "easement"})
CANONICAL_DEED_TYPES = frozenset({"deed"})
CANONICAL_SATISFACTION_TYPES = frozenset({"satisfaction", "release", "partial_release"})


def normalize_encumbrance_type(raw: str) -> str:
    """Normalize any encumbrance type variant to canonical form.

    Handles ORI format like '(MTG) MORTGAGE', short codes like 'MTG',
    and already-normalized forms like 'mortgage'.
    """
    t = (raw or "").upper().strip()
    if not t:
        return "other"
    if "MORTGAGE" in t or "MTG" in t or "DOT" in t or "HELOC" in t:
        return "mortgage"
    if "JUDGMENT" in t or "JUD" in t or "CCJ" in t:
        return "judgment"
    if "LIS PENDENS" in t or "LIS_PENDENS" in t or "(LP)" in t or t == "LP":
        return "lis_pendens"
    if "LIEN" in t or "(LN)" in t or t == "LN" or "MEDLN" in t or "FINANCING" in t or "(FIN)" in t or t == "FIN":
        return "lien"
    if "EASEMENT" in t or "(EAS)" in t or t == "EAS":
        return "easement"
    if "SATISFACTION" in t or "SAT" in t:
        return "satisfaction"
    if "RELEASE" in t or "REL" in t or "TERMINATION" in t or "(TER)" in t or t == "TER":
        return "release"
    if "ASSIGNMENT" in t or "ASG" in t:
        return "assignment"
    return "other"


# ORI code → canonical document type
_DOC_TYPE_MAP = {
    # Deeds
    "D": "deed", "WD": "deed", "QCD": "deed", "TAXDEED": "deed",
    "QC": "deed", "CD": "deed", "TD": "deed", "SD": "deed", "SWD": "deed",
    "PRD": "deed", "CT": "deed",
    # Mortgages
    "MTG": "mortgage", "MTGREV": "mortgage", "MTGNT": "mortgage",
    "MTGNIT": "mortgage", "DOT": "mortgage", "HELOC": "mortgage",
    # Liens
    "LN": "lien", "MEDLN": "lien", "LNCORPTX": "lien",
    "LIEN": "lien", "TL": "lien", "ML": "lien", "HOA": "lien",
    "COD": "lien", "MECH": "lien",
    # Judgments
    "JUD": "judgment", "CCJ": "judgment", "FJ": "judgment",
    # Lis Pendens
    "LP": "lis_pendens", "LISPEN": "lis_pendens",
    "RELLP": "release_lis_pendens",
    # Satisfactions
    "SAT": "satisfaction", "SATCORPTX": "satisfaction",
    "SATMTG": "satisfaction", "RELMTG": "satisfaction",
    # Releases / Terminations
    "REL": "release", "PR": "partial_release",
    "TER": "release",
    # Assignments
    "ASG": "assignment", "ASGT": "assignment",
    "ASGN": "assignment", "ASGNMTG": "assignment",
    "ASSIGN": "assignment", "ASINT": "assignment",
    # Domestic relations (divorce judgments can transfer property / create liens)
    "DRJUD": "judgment",
    # UCC / financing
    "FIN": "lien", "AGD": "mortgage",
    # Easements (encumbrances that survive foreclosure)
    "EAS": "easement",
    # Other
    "NOC": "noc", "MOD": "modification", "AFF": "affidavit",
    "ORD": "court_order", "CP": "court_paper",
}

_PAREN_RE = re.compile(r"\(([^)]+)\)\s*(.*)")


def normalize_document_type(raw: str) -> str:
    """Normalize ORI document type codes to canonical form.

    ORI returns '(MTG) MORTGAGE', '(D) DEED', etc.
    Extracts the parenthetical code and maps to canonical form.
    """
    t = (raw or "").strip()
    if not t:
        return ""
    # Extract code from parenthetical: "(MTG) MORTGAGE" -> "MTG"
    m = _PAREN_RE.match(t)
    code = m.group(1).upper() if m else t.upper()
    return _DOC_TYPE_MAP.get(code, raw)
