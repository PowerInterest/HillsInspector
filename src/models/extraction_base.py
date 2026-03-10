"""Shared base model for all ORI document extractions.

Every document type in the Hillsborough County Official Records Index (ORI)
shares a common set of fields: recording references, property identification,
and extraction metadata.  This module defines:

- ``BaseDocumentExtraction`` — the Pydantic v2 base model all doc-type models inherit
- Shared validators for dates, dollar amounts, and party names
- Hillsborough County domain knowledge constants that get embedded into
  LLM prompt descriptions so the extraction model itself teaches the LLM
  how to normalize its output

Downstream consumers:
    - ``pg_ori_service`` ingests extracted data into ``ori_encumbrances``
    - ``pg_survival_service`` uses recording refs for foreclosing lien matching
    - ``pg_title_chain_service`` uses party names + recording dates for chain building
    - ``properties.py`` + Jinja templates render extracted data in the web dashboard

Architecture:
    Tesseract OCR produces raw text → text is sent to an LLM with the
    JSON Schema from ``SubModel.model_json_schema()`` → LLM returns JSON →
    ``SubModel.model_validate(raw_dict)`` coerces and validates → clean dict
    is persisted to PG JSONB column.
"""

from __future__ import annotations

import re
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic.json_schema import (
    DEFAULT_REF_TEMPLATE,
    GenerateJsonSchema,
    JsonSchemaMode,
    SkipJsonSchema,
)


# ---------------------------------------------------------------------------
# Hillsborough County domain knowledge
#
# These constants are referenced in Field descriptions so the LLM prompt
# itself carries domain expertise.  They also power post-extraction
# validation (e.g. stripping judge names from defendant lists).
# ---------------------------------------------------------------------------

#: 13th Judicial Circuit judges — these names appear in case captions
#: but are NOT parties to the action.  The LLM should never list them
#: as plaintiffs or defendants.
HILLSBOROUGH_JUDGES: frozenset[str] = frozenset({
    "CHERYL THOMAS",
    "VICTOR CRIST",
    "VICTOR D. CRIST",
    "LISA CAMPBELL",
    "RALPH STODDARD",
    "MARK WOLFE",
    "CLAUDIA ISOM",
    "MARTHA COOK",
    "JAMES BARTON",
    "JAMES D. BARTON",
    "CATHERINE CATLIN",
    "EMILY PEACOCK",
    "SAMANTHA WARD",
    "PAUL HUEY",
    "ROBERT SURDI",
    "JARED SMITH",
    "JESSICA COSTELLO",
    "REX BARBAS",
    "THOMAS BARBER",
})

#: Known federal entity substrings — if any of these appear in a party
#: name, the party is likely a US federal entity with extended redemption
#: rights (28 USC § 2410).
FEDERAL_ENTITY_MARKERS: tuple[str, ...] = (
    "UNITED STATES",
    "INTERNAL REVENUE",
    "IRS",
    "DEPARTMENT OF THE TREASURY",
    "DEPARTMENT OF HOUSING",
    "FEDERAL HOUSING",
    "FHA",
    "DEPARTMENT OF VETERANS",
    "VETERANS AFFAIRS",
    " VA ",  # spaces to avoid false positives
    "HUD",
    "SMALL BUSINESS ADMIN",
    "SBA",
    "US ARMY",
    "US NAVY",
    "US AIR FORCE",
)

#: Common MERS naming patterns in Hillsborough County recordings
MERS_PATTERNS: tuple[str, ...] = (
    "MORTGAGE ELECTRONIC REGISTRATION",
    "MERS",
    "MERSCORP",
)

#: Major banks and servicers frequently seen in Hillsborough foreclosures.
#: Used in Field descriptions to help the LLM normalize plaintiff names.
COMMON_SERVICERS: tuple[str, ...] = (
    "WELLS FARGO",
    "JPMORGAN CHASE",
    "BANK OF AMERICA",
    "NATIONSTAR",
    "MR. COOPER",
    "PENNYMAC",
    "FREEDOM MORTGAGE",
    "NEWREZ",
    "SHELLPOINT",
    "LAKEVIEW LOAN",
    "NAVY FEDERAL",
    "USAA FEDERAL",
    "REGIONS BANK",
    "TRUIST",
    "US BANK",
    "DEUTSCHE BANK",
    "WILMINGTON SAVINGS",
    "WILMINGTON TRUST",
    "HSBC",
    "CITIMORTGAGE",
    "CITIBANK",
    "CARRINGTON MORTGAGE",
    "SPECIALIZED LOAN SERVICING",
    "SELECT PORTFOLIO",
    "OCWEN",
    "PHH MORTGAGE",
    "DITECH",
    "BAYVIEW LOAN",
    "CALIBER HOME LOANS",
    "GUILD MORTGAGE",
    "ROCKET MORTGAGE",
    "QUICKEN LOANS",
)

#: Defendant name fragments that indicate generic/unknown occupants,
#: not real parties.  These still must be captured (they appear in the
#: judgment) but should be typed as ``tenant`` or ``unknown``.
GENERIC_OCCUPANT_PATTERNS: tuple[str, ...] = (
    "UNKNOWN TENANT",
    "UNKNOWN SPOUSE",
    "UNKNOWN HEIRS",
    "UNKNOWN PARTIES",
    "JOHN DOE",
    "JANE DOE",
    "ALL UNKNOWN",
    "ALL OTHER",
    "AND ALL UNKNOWN",
)

#: Common HOA/COA name suffixes in Hillsborough County
HOA_SUFFIXES: tuple[str, ...] = (
    "HOMEOWNERS ASSOCIATION",
    "HOMEOWNER'S ASSOCIATION",
    "HOMEOWNERS' ASSOCIATION",
    "HOME OWNERS ASSOCIATION",
    "CONDOMINIUM ASSOCIATION",
    "CONDO ASSOCIATION",
    "PROPERTY OWNERS ASSOCIATION",
    "MASTER ASSOCIATION",
    "COMMUNITY ASSOCIATION",
    "MAINTENANCE ASSOCIATION",
)


# ---------------------------------------------------------------------------
# Shared parsing helpers
# ---------------------------------------------------------------------------

_DATE_RE = re.compile(r"(\d{4})-(\d{1,2})-(\d{1,2})")
_DATE_MDY_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")
_DATE_LONG_RE = re.compile(
    r"(?:January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2}),?\s+(\d{4})",
    re.IGNORECASE,
)
_DOLLAR_RE = re.compile(r"[\$,\s]")
_MONTH_MAP: dict[str, int] = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def normalize_date(v: Any) -> str | None:
    """Coerce a date-like value to YYYY-MM-DD or None.

    Handles ISO (2025-03-15), US slash (03/15/2025), and long-form
    (March 15, 2025) formats.  Returns None for null/empty/garbage.
    """
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() in ("null", "none", "n/a", ""):
        return None

    # ISO: 2025-03-15 or 2025-3-5
    m = _DATE_RE.search(s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    # US slash: 03/15/2025
    m = _DATE_MDY_RE.match(s)
    if m:
        return f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"

    # Long form: March 15, 2025
    m = _DATE_LONG_RE.search(s)
    if m:
        month_str = s[:m.start() + len(m.group(0))].strip().split()[0].lower()
        month_num = _MONTH_MAP.get(month_str)
        if month_num:
            return f"{m.group(2)}-{month_num:02d}-{int(m.group(1)):02d}"

    return None


def parse_dollar(v: Any) -> float | None:
    """Coerce a dollar amount (string or numeric) to float or None.

    Handles: "$123,456.78", "123456.78", 123456.78, 0, "0.00", None.
    Returns None for zero/empty values (matching existing extract_key_amounts
    behavior where zero means "not found").
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v) if v != 0 else None
    s = _DOLLAR_RE.sub("", str(v).strip())
    if not s or s.lower() in ("null", "none", "n/a"):
        return None
    try:
        result = float(s)
        return result if result != 0 else None
    except (ValueError, TypeError):
        return None


def normalize_party_name(name: str) -> str:
    """Normalize a party name: uppercase, strip non-alphanum, collapse spaces.

    Mirrors the PG ``normalize_party_name()`` function so extracted names
    can be compared against the database.
    """
    if not name:
        return ""
    upper = name.upper().strip()
    cleaned = re.sub(r"[^A-Z0-9\s'.,&/-]", "", upper)
    return re.sub(r"\s+", " ", cleaned).strip()


def is_federal_entity(name: str) -> bool:
    """Check if a party name matches known federal entity patterns."""
    upper = name.upper()
    return any(marker in upper for marker in FEDERAL_ENTITY_MARKERS)


def is_judge_name(name: str) -> bool:
    """Check if a name matches a known 13th Circuit judge."""
    return normalize_party_name(name) in {
        normalize_party_name(j) for j in HILLSBOROUGH_JUDGES
    }


def is_hoa_entity(name: str) -> bool:
    """Check if a party name looks like an HOA/COA."""
    upper = name.upper()
    return any(suffix in upper for suffix in HOA_SUFFIXES)


def is_generic_occupant(name: str) -> bool:
    """Check if a defendant name is a generic/unknown occupant placeholder."""
    upper = name.upper()
    return any(pattern in upper for pattern in GENERIC_OCCUPANT_PATTERNS)


# ---------------------------------------------------------------------------
# Shared recording reference model
# ---------------------------------------------------------------------------

class StrictExtractionModel(BaseModel):
    """Base class for extraction schemas with hard validation semantics.

    These models are used as the contract passed to the LLM and as the
    validator run on the returned JSON.  Unknown keys must fail validation
    instead of being silently dropped.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    @classmethod
    def _required_input_fields(cls) -> set[str]:
        """Return the fields the LLM must explicitly include in the JSON."""
        required: set[str] = set()
        for name, field in cls.model_fields.items():
            if name == "raw_text":
                continue
            if field.exclude is True:
                continue
            required.add(name)
        return required

    @model_validator(mode="before")
    @classmethod
    def _require_explicit_field_keys(cls, value: Any) -> Any:
        """Reject partial objects that omit declared schema keys.

        The extraction contract is "every key is present, nullable when
        unknown", not "keys may be silently omitted".  That matters both for
        constrained decoding and for deterministic post-extraction validation.
        """
        if not isinstance(value, dict):
            return value
        missing = sorted(cls._required_input_fields() - set(value))
        if missing:
            raise ValueError(
                "Missing required field key(s): " + ", ".join(missing)
            )
        return value

    @classmethod
    def _apply_required_schema_contract(cls, schema: dict[str, Any]) -> None:
        """Recursively mark object properties as required in JSON Schema."""
        if schema.get("type") == "object":
            properties = schema.get("properties")
            if isinstance(properties, dict) and properties:
                schema["required"] = [name for name in properties if name != "raw_text"]
                schema["additionalProperties"] = False

        for key in ("$defs", "properties", "patternProperties"):
            child_map = schema.get(key)
            if isinstance(child_map, dict):
                for child in child_map.values():
                    if isinstance(child, dict):
                        cls._apply_required_schema_contract(child)

        items = schema.get("items")
        if isinstance(items, dict):
            cls._apply_required_schema_contract(items)

        for key in ("allOf", "anyOf", "oneOf"):
            children = schema.get(key)
            if isinstance(children, list):
                for child in children:
                    if isinstance(child, dict):
                        cls._apply_required_schema_contract(child)

    @classmethod
    def model_json_schema(
        cls,
        by_alias: bool = True,
        ref_template: str = DEFAULT_REF_TEMPLATE,
        schema_generator: type[GenerateJsonSchema] = GenerateJsonSchema,
        mode: JsonSchemaMode = "validation",
    ) -> dict[str, Any]:
        """Force JSON Schema to advertise the same strict contract we validate."""
        schema = super().model_json_schema(
            by_alias=by_alias,
            ref_template=ref_template,
            schema_generator=schema_generator,
            mode=mode,
        )
        cls._apply_required_schema_contract(schema)
        return schema


class RecordingReference(StrictExtractionModel):
    """A reference to a recorded document in the Official Records.

    Used for cross-referencing: the parent mortgage in a satisfaction,
    the lis pendens instrument in a judgment, etc.
    """

    instrument_number: str | None = Field(
        ...,
        description="Official Records instrument number (preferred over book/page for modern records).",
    )
    recording_book: str | None = Field(
        ...,
        description="Official Records book number.",
    )
    recording_page: str | None = Field(
        ...,
        description="Official Records page number.",
    )
    recording_date: str | None = Field(
        ...,
        description="Date recorded in Official Records (YYYY-MM-DD).",
    )

    @field_validator("recording_date", mode="before")
    @classmethod
    def _clean_date(cls, v: Any) -> str | None:
        return normalize_date(v)

    def has_reference(self) -> bool:
        """True if at least one identifying field is populated."""
        return bool(self.instrument_number or self.recording_book or self.recording_page)

    def book_page_str(self) -> str | None:
        """Format Book/Page as a human-readable string."""
        if self.recording_book and self.recording_page:
            return f"Book {self.recording_book}, Page {self.recording_page}"
        return None


# ---------------------------------------------------------------------------
# Base extraction model
# ---------------------------------------------------------------------------

class BaseDocumentExtraction(StrictExtractionModel):
    """Base model for all Hillsborough County ORI document extractions.

    Subclasses add document-type-specific fields (parties, amounts,
    cross-references).  The base provides:

    - Recording identification (instrument, book/page)
    - Property identification (address, legal description, parcel)
    - Extraction quality metadata
    - Shared validators for dates and dollar amounts

    **Domain context for LLM prompts:**

    This document was recorded in Hillsborough County, Florida (13th Judicial
    Circuit).  The Official Records Index (ORI) assigns each document an
    instrument number and optionally a Book/Page reference.  Older documents
    (pre-2000) typically use Book/Page only; modern documents have instrument
    numbers.

    Known 13th Circuit judges (e.g. Cheryl Thomas, Victor Crist, Lisa Campbell,
    etc.) appear in case captions and document headers but are NOT parties -
    never include them as plaintiff, defendant, grantor, or grantee.
    """

    # -- Recording identification -------------------------------------------

    instrument_number: str | None = Field(
        ...,
        description=(
            "This document's Official Records instrument number.  "
            "For modern Hillsborough County records (post-2000), this is the "
            "primary identifier.  Format is typically a 10-digit number."
        ),
    )
    recording_book: str | None = Field(
        ...,
        description="Official Records book number where this document is recorded.",
    )
    recording_page: str | None = Field(
        ...,
        description="Official Records page number where this document is recorded.",
    )
    recording_date: str | None = Field(
        ...,
        description="Date this document was recorded in the Official Records (YYYY-MM-DD).",
    )
    execution_date: str | None = Field(
        ...,
        description="Date this document was signed/executed (YYYY-MM-DD).  Often earlier than recording_date.",
    )

    # -- Property identification --------------------------------------------

    property_address: str | None = Field(
        ...,
        description="Street address of the property, if stated in the document.",
    )
    legal_description: str | None = Field(
        ...,
        description=(
            "VERBATIM transcription of the full legal description.  "
            "Transcribe EVERY word, number, and abbreviation exactly as "
            "printed.  Never guess or fill in missing parts - use null if "
            "not present or illegible.\n\n"
            "Hillsborough County legal descriptions follow these patterns:\n\n"
            "1. PLATTED SUBDIVISION (most common residential): "
            "'LOT X, BLOCK Y, [SUBDIVISION NAME], ACCORDING TO THE PLAT "
            "THEREOF RECORDED IN PLAT BOOK Z, PAGE W, OF THE PUBLIC RECORDS "
            "OF HILLSBOROUGH COUNTY, FLORIDA.'  Extract the full string "
            "including the Plat Book/Page boilerplate.\n\n"
            "2. CONDO UNIT: 'UNIT 203B, [CONDOMINIUM NAME], A CONDOMINIUM, "
            "ACCORDING TO THE DECLARATION OF CONDOMINIUM RECORDED IN O.R. "
            "BOOK X, PAGE Y...'  Condos use UNIT instead of LOT/BLOCK.\n\n"
            "3. METES AND BOUNDS (unplatted land): 'THE NORTHWEST 1/4 OF "
            "SECTION 16, TOWNSHIP 27 SOUTH, RANGE 17 EAST...' followed by "
            "bearing/distance descriptions.  Copy the full text.\n\n"
            "4. MULTI-LOT: 'LOTS 1, 2 AND 3, BLOCK 100...' - include all "
            "lot numbers.\n\n"
            "5. PARTIAL LOT: 'LOT 11 AND THE WEST 15.00 FEET OF LOT 12...' "
            "or 'LESS THE WEST 50 FEET THEREOF' - include the full clause.\n\n"
            "CRITICAL DISTINCTIONS:\n"
            "- PLAT BOOK/PAGE is the subdivision map reference (property "
            "location).  O.R. BOOK/PAGE is the Official Records reference "
            "(document location).  Do NOT confuse them.  Plat Book goes in "
            "this field; O.R. Book goes in recording_book/instrument fields.\n"
            "- 'UNIT NO 2' after a subdivision name is often a PHASE, not a "
            "condo unit (e.g. 'NORTHDALE, SECTION B, UNIT NO. 2' means "
            "Phase 2 of the subdivision, not condo unit 2).\n"
            "- Apostrophes in subdivision names are significant: "
            "'TURMAN\\'S EAST YBOR', 'CLEWIS\\'S ADDITION'.\n"
            "- If Exhibit A or Schedule A contains a longer/fuller legal "
            "description than the inline text, use the Exhibit version."
        ),
    )
    parcel_id: str | None = Field(
        ...,
        description=(
            "Parcel ID / folio number if stated.  Hillsborough County uses "
            "two formats: 10-digit folio (e.g. '1929084000') and HCPA strap "
            "(e.g. '1929084NUB00000000040A').  Extract whichever is shown."
        ),
    )

    # -- Extraction quality metadata ----------------------------------------

    confidence_score: float | None = Field(
        ...,
        ge=0.0,
        le=1.0,
        description=(
            "Self-assessed confidence in extraction accuracy (0.0–1.0).  "
            "Lower this if text was illegible, fields were ambiguous, or "
            "the document format was unusual.  Be honest — a confident "
            "wrong answer is worse than an honest low score."
        ),
    )
    unclear_sections: list[str] = Field(
        ...,
        description=(
            "Sections or fields that were difficult to read or ambiguous.  "
            "Be specific (e.g. 'Page 3 amount partially illegible', "
            "'Party name unclear — could be SMITH or SMYTH')."
        ),
    )
    raw_text: SkipJsonSchema[str] = Field(
        default="",
        description="Full OCR text of the document.  Populated by the extraction pipeline, not the LLM.",
        exclude=True,
    )

    # -------------------------------------------------------------------
    # Shared validators
    # -------------------------------------------------------------------

    @field_validator("recording_date", "execution_date", mode="before")
    @classmethod
    def _clean_dates(cls, v: Any) -> str | None:
        return normalize_date(v)

    # -------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------

    def book_page_str(self) -> str | None:
        """Format Book/Page as a human-readable string."""
        if self.recording_book and self.recording_page:
            return f"Book {self.recording_book}, Page {self.recording_page}"
        return None

    def has_recording_ref(self) -> bool:
        """True if the document has at least one recording identifier."""
        return bool(
            self.instrument_number or self.recording_book or self.recording_page
        )
