"""Pydantic schema for Satisfaction and Release extraction (SAT, REL, etc.).

Satisfactions and releases are the instruments that EXTINGUISH prior liens.
When a mortgage is paid off, the lender records a Satisfaction of Mortgage.
When a lien is released, the creditor records a Release.  These documents
ALWAYS reference the parent instrument they are extinguishing — extracting
that cross-reference is their primary value.

Key downstream consumers:
    - ``pg_ori_service``: links satisfaction to parent encumbrance, marks
      the parent as satisfied/released in ori_encumbrances
    - ``pg_survival_service``: satisfied mortgages don't need survival analysis
    - ``encumbrance_audit_signals``: validates satisfaction linkage completeness
    - ``pg_title_chain_service``: satisfactions close out chain entries

**Hillsborough County patterns:**
    - SAT (Satisfaction of Mortgage): Most common — mortgage paid in full
    - SATMTG: Specific satisfaction of mortgage instrument
    - SATCORPTX: Satisfaction of corporate tax lien
    - RELMTG: Release of mortgage (functionally identical to satisfaction)
    - REL: General release
    - PR: Partial Release — DOES NOT fully extinguish the lien, only
      releases specific property from a blanket lien
    - TER: Termination (of UCC filing, NOC, etc.)
    - PRREL: Partial Release (alternate code)
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import Field, field_validator

from src.models.extraction_base import (
    BaseDocumentExtraction,
    RecordingReference,
    normalize_date,
)


class SatisfactionType(StrEnum):
    SATISFACTION = "SAT"
    SATISFACTION_MORTGAGE = "SATMTG"
    SATISFACTION_CORP_TAX = "SATCORPTX"
    RELEASE_MORTGAGE = "RELMTG"
    RELEASE = "REL"
    PARTIAL_RELEASE = "PR"
    PARTIAL_RELEASE_ALT = "PRREL"
    TERMINATION = "TER"
    OTHER = "OTHER"


class SatisfactionExtraction(BaseDocumentExtraction):
    """Structured extraction from a Satisfaction or Release instrument.

    **ORI doc type codes:** SAT, SATMTG, SATCORPTX, RELMTG, REL, PR, TER, PRREL

    **Normalization rules for the LLM:**

    - **Releasor (creditor)**: The entity releasing/satisfying the lien.
      For mortgage satisfactions, this is the lender or servicer.  MERS
      often appears as the releasor for MERS-registered mortgages.
    - **Releasee (debtor)**: The property owner or borrower being released
      from the obligation.
    - **Parent instrument**: The MOST CRITICAL field.  Every satisfaction
      or release explicitly cites the Book/Page or Instrument Number of
      the mortgage or lien being satisfied.  Extract this exactly — it's
      the link that marks the parent encumbrance as dead.
    - **Partial release**: If this document only releases specific collateral
      (e.g. one lot from a blanket mortgage), set ``is_partial = true``.
      Partial releases do NOT fully extinguish the lien — the mortgage
      continues on other collateral.
    """

    # -- Satisfaction-specific fields ----------------------------------------

    satisfaction_type: SatisfactionType | None = Field(
        default=None,
        description="Type of satisfaction/release instrument.",
    )
    is_partial: bool = Field(
        default=False,
        description=(
            "True if this is a PARTIAL release — the lien continues to "
            "encumber other property.  A partial release on a blanket "
            "mortgage releases one parcel but the mortgage survives on "
            "remaining parcels.  Full satisfactions set this to false."
        ),
    )

    # -- Parties ------------------------------------------------------------

    releasor: str | None = Field(
        default=None,
        description=(
            "Full legal name of the releasor (creditor releasing the lien).  "
            "For mortgage satisfactions, this is the lender, servicer, or "
            "MERS.  Should match the mortgagee or assignee of the parent "
            "mortgage."
        ),
    )
    releasee: str | None = Field(
        default=None,
        description=(
            "Full legal name of the releasee (debtor being released).  "
            "Should match the mortgagor/borrower of the parent mortgage."
        ),
    )

    # -- Parent instrument (the one being satisfied) ------------------------

    parent_instrument: RecordingReference | None = Field(
        default=None,
        description=(
            "CRITICAL: Recording reference (Book/Page or Instrument Number) "
            "of the mortgage or lien being satisfied/released.  This is "
            "the most important field in the document.  Look for: "
            "'that certain mortgage recorded in O.R. Book X, Page Y', "
            "'Instrument No. XXXXXXXXXX', or similar language."
        ),
    )

    # -- Validators ---------------------------------------------------------

    @field_validator("satisfaction_type", mode="before")
    @classmethod
    def _coerce_type(cls, v: Any) -> Any:
        if isinstance(v, str):
            v = v.strip().upper()
            aliases = {
                "SATISFACTION": "SAT",
                "SATISFACTION OF MORTGAGE": "SATMTG",
                "RELEASE": "REL",
                "RELEASE OF MORTGAGE": "RELMTG",
                "PARTIAL RELEASE": "PR",
                "TERMINATION": "TER",
            }
            v = aliases.get(v, v)
            try:
                return SatisfactionType(v)
            except ValueError:
                return v
        return v

    @field_validator("execution_date", mode="before")
    @classmethod
    def _clean_exec_date(cls, v: Any) -> str | None:
        return normalize_date(v)
