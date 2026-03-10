"""Pydantic schema for Notice of Commencement extraction (NOC).

A Notice of Commencement (NOC) is recorded when construction or improvement
work begins on a property.  It establishes the owner and general contractor,
and creates the window during which mechanic's liens can be filed.

Key downstream consumers:
    - ``pg_ori_service``: creates encumbrance record of type 'noc'
    - ``pg_survival_service``: NOCs themselves don't survive foreclosure but
      mechanic's liens filed under them may
    - ``encumbrance_audit_signals``: validates contractor/owner against chain
    - Permit cross-referencing: NOC contractor name matches building permits

**Hillsborough County patterns:**
    - NOCs expire 1 year from recording date unless a different expiration is
      stated or the NOC is terminated early via a TER (Termination) recording
    - The owner on the NOC should match the current deed owner — if it doesn't,
      this signals an unrecorded contract for deed or recent unrecorded flip
    - General contractor name from the NOC is the same entity that would file
      a mechanic's lien if unpaid
    - Subcontractors and suppliers can also file mechanic's liens under the NOC
      without being named in it
    - A Termination of NOC (TER) closes the lien window early
"""

from __future__ import annotations

from typing import Any

from pydantic import Field, field_validator

from src.models.extraction_base import (
    BaseDocumentExtraction,
    normalize_date,
    parse_dollar,
)


class NOCExtraction(BaseDocumentExtraction):
    """Structured extraction from a recorded Notice of Commencement.

    **ORI doc type codes:** NOC

    **Normalization rules for the LLM:**

    - **Owner**: The property owner who authorized the construction work.
      This should match the current grantee in the deed chain.  If it's
      an LLC or corporate entity, extract the full legal name.
    - **General contractor**: The licensed contractor performing the work.
      Include their license number if stated.  This name will be used to
      cross-reference building permits and potential mechanic's liens.
    - **Expiration date**: If explicitly stated in the NOC, extract it.
      If not stated, the default under Florida law is 1 year from recording
      date — but do NOT calculate this, leave it null and note the absence.
    - **Improvements description**: Brief summary of the work (e.g. "re-roof",
      "kitchen renovation", "new construction single family home").
    - **Contract amount**: The stated contract price for the improvements,
      if included.  This sets the upper bound for potential mechanic's liens.
    """

    # -- NOC-specific fields ------------------------------------------------

    # -- Parties ------------------------------------------------------------

    owner: str | None = Field(
        default=None,
        description=(
            "Full legal name of the property owner who filed the NOC.  "
            "Should match the current grantee in the deed chain.  If the "
            "owner is an entity (LLC, trust), include the full legal name."
        ),
    )
    general_contractor: str | None = Field(
        default=None,
        description=(
            "Full legal name of the general contractor.  Include license "
            "number if stated (e.g. 'ABC ROOFING INC., License #CGC123456')."
        ),
    )
    contractor_license_number: str | None = Field(
        default=None,
        description="Contractor's license number if stated separately.",
    )
    surety_bond_company: str | None = Field(
        default=None,
        description="Name of the surety bond company if a payment bond is stated.",
    )
    surety_bond_amount: float | None = Field(
        default=None,
        description="Surety bond amount in dollars, if stated.",
    )

    # -- Work details -------------------------------------------------------

    improvements_description: str | None = Field(
        default=None,
        description=(
            "Brief description of the construction/improvement work being "
            "performed (e.g. 're-roof', 'new single family residence', "
            "'kitchen and bathroom renovation')."
        ),
    )
    contract_amount: float | None = Field(
        default=None,
        description="Stated contract price for the improvements in dollars.",
    )

    # -- Dates --------------------------------------------------------------

    expiration_date: str | None = Field(
        default=None,
        description=(
            "Expiration date of the NOC if explicitly stated (YYYY-MM-DD).  "
            "Default under FL law is 1 year from recording, but do NOT "
            "calculate — only extract if explicitly stated in the document."
        ),
    )
    commencement_date: str | None = Field(
        default=None,
        description="Date construction/improvement work is scheduled to begin (YYYY-MM-DD).",
    )

    # -- Validators ---------------------------------------------------------

    @field_validator("contract_amount", "surety_bond_amount", mode="before")
    @classmethod
    def _clean_amounts(cls, v: Any) -> float | None:
        return parse_dollar(v)

    @field_validator("expiration_date", "commencement_date", mode="before")
    @classmethod
    def _clean_dates(cls, v: Any) -> str | None:
        return normalize_date(v)
