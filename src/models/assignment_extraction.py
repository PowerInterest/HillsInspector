"""Pydantic schema for Assignment document extraction (ASG, ASGT, ASGN, ASGNMTG, ASINT).

Assignments transfer the right to collect on a mortgage from one lender to
another.  When the foreclosure plaintiff doesn't match the original mortgagee,
the chain of assignments bridges the gap.  Without them, the title chain shows
a "plaintiff chain gap" that makes survival analysis unreliable.

Key downstream consumers:
    - ``pg_ori_service``: tracks assignment_count and current_holder on
      ori_encumbrances records
    - ``pg_survival_service``: uses current_holder for lien matching
    - ``pg_title_chain_service``: assignments explain ownership changes in
      the encumbrance chain
    - ``encumbrance_audit_signals``: validates plaintiff-to-assignee chain

**Hillsborough County patterns:**
    - MERS assignments: "MORTGAGE ELECTRONIC REGISTRATION SYSTEMS, INC. AS
      NOMINEE FOR [Original Lender]" assigns to the current servicer.  ~60%
      of modern mortgage assignments involve MERS.
    - Corporate mergers: Some "assignments" are actually merger documents
      (e.g. "REGIONS BANK SUCCESSOR BY MERGER WITH AMSOUTH BANK").  These
      aren't recorded as ASG but appear in the case caption.
    - Multiple assignments: A single mortgage may be assigned 3-5 times over
      its life.  Each assignment references the original mortgage and the
      prior assignee.
    - ASGNMTG = Assignment of Mortgage (most common)
    - ASINT = Assignment of Interest (partial assignment)
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import Field, field_validator

from src.models.extraction_base import (
    BaseDocumentExtraction,
    RecordingReference,
    normalize_date,
    parse_dollar,
)


class AssignmentType(StrEnum):
    ASSIGNMENT = "ASG"
    ASSIGNMENT_TRUST = "ASGT"
    ASSIGNMENT_GENERAL = "ASGN"
    ASSIGNMENT_MORTGAGE = "ASGNMTG"
    ASSIGNMENT_INTEREST = "ASINT"
    OTHER = "OTHER"


class AssignmentExtraction(BaseDocumentExtraction):
    """Structured extraction from a recorded Assignment instrument.

    **ORI doc type codes:** ASG, ASGT, ASGN, ASGNMTG, ASINT

    **Normalization rules for the LLM:**

    - **Assignor (old lender)**: The entity transferring their interest.
      This should match the current holder of the mortgage at the time of
      the assignment.  If MERS, include the full "as nominee for" clause.
    - **Assignee (new lender)**: The entity receiving the mortgage interest.
      After this assignment, this entity is the current holder.  If it's a
      trust, include the full trust name (e.g. "DEUTSCHE BANK NATIONAL TRUST
      COMPANY, AS TRUSTEE FOR FIRST FRANKLIN MORTGAGE LOAN TRUST 2006-FF14").
    - **Parent instrument**: The Book/Page or Instrument Number of the
      ORIGINAL mortgage being assigned.  NOT the prior assignment — the
      base mortgage instrument.  Look for "that certain mortgage dated..."
      or "recorded in O.R. Book X, Page Y".
    - **MERS assignment**: If the assignor is MERS "as nominee for" a lender,
      set ``is_mers_assignment = true`` and capture the MERS MIN if stated.
    """

    # -- Assignment-specific fields -----------------------------------------

    assignment_type: AssignmentType | None = Field(
        default=None,
        description="Type of assignment instrument.",
    )

    # -- Parties ------------------------------------------------------------

    assignor: str | None = Field(
        default=None,
        description=(
            "Full legal name of the assignor (old lender/transferor).  "
            "For MERS assignments, include the full designation: "
            "'MORTGAGE ELECTRONIC REGISTRATION SYSTEMS, INC. AS NOMINEE "
            "FOR [Original Lender]'.  The assignor should match the "
            "current holder from the previous assignment or the original "
            "mortgagee."
        ),
    )
    assignor_type: str | None = Field(
        default=None,
        description="Entity type: bank, servicer, trust, mers, individual.",
    )
    assignee: str | None = Field(
        default=None,
        description=(
            "Full legal name of the assignee (new lender/recipient).  "
            "After this assignment, this entity is the mortgage holder.  "
            "Include full trust designations if applicable."
        ),
    )
    assignee_type: str | None = Field(
        default=None,
        description="Entity type: bank, servicer, trust, mers, individual.",
    )

    # -- Parent instrument --------------------------------------------------

    parent_instrument: RecordingReference | None = Field(
        default=None,
        description=(
            "Recording reference of the ORIGINAL mortgage being assigned.  "
            "This is the base instrument, not a prior assignment.  Look for "
            "'that certain mortgage recorded in O.R. Book X, Page Y' or "
            "'Instrument No. XXXXXXXXXX'."
        ),
    )
    original_mortgage_amount: float | None = Field(
        default=None,
        description="Original principal amount of the mortgage being assigned.",
    )
    original_borrower: str | None = Field(
        default=None,
        description="Name of the original borrower/mortgagor on the base mortgage.",
    )

    # -- MERS ---------------------------------------------------------------

    is_mers_assignment: bool = Field(
        default=False,
        description="True if the assignor is MERS acting as nominee.",
    )
    mers_min: str | None = Field(
        default=None,
        description="18-digit MERS Mortgage Identification Number if stated.",
    )

    # -- Consideration ------------------------------------------------------

    consideration: str | None = Field(
        default=None,
        description="Stated consideration for the assignment, if any.",
    )

    # -- Validators ---------------------------------------------------------

    @field_validator("assignment_type", mode="before")
    @classmethod
    def _coerce_type(cls, v: Any) -> Any:
        if isinstance(v, str):
            v = v.strip().upper()
            aliases = {
                "ASSIGNMENT": "ASG",
                "ASSIGNMENT OF MORTGAGE": "ASGNMTG",
                "ASSIGNMENT OF INTEREST": "ASINT",
                "ASSIGNMENT OF TRUST": "ASGT",
            }
            v = aliases.get(v, v)
            try:
                return AssignmentType(v)
            except ValueError:
                return v
        return v

    @field_validator("original_mortgage_amount", mode="before")
    @classmethod
    def _clean_amount(cls, v: Any) -> float | None:
        return parse_dollar(v)

    @field_validator("execution_date", mode="before")
    @classmethod
    def _clean_exec_date(cls, v: Any) -> str | None:
        return normalize_date(v)
