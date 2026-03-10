"""Pydantic schema for Mortgage document extraction (MTG, MTGNT, DOT, HELOC, AGD).

Mortgages are the primary encumbrance type in foreclosure analysis.  Extracting
the recorded mortgage instrument provides the original loan terms, the lender,
and critically, any PUD/Condo Rider that names the HOA — which is often the
only way to discover the exact legal name of the association for lien searches.

Key downstream consumers:
    - ``pg_ori_service``: mortgage amount, lender for encumbrance records
    - ``pg_survival_service``: matches this instrument to the foreclosing lien
    - ``pg_title_chain_service``: mortgagor = owner at time of origination
    - ``encumbrance_audit_signals``: MERS tracking, HOA discovery

**Hillsborough County patterns:**
    - Most residential mortgages are standard Fannie Mae/Freddie Mac uniform
      instruments with a Florida rider
    - PUD Rider (Planned Unit Development) names the HOA
    - Condominium Rider names the COA
    - MERS is named as nominee on ~60% of mortgages since 2000
    - Adjustable Rate Rider shows rate change terms
    - Maturity dates are typically 30 years from execution
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import Field, field_validator

from src.models.extraction_base import (
    BaseDocumentExtraction,
    normalize_date,
    parse_dollar,
)


class MortgageType(StrEnum):
    MORTGAGE = "MTG"
    MORTGAGE_NOTE = "MTGNT"
    MORTGAGE_NOTE_INT = "MTGNIT"
    MORTGAGE_REVISION = "MTGREV"
    DEED_OF_TRUST = "DOT"
    HELOC = "HELOC"
    AGREEMENT = "AGD"
    OTHER = "OTHER"


class MortgageExtraction(BaseDocumentExtraction):
    """Structured extraction from a recorded mortgage instrument.

    **ORI doc type codes:** MTG, MTGNT, MTGNIT, MTGREV, DOT, HELOC, AGD

    **Normalization rules for the LLM:**

    - **Mortgagor (borrower)**: The property owner taking out the loan.  If
      multiple borrowers, list all separated by ' AND '.
    - **Mortgagee (lender)**: The bank or entity providing the loan.  If MERS
      is named "as nominee for [Lender]", extract BOTH the MERS designation
      AND the actual lender name separately.
    - **Principal amount**: The face amount of the note, NOT the appraised value.
    - **MERS MIN**: The 18-digit Mortgage Identification Number assigned by
      MERS.  Usually appears on page 1 near the top, in a box or bold text.
      Format: 1000XXX-XXXXXXXXXX-X.
    - **HOA/COA name**: Look SPECIFICALLY in the PUD Rider or Condominium Rider
      (typically the last 2-5 pages of the document).  The rider will say
      something like "The Property is part of a planned unit development known
      as [Subdivision] and the Homeowners Association is [Name], Inc."
      Extract the EXACT legal name including "Inc." or "Association".
    """

    # -- Mortgage-specific fields -------------------------------------------

    mortgage_type: MortgageType | None = Field(
        default=None,
        description="Type of mortgage instrument.",
    )

    # -- Parties ------------------------------------------------------------

    mortgagor: str | None = Field(
        default=None,
        description=(
            "Full legal name of the mortgagor (borrower/property owner).  "
            "If multiple borrowers, include all names.  This name should "
            "match a grantee in a prior deed in the chain of title."
        ),
    )
    mortgagee: str | None = Field(
        default=None,
        description=(
            "Full legal name of the mortgagee (lender).  If MERS is the "
            "nominee, this should be the ACTUAL lender, not MERS.  "
            "E.g. 'COUNTRYWIDE HOME LOANS, INC.' not 'MERS'."
        ),
    )

    # -- Loan terms ---------------------------------------------------------

    principal_amount: float | None = Field(
        default=None,
        description="Face amount of the mortgage note in dollars.",
    )
    interest_rate: float | None = Field(
        default=None,
        description=(
            "Annual interest rate as a percentage (e.g. 6.5 for 6.5%).  "
            "For adjustable rate mortgages, this is the initial rate."
        ),
    )
    maturity_date: str | None = Field(
        default=None,
        description=(
            "Date the loan matures / final payment is due (YYYY-MM-DD).  "
            "If only year given, use January 1 of that year."
        ),
    )
    is_adjustable_rate: bool = Field(
        default=False,
        description="True if an Adjustable Rate Rider is attached.",
    )

    # -- MERS ---------------------------------------------------------------

    mers_min: str | None = Field(
        default=None,
        description=(
            "18-digit MERS Mortgage Identification Number if present.  "
            "Format: 1000XXX-XXXXXXXXXX-X.  Usually printed on page 1."
        ),
    )
    is_mers_nominee: bool = Field(
        default=False,
        description=(
            "True if MERS is named as nominee for the lender.  This means "
            "the mortgage may have been assigned through MERS without a "
            "recorded assignment in the Official Records."
        ),
    )

    # -- HOA/COA discovery --------------------------------------------------

    association_name: str | None = Field(
        default=None,
        description=(
            "Exact legal name of the Homeowners Association or Condominium "
            "Association from the PUD Rider or Condominium Rider.  Include "
            "'Inc.' or 'Association' exactly as printed.  This is often the "
            "ONLY source for the HOA's legal name in the entire chain."
        ),
    )
    has_pud_rider: bool = Field(
        default=False,
        description="True if a Planned Unit Development (PUD) Rider is attached.",
    )
    has_condo_rider: bool = Field(
        default=False,
        description="True if a Condominium Rider is attached.",
    )

    # -- Validators ---------------------------------------------------------

    @field_validator("mortgage_type", mode="before")
    @classmethod
    def _coerce_type(cls, v: Any) -> Any:
        if isinstance(v, str):
            v = v.strip().upper()
            aliases = {
                "MORTGAGE": "MTG",
                "DEED OF TRUST": "DOT",
                "HOME EQUITY LINE": "HELOC",
                "HOME EQUITY LINE OF CREDIT": "HELOC",
            }
            v = aliases.get(v, v)
            try:
                return MortgageType(v)
            except ValueError:
                return v
        return v

    @field_validator("principal_amount", mode="before")
    @classmethod
    def _clean_amount(cls, v: Any) -> float | None:
        return parse_dollar(v)

    @field_validator("maturity_date", mode="before")
    @classmethod
    def _clean_date(cls, v: Any) -> str | None:
        return normalize_date(v)

    @field_validator("interest_rate", mode="before")
    @classmethod
    def _clean_rate(cls, v: Any) -> float | None:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v) if v != 0 else None
        s = str(v).strip().rstrip("%").strip()
        if not s or s.lower() in ("null", "none", "n/a"):
            return None
        try:
            return float(s) or None
        except (ValueError, TypeError):
            return None
