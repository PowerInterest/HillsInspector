"""Pydantic schema for Lien document extraction (LN, HOA, MECH, CEL, ML, etc.).

Liens are monetary claims recorded against property.  They come in many flavors:
judgment liens, HOA liens, mechanic's liens, code enforcement liens, municipal
liens, and tax liens.  Each type has different survival rules in foreclosure.

Key downstream consumers:
    - ``pg_ori_service``: creates ori_encumbrances records
    - ``pg_survival_service``: lien type + recording date determine survival
    - ``encumbrance_audit_signals``: validates lien amounts and parties
    - ``pg_municipal_lien_service``: municipal/code enforcement lien handling

**Hillsborough County patterns:**
    - HOA liens (type HOA): Reference the HOA's declaration of covenants
      Book/Page.  Often filed by property management companies on behalf
      of the association.  Survive first-mortgage foreclosure under FL safe
      harbor (§ 720.3085) up to 12 months of assessments or 1% of original
      mortgage.
    - Mechanic's liens (MECH): Reference the Notice of Commencement (NOC)
      Book/Page.  Must be filed within 90 days of last work.  Expire 1 year
      from recording unless a lis pendens is filed.
    - Code enforcement liens (CEL): Filed by City of Tampa, Hillsborough
      County, or Temple Terrace for code violations.  Reference a municipal
      case number.  Can be very large ($100K+) and survive foreclosure.
    - Municipal liens (ML): Water/sewer/utility liens filed by the municipality.
      Typically survive all foreclosures as they run with the land.
    - Judgment liens (LN, FIN): Money judgments recorded against the owner.
      Expire 10 years from entry (renewable for 10 more).  Extinguished by
      homestead exemption (FL Art. X § 4) EXCEPT federal tax liens and
      certain others.
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


class LienType(StrEnum):
    GENERAL = "LN"
    CORP_TAX = "LNCORPTX"
    FINANCIAL = "FIN"
    MEDICAL = "MEDLN"
    HOA = "HOA"
    MECHANIC = "MECH"
    CODE_ENFORCEMENT = "CEL"
    SPECIAL_ASSESSMENT = "SPECASMT"
    MUNICIPAL = "ML"
    STATE_ASSESSMENT = "SA"
    OTHER = "OTHER"


class LienExtraction(BaseDocumentExtraction):
    """Structured extraction from a recorded lien instrument.

    **ORI doc type codes:** LN, LNCORPTX, FIN, MEDLN, HOA, MECH, CEL,
    SA, SPECASMT, ML

    **Normalization rules for the LLM:**

    - **Lienor (creditor)**: The entity asserting the claim.  For HOA liens,
      this is the association name.  For mechanic's liens, the contractor or
      subcontractor.  For code enforcement, the municipality.
    - **Lienee (debtor/owner)**: The property owner against whom the lien is
      filed.  Should match a recent grantee in the chain of title.
    - **Lien amount**: The dollar amount of the claim.  For HOA liens, this
      may include assessments, late fees, interest, and attorney fees.  For
      code enforcement, this is the accumulated fine amount.
    - **NOC reference** (mechanic's liens only): The Book/Page of the Notice
      of Commencement that authorized the construction work.  This is the
      critical cross-reference for linking mechanic's liens to NOCs.
    - **Municipal case number** (code enforcement only): The local violation
      or case number from the city/county code enforcement board.
    """

    # -- Lien-specific fields -----------------------------------------------

    lien_type: LienType | None = Field(
        default=None,
        description=(
            "Type of lien: LN = general/judgment lien, HOA = homeowners "
            "association assessment lien, MECH = mechanic's lien, "
            "CEL = code enforcement lien, ML = municipal lien, "
            "FIN = financial/money judgment, MEDLN = medical lien, "
            "SPECASMT = special assessment, SA = state assessment."
        ),
    )

    # -- Parties ------------------------------------------------------------

    lienor: str | None = Field(
        default=None,
        description=(
            "Full legal name of the lienor (creditor/claimant).  "
            "For HOA liens: the association's exact legal name.  "
            "For mechanic's liens: the contractor or supplier.  "
            "For code enforcement: 'CITY OF TAMPA', 'HILLSBOROUGH COUNTY', "
            "or 'CITY OF TEMPLE TERRACE'."
        ),
    )
    lienee: str | None = Field(
        default=None,
        description=(
            "Full legal name of the lienee (debtor/property owner) against "
            "whom the lien is filed.  Should match a grantee in the deed chain."
        ),
    )

    # -- Financial ----------------------------------------------------------

    lien_amount: float | None = Field(
        default=None,
        description=(
            "Total lien amount in dollars.  For HOA liens, includes "
            "assessments + late fees + interest + attorney fees if stated."
        ),
    )
    per_diem_rate: float | None = Field(
        default=None,
        description="Daily accrual rate in dollars, if stated.",
    )

    # -- Cross-references ---------------------------------------------------

    referenced_noc: RecordingReference | None = Field(
        default=None,
        description=(
            "For MECHANIC'S LIENS ONLY: Recording reference (Book/Page or "
            "Instrument Number) of the Notice of Commencement (NOC) that "
            "authorized the construction work.  Leave null for non-mechanic liens."
        ),
    )
    municipal_case_number: str | None = Field(
        default=None,
        description=(
            "For CODE ENFORCEMENT LIENS ONLY: The local municipal violation "
            "or case number from the code enforcement board (e.g. "
            "'CEB-2024-12345').  Leave null for non-code-enforcement liens."
        ),
    )
    declaration_reference: RecordingReference | None = Field(
        default=None,
        description=(
            "For HOA LIENS ONLY: Recording reference of the Declaration of "
            "Covenants, Conditions, and Restrictions (CC&Rs) that grants the "
            "HOA lien rights.  Leave null for non-HOA liens."
        ),
    )

    # -- Timestamps ---------------------------------------------------------

    delinquency_start_date: str | None = Field(
        default=None,
        description="Date the delinquency began, if stated (YYYY-MM-DD).",
    )

    # -- Validators ---------------------------------------------------------

    @field_validator("lien_type", mode="before")
    @classmethod
    def _coerce_type(cls, v: Any) -> Any:
        if isinstance(v, str):
            v = v.strip().upper()
            aliases = {
                "JUDGMENT LIEN": "LN",
                "JUDGMENT": "LN",
                "HOA LIEN": "HOA",
                "HOMEOWNER": "HOA",
                "MECHANIC'S LIEN": "MECH",
                "MECHANICS LIEN": "MECH",
                "CODE ENFORCEMENT": "CEL",
                "CODE ENFORCEMENT LIEN": "CEL",
                "MUNICIPAL LIEN": "ML",
                "MUNICIPAL": "ML",
                "SPECIAL ASSESSMENT": "SPECASMT",
                "MEDICAL LIEN": "MEDLN",
                "MEDICAL": "MEDLN",
                "FINANCIAL": "FIN",
                "LIEN": "LN",
            }
            v = aliases.get(v, v)
            try:
                return LienType(v)
            except ValueError:
                return v
        return v

    @field_validator("lien_amount", "per_diem_rate", mode="before")
    @classmethod
    def _clean_amounts(cls, v: Any) -> float | None:
        return parse_dollar(v)

    @field_validator("delinquency_start_date", mode="before")
    @classmethod
    def _clean_date(cls, v: Any) -> str | None:
        return normalize_date(v)
