"""Pydantic schema for Deed document extraction (D, WD, QCD, SWD, SD, PRD, CT, TAXDEED, DC).

Deeds are the primary instruments that transfer property ownership.  Extracting
them powers chain-of-title validation: each deed's grantee should be the next
deed's grantor.  Gaps in this chain reveal unrecorded transfers, probate issues,
or fraudulent conveyances.

Key downstream consumers:
    - ``pg_title_chain_service``: grantor/grantee for chain building
    - ``pg_ori_service``: legal description for property matching
    - ``pg_survival_service``: deed type determines lien survival rules
      (e.g. tax deeds extinguish most liens)
    - ``encumbrance_audit_signals``: "Subject to" clauses reveal assumed mortgages

**Hillsborough County patterns:**
    - Warranty Deeds (WD, SWD) are the most common residential transfer
    - Quit Claim Deeds (QCD) often appear in divorce, estate, or entity transfers
    - Certificates of Title (CT) result from foreclosure sales and reference
      the civil case number
    - Tax Deeds (TAXDEED) result from tax certificate sales and reference the
      tax certificate number
    - Personal Representative Deeds (PRD) indicate probate — the grantor is
      an estate representative
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


class DeedType(StrEnum):
    WARRANTY = "WD"
    SPECIAL_WARRANTY = "SWD"
    QUIT_CLAIM = "QCD"
    GENERAL = "D"
    PERSONAL_REP = "PRD"
    CERTIFICATE_OF_TITLE = "CT"
    TAX_DEED = "TAXDEED"
    DEED_OF_CORRECTION = "DC"
    SHERIFFS_DEED = "SD"
    OTHER = "OTHER"


class DeedExtraction(BaseDocumentExtraction):
    """Structured extraction from a recorded deed instrument.

    **ORI doc type codes:** D, WD, QCD, SWD, SD, PRD, CT, TAXDEED, DC

    **Normalization rules for the LLM:**

    - **Grantor (seller)**: The person/entity transferring ownership.  In a
      foreclosure Certificate of Title, the grantor is the Clerk of Court.
      In a Personal Representative Deed, it's the estate representative
      (e.g. "JOHN SMITH, as Personal Representative of the Estate of...").
    - **Grantee (buyer)**: The person/entity receiving ownership.  At a
      foreclosure sale, this is either the winning bidder or the plaintiff
      bank (if no third-party bids).
    - **"Subject to" clauses**: Critical — if the deed says "Subject to
      mortgage recorded in O.R. Book X, Page Y", that mortgage was ASSUMED
      by the buyer and survives the transfer.  Extract every such reference.
    - **Consideration**: The stated purchase price.  "$10.00" or "$10 and
      other good and valuable consideration" is nominal — still extract it.
    - **Documentary stamps**: Florida charges $0.70 per $100 of consideration.
      If doc stamps are stated, the true consideration = stamps / 0.007.
    """

    # -- Deed-specific fields -----------------------------------------------

    deed_type: DeedType | None = Field(
        default=None,
        description=(
            "Type of deed: WD = Warranty Deed, SWD = Special Warranty Deed, "
            "QCD = Quit Claim Deed, D = General Deed, PRD = Personal "
            "Representative Deed, CT = Certificate of Title (foreclosure), "
            "TAXDEED = Tax Deed, DC = Deed of Correction, SD = Sheriff's Deed."
        ),
    )

    # -- Parties ------------------------------------------------------------

    grantor: str | None = Field(
        default=None,
        description=(
            "Full legal name of the grantor (seller/transferor).  "
            "In a Certificate of Title, this is typically 'CLERK OF THE "
            "CIRCUIT COURT' or 'PAT FRANK, CLERK'.  In a PRD, include the "
            "representative capacity (e.g. 'JANE DOE, as Personal "
            "Representative of the Estate of JOHN DOE')."
        ),
    )
    grantee: str | None = Field(
        default=None,
        description=(
            "Full legal name of the grantee (buyer/recipient).  If multiple "
            "grantees, list all separated by ' AND ' or ' ; '.  Note tenancy "
            "type if stated (e.g. 'as joint tenants with right of survivorship')."
        ),
    )
    consideration: float | None = Field(
        default=None,
        description=(
            "Stated consideration / purchase price in dollars.  Nominal "
            "consideration ('$10.00 and other good and valuable consideration') "
            "should be extracted as 10.00."
        ),
    )
    documentary_stamps: float | None = Field(
        default=None,
        description=(
            "Documentary stamp tax paid, in dollars.  Florida rate is $0.70 "
            "per $100 of consideration.  True consideration = stamps / 0.007."
        ),
    )

    # -- Cross-references ---------------------------------------------------

    assumed_encumbrances: str | None = Field(
        default=None,
        description=(
            "Any explicit 'Subject to...' clauses referencing existing "
            "mortgages or liens the property is being transferred subject to.  "
            "Extract the full clause including any Book/Page or Instrument "
            "Number references.  These encumbrances SURVIVED the transfer."
        ),
    )
    assumed_encumbrance_refs: list[RecordingReference] = Field(
        default_factory=list,
        description=(
            "Structured recording references for each assumed encumbrance "
            "mentioned in a 'Subject to' clause.  Extract Book/Page and/or "
            "Instrument Number for each one."
        ),
    )
    related_case_number: str | None = Field(
        default=None,
        description=(
            "For Certificates of Title (CT) or Sheriff's Deeds (SD): the "
            "civil case number that generated this deed (e.g. '2024-CA-012345').  "
            "For Tax Deeds: the tax certificate number."
        ),
    )

    # -- Validators ---------------------------------------------------------

    @field_validator("deed_type", mode="before")
    @classmethod
    def _coerce_deed_type(cls, v: Any) -> Any:
        if isinstance(v, str):
            v = v.strip().upper()
            aliases = {
                "WARRANTY DEED": "WD",
                "SPECIAL WARRANTY DEED": "SWD",
                "QUIT CLAIM DEED": "QCD",
                "QUITCLAIM DEED": "QCD",
                "PERSONAL REPRESENTATIVE DEED": "PRD",
                "CERTIFICATE OF TITLE": "CT",
                "TAX DEED": "TAXDEED",
                "DEED OF CORRECTION": "DC",
                "SHERIFF'S DEED": "SD",
                "SHERIFFS DEED": "SD",
                "DEED": "D",
            }
            v = aliases.get(v, v)
            try:
                return DeedType(v)
            except ValueError:
                return v
        return v

    @field_validator("consideration", "documentary_stamps", mode="before")
    @classmethod
    def _clean_amounts(cls, v: Any) -> float | None:
        return parse_dollar(v)

    @field_validator("execution_date", mode="before")
    @classmethod
    def _clean_exec_date(cls, v: Any) -> str | None:
        return normalize_date(v)
