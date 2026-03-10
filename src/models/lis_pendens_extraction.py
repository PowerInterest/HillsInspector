"""Pydantic schema for Lis Pendens extraction (LP, RELLP).

The lis pendens is the "Rosetta Stone" of foreclosure analysis.  It is filed
at the start of the lawsuit and recorded in the Official Records, providing:

1. The civil case number linking the O.R. to the court docket
2. The plaintiff (foreclosing party) and ALL defendants
3. The legal description of the property
4. The specific mortgage or lien being foreclosed (Book/Page reference)

Comparing the LP defendant list against the Final Judgment defendant list
reveals junior lienholders who were joined after filing — a critical signal
for survival analysis.

Key downstream consumers:
    - ``pg_ori_service``: LP instrument is the foreclosure's anchor document
    - ``pg_survival_service``: LP recording date is the priority cutoff
    - ``pg_title_chain_service``: LP defendants reveal all known interests
    - ``encumbrance_audit_signals``: LP-vs-FJ defendant delta analysis
    - ``pg_judgment_recovery_service``: LP has the real CA case number for
      CC cases where the auction PDF only shows a fee order

**Hillsborough County patterns:**
    - LPs are recorded same day the complaint is filed
    - The LP always references the mortgage/lien being foreclosed by Book/Page
      or Instrument Number — this is the PARENT INSTRUMENT
    - RELLP (Release of Lis Pendens) means the case was dismissed or settled
    - The defendant list on the LP may be shorter than the FJ because additional
      defendants are joined during litigation
"""

from __future__ import annotations

from typing import Any

from pydantic import Field, field_validator

from src.models.extraction_base import (
    BaseDocumentExtraction,
    RecordingReference,
    normalize_date,
)


class LisPendensExtraction(BaseDocumentExtraction):
    """Structured extraction from a recorded Lis Pendens.

    **ORI doc type codes:** LP, RELLP

    **Normalization rules for the LLM:**

    - **Plaintiff**: The party initiating the foreclosure.  Same rules as
      judgment extraction — exact legal name, all caps is fine.
    - **Defendants**: ALL parties named in the LP.  Unlike the judgment,
      the LP defendant list is the ORIGINAL filing — additional defendants
      may be added later.  Still capture every single one.
    - **Foreclosed instrument**: The Book/Page or Instrument Number of the
      mortgage or lien being foreclosed.  This is NOT the Plat Book — do
      not confuse "PB" (Plat Book) references from the legal description
      with "O.R. Book" references for the parent instrument.
    - **Civil case number**: The court case number (e.g. '2024-CA-012345').
      For CC (County Court) cases, this LP may be the only way to find the
      real CA case number through judgment recovery.
    - **Is release**: If this is a RELLP (Release of Lis Pendens), set
      ``is_release = true``.  This means the foreclosure was dismissed.
    """

    # -- LP-specific fields -------------------------------------------------

    is_release: bool = Field(
        default=False,
        description=(
            "True if this is a Release of Lis Pendens (RELLP), meaning "
            "the foreclosure case was dismissed, settled, or voluntarily "
            "dismissed.  False for the original LP filing."
        ),
    )

    # -- Parties ------------------------------------------------------------

    plaintiff: str | None = Field(
        default=None,
        description=(
            "Full legal name of the plaintiff (foreclosing party) exactly "
            "as it appears in the LP.  This should match the plaintiff in "
            "the Final Judgment."
        ),
    )
    defendants: list[str] = Field(
        default_factory=list,
        description=(
            "ALL defendants listed in the lis pendens.  Extract every name "
            "as a separate string in the list.  Include borrowers, spouses, "
            "co-borrowers, junior lienholders, HOAs, condo associations, "
            "'Unknown Tenant' placeholders, IRS, federal agencies, etc.  "
            "Do NOT include the presiding judge.  If the Clerk of Court is "
            "actually named as a party in the document, keep it."
        ),
    )

    # -- Case reference -----------------------------------------------------

    civil_case_number: str | None = Field(
        default=None,
        description=(
            "Court case number exactly as printed (e.g. '2024-CA-012345' or "
            "'2024-CC-054321').  This links the LP back to the court docket "
            "and to the foreclosure record in our database."
        ),
    )

    # -- Parent instrument --------------------------------------------------

    foreclosed_instrument: RecordingReference | None = Field(
        default=None,
        description=(
            "CRITICAL: Recording reference (Book/Page or Instrument Number) "
            "of the mortgage or lien being foreclosed.  Look for phrases "
            "like 'that certain mortgage recorded in O.R. Book X, Page Y' "
            "or 'Instrument No. XXXXXXXXXX'.  "
            "WARNING: Do NOT extract Plat Book (PB) references from the "
            "legal description here — those are property references, not "
            "lien references."
        ),
    )

    # -- Validators ---------------------------------------------------------

    @field_validator("execution_date", mode="before")
    @classmethod
    def _clean_exec_date(cls, v: Any) -> str | None:
        return normalize_date(v)
