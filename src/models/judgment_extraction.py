"""Pydantic schema for Final Judgment of Foreclosure extraction (JUD, FJ, CCJ).

This module defines the canonical structured output schema for extracting data
from Florida Final Judgment of Foreclosure PDFs.  It serves three purposes:

1. **LLM prompt schema** — ``JudgmentExtraction.model_json_schema()`` generates
   the JSON Schema embedded in the extraction prompt so the LLM knows exactly
   what to produce.

2. **Output validation** — ``JudgmentExtraction.model_validate(raw_dict)``
   coerces and validates LLM output before it reaches the database.  Bad dates,
   non-numeric amounts, and unknown enum values are caught here rather than
   silently corrupting ``foreclosures.judgment_data``.

3. **Type safety** — Downstream consumers (``pg_judgment_service``,
   ``pg_survival_service``, ``pg_ori_service``, ``properties.py`` router,
   Jinja templates) can import these models for IDE autocompletion and static
   analysis instead of reaching into untyped dicts.

The field names match the existing ``*_extracted.json`` files exactly — no
renames — so adopting this schema is a drop-in replacement.

Downstream field access map (why each field exists):
    - ``pg_judgment_service``: total_judgment_amount, recording_date,
      instrument_number, parcel_id, legal_description
    - ``pg_ori_service``: legal_description, subdivision, lot, block, unit,
      parcel_id, plaintiff, defendant (first defendant name), filing_date,
      judgment_date, judgment_amount
    - ``pg_survival_service``: foreclosed_mortgage.instrument_number/book/page
      (→ foreclosing_refs), case_number, plaintiff, defendants, lis_pendens,
      foreclosure_type
    - ``survival_service``: plaintiff, case_number, lis_pendens,
      lis_pendens_date, defendants, foreclosing_refs, foreclosure_type
    - ``properties.py``: All financial fields, plaintiff, defendants,
      foreclosure_type, lis_pendens, legal_description, property_address,
      parcel_id, plaintiff_maximum_bid, per_diem_rate
    - ``judgment.html``: confidence_score, foreclosed_mortgage.*,
      original_lender, current_holder, red_flags, per_diem_rate/per_diem_interest
    - ``audit_signals``: defendants, plaintiff, legal_description,
      property_address, lis_pendens, judgment_date
"""

from __future__ import annotations

from enum import StrEnum
import re
from typing import Any

from pydantic import Field, field_validator, model_validator

from src.models.extraction_base import (
    HILLSBOROUGH_JUDGES,
    BaseDocumentExtraction,
    StrictExtractionModel,
    normalize_date,
    normalize_party_name,
    parse_dollar,
)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

_CREDIT_AMOUNT_RE = re.compile(
    r"(?P<prefix>-?\$|\(\$|\$ ?\()?\s*"
    r"(?P<amount>\d{1,3}(?:,\d{3})*(?:\.\d{2})|\d+\.\d{2})\)?",
    re.IGNORECASE,
)
_CREDIT_LINE_MARKERS = (
    "LESS PAYMENTS",
    "PAYMENTS RECEIVED",
    "LESS CREDITS",
    "ESCROW CREDIT",
    "UNAPPLIED FUNDS",
    "MISCELLANEOUS DEDUCTIONS",
    "LESS:",
)
_ATTORNEY_FEE_LINE_RE = re.compile(
    r"^\s*(?:ADDITIONAL\s+)?ATTORNEY[’']?S?\s+FEES?\s+\$?(?P<amount>\d{1,3}(?:,\d{3})*(?:\.\d{2})|\d+\.\d{2})\s*$",
    re.IGNORECASE,
)
_PER_DIEM_INTEREST_GOOD_THROUGH_RE = re.compile(
    r"PER\s+DIEM\s+INTEREST\b.*?\bGOOD\s+THROUGH\b.*?\$?(?P<amount>\d{1,3}(?:,\d{3})*(?:\.\d{2})|\d+\.\d{2})",
    re.IGNORECASE,
)
_AMOUNT_LINE_RE = re.compile(
    r"\b(?P<label>[A-Z][A-Z0-9/&'().,\- ]{2,}?)\s*:?\s*\$?(?P<amount>\d{1,3}(?:,\d{3})*(?:\.\d{2})|\d+\.\d{2})\b",
    re.IGNORECASE,
)
_CORPORATE_ADVANCE_EMBEDDED_LABELS = (
    "PROBATE REVIEW",
    "DEATH CERTIFICATE",
    "SKIP TRACE",
    "LIS PENDENS",
    "COMPLAINT FILING FEE",
    "CLERK SUMMONS",
    "PUBLICATION",
    "SERVICE OF PROCESS",
    "ATTENDANCE AT COURT",
    "DOCUMENT PREPARATION",
    "MOTIONS FOR AMENDED COMPLAINT",
    "FLAT FEE ALREADY PAID OUT",
    "REMAINING CORPORATE ADVANCES",
)


def _parse_credit_amount(raw_amount: str) -> float:
    return float(raw_amount.replace(",", ""))


def extract_credit_adjustments(raw_text: str) -> float:
    """Sum document-stated credits/payments that reduce the total due.

    Final judgments often present the amount table as:

    - major line items (principal, interest, fees, costs)
    - one or more explicit credits/payments (``less payments received``,
      ``less escrow credits``, ``unapplied funds``)
    - the final total

    The LLM frequently extracts the positive line items but omits the
    subtractive credit line.  We do not want to weaken the arithmetic gate, so
    this helper recovers the credit directly from OCR text and lets the
    validator do the math on the complete formula.
    """
    if not raw_text:
        return 0.0

    adjustments: list[float] = []
    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]

    for idx, line in enumerate(lines):
        upper = line.upper()
        for marker in _CREDIT_LINE_MARKERS:
            pos = upper.find(marker)
            if pos == -1:
                continue
            segment = " ".join(lines[idx : idx + 2])
            tail = segment[pos:]
            match = _CREDIT_AMOUNT_RE.search(tail)
            if match:
                adjustments.append(_parse_credit_amount(match.group("amount")))
            break
        else:
            if "SUB-TOTAL" in upper or "SUBTOTAL" in upper:
                for candidate in lines[idx + 1 : idx + 3]:
                    if not ("(" in candidate or "-$" in candidate):
                        continue
                    for match in _CREDIT_AMOUNT_RE.finditer(candidate):
                        adjustments.append(_parse_credit_amount(match.group("amount")))

    return round(sum(adjustments), 2)


def extract_authoritative_attorney_fees(raw_text: str) -> float | None:
    """Recover the top-level attorney fee total from OCR text when duplicated.

    Some judgments list one authoritative attorney-fee total followed by
    subcomponents, for example:

    - ``Attorney's Fees $20,146.50``
    - ``Attorney's fees $5,400.00``
    - ``Additional Attorney's fees $14,746.50``

    The LLM sometimes sums the total and the subcomponents together. When the
    largest fee line equals the sum of the remaining fee lines, treat that
    largest line as the authoritative fee total.
    """
    if not raw_text:
        return None

    fee_lines: list[float] = []
    for line in raw_text.splitlines():
        match = _ATTORNEY_FEE_LINE_RE.match(line.strip())
        if match:
            fee_lines.append(_parse_credit_amount(match.group("amount")))

    if len(fee_lines) < 2:
        return None

    rounded = [round(value, 2) for value in fee_lines]
    largest = max(rounded)
    remaining_sum = round(sum(rounded) - largest, 2)
    if abs(remaining_sum - largest) <= 0.5:
        return largest

    if len(set(rounded)) == 1:
        return rounded[0]

    return None


def extract_accrued_per_diem_interest(raw_text: str) -> float | None:
    """Recover accrued per-diem interest totals misread as daily rates."""
    if not raw_text:
        return None

    for line in raw_text.splitlines():
        upper = line.upper()
        if "PER DIEM INTEREST" not in upper or "GOOD THROUGH" not in upper:
            continue
        match = _PER_DIEM_INTEREST_GOOD_THROUGH_RE.search(line)
        if match:
            return _parse_credit_amount(match.group("amount"))
    return None


def extract_embedded_corporate_advance_total(raw_text: str) -> float | None:
    """Sum embedded cost lines when a judgment itemizes a Corporate Advances subtotal.

    Some Hillsborough mortgage judgments present ``Corporate Advances`` as a
    subtotal and then continue with the underlying filing/service/detail lines
    on the next page. When the LLM also extracts those detail lines into
    ``court_costs``, adding both against the final total double-counts the same
    dollars. This helper recognizes the known embedded breakdown labels and
    returns their summed total so validation can suppress the duplicated
    ``court_costs`` field when the subtotal matches.
    """
    if not raw_text or "CORPORATE ADVANCES" not in raw_text.upper():
        return None

    label_totals: dict[str, float] = {}
    for line in raw_text.splitlines():
        match = _AMOUNT_LINE_RE.search(line.strip())
        if not match:
            continue
        label = re.sub(r"\s+", " ", match.group("label").upper()).strip(" :")
        if label not in _CORPORATE_ADVANCE_EMBEDDED_LABELS:
            continue
        label_totals.setdefault(label, _parse_credit_amount(match.group("amount")))

    if len(label_totals) < 4:
        return None

    return round(sum(label_totals.values()), 2)

class PlaintiffType(StrEnum):
    BANK = "bank"
    SERVICER = "servicer"
    TRUST = "trust"
    GSE = "gse"
    HOA = "hoa"
    PRIVATE_LENDER = "private_lender"
    OTHER = "other"


class PartyType(StrEnum):
    BORROWER = "borrower"
    CO_BORROWER = "co_borrower"
    SPOUSE = "spouse"
    SECOND_MORTGAGE_HOLDER = "second_mortgage_holder"
    JUDGMENT_CREDITOR = "judgment_creditor"
    HOA = "hoa"
    CONDO_ASSOCIATION = "condo_association"
    IRS = "irs"
    FEDERAL_AGENCY = "federal_agency"
    MUNICIPALITY = "municipality"
    TENANT = "tenant"
    UNKNOWN = "unknown"


class ForeclosureType(StrEnum):
    FIRST_MORTGAGE = "FIRST MORTGAGE"
    SECOND_MORTGAGE = "SECOND MORTGAGE"
    HOA = "HOA"
    CONDO = "CONDO"
    TAX = "TAX"
    OTHER = "OTHER"


class RedFlagType(StrEnum):
    FEDERAL_DEFENDANT = "federal_defendant"
    LOST_NOTE = "lost_note"
    DECEASED_BORROWER = "deceased_borrower"
    SERVICE_ISSUE = "service_issue"
    MISSING_HOA_DEFENDANT = "missing_hoa_defendant"


class Severity(StrEnum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"


# ---------------------------------------------------------------------------
# Judgment-specific nested models
# ---------------------------------------------------------------------------

class Defendant(StrictExtractionModel):
    """A named defendant in the foreclosure action.

    **Hillsborough County normalization rules:**

    - The case caption lists the presiding judge (e.g. Cheryl Thomas, Victor
      Crist, Lisa Campbell, etc.) - these are NOT defendants.  Never include
      judge names in this list.
    - ``UNKNOWN TENANT #1``, ``UNKNOWN TENANT #2`` etc. are generic occupant
      placeholders.  Still capture them but type as ``tenant``.
    - ``A/K/A`` or ``F/K/A`` indicate name aliases for the same person —
      include the full string as one defendant, not multiple entries.
    - Banks appearing as defendants (Wells Fargo, Chase, etc.) are typically
      junior lienholders — type as ``second_mortgage_holder`` or
      ``judgment_creditor``.
    - HOA/COA names typically end with "Association, Inc." — type as ``hoa``
      or ``condo_association``.
    - Federal entities (IRS, United States of America, HUD, VA, FHA) trigger
      extended redemption rights - set ``is_federal_entity = true``.
    """

    name: str = Field(
        ...,
        description=(
            "Full legal name of the defendant exactly as it appears in the "
            "judgment.  Include 'A/K/A' aliases as part of the name string."
        ),
    )
    party_type: PartyType = Field(
        default=PartyType.UNKNOWN,
        description=(
            "Role of this defendant.  borrower = primary obligor on the note, "
            "co_borrower = co-signer, spouse = non-borrowing spouse, "
            "second_mortgage_holder = holder of a junior mortgage, "
            "judgment_creditor = holder of a money judgment lien, "
            "hoa = homeowners association, "
            "condo_association = condominium association, "
            "irs = Internal Revenue Service, "
            "federal_agency = any other US federal entity (FHA, VA, HUD), "
            "municipality = city/county government, "
            "tenant = occupant or unknown tenant, "
            "unknown = cannot determine role."
        ),
    )
    is_federal_entity: bool = Field(
        default=False,
        description=(
            "True if this defendant is a US federal government entity "
            "(IRS, United States of America, FHA, VA, HUD, US Army, etc.).  "
            "Federal defendants trigger extended redemption rights under "
            "28 USC § 2410.  CRITICAL for investment analysis."
        ),
    )
    is_deceased: bool = Field(
        default=False,
        description=(
            "True if the judgment indicates this defendant is deceased "
            "(look for 'deceased', 'estate of', 'personal representative')."
        ),
    )
    lien_recording_reference: str | None = Field(
        default=None,
        description=(
            "Recording reference (Book/Page or Instrument Number) for any lien "
            "this defendant holds against the property, if stated in the judgment."
        ),
    )

    @field_validator("party_type", mode="before")
    @classmethod
    def _coerce_party_type(cls, v: Any) -> Any:
        if isinstance(v, str):
            v = v.strip().lower().replace(" ", "_")
            try:
                return PartyType(v)
            except ValueError:
                return v
        return v


class ForeclosedMortgage(StrictExtractionModel):
    """Recording details for the mortgage being foreclosed.

    This is the PARENT LIEN — the instrument whose default triggered the
    foreclosure.  The instrument_number or Book/Page here is used by the
    survival service to match the foreclosing lien in ori_encumbrances and
    mark it as FORECLOSING (all other liens are then ranked relative to it).

    Common patterns in Hillsborough County:
    - Modern recordings (post-2000): instrument_number is a 10-digit number
    - Older recordings: Book/Page only (e.g. Book 12345, Page 678)
    - Assignments may change the holder but the recording ref stays the same
    """

    original_date: str | None = Field(
        default=None,
        description="Date the original mortgage was executed/signed (YYYY-MM-DD).",
    )
    original_amount: float | None = Field(
        default=None,
        description=(
            "Original principal amount of the mortgage in dollars.  "
            "This is the amount on the note, NOT the judgment amount."
        ),
    )
    recording_date: str | None = Field(
        default=None,
        description="Date the mortgage was recorded in the Official Records (YYYY-MM-DD).",
    )
    recording_book: str | None = Field(
        default=None,
        description="Official Records book number where the mortgage is recorded.",
    )
    recording_page: str | None = Field(
        default=None,
        description="Official Records page number where the mortgage is recorded.",
    )
    instrument_number: str | None = Field(
        default=None,
        description="Official Records instrument number for the recorded mortgage.",
    )
    original_lender: str | None = Field(
        default=None,
        description=(
            "Name of the original mortgage lender as stated in the judgment "
            "(e.g. 'COUNTRYWIDE HOME LOANS').  May differ from plaintiff if "
            "the mortgage was assigned."
        ),
    )
    current_holder: str | None = Field(
        default=None,
        description=(
            "Current holder/servicer of the mortgage if different from the "
            "original lender (often the plaintiff, but sometimes a trust "
            "like 'DEUTSCHE BANK AS TRUSTEE FOR...')."
        ),
    )

    @field_validator("original_amount", mode="before")
    @classmethod
    def _clean_amount(cls, v: Any) -> float | None:
        return parse_dollar(v)

    @field_validator("original_date", "recording_date", mode="before")
    @classmethod
    def _clean_date(cls, v: Any) -> str | None:
        return normalize_date(v)


class LisPendens(StrictExtractionModel):
    """Recording details for the lis pendens that initiated the foreclosure.

    The lis pendens (LP) is filed when the lawsuit begins and recorded in the
    Official Records.  Its instrument_number or Book/Page is used to match
    LP records in ori_encumbrances.  In Hillsborough County, LPs are typically
    recorded the same day the complaint is filed.
    """

    recording_date: str | None = Field(
        default=None,
        description="Date the lis pendens was recorded (YYYY-MM-DD).",
    )
    recording_book: str | None = Field(
        default=None,
        description="Official Records book number for the lis pendens.",
    )
    recording_page: str | None = Field(
        default=None,
        description="Official Records page number for the lis pendens.",
    )
    instrument_number: str | None = Field(
        default=None,
        description="Official Records instrument number for the lis pendens.",
    )

    @field_validator("recording_date", mode="before")
    @classmethod
    def _clean_date(cls, v: Any) -> str | None:
        return normalize_date(v)


class RedFlag(StrictExtractionModel):
    """A risk indicator found in the judgment document."""

    flag_type: RedFlagType = Field(
        ...,
        description=(
            "Category: federal_defendant = US government party with redemption "
            "rights, lost_note = affidavit of lost/destroyed note, "
            "deceased_borrower = borrower is dead, service_issue = defective "
            "service of process, missing_hoa_defendant = HOA/condo association "
            "not named as defendant."
        ),
    )
    severity: Severity = Field(
        ...,
        description="Impact level: critical, high, or medium.",
    )
    description: str = Field(
        ...,
        description="Plain-English explanation of the risk and its investment impact.",
    )

    @field_validator("flag_type", mode="before")
    @classmethod
    def _coerce_flag_type(cls, v: Any) -> Any:
        if isinstance(v, str):
            v = v.strip().lower().replace(" ", "_")
            try:
                return RedFlagType(v)
            except ValueError:
                return v
        return v

    @field_validator("severity", mode="before")
    @classmethod
    def _coerce_severity(cls, v: Any) -> Any:
        if isinstance(v, str):
            v = v.strip().lower()
            try:
                return Severity(v)
            except ValueError:
                return v
        return v


# ---------------------------------------------------------------------------
# Root extraction model
# ---------------------------------------------------------------------------

class JudgmentExtraction(BaseDocumentExtraction):
    """Structured extraction from a Florida Final Judgment of Foreclosure.

    **ORI doc type codes:** JUD, FJ, CCJ, DRJUD, CTF

    Every field name matches the existing ``*_extracted.json`` schema so
    downstream consumers require zero changes.

    **Document context for the LLM:**

    A Final Judgment of Foreclosure is a court order from the 13th Judicial
    Circuit (Hillsborough County, Florida) that:

    1. Establishes the total amount owed on a defaulted mortgage/lien
    2. Orders the property sold at public auction
    3. Lists ALL defendants whose interests are affected (missing a defendant
       means their lien survives — this is CRITICAL)
    4. References the original mortgage/lien being foreclosed (Book/Page or
       instrument number)
    5. May reference the lis pendens that started the case
    6. Sets the foreclosure sale date and location

    **Normalization rules:**

    - Dollar amounts: Extract as numbers with 2 decimal places.
      "$123,456.78" → 123456.78.  If amount is unclear, use null.
    - Dates: Always YYYY-MM-DD.  If only month/year, use first of month.
    - Party names: Exact transcription, ALL CAPS is fine.
    - Legal description: VERBATIM — every word, number, abbreviation.
      Exhibit A / Schedule A versions take priority over inline summaries.
    """

    # -- Case & court -------------------------------------------------------

    case_number: str | None = Field(
        default=None,
        description=(
            "Court case number exactly as printed (e.g. '2024-CA-012345').  "
            "CA = Circuit Civil, CC = County Court.  This links the judgment "
            "back to the foreclosure record in our database."
        ),
    )
    court_circuit: str | None = Field(
        default=None,
        description="Judicial circuit number (always '13th' for Hillsborough County).",
    )
    county: str | None = Field(
        default=None,
        description="County name (always 'Hillsborough' for our pipeline).",
    )
    judge_name: str | None = Field(
        default=None,
        description=(
            "Name of the presiding judge who signed the judgment.  "
            "This is NOT a defendant — do not include in the defendants list."
        ),
    )
    judgment_date: str | None = Field(
        default=None,
        description="Date the judgment was entered by the court (YYYY-MM-DD).",
    )

    # -- Parties ------------------------------------------------------------

    plaintiff: str = Field(
        ...,
        description=(
            "Full legal name of the foreclosing party (bank, servicer, trust, "
            "HOA, etc.) exactly as it appears in the case caption.  "
            "Common Hillsborough County plaintiffs include: Wells Fargo, "
            "JPMorgan Chase, Bank of America, Nationstar/Mr. Cooper, "
            "Navy Federal Credit Union, Pennymac, Freedom Mortgage."
        ),
    )
    plaintiff_type: PlaintiffType | None = Field(
        default=None,
        description=(
            "Entity category: bank = commercial bank/credit union, "
            "servicer = loan servicer (Nationstar, Mr. Cooper, Shellpoint), "
            "trust = securitization trust ('as trustee for...'), "
            "gse = government-sponsored enterprise (Fannie Mae, Freddie Mac), "
            "hoa = homeowners/condo association, "
            "private_lender = individual or private company."
        ),
    )
    defendants: list[Defendant] = Field(
        default_factory=list,
        description=(
            "EVERY defendant named in the judgment — this is the single most "
            "important list in the document.  A missing defendant means their "
            "lien SURVIVES the foreclosure sale.  Capture ALL: borrowers, "
            "spouses, co-borrowers, junior mortgage holders (banks), HOAs, "
            "condo associations, judgment creditors, 'Unknown Tenant #1/#2', "
            "IRS, United States of America, state agencies, municipalities.  "
            "Do NOT include the presiding judge."
        ),
    )

    # -- Property identification (extends base) -----------------------------

    subdivision: str | None = Field(
        default=None,
        description=(
            "Subdivision name extracted from the legal description.  This "
            "is the proper name of the platted subdivision (e.g. 'HYDE PARK', "
            "'CROSS CREEK UNIT 2', 'SYMPHONY ISLES #2').  Include phase or "
            "unit qualifiers that are part of the subdivision name (e.g. "
            "'NORTHDALE SECTION B UNIT NO 2' where 'UNIT NO 2' is a phase).  "
            "Preserve apostrophes ('TURMAN\\'S EAST YBOR').  Do NOT include "
            "'ACCORDING TO THE PLAT...' boilerplate."
        ),
    )
    lot: str | None = Field(
        default=None,
        description=(
            "Lot number(s) from the legal description.  For single lots: '5'.  "
            "For multiple lots: '1, 2 AND 3'.  For alphanumeric: '5A'.  "
            "Include partial lot language if present (e.g. '11 AND THE WEST "
            "15.00 FEET OF LOT 12')."
        ),
    )
    block: str | None = Field(
        default=None,
        description=(
            "Block identifier from the legal description.  Can be numeric "
            "('30') or alphabetic ('D', 'BB').  Extract exactly as printed."
        ),
    )
    unit: str | None = Field(
        default=None,
        description=(
            "Condo/townhouse unit number ONLY.  Extract this ONLY for "
            "condominium properties where there is no Lot/Block.  Do NOT "
            "extract subdivision phase numbers here (e.g. in 'NORTHDALE "
            "SECTION B UNIT NO 2', the '2' is a phase, not a condo unit)."
        ),
    )
    plat_book: str | None = Field(
        default=None,
        description=(
            "PLAT Book number from the legal description.  This references "
            "the subdivision plat map, NOT the Official Records book.  "
            "Look for 'PLAT BOOK X' or 'PB X' in the legal.  Do NOT "
            "confuse with 'O.R. BOOK' which is a recording reference."
        ),
    )
    plat_page: str | None = Field(
        default=None,
        description=(
            "PLAT Page number from the legal description.  Accompanies "
            "plat_book.  Look for 'PAGE Y' or 'PG Y' following 'PLAT BOOK'."
        ),
    )
    is_condo: bool = Field(
        default=False,
        description=(
            "True if the property is a condominium unit.  Indicators: "
            "'CONDOMINIUM' in the legal description, a UNIT number without "
            "LOT/BLOCK, or a 'Declaration of Condominium' reference."
        ),
    )

    # -- Recording references -----------------------------------------------

    foreclosed_mortgage: ForeclosedMortgage | None = Field(
        default=None,
        description=(
            "Recording details for the mortgage being foreclosed.  Extract "
            "Book/Page OR Instrument Number, original date, and original "
            "amount.  These are CRITICAL for matching the foreclosing lien "
            "in the Official Records Index.  Look for phrases like "
            "'that certain mortgage recorded in O.R. Book X, Page Y' or "
            "'Instrument No. XXXXXXXXXX'."
        ),
    )
    lis_pendens: LisPendens | None = Field(
        default=None,
        description=(
            "Recording details for the lis pendens that initiated this "
            "foreclosure action.  Often referenced as 'Notice of Lis Pendens "
            "recorded in O.R. Book X, Page Y'."
        ),
    )

    # -- Financial breakdown ------------------------------------------------

    principal_amount: float | None = Field(
        default=None,
        description="Unpaid principal balance in dollars.",
    )
    interest_amount: float | None = Field(
        default=None,
        description="Accrued interest amount in dollars.",
    )
    interest_through_date: str | None = Field(
        default=None,
        description=(
            "Date through which the interest_amount was calculated (YYYY-MM-DD).  "
            "Per diem accrues from this date to the auction date."
        ),
    )
    per_diem_rate: float | None = Field(
        default=None,
        description=(
            "Daily interest accrual rate in dollars (NOT a percentage).  "
            "Used to calculate: additional_interest = per_diem_rate × "
            "(auction_date - interest_through_date).  Typical range: "
            "$2–$100/day depending on loan size."
        ),
    )
    per_diem_interest: float | None = Field(
        default=None,
        description=(
            "Total additional interest accrued at the per_diem_rate, in "
            "dollars, if the judgment states that computed amount separately.  "
            "Do not confuse this with the daily rate itself."
        ),
    )
    late_charges: float | None = Field(
        default=None,
        description="Late fees / late charges in dollars.",
    )
    escrow_advances: float | None = Field(
        default=None,
        description=(
            "Escrow advances (taxes, insurance paid by lender on borrower's "
            "behalf) in dollars."
        ),
    )
    title_search_costs: float | None = Field(
        default=None,
        description="Title search / abstract costs in dollars.",
    )
    court_costs: float | None = Field(
        default=None,
        description="Court filing fees and costs in dollars.",
    )
    attorney_fees: float | None = Field(
        default=None,
        description="Attorney fees awarded in dollars.",
    )
    other_costs: float | None = Field(
        default=None,
        description="Any other costs not categorized above, in dollars.",
    )
    total_judgment_amount: float | None = Field(
        default=None,
        description=(
            "Total judgment amount — the sum of all amounts above.  This is "
            "the amount the court orders the defendant to pay.  Stored in "
            "the database as final_judgment_amount.  Typical range for "
            "Hillsborough County: $20,000–$500,000."
        ),
    )

    # -- Sale details -------------------------------------------------------

    foreclosure_sale_date: str | None = Field(
        default=None,
        description="Scheduled date of the foreclosure sale (YYYY-MM-DD).",
    )
    sale_location: str | None = Field(
        default=None,
        description=(
            "URL or physical address where the sale will be held.  "
            "Hillsborough County sales are typically at "
            "http://www.hillsborough.realforeclose.com"
        ),
    )
    is_online_sale: bool = Field(
        default=False,
        description="True if the sale is conducted online (realforeclose.com).",
    )

    # -- Foreclosure classification -----------------------------------------

    foreclosure_type: ForeclosureType | None = Field(
        default=None,
        description=(
            "Type of foreclosure: FIRST MORTGAGE (most common — bank "
            "foreclosing primary mortgage), SECOND MORTGAGE (junior lien "
            "foreclosure), HOA (homeowners association lien), CONDO "
            "(condominium association lien), TAX (tax certificate/deed), "
            "OTHER (anything else)."
        ),
    )
    hoa_safe_harbor_mentioned: bool = Field(
        default=False,
        description=(
            "True if the judgment references the HOA safe harbor provision "
            "(FL Stat § 718.116 for condos or § 720.3085 for HOAs).  "
            "This limits the HOA's claim to 12 months or 1% of original "
            "mortgage, whichever is less."
        ),
    )
    superiority_language: str | None = Field(
        default=None,
        description=(
            "Exact quote from the judgment about lien priority / superiority.  "
            "Look for: 'Plaintiff's lien is superior to...', 'first and "
            "paramount lien...', 'senior in priority to...'."
        ),
    )
    plaintiff_maximum_bid: float | None = Field(
        default=None,
        description=(
            "Plaintiff's maximum bid amount if stated in the judgment.  "
            "This caps what the plaintiff will bid at auction.  If the "
            "max bid is below the judgment amount, the property may sell "
            "for less than owed."
        ),
    )

    # -- Additional fields --------------------------------------------------

    monthly_payment: float | None = Field(
        default=None,
        description="Monthly mortgage payment amount if stated in the judgment.",
    )
    default_date: str | None = Field(
        default=None,
        description="Date of first default / breach by the borrower (YYYY-MM-DD).",
    )
    service_by_publication: bool = Field(
        default=False,
        description=(
            "True if any defendant was served by publication (constructive "
            "service).  This is a potential due process issue — flag it."
        ),
    )

    # -- Red flags ----------------------------------------------------------

    red_flags: list[RedFlag] = Field(
        default_factory=list,
        description=(
            "Risk indicators found in the judgment.  Flag: federal defendants "
            "(extended redemption rights), lost note affidavits (chain of "
            "title risk), deceased borrowers (probate complications), "
            "service defects (due process risk), missing HOA defendants "
            "(potential surviving assessment lien)."
        ),
    )

    # -------------------------------------------------------------------
    # Validators
    # -------------------------------------------------------------------

    @field_validator("plaintiff_type", mode="before")
    @classmethod
    def _coerce_plaintiff_type(cls, v: Any) -> Any:
        if isinstance(v, str):
            v = v.strip().lower().replace(" ", "_")
            try:
                return PlaintiffType(v)
            except ValueError:
                return v
        return v

    @field_validator("foreclosure_type", mode="before")
    @classmethod
    def _coerce_foreclosure_type(cls, v: Any) -> Any:
        if isinstance(v, str):
            v = v.strip().upper()
            aliases = {
                "FIRST": "FIRST MORTGAGE",
                "SECOND": "SECOND MORTGAGE",
                "FIRST_MORTGAGE": "FIRST MORTGAGE",
                "SECOND_MORTGAGE": "SECOND MORTGAGE",
                "HOMEOWNER": "HOA",
                "HOMEOWNERS": "HOA",
                "CONDOMINIUM": "CONDO",
                "TAX DEED": "TAX",
                "TAX_DEED": "TAX",
            }
            v = aliases.get(v, v)
            try:
                return ForeclosureType(v)
            except ValueError:
                return v
        return v

    @field_validator(
        "judgment_date",
        "interest_through_date",
        "foreclosure_sale_date",
        "default_date",
        mode="before",
    )
    @classmethod
    def _clean_dates(cls, v: Any) -> str | None:
        return normalize_date(v)

    @field_validator(
        "principal_amount",
        "interest_amount",
        "per_diem_rate",
        "per_diem_interest",
        "late_charges",
        "escrow_advances",
        "title_search_costs",
        "court_costs",
        "attorney_fees",
        "other_costs",
        "total_judgment_amount",
        "monthly_payment",
        "plaintiff_maximum_bid",
        mode="before",
    )
    @classmethod
    def _clean_amounts(cls, v: Any) -> float | None:
        return parse_dollar(v)

    @model_validator(mode="after")
    def _clean_empty_nested(self) -> JudgmentExtraction:
        """Convert all-None nested objects to None."""
        fm = self.foreclosed_mortgage
        if fm is not None and not any([
            fm.original_date, fm.original_amount, fm.recording_date,
            fm.recording_book, fm.recording_page, fm.instrument_number,
            fm.original_lender, fm.current_holder,
        ]):
            self.foreclosed_mortgage = None
        lp = self.lis_pendens
        if lp is not None and not any([
            lp.recording_date, lp.recording_book,
            lp.recording_page, lp.instrument_number,
        ]):
            self.lis_pendens = None
        return self

    @model_validator(mode="after")
    def _strip_judge_from_defendants(self) -> JudgmentExtraction:
        """Remove the presiding judge if the LLM accidentally included them."""
        if not self.judge_name or not self.defendants:
            return self
        judge_norm = normalize_party_name(self.judge_name)
        judge_set = {normalize_party_name(j) for j in HILLSBOROUGH_JUDGES}
        judge_set.add(judge_norm)
        self.defendants = [
            d for d in self.defendants
            if normalize_party_name(d.name) not in judge_set
        ]
        return self

    @model_validator(mode="after")
    def _dedupe_defendants(self) -> JudgmentExtraction:
        """Collapse exact duplicate defendants while preserving richer metadata."""
        if not self.defendants:
            return self
        deduped: dict[str, Defendant] = {}
        order: list[str] = []
        for defendant in self.defendants:
            key = normalize_party_name(defendant.name)
            if key not in deduped:
                deduped[key] = defendant
                order.append(key)
                continue
            existing = deduped[key]
            if existing.party_type == PartyType.UNKNOWN and defendant.party_type != PartyType.UNKNOWN:
                existing.party_type = defendant.party_type
            existing.is_federal_entity = existing.is_federal_entity or defendant.is_federal_entity
            existing.is_deceased = existing.is_deceased or defendant.is_deceased
            if not existing.lien_recording_reference and defendant.lien_recording_reference:
                existing.lien_recording_reference = defendant.lien_recording_reference
        self.defendants = [deduped[key] for key in order]
        return self

    @model_validator(mode="after")
    def _enforce_hard_gates(self) -> JudgmentExtraction:
        """Reject extractions that fail the minimum acceptance contract."""
        failures: list[str] = []
        failures.extend(self._check_required_fields())
        failures.extend(self._check_amounts_sum_hard())
        failures.extend(self._check_judge_as_defendant())
        failures.extend(self._check_sale_terms_hard())
        if failures:
            raise ValueError("; ".join(failures))
        return self

    # -------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------

    def is_thin(self) -> bool:
        """Check if this extraction is 'thin' (likely a fee order, not a real judgment).

        A thin extraction has no legal description AND no mortgage recording
        reference.  This triggers judgment recovery workflows.
        """
        legal = (self.legal_description or "").strip()
        fm = self.foreclosed_mortgage
        has_mortgage_ref = False
        if fm:
            has_mortgage_ref = bool(
                fm.instrument_number or fm.recording_book or fm.recording_page
            )
        return not legal and not has_mortgage_ref

    def first_defendant_name(self) -> str | None:
        """Return the first defendant's name (used by ORI service)."""
        if self.defendants:
            return self.defendants[0].name
        return None

    def has_federal_defendant(self) -> bool:
        """True if any defendant is a federal entity."""
        return any(d.is_federal_entity for d in self.defendants)

    def to_foreclosing_refs(self) -> dict[str, str | None] | None:
        """Build the foreclosing_refs dict consumed by pg_survival_service.

        Maps foreclosed_mortgage fields to the flat dict format the survival
        engine expects: {instrument, book, page}.
        """
        fm = self.foreclosed_mortgage
        if not fm:
            return None
        refs = {
            "instrument": fm.instrument_number,
            "book": fm.recording_book,
            "page": fm.recording_page,
        }
        if any(refs.values()):
            return refs
        return None

    # -------------------------------------------------------------------
    # Cross-field validation (post-extraction sanity checks)
    # -------------------------------------------------------------------

    def validate_extraction(self) -> tuple[list[str], list[str]]:
        """Run all cross-field consistency checks against the extraction.

        Returns ``(failures, warnings)`` where:
        - **failures**: Hard gates that make the extraction unusable.  If any
          failures are present, the extraction should be rejected or retried.
        - **warnings**: Soft flags that indicate potential issues worth
          reviewing or cross-checking against the database.

        An extraction with zero failures and zero warnings is considered
        trustworthy without DB validation.
        """
        failures: list[str] = []
        warnings: list[str] = []

        # Hard gates (minimum data contract)
        failures.extend(self._check_required_fields())
        failures.extend(self._check_amounts_sum_hard())
        failures.extend(self._check_judge_as_defendant())
        failures.extend(self._check_sale_terms_hard())

        # Soft checks
        warnings.extend(self._check_amounts_sum_soft())
        warnings.extend(self._check_date_ordering())
        warnings.extend(self._check_federal_consistency())
        warnings.extend(self._check_type_consistency())
        warnings.extend(self._check_per_diem_reasonableness())
        warnings.extend(self._check_confidence_consistency())
        warnings.extend(self._check_condo_consistency())
        warnings.extend(self._check_property_identity())

        return failures, warnings

    def _check_required_fields(self) -> list[str]:
        """Enforce the minimum data contract for a usable judgment extraction.

        A judgment is only investment-grade if we have enough data to:
        1. Link it to the right property (legal_description or property_address)
        2. Link it to the right case (case_number)
        3. Know the total owed (total_judgment_amount)
        4. Know who owes it (defendants or explanation)
        5. Know when to bid (judgment_date, foreclosure_sale_date)
        """
        failures: list[str] = []

        if not self.case_number:
            failures.append("REQUIRED: case_number is missing")
        if not self.judgment_date:
            failures.append("REQUIRED: judgment_date is missing")
        if not self.total_judgment_amount:
            failures.append("REQUIRED: total_judgment_amount is missing")
        if not self.foreclosure_sale_date:
            failures.append("REQUIRED: foreclosure_sale_date is missing")
        if not self.legal_description and not self.property_address:
            failures.append(
                "REQUIRED: need at least one of legal_description or "
                "property_address to identify the property"
            )
        if not self.defendants:
            # Allow if there's an explicit explanation
            has_explanation = any(
                "defendant" in s.lower() or "party" in s.lower()
                for s in self.unclear_sections
            )
            if not has_explanation:
                failures.append(
                    "REQUIRED: defendants list is empty with no explanation "
                    "in unclear_sections"
                )

        return failures

    def _check_amounts_sum_hard(self) -> list[str]:
        """HARD FAIL: Line items present but don't reconcile to total.

        LLMs can read numbers but can't add reliably.  When the known
        line items leave a remainder vs the stated total, we recompute
        ``other_costs`` as the difference rather than trusting the
        model's arithmetic.  The gate then fires only when the major
        components (principal, interest, fees) themselves are wrong,
        not when the model botched a subtotal.
        """
        if self.total_judgment_amount is None:
            return []

        # Known line items the model extracts individually
        known = self._known_amounts()
        non_null_known = [c for c in known if c is not None]
        if len(non_null_known) < 3:
            return []  # not enough itemization to enforce

        known_sum = sum(non_null_known)
        credit_adjustments = extract_credit_adjustments(self.raw_text)
        adjusted_known_sum = known_sum - credit_adjustments
        remainder = self.total_judgment_amount - adjusted_known_sum

        # Recompute other_costs as the remainder if it's non-negative.
        # This lets us validate without relying on the LLM's arithmetic.
        if remainder >= 0:
            self.other_costs = round(remainder, 2)
            return []

        # Negative remainder means known items exceed the total — that's
        # a real extraction error (wrong number read from OCR).
        diff = abs(remainder)
        threshold = max(self.total_judgment_amount * 0.0001, 10.0)
        if diff > threshold:
            return [
                f"FAIL: Known line items ${known_sum:,.2f} exceed "
                f"total ${self.total_judgment_amount:,.2f} by "
                f"${diff:,.2f} (>{threshold:,.0f} threshold)"
            ]
        return []

    def _check_judge_as_defendant(self) -> list[str]:
        """HARD FAIL: Judge still appears as defendant after stripping."""
        if not self.judge_name or not self.defendants:
            return []
        judge_norm = normalize_party_name(self.judge_name)
        for d in self.defendants:
            if normalize_party_name(d.name) == judge_norm:
                return [
                    f"FAIL: Judge '{self.judge_name}' appears as defendant "
                    f"'{d.name}' - extraction is confused about parties"
                ]
        return []

    def _check_amounts_sum_soft(self) -> list[str]:
        """Soft warning: other_costs was recomputed or residual is notable."""
        if self.total_judgment_amount is None:
            return []

        known = self._known_amounts()
        non_null_known = [c for c in known if c is not None]
        if len(non_null_known) < 2:
            return []

        known_sum = sum(non_null_known)
        credit_adjustments = extract_credit_adjustments(self.raw_text)
        adjusted_known_sum = known_sum - credit_adjustments
        remainder = self.total_judgment_amount - adjusted_known_sum

        # Warn when the remainder (other_costs) is a sizable fraction of
        # the total — may indicate a missing major component.
        if remainder > 0 and remainder / self.total_judgment_amount > 0.05:
            return [
                f"other_costs remainder ${remainder:,.2f} is "
                f"{remainder / self.total_judgment_amount:.1%} of total — "
                f"a major line item may be missing from extraction"
            ]
        return []

    def _known_amounts(self) -> list[float | None]:
        if self.per_diem_interest is None:
            accrued_per_diem = extract_accrued_per_diem_interest(self.raw_text)
            if accrued_per_diem is not None:
                self.per_diem_interest = accrued_per_diem
                if (
                    self.per_diem_rate is not None
                    and abs(self.per_diem_rate - accrued_per_diem) < 0.01
                ):
                    self.per_diem_rate = None

        attorney_fees = self.attorney_fees
        authoritative_attorney_fees = extract_authoritative_attorney_fees(
            self.raw_text
        )
        if (
            attorney_fees is not None
            and authoritative_attorney_fees is not None
            and attorney_fees > authoritative_attorney_fees
        ):
            attorney_fees = authoritative_attorney_fees
            self.attorney_fees = authoritative_attorney_fees

        late_charges = self.late_charges
        if (
            late_charges is not None
            and self.court_costs is not None
            and abs(late_charges - self.court_costs) < 0.01
            and not self._mentions_late_fees()
        ):
            late_charges = None
            self.late_charges = None

        court_costs = self.court_costs
        embedded_corporate_total = extract_embedded_corporate_advance_total(
            self.raw_text
        )
        if (
            court_costs is not None
            and self.escrow_advances is not None
            and embedded_corporate_total is not None
            and abs(embedded_corporate_total - self.escrow_advances) <= 1.0
        ):
            court_costs = None
            self.court_costs = None

        return [
            self.principal_amount,
            self.interest_amount,
            self.per_diem_interest,
            late_charges,
            self.escrow_advances,
            self.title_search_costs,
            court_costs,
            attorney_fees,
        ]

    def _mentions_late_fees(self) -> bool:
        raw = (self.raw_text or "").upper()
        return any(marker in raw for marker in ("LATE FEE", "LATE FEES", "LATE CHARGE", "LATE CHARGES"))

    def _check_sale_terms_hard(self) -> list[str]:
        """HARD FAIL: sale modality contradicts the stated sale location."""
        failures: list[str] = []
        sale_location = (self.sale_location or "").strip().upper()
        looks_online = any(
            marker in sale_location
            for marker in ("REALFORECLOSE", "HTTP://", "HTTPS://", "WWW.")
        )

        if looks_online and not self.is_online_sale:
            failures.append(
                "FAIL: sale_location indicates an online sale but "
                "is_online_sale is false"
            )
        if self.is_online_sale and not self.foreclosure_sale_date:
            failures.append(
                "FAIL: is_online_sale is true but foreclosure_sale_date is missing"
            )

        return failures

    def _check_date_ordering(self) -> list[str]:
        """Verify chronological consistency of dates."""
        warnings: list[str] = []

        dates = {
            "default_date": self.default_date,
            "interest_through_date": self.interest_through_date,
            "judgment_date": self.judgment_date,
            "foreclosure_sale_date": self.foreclosure_sale_date,
        }
        # Expected order: default < interest_through <= judgment < sale
        pairs = [
            ("default_date", "judgment_date", "Default date should precede judgment date"),
            ("judgment_date", "foreclosure_sale_date", "Judgment date should precede sale date"),
            ("interest_through_date", "foreclosure_sale_date", "Interest-through date should precede sale date"),
        ]
        for early_key, late_key, msg in pairs:
            early = dates.get(early_key)
            late = dates.get(late_key)
            if early and late and early > late:
                warnings.append(f"{msg}: {early_key}={early} vs {late_key}={late}")

        return warnings

    def _check_federal_consistency(self) -> list[str]:
        """If a defendant is flagged federal, there should be a matching red flag."""
        warnings: list[str] = []
        has_federal = self.has_federal_defendant()
        has_federal_flag = any(
            rf.flag_type == RedFlagType.FEDERAL_DEFENDANT for rf in self.red_flags
        )
        if has_federal and not has_federal_flag:
            fed_names = [d.name for d in self.defendants if d.is_federal_entity]
            warnings.append(
                f"Federal defendant(s) found ({', '.join(fed_names)}) but no "
                f"'federal_defendant' red flag was generated"
            )
        return warnings

    def _check_type_consistency(self) -> list[str]:
        """Check plaintiff_type vs foreclosure_type alignment."""
        warnings: list[str] = []
        if self.plaintiff_type and self.foreclosure_type:
            if self.plaintiff_type == PlaintiffType.HOA and self.foreclosure_type not in (
                ForeclosureType.HOA, ForeclosureType.CONDO, ForeclosureType.OTHER,
            ):
                warnings.append(
                    f"plaintiff_type is 'hoa' but foreclosure_type is "
                    f"'{self.foreclosure_type}' - expected HOA or CONDO"
                )
            if self.foreclosure_type in (ForeclosureType.HOA, ForeclosureType.CONDO) and \
               self.plaintiff_type not in (PlaintiffType.HOA, PlaintiffType.OTHER):
                warnings.append(
                    f"foreclosure_type is '{self.foreclosure_type}' but "
                    f"plaintiff_type is '{self.plaintiff_type}' - expected 'hoa'"
                )
        return warnings

    def _check_per_diem_reasonableness(self) -> list[str]:
        """Check per_diem_rate is reasonable relative to principal."""
        warnings: list[str] = []
        if self.per_diem_rate and self.principal_amount:
            # Per diem should be roughly (principal * annual_rate / 365)
            # Reasonable annual rates: 2% to 25%
            # So per diem should be principal * 0.02/365 to principal * 0.25/365
            min_reasonable = self.principal_amount * 0.02 / 365
            max_reasonable = self.principal_amount * 0.25 / 365
            if self.per_diem_rate < min_reasonable * 0.5:
                warnings.append(
                    f"per_diem_rate ${self.per_diem_rate:.2f}/day seems too low "
                    f"for principal ${self.principal_amount:,.2f} "
                    f"(implies < 1% annual rate)"
                )
            if self.per_diem_rate > max_reasonable * 2:
                warnings.append(
                    f"per_diem_rate ${self.per_diem_rate:.2f}/day seems too high "
                    f"for principal ${self.principal_amount:,.2f} "
                    f"(implies > 50% annual rate)"
                )
        return warnings

    def _check_confidence_consistency(self) -> list[str]:
        """High confidence with unclear sections is contradictory."""
        warnings: list[str] = []
        if (
            self.confidence_score
            and self.confidence_score >= 0.95
            and len(self.unclear_sections) >= 3
        ):
            warnings.append(
                f"confidence_score is {self.confidence_score} but "
                f"{len(self.unclear_sections)} unclear section(s) reported - "
                f"consider lowering confidence"
            )
        return warnings

    def _check_condo_consistency(self) -> list[str]:
        """If is_condo is True, unit should be set and lot/block empty."""
        warnings: list[str] = []
        if self.is_condo and not self.unit and (self.lot or self.block):
            warnings.append(
                "is_condo is True but unit is empty and lot/block are set - "
                "verify this is actually a condo"
            )
        if self.unit and not self.is_condo and not self.lot:
            warnings.append(
                f"unit='{self.unit}' is set without lot/block and is_condo=False - "
                f"this may be a condo (set is_condo=True) or a phase number "
                f"(move to subdivision name)"
            )
        return warnings

    def _check_property_identity(self) -> list[str]:
        """Check that property identification fields are internally consistent."""
        warnings: list[str] = []

        # Subdivision in legal_description should match subdivision field
        if self.subdivision and self.legal_description:
            sub_upper = self.subdivision.upper()
            legal_upper = self.legal_description.upper()
            if sub_upper not in legal_upper:
                warnings.append(
                    f"subdivision '{self.subdivision}' not found in "
                    f"legal_description text - may be extracted from wrong section"
                )

        # Lot/block should appear in legal_description if both are set
        if self.lot and self.legal_description:
            legal_upper = self.legal_description.upper()
            lot_str = re.sub(
                r"\bLOTS?\b|\bNO\.?\b",
                "",
                str(self.lot).upper(),
            )
            lot_str = re.sub(r"\s+", " ", lot_str).strip(" ,")
            if lot_str not in legal_upper:
                warnings.append(
                    f"lot '{self.lot}' not found in legal_description text"
                )

        if self.block and self.legal_description:
            legal_upper = self.legal_description.upper()
            block_str = re.sub(r"\bBLOCK\b", "", str(self.block).upper())
            block_str = re.sub(r"\s+", " ", block_str).strip(" ,")
            if block_str and f"BLOCK {block_str}" not in legal_upper:
                warnings.append(
                    f"block '{self.block}' not found in legal_description text"
                )

        # Plat book/page: warn if plat_book is set but plat_page isn't (or vice versa)
        if bool(self.plat_book) != bool(self.plat_page):
            warnings.append(
                f"plat_book='{self.plat_book}' and plat_page='{self.plat_page}' - "
                f"expected both or neither"
            )

        return warnings
