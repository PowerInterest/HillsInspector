import base64
import json
import os
import re
import time
import requests
import io
import asyncio
import functools
from typing import Dict, Optional, Any
from loguru import logger
from PIL import Image
from json_repair import repair_json


def _extract_json_candidate(text: str) -> Optional[str]:
    start = text.find("{")
    if start == -1:
        return None
    in_string = False
    escape = False
    depth = 0
    for idx in range(start, len(text)):
        ch = text[idx]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : idx + 1]
    return text[start:]


def _sanitize_json_text(text: str) -> str:
    out = []
    in_string = False
    escape = False
    for ch in text:
        if in_string:
            if escape:
                escape = False
                out.append(ch)
                continue
            if ch == "\\":
                escape = True
                out.append(ch)
                continue
            if ch == '"':
                in_string = False
                out.append(ch)
                continue
            if ch == "\n":
                out.append("\\n")
                continue
            if ch == "\r":
                out.append("\\r")
                continue
            if ch == "\t":
                out.append("\\t")
                continue
            out.append(ch)
            continue
        if ch == '"':
            in_string = True
        out.append(ch)
    return "".join(out)


def _append_missing_braces(text: str) -> str:
    in_string = False
    escape = False
    depth = 0
    for ch in text:
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth = max(depth - 1, 0)
    if depth > 0:
        return text + ("}" * depth)
    return text


def robust_json_parse(text: str, context: str = "") -> Optional[Dict[str, Any]]:
    """
    Robustly parse JSON that may have common LLM formatting issues.

    Handles:
    - Missing commas between properties
    - Trailing commas
    - Markdown code blocks
    - Extra whitespace/newlines
    """
    if not text:
        return None

    # Clean up markdown code blocks, GLM box tokens, and isolate a JSON candidate.
    cleaned = re.sub(r"<\|(?:begin|end)_of_box\|>", "", text)
    cleaned = cleaned.replace("```json", "").replace("```", "").strip()
    candidate = _extract_json_candidate(cleaned)
    if candidate:
        cleaned = candidate
    cleaned = _sanitize_json_text(cleaned)

    # First try direct parsing
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as err:
        logger.debug(
            "Direct JSON parse failed ({}): {} at line {}, col {}",
            context,
            err.msg,
            err.lineno,
            err.colno,
        )

    # Repair common LLM JSON issues (e.g., stray text, trailing commas, missing quotes).
    try:
        repaired = repair_json(cleaned)
        return json.loads(repaired)
    except Exception as e:
        logger.debug(f"JSON repair attempt 1 failed: {e}")

    # Try to fix missing commas between properties
    # Pattern: "value"\n\n  "key" or "value"\n  "key" (missing comma)
    fixed = re.sub(r"([\"\d\]\}])\s*\n+\s*(\")", r"\1,\n  \2", cleaned)

    try:
        return json.loads(fixed)
    except json.JSONDecodeError as err:
        logger.debug(
            "JSON parse failed after missing-comma repair ({}): {} at line {}, col {}",
            context,
            err.msg,
            err.lineno,
            err.colno,
        )

    # Try removing trailing commas before } or ]
    fixed2 = re.sub(r",\s*([}\]])", r"\1", fixed)

    try:
        return json.loads(fixed2)
    except json.JSONDecodeError as err:
        logger.debug(
            "JSON parse failed after trailing-comma cleanup ({}): {} at line {}, col {}",
            context,
            err.msg,
            err.lineno,
            err.colno,
        )

    # Attempt to close any missing braces for truncated outputs.
    fixed3 = _append_missing_braces(fixed2)
    try:
        return json.loads(fixed3)
    except json.JSONDecodeError:
        snippet = cleaned[:500].replace("\n", " ")
        logger.warning(
            "Failed to parse JSON from Vision API response ({}): {}...",
            context,
            snippet,
        )
        return None


# Document extraction prompts
DEED_PROMPT = """
You are analyzing a recorded deed document from Hillsborough County Official Records. Extract ALL information for title examination purposes.

## DEED TYPES TO IDENTIFY
- WARRANTY DEED (WD): Full covenants, seller guarantees clear title
- SPECIAL WARRANTY DEED (SWD): Limited covenants, only guarantees seller's ownership period
- QUIT CLAIM DEED (QC): No warranties, transfers whatever interest grantor has
- PERSONAL REPRESENTATIVE'S DEED (PRD): Estate/probate transfer
- TRUSTEE'S DEED (TD): Transfer from trust
- CERTIFICATE OF TITLE (CT): Court-ordered transfer (foreclosure sale)
- TAX DEED: Transfer from tax sale
- CORRECTIVE DEED: Fixes errors in prior deed

## CRITICAL DATA TO EXTRACT
1. **Parties**: Exact names of grantor(s) and grantee(s) - spelling matters for title
2. **Legal Description**: VERBATIM - every word, lot, block, subdivision, plat reference
3. **Consideration**: Sale price or stated value ($10.00 often means non-arm's length)
4. **Execution Date vs Recording Date**: Both are important
5. **Exceptions/Reservations**: Any rights retained by grantor

## OUTPUT FORMAT
Return ONLY valid JSON:
{
  "document_type": "WARRANTY_DEED|QUIT_CLAIM|SPECIAL_WARRANTY|PERSONAL_REP|TRUSTEE|TAX_DEED|CERTIFICATE_OF_TITLE|CORRECTIVE|OTHER",
  "deed_subtype": "more specific description if applicable",

  "grantor": "Full name(s) of seller/transferor exactly as written",
  "grantor_capacity": "individual|married_couple|trustee|personal_rep|corporation|llc|other",
  "grantor_marital_status": "single|married|joined_by_spouse|divorced|widowed|null",

  "grantee": "Full name(s) of buyer/transferee exactly as written",
  "grantee_capacity": "individual|married_couple|trustee|tenants_in_common|joint_tenants|corporation|llc|other",
  "grantee_vesting": "how title is held (e.g., 'as tenants by the entirety')",

  "consideration": 250000.00,
  "consideration_text": "exact text (e.g., '$10.00 and other good and valuable consideration')",
  "is_arms_length": true,

  "legal_description": "VERBATIM full legal description - every word",
  "subdivision": "subdivision name if mentioned",
  "lot": "lot number",
  "block": "block number",
  "unit": "unit number for condos",
  "plat_book": "plat book reference",
  "plat_page": "plat page reference",
  "section_township_range": "SEC-TWP-RGE if metes and bounds",

  "property_address": "street address if shown",
  "parcel_id": "folio/parcel ID if shown",

  "execution_date": "YYYY-MM-DD date deed was signed",
  "recording_date": "YYYY-MM-DD date recorded with clerk",
  "instrument_number": "recording instrument number",
  "book": "recording book",
  "page": "recording page",

  "exceptions_reservations": ["list any exceptions, easements, or reservations mentioned"],
  "subject_to": ["list any 'subject to' clauses (mortgages, liens, etc.)"],

  "documentary_stamps": 123.45,
  "intangible_tax": 0.00,

  "notary_state": "state where notarized",
  "notary_date": "YYYY-MM-DD",

  "red_flags": [
    {"flag": "description of concern", "severity": "high|medium|low"}
  ],

  "confidence": "high|medium|low"
}

## RED FLAGS TO IDENTIFY
- $10 consideration (non-arm's length, possible gift or related party)
- Quit claim deeds in the chain (no warranties)
- Personal representative deeds (estate issues)
- Missing marital status or spouse signature
- Legal description discrepancies
- Recent transfers before foreclosure (possible fraud)
"""

MORTGAGE_PROMPT = """
You are analyzing a recorded mortgage document from Hillsborough County Official Records. Extract ALL information for lien analysis and title examination.

## MORTGAGE TYPES
- CONVENTIONAL MORTGAGE: Standard bank/lender mortgage
- FHA MORTGAGE: Federal Housing Administration insured
- VA MORTGAGE: Veterans Affairs guaranteed
- USDA MORTGAGE: Rural development loan
- HELOC: Home Equity Line of Credit
- SECOND MORTGAGE: Junior lien position
- PURCHASE MONEY MORTGAGE: Seller financing
- CONSTRUCTION MORTGAGE: For new construction
- REVERSE MORTGAGE: Home Equity Conversion Mortgage (HECM)

## CRITICAL DATA FOR LIEN PRIORITY
1. **Recording Date**: Determines lien priority
2. **Principal Amount**: Original loan amount
3. **Lender/Mortgagee**: Current holder (may have been assigned)
4. **MERS**: If MERS is mortgagee, note the MIN (MERS ID Number)

## OUTPUT FORMAT
Return ONLY valid JSON:
{
  "document_type": "MORTGAGE|DEED_OF_TRUST|HELOC|SECOND_MORTGAGE|CONSTRUCTION|REVERSE",
  "mortgage_subtype": "FHA|VA|USDA|CONVENTIONAL|PURCHASE_MONEY|OTHER",

  "borrower": "Full name(s) of mortgagor/borrower exactly as written",
  "borrower_capacity": "individual|married_couple|trust|corporation|llc",
  "co_borrower": "co-borrower name if present",

  "lender": "Full name of mortgagee/lender exactly as written",
  "lender_type": "bank|credit_union|mortgage_company|private|seller|gse",
  "is_mers": false,
  "mers_min": "MERS Identification Number if shown",

  "principal_amount": 250000.00,
  "principal_text": "exact text (e.g., 'Two Hundred Fifty Thousand and 00/100 Dollars')",

  "interest_rate": 6.5,
  "interest_type": "fixed|adjustable|variable",
  "arm_details": "if adjustable, index and margin info",

  "loan_term_months": 360,
  "maturity_date": "YYYY-MM-DD",
  "first_payment_date": "YYYY-MM-DD",

  "legal_description": "VERBATIM full legal description",
  "subdivision": "subdivision name",
  "lot": "lot number",
  "block": "block number",
  "property_address": "street address",
  "parcel_id": "folio/parcel ID if shown",

  "execution_date": "YYYY-MM-DD date mortgage was signed",
  "recording_date": "YYYY-MM-DD date recorded",
  "instrument_number": "recording instrument number",
  "book": "recording book",
  "page": "recording page",

  "documentary_stamps": 875.00,
  "intangible_tax": 500.00,

  "prepayment_penalty": false,
  "balloon_payment": false,
  "balloon_amount": null,

  "future_advances_clause": false,
  "dragnet_clause": false,
  "cross_collateralization": false,

  "assignment_info": {
    "has_assignment": false,
    "assigned_to": null,
    "assignment_date": null,
    "assignment_instrument": null
  },

  "red_flags": [
    {"flag": "description", "severity": "high|medium|low"}
  ],

  "confidence": "high|medium|low"
}

## RED FLAGS TO IDENTIFY
- MERS mortgages (assignment chain issues common)
- Future advances/dragnet clauses (may secure more than stated)
- Private/hard money lenders (short terms, high rates)
- Second mortgages recording close to first (possible fraud)
- Balloon payments
- Missing or illegible signatures
"""

LIEN_PROMPT = """
You are analyzing a recorded lien document from Hillsborough County Official Records. Extract ALL information for lien survival analysis.

## LIEN TYPES AND PRIORITY
1. **TAX LIENS**: Always superior (IRS, State, Property Tax)
2. **JUDGMENT LIENS**: Court-ordered, attaches to all debtor's property
3. **MECHANICS/CONSTRUCTION LIENS**: For unpaid work, relates back to NOC
4. **HOA/CONDO LIENS**: Association assessments, may have super-priority
5. **CODE ENFORCEMENT LIENS**: Municipal fines/violations
6. **CHILD SUPPORT LIENS**: Court-ordered support arrears
7. **FEDERAL TAX LIENS**: IRS liens, 120-day redemption rights
8. **STATE TAX LIENS**: FL DOR liens

## CRITICAL FOR LIEN SURVIVAL
- Recording date determines general priority
- HOA liens may have limited super-priority (safe harbor amount)
- Federal tax liens have special redemption rights
- Mechanics liens relate back to Notice of Commencement date

## OUTPUT FORMAT
Return ONLY valid JSON:
{
  "document_type": "JUDGMENT|TAX_LIEN|MECHANICS_LIEN|HOA_LIEN|CONDO_LIEN|CODE_ENFORCEMENT|IRS_LIEN|STATE_TAX_LIEN|CHILD_SUPPORT|OTHER",
  "lien_subtype": "more specific description",

  "debtor": "Full name(s) of property owner/debtor exactly as written",
  "debtor_address": "debtor address if shown",

  "creditor": "Full name of lien holder/claimant exactly as written",
  "creditor_type": "federal_agency|state_agency|municipality|hoa|contractor|judgment_creditor|other",
  "is_federal_entity": false,

  "amount": 15000.00,
  "amount_text": "exact text showing amount",
  "amount_breakdown": {
    "principal": 10000.00,
    "interest": 3000.00,
    "fees": 2000.00,
    "penalties": 0.00
  },

  "interest_rate": 12.0,
  "per_diem": 4.11,

  "case_number": "court case number if judgment",
  "case_court": "court name if judgment",
  "judgment_date": "YYYY-MM-DD if judgment",

  "legal_description": "VERBATIM full legal description if shown",
  "property_address": "property address if shown",
  "parcel_id": "folio if shown",

  "recording_date": "YYYY-MM-DD",
  "instrument_number": "recording instrument",
  "book": "recording book",
  "page": "recording page",

  "expiration_date": "YYYY-MM-DD if lien expires",
  "renewal_required": false,

  "hoa_specific": {
    "is_hoa": false,
    "association_name": null,
    "assessment_type": "regular|special|delinquent",
    "assessment_period": "date range covered",
    "safe_harbor_applies": false
  },

  "mechanics_lien_specific": {
    "is_mechanics": false,
    "noc_date": "YYYY-MM-DD Notice of Commencement date",
    "noc_instrument": "NOC recording reference",
    "work_description": "type of work performed",
    "contractor_name": "contractor/supplier name",
    "owner_builder": false
  },

  "satisfaction_info": {
    "has_satisfaction": false,
    "satisfaction_date": null,
    "satisfaction_instrument": null
  },

  "red_flags": [
    {"flag": "description", "severity": "high|medium|low"}
  ],

  "survival_notes": "notes about whether this lien survives foreclosure",
  "confidence": "high|medium|low"
}

## RED FLAGS TO IDENTIFY
- IRS/Federal liens (120-day redemption right)
- Large HOA arrears (may have super-priority portion)
- Multiple liens from same creditor (pattern of debt)
- Mechanics liens without NOC reference (priority uncertain)
- Expired judgment liens (10 years in FL without renewal)
- Code enforcement with ongoing violations
"""

ENCUMBRANCE_AMOUNT_PROMPT = """
You are analyzing a recorded document from the Hillsborough County Official Records to extract financial amounts.

## YOUR TASK
Extract the PRIMARY DOLLAR AMOUNT from this document. This is typically:
- For MORTGAGES: The principal loan amount (usually the largest dollar figure on page 1)
- For LIENS: The amount owed/claimed
- For JUDGMENTS: The judgment amount
- For HOA LIENS: The assessment amount or total due
- For TAX CERTIFICATES: The face value or redemption amount

## CRITICAL INSTRUCTIONS
1. Look for dollar amounts with $ signs or written as "Dollars"
2. The principal/face amount is usually prominently displayed on page 1
3. For mortgages, look for phrases like:
   - "principal sum of $XXX,XXX.XX"
   - "in the amount of $XXX,XXX.XX"
   - "for the sum of $XXX,XXX.XX"
   - "NOTE AMOUNT: $XXX,XXX.XX"
4. IGNORE smaller amounts like recording fees, documentary stamps, or per diem rates
5. If multiple amounts shown, extract the LARGEST principal/face amount
6. Include cents (e.g., $250,000.00 not $250,000)

## OUTPUT FORMAT
Return ONLY valid JSON:
{
  "amount": 250000.00,
  "amount_text": "$250,000.00",
  "amount_type": "principal|judgment|lien|assessment|tax_certificate",
  "confidence": "high|medium|low",
  "source_phrase": "exact text where amount was found",
  "additional_amounts": [
    {"description": "what this amount is for", "amount": 1234.56}
  ]
}

## EXAMPLES
- Mortgage showing "principal sum of Two Hundred Fifty Thousand and 00/100 Dollars ($250,000.00)"
  -> amount: 250000.00, amount_type: "principal", confidence: "high"

- Lien stating "NOW DUE AND OWING: $5,432.18"
  -> amount: 5432.18, amount_type: "lien", confidence: "high"

If you cannot find a clear dollar amount, return:
{"amount": null, "confidence": "low", "reason": "explanation"}
"""

SATISFACTION_PROMPT = """
You are analyzing a Satisfaction of Mortgage or Release of Lien document from Hillsborough County Official Records.

## DOCUMENT PURPOSE
This document releases/satisfies a previously recorded lien (mortgage, judgment, etc.). It's critical to identify WHICH lien is being released.

## OUTPUT FORMAT
Return ONLY valid JSON:
{
  "document_type": "SATISFACTION_OF_MORTGAGE|RELEASE_OF_LIEN|PARTIAL_RELEASE|DISCHARGE",

  "releasing_party": "Name of lender/creditor releasing the lien",
  "releasing_party_type": "bank|servicer|trust|attorney|individual",

  "property_owner": "Current owner name if shown",

  "original_instrument": {
    "instrument_number": "instrument being satisfied",
    "book": "book of original document",
    "page": "page of original document",
    "recording_date": "YYYY-MM-DD original recording date",
    "original_amount": 250000.00,
    "document_type": "MORTGAGE|LIEN|JUDGMENT"
  },

  "legal_description": "legal description if included",
  "property_address": "property address if shown",
  "parcel_id": "folio if shown",

  "execution_date": "YYYY-MM-DD date signed",
  "recording_date": "YYYY-MM-DD date recorded",
  "instrument_number": "this satisfaction's instrument number",
  "book": "recording book",
  "page": "recording page",

  "is_partial_release": false,
  "partial_release_description": "if partial, what is being released",

  "confidence": "high|medium|low"
}

## CRITICAL
The most important field is the ORIGINAL INSTRUMENT reference - this tells us which mortgage/lien is now paid off.
"""

ASSIGNMENT_PROMPT = """
You are analyzing an Assignment of Mortgage document from Hillsborough County Official Records.

## DOCUMENT PURPOSE
Assignments transfer mortgage ownership from one lender to another. Critical for determining who currently holds the mortgage.

## OUTPUT FORMAT
Return ONLY valid JSON:
{
  "document_type": "ASSIGNMENT_OF_MORTGAGE|CORPORATE_ASSIGNMENT|CORRECTIVE_ASSIGNMENT",

  "assignor": "Name of party assigning/selling the mortgage",
  "assignor_type": "bank|servicer|trust|mers|individual",

  "assignee": "Name of party receiving the mortgage",
  "assignee_type": "bank|servicer|trust|mers|individual",

  "original_mortgage": {
    "instrument_number": "original mortgage instrument",
    "book": "book",
    "page": "page",
    "recording_date": "YYYY-MM-DD",
    "original_amount": 250000.00,
    "original_borrower": "borrower name from mortgage",
    "original_lender": "original lender name"
  },

  "legal_description": "legal description if included",
  "property_address": "property address if shown",
  "parcel_id": "folio if shown",

  "execution_date": "YYYY-MM-DD date signed",
  "recording_date": "YYYY-MM-DD date recorded",
  "instrument_number": "this assignment's instrument number",
  "book": "recording book",
  "page": "recording page",

  "is_mers_assignment": false,
  "mers_min": "MERS ID if shown",

  "consideration": "stated consideration if any",

  "prior_assignments": [
    {"from": "name", "to": "name", "date": "YYYY-MM-DD", "instrument": "number"}
  ],

  "red_flags": [
    {"flag": "description", "severity": "high|medium|low"}
  ],

  "confidence": "high|medium|low"
}

## RED FLAGS
- Robo-signing indicators (illegible signatures, same signature different names)
- MERS as assignor without proper authority language
- Gap in assignment chain (missing intermediate assignments)
- Assignment recorded after foreclosure filed
"""

LIS_PENDENS_PROMPT = """
You are analyzing a Lis Pendens (Notice of Pending Litigation) from Hillsborough County Official Records.

## DOCUMENT PURPOSE
A Lis Pendens provides constructive notice that litigation affecting the property is pending. Critical for foreclosure timeline analysis.

## OUTPUT FORMAT
Return ONLY valid JSON:
{
  "document_type": "LIS_PENDENS|AMENDED_LIS_PENDENS|NOTICE_OF_ACTION",

  "case_number": "court case number",
  "court": "court name (e.g., Circuit Court, 13th Judicial Circuit)",
  "case_type": "FORECLOSURE|PARTITION|QUIET_TITLE|OTHER",

  "plaintiff": "Name of plaintiff (usually foreclosing lender)",
  "plaintiff_type": "bank|servicer|trust|hoa|individual",
  "plaintiff_attorney": "attorney name if shown",

  "defendants": [
    {
      "name": "defendant name",
      "party_type": "borrower|spouse|junior_lienholder|tenant|unknown",
      "is_federal_entity": false
    }
  ],

  "property_description": {
    "legal_description": "full legal description",
    "property_address": "street address",
    "parcel_id": "folio",
    "subdivision": "subdivision name",
    "lot": "lot",
    "block": "block"
  },

  "mortgage_reference": {
    "instrument_number": "mortgage being foreclosed",
    "book": "book",
    "page": "page",
    "recording_date": "YYYY-MM-DD",
    "original_amount": 250000.00
  },

  "filing_date": "YYYY-MM-DD case filed",
  "recording_date": "YYYY-MM-DD lis pendens recorded",
  "instrument_number": "lis pendens instrument number",
  "book": "recording book",
  "page": "recording page",

  "red_flags": [
    {"flag": "description", "severity": "high|medium|low"}
  ],

  "confidence": "high|medium|low"
}

## CRITICAL
Lis Pendens date is important - liens recorded AFTER this date are junior to the foreclosure.
"""

NOC_PROMPT = """
You are analyzing a Notice of Commencement (NOC) from Hillsborough County Official Records.

## DOCUMENT PURPOSE
NOC establishes the priority date for mechanics/construction liens. Any contractor lien relates back to this date.

## OUTPUT FORMAT
Return ONLY valid JSON:
{
  "document_type": "NOTICE_OF_COMMENCEMENT|AMENDED_NOC|TERMINATION_OF_NOC",

  "property_owner": "Name of owner authorizing construction",
  "owner_type": "individual|married_couple|trust|corporation|llc",

  "contractor": {
    "name": "General contractor name",
    "license_number": "contractor license if shown",
    "address": "contractor address"
  },

  "lender": {
    "name": "Construction lender if any",
    "loan_amount": 500000.00
  },

  "surety_bond": {
    "has_bond": false,
    "bond_amount": null,
    "surety_company": null
  },

  "project_description": "description of work",
  "estimated_cost": 250000.00,
  "commencement_date": "YYYY-MM-DD work started or will start",
  "expiration_date": "YYYY-MM-DD NOC expires (usually 1 year)",

  "legal_description": "full legal description",
  "property_address": "street address",
  "parcel_id": "folio",

  "recording_date": "YYYY-MM-DD",
  "instrument_number": "instrument number",
  "book": "recording book",
  "page": "recording page",

  "is_owner_builder": false,

  "designated_agent": {
    "name": "agent for service if designated",
    "address": "agent address"
  },

  "confidence": "high|medium|low"
}

## CRITICAL
The recording date of the NOC determines priority for ALL mechanics liens on this project.
"""

AFFIDAVIT_PROMPT = """
You are analyzing an Affidavit document from Hillsborough County Official Records.

## COMMON AFFIDAVIT TYPES
- Affidavit of Heirship (estate/inheritance)
- Affidavit of Domicile
- Affidavit of Continuous Marriage
- Affidavit of Identity (name variations)
- Affidavit of No Liens
- Affidavit of Title

## OUTPUT FORMAT
Return ONLY valid JSON:
{
  "document_type": "AFFIDAVIT_OF_HEIRSHIP|AFFIDAVIT_OF_DOMICILE|AFFIDAVIT_OF_IDENTITY|AFFIDAVIT_OF_TITLE|OTHER_AFFIDAVIT",
  "affidavit_subtype": "specific description",

  "affiant": "Name of person making affidavit",
  "affiant_relationship": "relationship to property/decedent",

  "subject_matter": {
    "decedent_name": "if heirship, name of deceased",
    "date_of_death": "YYYY-MM-DD if applicable",
    "heirs": [
      {"name": "heir name", "relationship": "relationship", "share": "1/2"}
    ],
    "identity_names": ["list of name variations if identity affidavit"],
    "key_statements": ["important sworn statements"]
  },

  "legal_description": "legal description if property-related",
  "property_address": "address if shown",
  "parcel_id": "folio if shown",

  "execution_date": "YYYY-MM-DD",
  "recording_date": "YYYY-MM-DD",
  "instrument_number": "instrument number",
  "book": "recording book",
  "page": "recording page",

  "notary_info": {
    "notary_name": "notary name",
    "notary_state": "state",
    "commission_expiration": "date if shown"
  },

  "confidence": "high|medium|low"
}
"""

FINAL_JUDGMENT_PROMPT = """
You are a professional title examiner analyzing a Florida Final Judgment of Foreclosure document. Extract ALL information into the structured JSON format below. Be extremely precise—title work requires exact transcription of legal descriptions, recording references, and dollar amounts.

## DOCUMENT CONTEXT
This is a court order from a Florida Circuit Court (likely 13th Judicial Circuit / Hillsborough County) that:
1. Establishes the amount owed on a defaulted mortgage
2. Orders the property sold at public auction
3. Determines which liens are extinguished vs. survive
4. Sets the timeline for redemption termination

## EXTRACTION INSTRUCTIONS

### PARTIES
- **Plaintiffs**: Usually the bank, loan servicer, or trust foreclosing the mortgage
- **Defendants**: CAPTURE EVERY SINGLE ONE - this list determines which liens are wiped out
  - Look for: borrowers, spouses, second mortgage holders, HOAs, condo associations, judgment creditors, "Unknown Tenant", IRS, USA, state agencies
  - Federal defendants (IRS, USA, FHA, VA, HUD) trigger extended redemption rights - FLAG THESE

### FINANCIAL AMOUNTS
Extract the itemized breakdown exactly as shown. Include cents. "$123,456.78" not "$123,456".

### LEGAL DESCRIPTION
Transcribe the ENTIRE legal description verbatim - every word, number, abbreviation. This includes subdivision name, Lot, Block, Unit, Plat Book/Page references, Section-Township-Range.

**CRITICAL**: The legal description is often attached as "Exhibit A", "Schedule A", or "Attachment 1" on the final pages of the document. If a short legal description is on page 1 but a full one is on a later page (Exhibit A), ALWAYS prioritize the full verbatim text from the Exhibit. If it spans multiple pages, join them into one continuous string.

### RECORDING REFERENCES
Capture all Book/Page or Instrument Number references for the mortgage being foreclosed, assignments, and lis pendens.

## OUTPUT FORMAT
Return ONLY a valid JSON object with this structure:

{
  "case_number": "string",
  "court_circuit": "string (e.g., '13th')",
  "county": "string",
  "judge_name": "string or null",
  "judgment_date": "YYYY-MM-DD",

  "plaintiff": "string - full name of foreclosing party",
  "plaintiff_type": "bank|servicer|trust|gse|hoa|private_lender",

  "defendants": [
    {
      "name": "string",
      "party_type": "borrower|co_borrower|spouse|second_mortgage_holder|judgment_creditor|hoa|condo_association|irs|federal_agency|municipality|tenant|unknown",
      "is_federal_entity": false,
      "is_deceased": false,
      "lien_recording_reference": "string or null (Book/Page or Instrument #)"
    }
  ],

  "property_address": "string or null",
  "legal_description": "string - VERBATIM TRANSCRIPTION of full legal description",
  "parcel_id": "string or null",
  "subdivision": "string or null",
  "lot": "string or null",
  "block": "string or null",
  "unit": "string or null (for condos)",
  "plat_book": "string or null",
  "plat_page": "string or null",
  "is_condo": false,

  "foreclosed_mortgage": {
    "original_date": "YYYY-MM-DD or null",
    "original_amount": 0.00,
    "recording_date": "YYYY-MM-DD or null",
    "recording_book": "string or null",
    "recording_page": "string or null",
    "instrument_number": "string or null"
  },

  "lis_pendens": {
    "recording_date": "YYYY-MM-DD or null",
    "recording_book": "string or null",
    "recording_page": "string or null",
    "instrument_number": "string or null"
  },

  "principal_amount": 0.00,
  "interest_amount": 0.00,
  "interest_through_date": "YYYY-MM-DD or null",
  "per_diem_rate": 0.00,
  "late_charges": 0.00,
  "escrow_advances": 0.00,
  "title_search_costs": 0.00,
  "court_costs": 0.00,
  "attorney_fees": 0.00,
  "other_costs": 0.00,
  "total_judgment_amount": 0.00,

  "foreclosure_sale_date": "YYYY-MM-DD or null",
  "sale_location": "string (URL or address) or null",
  "is_online_sale": false,

  "foreclosure_type": "FIRST MORTGAGE|SECOND MORTGAGE|HOA|CONDO|TAX|OTHER",
  "hoa_safe_harbor_mentioned": false,
  "superiority_language": "string - quote exact language about lien priority or null",

  "red_flags": [
    {
      "flag_type": "federal_defendant|lost_note|deceased_borrower|service_issue|missing_hoa_defendant",
      "severity": "critical|high|medium",
      "description": "string explaining the concern"
    }
  ],

  "monthly_payment": 0.00,
  "default_date": "YYYY-MM-DD or null",
  "service_by_publication": false,

  "confidence_score": 0.95,
  "unclear_sections": ["list any sections that were difficult to read"]
}

## CRITICAL REMINDERS

1. **NEVER GUESS** on legal descriptions, recording references, or dollar amounts. If unclear, set to null.
2. **CAPTURE ALL DEFENDANTS** - A missing defendant means their lien survives. This is critical.
3. **FEDERAL ENTITIES** - If you see "United States of America", "IRS", "FHA", "VA", "HUD" - flag as federal entity with redemption rights.
4. **DATES** - Use ISO format YYYY-MM-DD. If only month/year given, use first of month.
5. **DOLLAR AMOUNTS** - Include cents as decimals.
"""

CAPTCHA_PROMPT = """
Analyze this CAPTCHA image. Return JSON with:
{
  "captcha_type": "text" | "image_select" | "recaptcha" | "unknown",
  "solution": "The text/answer to solve the CAPTCHA",
  "confidence": 0-100 (your confidence in the solution),
  "instructions": "Any visible instructions for solving"
}
Only attempt to solve text-based CAPTCHAs. For image selection or reCAPTCHA, set confidence to 0.
"""

MARKET_LISTING_PROMPT = """
Analyze this real estate listing screenshot. Extract the following information in JSON format:
{
  "price": "Listed price (number only, no symbols)",
  "zestimate": "Zestimate value if visible (number only)",
  "rent_zestimate": "Rent Zestimate if visible (number only)",
  "address": "Property address",
  "beds": "Number of bedrooms",
  "baths": "Number of bathrooms",
  "sqft": "Square footage",
  "lot_size": "Lot size if shown",
  "year_built": "Year built if shown",
  "hoa_fee": "HOA fee if shown (number only)",
  "days_on_market": "Days on market if shown",
  "description": "Brief summary of property details visible"
}
"""

PERMIT_SEARCH_PROMPT = """
Analyze this building permit search results page from Accela Citizen Access.
Extract ALL permits shown in JSON format:

{
    "permits": [
        {
            "permit_number": "<permit ID/record number>",
            "permit_type": "<Building/Electrical/Plumbing/Mechanical/Roofing/etc>",
            "status": "<Issued/Finaled/Expired/Pending/Active/Closed/etc>",
            "issue_date": "<MM/DD/YYYY or null>",
            "expiration_date": "<MM/DD/YYYY or null>",
            "description": "<work description/project name>",
            "address": "<property address if shown>",
            "contractor": "<contractor name if shown>"
        }
    ],
    "total_records": <number of records found>,
    "search_address": "<address that was searched>"
}

Extract every permit visible in the results. Return ONLY valid JSON.
"""

REALTOR_LISTING_PROMPT = """
Analyze this real estate listing screenshot from Realtor.com.
Extract ALL available information in JSON format:

{
    "list_price": <number or null>,
    "listing_status": "<For Sale/Sold/Pending/Off Market/Active/etc>",
    "beds": <number or null>,
    "baths": <number or null>,
    "sqft": <number or null>,
    "lot_size": "<string or null>",
    "year_built": <number or null>,
    "property_type": "<Single Family/Condo/Townhouse/Multi-Family/etc>",
    "hoa_fee": <number or null>,
    "hoa_frequency": "<Monthly/Annually/Quarterly/etc or null>",
    "days_on_market": <number or null>,
    "price_per_sqft": <number or null>,
    "estimated_payment": <number or null>,
    "description": "<property description text>",
    "mls_number": "<MLS# or null>",
    "address": "<full property address>",
    "agent_name": "<listing agent name if shown>",
    "price_history": [
        {"date": "<MM/DD/YYYY>", "event": "<Listed/Sold/Price Change/etc>", "price": <number>}
    ]
}

Focus especially on HOA fees, price history, and property details.
Return ONLY valid JSON, no other text.
"""

HCPA_PROMPT = """
Analyze this Property Appraiser (HCPA) details page. Extract ALL available data into a structured JSON.
Include the following sections if visible:
{
  "owner_info": {
    "owner_name": "Name of owner(s)",
    "mailing_address": "Full mailing address"
  },
  "property_details": {
    "folio": "Folio/Parcel ID",
    "site_address": "Site address",
    "legal_description": "Full legal description",
    "use_code": "DOR Code / Description",
    "tax_district": "Tax District name"
  },
  "value_summary": {
    "year": "Current Tax Year",
    "just_market_value": "Just/Market Value",
    "assessed_value": "Assessed Value",
    "taxable_value": "Taxable Value (County/School/Muni)"
  },
  "sales_history": [
    {
      "date": "Sale Date",
      "price": "Price",
      "instrument": "Instrument Number",
      "deed_type": "Deed Code/Type",
      "grantor": "Grantor (Seller)",
      "grantee": "Grantee (Buyer)"
    }
  ],
  "building_info": {
    "year_built": "Year Built",
    "beds": "Bedrooms",
    "baths": "Bathrooms",
    "heated_area": "Heated Area (sq ft)",
    "gross_area": "Gross Area (sq ft)",
    "stories": "Stories"
  },
  "extra_features": [
    {
      "description": "Feature description (e.g. Pool, Fence)",
      "units": "Units/Size",
      "value": "Value"
    }
  ],
  "land_lines": [
    {
      "use_code": "Use Code",
      "description": "Description",
      "zone": "Zone",
      "units": "Units",
      "value": "Value"
    }
  ]
}
"""


class VisionService:
    """
    Service for interacting with Qwen Vision API for image analysis and OCR.
    """

    # API Configuration - Remote vLLM servers (with failover)
    # 10.10.0.76 (truckws.dbag.lab) has GLM-4V-Flash + Qwen3-VL-30B + Qwen3-VL-8B
    # 10.10.0.33 has Qwen3-VL-8B with 262k context
    # 10.10.1.5 has 262k context, 10.10.2.27 only has 11k - prioritize the larger one
    # 192.168.86.26:1234 is LM Studio on Windows (uses different model ID)
    _LOCAL_ENDPOINTS = [
        {"url": "http://10.10.1.5:1234/v1/chat/completions", "model": "glm-4.6v"},
        {"url": "http://10.10.0.76:6969/v1/chat/completions", "model": "zai-org/glm-4.6v-flash"},
        {"url": "http://192.168.86.26:6969/v1/chat/completions", "model": "zai-org/glm-4.6v-flash"},
        {"url": "http://10.10.0.76:6969/v1/chat/completions", "model": "qwen/qwen3-vl-30b"},
        {"url": "http://10.10.0.33:6969/v1/chat/completions", "model": "Qwen/Qwen3-VL-8B-Instruct"},
        # 10.10.1.5:6969 is down — only 1234 (glm-4.6v) is active
        {"url": "http://10.10.2.27:6969/v1/chat/completions", "model": "Qwen/Qwen3-VL-8B-Instruct"},
        {"url": "http://192.168.86.26:1234/v1/chat/completions", "model": "qwen/qwen3-vl-8b"},
    ]
    # Legacy attributes for backwards compatibility
    MODEL = "Qwen/Qwen3-VL-8B-Instruct"

    @classmethod
    def _parse_api_keys(cls, env_var: str) -> list[str]:
        """Parse one or more API keys from an env var.

        Supports formats:
          - Single key: ``KEY=sk-abc123``
          - Comma-separated: ``KEY=sk-abc,sk-def``
          - JSON-ish array: ``KEY=["sk-abc", "sk-def"]``
        """
        raw = os.getenv(env_var, "").strip()
        if not raw:
            return []
        # Strip surrounding brackets
        if raw.startswith("[") and raw.endswith("]"):
            raw = raw[1:-1]
        # Split on commas, strip quotes and whitespace
        keys = []
        for part in raw.split(","):
            key = part.strip().strip("'\"")
            if key:
                keys.append(key)
        return keys

    @classmethod
    def _build_endpoints(cls) -> list[dict]:
        """Build endpoint list: local first, then cloud fallbacks if API keys are set."""
        cloud_endpoints: list[dict] = []
        gemini_keys = cls._parse_api_keys("GEMINI_API_KEY")
        for key in gemini_keys:
            cloud_endpoints.append({
                "url": "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
                "model": "gemini-2.5-flash-lite",
                "api_key": key,
            })
        # When cloud keys are configured, prefer cloud first to avoid long timeouts
        # on stale LAN hosts.
        if cloud_endpoints:
            return cloud_endpoints + list(cls._LOCAL_ENDPOINTS)
        return list(cls._LOCAL_ENDPOINTS)

    API_ENDPOINTS = _LOCAL_ENDPOINTS  # Default; overridden at init via _build_endpoints()
    API_URLS = [ep["url"] for ep in _LOCAL_ENDPOINTS]

    # Class-level storage for healthy endpoints (shared across instances)
    _healthy_endpoints: list[dict] | None = None
    _health_check_done: bool = False

    _endpoints_built = False

    # Track temporarily suspended endpoints (URL → resume_time)
    # When an endpoint times out or errors, suspend it to avoid wasting time.
    # Tiered durations: dead servers get longer suspension, slow servers shorter.
    _suspended_endpoints: dict[str, float] = {}
    _SUSPEND_CONN_REFUSED = 600   # 10 min — server is down
    _SUSPEND_READ_TIMEOUT = 120   # 2 min — server is slow but alive, retry soon
    _SUSPEND_HTTP_ERROR = 300     # 5 min — rate limited or server error

    # Connect timeout: fail fast on dead servers.  Read timeout: be patient.
    _CONNECT_TIMEOUT = 10  # seconds
    _CLOUD_READ_TIMEOUT = 60  # seconds — cloud endpoints are fast

    @classmethod
    def _ensure_endpoints_built(cls):
        """Build endpoint list once (includes cloud fallbacks if API keys are set)."""
        if cls._endpoints_built:
            return
        cls.API_ENDPOINTS = cls._build_endpoints()
        cls.API_URLS = [ep["url"] for ep in cls.API_ENDPOINTS]
        cloud_count = sum(1 for ep in cls.API_ENDPOINTS if ep.get("api_key"))
        if cloud_count:
            logger.info("Vision service configured with {} cloud fallback endpoint(s)", cloud_count)
        cls._endpoints_built = True

    def __init__(self):
        """
        Initialize VisionService.
        """
        VisionService._ensure_endpoints_built()

        self.session = requests.Session()
        self.session.headers.update({"Connection": "keep-alive"})
        self._active_endpoint = None
        if not hasattr(VisionService, "_global_semaphore"):
            VisionService._global_semaphore = asyncio.Semaphore(1)
        self._semaphore = VisionService._global_semaphore

    @classmethod
    def health_check_endpoints(cls, timeout: int = 15) -> list[dict]:
        """
        Check all vision endpoints at startup, return only healthy ones.

        This should be called once at pipeline startup. Results are cached
        at the class level so all VisionService instances share the same
        list of healthy endpoints.

        Args:
            timeout: Connection timeout in seconds for health check

        Returns:
            List of healthy endpoint configs

        Raises:
            RuntimeError: If no vision endpoints are available
        """
        cls._ensure_endpoints_built()
        import requests as req

        def check_endpoint(endpoint: dict) -> tuple[dict, str, str | None]:
            # Cloud endpoints (with api_key) are always considered available
            if endpoint.get("api_key"):
                return endpoint, "cloud", None
            base_url = endpoint["url"].rsplit("/v1/", 1)[0]
            models_url = f"{base_url}/v1/models"
            try:
                response = req.get(models_url, timeout=timeout)
                if response.ok:
                    return endpoint, "healthy", None
                return endpoint, "unhealthy", f"HTTP {response.status_code}"
            except req.exceptions.Timeout:
                return endpoint, "timeout", models_url
            except req.exceptions.ConnectionError:
                return endpoint, "connection_error", models_url
            except Exception as e:
                return endpoint, "failed", f"{models_url} - {e}"

        import concurrent.futures

        # Check all endpoints in parallel but preserve original order
        # (local endpoints first, cloud fallbacks last)
        results: dict[int, tuple[dict, str, str | None]] = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(cls.API_ENDPOINTS))) as executor:
            future_to_idx = {executor.submit(check_endpoint, endpoint): idx for idx, endpoint in enumerate(cls.API_ENDPOINTS)}
            for future in concurrent.futures.as_completed(future_to_idx):
                idx = future_to_idx[future]
                results[idx] = future.result()

        healthy = []
        for idx in sorted(results):
            endpoint, status, detail = results[idx]
            if status == "healthy":
                healthy.append(endpoint)
                logger.success(f"Vision endpoint healthy: {endpoint['url']}")
            elif status == "cloud":
                healthy.append(endpoint)
                logger.info(f"Cloud vision fallback available: {endpoint['url']} (model: {endpoint['model']})")
            elif status == "unhealthy":
                logger.warning(f"Vision endpoint unhealthy ({detail}): {endpoint['url']}")
            elif status == "timeout":
                logger.warning(f"Vision endpoint timeout: {detail}")
            elif status == "connection_error":
                logger.warning(f"Vision endpoint connection refused: {detail}")
            else:
                logger.warning(f"Vision endpoint check failed: {detail}")

        if healthy:
            cls._healthy_endpoints = healthy
            cls._health_check_done = True
            logger.info(f"Vision service initialized with {len(healthy)}/{len(cls.API_ENDPOINTS)} healthy endpoints")
        else:
            cls._healthy_endpoints = None
            cls._health_check_done = True
            logger.error("No vision endpoints available! Vision-dependent features will fail.")

        return healthy

    @classmethod
    def get_available_endpoints(cls) -> list[dict]:
        """
        Get list of available endpoints.

        Returns healthy endpoints if health check was done, otherwise all endpoints.
        """
        if cls._health_check_done and cls._healthy_endpoints is not None:
            return cls._healthy_endpoints
        return cls.API_ENDPOINTS

    @classmethod
    def reset_health_check(cls):
        """Reset health check state to allow re-checking endpoints."""
        cls._healthy_endpoints = None
        cls._health_check_done = False
        cls._endpoints_built = False

    @classmethod
    def global_semaphore(cls) -> asyncio.Semaphore:
        """Expose the shared semaphore for cross-service throttling."""
        if not hasattr(cls, "_global_semaphore"):
            cls._global_semaphore = asyncio.Semaphore(1)
        return cls._global_semaphore

    @property
    def API_URL(self) -> str:  # noqa: N802
        """Get the active API URL, checking availability if needed."""
        return self._get_active_endpoint()["url"]

    @property
    def active_model(self) -> str:
        """Get the model ID for the active endpoint."""
        return self._get_active_endpoint()["model"]

    def _get_active_endpoint(self) -> dict:
        """Get the active endpoint config, checking availability if needed."""
        if self._active_endpoint:
            return self._active_endpoint
        # Use pre-filtered healthy endpoints if available
        available = self.get_available_endpoints()
        if not available:
            logger.error("No vision endpoints available")
            return self.API_ENDPOINTS[0]  # Fallback to first configured
        # Find first available server from healthy list
        for endpoint in available:
            # Cloud endpoints don't need a health probe - they're always reachable
            if endpoint.get("api_key"):
                continue  # Skip cloud endpoints for active selection; prefer local
            try:
                base_url = endpoint["url"].rsplit("/v1/", 1)[0]
                response = self.session.get(f"{base_url}/v1/models", timeout=3)
                if response.status_code == 200:
                    self._active_endpoint = endpoint
                    logger.info("Using vision endpoint: {} (model: {})", endpoint["url"], endpoint["model"])
                    return endpoint
            except Exception as exc:
                logger.debug("Vision endpoint {} unavailable: {}", endpoint["url"], exc)
                continue
        # If no local endpoints available, use first cloud endpoint
        for endpoint in available:
            if endpoint.get("api_key"):
                self._active_endpoint = endpoint
                logger.info("Using cloud vision endpoint: {} (model: {})", endpoint["url"], endpoint["model"])
                return endpoint
        # Default to first available endpoint
        return available[0]

    def _try_all_endpoints(self, payload: dict, timeout: int = 120) -> Optional[requests.Response]:
        """
        Try to post to all available endpoints until one succeeds.
        On timeout or connection error, try the next endpoint.

        Uses pre-filtered healthy endpoints from startup health check if available.
        """
        available = self.get_available_endpoints()
        if not available:
            logger.error("No vision endpoints available to try")
            return None

        errors = []
        tried_endpoints: set[tuple[str, str]] = set()

        def _attempt(endpoints: list[dict], read_timeout: int) -> Optional[requests.Response]:
            now = time.monotonic()
            for endpoint in endpoints:
                url = endpoint["url"]
                model = endpoint["model"]
                endpoint_key = (url, model)
                if endpoint_key in tried_endpoints:
                    logger.debug(
                        "Skipping already-tried endpoint {} (model: {})",
                        url,
                        model,
                    )
                    continue
                # Skip endpoints that are temporarily suspended (recently timed out)
                resume_at = VisionService._suspended_endpoints.get(url)
                if resume_at is not None:
                    if now < resume_at:
                        logger.debug(
                            "Skipping suspended endpoint {} (model: {}) ({:.0f}s remaining)",
                            url,
                            model,
                            resume_at - now,
                        )
                        continue
                    # Suspension expired, allow retry
                    del VisionService._suspended_endpoints[url]
                tried_endpoints.add(endpoint_key)
                try:
                    is_cloud = bool(endpoint.get("api_key"))
                    label = "cloud" if is_cloud else "local"
                    # Cloud endpoints are fast — cap read timeout.
                    # Local endpoints get the full scaled timeout.
                    ep_read_timeout = (
                        min(read_timeout, VisionService._CLOUD_READ_TIMEOUT)
                        if is_cloud
                        else read_timeout
                    )
                    logger.info(
                        "Trying {} vision endpoint: {} (model: {}, timeout={}+{}s)",
                        label,
                        url,
                        model,
                        VisionService._CONNECT_TIMEOUT,
                        ep_read_timeout,
                    )
                    payload_copy = payload.copy()
                    payload_copy["model"] = model
                    headers = {}
                    if endpoint.get("api_key"):
                        headers["Authorization"] = f"Bearer {endpoint['api_key']}"
                    response = self.session.post(
                        url,
                        json=payload_copy,
                        headers=headers,
                        timeout=(VisionService._CONNECT_TIMEOUT, ep_read_timeout),
                    )
                    if response.ok:
                        self._active_endpoint = endpoint
                        return response
                    try:
                        err_body = response.text[:300]
                    except Exception as body_err:
                        logger.warning(
                            "Failed to read error body from vision endpoint {}: {}",
                            url,
                            body_err,
                        )
                        err_body = f"(unreadable: {type(body_err).__name__})"
                    logger.warning(
                        "Vision endpoint {} (model: {}) returned HTTP {}: {}",
                        url,
                        model,
                        response.status_code,
                        err_body,
                    )
                    errors.append(f"{url} ({model}): HTTP {response.status_code}")
                    # Suspend endpoints returning 429 (quota) or 5xx (server error)
                    if response.status_code == 429 or response.status_code >= 500:
                        suspend_secs = VisionService._SUSPEND_HTTP_ERROR
                        VisionService._suspended_endpoints[url] = time.monotonic() + suspend_secs
                        logger.info(
                            "Suspended endpoint {} for {}s after HTTP {}",
                            url,
                            suspend_secs,
                            response.status_code,
                        )
                except requests.exceptions.Timeout as e:
                    logger.warning("Timeout on endpoint {} (model: {}): {}", url, model, e)
                    errors.append(f"{url} ({model}): Timeout")
                    # Read timeout = slow but alive → short suspension.
                    # Connect timeout = unreachable → long suspension.
                    is_connect_timeout = "connect" in str(e).lower() or "NewConnection" in str(e)
                    suspend_secs = (
                        VisionService._SUSPEND_CONN_REFUSED
                        if is_connect_timeout
                        else VisionService._SUSPEND_READ_TIMEOUT
                    )
                    VisionService._suspended_endpoints[url] = time.monotonic() + suspend_secs
                    logger.info(
                        "Suspended endpoint {} for {}s after timeout (connect={})",
                        url,
                        suspend_secs,
                        is_connect_timeout,
                    )
                    continue
                except requests.exceptions.ConnectionError as e:
                    logger.warning("Connection error on endpoint {} (model: {}): {}", url, model, e)
                    errors.append(f"{url} ({model}): Connection error")
                    VisionService._suspended_endpoints[url] = time.monotonic() + VisionService._SUSPEND_CONN_REFUSED
                    continue
                except Exception as e:
                    logger.warning("Error on endpoint {} (model: {}): {}", url, model, e)
                    errors.append(f"{url} ({model}): {e}")
                    continue
            return None

        response = _attempt(available, timeout)
        if response is not None:
            return response

        # If healthy endpoints failed, try remaining endpoints with a shorter timeout.
        if VisionService._health_check_done and VisionService._healthy_endpoints is not None:
            extras = [
                ep
                for ep in self.API_ENDPOINTS
                if (ep["url"], ep["model"]) not in tried_endpoints
            ]
            if extras:
                fallback_timeout = min(timeout, 60)
                logger.warning(
                    "Healthy endpoints failed; trying {} fallback endpoint(s)",
                    len(extras),
                )
                response = _attempt(extras, fallback_timeout)
                if response is not None:
                    return response

        logger.error("All vision endpoints failed: {}", errors)
        return None

    def reset_active_url(self):
        """Reset cached URL to force re-check on next request."""
        self._active_endpoint = None

    async def process_async(self, func, *args, **kwargs):
        """
        Run a synchronous vision method in a thread pool with concurrency limiting.

        Args:
            func: The synchronous method to call (e.g., self.analyze_image)
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func

        Returns:
            The result of func(*args, **kwargs)
        """
        async with self._semaphore:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, functools.partial(func, *args, **kwargs))

    def check_server(self) -> bool:
        """
        Check if the Qwen Vision API server is available.

        Returns:
            True if server is up and responding, False otherwise.
        """
        try:
            # Try a simple models endpoint or health check
            base_url = self.API_URL.rsplit("/v1/", 1)[0]
            response = self.session.get(f"{base_url}/v1/models", timeout=5)
            return response.status_code == 200
        except Exception as e:
            logger.debug(f"Vision health check primary failed for {base_url}: {e}")
            # Fallback: try a minimal completion request
            try:
                payload = {"model": self.active_model, "messages": [{"role": "user", "content": "test"}], "max_tokens": 1}
                response = self.session.post(self.API_URL, json=payload, timeout=10)
                return response.status_code == 200
            except Exception as e2:
                logger.debug(f"Vision health check fallback failed: {e2}")
                return False

    def _encode_image(self, image_path: str, max_dimension: int = 1024) -> str:
        """
        Encode image to base64 string, resizing if necessary.

        Qwen2-VL Recommendation:
        - 1024-1280px balances document legibility and token usage.
        - Higher resolutions exponentially increase token count and VRAM usage.
        - 1024px is a safe default for most document pages.
        """
        try:
            with Image.open(image_path) as img:
                # Convert to RGB if needed (e.g. for RGBA or P modes)
                if img.mode not in ("RGB", "L"):
                    img = img.convert("RGB")

                # Resize if too large
                width, height = img.size
                if width > max_dimension or height > max_dimension:
                    ratio = min(max_dimension / width, max_dimension / height)
                    new_size = (int(width * ratio), int(height * ratio))
                    img = img.resize(new_size, Image.Resampling.LANCZOS)
                    # logger.debug(f"Resized image {image_path} from {width}x{height} to {new_size[0]}x{new_size[1]}")

                # Save to buffer
                buffer = io.BytesIO()
                img.save(buffer, format="JPEG", quality=85)
                return base64.b64encode(buffer.getvalue()).decode()
        except Exception as e:
            logger.warning(f"Failed to process image {image_path} with PIL: {e}. Falling back to raw read.")
            with open(image_path, "rb") as f:
                return base64.b64encode(f.read()).decode()

    def analyze_image(self, image_path: str, prompt: str, max_tokens: int = 1024) -> Optional[str]:
        """
        Analyze an image with a text prompt.
        Tries all available endpoints on failure.

        Args:
            image_path: Path to the image file.
            prompt: Text prompt for the model.
            max_tokens: Max tokens for response.

        Returns:
            The text response from the model, or None if failed.
        """
        try:
            base64_image = self._encode_image(image_path)

            payload = {
                "model": "",  # Will be set by _try_all_endpoints
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{base64_image}"}},
                            {"type": "text", "text": prompt},
                        ],
                    }
                ],
                "max_tokens": 2000,
                "temperature": 0.1,
            }

            response = self._try_all_endpoints(payload, timeout=120)
            if response is None:
                logger.error("All vision endpoints failed for {}", image_path)
                return None

            result = response.json()
            content = result["choices"][0]["message"]["content"]
            return content.strip() if content else None

        except Exception as e:
            logger.exception("Vision API error while analyzing {}: {}", image_path, e)
            logger.error(
                "Vision image analysis returning None for {} after exception; "
                "caller must treat this as extraction failure.",
                image_path,
            )
            return None

    def analyze_images(self, image_paths: list[str], prompt: str, max_tokens: int = 4000) -> Optional[str]:
        """
        Analyze multiple images with a single text prompt in one request.
        Tries all available endpoints on failure.

        Args:
            image_paths: List of image file paths.
            prompt: Text prompt for the model.
            max_tokens: Max tokens for response.

        Returns:
            The text response from the model, or None if failed.
        """
        if not image_paths:
            return None
        try:
            content_blocks = []
            for path in image_paths:
                base64_image = self._encode_image(path)
                content_blocks.append({"type": "image_url", "image_url": {"url": f"data:image/png;base64,{base64_image}"}})
            content_blocks.append({"type": "text", "text": prompt})

            payload = {
                "model": "",  # Will be set by _try_all_endpoints
                "messages": [{"role": "user", "content": content_blocks}],
                "max_tokens": max_tokens,  # Use parameter (default 4000) - multi-page docs need more
                "temperature": 0.1,
            }

            # Scale timeout with page count: 60s base + 60s per image (local GLM is slow)
            timeout = 60 + 60 * len(image_paths)
            response = self._try_all_endpoints(payload, timeout=timeout)
            if response is None:
                logger.error("All vision endpoints failed for multi-image request ({} images)", len(image_paths))
                return None

            result = response.json()
            content = result["choices"][0]["message"]["content"]
            return content.strip() if content else None

        except Exception as e:
            logger.exception("Vision API Error (multi-image): {}", e)
            return None

    def extract_text(self, image_path: str) -> str:
        """
        Extract all visible text from the image (OCR).
        """
        prompt = "Transcribe all visible text in this image exactly as it appears. Do not summarize or describe the image, just output the text."
        result = self.analyze_image(image_path, prompt)
        return result or ""

    def extract_json(self, image_path: str, prompt: str) -> Optional[Dict[str, Any]]:
        """
        Extract structured data as JSON.
        """
        full_prompt = f"{prompt}\n\nRespond ONLY with a valid JSON object. Do not include markdown formatting like ```json."
        result = self.analyze_image(image_path, full_prompt)

        return robust_json_parse(result, "extract_json") if result else None

    def extract_deed(self, image_path: str) -> Optional[Dict[str, Any]]:
        """Extract structured data from a deed document image."""
        return self.extract_json(image_path, DEED_PROMPT)

    def extract_deed_multi(self, image_paths: list[str]) -> Optional[Dict[str, Any]]:
        """Extract structured data from a deed using multiple images in one prompt."""
        result = self.analyze_images(image_paths, DEED_PROMPT)
        return robust_json_parse(result, "deed_multi") if result else None

    def extract_mortgage(self, image_path: str) -> Optional[Dict[str, Any]]:
        """Extract structured data from a mortgage document image."""
        return self.extract_json(image_path, MORTGAGE_PROMPT)

    def extract_mortgage_multi(self, image_paths: list[str]) -> Optional[Dict[str, Any]]:
        """Extract structured data from a mortgage using multiple images in one prompt."""
        result = self.analyze_images(image_paths, MORTGAGE_PROMPT)
        return robust_json_parse(result, "mortgage_multi") if result else None

    def extract_lien(self, image_path: str) -> Optional[Dict[str, Any]]:
        """Extract structured data from a lien document image."""
        return self.extract_json(image_path, LIEN_PROMPT)

    def extract_lien_multi(self, image_paths: list[str]) -> Optional[Dict[str, Any]]:
        """Extract structured data from a lien using multiple images in one prompt."""
        result = self.analyze_images(image_paths, LIEN_PROMPT)
        return robust_json_parse(result, "lien_multi") if result else None

    def extract_final_judgment(self, image_path: str) -> Optional[Dict[str, Any]]:
        """Extract structured data from a Final Judgment of Foreclosure document."""
        return self.extract_json(image_path, FINAL_JUDGMENT_PROMPT)

    def extract_final_judgment_multi(self, image_paths: list[str]) -> Optional[Dict[str, Any]]:
        """Extract structured data from a multi-page Final Judgment (batch images)."""
        # Final judgments need more tokens - many defendants, legal description, financial breakdown
        result = self.analyze_images(image_paths, FINAL_JUDGMENT_PROMPT, max_tokens=6000)
        return robust_json_parse(result, "final_judgment_multi") if result else None

    def extract_encumbrance_amount(self, image_path: str) -> Optional[Dict[str, Any]]:
        """
        Extract dollar amount from a mortgage, lien, or other encumbrance document.

        Args:
            image_path: Path to the document image (first page is usually sufficient)

        Returns:
            Dict with amount, confidence, and metadata or None if failed
        """
        return self.extract_json(image_path, ENCUMBRANCE_AMOUNT_PROMPT)

    def extract_encumbrance_amount_multi(self, image_paths: list[str]) -> Optional[Dict[str, Any]]:
        """
        Extract dollar amount from multiple pages of an encumbrance document.

        Args:
            image_paths: List of page image paths

        Returns:
            Dict with amount, confidence, and metadata or None if failed
        """
        result = self.analyze_images(image_paths, ENCUMBRANCE_AMOUNT_PROMPT)
        return robust_json_parse(result, "encumbrance_amount_multi") if result else None

    def extract_satisfaction(self, image_path: str) -> Optional[Dict[str, Any]]:
        """Extract data from a Satisfaction of Mortgage or Release document."""
        return self.extract_json(image_path, SATISFACTION_PROMPT)

    def extract_satisfaction_multi(self, image_paths: list[str]) -> Optional[Dict[str, Any]]:
        """Extract data from multi-page Satisfaction document."""
        result = self.analyze_images(image_paths, SATISFACTION_PROMPT)
        return robust_json_parse(result, "satisfaction_multi") if result else None

    def extract_assignment(self, image_path: str) -> Optional[Dict[str, Any]]:
        """Extract data from an Assignment of Mortgage document."""
        return self.extract_json(image_path, ASSIGNMENT_PROMPT)

    def extract_assignment_multi(self, image_paths: list[str]) -> Optional[Dict[str, Any]]:
        """Extract data from multi-page Assignment document."""
        result = self.analyze_images(image_paths, ASSIGNMENT_PROMPT)
        return robust_json_parse(result, "assignment_multi") if result else None

    def extract_lis_pendens(self, image_path: str) -> Optional[Dict[str, Any]]:
        """Extract data from a Lis Pendens document."""
        return self.extract_json(image_path, LIS_PENDENS_PROMPT)

    def extract_lis_pendens_multi(self, image_paths: list[str]) -> Optional[Dict[str, Any]]:
        """Extract data from multi-page Lis Pendens document."""
        result = self.analyze_images(image_paths, LIS_PENDENS_PROMPT)
        return robust_json_parse(result, "lis_pendens_multi") if result else None

    def extract_noc(self, image_path: str) -> Optional[Dict[str, Any]]:
        """Extract data from a Notice of Commencement document."""
        return self.extract_json(image_path, NOC_PROMPT)

    def extract_noc_multi(self, image_paths: list[str]) -> Optional[Dict[str, Any]]:
        """Extract data from multi-page NOC document."""
        result = self.analyze_images(image_paths, NOC_PROMPT)
        return robust_json_parse(result, "noc_multi") if result else None

    def extract_affidavit(self, image_path: str) -> Optional[Dict[str, Any]]:
        """Extract data from an Affidavit document."""
        return self.extract_json(image_path, AFFIDAVIT_PROMPT)

    def extract_affidavit_multi(self, image_paths: list[str]) -> Optional[Dict[str, Any]]:
        """Extract data from multi-page Affidavit document."""
        result = self.analyze_images(image_paths, AFFIDAVIT_PROMPT)
        return robust_json_parse(result, "affidavit_multi") if result else None

    def extract_market_listing(self, image_path: str) -> Optional[Dict[str, Any]]:
        """Extract structured data from a real estate listing screenshot."""
        return self.extract_json(image_path, MARKET_LISTING_PROMPT)

    def extract_hcpa_details(self, image_path: str) -> Optional[Dict[str, Any]]:
        """Extract structured data from HCPA property details page."""
        return self.extract_json(image_path, HCPA_PROMPT)

    def extract_permit_results(self, image_path: str) -> Optional[Dict[str, Any]]:
        """Extract structured data from Accela permit search results screenshot."""
        return self.extract_json(image_path, PERMIT_SEARCH_PROMPT)

    def extract_realtor_listing(self, image_path: str) -> Optional[Dict[str, Any]]:
        """Extract structured data from Realtor.com listing screenshot."""
        return self.extract_json(image_path, REALTOR_LISTING_PROMPT)

    def solve_captcha(self, image_path: str, confidence_threshold: int = 80) -> Optional[Dict[str, Any]]:
        """
        Attempt to solve a CAPTCHA using vision analysis.

        Args:
            image_path: Path to CAPTCHA image
            confidence_threshold: Minimum confidence (0-100) to return a solution

        Returns:
            Dict with 'solution', 'confidence', 'captcha_type' if confident enough,
            None if confidence below threshold or failed.
        """
        result = self.extract_json(image_path, CAPTCHA_PROMPT)

        if result and result.get("confidence", 0) >= confidence_threshold:
            return result
        if result:
            logger.warning(f"CAPTCHA confidence {result.get('confidence', 0)} below threshold {confidence_threshold}")
            return result  # Return anyway so caller can decide
        return None

    def extract_document_by_type(self, image_path: str, doc_type: str) -> Optional[Dict[str, Any]]:
        """
        Extract data from a document based on its type.

        Args:
            image_path: Path to the document image
            doc_type: Type code like 'WD', 'QC', 'MTG', 'LN', 'SAT', etc.

        Returns:
            Extracted data dict or None
        """
        doc_type = doc_type.upper()

        # Deed types
        if doc_type in ["WD", "QC", "D", "DEED", "CD", "TD", "SD", "SWD", "PRD", "CT"]:
            return self.extract_deed(image_path)
        # Mortgage types
        if doc_type in ["MTG", "MORTGAGE", "DOT", "MTGNT", "MTGNIT", "HELOC"]:
            return self.extract_mortgage(image_path)
        # Lien types (not lis pendens - that's separate)
        if doc_type in ["LN", "LIEN", "JUD", "TL", "ML", "HOA", "COD", "MECH"]:
            return self.extract_lien(image_path)
        # Lis Pendens - foreclosure notice
        if doc_type in ["LP", "LIS PENDENS", "LISPEN"]:
            return self.extract_lis_pendens(image_path)
        # Final Judgment
        if doc_type in ["FJ", "FINAL JUDGMENT", "JUDGMENT"]:
            return self.extract_final_judgment(image_path)
        # Satisfaction/Release
        if doc_type in ["SAT", "REL", "SATISFACTION", "RELEASE", "SATMTG", "RELMTG"]:
            return self.extract_satisfaction(image_path)
        # Assignment
        if doc_type in ["ASGN", "ASSIGNMENT", "ASGNMTG", "ASSIGN"]:
            return self.extract_assignment(image_path)
        # Notice of Commencement
        if doc_type in ["NOC", "NOTICE OF COMMENCEMENT", "COMMENCE"]:
            return self.extract_noc(image_path)
        # Affidavit
        if doc_type in ["AFF", "AFFIDAVIT", "AFFD"]:
            return self.extract_affidavit(image_path)
        # Generic extraction - just OCR text
        return {"document_type": doc_type, "ocr_text": self.extract_text(image_path)}

    def extract_document_by_type_multi(self, image_paths: list[str], doc_type: str) -> Optional[Dict[str, Any]]:
        """Multi-image variant of extract_document_by_type."""
        doc_type = doc_type.upper()

        # Deed types
        if doc_type in ["WD", "QC", "D", "DEED", "CD", "TD", "SD", "SWD", "PRD", "CT"]:
            return self.extract_deed_multi(image_paths)
        # Mortgage types
        if doc_type in ["MTG", "MORTGAGE", "DOT", "MTGNT", "MTGNIT", "HELOC"]:
            return self.extract_mortgage_multi(image_paths)
        # Lien types (not lis pendens)
        if doc_type in ["LN", "LIEN", "JUD", "TL", "ML", "HOA", "COD", "MECH"]:
            return self.extract_lien_multi(image_paths)
        # Lis Pendens - foreclosure notice
        if doc_type in ["LP", "LIS PENDENS", "LISPEN"]:
            return self.extract_lis_pendens_multi(image_paths)
        # Final Judgment
        if doc_type in ["FJ", "FINAL JUDGMENT", "JUDGMENT"]:
            return self.extract_final_judgment_multi(image_paths)
        # Satisfaction/Release
        if doc_type in ["SAT", "REL", "SATISFACTION", "RELEASE", "SATMTG", "RELMTG"]:
            return self.extract_satisfaction_multi(image_paths)
        # Assignment
        if doc_type in ["ASGN", "ASSIGNMENT", "ASGNMTG", "ASSIGN"]:
            return self.extract_assignment_multi(image_paths)
        # Notice of Commencement
        if doc_type in ["NOC", "NOTICE OF COMMENCEMENT", "COMMENCE"]:
            return self.extract_noc_multi(image_paths)
        # Affidavit
        if doc_type in ["AFF", "AFFIDAVIT", "AFFD"]:
            return self.extract_affidavit_multi(image_paths)
        # Fallback: just OCR the first page for unknown types
        text = self.extract_text(image_paths[0])
        return {"document_type": doc_type, "ocr_text": text} if text else None
