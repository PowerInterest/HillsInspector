import base64
import json
import requests
from typing import Dict, Optional, Any

# Document extraction prompts
DEED_PROMPT = """
Analyze this deed document. Extract and return JSON:
{
  "document_type": "WARRANTY DEED" | "QUIT CLAIM" | "SPECIAL WARRANTY",
  "recording_date": "MM/DD/YYYY",
  "grantor": "Name of seller/transferor",
  "grantee": "Name of buyer/transferee",
  "consideration": "$amount or stated consideration",
  "legal_description": "Full legal description text",
  "property_address": "Street address if shown",
  "notary_date": "Date of notarization"
}
"""

MORTGAGE_PROMPT = """
Analyze this mortgage document. Extract and return JSON:
{
  "document_type": "MORTGAGE" | "DEED OF TRUST",
  "recording_date": "MM/DD/YYYY",
  "borrower": "Name(s) of borrower",
  "lender": "Name of lender/mortgagee",
  "original_amount": "$principal amount",
  "property_address": "Street address",
  "legal_description": "Full legal description",
  "maturity_date": "Loan maturity date if shown"
}
"""

LIEN_PROMPT = """
Analyze this lien document. Extract and return JSON:
{
  "document_type": "LIEN" | "JUDGMENT" | "TAX LIEN" | "HOA LIEN" | "MECHANICS LIEN",
  "recording_date": "MM/DD/YYYY",
  "debtor": "Name of property owner/debtor",
  "creditor": "Name of lien holder",
  "amount": "$amount owed",
  "case_number": "Court case number if applicable",
  "legal_description": "Full legal description"
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

    # API Configuration - Remote vLLM server
    API_URL = "http://10.10.1.5:6969/v1/chat/completions"
    MODEL = "Qwen/Qwen3-VL-8B-Instruct"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'Connection': 'keep-alive'})

    def check_server(self) -> bool:
        """
        Check if the Qwen Vision API server is available.

        Returns:
            True if server is up and responding, False otherwise.
        """
        try:
            # Try a simple models endpoint or health check
            base_url = self.API_URL.rsplit('/v1/', 1)[0]
            response = self.session.get(f"{base_url}/v1/models", timeout=5)
            return response.status_code == 200
        except Exception:
            # Fallback: try a minimal completion request
            try:
                payload = {
                    "model": self.MODEL,
                    "messages": [{"role": "user", "content": "test"}],
                    "max_tokens": 1
                }
                response = self.session.post(self.API_URL, json=payload, timeout=10)
                return response.status_code == 200
            except Exception:
                return False
        
    def _encode_image(self, image_path: str) -> str:
        """Encode image to base64 string."""
        with open(image_path, "rb") as f:
            return base64.b64encode(f.read()).decode()

    def analyze_image(self, image_path: str, prompt: str, max_tokens: int = 1024) -> Optional[str]:
        """
        Analyze an image with a text prompt.
        
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
                "model": self.MODEL,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {"url": f"data:image/png;base64,{base64_image}"}
                            },
                            {"type": "text", "text": prompt}
                        ]
                    }
                ],
                "max_tokens": max_tokens,
                "temperature": 0.1
            }
            
            response = self.session.post(self.API_URL, json=payload, timeout=120)
            response.raise_for_status()
            
            result = response.json()
            content = result["choices"][0]["message"]["content"]
            return content.strip()
            
        except Exception as e:
            print(f"Vision API Error: {e}")
            return None

    def extract_text(self, image_path: str) -> str:
        """
        Extract all visible text from the image (OCR).
        """
        prompt = "Transcribe all visible text in this image exactly as it appears. Do not summarize or describe the image, just output the text."
        result = self.analyze_image(image_path, prompt)
        return result if result else ""

    def extract_json(self, image_path: str, prompt: str) -> Optional[Dict[str, Any]]:
        """
        Extract structured data as JSON.
        """
        full_prompt = f"{prompt}\n\nRespond ONLY with a valid JSON object. Do not include markdown formatting like ```json."
        result = self.analyze_image(image_path, full_prompt)

        if result:
            try:
                # Clean up potential markdown
                cleaned = result.replace("```json", "").replace("```", "").strip()
                return json.loads(cleaned)
            except json.JSONDecodeError:
                print(f"Failed to parse JSON from Vision API response: {result}")
                return None
        return None

    def extract_deed(self, image_path: str) -> Optional[Dict[str, Any]]:
        """Extract structured data from a deed document image."""
        return self.extract_json(image_path, DEED_PROMPT)

    def extract_mortgage(self, image_path: str) -> Optional[Dict[str, Any]]:
        """Extract structured data from a mortgage document image."""
        return self.extract_json(image_path, MORTGAGE_PROMPT)

    def extract_lien(self, image_path: str) -> Optional[Dict[str, Any]]:
        """Extract structured data from a lien document image."""
        return self.extract_json(image_path, LIEN_PROMPT)

    def extract_final_judgment(self, image_path: str) -> Optional[Dict[str, Any]]:
        """Extract structured data from a Final Judgment of Foreclosure document."""
        return self.extract_json(image_path, FINAL_JUDGMENT_PROMPT)

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

        if result and result.get('confidence', 0) >= confidence_threshold:
            return result
        if result:
            print(f"CAPTCHA confidence {result.get('confidence', 0)} below threshold {confidence_threshold}")
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
        if doc_type in ['WD', 'QC', 'D', 'DEED', 'CD', 'TD', 'SD']:
            return self.extract_deed(image_path)
        # Mortgage types
        if doc_type in ['MTG', 'MORTGAGE', 'DOT']:
            return self.extract_mortgage(image_path)
        # Lien types
        if doc_type in ['LN', 'LIEN', 'LP', 'LIS PENDENS']:
            return self.extract_lien(image_path)
        # Final Judgment
        if doc_type in ['FJ', 'FINAL JUDGMENT', 'JUDGMENT', 'JUD']:
            return self.extract_final_judgment(image_path)
        # Satisfaction/Release - use lien prompt (similar structure)
        if doc_type in ['SAT', 'REL', 'SATISFACTION', 'RELEASE']:
            return self.extract_json(image_path, """
Analyze this satisfaction/release document. Extract and return JSON:
{
  "document_type": "SATISFACTION" | "RELEASE",
  "recording_date": "MM/DD/YYYY",
  "original_instrument": "The instrument number being satisfied/released",
  "original_book": "Book of original document",
  "original_page": "Page of original document",
  "lender": "Name of lender releasing the lien",
  "legal_description": "Full legal description if present"
}
""")
        # Generic extraction
        return self.extract_text(image_path)
