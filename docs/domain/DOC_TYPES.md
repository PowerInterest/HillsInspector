| Category | Codes in Data | Count |
|---|---|---|
| Deeds | D, WD, QCD, SWD, SD, PRD, CT, TAXDEED, DC | 9+ |
| Mortgages | MTG, MTGNT, MTGNIT, MTGREV, DOT, HELOC, AGD | 7 |
| Judgments | JUD, CCJ, FJ, DRJUD, CTF | 5 |
| Liens | LN, LNCORPTX, FIN, MEDLN, HOA, MECH, CEL, SA, SPECASMT, ML | 10+ |
| Lis Pendens | LP, RELLP | 2 |
| Satisfactions | SAT, SATCORPTX, SATMTG, RELMTG | 4 |
| Releases | REL, PR, TER, PRREL | 4 |
| Assignments | ASG, ASGT, ASGN, ASGNMTG, ASINT | 5 |
| Court Papers | CP, DRCP, ORD, BND | 4 |
| Other Recorded | NOC, MOD, SUB, NCL, AFF, POA, AGR, NOT, EAS, PL, GOV, RES, COHOME, PRO | 14+ |

## Document Intelligence & Cross-Referencing (OCR Extraction Value)

Merely knowing a document exists is only half the battle. Extracting the text from these instruments is the key to solving the isolation gaps identified in the [Encumbrance Audit Buckets](ENCUMBRANCE_AUDIT_BUCKETS.md). Below is an analysis of what each category typically contains and how reading it directly links to other documents or county systems:

### 1. Deeds (D, WD, QCD, CT, TAXDEED)
*   **Typical Content:** Grantor, Grantee, exact Legal Description, Parcel ID, execution date, and encumbrance clauses ("Subject to...").
*   **Cross-Reference Value:** 
    *   **Chain Validation:** Establishes the exact names for the grantor/grantee search to find liens.
    *   **Subject-To Clauses:** A deed might explicitly state "Subject to Mortgage recorded in O.R. Book 1234, Page 567," immediately discovering an assumed mortgage.
    *   **Foreclosure/Tax Links:** A Certificate of Title (CT) or Tax Deed explicitly lists the civil Case Number or Tax Certificate Number that generated it, bridging the O.R. back to the civil or tax dockets.
*   **OCR Vision Prompt:**
    ```text
    Extract the following data into JSON:
    1. instrument_number and book_page
    2. property_address and legal_description
    3. party_1 (Grantor/Seller) and party_2 (Grantee/Buyer)
    4. execution_date
    5. assumed_encumbrances: Any explicit mention of existing mortgages or liens the property is "subject to" (extract Book/Page if available).
    6. related_case_number: If a Certificate of Title or Tax Deed, extract the related Civil Case or Tax Certificate number.
    
    Expected JSON Schema:
    {
      "type": "object",
      "properties": {
        "instrument_number": { "type": ["string", "null"] },
        "book_page": { "type": ["string", "null"], "description": "Format: Book X, Page Y" },
        "property_address": { "type": ["string", "null"] },
        "legal_description": { "type": ["string", "null"] },
        "party_1": { "type": ["string", "null"], "description": "Grantor/Seller" },
        "party_2": { "type": ["string", "null"], "description": "Grantee/Buyer" },
        "execution_date": { "type": ["string", "null"], "description": "YYYY-MM-DD" },
        "assumed_encumbrances": { "type": ["string", "null"], "description": "Explicit mention of existing mortgages/liens subject to" },
        "related_case_number": { "type": ["string", "null"] }
      },
      "required": ["instrument_number", "book_page", "property_address", "legal_description", "party_1", "party_2", "execution_date", "assumed_encumbrances", "related_case_number"],
      "additionalProperties": false
    }
    ```

### 2. Mortgages (MTG, DOT, HELOC)
*   **Typical Content:** Mortgagor, Mortgagee, principal amount, maturity date, riders (PUD, Condo, 1-4 Family).
*   **Cross-Reference Value:**
    *   **Association Discovery:** PUD (Planned Unit Development) or Condominium Riders almost always state the **exact legal name** of the HOA or COA. This provides the exact search string needed to find association liens.
    *   **MERS MIN:** The Mortgage Electronic Registration Systems MIN number can be used to track unseen assignments online.
*   **OCR Vision Prompt:**
    ```text
    Extract the following data into JSON:
    1. instrument_number and book_page
    2. property_address and legal_description
    3. party_1 (Mortgagor/Borrower) and party_2 (Mortgagee/Lender)
    4. principal_amount and maturity_date
    5. mers_min: Extract the 18-digit MERS MIN number if present.
    6. association_name: Look specifically for PUD or Condominium Riders attached at the end and extract the exact legal name of the Homeowners or Condominium Association.

    Expected JSON Schema:
    {
      "type": "object",
      "properties": {
        "instrument_number": { "type": ["string", "null"] },
        "book_page": { "type": ["string", "null"] },
        "property_address": { "type": ["string", "null"] },
        "legal_description": { "type": ["string", "null"] },
        "party_1": { "type": ["string", "null"], "description": "Mortgagor/Borrower" },
        "party_2": { "type": ["string", "null"], "description": "Mortgagee/Lender" },
        "principal_amount": { "type": ["number", "null"] },
        "maturity_date": { "type": ["string", "null"], "description": "YYYY-MM-DD" },
        "mers_min": { "type": ["string", "null"], "description": "18-digit MERS MIN" },
        "association_name": { "type": ["string", "null"], "description": "From PUD/Condo Rider" }
      },
      "required": ["instrument_number", "book_page", "property_address", "legal_description", "party_1", "party_2", "principal_amount", "maturity_date", "mers_min", "association_name"],
      "additionalProperties": false
    }
    ```

### 3. Judgments (JUD, FJ)
*   **Typical Content:** Civil Case Number, Plaintiff, all adjudicated Defendants, final judgment amount, and the legal description of the property to be sold.
*   **Cross-Reference Value:**
    *   **Delta Signals:** As noted in the Audit Buckets, checking the defendants in the Final Judgment against the original Lis Pendens reveals junior lienholders who joined late.
    *   **Parent Instrument:** The FJ usually explicitly cites the O.R. Book and Page of the mortgage or lien being foreclosed.
*   **OCR Vision Prompt:**
    ```text
    Extract the following data into JSON:
    1. instrument_number and book_page
    2. property_address and legal_description
    3. civil_case_number
    4. party_1 (Plaintiff) and party_2 (List of ALL Adjudicated Defendants)
    5. judgment_amount
    6. foreclosed_instrument: Extract the Book and Page of the specific mortgage or lien this judgment is foreclosing on.

    Expected JSON Schema:
    {
      "type": "object",
      "properties": {
        "instrument_number": { "type": ["string", "null"] },
        "book_page": { "type": ["string", "null"] },
        "property_address": { "type": ["string", "null"] },
        "legal_description": { "type": ["string", "null"] },
        "civil_case_number": { "type": ["string", "null"] },
        "party_1": { "type": ["string", "null"], "description": "Plaintiff" },
        "party_2": { "type": ["array", "null"], "items": { "type": "string" }, "description": "List of all adjudicated defendants" },
        "judgment_amount": { "type": ["number", "null"] },
        "foreclosed_instrument": { "type": ["string", "null"], "description": "Book and Page of the specific mortgage/lien foreclosed" }
      },
      "required": ["instrument_number", "book_page", "property_address", "legal_description", "civil_case_number", "party_1", "party_2", "judgment_amount", "foreclosed_instrument"],
      "additionalProperties": false
    }
    ```

### 4. Liens (LN, HOA, MECH, CEL)
*   **Typical Content:** Lienor (Creditor), Lienee (Debtor), delinquency amount, and property description.
*   **Cross-Reference Value:**
    *   **Code Enforcement (CEL):** Often lists the specific municipal violation/case number, triggering a direct search in the local magistrate/code portal.
    *   **Mechanic's Liens (MECH):** Frequently references the original Notice of Commencement (NOC) Book/Page and the General Contractor's name.
*   **OCR Vision Prompt:**
    ```text
    Extract the following data into JSON:
    1. instrument_number and book_page
    2. property_address and legal_description
    3. party_1 (Lienor/Creditor) and party_2 (Lienee/Debtor/Owner)
    4. lien_amount
    5. referenced_noc: For Mechanic's Liens, extract the Book/Page of the related Notice of Commencement.
    6. municipal_case_number: For Code Enforcement Liens, extract the specific local municipal violation or case number.

    Expected JSON Schema:
    {
      "type": "object",
      "properties": {
        "instrument_number": { "type": ["string", "null"] },
        "book_page": { "type": ["string", "null"] },
        "property_address": { "type": ["string", "null"] },
        "legal_description": { "type": ["string", "null"] },
        "party_1": { "type": ["string", "null"], "description": "Lienor/Creditor" },
        "party_2": { "type": ["string", "null"], "description": "Lienee/Debtor/Owner" },
        "lien_amount": { "type": ["number", "null"] },
        "referenced_noc": { "type": ["string", "null"], "description": "Book/Page of related Notice of Commencement (for Mechanic's Liens)" },
        "municipal_case_number": { "type": ["string", "null"], "description": "Local municipal violation/case number (for Code Enforcement Liens)" }
      },
      "required": ["instrument_number", "book_page", "property_address", "legal_description", "party_1", "party_2", "lien_amount", "referenced_noc", "municipal_case_number"],
      "additionalProperties": false
    }
    ```

### 5. Lis Pendens (LP) - *The Rosetta Stone*
*   **Typical Content:** Civil Case Number, Plaintiff, list of all Defendants, Legal Description, and the instrument being foreclosed.
*   **Cross-Reference Value:**
    *   **Parent Linkage:** The LP explicitly names the O.R. Book and Page of the Mortgage or Lien being foreclosed.
    *   **Hidden Parties:** Any individual listed as a defendant who is *not* on the deed is likely a silent partner, an heir (indicating an unrecorded probate gap), or an unrecorded junior interest.
*   **OCR Vision Prompt:**
    ```text
    Extract the following data into JSON:
    1. instrument_number and book_page
    2. property_address and legal_description
    3. civil_case_number
    4. party_1 (Plaintiff) and party_2 (List of ALL Defendants and any other parties being notified, such as spouses, heirs, or junior lienholders)
    5. foreclosed_instrument: Extract the exact Official Records Book and Page of the Mortgage or Lien that triggered this Lis Pendens. WARNING: Do NOT extract "Plat Book" or "PB" references from the legal description here.

    Expected JSON Schema:
    {
      "type": "object",
      "properties": {
        "instrument_number": { "type": ["string", "null"] },
        "book_page": { "type": ["string", "null"] },
        "property_address": { "type": ["string", "null"] },
        "legal_description": { "type": ["string", "null"] },
        "civil_case_number": { "type": ["string", "null"] },
        "party_1": { "type": ["string", "null"], "description": "Plaintiff" },
        "party_2": { "type": ["array", "null"], "items": { "type": "string" }, "description": "List of all defendants and notified parties" },
        "foreclosed_instrument": { "type": ["string", "null"], "description": "Exact Official Records Book and Page of Mortgage/Lien (NOT Plat Book)" }
      },
      "required": ["instrument_number", "book_page", "property_address", "legal_description", "civil_case_number", "party_1", "party_2", "foreclosed_instrument"],
      "additionalProperties": false
    }
    ```

### 6. Assignments (ASG) & Modifications (MOD)
*   **Typical Content:** Assignor (old lender), Assignee (new lender), Modification terms.
*   **Cross-Reference Value:** 
    *   **Chain of Title Fix:** Both documents explicitly state the **O.R. Book and Page** of the original base mortgage. If a foreclosure Plaintiff doesn't match the original Mortgagee, reading the ASG bridges the `plaintiff_chain_gap`.
*   **OCR Vision Prompt:**
    ```text
    Extract the following data into JSON:
    1. instrument_number and book_page
    2. property_address (if present)
    3. party_1 (Assignor/Old Lender) and party_2 (Assignee/New Lender)
    4. parent_instrument: Extract the exact Book and Page of the original Mortgage or Lien being assigned or modified.

    Expected JSON Schema:
    {
      "type": "object",
      "properties": {
        "instrument_number": { "type": ["string", "null"] },
        "book_page": { "type": ["string", "null"] },
        "property_address": { "type": ["string", "null"] },
        "party_1": { "type": ["string", "null"], "description": "Assignor/Old Lender" },
        "party_2": { "type": ["string", "null"], "description": "Assignee/New Lender" },
        "parent_instrument": { "type": ["string", "null"], "description": "Exact Book and Page of original Mortgage/Lien assigned/modified" }
      },
      "required": ["instrument_number", "book_page", "property_address", "party_1", "party_2", "parent_instrument"],
      "additionalProperties": false
    }
    ```

### 7. Satisfactions & Releases (SAT, REL)
*   **Typical Content:** Releasor, Releasee, and a statement that a specific debt is paid.
*   **Cross-Reference Value:**
    *   **Parent Demise:** These documents *always* explicitly cite the **O.R. Book and Page** of the Mortgage or Lien being released. OCRing this allows us to instantly link the `sat_parent_gap` and mark the baseline encumbrance as dead.
*   **OCR Vision Prompt:**
    ```text
    Extract the following data into JSON:
    1. instrument_number and book_page
    2. property_address (if present)
    3. party_1 (Releasor/Creditor) and party_2 (Releasee/Debtor)
    4. parent_instrument: Extract the exact Book and Page of the Mortgage or Lien that is being satisfied or released. This is the most critical field.
    5. partial_release_flag: True if this only partially releases the property or debt, False if it is a full satisfaction.

    Expected JSON Schema:
    {
      "type": "object",
      "properties": {
        "instrument_number": { "type": ["string", "null"] },
        "book_page": { "type": ["string", "null"] },
        "property_address": { "type": ["string", "null"] },
        "party_1": { "type": ["string", "null"], "description": "Releasor/Creditor" },
        "party_2": { "type": ["string", "null"], "description": "Releasee/Debtor" },
        "parent_instrument": { "type": ["string", "null"], "description": "Exact Book and Page of Mortgage/Lien being satisfied/released" },
        "partial_release_flag": { "type": "boolean", "description": "True if partial release, False if full satisfaction" }
      },
      "required": ["instrument_number", "book_page", "property_address", "party_1", "party_2", "parent_instrument", "partial_release_flag"],
      "additionalProperties": false
    }
    ```

### 8. Notice of Commencement (NOC)
*   **Typical Content:** Owner name, General Contractor name, description of improvements, and expiration date (default 1 year).
*   **Cross-Reference Value:**
    *   **Permit & Lien Prediction:** The NOC provides the exact Contractor name needed to track potential Mechanic's Liens. If the "Owner" on the NOC doesn't match the record deed owner, it signals an unrecorded contract for deed or a recent unrecorded flip.
*   **OCR Vision Prompt:**
    ```text
    Extract the following data into JSON:
    1. instrument_number and book_page
    2. property_address and legal_description
    3. party_1 (Owner) and party_2 (General Contractor)
    4. expiration_date: Extract if explicitly stated (default is 1 year from recording if absent).
    5. improvements_description: Brief summary of the work being performed.

    Expected JSON Schema:
    {
      "type": "object",
      "properties": {
        "instrument_number": { "type": ["string", "null"] },
        "book_page": { "type": ["string", "null"] },
        "property_address": { "type": ["string", "null"] },
        "legal_description": { "type": ["string", "null"] },
        "party_1": { "type": ["string", "null"], "description": "Owner" },
        "party_2": { "type": ["string", "null"], "description": "General Contractor" },
        "expiration_date": { "type": ["string", "null"], "description": "If explicitly stated, YYYY-MM-DD" },
        "improvements_description": { "type": ["string", "null"] }
      },
      "required": ["instrument_number", "book_page", "property_address", "legal_description", "party_1", "party_2", "expiration_date", "improvements_description"],
      "additionalProperties": false
    }
    ```