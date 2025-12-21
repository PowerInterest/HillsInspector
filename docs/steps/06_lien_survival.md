# Lien Research Documentation

## Overview
This document outlines the process and resources for researching liens, mortgages, deeds, and other property records for foreclosure analysis in Hillsborough County, Florida.

---

## Data Sources

### 1. Official Records Index (ORI) - PRIMARY SOURCE FOR PROPERTY RECORDS
**Website:** https://publicaccess.hillsclerk.com/oripublicaccess/

**Purpose:** Search for property-related documents including:
- Mortgages (first, second, etc.)
- Deeds (Warranty Deed, Quit Claim, etc.)
- Liens (tax liens, mechanic's liens, judgment liens)
- Assignments
- Releases/Satisfactions
- Lis Pendens
- Notices of Commencement

**How to Use:**
1. Navigate to the ORI website
2. **Set Document Type** - Choose the specific document type you're searching for:
   - Mortgage
   - Deed
   - Lien
   - Lis Pendens
   - Assignment
   - Release/Satisfaction
3. **Build Query** - Search by:
   - Parcel ID (Folio Number)
   - Name (Grantor/Grantee)
   - Book and Page
   - Document Number
   - Date Range

**Key Information:**
- This is the **Official Records Index** maintained by the Clerk of Court
- Contains ALL recorded documents affecting property ownership
- Searchable back to historical records
- Can search by parcel ID to find ALL documents recorded against a property
- Critical for determining lien priority and ownership chain

---

### 2. HOVER (Court Case Documents) - SECONDARY SOURCE
**Website:** https://hover.hillsclerk.com

**Purpose:** Search for court case documents including:
- Final Judgments
- Lis Pendens (court filing)
- Court orders
- Case dockets
- Pleadings

**Current Status:** 
?? **SCRAPER NEEDS FIXING**
- Homepage shows navigation buttons, not direct search form
- Need to navigate to: `/html/case/caseSearch.html`
- Can search by:
  - Case Number
  - Citation Number
  - Party Name
  - Date Filed
  - Court Date

**How to Use:**
1. Navigate to HOVER homepage
2. Click "Case Number" search button
3. Fill in case search form:
   - Year: 2020
   - Type: CA (Civil)
   - Sequence: 001241
4. Search and click on case result
5. View docket and documents

---

## Lien Research Workflow

### Step 1: Identify the Property
- **Input:** Parcel ID from foreclosure case
- **Example:** 1828243EN000007000350A

### Step 2: Search Official Records Index
1. Go to https://publicaccess.hillsclerk.com/oripublicaccess/
2. Search by Parcel ID (Folio)
3. Filter by Document Type:
   - **Mortgages** - Look for all mortgages (1st, 2nd, HELOC, etc.)
   - **Liens** - Tax liens, judgment liens, HOA liens, mechanic's liens
   - **Lis Pendens** - Foreclosure filing date (CRITICAL for priority)
   - **Deeds** - Verify current ownership
4. Record each document:
   - Document Type
   - Recording Date
   - Book and Page OR Document Number
   - Principal Amount (if loan/lien)
   - Lender/Lienholder Name

### Step 3: Determine Lis Pendens Date
- **Critical:** The Lis Pendens date determines which liens survive
- Search ORI for "Lis Pendens" documents on the parcel
- The foreclosure Lis Pendens date is the cutoff
- Liens recorded BEFORE Lis Pendens generally survive
- Liens recorded AFTER Lis Pendens are wiped out

### Step 4: Analyze Lien Priority

**Florida Lien Priority (General Rule):**
1. **Tax Liens** (County, City, IRS) - ALWAYS SURVIVE (superpriority)
2. **First Mortgage** (if foreclosing, this is being paid off)
3. **HOA Liens** - Complex rules, see "Safe Harbor" section below
4. **Second Mortgages** - Wiped out if first mortgage forecloses
5. **Judgment Liens** - Depends on recording date vs. Lis Pendens
6. **Mechanic's Liens** - Can relate back to Notice of Commencement date

**Priority by Date Rule:**
- Among same-priority liens, earlier recording date = higher priority
- Exception: Tax liens always have priority regardless of recording date
- Exception: Mechanic's liens can relate back to Notice of Commencement

---

## Florida Lien Survival Logic (Implementation)

### Core Principle: Senior vs Junior Liens
When a **junior lien** forecloses, all **senior liens survive**. The foreclosure buyer takes title subject to those senior liens.

**Example:** If an HOA forecloses (junior position), the first mortgage survives and remains attached to the property.

### What ALWAYS Survives Foreclosure
These liens have **superpriority** and survive regardless of who forecloses:

| Lien Type | Statute | Notes |
|-----------|---------|-------|
| **Property Tax Liens** | Fla. Stat. 197 | County/City taxes always survive |
| **IRS Tax Liens** | Federal law | Federal tax liens survive |
| **Municipal Utility Liens** | Local ordinance | Water/sewer often have superpriority |
| **Code Enforcement Liens** | Varies by municipality | Tampa code liens can survive |

### What Gets Wiped Out
When a **first mortgage** forecloses, these are typically extinguished:

| Lien Type | Survives? | Condition |
|-----------|-----------|-----------|
| **Second Mortgage** | ? NO | Junior to first mortgage |
| **HELOC** | ? NO | Usually junior |
| **Judgment Liens** | ? NO | If recorded after first mortgage |
| **HOA Liens** | ?? PARTIAL | See Safe Harbor below |

### Florida HOA "Safe Harbor" Rules

**Florida is NOT a super lien state.** HOA/COA liens do not automatically take priority over first mortgages.

**Relevant Statutes:**
- **Fla. Stat. 720.3085** - Homeowners Associations (HOA)
- **Fla. Stat. 718.116** - Condominium Associations (COA)

**The 12-Month / 1% Rule:**
When a first mortgagee forecloses and acquires title, their liability to the HOA is LIMITED to the **lesser of**:
1. **12 months** of unpaid regular assessments, OR
2. **1% of the original mortgage debt**

**Example Calculation:**
```
Original Mortgage:     $300,000
Monthly HOA Dues:      $400
Months Unpaid:         24 (2 years)

Option 1: 12 months x $400 = $4,800
Option 2: 1% x $300,000   = $3,000

Safe Harbor Amount = $3,000 (lesser of the two)

Actual HOA debt: 24 x $400 = $9,600
Bank pays:                   $3,000
Remaining unpaid:           $6,600 (may follow previous owner)
```

**Important:** The Safe Harbor only applies if the HOA was **named as a party** in the foreclosure action. Check the case docket.

### Mechanic's Lien Relation-Back

Mechanic's liens can "relate back" to the **Notice of Commencement (NOC)** recording date, potentially jumping ahead of mortgages recorded after the NOC.

**Logic:**
1. Find NOC recording date in ORI
2. If mortgage was recorded AFTER NOC date, mechanic's lien may have priority
3. Mechanic's liens expire 1 year from recording if not renewed (Fla. Stat. 713.22)

### Lien Survival Decision Tree

```
INPUT: Foreclosing Lien Type, Target Lien Type, Recording Dates

1. Is target lien a TAX LIEN?
    YES: SURVIVES (always superpriority)

2. Is target lien a CODE ENFORCEMENT LIEN?
    YES: Check municipality - Tampa code liens often survive

3. Is foreclosing lien the FIRST MORTGAGE?
    YES:
      - Second mortgages: WIPED OUT
      - Judgment liens recorded after 1st mtg: WIPED OUT
      - HOA liens: Apply Safe Harbor (12mo / 1% rule)
      - Mechanic's liens: Check NOC date

4. Is foreclosing lien an HOA/COA?
    YES: First mortgage SURVIVES (senior position)

5. Is foreclosing lien a SECOND MORTGAGE?
    YES: First mortgage SURVIVES (senior position)

6. Default: Compare recording dates
    Earlier recording date = higher priority = survives
```

## Orchestrator Integration (Parallel Pipeline)

### Phase 3: Survival Analysis
Lien survival analysis runs as Phase 3 in the orchestrator, after ORI ingestion completes:

```python
# PHASE 3: Lien Survival Analysis (Depends on encumbrances from Phase 2)
logger.info(f"Phase 3: Lien Survival Analysis for {parcel_id}")

# Skip logic: already analyzed for this case
last_case = db.get_last_analyzed_case(parcel_id)
if db.folio_has_survival_analysis(parcel_id) and last_case == case_number:
    logger.debug(f"Skipping survival for {parcel_id} - already analyzed")
    db.mark_step_complete(case_number, "needs_lien_survival")
else:
    await self._analyze_survival(parcel_id, case_number)
```

### Survival Analysis Flow
```python
async def _analyze_survival(self, parcel_id: str, case_number: str):
    """Analyze lien survival for a property."""
    # Get auction data for foreclosure type
    auction = self.db.get_auction_by_case(case_number)
    foreclosure_type = auction.get("foreclosure_type", "MORTGAGE")

    # Get all encumbrances for this property
    encumbrances = self.db.get_encumbrances_for_folio(parcel_id)

    # Get lis pendens date (critical for priority)
    lis_pendens_date = auction.get("lis_pendens_date")

    # Analyze each encumbrance
    for enc in encumbrances:
        result = self.survival_analyzer.analyze(
            encumbrance=enc,
            foreclosure_type=foreclosure_type,
            lis_pendens_date=lis_pendens_date
        )

        # Update encumbrance with survival status
        await self.db_writer.enqueue("update_encumbrance_survival", {
            "id": enc["id"],
            "survival_status": result.status,
            "survival_reason": result.reason,
            "surviving_amount": result.amount
        })

    # Mark analysis complete
    db.mark_step_complete(case_number, "needs_lien_survival")
    db.mark_as_analyzed(parcel_id)
    db.set_last_analyzed_case(parcel_id, case_number)
```

### Defendant Names Extraction
The orchestrator extracts defendant names for party matching:

```python
# Get defendant names from auction data
defendant = auction_dict.get('defendant')
defendant_names = []

if defendant:
    if isinstance(defendant, list):
        defendant_names = defendant
    else:
        # Single defendant string
        defendant_names = [defendant]
```

### Implementation Code Structure

```python
# src/analyzers/lien_survival.py

class LienSurvivalAnalyzer:
    """Determines which liens survive a foreclosure."""

    SUPERPRIORITY_TYPES = [
        'TAX_LIEN',
        'IRS_LIEN',
        'MUNICIPAL_UTILITY',
        'CODE_ENFORCEMENT'  # Check municipality
    ]

    def calculate_hoa_safe_harbor(
        self,
        original_mortgage_amount: float,
        monthly_hoa_dues: float,
        months_unpaid: int
    ) -> float:
        """
        Calculate HOA safe harbor amount per Fla. Stat. 720.3085
        Returns the MAXIMUM the foreclosure buyer owes to HOA.
        """
        option_1 = min(months_unpaid, 12) * monthly_hoa_dues
        option_2 = original_mortgage_amount * 0.01
        return min(option_1, option_2)

    def does_lien_survive(
        self,
        foreclosing_lien: Lien,
        target_lien: Lien,
        lis_pendens_date: date
    ) -> tuple[bool, str]:
        """
        Determine if target_lien survives the foreclosure.
        Returns (survives: bool, reason: str)
        """
        # Superpriority liens always survive
        if target_lien.type in self.SUPERPRIORITY_TYPES:
            return True, "Superpriority lien - always survives"

        # If target is senior (recorded earlier), it survives
        if target_lien.recording_date < foreclosing_lien.recording_date:
            return True, "Senior lien - recorded before foreclosing lien"

        # Junior liens get wiped out
        return False, "Junior lien - extinguished by foreclosure"
```

## Encumbrance Analysis (TitleChainService)

The `TitleChainService._analyze_encumbrances()` method determines which encumbrances are open vs. satisfied, tracking assignments.

### Document Classification

```python
# Encumbrance document types
encumbrance_keywords = ['MORTGAGE', 'LIEN', 'JUDGMENT', 'LIS PENDENS', 'TAX']

# Satisfaction types (NOT partial releases)
satisfaction_keywords = ['SATISFACTION', 'RELEASE', 'RECONVEYANCE', 'DISCHARGE']
partial_keywords = ['PARTIAL']  # Partial releases don't satisfy fully

# Modifications (change creditor, not satisfaction)
modification_keywords = ['MODIFICATION', 'ASSIGNMENT', 'AMENDMENT']

# Restrictions (don't affect survival)
restriction_keywords = ['EASEMENT', 'RESTRICTION', 'COVENANT', 'DECLARATION', 'PLAT']
```

### Encumbrance Object Structure

```python
enc_obj = {
    'type': 'MORTGAGE',
    'date': '2020-01-15',
    'amount': 250000.00,
    'original_creditor': 'BANK OF AMERICA, N.A.',
    'current_creditor': 'NATIONSTAR MORTGAGE LLC',  # Updated by assignments
    'debtor': 'JOHN SMITH',
    'book_page': '12345/678',
    'instrument': '2020012345',
    'status': 'OPEN',  # or 'SATISFIED'
    'satisfaction_ref': None,  # Instrument # of satisfaction
    'match_method': None,  # 'REF_MATCH' or 'NAME_MATCH'
    'assignments': [
        {
            'date': '2021-06-01',
            'assignee': 'NATIONSTAR MORTGAGE LLC',
            'instrument': '2021067890'
        }
    ]
}
```

### Satisfaction Matching Algorithm

Events (assignments and satisfactions) are processed chronologically:

```python
for event in sorted(events, key=lambda x: parse_date(x['recording_date'])):

    # 1. Try to match by instrument/book-page reference
    target_enc = None
    ref_bk_pgs, ref_insts = extract_refs(event.get('legal_description', ''))

    # Check instrument match
    for ref_inst in ref_insts:
        if ref_inst in active_map:
            target_enc = active_map[ref_inst]
            break

    # Check book/page match
    if not target_enc:
        for ref_bk, ref_pg in ref_bk_pgs:
            if f"{ref_bk}/{ref_pg}" in active_map:
                target_enc = active_map[f"{ref_bk}/{ref_pg}"]
                break

    # 2. Fallback: Name match for satisfactions only
    if not target_enc and event_type == 'SATISFACTION':
        releasor = event.get('party1', '')
        for enc in open_encumbrances:
            if NameMatcher.are_linked(enc['current_creditor'], releasor, threshold=0.85):
                target_enc = enc
                enc['match_method'] = 'NAME_MATCH'
                break

    # 3. Apply event (only if recorded AFTER the encumbrance)
    if target_enc and event_date > enc_date:
        if event_type == 'ASSIGNMENT':
            # Update creditor
            target_enc['current_creditor'] = event.get('party2')
            target_enc['assignments'].append({...})
        elif event_type == 'SATISFACTION':
            target_enc['status'] = 'SATISFIED'
            target_enc['satisfaction_ref'] = event.get('instrument_number')
```

### Assignment Tracking

Mortgages are frequently assigned between servicers. The system tracks the current creditor:

```python
# Original: Bank of America
# Assignment 1: Bank of America → Nationstar
# Assignment 2: Nationstar → Mr. Cooper

# Final current_creditor: "MR. COOPER"
# original_creditor still: "BANK OF AMERICA, N.A."
```

This is critical because satisfactions name the **current** creditor, not the original lender.

### Name Match Fallback

When satisfactions don't include explicit book/page references, we match by party name:

```python
# Satisfaction: "NATIONSTAR MORTGAGE LLC releases..."
# Find OPEN encumbrance where current_creditor matches

if NameMatcher.are_linked(
    "NATIONSTAR MORTGAGE LLC",   # Releasor in satisfaction
    "NATIONSTAR MORTGAGE, LLC",  # Current creditor in encumbrance
    threshold=0.85
):
    # Match found - mark as satisfied
```

This uses the same NameMatcher logic as chain of title linking (stopword removal, fuzzy matching, alias resolution).

**Sources:**
- [ProTitleUSA: What Liens Survive Foreclosure?](https://protitleusa.com/lienssurvivefc)
- [EasyTitleSearch: Which liens survive a Florida foreclosure](https://easytitlesearch.com/articles/which-liens-survive-a-florida-foreclosure/)
- [LegalClarity: Is Florida a Super Lien State?](https://legalclarity.org/is-florida-a-super-lien-state-for-hoa-coa-liens/)
- [Florida Statutes 720.3085](https://www.leg.state.fl.us/statutes/index.cfm?App_mode=Display_Statute&URL=0700-0799/0720/Sections/0720.3085.html)

### Step 5: Calculate Surviving Liens

**Example for Case 292020CA001241A001HC:**

```
Property Value:           $138,609.00
First Mortgage (foreclosing):  $37,493.88

Hypothetical Liens Found in ORI:
1. Tax Lien (2019):              $2,500.00   SURVIVES (tax lien)
2. HOA Lien (2020):              $3,000.00   SURVIVES (recorded before LP)
3. Second Mortgage (2018):      $25,000.00   WIPED OUT (junior to 1st)
4. Judgment Lien (2021):         $5,000.00   WIPED OUT (after Lis Pendens)

Surviving Liens Total:           $5,500.00

True Equity Calculation:
  Assessed Value:          $138,609.00
- First Mortgage:         - $37,493.88
- Surviving Tax Lien:     -  $2,500.00
- Surviving HOA Lien:     -  $3,000.00
?????????????????????????????????????
= TRUE EQUITY:             $95,615.12
```

---

## Technical Implementation

### Document Types in ORI

When querying the ORI system, use these document type codes:

```python
DOCUMENT_TYPES = {
    'MORTGAGE': 'MTG',
    'DEED': 'DEED',
    'LIEN': 'LIEN',
    'LIS_PENDENS': 'LP',
    'ASSIGNMENT': 'ASGN',
    'RELEASE': 'REL',
    'SATISFACTION': 'SAT',
    'NOTICE_COMMENCEMENT': 'NOC',
    'FINAL_JUDGMENT': 'FJ',
}
```

### Query Building Example

**Search by Parcel ID:**
```
Base URL: https://publicaccess.hillsclerk.com/oripublicaccess/
Document Type: Mortgage
Parcel/Folio: 1828243EN000007000350A
Date Range: 01/01/2000 to 11/23/2025
```

**Search by Name:**
```
Grantor Name: [Current Owner Name]
Document Type: All
Date Range: [As needed]
```

---

## Important Notes

### Tax Liens
- **ALWAYS check with Tax Collector separately**
- Website: https://www.hcpafl.org/
- Tax liens ALWAYS survive foreclosure
- Can accumulate quickly (penalties + interest)
- Check both current year and back taxes

### HOA Liens
- Florida is NOT a super lien state
- **Safe Harbor Rule:** When first mortgage forecloses, buyer liability limited to:
  - Lesser of: 12 months unpaid dues OR 1% of original mortgage
  - Per Fla. Stat. 720.3085 (HOA) and 718.116 (COA)
- Safe Harbor only applies if HOA named as party in foreclosure
- Check HOA estoppel letter for exact amount (manual process)

### Code Enforcement Liens
- City/County code violations
- Can survive foreclosure
- Check municipal records

### Mechanic's Liens
- Have special priority rules
- Can relate back to Notice of Commencement date
- Usually expire after 1 year if not renewed

---

## Scraper Implementation Status

### ? Working:
- **HCPA Scraper** (Property Appraiser - owner info, assessed value)
- **ORI Scraper (Deep Search)**
  - Uses OnBase Direct Search Endpoints (CQID 319, 326, 318, 321)
  - Bypasses main page rate limiting and complexity
  - PDF Download working
  - Integration with analysis pipeline complete
- **Property enrichment pipeline**
- **Database storage** (Auctions, Parcels, Liens tables)
- **Lien Priority Logic** (Basic date-based analysis implemented)

### ?? Needs Attention:
- **HOVER Scraper** (Court Case Documents)
  - **BLOCKED**: PerimeterX Bot Detection on main site.
  - **SOLUTION**: Investigate using **CQID 324-348** (OnBase Court Case Search) to bypass the frontend blocker.
  - Workaround: Currently relying on ORI Scraper for lien research.

### ?? To Be Implemented / Refined:
- **Court Case Integration**: Implement `CourtCaseScraper` using CQID 324-348.
- **Advanced PDF Parsing**: Extracting exact dollar amounts from downloaded PDFs.
- **Tax Collector Integration**: Direct check for tax certificates.
- **HOA Estoppel**: Automated request (likely manual).

---

## Next Steps

1. **Refine Lien Analysis**
   - Improve "Lis Pendens" date detection (currently estimates or uses first found)
   - Add logic to parse lien amounts from document text (OCR/Text extraction)

2. **Scale Up**
   - Run analysis on batch of auctions
   - Flag properties with "Clean Title" (high equity, no surviving liens)

3. **Verification**
   - Cross-reference with manual research
   - Validate lien amounts
   - Confirm priority calculations

---

## Resources

- **Hillsborough County Official Records:** https://publicaccess.hillsclerk.com/oripublicaccess/
- **HOVER Court Records:** https://hover.hillsclerk.com
- **Property Appraiser:** https://www.hcpafl.org/
- **Tax Collector:** https://www.hctax.net/
- **Florida Lien Law:** Florida Statutes Chapter 55, 713, 718

---

## Contact Information

**Hillsborough County Clerk of Court**
- Phone: (813) 276-8100
- Address: 800 E Twiggs St, Tampa, FL 33602

**Property Appraiser**
- Phone: (813) 272-6100
- Website: https://www.hcpafl.org/

**Tax Collector**
- Phone: (813) 635-5200
- Website: https://www.hctax.net/

