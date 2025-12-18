# Lien Survival Strategy & Logic

## Overview
Accurately predicting which liens will survive a foreclosure sale is critical for estimating post-foreclosure liability. This document outlines the strategy and logic implemented in `src/services/lien_survival_analyzer.py`.

## Core Logic: "First in Time, First in Right" vs. Exceptions
Florida generally follows "First in Time, First in Right," meaning priority is determined by the recording date relative to the *Lis Pendens* of the foreclosing action. However, significant statutory exceptions apply.

## Survival Hierarchy

### 1. Superpriority Liens (Always Survive)
These liens survive **ALL** foreclosures (Mortgage, HOA, and usually Tax Deed sales).
*   **Ad Valorem Property Taxes:** The most senior lien.
*   **PACE Liens (Property Assessed Clean Energy):** Treated as non-ad valorem assessments, they have equal dignity to taxes and survive.
*   **Municipal Utility Liens:** Liens for water/sewer services (under Fla. Stat. 159.17) often have superpriority status.
*   **Federal Tax Liens (IRS):**
    *   Technically junior if recorded after the mortgage.
    *   **CRITICAL NUANCE:** Even if "extinguished," the IRS retains a **120-day Right of Redemption** from the date of the sale. This clouds the title effectively making it a surviving liability for 4 months.

### 2. Foreclosing Lien
The lien initiating the foreclosure (the Plaintiff's lien).
*   **Status:** "FORECLOSING"
*   **Outcome:** Satisfied from the sale proceeds (up to the judgment amount). It does not survive as a lien against the property (it merges into the title), but the debt is what drives the auction.

### 3. Senior Liens
Liens recorded **BEFORE** the *Lis Pendens* of the foreclosing action (and before the foreclosing mortgage).
*   **Status:** "SURVIVED"
*   **Outcome:** The new owner takes title *subject to* these liens. They must be paid off.

### 4. Junior Liens
Liens recorded **AFTER** the *Lis Pendens* (or specifically named as junior defendants).
*   **Status:** "EXTINGUISHED"
*   **Outcome:** Wiped off the title by the issuance of the Certificate of Title.
*   **Includes:** Second mortgages, HELOCs, personal judgments, credit card liens, etc.

### 5. HOA/COA Foreclosure Specifics (Safe Harbor)
When an HOA/COA forecloses:
*   **First Mortgage:** SURVIVES. (Fla. Stat. 720.3085 / 718.116).
*   **Safe Harbor Liability:** The First Mortgagee (bank) or their assignee is liable for the *lesser* of:
    *   12 months of unpaid assessments.
    *   1% of the original mortgage debt.
*   **Third-Party Bidders:** DO NOT get Safe Harbor protection. They are liable for **ALL** past due assessments + interest + legal fees. *Our system currently calculates the Safe Harbor amount, but investors should be wary.*

## Implementation Details

### Identifying the Foreclosing Lien
We identify the foreclosing lien not just by Plaintiff name (which is fuzzy), but by extracting the specific **Mortgage Recording Information** (Book/Page or Instrument Number) cited in the **Final Judgment**.
*   **Source:** `src/services/final_judgment_processor.py` extracts `foreclosed_mortgage` data.
*   **Matching:** `LienSurvivalAnalyzer` matches this against the `encumbrances` table.

### Federal Lien Handling
*   We flag Federal Liens (IRS, USA, Dept of Justice) specifically.
*   If they are junior, they are marked as `EXTINGUISHED (Redemption Right)` to warn the user of the 120-day cloud.

### Municipal vs. Code Enforcement
*   **Municipal Utility (Water/Sewer):** Superpriority (Survives).
*   **Code Enforcement:** generally **NOT** superpriority over a prior mortgage (per FL Supreme Court). They are treated based on recording date (Junior = Extinguished, Senior = Survives).

## Code References
*   **Analyzer:** `src/services/lien_survival_analyzer.py`
*   **Data Extraction:** `src/services/final_judgment_processor.py`
*   **Pipeline Integration:** `src/pipeline.py` (Step 6)
