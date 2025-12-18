# Chain of Title Construction Strategy

## Overview
Constructing an accurate chain of title is difficult due to missing digital records, non-standard document types, name variations, and data gaps. This document outlines the improved "Anchor & Fill" algorithm designed to robustly build ownership timelines and handle common failures found in the database (e.g., missing deeds, implied ownership).

## The "Anchor & Fill" Algorithm

The core concept is to treat **Deeds** as "Anchors" that define definitive ownership periods, and then use **Support Documents** (Mortgages, Liens) to "Fill" gaps where deeds are missing by inferring ownership.

### Logic Flowchart

```mermaid
flowchart TD
    %% --- PREPARATION ---
    subgraph PREP ["Phase 1: Preparation"]
        Input[Raw Document List] --> Normalize[Normalize Doc Types & Dates]
        Normalize --> Sort[Sort by Recording Date (Ascending)]
        Sort --> Classify{Classify Doc Type}
        
        Classify -- "Deed / Transfer" --> List_Anchors[Anchor List]
        Classify -- "Encumbrance (Mtg, Lien)" --> List_Support[Support List]
        Classify -- "Probate / Death" --> List_Events[Event List]
    end

    %% --- SKELETON BUILD ---
    subgraph SKELETON ["Phase 2: Build Skeleton Chain"]
        List_Anchors --> Build_Segs[Create Ownership Segments]
        Build_Segs --> Link_Check{Check Sequential Links}
        
        Link_Check -- "Grantee == Next Grantor" --> Link_Solid[Solid Link]
        Link_Check -- "Grantee ~= Next Grantor" --> Link_Fuzzy[Fuzzy Link (Name Var)]
        Link_Check -- "Mismatch" --> Gap_Detected[GAP DETECTED]
    end

    %% --- GAP FILLING ---
    subgraph FILL ["Phase 3: Gap Analysis & Inference"]
        Gap_Detected --> Scan_Gap[Scan Support Docs in Gap Interval]
        Scan_Gap --> Find_Signer{Found Mortgage/Lien Grantor?}
        
        Find_Signer -- "Yes (New Name)" --> Create_Implied[Create 'Implied Ownership' Segment]
        Create_Implied --> Re_Link[Insert into Chain]
        
        Find_Signer -- "No" --> Mark_Broken[Flag: BROKEN CHAIN]
    end

    %% --- FINALIZATION ---
    subgraph FINAL ["Phase 4: Finalization"]
        Link_Solid --> Final_Chain
        Link_Fuzzy --> Final_Chain
        Re_Link --> Final_Chain
        Mark_Broken --> Final_Chain
        
        Final_Chain --> Calc_Equity[Calculate Equity/Encumbrances]
    end

    %% Styles
    style Gap_Detected fill:#ffcccc,stroke:#cc0000
    style Create_Implied fill:#e6f3ff,stroke:#0066cc
    style Link_Solid fill:#ccffcc,stroke:#006600
```

## Detailed Logic Breakdown

### 1. Document Classification
We must expand our definition of "Deed" and "Owner Activity".

*   **Anchor Documents (Transfers):**
    *   `WARRANTY DEED`, `QUIT CLAIM DEED`, `SPECIAL WARRANTY DEED`
    *   `CERTIFICATE OF TITLE` (Foreclosure outcome)
    *   `TAX DEED` (Resets chain, usually)
    *   `AGREEMENT FOR DEED` / `CONTRACT FOR DEED` (Treat as transfer)
    *   `PROBATE` / `ORDER OF SUMMARY ADMINISTRATION` (Transfers to heirs)
*   **Support Documents (Evidence of Ownership):**
    *   `MORTGAGE` (Signed by Owner)
    *   `NOTICE OF COMMENCEMENT` (Signed by Owner)
    *   `LIS PENDENS` (Names Owner as Defendant)
    *   `HOA LIEN` (Names Owner)

### 2. Linking Logic (The "Handshake")
For every deed $D_i$ and the next deed $D_{i+1}$:
*   **Ideal:** $Grantee(D_i) == Grantor(D_{i+1})$
*   **Fuzzy:** $Grantee(D_i) \approx Grantor(D_{i+1})$ (e.g., "Smith John" vs "Smith John A")
*   **Self-Transfer:** $Grantee(D_i) == Grantor(D_{i+1})$ AND $Grantee(D_{i+1})$ includes $Grantor(D_{i+1})$. (e.g. Adding a spouse). This is a **Continuous Chain**, not a break.

### 3. Gap Filling (The "Implied Owner")
If $Grantee(D_i) \neq Grantor(D_{i+1})$, we have a **GAP**.
*   **Interval:** Time between $Date(D_i)$ and $Date(D_{i+1})$.
*   **Search:** Look for *Support Documents* recorded in this interval.
*   **Logic:**
    *   If we find a `MORTGAGE` signed by "Person X" in the middle of the gap...
    *   AND "Person X" is neither the previous Grantee nor the next Grantor...
    *   **Inference:** "Person X" likely acquired the property via an unrecorded or missed deed.
    *   **Action:** Insert an **Implied Ownership Segment** for "Person X".
    *   **Start Date:** Date of first Support Document.
    *   **Source:** "Implied by Mortgage [Instrument #]".

### 4. Handling "No Deed" Scenarios
If a property has documents (e.g., Liens, NOCs) but **zero** deeds found (common in very old records or bad OCR):
*   **Fallback:** Sort all *Support Documents*.
*   **Grouping:** Group by "Party 1" (Grantor/Owner).
*   **Timeline:** Construct a timeline based purely on who was signing mortgages/NOCs and when.
*   **Flag:** Mark chain as **"Inferred (No Deeds)"**.

## Failure Modes & Error Handling

| Scenario | Detection | Resolution | Status Flag |
| :--- | :--- | :--- | :--- |
| **Missing Deed** | Grantor of $D_{i+1}$ does not match Grantee of $D_i$. | Search for Implied Owner via Mortgages. | `CHAIN_GAP_FILLED` or `CHAIN_BROKEN` |
| **Name Variation** | "John Smith" vs "John A. Smith" | Levenshtein distance or Token Set Ratio match > 85%. | `FUZZY_MATCH` |
| **Circular Transfer** | A -> B, B -> A, A -> C | Detect Grantee=PrevGrantor. Treat as correction/refinance. | `SELF_TRANSFER` |
| **Zero Deeds** | Doc count > 0, Deed count = 0 | Build Implied Chain from Mortgages/NOCs. | `INFERRED_CHAIN` |
| **Tax Deed** | Doc Type = `TAX DEED` | **Hard Reset.** Previous chain is wiped/irrelevant for encumbrances (mostly). | `TAX_DEED_RESET` |
| **GSE Transfer** | Grantee = `FANNIE MAE` / `FREDDIE MAC` | **Soft Reset.** Indicates recent foreclosure. Title likely scrutinized but sold via Special Warranty Deed. | `GSE_REO` |

## Database Schema Updates (Proposed)
To support this, the `chain_of_title` table needs to store the "Link Quality":

```sql
ALTER TABLE chain_of_title ADD COLUMN link_status VARCHAR; -- 'VERIFIED', 'FUZZY', 'IMPLIED', 'BROKEN'
ALTER TABLE chain_of_title ADD COLUMN confidence_score FLOAT; -- 0.0 to 1.0

## 6. Scope & Duration: The 30-Year Standard (MRTA)

Per Florida's **Marketable Record Title Act (MRTA)** (Chapter 712, Fla. Stat.), we do not need to trace title back to the original land grant.

*   **Root of Title:** We must find a title transaction (Deed) recorded **at least 30 years prior** to the current date.
*   **Search Duration:** The chain must remain continuous from the "Root of Title" to the present day.
*   **Goal:** Establish a verified chain for >30 years. Any defects older than the Root of Title are typically cured by MRTA (with exceptions for easements, etc., which are handled separately).

**Algorithm Adjustment:**
1.  Target a "Root Deed" recorded $\le$ `Current Date - 30 Years`.
2.  If found, stop searching backwards (unless a gap exists immediately after).
3. If not found, flag as **"Insufficent History ( < 30 Years)"**.

## 7. MERS Handling Strategy (The "Invisible Chain")

**MERS (Mortgage Electronic Registration Systems)** acts as a "Nominee" for lenders, creating unique challenges because loan transfers happen privately within the MERS database, not in the public County Records.

### A. The "Black Box" Phenomenon
*   **Scenario:** Mortgage recorded with "MERS as nominee for Lender A".
*   **Reality:** Lender A sells to Lender B, then Lender C.
*   **Public Record:** *No assignments are recorded.* The Mortgage remains with MERS.
*   **Strategy:** Do **NOT** flag "Missing Assignments" as a chain break if MERS is the mortgagee. This is "Feature, not Bug" behavior for MERS.

### B. The Foreclosure Signal
*   **Event:** `ASSIGNMENT OF MORTGAGE` from **MERS** $\rightarrow$ **Specific Bank** (e.g., Wells Fargo).
*   **Meaning:** This is a strong **Pre-Foreclosure Signal**. Banks typically "assign out" of MERS immediately before filing a foreclosure lawsuit so they can sue in their own name.

### C. Matching Satisfactions
*   **Rule:** A Satisfaction/Release from MERS is **valid** and clears the lien, regardless of who the original "Nominee for" lender was.
*   **Logic:** `Creditor(Mortgage) = MERS` matches `Releasor(Satisfaction) = MERS`.

## 8. Partial Releases & Modifications

### A. Partial Release (The "Blanket" Problem)
A **Partial Release** clears a lien from *some* land but keeps it active on the rest.
*   **Scenario:** A developer has a mortgage on a 50-lot subdivision. As they sell Lot 1, they record a Partial Release for *only* Lot 1.
*   **Logic:**
    *   **If Legal Description Matches Target:** Treat as **SATISFIED**. (The lien is gone for *this* house).
    *   **If Legal Description Does NOT Match:** Treat as **ACTIVE**. (The lien still exists, just not on the neighbor's land).
    *   *Note:* This requires robust Legal Description parsing. If unsure, flag as **"CHECK PARTIAL"**.

### B. Modifications
*   **Loan Modification:** Changes terms (rate, maturity) but **preserves** the original lien priority.
*   **Action:** Do **NOT** treat as a new lien or a satisfaction. Link it to the original mortgage.

## 9. Evidence & Linking (Trust But Verify)

Professional title examination requires verifying the source document. We must provide **dual linking** for every node in the chain (Deeds, Mortgages, Liens).

1.  **Local Link (Fast):** Direct access to the cached PDF stored in our system (e.g., `data/properties/{folio}/documents/`). This ensures availability and speed.
2.  **County Link (Official):** A deep link to the Hillsborough County Clerk (ORI) page for that specific Instrument Number. This validates the "Source of Truth".

**Requirement:** Every `ChainLink` object must contain:
*   `local_path`: Path to stored PDF.
*   `ori_url`: Direct URL to official record.

## 10. Professional Standards Checklist (Florida)

To ensure our automated analysis meets professional standards, we map our features to the standard components of a **Florida Title Commitment (Schedule B-1 & B-2)**.

| Component | Industry Requirement | Our Implementation | Status |
| :--- | :--- | :--- | :--- |
| **Chain of Title** | 30-Year History (MRTA) | `ChainBuilder` with 30-year lookback check. | ✅ Implemented |
| **Ownership** | Verified current owner | `ChainBuilder` + Gap Analysis. | ✅ Implemented |
| **Mortgages** | Open mortgages identified | `LienSurvivalAnalyzer` (Survival logic). | ✅ Implemented |
| **Judgments** | Court judgments against owner | `FinalJudgmentProcessor` + Name Search*. | ⚠️ Partial (FJ only) |
| **Taxes** | Ad Valorem Taxes Paid | `TaxScraper` (Step 13). | ✅ Implemented |
| **HOA/COA** | Assessments & Liens | `LienSurvivalAnalyzer` + Estoppel*. | ⚠️ Liens Only (No Estoppel) |
| **Municipal** | Utility/Code Liens | `LienSurvivalAnalyzer` (Text Search). | ⚠️ Limited (No unrecorded) |
| **Easements** | Recorded Easements | `ChainBuilder` (Doc Type check). | ⚠️ Basic Detection |
| **Surveys** | Encroachments/Boundary | **Out of Scope** (Requires physical survey). | ❌ Not Possible |
| **Zoning** | Land Use Compliance | `HCPAScraper` (Zoning Code). | ✅ Basic Data |

**Gap Acknowledgement:**
*   **Name Search:** We currently search by *Property*, not *Person*. A true title search also searches the owner's name for judgments that attach to *any* property they own. (Feature Request: "Grantor/Grantee Index Search").
*   **Unrecorded Liens:** We cannot find unrecorded municipal liens (water/sewer) without a specific lien search request to the city.
*   **Estoppels:** We cannot get exact HOA payoff amounts (Estoppel Letters) without paying the HOA ~\$250. We only see recorded liens.

### Warning: The "Year Built" Fallacy

Do **NOT** use the property's "Year Built" date as a limit for the title search.
*   **Risk:** Title attaches to the *land*, not the structure. Easements, Restrictions, and Plats are often recorded years before construction.
*   **Example:** A house built in 2020 requires a search back to ~1994 (30 years), not 2020. Searching only to 2020 would miss the developer's acquisition deed, the subdivision plat, and potential prior liens.

## 5. Name Resolution & Matching Logic

Handling "slight name changes" (typos, middle initials) and "original name plus others" (marriage, adding partners) is critical to preventing false chain breaks. We use a **Token-Set & Superset Strategy**.

### A. Normalization
Before comparing, all names are normalized:
1.  **Strip Legal Entity Suffixes:** `LLC`, `INC`, `CORP`, `PA`, `TRUST`.
2.  **Strip Noise Words:** `THE`, `AND`, `A SINGLE MAN`, `HUSBAND AND WIFE`, `F/K/A` (Formerly Known As).
3.  **Tokenize:** Split into a set of distinct words.
    *   "John A. Smith" $\rightarrow$ `{JOHN, A, SMITH}`

### B. Matching Scenarios

#### Scenario 1: The "Add Party" (Superset)
*   **Case:** Grantor `John Smith` $\rightarrow$ Grantee `John Smith and Jane Doe`.
*   **Logic:** `Set(Grantor)` is a **Subset** of `Set(Grantee)`.
*   **Result:** **Valid Link (Party Added)**.
    *   *Interpretation:* Owner added a spouse, child, or investor.

#### Scenario 2: The "Remove Party" (Subset)
*   **Case:** Grantor `John Smith and Jane Doe` $\rightarrow$ Grantee `Jane Doe`.
*   **Logic:** `Set(Grantee)` is a **Subset** of `Set(Grantor)`.
*   **Result:** **Valid Link (Party Removed)**.
    *   *Interpretation:* Divorce, buyout, or death (survivorship).

#### Scenario 3: The "Slight Variation" (Fuzzy Intersection)
*   **Case:** Grantor `Robert Johnson` $\rightarrow$ Grantee `Bob Johnson` or `Robert L. Johnson`.
*   **Logic:** Calculate **Jaccard Similarity** (Intersection over Union).
    *   If overlap > 75%, treat as **Fuzzy Link**.
    *   *Refinement:* Use specific alias lookups (`Bob` $\leftrightarrow$ `Robert`).
*   **Result:** **Fuzzy Link (High Confidence)**.

#### Scenario 4: The "Typo" (Levenshtein)
*   **Case:** Grantor `Steven Jobs` $\rightarrow$ Grantee `Stephen Jobs`.
*   **Logic:** If sets don't match, check **Levenshtein Distance** (edit distance) on individual non-matching tokens.
    *   `Steven` vs `Stephen` = Distance 2.
*   **Result:** **Fuzzy Link (Typo)**.
```

