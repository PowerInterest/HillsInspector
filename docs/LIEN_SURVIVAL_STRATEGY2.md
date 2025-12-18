# Lien Survival Flowchart & Logic Breakdown

This document visualizes the decision tree used in `LienSurvivalAnalyzer` and `ChainBuilder`, covering the entire lifecycle from raw document ingestion to the final determination of "What debt do I inherit if I buy this?".

## The Logic Flowchart

```mermaid
flowchart TD
    %% --- INGESTION PHASE ---
    subgraph INGESTION
        Input_Folio[("Input: Parcel ID (Folio)")]
        Input_Docs[("Input: Document List (ORI)")]
        Input_FJ[("Input: Final Judgment (PDF Data)")]
        
        Input_Folio --> Fetch_Docs[Fetch All Documents for Folio]
        Input_Folio --> Fetch_FJ[Extract Judgment Details]
    end

    %% --- CHAIN OF TITLE PHASE ---
    subgraph CHAIN_BUILDER
        Fetch_Docs --> Sort_Time[Sort Documents by Recording Date (Desc)]
        Sort_Time --> ID_Deeds{Is Document a Deed?}
        
        ID_Deeds -- Yes --> Extract_Owner[Extract Grantor/Grantee]
        ID_Deeds -- No --> Classify_Enc[Classify as Encumbrance/Lien]
        
        Extract_Owner --> Link_Chain{Does Grantee Match Previous Grantor?}
        Link_Chain -- Yes --> Chain_Link[Link Established]
        Link_Chain -- No --> Chain_Break[FLAG: Broken Chain / Name Mismatch]
        
        Chain_Link --> Def_Owner[Define 'Current Ownership Period']
        Chain_Break --> Def_Owner
    end

    %% --- ENCUMBRANCE FILTERING PHASE ---
    subgraph LIEN_FILTER
        Classify_Enc --> Filter_Date{Recorded During Current Ownership?}
        Filter_Date -- No (Prior Owner) --> Check_RunLand{Type = Gov/Muni/Super?}
        Filter_Date -- Yes --> Check_Sat{Is Satisfaction Recorded?}
        
        Check_RunLand -- No --> Status_Hist[Status: HISTORICAL (Wiped)]
        Check_RunLand -- Yes --> Pool_Active[Add to Active Pool]
        
        Check_Sat -- Yes --> Status_Sat[Status: SATISFIED]
        Check_Sat -- No --> Check_Exp{Is Expired by Statute?}
        
        Check_Exp -- Yes (e.g. constr. > 1yr) --> Status_Exp[Status: EXPIRED]
        Check_Exp -- No --> Pool_Active
    end

    %% --- SURVIVAL ANALYSIS PHASE ---
    subgraph SURVIVAL_LOGIC
        Input_FJ --> Def_FC_Type{Identify Foreclosure Type}
        Input_FJ --> Def_LP_Date[Identify Lis Pendens Date]
        Input_FJ --> Def_Plaintiff[Identify Plaintiff/Foreclosing Lien]
        
        Pool_Active --> Analyze_Lien[Analyze Single Lien]
        
        Analyze_Lien --> Is_It_FC{Is this the Foreclosing Lien?}
        Is_It_FC -- Yes (Matches Judgment) --> Res_FC[Status: FORECLOSING<br/>(Paid by Bid/Merged)]
        
        Is_It_FC -- No --> Check_Super{Is Superpriority?}
        
        %% Superpriority Path
        Check_Super -- Yes (PACE/Tax/Muni) --> Res_Survive_Super[Status: SURVIVED<br/>(Gov Priority)]
        Check_Super -- Yes (Federal/IRS) --> Res_Fed[Status: EXTINGUISHED*<br/>(Subject to 120-Day Redemption)]
        
        %% Non-Super Path (The Complex Part)
        Check_Super -- No --> FC_Scenario{Foreclosure Scenario?}
        
        %% Scenario A: Tax Deed
        FC_Scenario -- Tax Deed Sale --> Res_Wipe_All[Status: EXTINGUISHED<br/>(Tax Deed Wipes All)]
        
        %% Scenario B: HOA Foreclosure
        FC_Scenario -- HOA Foreclosure --> Is_1st_Mtg{Is Lien 1st Mortgage?}
        Is_1st_Mtg -- Yes --> Res_Safe_Harbor[Status: SURVIVED<br/>(Safe Harbor Limits Apply)]
        Is_1st_Mtg -- No --> Res_Wipe_HOA[Status: EXTINGUISHED<br/>(Junior to HOA)]
        
        %% Scenario C: Mortgage Foreclosure (Standard)
        FC_Scenario -- Mortgage Foreclosure --> Check_Time{Recorded Before Lis Pendens?}
        Check_Time -- Yes (Senior) --> Res_Survive_Time[Status: SURVIVED<br/>(Senior Lien)]
        Check_Time -- No (Junior) --> Res_Wipe_Time[Status: EXTINGUISHED<br/>(Junior Lien)]
    end

    %% Styles
    style Res_Survive_Super fill:#ffcccc,stroke:#cc0000,stroke-width:2px
    style Res_Safe_Harbor fill:#ffcccc,stroke:#cc0000,stroke-width:2px
    style Res_Survive_Time fill:#ffcccc,stroke:#cc0000,stroke-width:2px
    style Res_Fed fill:#fff4cc,stroke:#ffbb00,stroke-width:2px
    
    style Res_Wipe_All fill:#ccffcc,stroke:#006600
    style Res_Wipe_HOA fill:#ccffcc,stroke:#006600
    style Res_Wipe_Time fill:#ccffcc,stroke:#006600
    style Res_FC fill:#e6f3ff,stroke:#0066cc
```

---

## Detailed Input & Effect Table

Here is every possible valid input mapped to how it changes the decision logic.

### 1. Foreclosure Type Inputs (From Final Judgment)
| Input Value | Logic Path | Effect on Liens |
| :--- | :--- | :--- |
| **TAX_DEED** | `FC_Scenario -> Tax Deed` | **Nuclear Option:** Wipes almost everything (Mortgages, HOA, Judgments, Mechanics). Only Government/Federal liens survive. |
| **HOA / CONDO** | `FC_Scenario -> HOA` | **Safe Harbor Rule:** 1st Mortgages SURVIVE. Junior liens (2nd mtg, judgments) are wiped. |
| **MORTGAGE** | `FC_Scenario -> Mortgage` | **Standard Priority:** Survival depends strictly on recording date vs. Lis Pendens date (First in time, first in right). |

### 2. Lien Type Inputs (From ORI Documents)
| Input Value | Logic Path | Effect on Survival |
| :--- | :--- | :--- |
| **PACE / CLEAN ENERGY** | `Check_Super` | **Always Survives.** Treated as a tax. Dangerous if missed. |
| **TAX CERT / WARRANT** | `Check_Super` | **Always Survives.** Government debt. |
| **NOTICE OF COMMENCEMENT** | `Classify_Enc` | **Trigger:** Looks for "Mechanic's Liens". If found, priority dates back to the NOC filing, not the Lien filing date (Retroactive priority). |
| **LIS PENDENS** | `Def_LP_Date` | **The Cutoff Line.** Establishes the date split for Senior (Surviving) vs. Junior (Wiped). |
| **SATISFACTION / RELEASE** | `Check_Sat` | **Removes Liability.** Matches with a prior mortgage/lien to neutralize it before survival analysis even begins. |
| **JUDGMENT** | `Check_Exp` | **Expiration:** 10 years (unless re-recorded). If >10 years old, it is marked EXPIRED. |

### 3. Party Inputs (Names)
| Input Value | Logic Path | Effect |
| :--- | :--- | :--- |
| **"USA" / "IRS" / "DEPT OF TREASURY"** | `Check_Super -> Federal` | **Redemption Cloud.** Even if technically junior, we flag this because the IRS can seize the property back within 120 days. |
| **"STATE OF FLORIDA"** | `Check_Super` | Usually survives if it's a tax warrant. |
| **"COUNTY" / "CITY"** | `Check_RunLand` | **Run with Land.** Code enforcement or Utility liens often attach to the property, not just the owner, meaning they survive even if from a *previous* owner. |
| **"Plaintiff Name" vs "Lien Creditor"** | `Is_It_FC` | Used to identify which specific lien is being foreclosed (and thus doesn't survive as an encumbrance). |

### 4. Date Inputs
| Input | Logic Path | Effect |
| :--- | :--- | :--- |
| **Recording Date** | `Check_Time` | The primary sorting mechanism for priority. |
| **Judgment Date** | `Check_Exp` | Used to calculate if a judgment lien has expired (Statute of Limitations). |
| **Acquisition Date** | `Filter_Date` | Anything recorded *before* this date is usually "Historical" (belonged to previous owner), unless it is a specific type that "Runs with the Land" (Muni/Gov). |

### Summary of Outcomes
1.  **SURVIVED:** The bidder (you) will be responsible for paying this. (Red Flag).
2.  **EXTINGUISHED:** This debt is wiped clean by the foreclosure process. (Safe).
3.  **EXTINGUISHED (Redemption Right):** Wiped, but Federal Gov has 120 days to take the house back. (Yellow Flag).
4.  **FORECLOSING:** This is the debt being paid by the auction bid.
5.  **SATISFIED/EXPIRED:** Old news. No liability.
