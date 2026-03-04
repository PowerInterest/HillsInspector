# Lien Survival Analysis

See also [Encumbrance Audit Buckets](ENCUMBRANCE_AUDIT_BUCKETS.md) for the
active-foreclosure audit buckets that separate ORI discovery gaps from
superpriority and identity-resolution risk.

The `SurvivalService` (`src/services/lien_survival/survival_service.py`) determines which liens will survive the **upcoming** foreclosure sale based on Florida law and the foreclosure type.

### Survival Status Values

| Status | Meaning |
|--------|---------|
| **SURVIVED** | Will survive the upcoming foreclosure sale (senior liens, superpriority, first mortgage in HOA foreclosure) |
| **EXTINGUISHED** | Will be wiped out by the upcoming sale (junior liens) |
| **EXPIRED** | Already expired by statute of limitations (e.g., mechanic's lien >1 year, judgment >10 years) |
| **SATISFIED** | Already paid off/released (satisfaction recorded) |
| **HISTORICAL** | From a prior ownership period - already wiped by a previous foreclosure |
| **FORECLOSING** | This is the lien being foreclosed (the plaintiff's lien) |

### Key Logic

1. **Historical Detection**: Liens recorded before the current owner's acquisition date are marked `HISTORICAL`. These were already wiped by a prior foreclosure that transferred title.

2. **Foreclosing Party Detection**: Liens where the creditor matches the plaintiff (foreclosing party) are marked `FORECLOSING`.

3. **Superpriority Liens (Survive All Foreclosures)**:
   - **Ad Valorem Property Taxes**: Absolute first priority.
   - **PACE Loans (Property Assessed Clean Energy)**: Billed as non-ad valorem taxes. *Recent Law: 2024 SB 770 explicitly reaffirmed their super-priority status above first mortgages.* Fannie/Freddie often refuse to underwrite properties with PACE liens until paid off.
   - **CDD Assessments (Community Development District)**: Also billed on the tax roll. Constitute a "first lien" co-equal with county taxes. Survive mortgage foreclosures.
   - **Municipal Utility Liens (Chapter 159)**: Unpaid water/sewer/gas. Co-equal with state/county taxes. They survive "until paid" and uniquely do **not** need to be recorded in official records to be valid against a new buyer.
   - **Code Enforcement Liens (Chapter 162)**: Often granted "super priority" status by local municipal Home Rule ordinances. They run with the land and survive both mortgage foreclosures and tax deed sales depending on local code.
   - **Federal IRS Tax Liens**: The lien itself is extinguished by a proper mortgage foreclosure *if* the US is joined as a party, BUT the IRS retains a **120-day statutory Right of Redemption**. The IRS can buy the property from the winning bidder for the winning bid amount plus 6% interest. Until 120 days pass, the title is clouded.

4. **Foreclosure Type Rules**:
   - **HOA/COA Foreclosure**: First mortgage **SURVIVES** per Florida Safe Harbor (Fla. Stat. 720.3085 / 718.116). Junior liens are `EXTINGUISHED`. *Recent Law: Under the 2024 Homeowners' Bill of Rights (HB 1203), HOAs are now prohibited from placing a lien on a parcel for fines totaling less than $1,000.*
   - **First Mortgage Foreclosure**: Everything junior (second mortgages, HOA liens, judgments) is `EXTINGUISHED`.
   - **Tax Deed Sale**: Extinguishes almost all private liens (mortgages, HOA, judgments). However, it **does not** extinguish governmental liens (code enforcement, utility) or easements/covenants.

5. **Expiration Rules** (Florida Statutes):
   - **Mechanic's/Construction Liens**: 1 year to file suit (Fla. Stat. 713.22). *Note: 2023 HB 331 changed notice and termination rules, extending deadlines that fall on weekends/holidays. Ensure date calculations use the new business day rules.*
   - **HOA Claim of Lien**: 1 year to file suit (Fla. Stat. 720.3085).
   - **Judgment Liens**: 10 years, renewable to 20 (Fla. Stat. 55.10).
   - **Code Enforcement**: 20 years (Fla. Stat. 162.09).
   - **Mortgages**: 5 years after maturity (~35 years total).

### Example Analysis

For an **HOA foreclosure** on a property acquired in 2023:
- 1996-2002 mortgages from prior owner: **HISTORICAL** (wiped by 2003 foreclosure)
- 2023 first mortgage ($211k): **SURVIVED** (Florida Safe Harbor)
- 2023-2025 HOA liens: **FORECLOSING** (plaintiff's liens)
