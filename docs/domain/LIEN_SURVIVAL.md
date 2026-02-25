# Lien Survival Analysis

The `LienSurvivalAnalyzer` (`src/services/lien_survival_analyzer.py`) determines which liens will survive the **upcoming** foreclosure sale based on Florida law and the foreclosure type.

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

3. **Superpriority Liens**: Tax liens, IRS liens, municipal liens, utility liens, and code enforcement liens **always survive** any foreclosure.

4. **Foreclosure Type Rules**:
   - **HOA/COA Foreclosure**: First mortgage **SURVIVES** per Florida Safe Harbor (Fla. Stat. 720.3085 / 718.116). Junior liens are `EXTINGUISHED`.
   - **First Mortgage Foreclosure**: Everything junior (second mortgages, HOA liens, judgments) is `EXTINGUISHED`.
   - **Tax Deed Sale**: Everything is `EXTINGUISHED` except federal tax liens.

5. **Expiration Rules** (Florida Statutes):
   - Mechanic's/Construction Liens: 1 year to file suit (Fla. Stat. 713.22)
   - HOA Claim of Lien: 1 year to file suit (Fla. Stat. 720.3085)
   - Judgment Liens: 10 years, renewable to 20 (Fla. Stat. 55.10)
   - Code Enforcement: 20 years (Fla. Stat. 162.09)
   - Mortgages: 5 years after maturity (~35 years total)

### Example Analysis

For an **HOA foreclosure** on a property acquired in 2023:
- 1996-2002 mortgages from prior owner: **HISTORICAL** (wiped by 2003 foreclosure)
- 2023 first mortgage ($211k): **SURVIVED** (Florida Safe Harbor)
- 2023-2025 HOA liens: **FORECLOSING** (plaintiff's liens)
