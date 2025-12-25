# Tax Deed Sales Documentation

## Overview
This document covers the unique logic and legal principles governing Tax Deed sales in Hillsborough County, Florida. Tax deed sales are distinct from mortgage foreclosures and follow different rules for lien survival.

## Survival Logic in Tax Deed Sales
A tax deed sale is initiated by the county when property taxes are delinquent for a significant period (usually after tax certificates have been sold and an application for tax deed is made).

### What Wipes Out?
In a Florida Tax Deed sale, the general rule is that **the sale extinguishes all private interests in the property.** This includes:
- All Mortgages (1st, 2nd, etc.)
- All Judgment Liens
- HOA/COA Liens
- Mechanic's Liens

### What Survives?
The following typically survive a Tax Deed sale (Fla. Stat. 197.552):
1.  **Governmental Liens**: Liens held by a county, municipality, or the state (e.g., municipal utility liens, code enforcement liens if they are from a governmental entity).
2.  **Federal Tax Liens**: Federal tax liens (IRS) often survive tax deed sales unless specific notice requirements were met, and the federal government retains a redemption right.
3.  **Easements and Restrictions**: Valid easements and recorded restrictive covenants generally survive.
4.  **Other Tax Liens**: Other property tax liens may still apply if not included in the sale.

## Technical Implementation (Removed from Main Pipeline)
Previously, the `LienSurvivalAnalyzer` contained logic for tax deeds. This has been moved here for reference:

```python
if is_tax_deed:
    # Tax deed sale wipes EVERYTHING except federal tax liens (maybe) and other government liens
    if self._is_federal_lien(enc_type, creditor):
        entry["status"] = "SURVIVED"
        entry["reason"] = "Federal tax lien survives tax deed (often)"
    else:
        entry["status"] = "EXTINGUISHED"
        entry["reason"] = "Tax deed sale extinguishes most non-government liens"
```

## Difference from Mortgage Foreclosure
| Feature | Mortgage Foreclosure | Tax Deed Sale |
| :--- | :--- | :--- |
| **Initiator** | Lender | County / Tax Collector |
| **Senior Liens** | Survive | Wiped (mostly) |
| **Private Liens** | Junior are wiped, Senior survive | All wiped |
| **Redemption** | Limited (until sale) | Up to 2 years for certificates |
