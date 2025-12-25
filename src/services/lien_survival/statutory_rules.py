"""
Statutory Rules for Florida Lien Survival.

Handles:
- Expiration check (Statute of Limitations)
- Superpriority detection (Tax, PACE, Utility)
- Federal lien identification
- HOA Safe Harbor calculations
"""

from datetime import date
from typing import Optional, Tuple

from src.utils.time import now_utc

# Liens that ALWAYS survive any foreclosure (government priority)
SUPERPRIORITY_TYPES = (
    "TAX", "IRS", "MUNICIPAL", "UTILITY", "CODE ENFORCEMENT", 
    "PACE", "CLEAN ENERGY", "WATER", "SEWER"
)

def is_superpriority(lien_type: str, creditor: str = "") -> bool:
    """Check if lien type is superpriority per Florida law."""
    doc_type = (lien_type or "").upper()
    creditor_upper = (creditor or "").upper()
    
    # PACE Liens
    if any(kw in doc_type or kw in creditor_upper for kw in ("PACE", "CLEAN ENERGY")):
        return True
        
    # Property Taxes (Exclude Tax Deeds)
    if "TAX" in doc_type and "DEED" not in doc_type:
        return True
    
    # Municipal utilities
    if any(kw in doc_type for kw in ("UTILITY", "WATER", "SEWER")):
        return True
        
    # Code Enforcement
    return any(kw in doc_type for kw in ("CODE", "ENFORCEMENT"))

def is_federal_lien(lien_type: str, creditor: str) -> bool:
    """Check if lien is held by Federal Government (IRS, DOJ)."""
    doc_type = (lien_type or "").upper()
    creditor_upper = (creditor or "").upper()
    
    # IRS
    if "IRS" in doc_type or "INTERNAL REVENUE" in creditor_upper:
        return True
        
    # Other Federal
    return any(kw in creditor_upper for kw in ("USA", "UNITED STATES"))

def is_expired(lien_type: str, recording_date: Optional[date]) -> Tuple[bool, Optional[str]]:
    """
    Check if a lien has expired based on Florida statutes.
    Returns (is_expired, reason).
    """
    if not recording_date:
        return False, None

    age_years = (now_utc().date() - recording_date).days / 365.25
    doc_type = (lien_type or "").upper()

    # Mechanic's Liens - 1 year (Fla. Stat. 713.22)
    if ("MECHANIC" in doc_type or "CONSTRUCTION" in doc_type) and age_years > 1:
        return True, "Expired Mechanic's Lien (>1 year)"

    # HOA/COA Claim of Lien - 1 year (Fla. Stat. 720.3085 / 718.116)
    if ("HOA" in doc_type or "CONDO" in doc_type or "ASSOCIATION" in doc_type) and "CLAIM" in doc_type and age_years > 1:
        return True, "Expired HOA Claim of Lien (>1 year without suit)"

    # Judgment Liens - 10-20 years (Fla. Stat. 55.10)
    if "JUDGMENT" in doc_type:
        if age_years > 20:
            return True, "Expired Judgment Lien (>20 years)"
        if age_years > 10:
            return True, "Likely Expired Judgment Lien (>10 years, not re-recorded)"

    # Code Enforcement - 20 years (Fla. Stat. 162.09)
    if ("CODE" in doc_type or "ENFORCEMENT" in doc_type) and age_years > 20:
        return True, "Expired Code Enforcement Lien (>20 years)"

    # Mortgages - 5 years after maturity (assume 30yr mortgage + 5yr margin = 35yr total)
    if ("MORTGAGE" in doc_type or "MTG" in doc_type) and age_years > 35:
        return True, "Likely Expired Mortgage (>35 years)"

    return False, None

def calculate_hoa_safe_harbor(
    original_mortgage_amount: Optional[float],
    monthly_hoa_dues: Optional[float] = None,
    months_unpaid: int = 12
) -> Optional[float]:
    """
    Calculate HOA Safe Harbor (lesser of 12mo dues or 1% of mortgage).
    Fla. Stat. 720.3085 / 718.116.
    """
    if original_mortgage_amount is None:
        return None
        
    # 1% of Original Mortgage
    cap_1_percent = original_mortgage_amount * 0.01
    
    if monthly_hoa_dues is None:
        return cap_1_percent # Best we can calculate
    
    # 12 months (or months unpaid if specify <= 12)
    accrued = min(months_unpaid, 12) * monthly_hoa_dues
    
    return min(accrued, cap_1_percent)
