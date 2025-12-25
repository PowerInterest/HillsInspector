"""
Priority Engine for Lien Survival Analysis.

Handles:
- Foreclosing lien identification
- Senior vs Junior determination
- Historical lien detection (prior owners)
- Uncertainty flagging for missing data
"""

from datetime import date
from typing import Optional, Dict, Any, List, Tuple
from src.utils.name_matcher import NameMatcher

def identify_foreclosing_lien(
    encumbrance: Dict[str, Any],
    plaintiff: str,
    foreclosing_refs: Optional[Dict[str, str]] = None
) -> Tuple[bool, str]:
    """
    Check if this encumbrance is the one being foreclosed.
    Returns (is_foreclosing, confidence_reason).
    """
    # 1. Check exact recording references (High Confidence)
    if foreclosing_refs:
        instr = encumbrance.get('instrument')
        if instr and str(instr).strip() == str(foreclosing_refs.get('instrument', '')).strip():
            return True, "EXACT_INSTRUMENT_MATCH"
            
        book = encumbrance.get('book')
        page = encumbrance.get('page')
        if (book and page and 
            str(book).strip() == str(foreclosing_refs.get('book', '')).strip() and
            str(page).strip() == str(foreclosing_refs.get('page', '')).strip()):
            return True, "EXACT_BOOK_PAGE_MATCH"

    # 2. Name matching with Plaintiff (Medium Confidence)
    creditor = encumbrance.get('creditor')
    if creditor and plaintiff:
        match_type, score = NameMatcher.match(creditor, plaintiff)
        if score >= 0.85:
            return True, f"PLAINTIFF_NAME_MATCH ({match_type})"

    return False, ""

def determine_seniority(
    target: Dict[str, Any],
    foreclosing: Dict[str, Any],
    lis_pendens_date: Optional[date] = None
) -> str:
    """
    Determine if the target lien is SENIOR or JUNIOR to the foreclosing lien.
    
    Priority is primarily determined by recording date. 
    If recording date is missing, fell back to instrument number sequence.
    """
    target_date = target.get('recording_date')
    foreclosing_date = foreclosing.get('recording_date') or lis_pendens_date

    if not target_date:
        return "UNKNOWN (Missing Date)"

    if not foreclosing_date:
        return "UNCERTAIN (Missing Foreclosure Date)"

    if target_date < foreclosing_date:
        return "SENIOR"
    if target_date > foreclosing_date:
        return "JUNIOR"

    # Same day - check instrument number if available
    target_inst = target.get('instrument')
    foreclosing_inst = foreclosing.get('instrument')
    if target_inst and foreclosing_inst:
        try:
            if int(target_inst) < int(foreclosing_inst):
                return "SENIOR"
            return "JUNIOR"
        except (ValueError, TypeError):
            pass
    
    # Tie-breaker: LP date vs target recording date if available
    if lis_pendens_date and target_date < lis_pendens_date:
        return "SENIOR"
        
    return "JUNIOR (Same Day Tie)"

def is_historical(
    encumbrance: Dict[str, Any],
    current_period_id: Optional[int],
    periods: List[Dict[str, Any]]
) -> bool:
    """
    Determine if a lien is 'Historical' (from a prior owner).
    
    A lien is historical if its recording date is before the 
    current owner's acquisition date.
    """
    if not current_period_id or not periods:
        return False
        
    # Find current period
    current_period = next((p for p in periods if p.get('id') == current_period_id), None)
    if not current_period:
        return False
        
    acquisition_date = current_period.get('acquisition_date')
    recording_date = encumbrance.get('recording_date')
    
    if not acquisition_date or not recording_date:
        return False
        
    # If recorded BEFORE current owner bought the property, it's historical
    # (unless it's a mortgage that was assumed, but that's rare and usually 
    # handled by seeing it hasn't been satisfied/wiped).
    return recording_date < acquisition_date
