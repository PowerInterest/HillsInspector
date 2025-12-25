"""
Joinder Validator for Lien Survival Analysis.

Validates if a lienholder was joined as a defendant in the foreclosure case.
A junior lien survives if the holder was NOT joined.
"""

from typing import List, Optional, Tuple, Any
from src.utils.name_matcher import NameMatcher

def is_joined(creditor: str, defendants: List[Any]) -> Tuple[bool, Optional[str], float]:
    """
    Check if a creditor is joined as a defendant.
    Returns (is_joined, matched_name, confidence_score).
    """
    if not creditor or not defendants:
        return False, None, 0.0
        
    best_match = None
    max_score = 0.0
    
    for defendant in defendants:
        # Handle both string names and structured defendant objects
        def_name = defendant.get('name') if isinstance(defendant, dict) else defendant
        if not def_name:
            continue
            
        _, score = NameMatcher.match(creditor, def_name)
        
        # EXACT or highly confident fuzzy match
        if score > max_score:
            max_score = score
            best_match = defendant
            
    if max_score >= 0.85:
        return True, best_match, max_score
        
    return False, None, max_score

def validate_all_junior_liens(
    junior_liens: List[dict], 
    defendants: List[str]
) -> List[dict]:
    """
    Helper to bulk-validate joinder for a list of junior liens.
    Updates the 'is_joined' key in each lien dict.
    """
    for lien in junior_liens:
        joined, match, score = is_joined(lien.get('creditor', ''), defendants)
        lien['is_joined'] = joined
        lien['joined_as'] = match
        lien['joinder_confidence'] = score
        
    return junior_liens
