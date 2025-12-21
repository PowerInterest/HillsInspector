"""
Service for linking related data entities, e.g. Permits to NOCs.
"""
from datetime import date
from typing import List, Dict
from loguru import logger
from src.models.property import Permit

def link_permits_to_nocs(permits: List[Permit], ori_docs: List[Dict]) -> List[Permit]:
    """
    Link Permits to Notices of Commencement (NOCs) based on matching logic.
    
    Logic:
    1. Filter ORI docs for "NOC" type.
    2. For each permit:
       a. Check if NOC date is within +/- 60 days of Permit Issue Date.
       b. (Future) Check if NOC text/legal description contains Permit Number.
       c. If match found, link the NOC Instrument Number to the Permit.
    
    Args:
        permits: List of Permit objects
        ori_docs: List of ORI document dictionaries (normalized)
        
    Returns:
        List of updated Permit objects (modified in-place)
    """
    if not permits or not ori_docs:
        return permits
        
    # Filter for NOCs
    nocs = []
    for d in ori_docs:
        doc_type = d.get("doc_type", "").upper()
        if "NOC" in doc_type or "COMMENCEMENT" in doc_type:
            # Parse record date
            rec_date_str = d.get("record_date", "")
            try:
                # Format is often "MM/DD/YYYY" or "MM/DD/YYYY HH:MM:SS AM/PM"
                # Simplify to date
                rec_date = None
                if rec_date_str:
                    clean_date = rec_date_str.split(" ")[0]
                    month, day, year = map(int, clean_date.split("/"))
                    rec_date = date(year, month, day)
                
                if rec_date:
                    d["_parsed_date"] = rec_date
                    nocs.append(d)
            except Exception as e:
                logger.debug(f"Failed to parse NOC date '{rec_date_str}': {e}")
                continue
                
    if not nocs:
        return permits
        
    logger.info(f"Found {len(nocs)} NOCs to check against {len(permits)} permits")
    
    for permit in permits:
        if not permit.issue_date:
            continue
            
        best_noc = None
        min_date_diff = 999
        
        for noc in nocs:
            noc_date = noc.get("_parsed_date")
            if not noc_date:
                continue
                
            # Check date proximity (NOC usually filed slightly before or after permit issues)
            # Window: -60 days (before) to +30 days (after)
            diff = (noc_date - permit.issue_date).days
            
            # NOC is usually BEFORE permit (diff < 0) or slightly after
            if -60 <= diff <= 30:
                abs_diff = abs(diff)
                if abs_diff < min_date_diff:
                    min_date_diff = abs_diff
                    best_noc = noc
                    
        if best_noc:
            instrument = best_noc.get("instrument")
            permit.noc_instrument = instrument
            logger.info(f"Linked Permit {permit.permit_number} ({permit.issue_date}) to NOC {instrument} (diff {min_date_diff} days)")
            
    return permits
