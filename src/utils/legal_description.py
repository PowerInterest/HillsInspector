"""
Legal Description Utilities

Handles parsing, normalization, and permutation of legal descriptions
for searching Official Records Index (ORI).

ORI search is picky about format. The same property might be recorded as:
- LOT 198 BLOCK 3 TUSCANY SUBDIVISION
- L 198 B 3 TUSCANY SUBDIVISION
- L 198 BLK 3 TUSCANY SUB
- LOT 198 TUSCANY SUBDIVISION AT TAMPA PALMS

This module generates search permutations to find the right match.
"""

import re
from typing import List, Optional, Tuple
from dataclasses import dataclass
from loguru import logger


@dataclass
class LegalDescription:
    """Parsed legal description components."""
    raw_text: str
    subdivision: Optional[str] = None
    lot: Optional[str] = None
    block: Optional[str] = None
    unit: Optional[str] = None
    phase: Optional[str] = None
    section: Optional[str] = None
    township: Optional[str] = None
    range: Optional[str] = None
    plat_book: Optional[str] = None
    plat_page: Optional[str] = None


# Common abbreviation mappings for ORI search
ABBREVIATIONS = {
    # Lot variations
    "LOT": ["LOT", "L", "LT"],
    "BLOCK": ["BLOCK", "BLK", "B", "BK"],
    "UNIT": ["UNIT", "U", "UN"],
    "PHASE": ["PHASE", "PH"],
    "SECTION": ["SECTION", "SEC", "S"],
    "TOWNSHIP": ["TOWNSHIP", "TWP", "T"],
    "RANGE": ["RANGE", "RNG", "R"],
    # Subdivision variations
    "SUBDIVISION": ["SUBDIVISION", "SUBDIV", "SUBD", "SUB", "S/D"],
    "ADDITION": ["ADDITION", "ADDN", "ADD"],
    "REPLAT": ["REPLAT", "REPL", "RP"],
    "AMENDED": ["AMENDED", "AMEND", "AMD"],
    "REVISED": ["REVISED", "REV"],
    # Condo variations
    "CONDOMINIUM": ["CONDOMINIUM", "CONDO", "COND"],
    # Direction variations
    "NORTH": ["NORTH", "N"],
    "SOUTH": ["SOUTH", "S"],
    "EAST": ["EAST", "E"],
    "WEST": ["WEST", "W"],
    "NORTHEAST": ["NORTHEAST", "NE"],
    "NORTHWEST": ["NORTHWEST", "NW"],
    "SOUTHEAST": ["SOUTHEAST", "SE"],
    "SOUTHWEST": ["SOUTHWEST", "SW"],
}


def combine_legal_fields(legal1: str, legal2: str | None = None,
                         legal3: str | None = None, legal4: str | None = None) -> str:
    """
    Combine multiple legal description fields into a single string.

    Args:
        legal1-4: Individual legal description fields from bulk data

    Returns:
        Combined legal description string
    """
    parts = []
    for field in [legal1, legal2, legal3, legal4]:
        if field and field.strip():
            parts.append(field.strip())
    return " ".join(parts)


def parse_legal_description(raw_text: str) -> LegalDescription:
    """
    Parse a legal description into its components.

    Args:
        raw_text: Raw legal description text

    Returns:
        LegalDescription with parsed components
    """
    if not raw_text:
        return LegalDescription(raw_text="")

    text = raw_text.upper().strip()
    result = LegalDescription(raw_text=raw_text)

    # Extract lot number (various patterns)
    # Lots can be numbers, letters, or alphanumeric (e.g., "5", "J", "5A", "AA")
    lot_patterns = [
        r'\bLOT\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',  # LOT 5, LOT J, LOT 5A, LOT AA
        r'\bL\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',     # L 5, L J
        r'\bLT\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',    # LT 5, LT J
    ]
    for pattern in lot_patterns:
        match = re.search(pattern, text)
        if match:
            result.lot = match.group(1)
            break

    # Extract block number (can also be letters)
    block_patterns = [
        r'\bBLOCK\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',
        r'\bBLK\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',
        r'\bB\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',
    ]
    for pattern in block_patterns:
        match = re.search(pattern, text)
        if match:
            result.block = match.group(1)
            break

    # Extract unit number (for condos) - can be numbers, letters, or alphanumeric
    unit_patterns = [
        r'\bUNIT\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',
        r'\bU\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',
        r'\bUN\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',
        r'#\s*([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',
    ]
    for pattern in unit_patterns:
        match = re.search(pattern, text)
        if match:
            result.unit = match.group(1)
            break

    # Extract phase
    phase_match = re.search(r'\bPHASE\s+(\d+[A-Z]?)\b|\bPH\s+(\d+[A-Z]?)\b', text)
    if phase_match:
        result.phase = phase_match.group(1) or phase_match.group(2)

    # Extract section-township-range (S-T-R)
    str_match = re.search(r'\b(\d+)-(\d+)-(\d+)\b', text)
    if str_match:
        result.section = str_match.group(1)
        result.township = str_match.group(2)
        result.range = str_match.group(3)

    # Extract plat book/page
    plat_match = re.search(r'PLAT\s+(?:BOOK|BK)?\s*(\d+)\s*(?:PAGE|PG|P)?\s*(\d+)', text)
    if plat_match:
        result.plat_book = plat_match.group(1)
        result.plat_page = plat_match.group(2)

    # Extract subdivision name (heuristic: longest capitalized phrase)
    # First, try patterns that include PHASE/SECTION numbers on the ORIGINAL text
    # These patterns capture the full subdivision name including phase/section
    early_subdiv_patterns = [
        # Pattern for names with PHASE at end like "LAKE ST CHARLES PHASE 1"
        r'\b([A-Z]{2}[A-Z\s]+PHASE\s+\d+[A-Z]?)\b',
        # Pattern for section-numbered subdivisions like "WESTCHASE SECTION 110"
        r'\b([A-Z]{2}[A-Z\s]*\s+SECTION\s+\d+[A-Z]?)\b',
    ]
    for pattern in early_subdiv_patterns:
        match = re.search(pattern, text)
        if match:
            result.subdivision = match.group(1).strip()
            break

    # If not found yet, remove known patterns and search for subdivision suffixes
    if not result.subdivision:
        subdivision_text = text
        # Patterns for lot/block identifiers (including letter blocks like "BLOCK D")
        removal_patterns = [
            r'\bLOT\s+[A-Z]?\d+[A-Z]?\b',           # LOT 27, LOT A5, LOT 5A
            r'\bL\s+[A-Z]?\d+[A-Z]?\b',             # L 27
            r'\bBLOCK\s+[A-Z]?\d*[A-Z]?\b',         # BLOCK D, BLOCK 3, BLOCK 3A
            r'\bBLK\s+[A-Z]?\d*[A-Z]?\b',           # BLK D
            r'\bB\s+[A-Z]?\d*[A-Z]?\b',             # B D, B 3
            r'\bUNIT\s+(?:NO\s+)?\d+[A-Z]?\b',      # UNIT 5, UNIT NO 3
            r'\bPHASE\s+\d+[A-Z]?\b',               # PHASE 2
            r'\d+-\d+-\d+',                          # Section-Township-Range
            r'PLAT\s+BOOK\s+\d+\s+PAGE\s+\d+',      # Plat references
        ]
        for pattern in removal_patterns:
            subdivision_text = re.sub(pattern, '', subdivision_text, flags=re.IGNORECASE)

        # Find the likely subdivision name using suffix words
        subdiv_patterns = [
            # Pattern with common suffix words - require at least 2 letters before suffix to avoid "A SUBDIVISION"
            r'\b([A-Z]{2}[A-Z\s]*(?:SUBDIVISION|SUBDIV|SUBD|SUB|S/D|ADDITION|ADDN|REPLAT|ESTATES?|HEIGHTS?|PARK|VILLAGE|GARDENS?|MANOR|PLACE|COURT|LANDING|POINT|COVE|BAY|LAKES?|WOODS?|GROVE|CROSSING|MEADOWS?|RIDGE|HILLS?|VALLEY|TERRACE|VISTA|VIEW|OAKS?|PINES?|PALMS?))\b',
            # Pattern for names followed by UNIT/PHASE numbers (capture just the name)
            r'\b([A-Z]{2}[A-Z\s]{2,})\s+(?:UNIT|PH)\s+\d+',
        ]
        for pattern in subdiv_patterns:
            match = re.search(pattern, subdivision_text)
            if match:
                result.subdivision = match.group(1).strip()
                break

    # Fallback: If we have lot/block but no subdivision yet, try extracting
    # the first capitalized word(s) that appear before lot/block
    if not result.subdivision and (result.lot or result.block):
        # Get everything before LOT or BLOCK
        before_lot = re.split(r'\b(LOT|L|LT|BLOCK|BLK|B)\b', text)[0].strip()
        if before_lot and len(before_lot) >= 3:
            # Clean up and use as subdivision name
            clean_name = re.sub(r'\s+', ' ', before_lot).strip()
            if clean_name and not clean_name.isdigit():
                result.subdivision = clean_name

    return result


def generate_search_permutations(legal: LegalDescription, max_permutations: int = 10) -> List[str]:
    """
    Generate search string permutations for ORI search.

    ORI uses CONTAINS search and REQUIRES wildcard (*) suffix for most searches.
    Without wildcards, exact matches fail to return results.

    Args:
        legal: Parsed legal description
        max_permutations: Maximum number of permutations to return

    Returns:
        List of search strings to try, ordered by specificity (all with wildcards)
    """
    permutations = []

    # If we have subdivision, wildcard search on subdivision name is most reliable
    if legal.subdivision:
        subdiv_words = legal.subdivision.split()
        subdiv_first_word = subdiv_words[0]

        # Priority 1: Subdivision name with wildcard (most reliable)
        # e.g., "TUSCANY*" returns all TUSCANY SUBDIVISION records
        permutations.append(f"{subdiv_first_word}*")

        # Priority 2: Lot + subdivision first word with wildcard
        if legal.lot:
            permutations.append(f"L {legal.lot} {subdiv_first_word}*")
            permutations.append(f"LOT {legal.lot} {subdiv_first_word}*")

            # With block if present
            if legal.block:
                permutations.append(f"L {legal.lot} B {legal.block} {subdiv_first_word}*")
                permutations.append(f"LOT {legal.lot} BLOCK {legal.block} {subdiv_first_word}*")

        # Priority 3: Two-word subdivision with wildcard
        if len(subdiv_words) >= 2:
            subdiv_two = " ".join(subdiv_words[:2])
            permutations.append(f"{subdiv_two}*")
            if legal.lot:
                permutations.append(f"L {legal.lot} {subdiv_two}*")

    # For condos, try unit + building name with wildcard
    if legal.unit and legal.subdivision:
        subdiv_first = legal.subdivision.split()[0]
        permutations.append(f"UNIT {legal.unit} {subdiv_first}*")
        permutations.append(f"U {legal.unit} {subdiv_first}*")

    # If we have section-township-range, add that with wildcard
    if legal.section and legal.township and legal.range:
        str_search = f"{legal.section}-{legal.township}-{legal.range}"
        if legal.subdivision:
            subdiv_short = legal.subdivision.split()[0]
            permutations.append(f"{str_search} {subdiv_short}*")
        permutations.append(f"{str_search}*")

    # Fallback: If we have lot/block but no subdivision, try lot with wildcard
    if not permutations and legal.lot:
        if legal.block:
            permutations.append(f"L {legal.lot} B {legal.block}*")
            permutations.append(f"LOT {legal.lot} BLOCK {legal.block}*")
        else:
            permutations.append(f"LOT {legal.lot}*")

    # Last resort: use first significant word from raw text with wildcard
    if not permutations and legal.raw_text:
        words = legal.raw_text.upper().strip().split()
        # Find first word that's not a common prefix
        for word in words:
            if word not in ['LOT', 'L', 'LT', 'BLOCK', 'BLK', 'B', 'UNIT', 'U', 'THE', 'OF', 'IN', 'AT']:
                if len(word) >= 4:  # Skip short words
                    permutations.append(f"{word}*")
                    break

    # Remove duplicates while preserving order
    seen = set()
    unique_perms = []
    for p in permutations:
        if p not in seen:
            seen.add(p)
            unique_perms.append(p)

    return unique_perms[:max_permutations]


def normalize_for_comparison(text: str) -> str:
    """
    Normalize legal description text for comparison.

    Expands all abbreviations to full form and normalizes whitespace.
    """
    if not text:
        return ""

    result = text.upper().strip()

    # Normalize whitespace
    result = re.sub(r'\s+', ' ', result)

    # Expand common abbreviations (in reverse - long form to short)
    # Actually for comparison we want to normalize TO the shortest form
    replacements = [
        (r'\bLOT\b', 'L'),
        (r'\bLT\b', 'L'),
        (r'\bBLOCK\b', 'B'),
        (r'\bBLK\b', 'B'),
        (r'\bBK\b', 'B'),
        (r'\bUNIT\b', 'U'),
        (r'\bUN\b', 'U'),
        (r'\bPHASE\b', 'PH'),
        (r'\bSUBDIVISION\b', 'SUB'),
        (r'\bSUBDIV\b', 'SUB'),
        (r'\bSUBD\b', 'SUB'),
        (r'\bS/D\b', 'SUB'),
        (r'\bCONDOMINIUM\b', 'CONDO'),
        (r'\bCOND\b', 'CONDO'),
    ]

    for pattern, replacement in replacements:
        result = re.sub(pattern, replacement, result)

    return result


def extract_subdivision_name(legal_text: str) -> Optional[str]:
    """
    Extract just the subdivision name from a legal description.

    Useful for grouping properties by subdivision.
    """
    parsed = parse_legal_description(legal_text)
    return parsed.subdivision


def build_ori_search_terms(folio: str, legal1: str | None, legal2: str | None = None,
                           legal3: str | None = None, legal4: str | None = None,
                           judgment_legal: str | None = None) -> List[str]:
    """
    Build a prioritized list of ORI search terms for a property.

    Combines legal descriptions from multiple sources and generates
    search permutations.

    Args:
        folio: Property folio number
        legal1-4: Legal description fields from bulk parcel data
        judgment_legal: Legal description from Final Judgment (most authoritative)

    Returns:
        List of search terms to try, ordered by likelihood of success
    """
    search_terms = []

    # Priority 1: Final Judgment legal description (most authoritative)
    if judgment_legal:
        parsed = parse_legal_description(judgment_legal)
        search_terms.extend(generate_search_permutations(parsed))

    # Priority 2: Combined bulk data legal description
    bulk_legal = combine_legal_fields(legal1, legal2, legal3, legal4)
    if bulk_legal:
        parsed = parse_legal_description(bulk_legal)
        for term in generate_search_permutations(parsed):
            if term not in search_terms:
                search_terms.append(term)

    # Priority 3: Try each legal field individually
    for field in [legal1, legal2, legal3, legal4]:
        if field and field.strip():
            # Extract just lot + block if present
            parsed = parse_legal_description(field)

            # Find first significant word (likely subdivision name)
            words = field.strip().upper().split()
            first_significant = None
            for word in words:
                if word not in ['LOT', 'L', 'LT', 'BLOCK', 'BLK', 'B', 'UNIT', 'U', 'THE', 'OF', 'IN', 'AT']:
                    if len(word) >= 4:
                        first_significant = word
                        break

            if first_significant:
                # Always add subdivision wildcard search
                term = f"{first_significant}*"
                if term not in search_terms:
                    search_terms.append(term)

                # Add lot + subdivision if we have lot
                if parsed.lot:
                    term = f"L {parsed.lot} {first_significant}*"
                    if term not in search_terms:
                        search_terms.append(term)

    # Filter out overly generic terms that would return too many results
    generic_terms = {'BLOCK*', 'LOT*', 'UNIT*', 'PHASE*', 'THE*', 'PLAT*', 'BOOK*', 'PAGE*',
                     'NORTH*', 'SOUTH*', 'EAST*', 'WEST*', 'SECTION*', 'TOWNSHIP*', 'RANGE*',
                     'LESS*', 'THAT*', 'PART*', 'BEING*', 'ALSO*', 'A*', 'AN*', 'AND*',
                     'A SUBDIVISION*', 'A SUB*', 'ACCORDING*', 'CORNER*'}
    filtered_terms = [t for t in search_terms if t.upper() not in generic_terms]

    # Ensure search terms are specific enough (at least one word with 4+ characters before wildcard)
    specific_terms = []
    for term in filtered_terms:
        # Get the part before wildcard
        base = term.rstrip('*')
        words = base.split()
        # Check if any word has 4+ characters (excluding L, B, LOT, BLOCK, etc.)
        has_specific = any(
            len(w) >= 4 and w not in {'BLOCK', 'UNIT', 'PHASE'}
            for w in words
        )
        if has_specific:
            specific_terms.append(term)

    logger.debug(f"Generated {len(specific_terms)} search terms for folio {folio} (filtered from {len(search_terms)})")
    return specific_terms


def match_legal_descriptions(desc1: str, desc2: str, threshold: float = 0.8) -> Tuple[bool, float]:
    """
    Check if two legal descriptions match (allowing for abbreviation differences).

    Args:
        desc1: First legal description
        desc2: Second legal description
        threshold: Minimum similarity score (0-1) to consider a match

    Returns:
        Tuple of (is_match, similarity_score)
    """
    if not desc1 or not desc2:
        return False, 0.0

    # Normalize both
    norm1 = normalize_for_comparison(desc1)
    norm2 = normalize_for_comparison(desc2)

    # Exact match after normalization
    if norm1 == norm2:
        return True, 1.0

    # One contains the other
    if norm1 in norm2 or norm2 in norm1:
        shorter = min(len(norm1), len(norm2))
        longer = max(len(norm1), len(norm2))
        score = shorter / longer
        return score >= threshold, score

    # Token-based similarity
    tokens1 = set(norm1.split())
    tokens2 = set(norm2.split())

    if not tokens1 or not tokens2:
        return False, 0.0

    intersection = tokens1 & tokens2
    union = tokens1 | tokens2

    score = len(intersection) / len(union)
    return score >= threshold, score


if __name__ == "__main__":
    # Test the utilities
    test_cases = [
        "LOT 198 BLOCK 3 TUSCANY SUBDIVISION AT TAMPA PALMS",
        "L 5 B 2 CARROLLWOOD VILLAGE UNIT 15",
        "KEYSTONE PARK COLONY TRACT 1 IN NE 1/4 OF SEC 1",
        "UNIT 304 BLDG A HARBOUR ISLAND CONDOMINIUM",
        "23 24 25 AND 26-33-15",
    ]

    for test in test_cases:
        print(f"\n{'='*60}")
        print(f"Input: {test}")
        parsed = parse_legal_description(test)
        print(f"Parsed: {parsed}")
        perms = generate_search_permutations(parsed)
        print(f"Search permutations: {perms}")
