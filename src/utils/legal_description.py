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
from dataclasses import dataclass, field
from loguru import logger
from rapidfuzz import fuzz


@dataclass
class LegalDescription:
    """Parsed legal description components."""
    raw_text: str
    subdivision: Optional[str] = None
    lot: Optional[str] = None
    lots: List[str] = field(default_factory=list)
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
    for legal_field in [legal1, legal2, legal3, legal4]:
        if legal_field and legal_field.strip():
            parts.append(legal_field.strip())
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

    # Strip leading section numbers (e.g., "1\tBELLMONT..." or "1 BELLMONT...")
    # HCPA bulk data often prefixes legal descriptions with section numbers
    text = re.sub(r'^\d+[\t\s]+', '', text)
    result = LegalDescription(raw_text=raw_text)

    # Extract lot number(s) (various patterns)
    # Lots can be numbers, letters, or alphanumeric (e.g., "5", "J", "5A", "AA")
    lot_patterns = [
        r'\bLOT\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',  # LOT 5, LOT J, LOT 5A, LOT AA
        r'\bL\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',     # L 5, L J
        r'\bLT\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',    # LT 5, LT J
    ]

    lots_found: List[str] = []

    # Priority 1: Handle "LOTS X, Y AND Z" or "LOTS X Y AND Z" patterns
    # This captures ALL lot numbers after LOTS keyword
    lots_multi_match = re.search(r'\bLOTS\s+((?:[A-Z]?\d+[A-Z]?\s*(?:,|AND|\s)\s*)+[A-Z]?\d+[A-Z]?)', text)
    if lots_multi_match:
        # Extract all alphanumeric lot identifiers from the matched group
        lot_nums = re.findall(r'\b([A-Z]?\d+[A-Z]?)\b', lots_multi_match.group(1))
        for num in lot_nums:
            # Filter out common false positives (years, measurements)
            if num and num not in lots_found and not (num.isdigit() and len(num) == 4 and 1900 <= int(num) <= 2100):
                lots_found.append(num)

    # Priority 2: Catch individual "LOT X" occurrences (handles "LOT 18 ... LOT 19 ...")
    for match in re.finditer(r'\bLOT\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b', text):
        val = match.group(1)
        if val and val not in lots_found:
            lots_found.append(val)

    # Priority 3: Abbreviated forms (L 5, LT 5)
    for pattern in lot_patterns[1:]:  # Skip LOT pattern, already handled
        for match in re.finditer(pattern, text):
            val = match.group(1)
            if val and val not in lots_found:
                lots_found.append(val)

    result.lots = lots_found
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
        r'\bUNIT\s+(?:NO\s+)?([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',
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
    # Common compact form: "11-30-20"
    str_match = re.search(r'\b(\d+)-(\d+)-(\d+)\b', text)
    if str_match:
        result.section = str_match.group(1)
        result.township = str_match.group(2)
        result.range = str_match.group(3)

    # Narrative form: "SECTION 11, TOWNSHIP 30 SOUTH, RANGE 20 EAST"
    if not (result.section and result.township and result.range):
        str_words = re.search(
            r'\bSECTION\s+(\d+)\b.*?\bTOWNSHIP\s+(\d+)\b.*?\bRANGE\s+(\d+)\b',
            text,
            flags=re.IGNORECASE,
        )
        if str_words:
            result.section = str_words.group(1)
            result.township = str_words.group(2)
            result.range = str_words.group(3)

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
        # Pattern for section subdivisions like "WESTCHASE SECTION 110" or "BLOOMINGDALE SECTION \"F\""
        r'\b([A-Z]{2}[A-Z\s]*\s+SECTION\s+\"?[A-Z0-9]+\"?)\b',
    ]
    for pattern in early_subdiv_patterns:
        match = re.search(pattern, text)
        if match:
            candidate = match.group(1).strip()
            # Strip leading stopwords and reject if starts with descriptor
            stopwords = {'THE', 'OF', 'IN', 'AT', 'A', 'AN', 'AND', 'OR', 'TO', 'FOR', 'AS'}
            words = candidate.upper().split()
            while words and words[0] in stopwords:
                words = words[1:]
            descriptors = {'REPLAT', 'PORTION', 'PART', 'PLAT', 'HALF', 'QUARTER', 'MAP'}
            if words and words[0] not in descriptors:
                result.subdivision = ' '.join(words)
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
                candidate = match.group(1).strip()
                # Strip leading stopwords (e.g., "OF THE REPLAT OF TAMPA HEIGHTS" -> check "REPLAT...")
                stopwords = {'THE', 'OF', 'IN', 'AT', 'A', 'AN', 'AND', 'OR', 'TO', 'FOR', 'AS'}
                words = candidate.upper().split()
                while words and words[0] in stopwords:
                    words = words[1:]
                # Reject if first remaining word is a descriptor (not a subdivision name)
                descriptors = {'REPLAT', 'PORTION', 'PART', 'PLAT', 'HALF', 'QUARTER',
                              'SECTION', 'EAST', 'WEST', 'NORTH', 'SOUTH', 'NE', 'NW', 'SE', 'SW', 'MAP'}
                if words and words[0] not in descriptors:
                    result.subdivision = ' '.join(words)
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

    # Fallback 2: Some legals have format "LOT X, BLOCK Y, SUBDIVISION_NAME, SECTION/UNIT..."
    # Try extracting the name AFTER "BLOCK X," if no subdivision found yet
    if not result.subdivision and result.block:
        # Look for pattern: "BLOCK X, NAME" where NAME is capitalized words before SECTION/UNIT/ACCORDING/PLAT
        after_block = re.search(
            r'\bBLOCK\s+[A-Z]?\d*[A-Z]?\s*,\s*([A-Z][A-Z\s\']+?)(?:,|\s+(?:SECTION|UNIT|ACCORDING|PLAT|AS\s+PER|A\s+SUBDIVISION))',
            text,
            re.IGNORECASE
        )
        if after_block:
            candidate = after_block.group(1).strip()
            # Strip leading stopwords (e.g., "OF BONITA" -> "BONITA")
            stopwords = {'THE', 'OF', 'IN', 'AT', 'A', 'AN', 'AND', 'OR', 'TO', 'FOR', 'AS', 'MAP'}
            words = candidate.upper().split()
            # Remove leading stopwords
            while words and words[0] in stopwords:
                words = words[1:]
            # Reject if first remaining word is a descriptor (not a subdivision name)
            # e.g., "OF THE REPLAT OF TAMPA HEIGHTS" -> "REPLAT OF TAMPA HEIGHTS" is wrong
            descriptors = {'REPLAT', 'PORTION', 'PART', 'PLAT', 'HALF', 'QUARTER',
                          'SECTION', 'EAST', 'WEST', 'NORTH', 'SOUTH', 'NE', 'NW', 'SE', 'SW'}
            if words and words[0] not in descriptors and len(' '.join(words)) >= 3:
                result.subdivision = ' '.join(words)

    # Fallback 3: "LOT X, SUBDIVISION_NAME, ACCORDING/PLAT..." (no block)
    if not result.subdivision and result.lot and not result.block:
        after_lot = re.search(
            r'\bLOT\s+[A-Z]?\d+[A-Z]?\s*,\s*([A-Z][A-Z\s\']+?)(?:,|\s+(?:ACCORDING|PLAT|AS\s+PER|A\s+SUBDIVISION))',
            text,
            re.IGNORECASE
        )
        if after_lot:
            candidate = after_lot.group(1).strip()
            stopwords = {'THE', 'OF', 'IN', 'AT', 'A', 'AN', 'AND', 'OR', 'TO', 'FOR', 'AS', 'MAP'}
            words = candidate.upper().split()
            if words and words[0] not in stopwords and len(candidate) >= 3:
                result.subdivision = candidate

    # If we *did* find a subdivision but it's very short, try to enrich it with the
    # fuller prefix before LOT/BLOCK (common for "UNIT NO 06 ..." style legal text).
    if result.subdivision and (result.lot or result.block):
        before_lot = re.split(r'\b(LOT|L|LT|BLOCK|BLK|B)\b', text)[0].strip()
        if before_lot:
            # Prefer the prefix up through "UNIT (NO) <num>" if present.
            unit_prefix = None
            unit_match = re.search(r'^(.*?\bUNIT\b(?:\s+NO)?\s+\d+)', before_lot)
            if unit_match:
                unit_prefix = unit_match.group(1).strip()

            candidate = unit_prefix or before_lot
            candidate = re.sub(r'\s+', ' ', candidate).strip()
            if candidate and len(candidate) > len(result.subdivision) and result.subdivision in candidate:
                result.subdivision = candidate

    return result


def generate_search_permutations(legal: LegalDescription, raw_legal: str = "", max_permutations: int = 20) -> List[str]:
    """
    Generate search string permutations for ORI search.

    ORI uses CONTAINS search and wildcards (*) for partial matching.

    CRITICAL: ORI indexes legal descriptions with LOT/BLOCK FIRST, like:
        "L 44 B 2 SYMPHONY ISLES #2"
        "L 9 B D BRUSSELS BOY"

    So we must search with lot/block first for best results:
        "L 44 B 2 SYMPHONY ISLES" - EXACT match first (no wildcard)
        "L 44 B 2 SYMPHONY*" - wildcard as fallback
        "SYMPHONY*" alone returns random lots from the subdivision

    SEARCH STRATEGY (to avoid >300 result searches):
        1. Try EXACT terms first (no wildcard) - most specific
        2. Try WILDCARD terms as fallback - broader but necessary for abbreviation variations

    Args:
        legal: Parsed legal description
        max_permutations: Maximum number of permutations to return

    Returns:
        List of search strings to try, ordered by specificity:
        - First: exact matches (no wildcard)
        - Then: wildcard matches
    """
    exact_terms = []      # No wildcard - exact match
    wildcard_terms = []   # With wildcard - broader search

    def _subdivision_prefixes(subdivision: str) -> List[str]:
        if not subdivision:
            return []

        # Use regex tokenization to split punctuation cleanly (e.g., "TOWN'N" -> ["TOWN", "N"]).
        raw_words = re.findall(r"[A-Z0-9]+", subdivision.upper())
        stop = {"THE", "A", "AN", "AT", "OF", "IN"}
        words = [w for w in raw_words if w and w not in stop]

        # Guard: metes-and-bounds false positives like "BEGIN AT..."
        if words and words[0] in {"BEGIN", "BEG", "COMMENCE", "COMMENCING", "COM", "TRACT"}:
            return []

        # Handle possessive apostrophe patterns (e.g., "TURMAN'S" -> ["TURMAN", "S"])
        # Merge back to "TURMANS" as an alternate form for ORI search
        # e.g., ["TURMAN", "S", "EAST", "YBOR"] -> ["TURMANS", "EAST", "YBOR"]
        merged_words = []
        i = 0
        while i < len(words):
            if i + 1 < len(words) and words[i + 1] == "S" and len(words[i + 1]) == 1:
                # This looks like "WORD'S" split into ["WORD", "S"]
                merged_words.append(words[i] + "S")  # TURMANS
                i += 2  # Skip the "S"
            else:
                merged_words.append(words[i])
                i += 1

        # Most-specific prefixes first (3 words -> 2 -> 1)
        prefixes: List[str] = []
        max_words = min(3, len(words))
        for k in range(max_words, 0, -1):
            prefixes.append(" ".join(words[:k]))

        # Also add merged versions (e.g., "TURMANS EAST" in addition to "TURMAN S EAST")
        if merged_words != words:
            max_merged = min(3, len(merged_words))
            for k in range(max_merged, 0, -1):
                merged_prefix = " ".join(merged_words[:k])
                if merged_prefix not in prefixes:
                    prefixes.append(merged_prefix)

        # De-dup while preserving order
        seen: set[str] = set()
        out: List[str] = []
        for p in prefixes:
            if p not in seen:
                seen.add(p)
                out.append(p)
        return out

    subdiv_prefixes = _subdivision_prefixes(legal.subdivision or "")

    # Prefer all detected lots (handles partial/multi-lot parcels like "LOT 18 ... LOT 19 ...").
    lots_to_use = [lot for lot in (legal.lots or []) if lot] or ([legal.lot] if legal.lot else [])

    # Detect partial lots (keywords indicating less than full lot)
    raw_legal_upper = raw_legal.upper() if raw_legal else ""
    is_partial_lot = any(kw in raw_legal_upper for kw in [
        "LESS THE", "LESS ", " PART OF", "PORTION OF", " PT OF",
        " N ", " S ", " E ", " W ",  # Directional partials like "N 54 FT OF"
        "NORTH ", "SOUTH ", "EAST ", "WEST ",
    ])
    partial_prefix = "PT " if is_partial_lot else ""

    # Check if lots are consecutive (for range notation like "L 1-3")
    def are_consecutive(lot_list: list[str]) -> bool:
        """Check if lots are consecutive integers."""
        try:
            nums = sorted(int(lot) for lot in lot_list if lot.isdigit())
            return len(nums) >= 2 and nums == list(range(nums[0], nums[-1] + 1))
        except (ValueError, TypeError):
            return False

    # Helper to add both exact and wildcard terms
    def add_term(term: str) -> None:
        """Add both exact (no wildcard) and wildcard version of a term."""
        # Clean up term - remove trailing wildcard if present
        base = term.rstrip('*').strip()
        if base:
            if base not in exact_terms:
                exact_terms.append(base)
            wildcard = f"{base}*"
            if wildcard not in wildcard_terms:
                wildcard_terms.append(wildcard)

    # Priority 0: Multi-lot combined format
    # ORI uses different formats:
    # - Full lots: "L 1 AND 2 B R" (CASTLE HEIGHTS style)
    # - Partial lots: "PT L 1-3 B 100" (PORT TAMPA style with range notation)
    if len(lots_to_use) >= 2 and legal.block:
        # For partial/consecutive lots, use range notation (e.g., "PT L 1-3 B 100")
        if is_partial_lot and are_consecutive(lots_to_use):
            lot_range = f"{lots_to_use[0]}-{lots_to_use[-1]}"
            if subdiv_prefixes:
                add_term(f"PT L {lot_range} B {legal.block} {subdiv_prefixes[0]}")
            add_term(f"PT L {lot_range} B {legal.block}")
            add_term(f"L {lot_range} B {legal.block}")  # Also try without PT

        # Generate "L 1 AND 2 B R" format for first two lots
        combined_lots = " AND ".join(lots_to_use[:2])
        # Try with subdivision prefix FIRST (more specific)
        if subdiv_prefixes:
            for prefix in subdiv_prefixes[:2]:  # Only first 2 prefixes to avoid explosion
                add_term(f"{partial_prefix}L {combined_lots} B {legal.block} {prefix}")
            # Try with MAP prefix (common in ORI: "L 1 AND 2 B R MAP OF CASTLE HEIGHTS")
            add_term(f"{partial_prefix}L {combined_lots} B {legal.block} MAP")
        # Then without subdivision prefix
        add_term(f"{partial_prefix}L {combined_lots} B {legal.block}")

    # Priority 1: Most specific - Lot + Block + Subdivision (FULL name first, then prefixes)
    # Try both formats: "L {lot} B {block}" (with space) FIRST for API compatibility,
    # then "L{lot} B{block}" (no space) for browser search
    if lots_to_use and legal.block and subdiv_prefixes:
        for lot in lots_to_use:
            # Try FULL subdivision name first (most specific)
            if legal.subdivision:
                add_term(f"L {lot} B {legal.block} {legal.subdivision}")
                add_term(f"L{lot} B{legal.block} {legal.subdivision}")
                if not legal.block.isdigit():
                    add_term(f"L {lot} BLK {legal.block} {legal.subdivision}")

            # Then try prefixes (progressively less specific)
            for prefix in subdiv_prefixes:
                # With space format (e.g., "L 40 B 1 TEMPLE OAKS") - works with API CONTAINS
                add_term(f"L {lot} B {legal.block} {prefix}")
                # No space format (e.g., "L40 B1 TEMPLE OAKS") - browser wildcard search
                add_term(f"L{lot} B{legal.block} {prefix}")
                # Also try with BLK for alpha blocks (some records use BLK D instead of B D)
                if not legal.block.isdigit():
                    add_term(f"L {lot} BLK {legal.block} {prefix}")
                    add_term(f"L{lot} BLK{legal.block} {prefix}")

    # Priority 2: Lot + Block only (no subdivision) - only if we have lot/block
    # This is useful when the subdivision name varies in ORI records
    if lots_to_use and legal.block:
        for lot in lots_to_use:
            # Only add exact matches for lot+block (wildcard would be too broad)
            # These won't get wildcards via add_term; we add exact only
            term = f"L {lot} B {legal.block}"
            if term not in exact_terms:
                exact_terms.append(term)

    # Priority 3: Lot + Subdivision (no block)
    if lots_to_use and subdiv_prefixes:
        for lot in lots_to_use:
            # Full subdivision first
            if legal.subdivision:
                add_term(f"L {lot} {legal.subdivision}")
            # Then prefixes
            for prefix in subdiv_prefixes:
                add_term(f"L {lot} {prefix}")
                add_term(f"L{lot} {prefix}")

    # Priority 4: Just subdivision name (broader search)
    # This returns all lots in subdivision - useful as fallback
    # NOTE: Only add wildcard versions for subdivision-only searches
    if subdiv_prefixes:
        # Include the longest prefixes first (most specific)
        for prefix in subdiv_prefixes:
            wildcard = f"{prefix}*"
            if wildcard not in wildcard_terms:
                wildcard_terms.append(wildcard)

    # Priority 5: For condos, try unit + building name
    if legal.unit and subdiv_prefixes:
        if legal.subdivision:
            add_term(f"UNIT {legal.unit} {legal.subdivision}")
            add_term(f"U {legal.unit} {legal.subdivision}")
        for prefix in subdiv_prefixes[:2]:
            add_term(f"UNIT {legal.unit} {prefix}")
            add_term(f"U {legal.unit} {prefix}")

    # Priority 6: If we have section-township-range, add those searches
    if legal.section and legal.township and legal.range:
        str_search = f"{legal.section}-{legal.township}-{legal.range}"
        if subdiv_prefixes:
            add_term(f"{str_search} {subdiv_prefixes[0]}")
        add_term(str_search)
        # Narrative STR search (works better for metes-and-bounds ORI indexing)
        add_term(f"SECTION {legal.section} TOWNSHIP {legal.township} RANGE {legal.range}")
        add_term(f"SECTION {legal.section} TOWNSHIP {legal.township}")

    # Fallback: If we have lot/block but no subdivision, DON'T search with wildcards
    # Searches like "L 6 B 26*" are too broad and match hundreds of properties
    # across different subdivisions. We only add exact "L X B Y" in Priority 2.

    # Last resort: use a specific prefix of the raw text (for metes-and-bounds).
    # For metes-and-bounds (often starts with COM/BEG/etc), a longer prefix is far more specific
    # than any single token and reduces irrelevant matches.
    has_terms = bool(exact_terms or wildcard_terms)
    if not has_terms and legal.raw_text:
        words = legal.raw_text.upper().strip().split()
        raw_upper = legal.raw_text.upper().lstrip()
        # Strip leading section numbers (e.g., "1\tCOM..." or "1 COM...")
        raw_upper = re.sub(r'^\d+[\t\s]+', '', raw_upper)
        if raw_upper.startswith(("COM ", "BEG ", "BEGIN", "COMMENCE", "COMMENCING", "TRACT")):
            # Try extracting road names (often stable across recordings) before falling back to a raw prefix.
            road_regex = re.compile(
                r"\b([A-Z]{3,}(?:\s+[A-Z]{3,}){0,3})\s+"
                r"(RD|ROAD|DR|DRIVE|AVE|AVENUE|ST|STREET|BLVD|BOULEVARD|LN|LANE|CT|COURT|PL|PLACE|WAY|HWY|HIGHWAY)\b"
            )
            for m in road_regex.finditer(raw_upper):
                name = re.sub(r"\s+", " ", m.group(1).strip())
                suffix = m.group(2).strip()
                if name:
                    # For road names, add both exact and wildcard
                    add_term(f"{name} {suffix}")
                    add_term(name)

            # Add a long prefix as wildcard-only fallback
            prefix = raw_upper[:60].strip()
            if prefix:
                wildcard = f"{prefix}*"
                if wildcard not in wildcard_terms:
                    wildcard_terms.append(wildcard)

            # Combine and return
            result = exact_terms + wildcard_terms
            return result[:max_permutations]

        common_prefixes = {
            # Lot/block identifiers
            "LOT", "L", "LT", "BLOCK", "BLK", "B", "UNIT", "U",
            # Articles/prepositions
            "THE", "OF", "IN", "AT", "TO", "FOR", "AND", "OR", "A", "AN",
            # Metes-and-bounds terms (too generic for searches)
            "COM", "COMMENCE", "COMMENCING", "BEG", "BEGIN", "BEGINNING",
            "RUN", "THN", "THENCE", "ALG", "ALONG", "CONT", "CONTINUE",
            "LINE", "LINES", "POINT", "CORNER", "COR", "BDRY", "BOUNDARY",
            "FEET", "FT", "FOOT", "DEGREES", "DEG", "MINUTES", "MIN",
            "NORTH", "SOUTH", "EAST", "WEST", "NLY", "SLY", "ELY", "WLY",
            "NORTHERLY", "SOUTHERLY", "EASTERLY", "WESTERLY",
            "RIGHT", "LEFT", "SAID", "BEING", "LYING", "LESS", "MORE",
            "PORTION", "PART", "HALF", "QUARTER", "SECTION", "SEC",
            "TOWNSHIP", "TWP", "RANGE", "RNG", "THAT", "THEN", "FROM",
            "WITH", "ALSO", "PLAT", "BOOK", "PAGE", "RECORD", "RECORDED",
        }

        for word in words:
            if word in common_prefixes:
                continue
            # Prefer alphabetic tokens; numeric tokens are too broad.
            cleaned = re.sub(r"[^A-Z]", "", word)
            if len(cleaned) >= 4:
                # For single-word fallback, use wildcard only
                wildcard = f"{cleaned}*"
                if wildcard not in wildcard_terms:
                    wildcard_terms.append(wildcard)
                break

        if not wildcard_terms:
            prefix = raw_upper[:60].strip()
            if prefix:
                wildcard_terms.append(f"{prefix}*")

    # Combine exact terms first, then wildcard terms
    # This ensures we try exact matches before broader wildcard searches
    result = exact_terms + wildcard_terms

    return result[:max_permutations]


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
        search_terms.extend(generate_search_permutations(parsed, judgment_legal))

    # Priority 2: Combined bulk data legal description
    bulk_legal = combine_legal_fields(legal1 or "", legal2, legal3, legal4)
    if bulk_legal:
        parsed = parse_legal_description(bulk_legal)
        for term in generate_search_permutations(parsed, bulk_legal):
            if term not in search_terms:
                search_terms.append(term)

    # Priority 3: Try each legal field individually
    for legal_field in [legal1, legal2, legal3, legal4]:
        if not legal_field or not legal_field.strip():
            continue

        parsed = parse_legal_description(legal_field)

        words = legal_field.strip().upper().split()
        first_significant = None
        for word in words:
            if word not in [
                "LOT",
                "L",
                "LT",
                "BLOCK",
                "BLK",
                "B",
                "UNIT",
                "U",
                "THE",
                "OF",
                "IN",
                "AT",
            ] and len(word) >= 4:
                first_significant = word
                break

        if first_significant:
            term = f"{first_significant}*"
            if term not in search_terms:
                search_terms.append(term)

            if parsed.lot:
                term = f"L {parsed.lot} {first_significant}*"
                if term not in search_terms:
                    search_terms.append(term)

    # Filter out overly generic terms that would return too many results
    generic_terms = {'BLOCK*', 'LOT*', 'UNIT*', 'PHASE*', 'THE*', 'PLAT*', 'BOOK*', 'PAGE*',
                     'NORTH*', 'SOUTH*', 'EAST*', 'WEST*', 'SECTION*', 'TOWNSHIP*', 'RANGE*',
                     'LESS*', 'THAT*', 'PART*', 'BEING*', 'ALSO*', 'A*', 'AN*', 'AND*',
                     'A SUBDIVISION*', 'A SUB*', 'ACCORDING*', 'CORNER*', 'COMMENCE*',
                     'THENCE*', 'RUN*', 'POINT*', 'TRACT*'}
    filtered_terms = [t for t in search_terms if t.upper() not in generic_terms]

    # Filter out year-only terms (e.g., "1997*", "2005*") - these are not subdivision names
    def is_year_term(term: str) -> bool:
        """Check if term is just a year (4 digits) with wildcard."""
        base = term.rstrip('*').strip()
        return base.isdigit() and len(base) == 4 and 1900 <= int(base) <= 2100

    # Filter out measurement terms (decimals like "251.29*", "72.15*")
    def is_measurement_term(term: str) -> bool:
        """Check if term looks like a measurement (decimal number)."""
        base = term.rstrip('*').strip()
        # Remove L/LOT prefix if present
        for prefix in ['L ', 'LOT ']:
            if base.startswith(prefix):
                base = base.removeprefix(prefix)
        # Check if it's a decimal number or numeric with decimal
        try:
            float(base)
            return True
        except ValueError:
            pass
        # Also catch patterns like "W 251.29" (direction + number)
        parts = base.split()
        if len(parts) >= 1:
            last_part = parts[-1]
            try:
                float(last_part)
                # If the last word is a number, it's likely a measurement
                return True
            except ValueError:
                pass
        return False

    filtered_terms = [t for t in filtered_terms if not is_year_term(t)]
    filtered_terms = [t for t in filtered_terms if not is_measurement_term(t)]

    # Ensure search terms are specific enough (at least one word with 4+ characters before wildcard)
    specific_terms = []
    for term in filtered_terms:
        # Get the part before wildcard
        base = term.rstrip('*')
        words = base.split()
        # Check if any word has 4+ characters (excluding L, B, LOT, BLOCK, etc.)
        # and is alphabetic (not a number)
        has_specific = any(
            len(w) >= 4 and w not in {'BLOCK', 'UNIT', 'PHASE'} and not w.replace('.', '').isdigit()
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


# Abbreviation normalizations for subdivision fuzzy matching
SUBDIVISION_NORMALIZATIONS = {
    # Phase variations
    r'\bPH\b': 'PHASE',
    r'\bPHASE\b': 'PHASE',
    # Section variations
    r'\bSEC\b': 'SECTION',
    r'\bSECT\b': 'SECTION',
    # Roman numerals to Arabic
    r'\bII\b': '2',
    r'\bIII\b': '3',
    r'\bIV\b': '4',
    r'\bV\b': '5',
    r'\bVI\b': '6',
    r'\bVII\b': '7',
    r'\bVIII\b': '8',
    r'\bIX\b': '9',
    r'\bX\b': '10',
    # Word numerals to Arabic
    r'\bONE\b': '1',
    r'\bTWO\b': '2',
    r'\bTHREE\b': '3',
    r'\bFOUR\b': '4',
    r'\bFIVE\b': '5',
    r'\bSIX\b': '6',
    r'\bSEVEN\b': '7',
    r'\bEIGHT\b': '8',
    r'\bNINE\b': '9',
    r'\bTEN\b': '10',
    # Block variations
    r'\bBLK\b': 'BLOCK',
    r'\bBK\b': 'BLOCK',
    # Subdivision variations
    r'\bSUBDIV\b': 'SUBDIVISION',
    r'\bSUBD\b': 'SUBDIVISION',
    r'\bSUB\b': 'SUBDIVISION',
    r'\bS/D\b': 'SUBDIVISION',
}


def normalize_subdivision_for_matching(subdivision: str) -> str:
    """
    Normalize a subdivision name for fuzzy matching.

    Expands abbreviations and normalizes numerals so that:
    - "TOUCHSTONE PH 2" → "TOUCHSTONE PHASE 2"
    - "TAMPA PALMS SEC II" → "TAMPA PALMS SECTION 2"

    Args:
        subdivision: Raw subdivision name

    Returns:
        Normalized subdivision name
    """
    if not subdivision:
        return ""

    result = subdivision.upper().strip()

    # Normalize whitespace
    result = re.sub(r'\s+', ' ', result)

    # Remove punctuation (commas, periods) but keep apostrophes for names like O'BRIEN
    result = re.sub(r'[,.]', ' ', result)
    result = re.sub(r'\s+', ' ', result).strip()

    # Apply normalizations
    for pattern, replacement in SUBDIVISION_NORMALIZATIONS.items():
        result = re.sub(pattern, replacement, result)

    return result


def legal_descriptions_match(
    legal_a: str,
    legal_b: str,
    threshold: float = 0.80
) -> Tuple[bool, float, str]:
    """
    Check if two legal descriptions refer to the same property.

    Uses component-based matching:
    - Lot number(s): EXACT match required
    - Block number(s): EXACT match required
    - Subdivision: FUZZY match (normalized, then compared)

    Args:
        legal_a: First legal description
        legal_b: Second legal description
        threshold: Minimum fuzzy ratio for subdivision match (0.0-1.0)

    Returns:
        Tuple of (is_match, confidence_score, reason)
        - is_match: True if descriptions match
        - confidence_score: 0.0-1.0 indicating match quality
        - reason: Explanation of match/no-match
    """
    if not legal_a or not legal_b:
        return False, 0.0, "One or both legal descriptions empty"

    # Parse both legal descriptions
    parsed_a = parse_legal_description(legal_a)
    parsed_b = parse_legal_description(legal_b)

    # Get lots (use lots list if available, else single lot)
    lots_a = set(parsed_a.lots) if parsed_a.lots else ({parsed_a.lot} if parsed_a.lot else set())
    lots_b = set(parsed_b.lots) if parsed_b.lots else ({parsed_b.lot} if parsed_b.lot else set())

    # Get blocks
    block_a = parsed_a.block
    block_b = parsed_b.block

    # Get subdivisions - include phase/section in comparison if parsed separately
    subdiv_a = parsed_a.subdivision or ""
    subdiv_b = parsed_b.subdivision or ""

    # Append phase to subdivision if parsed separately (for consistent comparison)
    # e.g., "TOUCHSTONE" + phase=2 → "TOUCHSTONE PHASE 2"
    if parsed_a.phase and parsed_a.phase not in subdiv_a:
        subdiv_a = f"{subdiv_a} PHASE {parsed_a.phase}".strip()
    if parsed_b.phase and parsed_b.phase not in subdiv_b:
        subdiv_b = f"{subdiv_b} PHASE {parsed_b.phase}".strip()

    # Also check for unit (condos)
    unit_a = parsed_a.unit
    unit_b = parsed_b.unit

    # Track match components
    lot_match = None
    block_match = None
    subdiv_match = None
    subdiv_score = 0.0
    unit_match = None

    # --- Lot Matching (EXACT) ---
    if lots_a and lots_b:
        # Check if there's any overlap in lots
        lot_overlap = lots_a & lots_b
        if lot_overlap:
            lot_match = True
        else:
            lot_match = False
            return False, 0.0, f"Lot mismatch: {lots_a} vs {lots_b}"
    elif lots_a or lots_b:
        # One has lots, other doesn't - can't confirm match on lots
        lot_match = None  # Indeterminate

    # --- Block Matching (EXACT) ---
    if block_a and block_b:
        if block_a == block_b:
            block_match = True
        else:
            block_match = False
            return False, 0.0, f"Block mismatch: {block_a} vs {block_b}"
    elif block_a or block_b:
        # One has block, other doesn't
        block_match = None  # Indeterminate

    # --- Unit Matching (EXACT, for condos) ---
    if unit_a and unit_b:
        if unit_a == unit_b:
            unit_match = True
        else:
            unit_match = False
            return False, 0.0, f"Unit mismatch: {unit_a} vs {unit_b}"

    # --- Subdivision Matching (FUZZY) ---
    if subdiv_a and subdiv_b:
        # Normalize both subdivisions
        norm_a = normalize_subdivision_for_matching(subdiv_a)
        norm_b = normalize_subdivision_for_matching(subdiv_b)

        # Use token_set_ratio for better partial matching
        # This handles cases like "TAMPA PALMS" vs "TAMPA PALMS SECTION 20"
        # token_set_ratio is order-independent and handles subsets
        subdiv_score = fuzz.token_set_ratio(norm_a, norm_b) / 100.0

        logger.debug(
            f"Legal fuzzy match: '{subdiv_a}' vs '{subdiv_b}' | "
            f"normalized: '{norm_a}' vs '{norm_b}' | score: {subdiv_score:.2f}"
        )

        if subdiv_score >= threshold:
            subdiv_match = True
        else:
            subdiv_match = False
            reason = f"Subdivision mismatch: '{subdiv_a}' vs '{subdiv_b}' (score: {subdiv_score:.2f})"
            logger.debug(f"Legal match FAILED: {reason}")
            return False, subdiv_score, reason
    elif subdiv_a or subdiv_b:
        # One has subdivision, other doesn't - this is suspicious
        # If one legal explicitly names a subdivision and other doesn't, don't match
        subdiv_match = None  # Indeterminate - but require at least one solid match
    else:
        # Neither has subdivision - we can't verify they're the same property
        # Just having matching lot/block isn't enough (lot 4 block 8 exists in many subdivisions)
        # Mark as indeterminate but we'll require subdivision match for high confidence
        subdiv_match = None

    # --- Calculate overall confidence ---
    # Count how many components matched vs were testable
    matched_components = sum(1 for m in [lot_match, block_match, subdiv_match, unit_match] if m is True)
    testable_components = sum(1 for m in [lot_match, block_match, subdiv_match, unit_match] if m is not None)

    if testable_components == 0:
        # No components could be compared - can't determine match
        return False, 0.0, "No comparable components (lot, block, subdivision, unit)"

    # All testable components must match
    all_matched = all(m is True for m in [lot_match, block_match, subdiv_match, unit_match] if m is not None)

    if all_matched:
        # CRITICAL: For lot/block matches, we REQUIRE subdivision confirmation
        # "Lot 4 Block 8" could exist in hundreds of subdivisions
        # Without subdivision match, we can't confirm it's the same property
        if (lot_match or block_match) and subdiv_match is None:
            # We have lot/block match but couldn't compare subdivisions
            # This is NOT a confident match - could be different properties
            reason = f"Lot/block match but no subdivision to verify (lot={lots_a}, block={block_a})"
            logger.debug(f"Legal match FAILED: {reason} | '{legal_a}' vs '{legal_b}'")
            return False, 0.3, reason

        # Calculate confidence based on what matched
        # More components matched = higher confidence
        base_confidence = matched_components / max(testable_components, 1)

        # Weight subdivision score into confidence if available
        if subdiv_match is True and subdiv_score > 0:
            confidence = (base_confidence + subdiv_score) / 2
        else:
            confidence = base_confidence

        # Build reason string
        matched_parts = []
        if lot_match:
            matched_parts.append(f"lot={list(lots_a & lots_b)}")
        if block_match:
            matched_parts.append(f"block={block_a}")
        if unit_match:
            matched_parts.append(f"unit={unit_a}")
        if subdiv_match:
            matched_parts.append(f"subdiv={subdiv_a} ({subdiv_score:.2f})")

        reason = f"Match: {', '.join(matched_parts)}"
        logger.debug(f"Legal match SUCCESS: {reason} | '{legal_a}' vs '{legal_b}'")
        return True, confidence, reason
    # Should not reach here due to early returns, but just in case
    return False, 0.0, "Component mismatch"


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
        perms = generate_search_permutations(parsed, test)
        print(f"Search permutations: {perms}")

    # Test legal_descriptions_match()
    print("\n" + "=" * 60)
    print("LEGAL DESCRIPTION MATCHING TESTS")
    print("=" * 60)

    match_test_cases = [
        # Should match - same property different formats
        ("L 4 B 8 TOUCHSTONE PH 2", "LOT 4 BLOCK 8 TOUCHSTONE PHASE 2", True),
        ("LOT 15 BLOCK 3 TAMPA PALMS SEC 20", "L 15 B 3 TAMPA PALMS SECTION 20", True),
        ("UNIT 5 HARBOUR ISLAND CONDO", "UNIT 5 HARBOUR ISLAND CONDOMINIUM", True),
        ("L 4 B 8 TOUCHSTONE SUBDIVISION", "LOT 4 BLOCK 8 TOUCHSTONE SUB", True),
        # Should NOT match - different lots
        ("L 4 B 8 TOUCHSTONE PH 2", "L 5 B 8 TOUCHSTONE PH 2", False),
        # Should NOT match - different blocks
        ("L 4 B 8 TOUCHSTONE PH 2", "L 4 B 9 TOUCHSTONE PH 2", False),
        # Should NOT match - different subdivisions
        ("L 4 B 8 TOUCHSTONE SUBDIVISION", "L 4 B 8 SWEETWATER SUBDIVISION", False),
        # Should NOT match - can't verify (parser limitation - no subdivision extracted)
        # "LOT 4 BLOCK 8 TOUCHSTONE PHASE TWO" - parser can't extract subdivision (TWO not digit)
        ("L 4 B 8 TOUCHSTONE PH 2", "LOT 4 BLOCK 8 TOUCHSTONE PHASE TWO", False),
        # Should NOT match - lot/block only without subdivision verification
        ("L 4 B 8 TOUCHSTONE PH 2", "L 4 B 8 SWEETWATER TOWNHOMES", False),
    ]

    for legal_a, legal_b, expected in match_test_cases:
        is_match, confidence, reason = legal_descriptions_match(legal_a, legal_b)
        status = "✅" if is_match == expected else "❌"
        print(f"\n{status} '{legal_a}' vs '{legal_b}'")
        print(f"   Expected: {expected}, Got: {is_match}, Confidence: {confidence:.2f}")
        print(f"   Reason: {reason}")
