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
    result = LegalDescription(raw_text=raw_text)

    # Extract lot number(s) (various patterns)
    # Lots can be numbers, letters, or alphanumeric (e.g., "5", "J", "5A", "AA")
    lot_patterns = [
        r'\bLOT\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',  # LOT 5, LOT J, LOT 5A, LOT AA
        r'\bLOTS\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',  # LOTS 5, LOTS J (captures first)
        r'\bL\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',     # L 5, L J
        r'\bLT\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b',    # LT 5, LT J
    ]

    lots_found: List[str] = []
    for pattern in lot_patterns[:2]:  # only LOT/LOTS patterns for multi-lot capture
        for match in re.finditer(pattern, text):
            val = match.group(1)
            if val and val not in lots_found:
                lots_found.append(val)

    # Also catch multiple "LOT <x>" occurrences (common: "LOT 18 ... LOT 19 ...")
    for match in re.finditer(r'\bLOT\s+([A-Z]?\d+[A-Z]?|[A-Z]{1,2})\b', text):
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


def generate_search_permutations(legal: LegalDescription, max_permutations: int = 10) -> List[str]:
    """
    Generate search string permutations for ORI search.

    ORI uses CONTAINS search and wildcards (*) for partial matching.

    CRITICAL: ORI indexes legal descriptions with LOT/BLOCK FIRST, like:
        "L 44 B 2 SYMPHONY ISLES #2"
        "L 9 B D BRUSSELS BOY"

    So we must search with lot/block first for best results:
        "L 44 B 2 SYMPHONY*" - finds the specific lot
        "SYMPHONY*" alone returns random lots from the subdivision

    Args:
        legal: Parsed legal description
        max_permutations: Maximum number of permutations to return

    Returns:
        List of search strings to try, ordered by specificity (all with wildcards)
    """
    permutations = []

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

        # Most-specific prefixes first (3 words -> 2 -> 1)
        prefixes: List[str] = []
        max_words = min(3, len(words))
        for k in range(max_words, 0, -1):
            prefixes.append(" ".join(words[:k]))

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

    # Priority 1: Most specific - Lot + Block + Subdivision first word
    # This is the ORI-optimized format: "L {lot} B {block} {subdivision}*"
    if lots_to_use and legal.block and subdiv_prefixes:
        for lot in lots_to_use:
            for prefix in subdiv_prefixes:
                permutations.append(f"L {lot} B {legal.block} {prefix}*")
                # Also try with BLK for alpha blocks (some records use BLK D instead of B D)
                if not legal.block.isdigit():
                    permutations.append(f"L {lot} BLK {legal.block} {prefix}*")

    # Priority 2: Lot + Subdivision (no block)
    if lots_to_use and subdiv_prefixes:
        for lot in lots_to_use:
            for prefix in subdiv_prefixes:
                permutations.append(f"L {lot} {prefix}*")

    # Priority 3: Just subdivision name with wildcard (broader search)
    # This returns all lots in subdivision - useful as fallback
    if subdiv_prefixes:
        # Include the shortest prefixes last (1-word, then 2-word if available)
        permutations.append(f"{subdiv_prefixes[-1]}*")
        if len(subdiv_prefixes) >= 2:
            permutations.append(f"{subdiv_prefixes[-2]}*")

    # For condos, try unit + building name with wildcard
    if legal.unit and subdiv_prefixes:
        permutations.append(f"UNIT {legal.unit} {subdiv_prefixes[0]}*")
        permutations.append(f"U {legal.unit} {subdiv_prefixes[0]}*")

    # If we have section-township-range, add that with wildcard
    if legal.section and legal.township and legal.range:
        str_search = f"{legal.section}-{legal.township}-{legal.range}"
        if subdiv_prefixes:
            permutations.append(f"{str_search} {subdiv_prefixes[0]}*")
        permutations.append(f"{str_search}*")
        # Narrative STR search (works better for metes-and-bounds ORI indexing)
        permutations.append(f"SECTION {legal.section} TOWNSHIP {legal.township}*")
        permutations.append(f"SECTION {legal.section} TOWNSHIP {legal.township} RANGE {legal.range}*")

    # Fallback: If we have lot/block but no subdivision, try lot patterns
    if not permutations and legal.lot:
        if legal.block:
            permutations.append(f"L {legal.lot} B {legal.block}*")
        else:
            permutations.append(f"L {legal.lot}*")

    # Last resort: use a specific prefix of the raw text.
    # For metes-and-bounds (often starts with COM/BEG/etc), a longer prefix is far more specific
    # than any single token and reduces irrelevant matches.
    if not permutations and legal.raw_text:
        words = legal.raw_text.upper().strip().split()
        raw_upper = legal.raw_text.upper().lstrip()
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
                    permutations.append(f"{name} {suffix}*")
                    permutations.append(f"{name}*")

            prefix = raw_upper[:60].strip()
            if prefix:
                permutations.append(f"{prefix}*")

            # Remove duplicates while preserving order, then return early.
            seen = set()
            deduped = []
            for p in permutations:
                if p not in seen:
                    seen.add(p)
                    deduped.append(p)
            return deduped[:max_permutations]

        common_prefixes = {
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
            "COM",
            "COMMENCE",
            "COMMENCING",
            "BEG",
            "BEGIN",
            "BEGINNING",
            "RUN",
            "THN",
            "THENCE",
            "ALG",
        }

        for word in words:
            if word in common_prefixes:
                continue
            # Prefer alphabetic tokens; numeric tokens are too broad.
            cleaned = re.sub(r"[^A-Z]", "", word)
            if len(cleaned) >= 4:
                permutations.append(f"{cleaned}*")
                break

        if not permutations:
            prefix = raw_upper[:60].strip()
            if prefix:
                permutations.append(f"{prefix}*")

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
