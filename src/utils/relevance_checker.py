"""
Document relevance checker - ensures documents belong to target property.
"""
import re
from typing import Dict, Optional, Tuple
from difflib import SequenceMatcher

from src.utils.legal_description import parse_legal_description


def extract_lot_block(legal_description: str) -> Optional[Tuple[str, str]]:
    """
    Extract LOT and BLOCK numbers from a legal description.

    Args:
        legal_description: Legal description text

    Returns:
        Tuple of (lot, block) if found, None otherwise
    """
    if not legal_description:
        return None

    legal = legal_description.upper()

    # Common patterns for LOT/BLOCK
    # "LOT 9 BLOCK 12", "L 9 B 12", "LOT 9 BLK 12", "L9 B12"
    patterns = [
        r'L(?:OT)?\s*(\d+)\s*B(?:L(?:OC)?K?)?\s*(\d+)',  # LOT 9 BLOCK 12, L 9 B 12
        r'B(?:L(?:OC)?K?)?\s*(\d+)\s*L(?:OT)?\s*(\d+)',  # BLOCK 12 LOT 9 (reversed)
    ]

    for pattern in patterns:
        match = re.search(pattern, legal)
        if match:
            groups = match.groups()
            # Handle reversed pattern
            if 'B' in pattern[:10]:  # Pattern starts with Block
                return (groups[1], groups[0])  # (lot, block)
            return (groups[0], groups[1])  # (lot, block)

    return None


def extract_unit_condo(legal_description: str) -> Optional[Tuple[str, str]]:
    """
    Extract UNIT and CONDO/BUILDING info from a legal description.

    Args:
        legal_description: Legal description text

    Returns:
        Tuple of (unit, condo_name) if found, None otherwise
    """
    if not legal_description:
        return None

    legal = legal_description.upper()

    # Pattern for condos: "UNIT 123 BUILDING A" or "UNIT 123, CONDO NAME"
    unit_match = re.search(r'UNIT\s*[#]?\s*(\w+)', legal)
    if unit_match:
        unit = unit_match.group(1)
        # Try to find condo name
        condo_match = re.search(r'(?:CONDO(?:MINIUM)?|BLDG?|BUILDING)\s*[#]?\s*(\w+)', legal)
        condo = condo_match.group(1) if condo_match else None
        return (unit, condo)

    return None


def normalize_address(address: str) -> str:
    """
    Normalize an address for comparison.

    Args:
        address: Street address

    Returns:
        Normalized address string
    """
    if not address:
        return ""

    addr = address.upper().strip()

    # Standard abbreviations
    replacements = {
        ' STREET': ' ST',
        ' AVENUE': ' AVE',
        ' BOULEVARD': ' BLVD',
        ' DRIVE': ' DR',
        ' ROAD': ' RD',
        ' LANE': ' LN',
        ' COURT': ' CT',
        ' CIRCLE': ' CIR',
        ' PLACE': ' PL',
        ' NORTH': ' N',
        ' SOUTH': ' S',
        ' EAST': ' E',
        ' WEST': ' W',
        ' NORTHEAST': ' NE',
        ' NORTHWEST': ' NW',
        ' SOUTHEAST': ' SE',
        ' SOUTHWEST': ' SW',
    }

    for full, abbr in replacements.items():
        addr = addr.replace(full, abbr)

    # Remove extra whitespace
    return ' '.join(addr.split())


def similarity_score(str1: str, str2: str) -> float:
    """
    Calculate similarity score between two strings.

    Args:
        str1: First string
        str2: Second string

    Returns:
        Similarity score between 0.0 and 1.0
    """
    if not str1 or not str2:
        return 0.0

    return SequenceMatcher(None, str1.upper(), str2.upper()).ratio()


def verify_document_relevance(document: Dict, property_info: Dict) -> Dict:
    """
    Ensure a document belongs to the target property.

    Args:
        document: Dict with document data (must have 'legal_description', optionally
                  'property_address', 'folio')
        property_info: Dict with property data (must have 'legal_description',
                       optionally 'property_address', 'folio')

    Returns:
        Dict with validation results:
        {
            "legal_match": bool,
            "address_match": bool,
            "folio_match": bool,
            "similarity_score": float,
            "is_relevant": bool,
            "match_details": str
        }
    """
    checks = {
        "legal_match": False,
        "address_match": False,
        "folio_match": False,
        "similarity_score": 0.0,
        "is_relevant": False,
        "match_details": ""
    }

    details = []

    # Legal description comparison
    doc_legal = document.get('legal_description', '').upper()
    prop_legal = property_info.get('legal_description', '').upper()

    lot_block_mismatch = False
    lot_mismatch = False
    block_mismatch = False

    if doc_legal and prop_legal:
        # Extract LOT and BLOCK
        doc_lot_block = extract_lot_block(doc_legal)
        prop_lot_block = extract_lot_block(prop_legal)

        if doc_lot_block and prop_lot_block:
            checks["legal_match"] = doc_lot_block == prop_lot_block
            if checks["legal_match"]:
                details.append(f"LOT/BLOCK match: L{doc_lot_block[0]} B{doc_lot_block[1]}")
            else:
                lot_block_mismatch = True
                details.append(f"LOT/BLOCK mismatch: doc L{doc_lot_block[0]} B{doc_lot_block[1]} vs prop L{prop_lot_block[0]} B{prop_lot_block[1]}")
        else:
            # Try UNIT/CONDO match
            doc_unit = extract_unit_condo(doc_legal)
            prop_unit = extract_unit_condo(prop_legal)

            if doc_unit and prop_unit:
                checks["legal_match"] = doc_unit[0] == prop_unit[0]  # Unit numbers match
                if checks["legal_match"]:
                    details.append(f"UNIT match: {doc_unit[0]}")

        # Parsed lot/block mismatch check (handles cases where one side is missing BLOCK)
        doc_parsed = parse_legal_description(doc_legal)
        prop_parsed = parse_legal_description(prop_legal)

        if prop_parsed.lot and doc_parsed.lot and doc_parsed.lot != prop_parsed.lot:
            lot_mismatch = True
            details.append(f"LOT mismatch: doc LOT {doc_parsed.lot} vs prop LOT {prop_parsed.lot}")
        if prop_parsed.block and doc_parsed.block and doc_parsed.block != prop_parsed.block:
            block_mismatch = True
            details.append(f"BLOCK mismatch: doc BLOCK {doc_parsed.block} vs prop BLOCK {prop_parsed.block}")

        # Calculate text similarity
        checks["similarity_score"] = similarity_score(doc_legal, prop_legal)
        details.append(f"Legal similarity: {checks['similarity_score']:.2f}")

    # Address comparison
    doc_addr = document.get('property_address', '')
    prop_addr = property_info.get('property_address', '')

    if doc_addr and prop_addr:
        doc_addr_norm = normalize_address(doc_addr)
        prop_addr_norm = normalize_address(prop_addr)
        checks["address_match"] = doc_addr_norm == prop_addr_norm

        if checks["address_match"]:
            details.append(f"Address match: {prop_addr_norm}")
        else:
            # Check partial match (street number + name)
            addr_similarity = similarity_score(doc_addr_norm, prop_addr_norm)
            if addr_similarity >= 0.85:
                checks["address_match"] = True
                details.append(f"Address partial match: {addr_similarity:.2f}")

    # Folio comparison
    doc_folio = document.get('folio', '')
    prop_folio = property_info.get('folio', '')

    if doc_folio and prop_folio:
        # Normalize folio (remove dashes, spaces)
        doc_folio_norm = re.sub(r'[\s\-]', '', doc_folio)
        prop_folio_norm = re.sub(r'[\s\-]', '', prop_folio)
        checks["folio_match"] = doc_folio_norm == prop_folio_norm

        if checks["folio_match"]:
            details.append(f"Folio match: {prop_folio}")

    # Overall verdict
    if lot_block_mismatch or lot_mismatch or block_mismatch:
        checks["is_relevant"] = False
    else:
        checks["is_relevant"] = (
            checks["legal_match"]
            or checks["address_match"]
            or checks["folio_match"]
            or checks["similarity_score"] >= 0.80
        )

    checks["match_details"] = "; ".join(details) if details else "No matching criteria found"

    return checks


def collect_legal_variations(documents: list, property_info: Dict) -> list:
    """
    Collect all legal description variations from documents.

    Args:
        documents: List of document dicts with 'legal_description'
        property_info: Property info with canonical 'legal_description'

    Returns:
        List of unique legal description variations with source info
    """
    variations = []
    seen = set()

    # Add canonical from property
    canonical = property_info.get('legal_description', '').strip()
    if canonical:
        variations.append({
            "text": canonical,
            "source": "HCPA",
            "source_type": "bulk_data",
            "is_canonical": True
        })
        seen.add(canonical.upper())

    # Collect from documents
    for doc in documents:
        legal = doc.get('legal_description', '').strip()
        if legal and legal.upper() not in seen:
            variations.append({
                "text": legal,
                "source": doc.get('instrument', 'unknown'),
                "source_type": doc.get('doc_type', 'document'),
                "is_canonical": False
            })
            seen.add(legal.upper())

    return variations


if __name__ == "__main__":
    # Test the relevance checker
    prop = {
        "legal_description": "MUNRO AND MC INTOSH'S ADDITION LOT 9 BLOCK 12",
        "property_address": "205 W AMELIA AVE",
        "folio": "191887-0000"
    }

    doc1 = {
        "legal_description": "L 9 B 12 MUNRO & MCINTOSHS ADDN",
        "property_address": "205 WEST AMELIA AVENUE"
    }

    doc2 = {
        "legal_description": "L 10 B 12 MUNRO & MCINTOSHS ADDN",  # Wrong lot
        "property_address": "207 W AMELIA AVE"
    }

    result1 = verify_document_relevance(doc1, prop)
    print(f"Doc1 relevance: {result1}")

    result2 = verify_document_relevance(doc2, prop)
    print(f"Doc2 relevance: {result2}")
