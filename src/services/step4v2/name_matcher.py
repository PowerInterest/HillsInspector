"""
Name Matcher - Name normalization and fuzzy matching for linked identity detection.

This module provides:
- Name normalization (removing titles, suffixes, sorting parts)
- Fuzzy matching with configurable thresholds
- Generic name detection
- Trust/name change detection
- Linked identity creation and lookup
"""

import re
from dataclasses import dataclass
from typing import Optional

import duckdb
from loguru import logger

from config.step4v2 import (
    GENERIC_NAMES_FILE,
    NAME_CHANGE_CONFIDENCE,
    NAME_FUZZY_THRESHOLD,
    TRUST_TRANSFER_CONFIDENCE,
)


@dataclass
class MatchResult:
    """Result of a name matching operation."""

    is_match: bool
    link_type: Optional[str] = None  # 'exact', 'trust_transfer', 'spelling_variation', 'name_change'
    confidence: float = 0.0
    canonical_name: Optional[str] = None


class NameMatcher:
    """
    Name matching and normalization service.

    Handles:
    - Normalizing names for comparison
    - Detecting if two names refer to the same person/entity
    - Creating and managing linked identities in the database
    """

    # Suffixes to remove from names
    SUFFIXES = [
        " JR",
        " SR",
        " II",
        " III",
        " IV",
        " V",
        " ESQ",
        " MD",
        " PHD",
        " DDS",
        " DO",
        " PA",
        " LLC",
        " INC",
        " CORP",
        " LTD",
        " LP",
        " LLP",
    ]

    # Titles to remove from names
    TITLES = ["MR ", "MRS ", "MS ", "DR ", "MISS ", "REV ", "HON "]

    # Trust indicators
    TRUST_INDICATORS = [
        "TRUSTEE",
        "TRUST",
        "REVOCABLE",
        "IRREVOCABLE",
        "LIVING TRUST",
        "FAMILY TRUST",
        "TR",
        "AS TRUSTEE",
        "SUCCESSOR TRUSTEE",
    ]

    def __init__(self, conn: Optional[duckdb.DuckDBPyConnection] = None):
        """Initialize the name matcher."""
        self.conn = conn
        self._generic_names: set[str] = set()
        self._load_generic_names()

    def _load_generic_names(self) -> None:
        """Load generic names from config file."""
        if not GENERIC_NAMES_FILE.exists():
            logger.warning(f"Generic names file not found: {GENERIC_NAMES_FILE}")
            return

        with open(GENERIC_NAMES_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    self._generic_names.add(line.upper())

        logger.info(f"Loaded {len(self._generic_names)} generic names")

    def normalize(self, name: str) -> str:
        """
        Normalize a name for comparison.

        Steps:
        1. Uppercase
        2. Remove suffixes (JR, SR, II, III, IV, ESQ, MD, PHD, LLC, etc.)
        3. Remove titles (MR, MRS, MS, DR, MISS)
        4. Remove punctuation
        5. Collapse whitespace
        6. Sort parts alphabetically (handles LAST FIRST vs FIRST LAST)
        """
        if not name:
            return ""

        name = name.upper().strip()

        # Remove suffixes
        for suffix in self.SUFFIXES:
            name = name.removesuffix(suffix)

        # Remove titles
        for title in self.TITLES:
            name = name.removeprefix(title)

        # Remove punctuation except apostrophes in names
        name = re.sub(r"[.,;:\(\)\[\]\{\}\"\\\/]", "", name)
        name = re.sub(r"['\`]", "", name)  # Remove apostrophes too for matching

        # Collapse whitespace
        name = re.sub(r"\s+", " ", name).strip()

        # Sort parts alphabetically for order-independent matching
        parts = sorted(name.split())
        return " ".join(parts)

    def is_generic(self, name: str) -> bool:
        """Check if a party name is too generic for searching."""
        if not name:
            return True

        name_upper = name.upper()

        # Check exact match
        if name_upper in self._generic_names:
            return True

        # Check partial match
        return any(generic in name_upper for generic in self._generic_names)

    def extract_base_from_trust(self, trust_name: str) -> str:
        """
        Extract the base person name from a trust name.

        Examples:
        - "JOHN SMITH TRUSTEE" -> "JOHN SMITH"
        - "SMITH FAMILY TRUST" -> "SMITH"
        - "JOHN AND MARY SMITH REVOCABLE TRUST" -> "JOHN AND MARY SMITH"
        """
        if not trust_name:
            return ""

        name = trust_name.upper()

        # Remove trust indicators
        for indicator in self.TRUST_INDICATORS:
            # Remove "AS TRUSTEE", "TRUSTEE", etc.
            name = re.sub(rf"\b{re.escape(indicator)}\b", "", name)

        # Remove "OF THE", "OF", etc.
        name = re.sub(r"\bOF THE\b", "", name)
        name = re.sub(r"\bOF\b", "", name)
        name = re.sub(r"\bTHE\b", "", name)
        name = re.sub(r"\bDATED\s+\d+[/-]\d+[/-]\d+\b", "", name)  # Remove dates

        # Clean up
        return re.sub(r"\s+", " ", name).strip()

    def fuzzy_match(self, name1: str, name2: str) -> float:
        """
        Calculate fuzzy match score between two names.

        Uses a combination of:
        - Jaccard similarity on word sets
        - Character-level edit distance ratio
        """
        if not name1 or not name2:
            return 0.0

        # Normalize both names
        n1 = self.normalize(name1)
        n2 = self.normalize(name2)

        if n1 == n2:
            return 1.0

        # Word-level Jaccard similarity
        words1 = set(n1.split())
        words2 = set(n2.split())

        if not words1 or not words2:
            return 0.0

        intersection = len(words1 & words2)
        union = len(words1 | words2)
        jaccard = intersection / union if union > 0 else 0.0

        # Bonus for same number of words
        length_bonus = 0.1 if len(words1) == len(words2) else 0.0

        return min(jaccard + length_bonus, 1.0)

    def same_first_name(self, name1: str, name2: str) -> bool:
        """Check if two names share the same first name (for marriage detection)."""
        if not name1 or not name2:
            return False

        parts1 = name1.upper().split()
        parts2 = name2.upper().split()

        if len(parts1) < 2 or len(parts2) < 2:
            return False

        # Try first word
        if parts1[0] == parts2[0] and len(parts1[0]) > 2:
            return True

        # Try last word (in case of LAST, FIRST format)
        return parts1[-1] == parts2[-1] and len(parts1[-1]) > 2

    def is_trust_indicator(self, name: str) -> bool:
        """Check if a name contains trust indicators."""
        if not name:
            return False
        name_upper = name.upper()
        return any(indicator in name_upper for indicator in self.TRUST_INDICATORS)

    def match(self, name1: str, name2: str) -> MatchResult:
        """
        Determine if two names refer to the same person/entity.

        Returns a MatchResult with:
        - is_match: True if names likely match
        - link_type: Type of link detected
        - confidence: Confidence score (0-1)
        - canonical_name: Preferred name to use
        """
        if not name1 or not name2:
            return MatchResult(is_match=False)

        # Normalize both
        n1_norm = self.normalize(name1)
        n2_norm = self.normalize(name2)

        # Rule 1: Exact match after normalization
        if n1_norm == n2_norm:
            return MatchResult(
                is_match=True,
                link_type="exact",
                confidence=1.0,
                canonical_name=name1,  # Use original form
            )

        # Rule 2: Trust transfer detection
        if self.is_trust_indicator(name2) and not self.is_trust_indicator(name1):
            # name1 -> name1's trust
            base_name = self.extract_base_from_trust(name2)
            base_norm = self.normalize(base_name)

            if n1_norm == base_norm or self.fuzzy_match(n1_norm, base_norm) >= NAME_FUZZY_THRESHOLD:
                return MatchResult(
                    is_match=True,
                    link_type="trust_transfer",
                    confidence=TRUST_TRANSFER_CONFIDENCE,
                    canonical_name=name1,
                )

        elif self.is_trust_indicator(name1) and not self.is_trust_indicator(name2):
            # trust -> person (reverse)
            base_name = self.extract_base_from_trust(name1)
            base_norm = self.normalize(base_name)

            if n2_norm == base_norm or self.fuzzy_match(n2_norm, base_norm) >= NAME_FUZZY_THRESHOLD:
                return MatchResult(
                    is_match=True,
                    link_type="trust_transfer",
                    confidence=TRUST_TRANSFER_CONFIDENCE,
                    canonical_name=name2,
                )

        # Rule 3: Spelling variation (high fuzzy match)
        fuzzy_score = self.fuzzy_match(name1, name2)
        if fuzzy_score >= NAME_FUZZY_THRESHOLD:
            return MatchResult(
                is_match=True,
                link_type="spelling_variation",
                confidence=fuzzy_score,
                canonical_name=name1,  # Use first name as canonical
            )

        # Rule 4: Marriage name change (same first name, different last)
        # This is low confidence and only triggered as last resort
        if self.same_first_name(name1, name2):
            return MatchResult(
                is_match=True,
                link_type="name_change",
                confidence=NAME_CHANGE_CONFIDENCE,
                canonical_name=name1,
            )

        return MatchResult(is_match=False)

    # Database operations for linked identities

    def get_or_create_linked_identity(
        self,
        name1: str,
        name2: str,
        link_type: str,
        confidence: float,
    ) -> int:
        """
        Get or create a linked identity for two names.

        Returns the linked_identity_id.
        """
        if not self.conn:
            raise ValueError("Database connection required for linked identity operations")

        canonical_name = name1  # Use first name as canonical

        # Determine entity type
        entity_type = "individual"
        if self.is_trust_indicator(name1) or self.is_trust_indicator(name2):
            entity_type = "trust"
        elif any(x in name1.upper() for x in ["LLC", "INC", "CORP", "LTD"]):
            entity_type = "llc"
        elif "BANK" in name1.upper():
            entity_type = "bank"

        # Check if this linked identity already exists
        existing = self.conn.execute(
            """
            SELECT id FROM linked_identities
            WHERE canonical_name = ? AND link_type = ?
            LIMIT 1
            """,
            [canonical_name, link_type],
        ).fetchone()

        if existing:
            return existing[0]

        # Create new linked identity using RETURNING to get the ID (DuckDB compatible)
        result = self.conn.execute(
            """
            INSERT INTO linked_identities (canonical_name, entity_type, link_type, confidence)
            VALUES (?, ?, ?, ?)
            RETURNING id
            """,
            [canonical_name, entity_type, link_type, confidence],
        ).fetchone()

        if not result:
            raise RuntimeError("Failed to get identity ID after insert")
        identity_id = result[0]

        logger.debug(f"Created linked identity {identity_id}: {canonical_name} ({link_type})")
        return identity_id

    def link_party_to_identity(self, folio: str, party_name: str, identity_id: int) -> None:
        """Link a party record to a linked identity."""
        if not self.conn:
            raise ValueError("Database connection required")

        self.conn.execute(
            """
            UPDATE property_parties
            SET linked_identity_id = ?
            WHERE folio = ? AND party_name = ?
            """,
            [identity_id, folio, party_name],
        )

    def detect_and_link(self, folio: str, party1: str, party2: str) -> Optional[int]:
        """
        Detect if two parties are the same person and create a linked identity.

        Returns the linked_identity_id if a link was created, None otherwise.
        """
        match_result = self.match(party1, party2)

        if not match_result.is_match:
            return None

        # Type guard: if is_match is True, link_type should be set
        link_type = match_result.link_type or "unknown"

        # Create linked identity
        identity_id = self.get_or_create_linked_identity(
            party1,
            party2,
            link_type,
            match_result.confidence,
        )

        # Link both parties
        self.link_party_to_identity(folio, party1, identity_id)
        self.link_party_to_identity(folio, party2, identity_id)

        logger.info(
            f"Linked identities for {folio}: '{party1}' <-> '{party2}' "
            f"(type={match_result.link_type}, confidence={match_result.confidence:.2f})"
        )

        return identity_id

    def get_linked_names(self, identity_id: int) -> list[str]:
        """Get all party names linked to an identity."""
        if not self.conn:
            return []

        result = self.conn.execute(
            """
            SELECT DISTINCT party_name
            FROM property_parties
            WHERE linked_identity_id = ?
            """,
            [identity_id],
        ).fetchall()

        return [row[0] for row in result]

    def find_identity_by_name(self, name: str) -> Optional[int]:
        """Find a linked identity by any of its associated names."""
        if not self.conn:
            return None

        # First check property_parties
        result = self.conn.execute(
            """
            SELECT linked_identity_id
            FROM property_parties
            WHERE party_name = ? AND linked_identity_id IS NOT NULL
            LIMIT 1
            """,
            [name],
        ).fetchone()

        if result:
            return result[0]

        # Check normalized match
        norm_name = self.normalize(name)
        result = self.conn.execute(
            """
            SELECT linked_identity_id
            FROM property_parties
            WHERE party_name_normalized = ? AND linked_identity_id IS NOT NULL
            LIMIT 1
            """,
            [norm_name],
        ).fetchone()

        return result[0] if result else None
