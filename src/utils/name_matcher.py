"""
Name Matcher Utility.

Handles robust name comparison for Chain of Title construction.
Implements Token-Set logic, Superset/Subset detection, and Fuzzy matching.
"""

import re
from difflib import SequenceMatcher
from typing import Set, Tuple

class NameMatcher:

    # Common legal/noise words to strip
    STOPWORDS = {
        "THE",
        "AND",
        "OR",
        "OF",
        "&",
        "AN",
        "AS",
        "LLC",
        "L.L.C.",
        "INC",
        "INC.",
        "INCORPORATED",
        "CORP",
        "CORPORATION",
        "PA",
        "P.A.",
        "LTD",
        "LIMITED",
        "COMPANY",
        "CO",
        "TRUST",
        "TRUSTEE",
        "REVOCABLE",
        "LIVING",
        "FAMILY",
        "ESTATE",
        "SUCCESSOR",
        "HUSBAND",
        "WIFE",
        "SINGLE",
        "MAN",
        "WOMAN",
        "PERSON",
        "MARRIED",
        "FKA",
        "F/K/A",
        "NKA",
        "N/K/A",
        "AKA",
        "A/K/A",
        "DBA",
        "D/B/A",
    }

    # Common nickname/alias map
    ALIASES = {
        "BOB": "ROBERT",
        "ROB": "ROBERT",
        "BOBBY": "ROBERT",
        "BILL": "WILLIAM",
        "WILL": "WILLIAM",
        "WILLIE": "WILLIAM",
        "JIM": "JAMES",
        "JIMMY": "JAMES",
        "JOHN": "JONATHAN",
        "JON": "JONATHAN",
        "MIKE": "MICHAEL",
        "TOM": "THOMAS",
        "DAVE": "DAVID",
        "DAN": "DANIEL",
        "DANNY": "DANIEL",
        "CHRIS": "CHRISTOPHER",
        "JOE": "JOSEPH",
        "STEVE": "STEVEN",
        "STEPHEN": "STEVEN",
        "DICK": "RICHARD",
        "RICK": "RICHARD",
    }

    @classmethod
    def normalize(cls, name: str) -> Set[str]:
        """
        Normalize a name string into a set of significant tokens.
        """
        if not name:
            return set()

        # Uppercase and remove basic punctuation
        clean = name.upper()
        clean = re.sub(r"[^\w\s]", " ", clean)

        tokens = set(clean.split())

        # Remove stopwords and single-character tokens (initials).
        # For title chains, initials add noise and create false matches.
        significant_tokens = {t for t in tokens if t not in cls.STOPWORDS and len(t) > 1}

        return significant_tokens

    @classmethod
    def match(cls, name1: str, name2: str) -> Tuple[str, float]:
        """
        Compare two names and return (Match Type, Confidence Score).
        
        Match Types:
        - EXACT: Identical token sets
        - SUPERSET: Name2 contains Name1 (Add Party)
        - SUBSET: Name1 contains Name2 (Remove Party)
        - ALIAS: Matches via nickname table
        - FUZZY: High Jaccard similarity or Levenshtein match
        - NONE: No match
        """
        if not name1 or not name2:
            return "NONE", 0.0

        set1 = cls.normalize(name1)
        set2 = cls.normalize(name2)
        
        if not set1 or not set2:
            return "NONE", 0.0

        # 1. Exact Match
        if set1 == set2:
            return "EXACT", 1.0

        # 2. Superset / Subset (Add/Remove Party)
        # Tighten this: require at least 2 meaningful tokens in the smaller name.
        # This avoids false positives like "SMITH" being a subset of many names.
        intersection = set1.intersection(set2)
        min_tokens = min(len(set1), len(set2))

        if min_tokens >= 2:
            if set1.issubset(set2) and len(intersection) >= 2:
                return "SUPERSET", 0.95  # Name2 adds parties to Name1

            if set2.issubset(set1) and len(intersection) >= 2:
                return "SUBSET", 0.95  # Name2 removes parties from Name1

        # 3. Alias / Nickname Check
        # If sets are disjoint or low overlap, check if one token maps to another via Alias
        # e.g. {BOB, SMITH} vs {ROBERT, SMITH} -> Intersection {SMITH}
        # Map set1 aliases
        set1_mapped = {cls.ALIASES.get(t, t) for t in set1}
        set2_mapped = {cls.ALIASES.get(t, t) for t in set2}
        
        if set1_mapped == set2_mapped:
            return "ALIAS", 0.90

        # 4. Fuzzy / Jaccard Similarity
        union = set1.union(set2)
        jaccard = len(intersection) / len(union)

        if jaccard >= 0.65:  # Slightly tighter than before
            return "FUZZY_JACCARD", round(jaccard, 2)

        # 5. String Similarity (Levenshtein via SequenceMatcher)
        # Good for typos: "Steven" vs "Stephen"
        ratio = SequenceMatcher(None, name1.upper(), name2.upper()).ratio()
        if ratio > 0.88:
            return "FUZZY_STRING", round(ratio, 2)

        return "NONE", 0.0

    @classmethod
    def are_linked(cls, name1: str, name2: str, threshold: float = 0.8) -> bool:
        """
        Simple boolean check if two names are considered linked in a chain.
        """
        match_type, score = cls.match(name1, name2)

        valid_types = {"EXACT", "SUPERSET", "SUBSET", "ALIAS", "FUZZY_JACCARD", "FUZZY_STRING"}

        if match_type in valid_types and score >= threshold:
            return True

        return False

if __name__ == "__main__":
    # Test cases
    cases = [
        ("John Smith", "John Smith"),
        ("John Smith", "John A. Smith"),
        ("John Smith", "John Smith and Jane Doe"), # Add party
        ("John Smith and Jane Doe", "John Smith"), # Remove party
        ("Robert Johnson", "Bob Johnson"), # Alias
        ("Steven Jobs", "Stephen Jobs"), # Typo
        ("Fannie Mae", "Federal National Mortgage Association"), # Hard (won't match without specific alias logic)
        ("Bank of America", "Bank of America, N.A."),
        ("John Smith", "Jane Doe")
    ]

    print(f"{'Name 1':<30} | {'Name 2':<30} | {'Type':<15} | {'Score'}")
    print("-" * 85)
    for n1, n2 in cases:
        m_type, score = NameMatcher.match(n1, n2)
        print(f"{n1:<30} | {n2:<30} | {m_type:<15} | {score}")
