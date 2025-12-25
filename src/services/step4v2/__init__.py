"""
Step 4v2: ORI Ingestion & Chain of Title.

This package implements iterative discovery for building complete chains of title.

Modules:
- name_matcher: Name normalization and fuzzy matching
- search_queue: Search queue management
- discovery: Main iterative discovery loop
- chain_builder: Chain of title construction
"""

from src.services.step4v2.name_matcher import NameMatcher
from src.services.step4v2.search_queue import SearchQueue
from src.services.step4v2.discovery import IterativeDiscovery, RateLimitError
from src.services.step4v2.chain_builder import ChainBuilder

__all__ = [
    "ChainBuilder",
    "IterativeDiscovery",
    "NameMatcher",
    "RateLimitError",
    "SearchQueue",
]
