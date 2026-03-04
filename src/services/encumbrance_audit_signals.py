"""Compatibility import for the encumbrance audit signal extractor.

The implementation now lives in ``src.services.audit.encumbrance_audit_signals``.
This module preserves the historical import path used by tests and any local
tools while keeping the actual logic under ``src/services/audit``.
"""

from src.services.audit.encumbrance_audit_signals import *  # noqa: F403
