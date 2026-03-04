"""Audit services package.

This package contains the encumbrance audit/reporting stack plus the
audit-driven recovery orchestrator. The audit modules themselves remain
read-only; the recovery layer simply routes selected audit gaps back through the
existing pipeline writers so source-backed facts can be retried without adding
audit-specific storage.
"""

from src.services.audit.encumbrance_recovery import EncumbranceRecoveryService
from src.services.audit.pg_audit_encumbrance import (
    AuditReport,
    BucketHit,
    BucketSummary,
    format_console,
    format_csv,
    format_json,
    run_audit,
)

__all__ = [
    "AuditReport",
    "BucketHit",
    "BucketSummary",
    "EncumbranceRecoveryService",
    "format_console",
    "format_csv",
    "format_json",
    "run_audit",
]
