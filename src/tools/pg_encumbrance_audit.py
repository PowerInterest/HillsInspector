"""Compatibility wrapper for the encumbrance audit CLI.

The implementation now lives under ``src.services.audit.pg_audit_encumbrance``.
This module remains as the stable CLI/module entrypoint so existing scripts and
tests importing ``src.tools.pg_encumbrance_audit`` continue to work unchanged.
"""

from src.services.audit.pg_audit_encumbrance import (
    AuditReport,
    BUCKET_DEFINITIONS,
    BucketHit,
    BucketSummary,
    format_console,
    format_csv,
    format_json,
    main,
    run_audit,
)

__all__ = [
    "BUCKET_DEFINITIONS",
    "AuditReport",
    "BucketHit",
    "BucketSummary",
    "format_console",
    "format_csv",
    "format_json",
    "main",
    "run_audit",
]


if __name__ == "__main__":
    main()
