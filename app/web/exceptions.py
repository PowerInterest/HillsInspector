"""Shared web application exceptions (no SQLite dependency)."""


class DatabaseLockedError(Exception):
    """Raised when the database is temporarily locked."""


class DatabaseUnavailableError(Exception):
    """Raised when the database is not accessible."""
