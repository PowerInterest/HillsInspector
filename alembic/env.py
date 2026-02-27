"""Alembic environment configuration.

Reads the PostgreSQL DSN from the same source as the pipeline
(``sunbiz.db.resolve_pg_dsn``), which checks the ``SUNBIZ_PG_DSN``
environment variable and falls back to the dev-default DSN.

No SQLAlchemy ORM metadata is used — all migrations are raw SQL.
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from sqlalchemy import create_engine, pool

from alembic import context

# Alembic Config object — provides access to .ini values.
config = context.config

# Set up Python logging from the .ini file.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# No ORM metadata — we use raw SQL migrations.
target_metadata = None

# ---------------------------------------------------------------------------
# DSN resolution — mirrors sunbiz.db.resolve_pg_dsn()
# ---------------------------------------------------------------------------
_DEFAULT_PG_DSN = "postgresql+psycopg://hills:hills_dev@localhost:5433/hills_sunbiz"


def _get_url() -> str:
    """Return the SQLAlchemy database URL.

    Priority:
      1. ``SUNBIZ_PG_DSN`` environment variable (same as pipeline).
      2. Hard-coded dev default.
    """
    return os.getenv("SUNBIZ_PG_DSN", _DEFAULT_PG_DSN)


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (emit SQL without a live connection)."""
    context.configure(
        url=_get_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode (with a live DB connection)."""
    connectable = create_engine(_get_url(), poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
