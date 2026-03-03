"""Merge civil alphabetical index into clerk_civil_cases + clerk_civil_parties.

Adds columns to existing tables so the alpha index data (1958-present, 1.86M+
party rows) can be merged into the normalised schema instead of living in the
separate clerk_name_index table.

Also seeds the ``clerk_civil_alpha`` scheduled job config row.

Revision ID: 004_merge_civil_alpha
Revises: 003_seed_scheduled_jobs
Create Date: 2026-03-02
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "004_merge_civil_alpha"
down_revision = "003_seed_scheduled_jobs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # -- clerk_civil_cases: new columns from alpha index ----------------------
    conn.execute(
        sa.text("""
            ALTER TABLE clerk_civil_cases
                ADD COLUMN IF NOT EXISTS court_type TEXT,
                ADD COLUMN IF NOT EXISTS status_date DATE
        """)
    )

    # -- clerk_civil_parties: new columns from alpha index --------------------
    conn.execute(
        sa.text("""
            ALTER TABLE clerk_civil_parties
                ADD COLUMN IF NOT EXISTS suffix TEXT,
                ADD COLUMN IF NOT EXISTS business_name TEXT,
                ADD COLUMN IF NOT EXISTS disposition_code TEXT,
                ADD COLUMN IF NOT EXISTS disposition_desc TEXT,
                ADD COLUMN IF NOT EXISTS disposition_date DATE,
                ADD COLUMN IF NOT EXISTS amount_paid TEXT,
                ADD COLUMN IF NOT EXISTS date_paid DATE,
                ADD COLUMN IF NOT EXISTS akas TEXT
        """)
    )

    # -- Trigram indexes on clerk_civil_parties for web fuzzy search -----------
    conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))

    conn.execute(
        sa.text("""
            CREATE INDEX IF NOT EXISTS idx_clerk_parties_last_name_trgm
                ON clerk_civil_parties USING gin (last_name gin_trgm_ops)
        """)
    )
    conn.execute(
        sa.text("""
            CREATE INDEX IF NOT EXISTS idx_clerk_parties_first_name_trgm
                ON clerk_civil_parties USING gin (first_name gin_trgm_ops)
        """)
    )
    conn.execute(
        sa.text("""
            CREATE INDEX IF NOT EXISTS idx_clerk_parties_business_name_trgm
                ON clerk_civil_parties USING gin (business_name gin_trgm_ops)
        """)
    )
    conn.execute(
        sa.text("""
            CREATE INDEX IF NOT EXISTS idx_clerk_parties_disposition_code
                ON clerk_civil_parties (disposition_code)
        """)
    )

    # -- Seed scheduled job config for civil alpha index ----------------------
    conn.execute(
        sa.text("""
            INSERT INTO pipeline_job_config (
                job_name, enabled, min_interval_sec, max_runtime_sec, singleton, args_json
            ) VALUES
                ('clerk_civil_alpha', TRUE, 604800, 7200, TRUE, '{}'::jsonb)
            ON CONFLICT (job_name) DO NOTHING
        """)
    )


def downgrade() -> None:
    conn = op.get_bind()

    # Remove scheduled job config
    conn.execute(
        sa.text("""
            DELETE FROM pipeline_job_config WHERE job_name = 'clerk_civil_alpha'
        """)
    )

    # Drop new indexes
    conn.execute(sa.text("DROP INDEX IF EXISTS idx_clerk_parties_last_name_trgm"))
    conn.execute(sa.text("DROP INDEX IF EXISTS idx_clerk_parties_first_name_trgm"))
    conn.execute(sa.text("DROP INDEX IF EXISTS idx_clerk_parties_business_name_trgm"))
    conn.execute(sa.text("DROP INDEX IF EXISTS idx_clerk_parties_disposition_code"))

    # Drop new columns from clerk_civil_parties
    conn.execute(
        sa.text("""
            ALTER TABLE clerk_civil_parties
                DROP COLUMN IF EXISTS suffix,
                DROP COLUMN IF EXISTS business_name,
                DROP COLUMN IF EXISTS disposition_code,
                DROP COLUMN IF EXISTS disposition_desc,
                DROP COLUMN IF EXISTS disposition_date,
                DROP COLUMN IF EXISTS amount_paid,
                DROP COLUMN IF EXISTS date_paid,
                DROP COLUMN IF EXISTS akas
        """)
    )

    # Drop new columns from clerk_civil_cases
    conn.execute(
        sa.text("""
            ALTER TABLE clerk_civil_cases
                DROP COLUMN IF EXISTS court_type,
                DROP COLUMN IF EXISTS status_date
        """)
    )
