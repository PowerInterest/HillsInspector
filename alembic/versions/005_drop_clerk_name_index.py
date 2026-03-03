"""Drop deprecated clerk_name_index table.

The clerk_name_index table stored denormalised civil alpha index data in its own
table with UCN-format case numbers that never matched clerk_civil_cases.
Migration 004 merged this data into the normalised clerk_civil_cases +
clerk_civil_parties tables. This migration drops the now-unused table.

Revision ID: 005_drop_clerk_name_index
Revises: 004_merge_civil_alpha
"""

import sqlalchemy as sa
from alembic import op

revision = "005_drop_clerk_name_index"
down_revision = "004_merge_civil_alpha"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_table("clerk_name_index")


def downgrade() -> None:
    # Recreate the table if rolling back.
    op.create_table(
        "clerk_name_index",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("court_type", sa.Text, nullable=False),
        sa.Column("business_name", sa.Text, nullable=True),
        sa.Column("last_name", sa.Text, nullable=True),
        sa.Column("first_name", sa.Text, nullable=True),
        sa.Column("middle_name", sa.Text, nullable=True),
        sa.Column("suffix", sa.Text, nullable=True),
        sa.Column("party_type", sa.Text, nullable=True),
        sa.Column("ucn", sa.String(64), nullable=False),
        sa.Column("case_number", sa.String(32), nullable=True),
        sa.Column("case_type", sa.Text, nullable=True),
        sa.Column("division", sa.Text, nullable=True),
        sa.Column("judge_name", sa.Text, nullable=True),
        sa.Column("date_filed", sa.Date, nullable=True),
        sa.Column("current_status", sa.Text, nullable=True),
        sa.Column("status_date", sa.Date, nullable=True),
        sa.Column("address1", sa.Text, nullable=True),
        sa.Column("address2", sa.Text, nullable=True),
        sa.Column("city", sa.Text, nullable=True),
        sa.Column("state", sa.Text, nullable=True),
        sa.Column("zip_code", sa.Text, nullable=True),
        sa.Column("disposition_code", sa.Text, nullable=True),
        sa.Column("disposition_desc", sa.Text, nullable=True),
        sa.Column("disposition_date", sa.Date, nullable=True),
        sa.Column("amount_paid", sa.Text, nullable=True),
        sa.Column("date_paid", sa.Date, nullable=True),
        sa.Column("akas", sa.Text, nullable=True),
        sa.Column("is_foreclosure", sa.Boolean, nullable=True),
        sa.Column("source_file", sa.Text, nullable=True),
        sa.Column(
            "loaded_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("ucn", "disposition_code", name="uq_clerk_name_index_ucn_disp"),
    )
    # Recreate indexes
    op.create_index("idx_clerk_ni_case_number", "clerk_name_index", ["case_number"])
    op.create_index("idx_clerk_ni_case_type", "clerk_name_index", ["case_type"])
    op.create_index("idx_clerk_ni_date_filed", "clerk_name_index", ["date_filed"])
    op.create_index("idx_clerk_ni_party_type", "clerk_name_index", ["party_type"])
    op.create_index("idx_clerk_ni_court_type", "clerk_name_index", ["court_type"])
    op.create_index("idx_clerk_ni_status", "clerk_name_index", ["current_status"])
    op.create_index("idx_clerk_ni_disposition_code", "clerk_name_index", ["disposition_code"])
    op.execute(sa.text("""
        CREATE INDEX idx_clerk_ni_last_name_trgm ON clerk_name_index
        USING gin (last_name gin_trgm_ops)
    """))
    op.execute(sa.text("""
        CREATE INDEX idx_clerk_ni_business_name_trgm ON clerk_name_index
        USING gin (business_name gin_trgm_ops)
    """))
