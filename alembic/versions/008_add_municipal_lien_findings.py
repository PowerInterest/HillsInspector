"""Add municipal_lien_findings table for Phase 0 municipal lien detection.

Revision ID: 008_add_municipal_lien_findings
Revises: 007_add_mod_link
Create Date: 2026-03-06
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "008_add_municipal_lien_findings"
down_revision = "007_add_mod_link"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "municipal_lien_findings",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column(
            "foreclosure_id",
            sa.BigInteger(),
            sa.ForeignKey("foreclosures.foreclosure_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("instrument_number", sa.Text(), nullable=True),
        sa.Column("amount", sa.Numeric(14, 2), nullable=True),
        sa.Column("as_of_date", sa.Date(), nullable=True),
        sa.Column("confidence", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("raw_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "foreclosure_id",
            "provider",
            "source",
            name="uq_municipal_lien_findings_foreclosure_provider_source",
        ),
    )
    op.create_index(
        "idx_municipal_lien_findings_provider_status",
        "municipal_lien_findings",
        ["provider", "status"],
    )
    op.create_index(
        "idx_municipal_lien_findings_foreclosure_provider",
        "municipal_lien_findings",
        ["foreclosure_id", "provider"],
    )


def downgrade() -> None:
    raise NotImplementedError("Forward-only migration policy")

