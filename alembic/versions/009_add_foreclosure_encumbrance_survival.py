"""Add per-foreclosure survival results for shared-strap correctness.

Revision ID: 009_add_fc_enc_survival
Revises: 008_add_municipal_lien_findings
Create Date: 2026-03-09
"""

import sqlalchemy as sa

from alembic import op

revision = "009_add_fc_enc_survival"
down_revision = "008_add_municipal_lien_findings"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "foreclosure_encumbrance_survival",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column(
            "foreclosure_id",
            sa.BigInteger(),
            sa.ForeignKey("foreclosures.foreclosure_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "encumbrance_id",
            sa.BigInteger(),
            sa.ForeignKey("ori_encumbrances.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("survival_status", sa.Text(), nullable=False),
        sa.Column("survival_reason", sa.Text(), nullable=True),
        sa.Column("survival_case_number", sa.Text(), nullable=True),
        sa.Column(
            "analyzed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
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
            "encumbrance_id",
            name="uq_foreclosure_encumbrance_survival_foreclosure_encumbrance",
        ),
    )
    op.create_index(
        "idx_foreclosure_enc_survival_foreclosure",
        "foreclosure_encumbrance_survival",
        ["foreclosure_id"],
    )
    op.create_index(
        "idx_foreclosure_enc_survival_encumbrance",
        "foreclosure_encumbrance_survival",
        ["encumbrance_id"],
    )


def downgrade() -> None:
    raise NotImplementedError("Forward-only migration policy")
