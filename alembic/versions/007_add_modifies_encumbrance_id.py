"""Add modifies_encumbrance_id to ori_encumbrances.

Links lifecycle documents (MOD, SUB, NCL, CTF) to their parent
mortgage or lien encumbrance, enabling modification tracking.

Revision ID: 007_add_mod_link
Revises: 006_seed_bulk_jobs
Create Date: 2026-03-05
"""

import sqlalchemy as sa

from alembic import op

revision = "007_add_mod_link"
down_revision = "006_seed_bulk_jobs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ori_encumbrances",
        sa.Column(
            "modifies_encumbrance_id",
            sa.BigInteger(),
            sa.ForeignKey("ori_encumbrances.id"),
            nullable=True,
        ),
    )


def downgrade() -> None:
    raise NotImplementedError("Forward-only migration policy")
