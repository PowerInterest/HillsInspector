"""Add identifier recovery attempt tracking to foreclosures.

Revision ID: 012_add_identifier_recovery_step
Revises: 011_data_quality
Create Date: 2026-03-10
"""

import sqlalchemy as sa

from alembic import op

revision = "012_add_identifier_recovery_step"
down_revision = "011_data_quality"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {
        column["name"]
        for column in inspector.get_columns("foreclosures")
    }
    if "step_identifier_recovery" not in columns:
        op.add_column(
            "foreclosures",
            sa.Column("step_identifier_recovery", sa.DateTime(timezone=True), nullable=True),
        )


def downgrade() -> None:
    raise NotImplementedError("Forward-only migration policy")
