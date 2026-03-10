"""Rename mortgage_data to extracted_data on ori_encumbrances.

This column will now store extraction results for ALL document types
(mortgages, deeds, liens, lis pendens, satisfactions, assignments, NOCs),
not just mortgages.

Revision ID: 013_rename_to_extracted_data
Revises: 012_add_identifier_recovery_step
Create Date: 2026-03-10
"""

from alembic import op

revision = "013_rename_to_extracted_data"
down_revision = "012_add_identifier_recovery_step"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "ori_encumbrances",
        "mortgage_data",
        new_column_name="extracted_data",
    )


def downgrade() -> None:
    raise NotImplementedError("Forward-only migration policy")
