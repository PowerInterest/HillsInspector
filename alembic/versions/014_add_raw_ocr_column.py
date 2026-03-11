"""Add raw OCR text column to ori_encumbrances.

Stores the full pytesseract output (with --- PAGE N --- delimiters)
that was sent to the LLM for structured extraction. Persisted before
the LLM call so the text survives even when extraction fails.

Revision ID: 014_add_raw_ocr_column
Revises: 013_rename_to_extracted_data
Create Date: 2026-03-10
"""

from alembic import op
import sqlalchemy as sa

revision = "014_add_raw_ocr_column"
down_revision = "013_rename_to_extracted_data"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ori_encumbrances",
        sa.Column("raw", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    raise NotImplementedError("Forward-only migration policy")
