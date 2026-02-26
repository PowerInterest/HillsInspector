"""add realtor to property market

Revision ID: add_realtor_cols
Revises:
Create Date: 2026-02-24 12:10:00.000000

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "add_realtor_cols"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("property_market", sa.Column("realtor_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True))


def downgrade() -> None:
    op.drop_column("property_market", "realtor_json")
