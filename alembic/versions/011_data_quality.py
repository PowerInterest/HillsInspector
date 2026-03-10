"""Add overwrite audit log and market spec source tracking.

Revision ID: 011_data_quality
Revises: 010_update_survival_pg_functions
Create Date: 2026-03-09
"""

import sqlalchemy as sa

from alembic import op

revision = "011_data_quality"
down_revision = "010_update_survival_pg_functions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    op.create_table(
        "data_change_log",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("table_name", sa.Text(), nullable=False),
        sa.Column("row_key", sa.Text(), nullable=False),
        sa.Column("column_name", sa.Text(), nullable=False),
        sa.Column("old_value", sa.Text(), nullable=True),
        sa.Column("new_value", sa.Text(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column(
            "changed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index(
        "idx_dcl_table_key",
        "data_change_log",
        ["table_name", "row_key"],
    )
    op.create_index(
        "idx_dcl_changed_at",
        "data_change_log",
        ["changed_at"],
    )

    if "property_market" not in inspector.get_table_names():
        return

    columns = {
        column["name"]
        for column in inspector.get_columns("property_market")
    }

    if "specs_source" not in columns:
        op.add_column(
            "property_market",
            sa.Column("specs_source", sa.String(length=16), nullable=True),
        )
    if "specs_updated_at" not in columns:
        op.add_column(
            "property_market",
            sa.Column("specs_updated_at", sa.DateTime(timezone=True), nullable=True),
        )

    realtor_branch = ""
    if "realtor_json" in columns:
        realtor_branch = """
                    WHEN realtor_json IS NOT NULL
                         AND realtor_json::text != 'null'
                    THEN 'realtor'
        """

    op.execute(
        f"""
        UPDATE property_market
        SET specs_source = CASE
                WHEN (
                    beds IS NOT NULL
                    OR baths IS NOT NULL
                    OR sqft IS NOT NULL
                    OR year_built IS NOT NULL
                    OR lot_size IS NOT NULL
                    OR property_type IS NOT NULL
                ) THEN CASE
                    WHEN homeharvest_json IS NOT NULL
                         AND homeharvest_json::text != 'null'
                    THEN 'homeharvest'
                    WHEN redfin_json IS NOT NULL
                         AND redfin_json::text != 'null'
                    THEN 'redfin'
                    WHEN zillow_json IS NOT NULL
                         AND zillow_json::text != 'null'
                    THEN 'zillow'
                    {realtor_branch}
                    ELSE NULL
                END
                ELSE NULL
            END,
            specs_updated_at = CASE
                WHEN (
                    beds IS NOT NULL
                    OR baths IS NOT NULL
                    OR sqft IS NOT NULL
                    OR year_built IS NOT NULL
                    OR lot_size IS NOT NULL
                    OR property_type IS NOT NULL
                ) THEN updated_at
                ELSE NULL
            END
        WHERE specs_source IS NULL
           OR specs_updated_at IS NULL
        """
    )


def downgrade() -> None:
    raise NotImplementedError("Forward-only migration policy")
