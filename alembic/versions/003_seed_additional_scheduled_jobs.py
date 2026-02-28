"""Backfill scheduled job config rows introduced after initial job-control rollout.

This migration preserves any operator-customized values by inserting only missing
`pipeline_job_config` rows. It exists because the initial job-control migration
was already applied in local development before the full scheduled job set was
seeded.

Revision ID: 003_seed_scheduled_jobs
Revises: 002_job_control
Create Date: 2026-02-28
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "003_seed_scheduled_jobs"
down_revision = "002_job_control"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(
        sa.text(
            """
            INSERT INTO pipeline_job_config (
                job_name, enabled, min_interval_sec, max_runtime_sec, singleton, args_json
            ) VALUES
                ('clerk_bulk', TRUE, 86400, 7200, TRUE, '{}'::jsonb),
                ('sunbiz_daily', TRUE, 86400, 3600, TRUE, '{}'::jsonb),
                ('sunbiz_flr_quarterly', TRUE, 7776000, 14400, TRUE, '{}'::jsonb),
                ('sunbiz_entity_quarterly', TRUE, 7776000, 14400, TRUE, '{}'::jsonb),
                ('dor_nal_annual', TRUE, 2419200, 7200, TRUE, '{}'::jsonb),
                ('hcpa_bulk', TRUE, 604800, 3600, TRUE, '{}'::jsonb)
            ON CONFLICT (job_name) DO NOTHING
            """
        )
    )


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(
        sa.text(
            """
            DELETE FROM pipeline_job_config
            WHERE job_name IN (
                'clerk_bulk',
                'sunbiz_daily',
                'sunbiz_flr_quarterly',
                'sunbiz_entity_quarterly',
                'dor_nal_annual',
                'hcpa_bulk'
            )
            """
        )
    )
