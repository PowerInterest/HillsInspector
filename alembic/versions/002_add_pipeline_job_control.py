"""Add pipeline job control tables for cron-driven Python workers.

Creates:
- `pipeline_job_config`  (runtime controls per job)
- `pipeline_job_runs`    (immutable execution history)

Seeds config rows for all scheduled jobs so cron can immediately use
PG-backed controls.

Revision ID: 002_job_control
Revises: 001_viewify
Create Date: 2026-02-27
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "002_job_control"
down_revision = "001_viewify"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    conn.execute(
        sa.text(
            """
            CREATE TABLE IF NOT EXISTS pipeline_job_config (
                job_name          TEXT PRIMARY KEY,
                enabled           BOOLEAN NOT NULL DEFAULT TRUE,
                min_interval_sec  INTEGER NOT NULL DEFAULT 3600 CHECK (min_interval_sec >= 0),
                max_runtime_sec   INTEGER NOT NULL DEFAULT 3600 CHECK (max_runtime_sec > 0),
                singleton         BOOLEAN NOT NULL DEFAULT TRUE,
                args_json         JSONB NOT NULL DEFAULT '{}'::jsonb,
                paused_reason     TEXT,
                created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
    )

    conn.execute(
        sa.text(
            """
            CREATE OR REPLACE FUNCTION pipeline_job_config_touch_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at := now();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql
            """
        )
    )
    conn.execute(sa.text("DROP TRIGGER IF EXISTS trg_pipeline_job_config_updated_at ON pipeline_job_config"))
    conn.execute(
        sa.text(
            """
            CREATE TRIGGER trg_pipeline_job_config_updated_at
            BEFORE UPDATE ON pipeline_job_config
            FOR EACH ROW
            EXECUTE FUNCTION pipeline_job_config_touch_updated_at()
            """
        )
    )

    conn.execute(
        sa.text(
            """
            CREATE TABLE IF NOT EXISTS pipeline_job_runs (
                run_id          BIGSERIAL PRIMARY KEY,
                job_name        TEXT NOT NULL REFERENCES pipeline_job_config(job_name),
                triggered_by    TEXT NOT NULL DEFAULT 'cron',
                started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                finished_at     TIMESTAMPTZ,
                status          TEXT NOT NULL
                                CHECK (status IN ('running', 'success', 'failed', 'skipped', 'timed_out')),
                summary_json    JSONB,
                error           TEXT
            )
            """
        )
    )
    conn.execute(
        sa.text("CREATE INDEX IF NOT EXISTS idx_pipeline_job_runs_job_started ON pipeline_job_runs(job_name, started_at DESC)")
    )
    conn.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS idx_pipeline_job_runs_running ON pipeline_job_runs(job_name) WHERE status = 'running'"
        )
    )

    conn.execute(
        sa.text(
            """
            INSERT INTO pipeline_job_config (
                job_name, enabled, min_interval_sec, max_runtime_sec, singleton, args_json
            ) VALUES
                ('auction_results', TRUE, 3600, 1800, TRUE, '{"lookback_days": 3}'::jsonb),
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
    conn.execute(sa.text("DROP TABLE IF EXISTS pipeline_job_runs"))
    conn.execute(sa.text("DROP TRIGGER IF EXISTS trg_pipeline_job_config_updated_at ON pipeline_job_config"))
    conn.execute(sa.text("DROP FUNCTION IF EXISTS pipeline_job_config_touch_updated_at()"))
    conn.execute(sa.text("DROP TABLE IF EXISTS pipeline_job_config"))
