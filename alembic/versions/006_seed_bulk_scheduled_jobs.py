"""Seed pipeline_job_config rows for remaining bulk data services.

Adds config rows for services that were previously only runnable via
Controller.py but now have scheduled job handlers:

- clerk_criminal, clerk_civil_alpha, trust_accounts (existing handlers, missing seeds)
- county_permits, tampa_permits, market_data, single_pin_permits (new handlers)

Uses ON CONFLICT DO NOTHING to preserve any operator-customized values.

Revision ID: 006_seed_bulk_jobs
Revises: 005_drop_clerk_name_index
Create Date: 2026-03-03
"""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision = "006_seed_bulk_jobs"
down_revision = "005_drop_clerk_name_index"
branch_labels = None
depends_on = None

_NEW_JOBS = [
    # (job_name, min_interval_sec, max_runtime_sec, args_json)
    ("clerk_criminal", 604800, 7200, {}),
    ("clerk_civil_alpha", 604800, 7200, {}),
    ("trust_accounts", 86400, 1800, {"force_reprocess": False}),
    ("county_permits", 86400, 3600, {"page_size": 2000, "force_full": False}),
    (
        "tampa_permits",
        86400,
        7200,
        {"lookback_days": 30, "keep_csv": False, "enrich_limit": 250},
    ),
    ("market_data", 86400, 14400, {"use_windows_chrome": False}),
    (
        "single_pin_permits",
        86400,
        3600,
        {"limit": 25, "max_permits_per_pin": 0, "timeout_seconds": 45},
    ),
]


def upgrade() -> None:
    conn = op.get_bind()
    insert_stmt = sa.text(
        """
        INSERT INTO pipeline_job_config (
            job_name, enabled, min_interval_sec, max_runtime_sec, singleton, args_json
        ) VALUES (
            :job_name, TRUE, :min_interval, :max_runtime, TRUE, :args_json
        )
        ON CONFLICT (job_name) DO NOTHING
        """
    ).bindparams(sa.bindparam("args_json", type_=JSONB))
    for job_name, min_interval, max_runtime, args_json in _NEW_JOBS:
        conn.execute(
            insert_stmt,
            {
                "job_name": job_name,
                "min_interval": min_interval,
                "max_runtime": max_runtime,
                "args_json": args_json,
            },
        )


def downgrade() -> None:
    conn = op.get_bind()
    job_names = [j[0] for j in _NEW_JOBS]
    delete_stmt = sa.text(
        "DELETE FROM pipeline_job_config WHERE job_name IN :names"
    ).bindparams(sa.bindparam("names", expanding=True))
    conn.execute(
        delete_stmt,
        {"names": job_names},
    )
