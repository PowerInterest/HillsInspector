"""PostgreSQL-backed scheduled job control and run tracking.

This module provides a lightweight control plane for cron-triggered Python jobs.
Cron remains a simple trigger, while execution policy is centralized in PG via:

- `pipeline_job_config`: runtime controls (`enabled`, min interval, singleton,
  max runtime, and JSON args).
- `pipeline_job_runs`: immutable run history with status and payload summary.

Execution model:
1. Acquire a per-job advisory lock (`pg_try_advisory_lock(hashtext(job_name))`)
   to prevent overlap across hosts/processes.
2. Read/seed `pipeline_job_config`.
3. Enforce policy gates (enabled, singleton, min interval).
4. Insert `pipeline_job_runs` row as `running`.
5. Execute the Python handler and finalize run status.

This gives operational control without editing crontabs or redeploying code.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable

from loguru import logger
from sqlalchemy import text

from src.utils.step_result import StepResult
from sunbiz.db import get_engine, resolve_pg_dsn

JobHandler = Callable[[str, dict[str, Any]], dict[str, Any] | StepResult]


@dataclass(frozen=True, slots=True)
class JobDefinition:
    """Static defaults for a scheduled job."""

    name: str
    handler: JobHandler
    default_min_interval_sec: int = 3600
    default_max_runtime_sec: int = 3600
    singleton: bool = True
    default_args_json: dict[str, Any] = field(default_factory=dict)


class PgJobControlService:
    """Run a single scheduled job with PG policy and audit logging."""

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = resolve_pg_dsn(dsn)
        self.engine = get_engine(self.dsn)

    def run_job(
        self,
        definition: JobDefinition,
        *,
        triggered_by: str = "cron",
        force: bool = False,
    ) -> dict[str, Any]:
        run_id: int | None = None
        lock_acquired = False

        with self.engine.connect() as conn:
            try:
                self._ensure_config_row(conn, definition)
                conn.commit()

                lock_acquired = bool(
                    conn.execute(
                        text("SELECT pg_try_advisory_lock(hashtext(:job_name))"),
                        {"job_name": definition.name},
                    ).scalar()
                )
                if not lock_acquired:
                    run_id = self._insert_run_row(
                        conn,
                        job_name=definition.name,
                        triggered_by=triggered_by,
                        status="skipped",
                        summary={"reason": "lock_not_acquired"},
                    )
                    conn.commit()
                    logger.warning(
                        "Scheduled job skipped: {} (lock_not_acquired)",
                        definition.name,
                    )
                    return {
                        "job_name": definition.name,
                        "run_id": run_id,
                        "status": "skipped",
                        "reason": "lock_not_acquired",
                    }

                config = self._get_config_row(conn, definition.name)
                if config is None:
                    raise RuntimeError(f"Missing config row for job {definition.name}")

                min_interval_sec = self._coerce_nonnegative_int(
                    config.get("min_interval_sec"),
                    definition.default_min_interval_sec,
                )
                max_runtime_sec = self._coerce_nonnegative_int(
                    config.get("max_runtime_sec"),
                    definition.default_max_runtime_sec,
                )
                singleton = bool(config.get("singleton"))
                enabled = bool(config.get("enabled"))
                args_json = self._coerce_args_json(config.get("args_json"))

                timed_out = self._expire_stale_running_rows(
                    conn,
                    job_name=definition.name,
                    max_runtime_sec=max_runtime_sec,
                )

                if not force and not enabled:
                    run_id = self._insert_run_row(
                        conn,
                        job_name=definition.name,
                        triggered_by=triggered_by,
                        status="skipped",
                        summary={
                            "reason": "disabled",
                            "timed_out_rows": timed_out,
                        },
                    )
                    conn.commit()
                    return {
                        "job_name": definition.name,
                        "run_id": run_id,
                        "status": "skipped",
                        "reason": "disabled",
                        "timed_out_rows": timed_out,
                    }

                if not force and singleton and self._has_running_row(conn, definition.name):
                    run_id = self._insert_run_row(
                        conn,
                        job_name=definition.name,
                        triggered_by=triggered_by,
                        status="skipped",
                        summary={"reason": "singleton_running"},
                    )
                    conn.commit()
                    return {
                        "job_name": definition.name,
                        "run_id": run_id,
                        "status": "skipped",
                        "reason": "singleton_running",
                    }

                if not force and min_interval_sec > 0:
                    recent = self._ran_recently(
                        conn,
                        job_name=definition.name,
                        min_interval_sec=min_interval_sec,
                    )
                    if recent:
                        run_id = self._insert_run_row(
                            conn,
                            job_name=definition.name,
                            triggered_by=triggered_by,
                            status="skipped",
                            summary={
                                "reason": "min_interval_not_elapsed",
                                "min_interval_sec": min_interval_sec,
                            },
                        )
                        conn.commit()
                        return {
                            "job_name": definition.name,
                            "run_id": run_id,
                            "status": "skipped",
                            "reason": "min_interval_not_elapsed",
                            "min_interval_sec": min_interval_sec,
                        }

                run_id = self._insert_run_row(
                    conn,
                    job_name=definition.name,
                    triggered_by=triggered_by,
                    status="running",
                    summary={
                        "force": force,
                        "config": {
                            "enabled": enabled,
                            "min_interval_sec": min_interval_sec,
                            "max_runtime_sec": max_runtime_sec,
                            "singleton": singleton,
                            "args_json": args_json,
                        },
                    },
                )
                conn.commit()

                # Execute outside transaction.
                payload = definition.handler(self.dsn, args_json)
                status = self._payload_status(payload)
                self._finalize_run(
                    conn,
                    run_id=run_id,
                    status=status,
                    summary=payload,
                    error=None,
                )
                conn.commit()
                return {
                    "job_name": definition.name,
                    "run_id": run_id,
                    "status": status,
                    "payload": payload,
                }

            except Exception as exc:
                with contextlib.suppress(Exception):
                    conn.rollback()
                if run_id is not None:
                    self._finalize_failed_run(conn, run_id=run_id, error=exc)
                logger.error("Scheduled job failed: {} ({})", definition.name, exc)
                return {
                    "job_name": definition.name,
                    "run_id": run_id,
                    "status": "failed",
                    "error": str(exc),
                }
            finally:
                if lock_acquired:
                    with contextlib.suppress(Exception):
                        conn.rollback()
                    try:
                        conn.execute(
                            text("SELECT pg_advisory_unlock(hashtext(:job_name))"),
                            {"job_name": definition.name},
                        )
                        conn.commit()
                    except Exception as exc:
                        logger.error(
                            "Failed to release advisory lock for {}: {}",
                            definition.name,
                            exc,
                        )

    @staticmethod
    def _coerce_nonnegative_int(value: Any, default: int) -> int:
        try:
            parsed = int(value)
            return parsed if parsed >= 0 else default
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _coerce_args_json(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                return {}
        return {}

    def _ensure_config_row(self, conn: Any, definition: JobDefinition) -> None:
        conn.execute(
            text(
                """
                INSERT INTO pipeline_job_config (
                    job_name,
                    enabled,
                    min_interval_sec,
                    max_runtime_sec,
                    singleton,
                    args_json,
                    paused_reason
                ) VALUES (
                    :job_name,
                    TRUE,
                    :min_interval_sec,
                    :max_runtime_sec,
                    :singleton,
                    CAST(:args_json AS JSONB),
                    NULL
                )
                ON CONFLICT (job_name) DO NOTHING
                """
            ),
            {
                "job_name": definition.name,
                "min_interval_sec": definition.default_min_interval_sec,
                "max_runtime_sec": definition.default_max_runtime_sec,
                "singleton": definition.singleton,
                "args_json": json.dumps(definition.default_args_json),
            },
        )

    def _get_config_row(self, conn: Any, job_name: str) -> dict[str, Any] | None:
        row = conn.execute(
            text(
                """
                SELECT job_name, enabled, min_interval_sec, max_runtime_sec,
                       singleton, args_json, paused_reason
                FROM pipeline_job_config
                WHERE job_name = :job_name
                """
            ),
            {"job_name": job_name},
        ).mappings().fetchone()
        return dict(row) if row else None

    def _expire_stale_running_rows(
        self,
        conn: Any,
        *,
        job_name: str,
        max_runtime_sec: int,
    ) -> int:
        rows = conn.execute(
            text(
                """
                UPDATE pipeline_job_runs
                SET status = 'timed_out',
                    finished_at = now(),
                    error = COALESCE(error, 'marked timed_out by scheduler gate')
                WHERE job_name = :job_name
                  AND status = 'running'
                  AND started_at < (now() - make_interval(secs => :max_runtime_sec))
                RETURNING run_id
                """
            ),
            {"job_name": job_name, "max_runtime_sec": max_runtime_sec},
        ).fetchall()
        return len(rows)

    def _has_running_row(self, conn: Any, job_name: str) -> bool:
        row = conn.execute(
            text(
                """
                SELECT 1
                FROM pipeline_job_runs
                WHERE job_name = :job_name
                  AND status = 'running'
                LIMIT 1
                """
            ),
            {"job_name": job_name},
        ).fetchone()
        return row is not None

    def _ran_recently(self, conn: Any, *, job_name: str, min_interval_sec: int) -> bool:
        row = conn.execute(
            text(
                """
                SELECT COALESCE(finished_at, started_at)
                FROM pipeline_job_runs
                WHERE job_name = :job_name
                  AND status IN ('running', 'success')
                ORDER BY COALESCE(finished_at, started_at) DESC
                LIMIT 1
                """
            ),
            {"job_name": job_name},
        ).fetchone()
        if row is None or row[0] is None:
            return False
        latest = row[0]
        if isinstance(latest, dt.date) and not isinstance(latest, dt.datetime):
            latest_dt = dt.datetime.combine(latest, dt.time.min, tzinfo=dt.UTC)
        elif isinstance(latest, dt.datetime):
            latest_dt = latest if latest.tzinfo else latest.replace(tzinfo=dt.UTC)
        else:
            return False
        return latest_dt > (dt.datetime.now(dt.UTC) - dt.timedelta(seconds=min_interval_sec))

    def _insert_run_row(
        self,
        conn: Any,
        *,
        job_name: str,
        triggered_by: str,
        status: str,
        summary: dict[str, Any] | None,
    ) -> int:
        run_id = conn.execute(
            text(
                """
                INSERT INTO pipeline_job_runs (
                    job_name,
                    triggered_by,
                    status,
                    summary_json,
                    started_at,
                    finished_at
                ) VALUES (
                    :job_name,
                    :triggered_by,
                    :status,
                    CAST(:summary_json AS JSONB),
                    now(),
                    CASE WHEN :status = 'running' THEN NULL ELSE now() END
                )
                RETURNING run_id
                """
            ),
            {
                "job_name": job_name,
                "triggered_by": triggered_by,
                "status": status,
                "summary_json": json.dumps(summary) if summary is not None else None,
            },
        ).scalar_one()
        return int(run_id)

    def _finalize_run(
        self,
        conn: Any,
        *,
        run_id: int,
        status: str,
        summary: dict[str, Any] | StepResult | None,
        error: str | None,
    ) -> None:
        conn.execute(
            text(
                """
                UPDATE pipeline_job_runs
                SET status = :status,
                    finished_at = now(),
                    summary_json = CAST(:summary_json AS JSONB),
                    error = :error
                WHERE run_id = :run_id
                """
            ),
            {
                "run_id": run_id,
                "status": status,
                "summary_json": self._serialize_summary(summary),
                "error": error,
            },
        )

    @staticmethod
    def _serialize_summary(summary: dict[str, Any] | StepResult | None) -> str | None:
        if summary is None:
            return None
        if isinstance(summary, StepResult):
            return json.dumps(summary.to_summary_dict())
        return json.dumps(summary)

    @staticmethod
    def _payload_status(payload: dict[str, Any] | StepResult) -> str:
        if isinstance(payload, StepResult):
            return payload.status
        if payload.get("status") == "skipped":
            return "skipped"
        if payload.get("status") == "degraded":
            return "degraded"
        if payload.get("status") == "failed":
            return "failed"
        if payload.get("skipped"):
            return "skipped"
        if payload.get("success") is False:
            return "failed"
        if payload.get("error") not in {None, ""}:
            return "failed"

        update = payload.get("update")
        if isinstance(update, dict):
            if update.get("status") == "degraded":
                return "degraded"
            if update.get("status") == "failed":
                return "failed"
            if update.get("success") is False:
                return "failed"
            if update.get("error") not in {None, ""}:
                return "failed"
        return "success"

    def _finalize_failed_run(
        self,
        conn: Any,
        *,
        run_id: int,
        error: Exception,
    ) -> None:
        try:
            self._finalize_run(
                conn,
                run_id=run_id,
                status="failed",
                summary=None,
                error=f"{error}\n{traceback.format_exc(limit=8)}",
            )
            conn.commit()
        except Exception as finalize_exc:
            with contextlib.suppress(Exception):
                conn.rollback()
            logger.error(
                "Failed to finalize scheduled job run {} after handler error: {}",
                run_id,
                finalize_exc,
            )
