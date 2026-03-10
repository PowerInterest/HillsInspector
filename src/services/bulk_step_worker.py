"""Internal worker for running one bulk ingestion step."""

from __future__ import annotations

import json
import os
from typing import Any

from loguru import logger

from src.services.pg_job_control_service import JobDefinition, PgJobControlService
from src.services.pg_pipeline_controller import ControllerSettings
from src.services.pg_pipeline_controller import PgPipelineController
from src.utils.step_result import StepResult, is_failed_payload


STEP_METHODS: dict[str, str] = {
    "hcpa_suite": "_run_hcpa_suite",
    "clerk_bulk": "_run_clerk_bulk",
    "clerk_criminal": "_run_clerk_criminal",
    "clerk_civil_alpha": "_run_clerk_civil_alpha",
    "dor_nal": "_run_nal",
    "sunbiz_flr": "_run_flr",
    "sunbiz_entity": "_run_sunbiz_entity",
    "county_permits": "_run_county_permits",
    "tampa_permits": "_run_tampa_permits",
}


def run_bulk_step(
    step_name: str,
    *,
    dsn: str | None = None,
    force_all: bool = False,
) -> dict[str, Any]:
    method_name = STEP_METHODS.get(step_name)
    if not method_name:
        return {"success": False, "error": f"unknown_bulk_step:{step_name}"}

    settings = ControllerSettings(dsn=dsn, force_all=force_all)
    controller = PgPipelineController(settings)
    method = getattr(controller, method_name)
    payload = method()
    if isinstance(payload, StepResult):
        return payload.to_summary_dict()
    return payload


def _env_true(value: str | None) -> bool:
    if not value:
        return False
    return value.lower() in {"1", "true", "yes", "on"}


def _bulk_step_job_name(step_name: str) -> str:
    return f"controller_bulk_step:{step_name}"


def main() -> None:
    step_name = (os.getenv("HI_BULK_STEP_NAME") or "").strip()
    if not step_name:
        payload = {"success": False, "error": "missing_env:HI_BULK_STEP_NAME"}
        logger.error(payload["error"])
        print(json.dumps(payload, indent=2))
        raise SystemExit(1)

    force_all = _env_true(os.getenv("HI_FORCE_ALL"))
    dsn = os.getenv("SUNBIZ_PG_DSN")

    logger.info(
        "Bulk step worker start: {} force_all={} job_name={}",
        step_name,
        force_all,
        _bulk_step_job_name(step_name),
    )
    job_control = PgJobControlService(dsn=dsn)
    result = job_control.run_job(
        JobDefinition(
            name=_bulk_step_job_name(step_name),
            handler=lambda active_dsn, _args: run_bulk_step(
                step_name,
                dsn=active_dsn,
                force_all=force_all,
            ),
            default_min_interval_sec=0,
            default_max_runtime_sec=6 * 60 * 60,
            singleton=True,
            default_args_json={"step_name": step_name, "force_all": force_all},
        ),
        triggered_by="controller_background",
        force=True,
    )
    logger.info("Bulk step worker complete: {} result={}", step_name, result)
    print(json.dumps(result, indent=2, default=str))
    if is_failed_payload(result):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
