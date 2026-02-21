"""Internal worker for running one bulk ingestion step."""

from __future__ import annotations

import json
import os
from typing import Any

from loguru import logger

from src.services.pg_pipeline_controller import ControllerSettings
from src.services.pg_pipeline_controller import PgPipelineController


STEP_METHODS: dict[str, str] = {
    "hcpa_suite": "_run_hcpa_suite",
    "clerk_bulk": "_run_clerk_bulk",
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
    return method()


def _env_true(value: str | None) -> bool:
    if not value:
        return False
    return value.lower() in {"1", "true", "yes", "on"}


def _payload_failed(payload: dict[str, Any]) -> bool:
    if payload.get("success") is False:
        return True
    if payload.get("error") not in (None, ""):
        return True

    update = payload.get("update")
    if isinstance(update, dict):
        if update.get("success") is False:
            return True
        if update.get("error") not in (None, ""):
            return True
    return False


def main() -> None:
    step_name = (os.getenv("HI_BULK_STEP_NAME") or "").strip()
    if not step_name:
        payload = {"success": False, "error": "missing_env:HI_BULK_STEP_NAME"}
        logger.error(payload["error"])
        print(json.dumps(payload, indent=2))
        raise SystemExit(1)

    force_all = _env_true(os.getenv("HI_FORCE_ALL"))
    dsn = os.getenv("SUNBIZ_PG_DSN")

    logger.info(f"Bulk step worker start: {step_name} force_all={force_all}")
    payload = run_bulk_step(step_name, dsn=dsn, force_all=force_all)
    logger.info(f"Bulk step worker complete: {step_name} payload={payload}")
    print(json.dumps(payload, indent=2, default=str))
    if _payload_failed(payload):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
