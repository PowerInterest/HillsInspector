"""
Script for sequentially testing all scheduled pipeline jobs.

This runs every cron job using the PG job controller and the `--force` flag
to verify that all scripts successfully cleanly boot, pull their configuration,
and execute effectively (or safely no-op if idempotency checks realize no new data exists).

Usage:
    uv run python -m src.scripts.test_all_cron
"""

import json
import subprocess
from pathlib import Path
from loguru import logger

JOBS = [
    "auction_results",
    "clerk_bulk",
    "clerk_criminal",
    "clerk_civil_alpha",
    "sunbiz_daily",
    "sunbiz_flr_quarterly",
    "sunbiz_entity_quarterly",
    "dor_nal_annual",
    "hcpa_bulk",
    "trust_accounts",
    "county_permits",
    "tampa_permits",
    "market_data",
    "single_pin_permits",
]


def run_job(job_name: str, repo_root: Path) -> dict:
    """Execute a single job and capture output."""
    cmd = ["uv", "run", "python", "-m", "src.tools.run_scheduled_job", "--job", job_name, "--force"]
    logger.info(f"Starting scheduled job: {job_name}")

    result = subprocess.run(cmd, check=False, capture_output=True, text=True, cwd=repo_root)

    if result.returncode == 0:
        logger.success(f"Finished {job_name} (Exit Code: 0)")
    else:
        logger.error(f"Failed {job_name} (Exit Code: {result.returncode})")
        logger.error(f"Stderr: {result.stderr}")

    return {"job": job_name, "exit_code": result.returncode, "stdout": result.stdout, "stderr": result.stderr}


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    out_file = repo_root / "logs" / "cron_test_results.json"

    logger.info(f"Sequentially testing {len(JOBS)} scheduled jobs...")

    results = []
    failed_jobs = []

    for job in JOBS:
        res = run_job(job, repo_root)
        results.append(res)
        if res["exit_code"] != 0:
            failed_jobs.append(job)

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with out_file.open("w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Saved full json output to {out_file}")

    if failed_jobs:
        logger.error(f"Test suite completed with {len(failed_jobs)} failures: {failed_jobs}")
        raise SystemExit(1)
    logger.success("Test suite completed flawlessly. All jobs exited with code 0.")


if __name__ == "__main__":
    main()
