import sys
from pathlib import Path

from loguru import logger
from sqlalchemy import text

# Add project root to sys.path to allow running as a script
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from sunbiz.db import get_engine, resolve_pg_dsn  # noqa: E402


def check_stats():
    dsn = resolve_pg_dsn()
    engine = get_engine(dsn)

    try:
        with engine.connect() as conn:
            # 1. Total Auctions (Base)
            total_auctions = conn.execute(text("SELECT COUNT(*) FROM foreclosures")).scalar()

            if total_auctions == 0:
                logger.warning("No foreclosures found in PG DB")
                return

            logger.info("Total foreclosures in DB: {}", total_auctions)

            # 2. Step Completion Rates from 'foreclosures' table timestamp columns
            steps = [
                "step_pdf_downloaded",
                "step_judgment_extracted",
                "step_ori_searched",
                "step_survival_analyzed",
            ]

            # Verify columns exist
            cursor = conn.execute(text("SELECT * FROM foreclosures LIMIT 0"))
            columns = list(cursor.keys())
            valid_steps = [s for s in steps if s in columns]

            if not valid_steps:
                logger.warning("No step timestamp columns found in foreclosures table")
                return

            logger.info("--- Step Success Rates ---")
            for step in valid_steps:
                count = conn.execute(text(f"SELECT COUNT(*) FROM foreclosures WHERE {step} IS NOT NULL")).scalar()
                pct = (count / total_auctions) * 100
                logger.info("{}: {}/{} ({:.1f}%)", step.replace("step_", ""), count, total_auctions, pct)

    except Exception as e:
        logger.exception("Error checking PG status: {}", e)


if __name__ == "__main__":
    check_stats()
