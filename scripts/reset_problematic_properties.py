#!/usr/bin/env python3
"""
Reset and re-process properties with excessive chain periods (>300).

This script:
1. Clears V2 data (documents, chain_of_title, encumbrances, etc.)
2. Resets V1 status flags (step_ori_ingested, step_survival_analyzed)
3. Re-runs the pipeline for just these properties

Usage:
    uv run python scripts/reset_problematic_properties.py [--dry-run]
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
from loguru import logger

# Properties with >300 chain periods that need to be reset
# Updated 2025-12-25 after applying folio-level document filtering
PROBLEMATIC_FOLIOS = [
    "192935B6Y000008000040U",  # 1537 periods - TOUCHSTONE LOT 4
    "2031162VBA00000000580U",  # 1482 periods
    "20290676F000014000020U",  # 1256 periods
    "222904A47F00000000220P",  # 1042 periods
    "1928231J1000005J00000T",  # 976 periods - ORANGE RIVER LOT J
    "1928071GR000000000090U",  # 969 periods
    "2228285BZ000020000030P",  # 920 periods - LINCOLN PARK SOUTH LOT 3
    "2030192RX000000000260U",  # 687 periods
    "193024ZZZ000001717900U",  # 640 periods
    "1829144PP000008000110A",  # 632 periods - MUNRO'S ADDITION LOT 11
    "172806045000004000260U",  # 598 periods
    "20271089U000029000050A",  # 490 periods
    "193122B4000124D000020U",  # 387 periods
]

V1_DB_PATH = "data/property_master.db"
V2_DB_PATH = "data/property_master_v2.db"


def clear_v2_data(folio: str, conn: duckdb.DuckDBPyConnection, dry_run: bool = False) -> dict:
    """Clear all V2 data for a folio."""
    tables = [
        "documents",
        "chain_of_title",
        "encumbrances",
        "property_parties",
        "ori_search_queue",
        "legal_variations",
    ]

    counts = {}
    for table in tables:
        # Check if table exists
        try:
            result = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE folio = ?", [folio]).fetchone()
            count = result[0] if result else 0
            counts[table] = count

            if not dry_run and count > 0:
                conn.execute(f"DELETE FROM {table} WHERE folio = ?", [folio])
                logger.info(f"  Deleted {count} rows from {table}")
        except Exception as e:
            logger.debug(f"  Table {table} not found or error: {e}")
            counts[table] = 0

    return counts


def reset_v1_status(folio: str, conn: duckdb.DuckDBPyConnection, dry_run: bool = False) -> list:
    """Reset V1 status flags so property will be re-processed."""

    # Get case numbers for this folio
    cases = conn.execute(
        "SELECT case_number FROM auctions WHERE parcel_id = ?",
        [folio]
    ).fetchall()

    case_numbers = [c[0] for c in cases]

    if not case_numbers:
        logger.warning(f"  No auctions found for folio {folio}")
        return []

    # Steps to reset (these are TIMESTAMP columns, set to NULL)
    timestamp_steps = [
        "step_ori_ingested",
        "step_survival_analyzed",
    ]

    for case_number in case_numbers:
        if not dry_run:
            # Reset timestamp steps
            for step in timestamp_steps:
                try:
                    conn.execute(
                        f"UPDATE status SET {step} = NULL WHERE case_number = ?",
                        [case_number]
                    )
                    logger.debug(f"  Reset {step} for case {case_number}")
                except Exception as e:
                    logger.debug(f"  Could not reset {step}: {e}")

            # Set pipeline_status to 'processing' so orchestrator picks it up
            try:
                conn.execute(
                    "UPDATE status SET pipeline_status = 'processing' WHERE case_number = ?",
                    [case_number]
                )
                logger.debug(f"  Set pipeline_status='processing' for case {case_number}")
            except Exception as e:
                logger.debug(f"  Could not set pipeline_status: {e}")

            # Clear last_analyzed_case_number from parcels
            try:
                conn.execute(
                    "UPDATE parcels SET last_analyzed_case_number = NULL WHERE folio = ?",
                    [folio]
                )
            except Exception as e:
                logger.debug(f"  Could not clear last_analyzed_case_number: {e}")

            # CRITICAL: Reset auction.status from 'ANALYZED' to 'ACTIVE'
            # This prevents backfill_status_steps from re-marking step_survival_analyzed
            try:
                conn.execute(
                    "UPDATE auctions SET status = 'ACTIVE' WHERE case_number = ? AND status = 'ANALYZED'",
                    [case_number]
                )
                logger.debug(f"  Reset auction.status to 'ACTIVE' for case {case_number}")
            except Exception as e:
                logger.debug(f"  Could not reset auction.status: {e}")

            # CRITICAL: Reset needs_* flags to TRUE so backfill doesn't mark steps complete
            # The backfill has a second path that checks these flags!
            try:
                conn.execute(
                    """UPDATE auctions SET
                        needs_ori_ingestion = TRUE,
                        needs_lien_survival = TRUE
                    WHERE case_number = ?""",
                    [case_number]
                )
                logger.debug(f"  Reset needs_ori_ingestion/needs_lien_survival to TRUE for case {case_number}")
            except Exception as e:
                logger.debug(f"  Could not reset needs_* flags: {e}")

        logger.info(f"  Reset status for case {case_number}")

    return case_numbers


def main():
    parser = argparse.ArgumentParser(description="Reset problematic properties")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    parser.add_argument("--folio", type=str, help="Reset a specific folio instead of all problematic ones")
    args = parser.parse_args()

    folios = [args.folio] if args.folio else PROBLEMATIC_FOLIOS

    if args.dry_run:
        logger.info("DRY RUN - no changes will be made")

    logger.info(f"Resetting {len(folios)} properties...")

    # Connect to databases
    v1_conn = duckdb.connect(V1_DB_PATH)
    v2_conn = duckdb.connect(V2_DB_PATH)

    total_v2_deleted = 0
    total_cases_reset = 0

    for folio in folios:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {folio}")
        logger.info(f"{'='*60}")

        # Clear V2 data
        logger.info("Clearing V2 data...")
        counts = clear_v2_data(folio, v2_conn, args.dry_run)
        v2_total = sum(counts.values())
        total_v2_deleted += v2_total
        logger.info(f"  Total V2 rows: {v2_total}")

        # Reset V1 status
        logger.info("Resetting V1 status...")
        cases = reset_v1_status(folio, v1_conn, args.dry_run)
        total_cases_reset += len(cases)

    # Commit changes
    if not args.dry_run:
        v1_conn.commit()
        v2_conn.commit()

    v1_conn.close()
    v2_conn.close()

    logger.info(f"\n{'='*60}")
    logger.info("SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Properties processed: {len(folios)}")
    logger.info(f"V2 rows deleted: {total_v2_deleted}")
    logger.info(f"Cases reset: {total_cases_reset}")

    if args.dry_run:
        logger.info("\nThis was a dry run. Run without --dry-run to make changes.")
    else:
        logger.info("\nDone! Run the pipeline to re-process these properties:")
        logger.info("  uv run main.py --update")


if __name__ == "__main__":
    main()
