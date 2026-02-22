#!/usr/bin/env python
"""
Cleanup script to remove documents created by rebuild script without party data.

These documents were created on 2025-12-22 from PDF filenames without proper
ORI party data. They break chain of title analysis.

Criteria for deletion:
- document_type does NOT start with '(' (underscore format like D_DEED)
- party1 IS NULL or empty
- These are rebuild artifacts, not proper ORI-sourced documents
"""
import sys
import duckdb
from pathlib import Path
from loguru import logger

DB_PATH = Path("data/property_master.db")


def analyze_bad_documents(conn: duckdb.DuckDBPyConnection) -> dict:
    """Analyze documents that should be deleted."""

    # Count bad documents
    result = conn.execute("""
        SELECT COUNT(*) FROM documents
        WHERE document_type NOT LIKE '(%%'
        AND (party1 IS NULL OR party1 = '')
    """).fetchone()
    bad_count = result[0]

    # Count unique folios affected
    result = conn.execute("""
        SELECT COUNT(DISTINCT folio) FROM documents
        WHERE document_type NOT LIKE '(%%'
        AND (party1 IS NULL OR party1 = '')
    """).fetchone()
    folios_affected = result[0]

    # Sample bad documents
    sample = conn.execute("""
        SELECT folio, document_type, instrument_number, recording_date
        FROM documents
        WHERE document_type NOT LIKE '(%%'
        AND (party1 IS NULL OR party1 = '')
        LIMIT 10
    """).fetchall()

    # Check if any folios will be left without documents
    orphan_folios = conn.execute("""
        WITH bad_docs AS (
            SELECT DISTINCT folio FROM documents
            WHERE document_type NOT LIKE '(%%'
            AND (party1 IS NULL OR party1 = '')
        ),
        good_docs AS (
            SELECT DISTINCT folio FROM documents
            WHERE document_type LIKE '(%%'
            OR (party1 IS NOT NULL AND party1 <> '')
        )
        SELECT folio FROM bad_docs
        WHERE folio NOT IN (SELECT folio FROM good_docs)
    """).fetchall()

    return {
        "bad_count": bad_count,
        "folios_affected": folios_affected,
        "sample": sample,
        "orphan_folios": [r[0] for r in orphan_folios],
    }


def delete_bad_documents(conn: duckdb.DuckDBPyConnection, dry_run: bool = True) -> int:
    """Delete documents without party data (rebuild artifacts)."""

    if dry_run:
        result = conn.execute("""
            SELECT COUNT(*) FROM documents
            WHERE document_type NOT LIKE '(%%'
            AND (party1 IS NULL OR party1 = '')
        """).fetchone()
        return result[0]

    # Actually delete
    result = conn.execute("""
        DELETE FROM documents
        WHERE document_type NOT LIKE '(%%'
        AND (party1 IS NULL OR party1 = '')
    """)

    return result.fetchone()[0] if result else 0


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Cleanup bad document records")
    parser.add_argument("--execute", action="store_true", help="Actually delete (default is dry run)")
    args = parser.parse_args()

    dry_run = not args.execute

    if not DB_PATH.exists():
        logger.error(f"Database not found: {DB_PATH}")
        return 1

    conn = duckdb.connect(str(DB_PATH), read_only=dry_run)

    try:
        # Analyze
        logger.info("Analyzing bad documents...")
        analysis = analyze_bad_documents(conn)

        print(f"\n{'='*60}")
        print("BAD DOCUMENT ANALYSIS")
        print(f"{'='*60}")
        print(f"Documents to delete: {analysis['bad_count']}")
        print(f"Folios affected: {analysis['folios_affected']}")
        print(f"Orphan folios (will have no docs after delete): {len(analysis['orphan_folios'])}")

        if analysis['orphan_folios']:
            print(f"\nOrphan folios: {analysis['orphan_folios'][:10]}")

        print(f"\nSample bad documents:")
        for row in analysis['sample']:
            print(f"  {row[0][:30]} | {row[1][:20]} | Instr: {row[2]}")

        print(f"\n{'='*60}")

        if dry_run:
            print("DRY RUN - No changes made")
            print(f"Would delete {analysis['bad_count']} documents")
            print("\nRun with --execute to actually delete")
        else:
            # Delete
            logger.info("Deleting bad documents...")
            deleted = delete_bad_documents(conn, dry_run=False)
            logger.success(f"Deleted {deleted} bad documents")

            # Verify
            remaining = conn.execute("""
                SELECT COUNT(*) FROM documents
                WHERE document_type NOT LIKE '(%%'
                AND (party1 IS NULL OR party1 = '')
            """).fetchone()[0]

            print(f"\nVerification: {remaining} bad documents remaining")

        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
