"""
Reset needs_ori_ingestion flag for properties with 0 mortgages.

These properties likely had incorrect ORI search terms and need to be re-ingested
with the new lot/block first search strategy.
"""

import sys
sys.path.insert(0, "/mnt/c/code/HillsInspector")

import duckdb

def main():
    conn = duckdb.connect("data/property_master.db")

    # First, find properties with 0 mortgages
    result = conn.execute("""
        SELECT
            a.case_number,
            a.folio,
            a.property_address
        FROM auctions a
        WHERE a.folio IS NOT NULL
        AND a.folio != ''
        AND a.folio NOT IN (
            SELECT DISTINCT folio FROM encumbrances
            WHERE document_type LIKE '%MTG%' OR document_type LIKE '%MORTGAGE%'
        )
        AND a.needs_ori_ingestion = FALSE
        ORDER BY a.auction_date DESC
    """).fetchall()

    print(f"Found {len(result)} properties with 0 mortgages that need re-ingestion")

    if len(result) == 0:
        print("Nothing to reset")
        return

    # Show first 10
    print("\nFirst 10 properties to re-ingest:")
    for row in result[:10]:
        print(f"  {row[0]}: {row[2]} (folio: {row[1]})")

    # Prompt for confirmation
    print(f"\nThis will reset needs_ori_ingestion=TRUE for {len(result)} properties.")
    response = input("Continue? (y/n): ")

    if response.lower() != 'y':
        print("Aborted")
        return

    # Reset the flags
    conn.execute("""
        UPDATE auctions
        SET needs_ori_ingestion = TRUE
        WHERE folio IS NOT NULL
        AND folio != ''
        AND folio NOT IN (
            SELECT DISTINCT folio FROM encumbrances
            WHERE document_type LIKE '%MTG%' OR document_type LIKE '%MORTGAGE%'
        )
        AND needs_ori_ingestion = FALSE
    """)

    print(f"Reset {len(result)} properties for ORI re-ingestion")

    # Also clear their existing documents so we get fresh data
    folios = [row[1] for row in result]
    print(f"\nClearing existing documents for {len(folios)} folios...")

    conn.execute("""
        DELETE FROM documents
        WHERE folio IN (
            SELECT folio FROM auctions a
            WHERE a.folio IS NOT NULL
            AND a.folio != ''
            AND a.folio NOT IN (
                SELECT DISTINCT folio FROM encumbrances
                WHERE document_type LIKE '%MTG%' OR document_type LIKE '%MORTGAGE%'
            )
        )
    """)

    conn.execute("""
        DELETE FROM encumbrances
        WHERE folio IN (
            SELECT folio FROM auctions a
            WHERE a.folio IS NOT NULL
            AND a.folio != ''
            AND a.folio NOT IN (
                SELECT DISTINCT folio FROM encumbrances
                WHERE document_type LIKE '%MTG%' OR document_type LIKE '%MORTGAGE%'
            )
        )
    """)

    conn.execute("""
        DELETE FROM chain_of_title
        WHERE folio IN (
            SELECT folio FROM auctions a
            WHERE a.folio IS NOT NULL
            AND a.folio != ''
            AND a.folio NOT IN (
                SELECT DISTINCT folio FROM encumbrances
                WHERE document_type LIKE '%MTG%' OR document_type LIKE '%MORTGAGE%'
            )
        )
    """)

    print("Done! Run 'uv run main.py --update --start-step 5' to re-ingest these properties.")

if __name__ == "__main__":
    main()
