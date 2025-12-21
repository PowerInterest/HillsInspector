"""
Database Audit Script for HillsInspector Pipeline.

Provides comprehensive statistics on data completeness across all pipeline steps.
"""
from datetime import UTC, datetime
from pathlib import Path

import duckdb

DB_PATH = "data/property_master.db"


def _fetchone_value(conn, query: str, params: list | None = None, default: int = 0) -> int:
    row = conn.execute(query, params or []).fetchone()
    return row[0] if row else default


def _fetchone_row(conn, query: str, params: list | None = None, default=None):
    row = conn.execute(query, params or []).fetchone()
    return row if row is not None else default


def audit_database():
    if not Path(DB_PATH).exists():
        print(f"Database not found at {DB_PATH}")
        return

    conn = duckdb.connect(DB_PATH, read_only=True)

    print("=" * 60)
    print("         HILLSINSPECTOR DATABASE AUDIT REPORT")
    print("=" * 60)

    # ==========================================================================
    # STEP 1 & 1.5: AUCTIONS
    # ==========================================================================
    print("\n[STEP 1 & 1.5] AUCTIONS")
    print("-" * 40)
    try:
        total_auctions = _fetchone_value(conn, "SELECT COUNT(*) FROM auctions")
        print(f"Total Auctions: {total_auctions}")

        # Breakdown by type
        by_type = conn.execute("""
            SELECT auction_type, COUNT(*) as cnt
            FROM auctions
            GROUP BY auction_type
        """).fetchall()
        for atype, cnt in by_type:
            print(f"  - {atype or 'UNKNOWN'}: {cnt}")

        # Date range
        date_range = _fetchone_row(
            conn,
            """
            SELECT MIN(auction_date), MAX(auction_date) FROM auctions
            """,
            default=(None, None),
        )
        print(f"Date Range: {date_range[0]} to {date_range[1]}")

        # Upcoming vs past
        today = datetime.now(tz=UTC).date()
        upcoming = _fetchone_value(
            conn, "SELECT COUNT(*) FROM auctions WHERE auction_date >= ?", [today]
        )
        past = _fetchone_value(
            conn, "SELECT COUNT(*) FROM auctions WHERE auction_date < ?", [today]
        )
        print(f"Upcoming: {upcoming} | Past: {past}")

        # Valid parcel IDs
        valid_parcels = _fetchone_value(
            conn,
            """
            SELECT COUNT(*) FROM auctions
            WHERE parcel_id IS NOT NULL
              AND parcel_id != ''
              AND LOWER(parcel_id) NOT IN ('n/a', 'none', 'unknown', 'property appraiser')
              AND LENGTH(parcel_id) >= 6
            """,
        )
        print(f"With Valid Parcel ID: {valid_parcels} ({total_auctions - valid_parcels} invalid/missing)")

    except Exception as e:
        print(f"Error: {e}")

    # ==========================================================================
    # STEP 2: FINAL JUDGMENT EXTRACTION
    # ==========================================================================
    print("\n[STEP 2] FINAL JUDGMENT EXTRACTION")
    print("-" * 40)
    try:
        needs_extraction = _fetchone_value(
            conn,
            "SELECT COUNT(*) FROM auctions WHERE needs_judgment_extraction = TRUE",
        )
        extracted = _fetchone_value(
            conn,
            "SELECT COUNT(*) FROM auctions WHERE extracted_judgment_data IS NOT NULL",
        )
        has_amounts = _fetchone_value(
            conn,
            "SELECT COUNT(*) FROM auctions WHERE total_judgment_amount IS NOT NULL AND total_judgment_amount > 0",
        )
        print(f"Needs Extraction: {needs_extraction}")
        print(f"Has Extracted Data: {extracted}")
        print(f"Has Judgment Amount: {has_amounts}")

        # Foreclosure type breakdown
        fc_types = conn.execute("""
            SELECT foreclosure_type, COUNT(*)
            FROM auctions
            WHERE foreclosure_type IS NOT NULL
            GROUP BY foreclosure_type
        """).fetchall()
        if fc_types:
            print("Foreclosure Types:")
            for fc_type, cnt in fc_types:
                print(f"  - {fc_type}: {cnt}")

    except Exception as e:
        print(f"Error: {e}")

    # ==========================================================================
    # STEP 3: BULK ENRICHMENT (PARCELS)
    # ==========================================================================
    print("\n[STEP 3] BULK ENRICHMENT (PARCELS)")
    print("-" * 40)
    try:
        total_parcels = _fetchone_value(conn, "SELECT COUNT(*) FROM parcels")
        print(f"Total Parcels: {total_parcels}")

        # Key fields
        has_owner = _fetchone_value(
            conn,
            "SELECT COUNT(*) FROM parcels WHERE owner_name IS NOT NULL AND owner_name != ''",
        )
        has_address = _fetchone_value(
            conn,
            "SELECT COUNT(*) FROM parcels WHERE property_address IS NOT NULL AND property_address != ''",
        )
        has_value = _fetchone_value(
            conn,
            "SELECT COUNT(*) FROM parcels WHERE assessed_value IS NOT NULL AND assessed_value > 0",
        )
        has_coords = _fetchone_value(
            conn,
            "SELECT COUNT(*) FROM parcels WHERE latitude IS NOT NULL AND longitude IS NOT NULL",
        )
        has_legal = _fetchone_value(
            conn,
            "SELECT COUNT(*) FROM parcels WHERE legal_description IS NOT NULL AND legal_description != ''",
        )

        print(f"With Owner Name: {has_owner} ({total_parcels - has_owner} missing)")
        print(f"With Address: {has_address} ({total_parcels - has_address} missing)")
        print(f"With Assessed Value: {has_value} ({total_parcels - has_value} missing)")
        print(f"With Coordinates: {has_coords} ({total_parcels - has_coords} missing)")
        print(f"With Legal Description: {has_legal} ({total_parcels - has_legal} missing)")

        # Check bulk_parcels table if exists
        has_bulk = _fetchone_value(
            conn,
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'bulk_parcels'",
        )
        if has_bulk:
            bulk_count = _fetchone_value(conn, "SELECT COUNT(*) FROM bulk_parcels")
            print(f"Bulk Parcels Table: {bulk_count:,} records")

    except Exception as e:
        print(f"Error: {e}")

    # ==========================================================================
    # STEP 3.5: HOMEHARVEST (MLS DATA & PHOTOS)
    # ==========================================================================
    print("\n[STEP 3.5] HOMEHARVEST (MLS DATA & PHOTOS)")
    print("-" * 40)
    try:
        has_hh = _fetchone_value(
            conn,
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'home_harvest'",
        )
        if has_hh:
            total_hh = _fetchone_value(conn, "SELECT COUNT(*) FROM home_harvest")
            with_photos = _fetchone_value(
                conn,
                """
                SELECT COUNT(*) FROM home_harvest
                WHERE primary_photo IS NOT NULL OR photos IS NOT NULL OR alt_photos IS NOT NULL
                """,
            )
            with_price = _fetchone_value(
                conn,
                "SELECT COUNT(*) FROM home_harvest WHERE list_price IS NOT NULL",
            )
            with_hoa = _fetchone_value(
                conn,
                "SELECT COUNT(*) FROM home_harvest WHERE hoa_fee IS NOT NULL AND hoa_fee > 0",
            )

            print(f"Total HomeHarvest Records: {total_hh}")
            print(f"With Photos: {with_photos}")
            print(f"With List Price: {with_price}")
            print(f"With HOA Fee: {with_hoa}")

            # Check pipeline flag
            needs_hh = _fetchone_value(
                conn,
                "SELECT COUNT(*) FROM auctions WHERE needs_homeharvest_enrichment = TRUE",
            )
            print(f"Auctions Needing HomeHarvest: {needs_hh}")
        else:
            print("HomeHarvest table not found")

    except Exception as e:
        print(f"Error: {e}")

    # ==========================================================================
    # PHASE 1: PARALLEL SCRAPERS
    # ==========================================================================
    print("\n[PHASE 1] PARALLEL SCRAPERS")
    print("-" * 40)

    # Tax Status
    try:
        print("\nTax Status:")
        needs_tax = _fetchone_value(
            conn,
            "SELECT COUNT(*) FROM auctions WHERE needs_tax_check = TRUE",
        )
        print(f"  Auctions Needing Tax Check: {needs_tax}")

        # Check parcels for tax_status column
        tax_stats = conn.execute("""
            SELECT tax_status, COUNT(*) FROM parcels
            WHERE tax_status IS NOT NULL
            GROUP BY tax_status
        """).fetchall()
        if tax_stats:
            for status, cnt in tax_stats:
                print(f"  - {status}: {cnt}")
        else:
            print("  No tax status data found")

    except Exception as e:
        print(f"  Tax Status Error: {e}")

    # Market Data (Zillow + Realtor)
    try:
        print("\nMarket Data:")
        has_market = _fetchone_value(
            conn,
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'market_data'",
        )
        if has_market:
            by_source = conn.execute("""
                SELECT source, COUNT(*) FROM market_data GROUP BY source
            """).fetchall()
            for source, cnt in by_source:
                print(f"  - {source}: {cnt} records")

            unique_parcels = _fetchone_value(
                conn,
                "SELECT COUNT(DISTINCT folio) FROM market_data",
            )
            print(f"  Unique Parcels with Market Data: {unique_parcels}")

            needs_market = _fetchone_value(
                conn,
                "SELECT COUNT(*) FROM auctions WHERE needs_market_data = TRUE",
            )
            print(f"  Auctions Needing Market Data: {needs_market}")
        else:
            print("  Market Data table not found")

    except Exception as e:
        print(f"  Market Data Error: {e}")

    # FEMA Flood
    try:
        print("\nFEMA Flood:")
        needs_flood = _fetchone_value(
            conn,
            "SELECT COUNT(*) FROM auctions WHERE needs_flood_check = TRUE",
        )
        completed_flood = _fetchone_value(
            conn,
            "SELECT COUNT(*) FROM auctions WHERE needs_flood_check = FALSE",
        )
        print(f"  Completed Flood Checks: {completed_flood}")
        print(f"  Auctions Needing Flood Check: {needs_flood}")

    except Exception as e:
        print(f"  Flood Error: {e}")

    # Permits
    try:
        print("\nBuilding Permits:")
        has_permits = _fetchone_value(
            conn,
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'permits'",
        )
        if has_permits:
            total_permits = _fetchone_value(conn, "SELECT COUNT(*) FROM permits")
            unique_parcels = _fetchone_value(
                conn,
                "SELECT COUNT(DISTINCT folio) FROM permits",
            )
            print(f"  Total Permit Records: {total_permits}")
            print(f"  Parcels with Permits: {unique_parcels}")

            needs_permits = _fetchone_value(
                conn,
                "SELECT COUNT(*) FROM auctions WHERE needs_permit_check = TRUE",
            )
            print(f"  Auctions Needing Permit Check: {needs_permits}")
        else:
            print("  Permits table not found")

    except Exception as e:
        print(f"  Permits Error: {e}")

    # ==========================================================================
    # PHASE 2: ORI INGESTION & CHAIN OF TITLE
    # ==========================================================================
    print("\n[PHASE 2] ORI INGESTION & CHAIN OF TITLE")
    print("-" * 40)

    # Documents
    try:
        print("\nDocuments:")
        has_docs = _fetchone_value(
            conn,
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'documents'",
        )
        if has_docs:
            total_docs = _fetchone_value(conn, "SELECT COUNT(*) FROM documents")
            unique_parcels = _fetchone_value(
                conn,
                "SELECT COUNT(DISTINCT folio) FROM documents WHERE folio IS NOT NULL",
            )
            print(f"  Total Document Records: {total_docs}")
            print(f"  Parcels with Documents: {unique_parcels}")

            # By type
            doc_types = conn.execute("""
                SELECT document_type, COUNT(*) FROM documents
                WHERE document_type IS NOT NULL
                GROUP BY document_type
                ORDER BY COUNT(*) DESC
                LIMIT 10
            """).fetchall()
            if doc_types:
                print("  Top Document Types:")
                for dtype, cnt in doc_types:
                    print(f"    - {dtype}: {cnt}")

            needs_ori = _fetchone_value(
                conn,
                "SELECT COUNT(*) FROM auctions WHERE needs_ori_ingestion = TRUE",
            )
            print(f"  Auctions Needing ORI Ingestion: {needs_ori}")
        else:
            print("  Documents table not found")

    except Exception as e:
        print(f"  Documents Error: {e}")

    # Chain of Title
    try:
        print("\nChain of Title:")
        has_chain = _fetchone_value(
            conn,
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'chain_of_title'",
        )
        if has_chain:
            total_entries = _fetchone_value(conn, "SELECT COUNT(*) FROM chain_of_title")
            unique_parcels = _fetchone_value(
                conn,
                "SELECT COUNT(DISTINCT folio) FROM chain_of_title",
            )
            print(f"  Total Chain Entries: {total_entries}")
            print(f"  Parcels with Chain: {unique_parcels}")

            # Chain depth stats
            chain_depth = _fetchone_row(
                conn,
                """
                SELECT
                    MIN(cnt) as min_depth,
                    AVG(cnt) as avg_depth,
                    MAX(cnt) as max_depth
                FROM (
                    SELECT folio, COUNT(*) as cnt FROM chain_of_title GROUP BY folio
                )
                """,
                default=(0, 0.0, 0),
            )
            min_depth, avg_depth, max_depth = chain_depth
            print(f"  Chain Depth: Min={min_depth}, Avg={avg_depth:.1f}, Max={max_depth}")

            # Shallow chains (potential gaps)
            shallow = _fetchone_value(
                conn,
                """
                SELECT COUNT(*) FROM (
                    SELECT folio FROM chain_of_title GROUP BY folio HAVING COUNT(*) < 2
                )
                """,
            )
            print(f"  Shallow Chains (<2 entries): {shallow}")

    except Exception as e:
        print(f"  Chain Error: {e}")

    # Sales History
    try:
        print("\nSales History (HCPA GIS):")
        has_sales = _fetchone_value(
            conn,
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'sales_history'",
        )
        if has_sales:
            total_sales = _fetchone_value(conn, "SELECT COUNT(*) FROM sales_history")
            unique_parcels = _fetchone_value(
                conn,
                "SELECT COUNT(DISTINCT folio) FROM sales_history",
            )
            print(f"  Total Sales Records: {total_sales}")
            print(f"  Parcels with Sales History: {unique_parcels}")
        else:
            print("  Sales History table not found")

    except Exception as e:
        print(f"  Sales History Error: {e}")

    # ==========================================================================
    # PHASE 3: ENCUMBRANCES & LIEN SURVIVAL
    # ==========================================================================
    print("\n[PHASE 3] ENCUMBRANCES & LIEN SURVIVAL")
    print("-" * 40)
    try:
        has_enc = _fetchone_value(
            conn,
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'encumbrances'",
        )
        if has_enc:
            total_enc = _fetchone_value(conn, "SELECT COUNT(*) FROM encumbrances")
            unique_parcels = _fetchone_value(
                conn,
                "SELECT COUNT(DISTINCT folio) FROM encumbrances",
            )
            print(f"Total Encumbrances: {total_enc}")
            print(f"Parcels with Encumbrances: {unique_parcels}")

            # By type
            enc_types = conn.execute("""
                SELECT encumbrance_type, COUNT(*) FROM encumbrances
                WHERE encumbrance_type IS NOT NULL
                GROUP BY encumbrance_type
                ORDER BY COUNT(*) DESC
            """).fetchall()
            if enc_types:
                print("By Type:")
                for etype, cnt in enc_types:
                    print(f"  - {etype}: {cnt}")

            # Survival status
            survival = conn.execute("""
                SELECT survival_status, COUNT(*) FROM encumbrances
                WHERE survival_status IS NOT NULL
                GROUP BY survival_status
            """).fetchall()
            if survival:
                print("Survival Analysis:")
                for status, cnt in survival:
                    print(f"  - {status}: {cnt}")

            # Satisfied vs Open
            satisfied = _fetchone_value(
                conn,
                "SELECT COUNT(*) FROM encumbrances WHERE is_satisfied = TRUE",
            )
            open_enc = _fetchone_value(
                conn,
                "SELECT COUNT(*) FROM encumbrances WHERE is_satisfied = FALSE OR is_satisfied IS NULL",
            )
            print(f"Satisfied: {satisfied} | Open: {open_enc}")

            needs_survival = _fetchone_value(
                conn,
                "SELECT COUNT(*) FROM auctions WHERE needs_lien_survival = TRUE",
            )
            print(f"Auctions Needing Survival Analysis: {needs_survival}")

        else:
            print("Encumbrances table not found")

    except Exception as e:
        print(f"Error: {e}")

    # ==========================================================================
    # PIPELINE COMPLETION SUMMARY
    # ==========================================================================
    print("\n" + "=" * 60)
    print("         PIPELINE STEP COMPLETION SUMMARY")
    print("=" * 60)
    try:
        steps = [
            ("needs_judgment_extraction", "Judgment Extraction"),
            ("needs_hcpa_enrichment", "HCPA Enrichment"),
            ("needs_ori_ingestion", "ORI Ingestion"),
            ("needs_lien_survival", "Lien Survival"),
            ("needs_sunbiz_search", "Sunbiz Search"),
            ("needs_permit_check", "Permit Check"),
            ("needs_flood_check", "Flood Check"),
            ("needs_market_data", "Market Data"),
            ("needs_tax_check", "Tax Check"),
            ("needs_homeharvest_enrichment", "HomeHarvest"),
        ]

        total = _fetchone_value(conn, "SELECT COUNT(*) FROM auctions")
        print(f"\n{'Step':<25} {'Pending':>10} {'Complete':>10} {'% Done':>10}")
        print("-" * 55)

        for col, name in steps:
            try:
                pending = _fetchone_value(
                    conn,
                    f"SELECT COUNT(*) FROM auctions WHERE {col} = TRUE",
                )
                complete = total - pending
                pct = (complete / total * 100) if total > 0 else 0
                print(f"{name:<25} {pending:>10} {complete:>10} {pct:>9.1f}%")
            except Exception:
                print(f"{name:<25} {'N/A':>10} {'N/A':>10} {'N/A':>10}")

    except Exception as e:
        print(f"Error: {e}")

    # ==========================================================================
    # DATA QUALITY ISSUES
    # ==========================================================================
    print("\n" + "=" * 60)
    print("         DATA QUALITY ISSUES")
    print("=" * 60)
    try:
        # Auctions without parcels
        orphan_auctions = _fetchone_value(
            conn,
            """
            SELECT COUNT(*) FROM auctions a
            LEFT JOIN parcels p ON a.parcel_id = p.folio OR a.folio = p.folio
            WHERE p.folio IS NULL AND a.parcel_id IS NOT NULL
            """,
        )
        print(f"Auctions without matching Parcel: {orphan_auctions}")

        # HCPA scrape failures
        hcpa_failed = _fetchone_value(
            conn,
            "SELECT COUNT(*) FROM auctions WHERE hcpa_scrape_failed = TRUE",
        )
        print(f"HCPA Scrape Failures: {hcpa_failed}")

        # Invalid parcel IDs (calculated based on validation rules)
        invalid_parcels = _fetchone_value(
            conn,
            """
            SELECT COUNT(*) FROM auctions
            WHERE parcel_id IS NULL
               OR parcel_id = ''
               OR LOWER(parcel_id) IN ('n/a', 'none', 'unknown', 'property appraiser')
               OR LENGTH(parcel_id) < 6
            """,
        )
        print(f"Auctions with Invalid Parcel ID: {invalid_parcels}")

        # Missing critical data for analysis
        missing_for_analysis = _fetchone_value(
            conn,
            """
            SELECT COUNT(*) FROM auctions a
            LEFT JOIN parcels p ON a.parcel_id = p.folio OR a.folio = p.folio
            WHERE (p.assessed_value IS NULL OR p.assessed_value = 0)
              AND a.auction_date >= CURRENT_DATE
            """,
        )
        print(f"Upcoming Auctions Missing Assessed Value: {missing_for_analysis}")

    except Exception as e:
        print(f"Error: {e}")

    print("\n" + "=" * 60)
    print("         AUDIT COMPLETE")
    print("=" * 60)

    conn.close()


if __name__ == "__main__":
    audit_database()
