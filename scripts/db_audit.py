"""
Database Audit Script for HillsInspector Pipeline.

Provides comprehensive statistics on data completeness across all pipeline steps.
"""
import duckdb
from pathlib import Path
from datetime import date, timedelta

DB_PATH = "data/property_master.db"


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
        total_auctions = conn.execute("SELECT COUNT(*) FROM auctions").fetchone()[0]
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
        date_range = conn.execute("""
            SELECT MIN(auction_date), MAX(auction_date) FROM auctions
        """).fetchone()
        print(f"Date Range: {date_range[0]} to {date_range[1]}")

        # Upcoming vs past
        today = date.today()
        upcoming = conn.execute(
            "SELECT COUNT(*) FROM auctions WHERE auction_date >= ?", [today]
        ).fetchone()[0]
        past = conn.execute(
            "SELECT COUNT(*) FROM auctions WHERE auction_date < ?", [today]
        ).fetchone()[0]
        print(f"Upcoming: {upcoming} | Past: {past}")

        # Valid parcel IDs
        valid_parcels = conn.execute("""
            SELECT COUNT(*) FROM auctions
            WHERE parcel_id IS NOT NULL
              AND parcel_id != ''
              AND LOWER(parcel_id) NOT IN ('n/a', 'none', 'unknown', 'property appraiser')
              AND LENGTH(parcel_id) >= 6
        """).fetchone()[0]
        print(f"With Valid Parcel ID: {valid_parcels} ({total_auctions - valid_parcels} invalid/missing)")

    except Exception as e:
        print(f"Error: {e}")

    # ==========================================================================
    # STEP 2: FINAL JUDGMENT EXTRACTION
    # ==========================================================================
    print("\n[STEP 2] FINAL JUDGMENT EXTRACTION")
    print("-" * 40)
    try:
        needs_extraction = conn.execute(
            "SELECT COUNT(*) FROM auctions WHERE needs_judgment_extraction = TRUE"
        ).fetchone()[0]
        extracted = conn.execute(
            "SELECT COUNT(*) FROM auctions WHERE extracted_judgment_data IS NOT NULL"
        ).fetchone()[0]
        has_amounts = conn.execute(
            "SELECT COUNT(*) FROM auctions WHERE total_judgment_amount IS NOT NULL AND total_judgment_amount > 0"
        ).fetchone()[0]
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
        total_parcels = conn.execute("SELECT COUNT(*) FROM parcels").fetchone()[0]
        print(f"Total Parcels: {total_parcels}")

        # Key fields
        has_owner = conn.execute(
            "SELECT COUNT(*) FROM parcels WHERE owner_name IS NOT NULL AND owner_name != ''"
        ).fetchone()[0]
        has_address = conn.execute(
            "SELECT COUNT(*) FROM parcels WHERE property_address IS NOT NULL AND property_address != ''"
        ).fetchone()[0]
        has_value = conn.execute(
            "SELECT COUNT(*) FROM parcels WHERE assessed_value IS NOT NULL AND assessed_value > 0"
        ).fetchone()[0]
        has_coords = conn.execute(
            "SELECT COUNT(*) FROM parcels WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
        ).fetchone()[0]
        has_legal = conn.execute(
            "SELECT COUNT(*) FROM parcels WHERE legal_description IS NOT NULL AND legal_description != ''"
        ).fetchone()[0]

        print(f"With Owner Name: {has_owner} ({total_parcels - has_owner} missing)")
        print(f"With Address: {has_address} ({total_parcels - has_address} missing)")
        print(f"With Assessed Value: {has_value} ({total_parcels - has_value} missing)")
        print(f"With Coordinates: {has_coords} ({total_parcels - has_coords} missing)")
        print(f"With Legal Description: {has_legal} ({total_parcels - has_legal} missing)")

        # Check bulk_parcels table if exists
        has_bulk = conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'bulk_parcels'"
        ).fetchone()[0]
        if has_bulk:
            bulk_count = conn.execute("SELECT COUNT(*) FROM bulk_parcels").fetchone()[0]
            print(f"Bulk Parcels Table: {bulk_count:,} records")

    except Exception as e:
        print(f"Error: {e}")

    # ==========================================================================
    # STEP 3.5: HOMEHARVEST (MLS DATA & PHOTOS)
    # ==========================================================================
    print("\n[STEP 3.5] HOMEHARVEST (MLS DATA & PHOTOS)")
    print("-" * 40)
    try:
        has_hh = conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'home_harvest'"
        ).fetchone()[0]
        if has_hh:
            total_hh = conn.execute("SELECT COUNT(*) FROM home_harvest").fetchone()[0]
            with_photos = conn.execute("""
                SELECT COUNT(*) FROM home_harvest
                WHERE primary_photo IS NOT NULL OR photos IS NOT NULL OR alt_photos IS NOT NULL
            """).fetchone()[0]
            with_price = conn.execute("""
                SELECT COUNT(*) FROM home_harvest WHERE list_price IS NOT NULL
            """).fetchone()[0]
            with_hoa = conn.execute("""
                SELECT COUNT(*) FROM home_harvest WHERE hoa_fee IS NOT NULL AND hoa_fee > 0
            """).fetchone()[0]

            print(f"Total HomeHarvest Records: {total_hh}")
            print(f"With Photos: {with_photos}")
            print(f"With List Price: {with_price}")
            print(f"With HOA Fee: {with_hoa}")

            # Check pipeline flag
            needs_hh = conn.execute(
                "SELECT COUNT(*) FROM auctions WHERE needs_homeharvest_enrichment = TRUE"
            ).fetchone()[0]
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
        needs_tax = conn.execute(
            "SELECT COUNT(*) FROM auctions WHERE needs_tax_check = TRUE"
        ).fetchone()[0]
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
        has_market = conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'market_data'"
        ).fetchone()[0]
        if has_market:
            by_source = conn.execute("""
                SELECT source, COUNT(*) FROM market_data GROUP BY source
            """).fetchall()
            for source, cnt in by_source:
                print(f"  - {source}: {cnt} records")

            unique_parcels = conn.execute(
                "SELECT COUNT(DISTINCT folio) FROM market_data"
            ).fetchone()[0]
            print(f"  Unique Parcels with Market Data: {unique_parcels}")

            needs_market = conn.execute(
                "SELECT COUNT(*) FROM auctions WHERE needs_market_data = TRUE"
            ).fetchone()[0]
            print(f"  Auctions Needing Market Data: {needs_market}")
        else:
            print("  Market Data table not found")

    except Exception as e:
        print(f"  Market Data Error: {e}")

    # FEMA Flood
    try:
        print("\nFEMA Flood:")
        needs_flood = conn.execute(
            "SELECT COUNT(*) FROM auctions WHERE needs_flood_check = TRUE"
        ).fetchone()[0]
        completed_flood = conn.execute(
            "SELECT COUNT(*) FROM auctions WHERE needs_flood_check = FALSE"
        ).fetchone()[0]
        print(f"  Completed Flood Checks: {completed_flood}")
        print(f"  Auctions Needing Flood Check: {needs_flood}")

    except Exception as e:
        print(f"  Flood Error: {e}")

    # Permits
    try:
        print("\nBuilding Permits:")
        has_permits = conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'permits'"
        ).fetchone()[0]
        if has_permits:
            total_permits = conn.execute("SELECT COUNT(*) FROM permits").fetchone()[0]
            unique_parcels = conn.execute(
                "SELECT COUNT(DISTINCT folio) FROM permits"
            ).fetchone()[0]
            print(f"  Total Permit Records: {total_permits}")
            print(f"  Parcels with Permits: {unique_parcels}")

            needs_permits = conn.execute(
                "SELECT COUNT(*) FROM auctions WHERE needs_permit_check = TRUE"
            ).fetchone()[0]
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
        has_docs = conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'documents'"
        ).fetchone()[0]
        if has_docs:
            total_docs = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
            unique_parcels = conn.execute(
                "SELECT COUNT(DISTINCT folio) FROM documents WHERE folio IS NOT NULL"
            ).fetchone()[0]
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

            needs_ori = conn.execute(
                "SELECT COUNT(*) FROM auctions WHERE needs_ori_ingestion = TRUE"
            ).fetchone()[0]
            print(f"  Auctions Needing ORI Ingestion: {needs_ori}")
        else:
            print("  Documents table not found")

    except Exception as e:
        print(f"  Documents Error: {e}")

    # Chain of Title
    try:
        print("\nChain of Title:")
        has_chain = conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'chain_of_title'"
        ).fetchone()[0]
        if has_chain:
            total_entries = conn.execute("SELECT COUNT(*) FROM chain_of_title").fetchone()[0]
            unique_parcels = conn.execute(
                "SELECT COUNT(DISTINCT folio) FROM chain_of_title"
            ).fetchone()[0]
            print(f"  Total Chain Entries: {total_entries}")
            print(f"  Parcels with Chain: {unique_parcels}")

            # Chain depth stats
            chain_depth = conn.execute("""
                SELECT
                    MIN(cnt) as min_depth,
                    AVG(cnt) as avg_depth,
                    MAX(cnt) as max_depth
                FROM (
                    SELECT folio, COUNT(*) as cnt FROM chain_of_title GROUP BY folio
                )
            """).fetchone()
            print(f"  Chain Depth: Min={chain_depth[0]}, Avg={chain_depth[1]:.1f}, Max={chain_depth[2]}")

            # Shallow chains (potential gaps)
            shallow = conn.execute("""
                SELECT COUNT(*) FROM (
                    SELECT folio FROM chain_of_title GROUP BY folio HAVING COUNT(*) < 2
                )
            """).fetchone()[0]
            print(f"  Shallow Chains (<2 entries): {shallow}")

    except Exception as e:
        print(f"  Chain Error: {e}")

    # Sales History
    try:
        print("\nSales History (HCPA GIS):")
        has_sales = conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'sales_history'"
        ).fetchone()[0]
        if has_sales:
            total_sales = conn.execute("SELECT COUNT(*) FROM sales_history").fetchone()[0]
            unique_parcels = conn.execute(
                "SELECT COUNT(DISTINCT folio) FROM sales_history"
            ).fetchone()[0]
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
        has_enc = conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'encumbrances'"
        ).fetchone()[0]
        if has_enc:
            total_enc = conn.execute("SELECT COUNT(*) FROM encumbrances").fetchone()[0]
            unique_parcels = conn.execute(
                "SELECT COUNT(DISTINCT folio) FROM encumbrances"
            ).fetchone()[0]
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
            satisfied = conn.execute(
                "SELECT COUNT(*) FROM encumbrances WHERE is_satisfied = TRUE"
            ).fetchone()[0]
            open_enc = conn.execute(
                "SELECT COUNT(*) FROM encumbrances WHERE is_satisfied = FALSE OR is_satisfied IS NULL"
            ).fetchone()[0]
            print(f"Satisfied: {satisfied} | Open: {open_enc}")

            needs_survival = conn.execute(
                "SELECT COUNT(*) FROM auctions WHERE needs_lien_survival = TRUE"
            ).fetchone()[0]
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

        total = conn.execute("SELECT COUNT(*) FROM auctions").fetchone()[0]
        print(f"\n{'Step':<25} {'Pending':>10} {'Complete':>10} {'% Done':>10}")
        print("-" * 55)

        for col, name in steps:
            try:
                pending = conn.execute(
                    f"SELECT COUNT(*) FROM auctions WHERE {col} = TRUE"
                ).fetchone()[0]
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
        orphan_auctions = conn.execute("""
            SELECT COUNT(*) FROM auctions a
            LEFT JOIN parcels p ON a.parcel_id = p.folio OR a.folio = p.folio
            WHERE p.folio IS NULL AND a.parcel_id IS NOT NULL
        """).fetchone()[0]
        print(f"Auctions without matching Parcel: {orphan_auctions}")

        # HCPA scrape failures
        hcpa_failed = conn.execute(
            "SELECT COUNT(*) FROM auctions WHERE hcpa_scrape_failed = TRUE"
        ).fetchone()[0]
        print(f"HCPA Scrape Failures: {hcpa_failed}")

        # Invalid parcel IDs (calculated based on validation rules)
        invalid_parcels = conn.execute("""
            SELECT COUNT(*) FROM auctions
            WHERE parcel_id IS NULL
               OR parcel_id = ''
               OR LOWER(parcel_id) IN ('n/a', 'none', 'unknown', 'property appraiser')
               OR LENGTH(parcel_id) < 6
        """).fetchone()[0]
        print(f"Auctions with Invalid Parcel ID: {invalid_parcels}")

        # Missing critical data for analysis
        missing_for_analysis = conn.execute("""
            SELECT COUNT(*) FROM auctions a
            LEFT JOIN parcels p ON a.parcel_id = p.folio OR a.folio = p.folio
            WHERE (p.assessed_value IS NULL OR p.assessed_value = 0)
              AND a.auction_date >= CURRENT_DATE
        """).fetchone()[0]
        print(f"Upcoming Auctions Missing Assessed Value: {missing_for_analysis}")

    except Exception as e:
        print(f"Error: {e}")

    print("\n" + "=" * 60)
    print("         AUDIT COMPLETE")
    print("=" * 60)

    conn.close()


if __name__ == "__main__":
    audit_database()
