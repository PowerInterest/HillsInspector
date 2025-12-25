"""Fix sequences after data migration."""

import duckdb


def fix_sequences():
    """Update all sequences to start after max IDs in tables."""

    # Fix V1 sequences
    print("=== Fixing V1 Database Sequences ===")
    conn = duckdb.connect("data/property_master.db")

    sequence_table_map = [
        ("seq_auctions_id", "auctions", "id"),
        ("seq_liens_id", "liens", "id"),
        ("seq_permits_id", "permits", "id"),
        ("seq_documents_id", "documents", "id"),
        ("seq_analysis_id", "analysis_results", "id"),
        ("sales_history_seq", "sales_history", "id"),
        ("chain_of_title_seq", "chain_of_title", "id"),
        ("encumbrances_seq", "encumbrances", "id"),
        ("market_data_id_seq", "market_data", "id"),
        ("homeharvest_id_seq", "home_harvest", "id"),
    ]

    for seq_name, table, col in sequence_table_map:
        try:
            check = conn.execute(
                f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table}' LIMIT 1"
            ).fetchone()
            if not check:
                continue

            max_id = conn.execute(f"SELECT COALESCE(MAX({col}), 0) FROM {table}").fetchone()[0]
            if max_id > 0:
                conn.execute(f"DROP SEQUENCE IF EXISTS {seq_name}")
                conn.execute(f"CREATE SEQUENCE {seq_name} START {max_id + 1}")
                print(f"  {seq_name}: set to start at {max_id + 1}")
        except Exception as e:
            print(f"  {seq_name}: ERROR - {e}")

    conn.execute("CHECKPOINT")
    conn.close()

    # Fix V2 sequences
    print()
    print("=== Fixing V2 Database Sequences ===")
    conn = duckdb.connect("data/property_master_v2.db")

    v2_sequence_table_map = [
        ("seq_auctions_id", "auctions", "id"),
        ("seq_liens_id", "liens", "id"),
        ("seq_permits_id", "permits", "id"),
        ("seq_documents_id", "documents", "id"),
        ("seq_analysis_id", "analysis_results", "id"),
        ("sales_history_seq", "sales_history", "id"),
        ("chain_of_title_seq", "chain_of_title", "id"),
        ("encumbrances_seq", "encumbrances", "id"),
        ("market_data_id_seq", "market_data", "id"),
        ("homeharvest_id_seq", "home_harvest", "id"),
        ("scraper_outputs_id_seq", "scraper_outputs", "id"),
        ("legal_variations_seq", "legal_variations", "id"),
        ("property_parties_seq", "property_parties", "id"),
        ("linked_identities_seq", "linked_identities", "id"),
        ("ori_search_queue_seq", "ori_search_queue", "id"),
    ]

    for seq_name, table, col in v2_sequence_table_map:
        try:
            check = conn.execute(
                f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table}' LIMIT 1"
            ).fetchone()
            if not check:
                continue

            max_id = conn.execute(f"SELECT COALESCE(MAX({col}), 0) FROM {table}").fetchone()[0]
            if max_id > 0:
                conn.execute(f"DROP SEQUENCE IF EXISTS {seq_name}")
                conn.execute(f"CREATE SEQUENCE {seq_name} START {max_id + 1}")
                print(f"  {seq_name}: set to start at {max_id + 1}")
        except Exception as e:
            print(f"  {seq_name}: ERROR - {e}")

    conn.execute("CHECKPOINT")
    conn.close()

    print()
    print("Sequences fixed!")


if __name__ == "__main__":
    fix_sequences()
