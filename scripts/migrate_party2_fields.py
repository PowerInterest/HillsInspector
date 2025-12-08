"""
Migration script to add Party 2 resolution tracking fields to existing databases.

Run this once to update the schema without losing data.
"""
import duckdb
from pathlib import Path


def migrate_database(db_path: str = "data/property_master.db"):
    """Add Party 2 resolution fields to existing tables."""

    if not Path(db_path).exists():
        print(f"Database not found: {db_path}")
        return

    conn = duckdb.connect(db_path)

    # Check what columns exist in documents table
    columns = conn.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'documents'
    """).fetchall()
    existing_cols = {col[0] for col in columns}

    print(f"Existing documents columns: {existing_cols}")

    # Add missing columns to documents table
    documents_new_cols = [
        ("party2_resolution_method", "VARCHAR"),
        ("is_self_transfer", "BOOLEAN DEFAULT FALSE"),
        ("self_transfer_type", "VARCHAR"),
        ("party2_confidence", "FLOAT DEFAULT 1.0"),
        ("party2_resolved_at", "TIMESTAMP"),
    ]

    for col_name, col_type in documents_new_cols:
        if col_name not in existing_cols:
            print(f"Adding documents.{col_name}...")
            conn.execute(f"ALTER TABLE documents ADD COLUMN {col_name} {col_type}")
        else:
            print(f"Column documents.{col_name} already exists")

    # Check what columns exist in encumbrances table
    columns = conn.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'encumbrances'
    """).fetchall()
    existing_cols = {col[0] for col in columns}

    print(f"\nExisting encumbrances columns: {existing_cols}")

    # Add missing columns to encumbrances table
    encumbrances_new_cols = [
        ("debtor", "VARCHAR"),
        ("party2_resolution_method", "VARCHAR"),
        ("is_self_transfer", "BOOLEAN DEFAULT FALSE"),
        ("self_transfer_type", "VARCHAR"),
    ]

    for col_name, col_type in encumbrances_new_cols:
        if col_name not in existing_cols:
            print(f"Adding encumbrances.{col_name}...")
            conn.execute(f"ALTER TABLE encumbrances ADD COLUMN {col_name} {col_type}")
        else:
            print(f"Column encumbrances.{col_name} already exists")

    conn.close()
    print("\nMigration complete!")


if __name__ == "__main__":
    migrate_database()
