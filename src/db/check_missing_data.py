import sqlite3
from pathlib import Path

from src.db.sqlite_paths import resolve_sqlite_db_path_str

DB_PATH = resolve_sqlite_db_path_str()


def analyze_missing_data():
    if not Path(DB_PATH).exists():
        print(f"Database not found at {DB_PATH}")
        return

    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row

        print("Analyzing 'auctions' table...")
        auctions_count = con.execute("SELECT COUNT(*) FROM auctions").fetchone()[0]
        print(f"Total Auctions: {auctions_count}")

        # Get column-level null counts for auctions
        cols = [row[1] for row in con.execute("PRAGMA table_info(auctions)").fetchall()]
        for col in cols:
            null_count = con.execute(
                f"SELECT COUNT(*) FROM auctions WHERE [{col}] IS NULL"
            ).fetchone()[0]
            if null_count > 0:
                print(f"  {col}: {null_count} NULL")
        print()

        print("Analyzing 'parcels' table...")
        parcels_count = con.execute("SELECT COUNT(*) FROM parcels").fetchone()[0]
        print(f"Total Parcels: {parcels_count}")

        cols = [row[1] for row in con.execute("PRAGMA table_info(parcels)").fetchall()]
        for col in cols:
            null_count = con.execute(
                f"SELECT COUNT(*) FROM parcels WHERE [{col}] IS NULL"
            ).fetchone()[0]
            if null_count > 0:
                print(f"  {col}: {null_count} NULL")
        print()

        # Check liens table if it exists
        has_liens = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='liens' LIMIT 1"
        ).fetchone()
        if has_liens:
            print("Analyzing 'liens' table...")
            liens_count = con.execute("SELECT COUNT(*) FROM liens").fetchone()[0]
            print(f"Total Liens: {liens_count}")

            cols = [row[1] for row in con.execute("PRAGMA table_info(liens)").fetchall()]
            for col in cols:
                null_count = con.execute(
                    f"SELECT COUNT(*) FROM liens WHERE [{col}] IS NULL"
                ).fetchone()[0]
                if null_count > 0:
                    print(f"  {col}: {null_count} NULL")
            print()

        # Specific checks
        print("--- Specific Data Quality Checks ---")

        def get_count(query: str) -> int:
            result = con.execute(query).fetchone()
            return result[0] if result else 0

        # Check for missing Final Judgment Content
        missing_fj = get_count("SELECT COUNT(*) FROM auctions WHERE final_judgment_content IS NULL OR final_judgment_content = ''")
        print(f"Auctions missing Final Judgment OCR: {missing_fj} / {auctions_count}")

        # Check for missing Market Analysis Content
        missing_ma = get_count("SELECT COUNT(*) FROM parcels WHERE market_analysis_content IS NULL OR market_analysis_content = ''")
        print(f"Parcels missing Market Analysis OCR: {missing_ma} / {parcels_count}")

        # Check for missing Images
        missing_img = get_count("SELECT COUNT(*) FROM parcels WHERE image_url IS NULL OR image_url = ''")
        print(f"Parcels missing Image URL: {missing_img} / {parcels_count}")

        # Check for missing Property Specs (Beds/Baths/Year)
        missing_specs = get_count("SELECT COUNT(*) FROM parcels WHERE beds IS NULL OR baths IS NULL OR year_built IS NULL")
        print(f"Parcels missing Specs (Beds/Baths/Year): {missing_specs} / {parcels_count}")

        con.close()

    except Exception as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    analyze_missing_data()
