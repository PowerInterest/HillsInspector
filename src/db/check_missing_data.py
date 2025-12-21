import duckdb
from pathlib import Path

DB_PATH = Path("../../data/property_master.db")

def analyze_missing_data():
    try:
        con = duckdb.connect(str(DB_PATH))
        
        print("Analyzing 'auctions' table...")
        auctions_df = con.execute("SELECT * FROM auctions").fetchdf()
        print(f"Total Auctions: {len(auctions_df)}")
        print(auctions_df.isna().sum())
        print("\n")

        print("Analyzing 'parcels' table...")
        parcels_df = con.execute("SELECT * FROM parcels").fetchdf()
        print(f"Total Parcels: {len(parcels_df)}")
        print(parcels_df.isna().sum())
        print("\n")
        
        print("Analyzing 'liens' table...")
        liens_df = con.execute("SELECT * FROM liens").fetchdf()
        print(f"Total Liens: {len(liens_df)}")
        print(liens_df.isna().sum())
        print("\n")
        
        # Specific checks
        print("--- Specific Data Quality Checks ---")

        def get_count(query: str) -> int:
            result = con.execute(query).fetchone()
            return result[0] if result else 0

        # Check for missing Final Judgment Content
        missing_fj = get_count("SELECT COUNT(*) FROM auctions WHERE final_judgment_content IS NULL OR final_judgment_content = ''")
        print(f"Auctions missing Final Judgment OCR: {missing_fj} / {len(auctions_df)}")

        # Check for missing Market Analysis Content
        missing_ma = get_count("SELECT COUNT(*) FROM parcels WHERE market_analysis_content IS NULL OR market_analysis_content = ''")
        print(f"Parcels missing Market Analysis OCR: {missing_ma} / {len(parcels_df)}")

        # Check for missing Images
        missing_img = get_count("SELECT COUNT(*) FROM parcels WHERE image_url IS NULL OR image_url = ''")
        print(f"Parcels missing Image URL: {missing_img} / {len(parcels_df)}")

        # Check for missing Property Specs (Beds/Baths/Year)
        missing_specs = get_count("SELECT COUNT(*) FROM parcels WHERE beds IS NULL OR baths IS NULL OR year_built IS NULL")
        print(f"Parcels missing Specs (Beds/Baths/Year): {missing_specs} / {len(parcels_df)}")

        con.close()
        
    except Exception as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    analyze_missing_data()
