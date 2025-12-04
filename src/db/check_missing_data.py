import duckdb
from pathlib import Path
import pandas as pd

DB_PATH = Path("../../data/property_master.db")

def analyze_missing_data():
    try:
        con = duckdb.connect(str(DB_PATH))
        
        print("Analyzing 'auctions' table...")
        auctions_df = con.execute("SELECT * FROM auctions").fetchdf()
        print(f"Total Auctions: {len(auctions_df)}")
        print(auctions_df.isnull().sum())
        print("\n")

        print("Analyzing 'parcels' table...")
        parcels_df = con.execute("SELECT * FROM parcels").fetchdf()
        print(f"Total Parcels: {len(parcels_df)}")
        print(parcels_df.isnull().sum())
        print("\n")
        
        print("Analyzing 'liens' table...")
        liens_df = con.execute("SELECT * FROM liens").fetchdf()
        print(f"Total Liens: {len(liens_df)}")
        print(liens_df.isnull().sum())
        print("\n")
        
        # Specific checks
        print("--- Specific Data Quality Checks ---")
        
        # Check for missing Final Judgment Content
        missing_fj = con.execute("SELECT COUNT(*) FROM auctions WHERE final_judgment_content IS NULL OR final_judgment_content = ''").fetchone()[0]
        print(f"Auctions missing Final Judgment OCR: {missing_fj} / {len(auctions_df)}")
        
        # Check for missing Market Analysis Content
        missing_ma = con.execute("SELECT COUNT(*) FROM parcels WHERE market_analysis_content IS NULL OR market_analysis_content = ''").fetchone()[0]
        print(f"Parcels missing Market Analysis OCR: {missing_ma} / {len(parcels_df)}")
        
        # Check for missing Images
        missing_img = con.execute("SELECT COUNT(*) FROM parcels WHERE image_url IS NULL OR image_url = ''").fetchone()[0]
        print(f"Parcels missing Image URL: {missing_img} / {len(parcels_df)}")
        
        # Check for missing Property Specs (Beds/Baths/Year)
        missing_specs = con.execute("SELECT COUNT(*) FROM parcels WHERE beds IS NULL OR baths IS NULL OR year_built IS NULL").fetchone()[0]
        print(f"Parcels missing Specs (Beds/Baths/Year): {missing_specs} / {len(parcels_df)}")

        con.close()
        
    except Exception as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    analyze_missing_data()
