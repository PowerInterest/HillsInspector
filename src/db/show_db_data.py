import sys
sys.path.insert(0, '../../src')
from src.db.operations import PropertyDB

# Write to file to avoid console encoding issues
output_file = open("database_report.txt", "w", encoding="utf-8")

def print_to_file(text=""):
    output_file.write(text + "\n")
    print(text)  # Also print to console

try:
    print_to_file("\n" + "=" * 80)
    print_to_file("HILLSBOROUGH COUNTY PROPERTY DATABASE - CURRENT DATA")
    print_to_file("=" * 80 + "\n")
    
    with PropertyDB(db_path="../../data/property_master.db") as db:
        conn = db.connect()
        
        # Get all auctions
        auctions = conn.execute("""
            SELECT * FROM auctions ORDER BY auction_date DESC LIMIT 10
        """).fetchall()
        
        print_to_file(f"AUCTIONS: {len(auctions)} records (showing first 10)\n")
        
        if auctions:
            columns = [desc[0] for desc in conn.description]
            print_to_file("Recent Auctions:")
            print_to_file("-" * 80)
            
            for i, row in enumerate(auctions, 1):
                data = dict(zip(columns, row))
                print_to_file(f"\n{i}. {data.get('auction_type', 'Unknown')} - Case: {data.get('case_number', 'N/A')}")
                print_to_file(f"   Date: {data.get('auction_date', 'N/A')}")
                print_to_file(f"   Address: {data.get('property_address', 'N/A')}")
                print_to_file(f"   Parcel: {data.get('parcel_id', 'N/A')}")
                if data.get('final_judgment_amount'):
                    print_to_file(f"   Final Judgment: ${data['final_judgment_amount']:,.2f}")
                if data.get('assessed_value'):
                    print_to_file(f"   Assessed Value: ${data['assessed_value']:,.2f}")
                print_to_file(f"   Status: {data.get('status', 'N/A')}")
        
        # Get parcels
        parcels = conn.execute("""
            SELECT * FROM parcels LIMIT 10
        """).fetchall()
        
        print_to_file(f"\nPARCELS (Enriched): {len(parcels)} records (showing first 10)\n")
        
        if parcels:
            columns = [desc[0] for desc in conn.description]
            print_to_file("Enriched Parcel Data:")
            print_to_file("-" * 80)
            
            for i, row in enumerate(parcels, 1):
                data = dict(zip(columns, row))
                print_to_file(f"\n{i}. Parcel: {data.get('folio', 'N/A')}")
                print_to_file(f"   Address: {data.get('property_address', 'N/A')}")
                print_to_file(f"   Owner: {data.get('owner_name', 'Not enriched')}")
                if data.get('year_built'):
                    print_to_file(f"   Built: {data['year_built']}")
                if data.get('beds') or data.get('baths'):
                    print_to_file(f"   Beds/Baths: {data.get('beds', 'N/A')}/{data.get('baths', 'N/A')}")
                if data.get('heated_area'):
                    print_to_file(f"   Heated Area: {data['heated_area']} sq ft")
        
        # Get statistics
        stats = conn.execute("""
            SELECT 
                COUNT(*) as total_auctions,
                COUNT(DISTINCT parcel_id) as unique_parcels,
                AVG(assessed_value) as avg_assessed_value,
                SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) as pending_analysis
            FROM auctions
        """).fetchone()
        
        print_to_file("\n" + "=" * 80)
        print_to_file("STATISTICS:")
        print_to_file(f"  Total Auctions: {stats[0]}")
        print_to_file(f"  Unique Parcels: {stats[1]}")
        if stats[2]:
            print_to_file(f"  Average Assessed Value: ${stats[2]:,.2f}")
        print_to_file(f"  Pending Analysis: {stats[3]}")
        print_to_file("=" * 80 + "\n")
        
        print_to_file("\nFull report written to database_report.txt")

except Exception as e:
    print_to_file(f"Error: {e}")
    import traceback
    traceback.print_exc(file=output_file)
finally:
    output_file.close()
