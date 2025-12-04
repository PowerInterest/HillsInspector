"""
Quick script to check database status and recent ingestions.
"""
import duckdb

def check_status():
    conn = duckdb.connect("../../data/property_master.db")
    
    print("=" * 60)
    print("DATABASE STATUS")
    print("=" * 60)
    
    # Count auctions
    result = conn.execute("SELECT COUNT(*) FROM auctions").fetchone()
    print(f"\nTotal Auctions: {result[0]}")
    
    # Count by type
    result = conn.execute("""
        SELECT auction_type, COUNT(*) 
        FROM auctions 
        GROUP BY auction_type
    """).fetchall()
    print("\nBy Type:")
    for row in result:
        print(f"  {row[0]}: {row[1]}")
    
    # Count parcels
    result = conn.execute("SELECT COUNT(*) FROM parcels").fetchone()
    print(f"\nTotal Parcels: {result[0]}")
    
    # Recent auctions with owner
    result = conn.execute("""
        SELECT a.case_number, a.auction_type, p.owner_name, a.property_address
        FROM auctions a
        LEFT JOIN parcels p ON a.folio = p.folio
        ORDER BY a.created_at DESC
        LIMIT 5
    """).fetchall()
    
    print("\nRecent Auctions:")
    for row in result:
        print(f"  {row[0]} | {row[1]} | {row[2]} | {row[3][:50]}")
    
    # Pending analysis
    result = conn.execute("""
        SELECT COUNT(*) FROM auctions WHERE status = 'PENDING'
    """).fetchone()
    print(f"\nPending Analysis: {result[0]}")
    
    conn.close()

if __name__ == "__main__":
    check_status()
