import requests
import json
import os
import re
from datetime import datetime
import numpy as np
import easyocr
from pdf2image import convert_from_path
import duckdb

class DatabaseManager:
    """Handles all database operations for DuckDB."""
    def __init__(self, db_file='property_records.db'):
        self.db_file = db_file
        self.con = duckdb.connect(database=self.db_file, read_only=False)
        self.create_tables()

    def create_tables(self):
        """Creates the tables for records and property details if they don't exist."""
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS records (
                instrument_id VARCHAR PRIMARY KEY,
                folio_number VARCHAR,
                record_date VARCHAR,
                doc_type VARCHAR,
                grantor VARCHAR,
                grantee VARCHAR,
                book VARCHAR,
                page VARCHAR,
                pdf_text TEXT,
                analysis_summary VARCHAR
            );
        """)
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS properties (
                folio_number VARCHAR PRIMARY KEY,
                property_address VARCHAR,
                market_area VARCHAR,
                neighborhood VARCHAR,
                subdivision VARCHAR,
                last_updated TIMESTAMP
            );
        """)

    def insert_record(self, record_data):
        """Inserts or replaces a document record in the database."""
        self.con.execute("""
            INSERT OR REPLACE INTO records VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, record_data)

    def insert_property_details(self, property_data):
        """Inserts or replaces property details in the database."""
        self.con.execute("""
            INSERT OR REPLACE INTO properties VALUES (?, ?, ?, ?, ?, ?)
        """, property_data)

    def get_summary_by_folio(self, folio):
        """Retrieves and prints a chronological summary for a given folio."""
        print(f"\n--- Chronological History for Folio: {folio} ---")
        
        # Get property details
        prop_details = self.con.execute("SELECT * FROM properties WHERE folio_number = ?", [folio]).fetchone()
        if prop_details:
            print("  Property Details from HCPA:")
            print(f"    Address: {prop_details[1]}")
            print(f"    Market Area: {prop_details[2]}")
            print(f"    Neighborhood: {prop_details[3]}")
            print(f"    Subdivision: {prop_details[4]}\n")
        else:
            print("  No detailed property information found in the database for this folio.\n")

        # Get document records
        records = self.con.execute("""
            SELECT record_date, doc_type, grantor, grantee, analysis_summary
            FROM records
            WHERE folio_number = ?
            ORDER BY record_date
        """, [folio]).fetchall()

        if not records:
            print("  No document records found for this folio.")
            return

        print("  Associated Documents from Clerk of Court:")
        for row in records:
            print(
                f"    Date: {row[0]} | Type: {row[1]}\n"
                f"      Grantor: {row[2]}\n"
                f"      Grantee: {row[3]}\n"
                f"      Summary: {row[4]}\n"
                f"    ----------------------------------------"
            )
    
    def close(self):
        self.con.close()


def search_hcpa_by_folio(folio_number: str, db_manager: DatabaseManager):
    """
    Searches the HCPA website for property details by folio number and stores them.
    """
    if not folio_number or folio_number == 'Not Found':
        return

    print(f"  -> Querying HCPA for details on Folio: {folio_number}")
    # This is the endpoint used by the HCPA property search website.
    url = f"https://gis.hcpafl.org/propertysearch/api/v1/parcel/folio/{folio_number}"
    headers = {'User-Agent': 'Mozilla/5.0'}

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()

        if data and data.get('success'):
            parcel_info = data['result']
            property_data = (
                folio_number,
                parcel_info.get('situs'),
                parcel_info.get('marketAreaDescription'),
                parcel_info.get('neighborhoodDescription'),
                parcel_info.get('subdivisionDescription'),
                datetime.now()
            )
            db_manager.insert_property_details(property_data)
            print("     ... Success. Property details saved to database.")
        else:
            print("     ... Folio not found on HCPA site.")

    except requests.exceptions.RequestException as e:
        print(f"     ... Error querying HCPA API: {e}")


def analyze_document_text(text):
    """Analyzes extracted text to find folio numbers, dollar amounts, and relationships."""
    if not text or text in ["DOCUMENT_UNAVAILABLE", "PROCESSING_ERROR"]:
        return {'folio_number': 'N/A', 'analysis_summary': text or 'No text to analyze.'}

    # Regex for Hillsborough County folio format. This is a best-effort extraction.
    folio_match = re.search(r'([A-Z]-\d{2}-\d{2}-\d{2}-\w{3}-\w{6}-\d{5}\.\d)', text)
    folio_number = folio_match.group(1) if folio_match else 'Not Found'

    dollar_amounts = set(re.findall(r'\$\s?[\d,]+(?:\.\d{2})?', text))
    amounts_str = f"Amounts: {', '.join(dollar_amounts)}" if dollar_amounts else "No amounts found."

    satisfaction_match = re.search(r'satisfaction of.*?mortgage.*?recorded in.*?book\s*(\d+).*?page\s*(\d+)', text, re.IGNORECASE | re.DOTALL)
    relationship_str = f"Satisfies mortgage in Book {satisfaction_match.group(1)}, Page {satisfaction_match.group(2)}." if satisfaction_match else ""

    summary = f"{amounts_str}. {relationship_str}".strip()
    
    return {'folio_number': folio_number, 'analysis_summary': summary}


def download_and_read_pdf(record, ocr_reader):
    """Downloads, converts, and reads the PDF for a given record using EasyOCR."""
    instrument_id = record.get("Instrument")
    pdf_url = f"https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocView/ShowImage.aspx?Book={record.get('Book')}&Page={record.get('Page')}&Instrument={instrument_id}"
    
    print(f"\n--- Processing Instrument ID: {instrument_id} ---")
    
    try:
        response = requests.get(pdf_url, timeout=60)
        response.raise_for_status()
        
        if 'application/pdf' not in response.headers.get('Content-Type', ''):
             print("  - Document is unavailable or sealed.")
             return "DOCUMENT_UNAVAILABLE"

        pdf_path = f"temp_instrument_{instrument_id}.pdf"
        with open(pdf_path, 'wb') as f: f.write(response.content)
        
        images = convert_from_path(pdf_path)
        full_text = ""
        for i, image in enumerate(images):
            print(f"  - Reading page {i + 1} with EasyOCR...")
            np_image = np.array(image)
            results = ocr_reader.readtext(np_image)
            page_text = "\n".join([res[1] for res in results])
            full_text += page_text + "\n\n--- End of Page ---\n\n"
        
        os.remove(pdf_path)
        return full_text

    except requests.exceptions.HTTPError:
        return "DOCUMENT_UNAVAILABLE"
    except Exception as e:
        print(f"  - An error occurred during PDF processing: {e}")
        if 'pdf_path' in locals() and os.path.exists(pdf_path): os.remove(pdf_path)
        return "PROCESSING_ERROR"


def run_search(search_by: str, search_value: str, db_manager: DatabaseManager, ocr_reader):
    """Runs a search against the Clerk's API and processes the results."""
    api_search_by_map = {"NAME": "NAME", "LEGAL": "LEGAL", "INSTRUMENT": "CFN"}
    if search_by.upper() not in api_search_by_map:
        print(f"Error: Invalid search type '{search_by}'. Use NAME, LEGAL, or INSTRUMENT.")
        return

    api_search_by = api_search_by_map[search_by.upper()]
    url = "https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search"
    headers = {'Content-Type': 'application/json; charset=UTF-8', 'User-Agent': 'Mozilla/5.0'}
    real_estate_doc_types = [
        "AGD", "ASG", "CCJ", "D", "EAS", "JUD", "LN", "LP", "MEDLN",
        "MOD", "MTG", "NOC", "NCL", "PR", "REL", "RELLP", "SAT", "TAXDEED", "TRA"
    ]
    payload = {
        "SearchParms": search_value, "SearchBy": api_search_by, "IsSoundEx": False,
        "DocTypeGroups": [], "DocTypes": real_estate_doc_types if api_search_by != "CFN" else [],
        "RecDateFrom": "01/01/1950", "RecDateTo": datetime.now().strftime("%m/%d/%Y"),
    }

    print(f"Searching Clerk records by {search_by} for: '{search_value}'...")
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        search_results = response.json()

        if not search_results:
            print("\nNo results found from Clerk.")
            return

        print(f"\nFound {len(search_results)} records. Processing documents...")
        
        for record in search_results:
            text = download_and_read_pdf(record, ocr_reader)
            analysis = analyze_document_text(text)
            
            db_manager.insert_record((
                record.get("Instrument"), analysis['folio_number'], record.get('RecDate'),
                record.get('DocTypeDescription'), record.get('Grantor'), record.get('Grantee'),
                record.get('Book'), record.get('Page'), text, analysis['analysis_summary']
            ))
            
            # Now, use the extracted folio to search HCPA
            search_hcpa_by_folio(analysis['folio_number'], db_manager)

        print("\nAll records processed and saved to the database.")

    except Exception as e:
        print(f"An error occurred during search: {e}")

# --- Main execution ---
if __name__ == "__main__":
    db = DatabaseManager()
    
    # Check environment variable for GPU usage (defaults to True if not set, but Dockerfile sets it to False)
    use_gpu = os.environ.get('EASYOCR_GPU', 'True').lower() == 'true'
    print(f"Initializing EasyOCR Reader (GPU={use_gpu})...")
    reader = easyocr.Reader(['en'], gpu=use_gpu)
    
    print("EasyOCR Reader initialized.")

    # --- Example Searches ---
    # 1. Search Clerk's office by Instrument Number
    run_search(search_by="INSTRUMENT", search_value="2025120873", db_manager=db, ocr_reader=reader)

    # 2. Search Clerk's office by Name
    # run_search(search_by="NAME", search_value="DUCK HOLDINGS LLC", db_manager=db, ocr_reader=reader)

    # --- Example Analysis ---
    # After running a search, you can get a combined summary for a folio you discovered
    # The folio number below was found in the example document.
    discovered_folio = "U-11-28-19-123-A00001-00001.0" 
    db.get_summary_by_folio(discovered_folio)

    db.close()
    print("\nScript finished.")