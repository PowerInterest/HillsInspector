import typer

def main():
    """
    Hillsborough County Property Data Acquisition Tool
    """
    print("Welcome to the Hillsborough County Property Data Acquisition Tool!")

if __name__ == "__main__":
    typer.run(main)

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