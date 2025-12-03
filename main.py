import typer
from src.database import DatabaseManager

def main(
    search_by: str = typer.Option("INSTRUMENT", help="Search criteria: INSTRUMENT or NAME"),
    search_value: str = typer.Option(..., help="The value to search for (e.g., instrument number or name)"),
):
    """
    Hillsborough County Property Data Acquisition Tool
    """
    print("Welcome to the Hillsborough County Property Data Acquisition Tool!")

    db = DatabaseManager()

    print(f"Starting search for {search_by}: {search_value}")

    # Placeholder for actual scraping logic
    # In a real scenario, we would call a scraper from src/scrapers/ here
    # and use the AI client to parse results.

    # Example usage of the DB
    if search_by == "INSTRUMENT":
         # Mock data saving
        mock_data = {
            "folio_number": f"MOCK-{search_value}",
            "owner_name": "JOHN DOE",
            "address": "123 EXAMPLE ST",
            "status": "Found via CLI",
            "data_source": "CLI_MOCK"
        }
        db.save_property(mock_data)

    db.close()
    print("Script finished.")

if __name__ == "__main__":
    typer.run(main)
