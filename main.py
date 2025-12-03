import typer
from src.database import DatabaseManager
from src.scrapers.hillsborough_clerk import HillsboroughClerkScraper
from src.models import ScrapeStatus

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

    # Initialize Scraper
    scraper = HillsboroughClerkScraper()
    result = scraper.search(search_value)

    # Handle Results
    if result.status == ScrapeStatus.SUCCESS:
        print(f"✅ Success! Found {len(result.data)} records.")
        for record in result.data:
            # Convert model to dict for DB
            db.save_property(record.model_dump(exclude_none=True))
    elif result.status == ScrapeStatus.NO_RESULTS:
        print(f"⚠️ Search completed, but no results were found for '{search_value}'.")
    elif result.status == ScrapeStatus.BLOCKED:
        print(f"🚫 Access Denied! The scraper was blocked. Message: {result.message}")
    elif result.status == ScrapeStatus.NETWORK_ERROR:
        print(f"🌐 Network Error. Could not reach the site. Message: {result.message}")
    else:
        print(f"❌ An error occurred: {result.status}. Message: {result.message}")
        if result.error_details:
            print("Details written to logs.")

    # Log the attempt (Pseudo-code as DB schema for logs isn't fully set up yet)
    # db.log_scrape_attempt(result)

    db.close()
    print("Script finished.")

if __name__ == "__main__":
    typer.run(main)
