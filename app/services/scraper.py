import requests
from typing import Optional, Dict, Any

class HCPAScraper:
    BASE_URL = "https://gis.hcpafl.org/propertysearch/api/v1/parcel"

    def get_parcel_by_folio(self, folio: str) -> Optional[Dict[str, Any]]:
        """Fetch property details from HCPA by folio number."""
        url = f"{self.BASE_URL}/folio/{folio}"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data.get("success"):
                return data.get("result")
        except Exception as e:
            print(f"Error fetching HCPA data for {folio}: {e}")
        return None

class ClerkScraper:
    SEARCH_URL = "https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search"

    def search_documents(self, query: str, start_date: str, end_date: str) -> list:
        """
        Search for documents in the Clerk's office.
        Note: This API often requires specific headers or cookies. 
        If simple requests fail, we may need to switch to Playwright.
        """
        payload = {
            "SearchValue": query,
            "StartDate": start_date,
            "EndDate": end_date,
            "DocTypes": [], # Empty list usually means all or default
            "MaxRows": 100
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Content-Type": "application/json"
        }
        try:
            response = requests.post(self.SEARCH_URL, json=payload, headers=headers, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error searching Clerk documents: {e}")
            return []
