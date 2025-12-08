"""Test API legal search to get document IDs."""
import requests
from datetime import datetime
import json

SEARCH_URL = "https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search"

HEADERS = {
    "Content-Type": "application/json; charset=UTF-8",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Origin": "https://publicaccess.hillsclerk.com",
    "Referer": "https://publicaccess.hillsclerk.com/oripublicaccess/",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

TITLE_DOC_TYPES = [
    "(MTG) MORTGAGE",
    "(D) DEED",
    "(LN) LIEN",
    "(LP) LIS PENDENS",
    "(SAT) SATISFACTION",
]

session = requests.Session()
# Initialize session
print("Initializing session...")
init_resp = session.get("https://publicaccess.hillsclerk.com/oripublicaccess/", timeout=10)
print(f"Init status: {init_resp.status_code}")

# Test with WESTCHASE
legal_desc = "WESTCHASE"
payload = {
    "DocType": TITLE_DOC_TYPES,
    "RecordDateBegin": "01/01/2024",
    "RecordDateEnd": datetime.now().strftime("%m/%d/%Y"),
    "Legal": ["CONTAINS", legal_desc],
}

print(f"\nPayload: {json.dumps(payload, indent=2)}")
print(f"\nSearching...")

response = session.post(SEARCH_URL, headers=HEADERS, json=payload, timeout=60)
print(f"Status: {response.status_code}")

if response.status_code == 200:
    data = response.json()
    results = data.get("ResultList", [])
    print(f"Results: {len(results)}")
    if results:
        print(f"\nFirst result: {json.dumps(results[0], indent=2)}")
else:
    print(f"Response: {response.text[:500]}")

# Also try the simpler test that worked before
print("\n=== Retry with specific date that worked ===")
payload2 = {
    "DocType": ["(D) DEED"],
    "RecordDateBegin": "11/25/2024",
    "RecordDateEnd": "11/25/2024",
    "Legal": ["CONTAINS", "WESTCHASE"],
}
print(f"Payload: {json.dumps(payload2)}")

response2 = session.post(SEARCH_URL, headers=HEADERS, json=payload2, timeout=60)
print(f"Status: {response2.status_code}")
if response2.status_code == 200:
    data2 = response2.json()
    results2 = data2.get("ResultList", [])
    print(f"Results: {len(results2)}")
    if results2:
        print(f"First ID: {results2[0].get('ID', 'NONE')}")
        print(f"Instrument: {results2[0].get('Instrument')}")
