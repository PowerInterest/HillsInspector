"""Debug ORI API payloads."""
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
    "(ASG) ASSIGNMENT",
]

session = requests.Session()
# Initialize session
session.get("https://publicaccess.hillsclerk.com/oripublicaccess/", timeout=10)

# Test different payload formats
test_payloads = [
    # As list like Legal
    {
        "DocType": TITLE_DOC_TYPES,
        "RecordDateBegin": "01/01/2024",
        "RecordDateEnd": "12/31/2024",
        "Instrument": ["EQUALS", 2024478600],
    },

    # As list with string
    {
        "DocType": TITLE_DOC_TYPES,
        "RecordDateBegin": "01/01/2024",
        "RecordDateEnd": "12/31/2024",
        "Instrument": ["EQUALS", "2024478600"],
    },

    # Try InstrumentBegin/InstrumentEnd range
    {
        "DocType": TITLE_DOC_TYPES,
        "RecordDateBegin": "01/01/2024",
        "RecordDateEnd": "12/31/2024",
        "InstrumentBegin": 2024478600,
        "InstrumentEnd": 2024478600,
    },

    # Try with very open search first to see what works
    {
        "DocType": ["(D) DEED"],
        "RecordDateBegin": "11/25/2024",
        "RecordDateEnd": "11/25/2024",
        "Legal": ["CONTAINS", "WESTCHASE"],
    },
]

for i, payload in enumerate(test_payloads):
    print(f"\n=== Test {i+1}: {json.dumps(payload)[:200]}")
    try:
        response = session.post(SEARCH_URL, headers=HEADERS, json=payload, timeout=30)
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Results: {len(data.get('ResultList', []))}")
            if data.get('ResultList'):
                first = data['ResultList'][0]
                print(f"First ID: {first.get('ID')}")
                print(f"Instrument: {first.get('Instrument')}")
                print(f"DocType: {first.get('DocType')}")
                print(f"RecordDate: {first.get('RecordDate')}")
        else:
            print(f"Response: {response.text[:300]}")
    except Exception as e:
        print(f"Error: {e}")
