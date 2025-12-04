"""Quick script to search ORI for a specific lot/block."""
import requests
import json
from datetime import datetime

ORI_SEARCH_URL = 'https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search'
HEADERS = {
    'Content-Type': 'application/json; charset=UTF-8',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Origin': 'https://publicaccess.hillsclerk.com',
    'Referer': 'https://publicaccess.hillsclerk.com/oripublicaccess/',
    'X-Requested-With': 'XMLHttpRequest',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
}

session = requests.Session()
session.get('https://publicaccess.hillsclerk.com/oripublicaccess/')

# Search for all doc types for L 9 B 12 MUNRO
all_doc_types = [
    '(D) DEED', '(MTG) MORTGAGE', '(LN) LIEN', '(LP) LIS PENDENS',
    '(SAT) SATISFACTION', '(REL) RELEASE', '(ASG) ASSIGNMENT',
    '(JUD) JUDGMENT', '(NOC) NOTICE OF COMMENCEMENT'
]

payload = {
    'DocType': all_doc_types,
    'RecordDateBegin': '01/01/1900',
    'RecordDateEnd': '11/26/2025',
    'Legal': ['CONTAINS', 'L 9 B 12 MUNRO'],
}

response = session.post(ORI_SEARCH_URL, headers=HEADERS, json=payload, timeout=60)
data = response.json()
results = data.get('ResultList', [])

print(f'Found {len(results)} documents for L 9 B 12 MUNRO:')
print()

for doc in results:
    dt = datetime.fromtimestamp(doc.get('RecordDate', 0)).strftime('%Y-%m-%d')
    print(doc.get('DocType'))
    print(f"  Date: {dt} | Instrument: {doc.get('Instrument')}")
    print(f"  Legal: {doc.get('Legal')}")
    print(f"  From: {', '.join(doc.get('PartiesOne', []))}")
    print(f"  To: {', '.join(doc.get('PartiesTwo', []))}")
    if doc.get('SalesPrice'):
        print(f"  Sale Price: ${doc.get('SalesPrice'):,.0f}")
    print()
