"""Test fetching document ID from viewer page."""
import requests
import re

session = requests.Session()

# Initialize session
session.get("https://publicaccess.hillsclerk.com/oripublicaccess/", timeout=10)

# Test instrument
instrument = "2024478600"

viewer_url = f"https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html?CQID=320&OBKey__1006_1={instrument}"

print(f"Fetching: {viewer_url}")
response = session.get(viewer_url, timeout=30)
print(f"Status: {response.status_code}")
print(f"Content length: {len(response.text)}")

# Save HTML for inspection
with open("data/temp/viewer_page.html", "w") as f:
    f.write(response.text)
print("Saved to data/temp/viewer_page.html")

# Look for patterns
html = response.text

# Look for document viewing related patterns
patterns = [
    (r'/api/Watermark/([A-Za-z0-9+/=ÉÁ_-]+)', 'Watermark API'),
    (r'documentId["\']?\s*[:=]\s*["\']([^"\']+)["\']', 'documentId'),
    (r'data-id=["\']([^"\']+)["\']', 'data-id'),
    (r'ViewDocument\(([^)]+)\)', 'ViewDocument()'),
    (r'OpenDocument\(([^)]+)\)', 'OpenDocument()'),
    (r'"ID"\s*:\s*"([^"]+)"', 'JSON ID field'),
    (r'imageId\s*[:=]\s*["\']([^"\']+)["\']', 'imageId'),
]

print("\n=== Pattern Matches ===")
for pattern, name in patterns:
    matches = re.findall(pattern, html)
    if matches:
        print(f"{name}: {matches[:3]}")  # First 3 matches

# Also check for any URL with document/image identifiers
url_pattern = r'https?://[^\s"\'<>]+(?:Document|Image|PDF|Watermark)[^\s"\'<>]*'
url_matches = re.findall(url_pattern, html, re.IGNORECASE)
if url_matches:
    print(f"\nDocument URLs found: {len(url_matches)}")
    for url in url_matches[:5]:
        print(f"  {url[:100]}")
