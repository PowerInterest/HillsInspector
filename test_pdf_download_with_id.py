"""Test PDF download with API-obtained ID."""
import requests
from urllib.parse import quote
from pathlib import Path

session = requests.Session()
session.get("https://publicaccess.hillsclerk.com/oripublicaccess/", timeout=10)

# ID from the API test (for instrument 2024489646)
doc_id = "AV6VGDkGÁJhSWo8F65afyfrXWvlOeOR1SkHQ0t5f2HMhNjÉMki53VxqZwkOmTSmeGfqTÉJJ4dLiPBEqkÉuPFg3Q="

PDF_URL = "https://publicaccess.hillsclerk.com/Public/ORIUtilities/OverlayWatermark/api/Watermark"

HEADERS = {
    "Accept": "application/pdf,*/*",
    "Origin": "https://publicaccess.hillsclerk.com",
    "Referer": "https://publicaccess.hillsclerk.com/oripublicaccess/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

pdf_url = f"{PDF_URL}/{quote(doc_id)}"
print(f"Downloading from: {pdf_url}")

response = session.get(pdf_url, headers=HEADERS, timeout=30)
print(f"Status: {response.status_code}")
print(f"Content-Type: {response.headers.get('Content-Type', 'unknown')}")
print(f"Content length: {len(response.content)} bytes")

if response.status_code == 200:
    # Check if it's a PDF
    if response.content[:4] == b"%PDF":
        output_dir = Path("data/temp/test_downloads")
        output_dir.mkdir(parents=True, exist_ok=True)
        filepath = output_dir / "test_deed_2024489646.pdf"
        with open(filepath, "wb") as f:
            f.write(response.content)
        print(f"SUCCESS: Saved to {filepath}")
        print(f"File size: {filepath.stat().st_size} bytes")
    else:
        print(f"Not a PDF! First 100 bytes: {response.content[:100]}")
else:
    print(f"Error response: {response.text[:500]}")
