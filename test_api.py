import requests

url = "https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search"
headers = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
}

for k in ["Instrument", "InstrumentNumber", "InstrumentNum", "InstNum"]:
    print(f"Testing {k}...")
    try:
        res = requests.post(url, json={k: "2022527083"}, headers=headers, timeout=10)
        data = res.json()
        print(k, res.status_code, "TotalCount:", data.get("TotalCount", 0))
    except Exception as e:
        print("Error:", e)

# Let's also test PAVDirectSearch with different keywords
pav_url = "https://publicaccess.hillsclerk.com/PAVDirectSearch/api/CustomQuery/KeywordSearch"
for k in ["1006", "Instrument", "InstrumentNumber"]:
    print(f"Testing PAV Keyword {k}...")
    payload = {
        "QueryID": "108",
        "Keywords": [{"KeywordName": k, "KeywordValue": "2022527083"}],
        "MaxRows": 25,
        "SortDir": "desc",
        "SortField": "RecDate",
    }
    try:
        res = requests.post(pav_url, json=payload, headers=headers, timeout=10)
        data = res.json()
        print(f"PAV {k}", res.status_code, "TotalCount:", data.get("TotalCount", 0))
    except Exception as e:
        print("Error:", e)
