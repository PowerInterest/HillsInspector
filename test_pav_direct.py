import requests

pav_url = 'https://publicaccess.hillsclerk.com/PAVDirectSearch/api/CustomQuery/KeywordSearch'
headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0'
}

for inst in ['2004148772', '2022527083']:
    payload = {
        "QueryID": "108",
        "Keywords": [{"KeywordName": "1006", "KeywordValue": inst}],
        "MaxRows": 25,
        "SortDir": "desc",
        "SortField": "RecDate"
    }
    res = requests.post(pav_url, json=payload, headers=headers, timeout=10)
    data = res.json()
    print(inst, "TotalCount:", data.get('TotalCount', 0))
    if data.get('TotalCount'):
        print(f"  ID: {data['Data'][0]['ID']}")
