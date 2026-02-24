import requests

url = 'https://publicaccess.hillsclerk.com/Public/ORIUtilities/DocumentSearch/api/Search'
headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}

for k in ['Instrument', 'InstrumentNumber', 'InstrumentNum']:
    res = requests.post(url, json={k: '2022527083'}, headers=headers)
    print(k, res.status_code, len(res.text))

