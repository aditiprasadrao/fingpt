import requests, json, sys
from datetime import datetime, timezone, timedelta
subreddit = "CryptoCurrency"
# fetch last 24 hours
after = int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp())
url = "https://api.pushshift.io/reddit/search/submission/"
params = {"subreddit": subreddit, "size": 100, "after": after, "sort": "asc"}
print("Querying Pushshift:", url, params)
r = requests.get(url, params=params, timeout=15)
print("HTTP status:", r.status_code)
try:
    data = r.json().get("data", [])
    print("Number of posts returned:", len(data))
    if len(data) > 0:
        print("Sample (first):")
        print(json.dumps(data[0], indent=2)[:1000])
except Exception as e:
    print("Failed to parse JSON or no data:", e)
