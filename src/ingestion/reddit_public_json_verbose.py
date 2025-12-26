import time, requests, sys
from datetime import datetime, timezone
from src.processing.sentiment import score_text
from src.storage import db as dbmod

SUBREDDITS = ["CryptoCurrency", "Bitcoin", "ethereum"]
HEADERS = {\"User-Agent\": \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36\"}
LIMIT = 50

def fetch_new(subreddit, after_utc=None):
    url = f"https://www.reddit.com/r/{subreddit}/new.json"
    params = {"limit": LIMIT}
    print(f"REQUEST -> {url} params={params} headers={HEADERS}")
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
        print("HTTP status:", resp.status_code)
        if resp.status_code != 200:
            print("Response text (first 300 chars):", resp.text[:300])
            return []
        data = resp.json().get("data", {}).get("children", [])
        posts = []
        for ch in data:
            d = ch.get("data", {})
            created = d.get("created_utc")
            if after_utc and created <= after_utc:
                continue
            posts.append(d)
        return posts
    except Exception as e:
        print("Fetch exception:", e)
        return []

def get_latest_ts_from_db():
    try:
        with dbmod.engine.connect() as conn:
            row = conn.execute(dbmod.reddit_posts.select().order_by(dbmod.reddit_posts.c.created_utc.desc()).limit(1)).first()
            if row:
                ts = row._mapping['created_utc']
                print("Latest reddit_posts.created_utc in DB:", ts)
                return int(ts.timestamp())
    except Exception as e:
        print("Could not read latest ts from DB:", e)
    return None

def insert_post(d):
    post_id = d.get("id")
    subreddit = d.get("subreddit")
    title = d.get("title","")[:200]
    selftext = d.get("selftext","")[:300]
    text = (title + " " + selftext).strip()
    created_dt = datetime.fromtimestamp(int(d.get("created_utc",0)), tz=timezone.utc)
    sentiment = float(score_text(text))
    dbid = f"t3_{post_id}"
    try:
        dbmod.insert_reddit_post(dbid, subreddit, text, sentiment, created_dt)
        print("Inserted", dbid, "sentiment=", sentiment)
        return True
    except Exception as e:
        print("DB insert failed for", dbid, e)
        return False

def run_once_verbose():
    latest_ts = get_latest_ts_from_db()
    print("Latest timestamp used for filtering (epoch) =", latest_ts)
    total = 0
    for sub in SUBREDDITS:
        posts = fetch_new(sub, after_utc=latest_ts)
        print(f"Fetched {len(posts)} posts from r/{sub}")
        for p in posts[:10]:
            print("Sample id:", p.get("id"), "title:", (p.get("title") or "")[:120])
            ok = insert_post(p)
            if ok:
                total += 1
    print("Total inserted this run:", total)
    return total

if __name__ == "__main__":
    print("Verbose Reddit public JSON debug run")
    run_once_verbose()
