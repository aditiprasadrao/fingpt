# seed_test_post.py
from datetime import datetime, timezone
from sqlalchemy import insert
from src.storage import db as dbmod
from uuid import uuid4

engine = dbmod.engine
now = datetime.now(timezone.utc)
stmt = insert(dbmod.reddit_posts).values(
    id=str(uuid4()),
    subreddit="CryptoCurrency",
    text="Bitcoin is extremely bullish today! moon incoming",
    sentiment=0.9,
    created_utc=now
)
with engine.begin() as c:
    c.execute(stmt)
print("Inserted test post at", now)
