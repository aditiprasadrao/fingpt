from src.storage import db as dbmod
from sqlalchemy import text
with dbmod.engine.connect() as conn:
    rows = conn.execute(text("SELECT created_utc, subreddit, sentiment FROM reddit_posts ORDER BY created_utc DESC LIMIT 5")).fetchall()
    print("Recent reddit posts:", rows)
