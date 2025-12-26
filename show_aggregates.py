from src.storage import db as dbmod
from sqlalchemy import text
with dbmod.engine.connect() as conn:
    rows = conn.execute(text("SELECT ts, symbol, avg_sentiment, post_count, close_price FROM aggregates ORDER BY ts DESC LIMIT 5")).fetchall()
    print("Recent aggregates:", rows)
