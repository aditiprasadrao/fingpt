from src.storage import db as dbmod
from sqlalchemy import text

create_sql = """
CREATE TABLE IF NOT EXISTS aggregates (
  ts TIMESTAMP,
  symbol TEXT,
  avg_sentiment REAL,
  post_count INTEGER,
  open_price REAL,
  close_price REAL,
  high_price REAL,
  low_price REAL,
  volume REAL,
  price_change_pct REAL,
  PRIMARY KEY (ts, symbol)
);
"""

with dbmod.engine.begin() as conn:
    conn.execute(text(create_sql))
    # show tables now
    res = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")).fetchall()
    print("Tables in sqlite_master:", res)
