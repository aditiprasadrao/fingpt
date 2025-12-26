import os
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, MetaData, DateTime, Text, select
from sqlalchemy.sql import func
from datetime import datetime

# Use DATABASE_URL env var if present, otherwise sqlite file in project root
DB_URL = os.getenv("DATABASE_URL", "sqlite:///./crypto.db")

# For sqlite we need check_same_thread=False; for other DBs it's harmless to omit
connect_args = {"check_same_thread": False} if DB_URL.startswith("sqlite") else {}

engine = create_engine(DB_URL, connect_args=connect_args, echo=False)

metadata = MetaData()

tickers = Table(
    "tickers", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("symbol", String, index=True),
    Column("price", Float),
    Column("volume", Float, nullable=True),
    Column("ts", DateTime(timezone=True), default=func.now())
)

reddit_posts = Table(
    "reddit_posts", metadata,
    Column("id", String, primary_key=True),
    Column("subreddit", String, index=True),
    Column("text", Text),
    Column("sentiment", Float),
    Column("created_utc", DateTime(timezone=True))
)

# create tables if they don't exist
metadata.create_all(engine)

def insert_ticker(symbol, price, volume=None, ts=None):
    ts = ts or datetime.utcnow()
    with engine.begin() as conn:
        conn.execute(
            tickers.insert().values(symbol=symbol, price=price, volume=volume, ts=ts)
        )

def insert_reddit_post(post_id, subreddit, text, sentiment, created_utc):
    with engine.begin() as conn:
        conn.execute(
            reddit_posts.insert().values(id=post_id, subreddit=subreddit, text=text, sentiment=sentiment, created_utc=created_utc)
        )

def get_recent_aggregates(symbol="BTC-USD", limit=200):
    stmt = select(tickers).where(tickers.c.symbol == symbol).order_by(tickers.c.ts.desc()).limit(limit)
    with engine.connect() as conn:
        res = conn.execute(stmt).all()
    rows = [dict(r._mapping) for r in res][::-1]
    return rows
