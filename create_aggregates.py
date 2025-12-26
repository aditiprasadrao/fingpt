from src.storage import db as dbmod
from sqlalchemy import Table, Column, Float, Integer, String, DateTime, MetaData

meta = MetaData()

aggregates = Table(
    "aggregates", meta,
    Column("ts", DateTime(timezone=True), primary_key=True),
    Column("symbol", String, primary_key=True),
    Column("avg_sentiment", Float),
    Column("post_count", Integer),
    Column("open_price", Float),
    Column("close_price", Float),
    Column("high_price", Float),
    Column("low_price", Float),
    Column("volume", Float),
    Column("price_change_pct", Float)
)

meta.create_all(dbmod.engine, tables=[aggregates])
print("Created aggregates table if it did not exist.")
print("Tables now:", list(dbmod.metadata.tables.keys()))
