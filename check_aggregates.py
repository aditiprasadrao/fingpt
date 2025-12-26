# check_aggregates.py
from src.storage import db as dbmod
from sqlalchemy import select
import pandas as pd

# preferred helper if available
try:
    rows = dbmod.get_recent_aggregates("BTC-USD", limit=10)
    print("get_recent_aggregates returned:", len(rows))
    for r in rows: print(r)
except Exception as e:
    print("get_recent_aggregates failed:", e)

# direct DB fallback to inspect raw aggregates table
with dbmod.engine.connect() as c:
    rows = c.execute(select(dbmod.metadata.tables['aggregates']).order_by(dbmod.metadata.tables['aggregates'].c.ts.desc()).limit(10)).all()
    print("\nDirect aggregates rows (raw):")
    for r in rows:
        print(dict(r._mapping))
