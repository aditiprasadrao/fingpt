# check_tickers.py
from src.storage import db as dbmod
from sqlalchemy import select
with dbmod.engine.connect() as c:
    rows = c.execute(select(dbmod.tickers).order_by(dbmod.tickers.c.ts.desc()).limit(5)).all()
    for r in rows:
        print(dict(r._mapping))
