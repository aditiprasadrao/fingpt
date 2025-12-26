import os, sqlite3
from src.storage import db as dbmod

print("---- src.storage.db DB_URL ----")
print(getattr(dbmod, "DB_URL", "NO DB_URL"))

print("---- Engine URL (sqlalchemy) ----")
try:
    print(dbmod.engine.url)
except Exception as e:
    print("Engine URL error:", e)

print("---- metadata.tables keys (src.storage.db) ----")
try:
    print(list(dbmod.metadata.tables.keys()))
except Exception as e:
    print("metadata error:", e)

# If sqlite, show actual file and its tables
db_url = getattr(dbmod, "DB_URL", "") or ""
if db_url.startswith("sqlite:///"):
    fpath = db_url.replace("sqlite:///", "")
    fpath = os.path.abspath(fpath)
    print("---- Resolved sqlite file path ----")
    print(fpath)
    if os.path.exists(fpath):
        try:
            conn = sqlite3.connect(fpath)
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cur.fetchall()
            print("---- Tables in sqlite file ----")
            print(tables)
            conn.close()
        except Exception as e:
            print("Error reading sqlite file:", e)
    else:
        print("SQLite file does NOT exist at that path.")
else:
    print("Not using sqlite or DB_URL missing; skipping sqlite file check.")
