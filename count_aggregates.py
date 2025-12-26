from src.storage import db as dbmod
from sqlalchemy import text
with dbmod.engine.connect() as conn:
    try:
        print("COUNT:", conn.execute(text("SELECT COUNT(*) FROM aggregates")).fetchone())
    except Exception as e:
        print("Error querying aggregates:", e)
