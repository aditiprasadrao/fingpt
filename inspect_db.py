# inspect_db.py
from src.storage import db as dbmod
from sqlalchemy import inspect

print("\n===== DB MODULE ATTRIBUTES =====\n")
attrs = [a for a in dir(dbmod) if not a.startswith("_")]
for a in attrs:
    print(a)

print("\n===== ENGINE URL =====\n")
try:
    print(dbmod.engine.url)
except:
    print("No engine found.")

print("\n===== TABLES DETECTED =====\n")
inspector = inspect(dbmod.engine)
print(inspector.get_table_names())

print("\n===== TABLE COLUMNS =====\n")
for table in inspector.get_table_names():
    print(f"\n--- {table} ---")
    for col in inspector.get_columns(table):
        print(f"{col['name']:20} {col['type']}")

print("\n===== CHECK get_recent_aggregates FUNCTION =====\n")
try:
    print(dbmod.get_recent_aggregates)
except:
    print("get_recent_aggregates NOT FOUND")
