from src.storage import db as dbmod
# reflect fresh metadata
dbmod.metadata.reflect(bind=dbmod.engine)
print("metadata tables after reflect:", list(dbmod.metadata.tables.keys()))
