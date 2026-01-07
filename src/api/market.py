from fastapi import APIRouter, HTTPException
from src.storage import db as dbmod
from sqlalchemy import select
import pandas as pd

router = APIRouter()

# Make sure metadata is aware of all tables
dbmod.metadata.reflect(bind=dbmod.engine)

@router.get("/api/market/{symbol}")
def get_market_data(symbol: str):
    try:
        # Prefer aggregates if available
        agg = dbmod.metadata.tables.get("aggregates")
        ticker = dbmod.metadata.tables.get("tickers")

        with dbmod.engine.connect() as conn:
            if agg is not None:
                rows = conn.execute(
                    select(agg)
                    .where(agg.c.symbol == symbol)
                    .order_by(agg.c.ts.desc())
                    .limit(120)
                ).mappings().all()
            else:
                rows = []

            # Fallback to tickers if aggregates empty
            if not rows and ticker is not None:
                rows = conn.execute(
                    select(ticker)
                    .where(ticker.c.symbol == symbol)
                    .order_by(ticker.c.ts.desc())
                    .limit(120)
                ).mappings().all()

        if not rows:
            raise HTTPException(status_code=404, detail="No market data found")

        df = pd.DataFrame(rows)

        # Normalize timestamp
        df["ts"] = pd.to_datetime(df["ts"])

        # --- PRICE COLUMN RESOLUTION (THE FIX) ---
        if "close_price" in df.columns:
            price_col = "close_price"
        elif "price" in df.columns:
            price_col = "price"
        else:
            raise RuntimeError(f"No price column found. Columns={list(df.columns)}")

        latest = df.iloc[0]

        return {
            "symbol": symbol,
            "latest": {
                "price": float(latest[price_col]),
                "volume": float(latest.get("volume", 0)),
                "ts": str(latest["ts"])
            },
            "series": df[["ts", price_col]].rename(
                columns={price_col: "price"}
            ).to_dict(orient="records")
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
