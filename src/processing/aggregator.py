"""
1-minute aggregator with sentiment strength (ABS sentiment).

Run:
python -m src.processing.aggregator
"""

from datetime import datetime, timedelta, timezone
import time
import os
import traceback
from typing import Optional

import pandas as pd
from sqlalchemy import (
    Table, Column, Float, Integer, String, DateTime,
    MetaData, select, insert
)
from sqlalchemy.exc import IntegrityError

from src.storage import db as dbmod

engine = dbmod.engine
metadata = MetaData()

# ---------------------------------------------------
# Config
# ---------------------------------------------------
FALLBACK_MINUTES = 5

SYMBOL_KEYWORDS = {
    "BTC-USD": ["btc", "bitcoin", "sats", "btcusd"],
    "ETH-USD": ["eth", "ethereum", "ether", "ethusd"],
}

# ---------------------------------------------------
# Aggregates table (UPDATED)
# ---------------------------------------------------
aggregates = Table(
    "aggregates", metadata,
    Column("ts", DateTime(timezone=True), primary_key=True),
    Column("symbol", String, primary_key=True),

    Column("avg_sentiment", Float),
    Column("sentiment_strength", Float),   # 🔥 NEW
    Column("post_count", Integer),

    Column("open_price", Float),
    Column("close_price", Float),
    Column("high_price", Float),
    Column("low_price", Float),
    Column("volume", Float),
    Column("price_change_pct", Float),
)

metadata.create_all(engine, tables=[aggregates])


def floor_to_minute(dt: datetime) -> Optional[datetime]:
    return dt.replace(second=0, microsecond=0) if dt else None


def get_latest_ticker_time() -> Optional[datetime]:
    with engine.connect() as conn:
        r = conn.execute(
            select(dbmod.tickers.c.ts)
            .order_by(dbmod.tickers.c.ts.desc())
            .limit(1)
        ).first()
        return r._mapping["ts"] if r else None


def _fetch_posts_window(conn, start, end):
    stmt = select(dbmod.reddit_posts).where(
        dbmod.reddit_posts.c.created_utc >= start,
        dbmod.reddit_posts.c.created_utc < end
    )
    rows = conn.execute(stmt).all()
    df = pd.DataFrame([dict(r._mapping) for r in rows])

    if df.empty:
        fb_start = start - timedelta(minutes=FALLBACK_MINUTES)
        stmt = select(dbmod.reddit_posts).where(
            dbmod.reddit_posts.c.created_utc >= fb_start,
            dbmod.reddit_posts.c.created_utc < end
        )
        rows = conn.execute(stmt).all()
        df = pd.DataFrame([dict(r._mapping) for r in rows])

    return df


def _filter_posts_for_symbol(df: pd.DataFrame, sym: str) -> pd.DataFrame:
    if df.empty:
        return df
    kws = SYMBOL_KEYWORDS.get(sym, [])
    if not kws:
        return pd.DataFrame()

    text = df["text"].astype(str).str.lower()
    mask = text.apply(lambda t: any(k in t for k in kws))
    return df[mask]


def aggregate_minute(window_min: datetime) -> int:
    start = window_min
    end = start + timedelta(minutes=1)
    inserted = 0

    with engine.connect() as conn:
        ticks = conn.execute(
            select(dbmod.tickers).where(
                dbmod.tickers.c.ts >= start,
                dbmod.tickers.c.ts < end
            )
        ).all()

        if not ticks:
            return 0

        df_ticks = pd.DataFrame([dict(r._mapping) for r in ticks])
        df_posts = _fetch_posts_window(conn, start, end)

        for sym in df_ticks["symbol"].unique():
            d = df_ticks[df_ticks["symbol"] == sym].sort_values("ts")

            open_p = float(d.iloc[0]["price"])
            close_p = float(d.iloc[-1]["price"])
            high_p = float(d["price"].max())
            low_p = float(d["price"].min())
            volume = float(d["volume"].fillna(0).sum())
            pct = ((close_p - open_p) / open_p) * 100 if open_p else 0

            df_sym = _filter_posts_for_symbol(df_posts, sym)

            if not df_sym.empty:
                s = df_sym["sentiment"].astype(float)
            elif not df_posts.empty:
                s = df_posts["sentiment"].astype(float)
            else:
                s = None

            if s is not None:
                avg_sent = float(s.mean())
                sent_strength = float(s.abs().mean())
                post_count = int(len(s))
            else:
                avg_sent = None
                sent_strength = None
                post_count = 0

            row = {
                "ts": start,
                "symbol": sym,
                "avg_sentiment": avg_sent,
                "sentiment_strength": sent_strength,
                "post_count": post_count,
                "open_price": open_p,
                "close_price": close_p,
                "high_price": high_p,
                "low_price": low_p,
                "volume": volume,
                "price_change_pct": pct,
            }

            try:
                with engine.begin() as c2:
                    c2.execute(insert(aggregates).values(**row))
                inserted += 1
            except IntegrityError:
                pass
            except Exception:
                traceback.print_exc()

    return inserted


def run_loop():
    print("Aggregator running...")
    while True:
        latest = get_latest_ticker_time()
        if latest:
            base = floor_to_minute(latest)
            for i in range(6):
                aggregate_minute(base - timedelta(minutes=i))
        time.sleep(30)


if __name__ == "__main__":
    run_loop()
