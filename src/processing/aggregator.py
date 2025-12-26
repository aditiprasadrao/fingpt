"""
1-minute aggregator (robust) with:
 - fallback sentiment window (look back N minutes if minute-bucket empty)
 - simple per-symbol sentiment mapping via keyword matching (falls back to global sentiment)

Behavior:
 - reads raw tickers and reddit_posts
 - computes open/close/high/low/volume_sum per-symbol per-minute
 - computes avg_sentiment and post_count for the same minute (or fallback window)
 - inserts into aggregates table (created automatically if missing)

Run: python -m src.processing.aggregator
"""
from datetime import datetime, timedelta, timezone
import time
import os
import traceback
from typing import Optional

import pandas as pd
from sqlalchemy import Table, Column, Float, Integer, String, DateTime, MetaData, select, insert
from sqlalchemy.exc import IntegrityError

# local import of your DB module
from src.storage import db as dbmod

engine = dbmod.engine
metadata = MetaData()

# --- Configuration ---
FALLBACK_MINUTES = 5  # if no posts in the exact minute, search back up to this many minutes
# Simple keyword mapping to associate reddit posts with a symbol.
# Expand or refine as needed.
SYMBOL_KEYWORDS = {
    "BTC-USD": ["btc", "bitcoin", "sats", "btcusd"],
    "ETH-USD": ["eth", "ethereum", "ether", "ethusd"],
    "USDT-USD": ["usdt", "tether"]
}

# define aggregates table (same schema used by your dashboard)
aggregates = Table(
    "aggregates", metadata,
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
# ensure table exists (no-op if already present)
metadata.create_all(engine, tables=[aggregates])


def floor_to_minute(dt: datetime) -> Optional[datetime]:
    """Round down to minute with preserved tzinfo if present."""
    if dt is None:
        return None
    return dt.replace(second=0, microsecond=0)


def get_latest_ticker_time() -> Optional[datetime]:
    """Return latest tick timestamp (datetime) from tickers table or None."""
    with engine.connect() as conn:
        r = conn.execute(select(dbmod.tickers.c.ts).order_by(dbmod.tickers.c.ts.desc()).limit(1)).first()
        return r._mapping['ts'] if r else None


def _fetch_posts_window(conn, start: datetime, end: datetime, fallback_minutes: int):
    """
    Return DataFrame of posts in [start,end). If empty, expand backwards up to fallback_minutes
    and return posts in [start - fallback_minutes, end).
    """
    stmt = select(dbmod.reddit_posts).where(dbmod.reddit_posts.c.created_utc >= start).where(dbmod.reddit_posts.c.created_utc < end)
    rows = conn.execute(stmt).all()
    df_posts = pd.DataFrame([dict(r._mapping) for r in rows]) if rows else pd.DataFrame()

    if df_posts.empty and fallback_minutes > 0:
        fb_start = start - timedelta(minutes=fallback_minutes)
        stmt_fb = select(dbmod.reddit_posts).where(dbmod.reddit_posts.c.created_utc >= fb_start).where(dbmod.reddit_posts.c.created_utc < end)
        rows_fb = conn.execute(stmt_fb).all()
        df_posts = pd.DataFrame([dict(r._mapping) for r in rows_fb]) if rows_fb else pd.DataFrame()

    return df_posts


def _filter_posts_for_symbol(df_posts: pd.DataFrame, sym: str) -> pd.DataFrame:
    """
    Given df_posts (may be empty), filter posts that mention the symbol using SYMBOL_KEYWORDS.
    Returns filtered DataFrame (may be empty). Matching is simple substring, case-insensitive.
    """
    if df_posts.empty:
        return df_posts
    kw_list = SYMBOL_KEYWORDS.get(sym, [])
    if not kw_list:
        return pd.DataFrame()  # no keywords configured -> return empty so caller can fallback to global
    # ensure text column exists
    if 'text' not in df_posts.columns and 'title' in df_posts.columns:
        df_posts['text'] = df_posts['title'].fillna("")  # fallback

    texts = df_posts['text'].astype(str).str.lower().fillna("")
    mask = texts.apply(lambda s: any(kw in s for kw in kw_list))
    return df_posts[mask]


def aggregate_minute(window_min: datetime) -> int:
    """
    Aggregate ticks and reddit posts for the minute window [window_min, window_min+1min)
    Return number of inserted aggregate rows.
    """
    start = window_min
    end = window_min + timedelta(minutes=1)
    inserted = 0

    with engine.connect() as conn:
        # get ticks in window
        stmt = select(dbmod.tickers).where(dbmod.tickers.c.ts >= start).where(dbmod.tickers.c.ts < end)
        rows = conn.execute(stmt).all()
        if not rows:
            # nothing to aggregate
            return 0

        # build DataFrame of ticks
        df_ticks = pd.DataFrame([dict(r._mapping) for r in rows])
        if df_ticks.empty or 'symbol' not in df_ticks.columns:
            return 0

        # unique symbols present in this minute
        symbols = df_ticks['symbol'].unique().tolist()

        # fetch posts for the window, with fallback
        df_posts = _fetch_posts_window(conn, start, end, FALLBACK_MINUTES)

        for sym in symbols:
            d = df_ticks[df_ticks['symbol'] == sym].sort_values('ts')
            if d.empty:
                continue

            # OHLC-like values from tick stream
            open_price = float(d.iloc[0]['price'])
            close_price = float(d.iloc[-1]['price'])
            high_price = float(d['price'].max())
            low_price = float(d['price'].min())
            volume_sum = float(d['volume'].fillna(0).sum())
            price_change_pct = ((close_price - open_price) / open_price) * 100.0 if open_price else 0.0

            # Determine per-symbol posts first (keyword mapping)
            df_sym_posts = _filter_posts_for_symbol(df_posts, sym)

            if not df_sym_posts.empty and 'sentiment' in df_sym_posts.columns:
                avg_sent = float(df_sym_posts['sentiment'].astype(float).mean())
                post_count = int(len(df_sym_posts))
            else:
                # fallback to global posts (df_posts) if per-symbol filtering produced nothing
                if not df_posts.empty and 'sentiment' in df_posts.columns:
                    avg_sent = float(df_posts['sentiment'].astype(float).mean())
                    post_count = int(len(df_posts))
                else:
                    avg_sent = None
                    post_count = 0

            row = {
                "ts": start,
                "symbol": sym,
                "avg_sentiment": avg_sent,
                "post_count": post_count,
                "open_price": open_price,
                "close_price": close_price,
                "high_price": high_price,
                "low_price": low_price,
                "volume": volume_sum,
                "price_change_pct": price_change_pct
            }

            try:
                with engine.begin() as conn2:
                    conn2.execute(insert(aggregates).values(**row))
                inserted += 1
            except IntegrityError:
                # already exists - skip. Use UPSERT if you want to update existing aggregates.
                pass
            except Exception:
                # unexpected: log and continue
                print("Error inserting aggregate for", sym, start)
                traceback.print_exc()

    return inserted


def run_once(latest_minutes: int = 5) -> int:
    """
    Compute aggregates for the last `latest_minutes` up to the most recent tick minute.
    Returns number of inserted rows.
    """
    latest_ts = get_latest_ticker_time()
    if not latest_ts:
        return 0

    latest_min = floor_to_minute(latest_ts)
    windows = [latest_min - timedelta(minutes=i) for i in range(latest_minutes)][::-1]
    total = 0
    for w in windows:
        total += aggregate_minute(w)
    return total


def run_loop(poll_seconds: int = 30):
    print("AGGREGATOR STARTED - entering loop. Press Ctrl+C to stop.")
    try:
        while True:
            created = run_once(latest_minutes=6)
            if created:
                print(f"Inserted {created} aggregate rows.")
            else:
                # helpful heartbeat
                print("No new aggregates this cycle.")
            time.sleep(poll_seconds)
    except KeyboardInterrupt:
        print("Aggregator stopped by user.")


if __name__ == "__main__":
    # optional: if you set env var AGGREGATOR_ONCE=1 it will run one pass and exit
    if os.getenv("AGGREGATOR_ONCE", "") in ("1", "true", "True"):
        inserted = run_once(latest_minutes=6)
        print("Inserted (one-shot):", inserted)
    else:
        run_loop()

