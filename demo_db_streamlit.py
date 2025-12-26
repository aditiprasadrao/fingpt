# demo_db_streamlit.py (updated with units calculation)
# DB-backed simulated Crypto Sentiment Dashboard

import streamlit as st
import pandas as pd
import numpy as np
import time
from datetime import datetime, timezone
import threading
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import uuid

# --- DB imports
from src.storage import db as dbmod
from sqlalchemy import insert

# ---------------------------------------------------
# Streamlit config
# ---------------------------------------------------
st.set_page_config(layout="wide", page_title="Demo DB Crypto Sentiment")

ANALYZER = SentimentIntensityAnalyzer()

# ---------------------------------------------------
# Sidebar controls
# ---------------------------------------------------
st.sidebar.title("Controls")

SYMBOL = st.sidebar.selectbox("Symbol", ["BTC-USD", "ETH-USD"])

START_BUTTON = st.sidebar.button("Start Simulation")
STOP_BUTTON = st.sidebar.button("Stop Simulation")

sim_rate = st.sidebar.slider("Tick interval (sec)", 0.5, 5.0, 1.0, 0.5)
post_rate = st.sidebar.slider("Post interval (sec)", 5, 60, 15, 5)

amount_usd = st.sidebar.number_input(
    "Investment amount (USD)",
    value=2300.00,
    step=50.0,
    format="%.2f"
)

# ---------------------------------------------------
# State
# ---------------------------------------------------
state = {
    "running": False,
    "price": {
        "BTC-USD": 50000.0,
        "ETH-USD": 3500.0
    }
}

# ---------------------------------------------------
# Sample Reddit posts
# ---------------------------------------------------
SAMPLE_POSTS = [
    "Bitcoin to the moon! Very bullish today.",
    "This looks like a huge dump, sell now!",
    "Great long-term fundamentals, buy the dip.",
    "Scared about the market, terrible news.",
    "Partnerships announced, promising future.",
    "Huge whale sell wall, panic.",
    "Amazing upgrade, ETH looking strong.",
    "Market manipulation? Not sure, cautious."
]

# ---------------------------------------------------
# Helper functions
# ---------------------------------------------------
def score_text(text: str) -> float:
    return float(ANALYZER.polarity_scores(str(text))["compound"])

# ---------------------------------------------------
# Simulation threads
# ---------------------------------------------------
def simulate_tick_loop(interval: float):
    engine = dbmod.engine
    while state["running"]:
        ts = datetime.now(timezone.utc)

        for sym in ["BTC-USD", "ETH-USD"]:
            base_price = state["price"][sym]
            drift = np.random.normal(loc=0.0, scale=0.2)
            new_price = max(0.1, base_price * (1 + drift / 100.0))
            volume = float(max(0.0, np.random.exponential(scale=0.5)))

            state["price"][sym] = new_price

            stmt = insert(dbmod.tickers).values(
                symbol=sym,
                price=float(new_price),
                volume=volume,
                ts=ts
            )

            try:
                with engine.begin() as conn:
                    conn.execute(stmt)
            except Exception as e:
                print("Tick insert error:", e)

        time.sleep(interval)


def simulate_posts_loop(interval: float):
    engine = dbmod.engine
    while state["running"]:
        ts = datetime.now(timezone.utc)
        text = np.random.choice(SAMPLE_POSTS)
        sentiment = score_text(text)

        stmt = insert(dbmod.reddit_posts).values(
            id=str(uuid.uuid4()),
            subreddit="CryptoCurrency",
            text=text,
            sentiment=sentiment,
            created_utc=ts
        )

        try:
            with engine.begin() as conn:
                conn.execute(stmt)
        except Exception as e:
            print("Reddit insert error:", repr(e))

        time.sleep(interval)


def start_simulation(tick_interval, post_interval):
    if state["running"]:
        return
    state["running"] = True
    threading.Thread(
        target=simulate_tick_loop,
        args=(tick_interval,),
        daemon=True
    ).start()
    threading.Thread(
        target=simulate_posts_loop,
        args=(post_interval,),
        daemon=True
    ).start()


def stop_simulation():
    state["running"] = False


# ---------------------------------------------------
# Button actions
# ---------------------------------------------------
if START_BUTTON:
    start_simulation(sim_rate, post_rate)

if STOP_BUTTON:
    stop_simulation()

# ---------------------------------------------------
# Main UI
# ---------------------------------------------------
st.title("Crypto Sentiment Dashboard")

# ---------------------------------------------------
# Read aggregates from DB
# ---------------------------------------------------
try:
    rows = dbmod.get_recent_aggregates(SYMBOL, limit=120)
    df = pd.DataFrame(rows)

    if df.empty:
        st.info("No aggregates yet. Start the simulation and ensure aggregator is running.")
    else:
        # Normalize timestamp
        if "ts" in df.columns:
            df["ts"] = pd.to_datetime(df["ts"])
        else:
            df["ts"] = pd.to_datetime(df.index)

        # Identify close price and sentiment columns
        close_col = None
        sent_col = None

        for c in ["close_price", "close", "price"]:
            if c in df.columns:
                close_col = c
                break

        for c in ["avg_sentiment", "sentiment"]:
            if c in df.columns:
                sent_col = c
                break

        if sent_col is None:
            df["__sent_fallback__"] = 0.0
            sent_col = "__sent_fallback__"

        # ---------------------------------------------------
        # Plot
        # ---------------------------------------------------
        import plotly.graph_objects as go

        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=df["ts"],
            y=df[close_col],
            name="Close Price"
        ))

        fig.add_trace(go.Scatter(
            x=df["ts"],
            y=df[sent_col],
            name="Avg Sentiment",
            yaxis="y2"
        ))

        fig.update_layout(
            yaxis=dict(title="Price (USD)"),
            yaxis2=dict(
                title="Sentiment",
                overlaying="y",
                side="right",
                range=[-1, 1]
            ),
            height=500
        )

        st.plotly_chart(fig, width="stretch")

        # ---------------------------------------------------
        # Investment → Units calculation
        # ---------------------------------------------------
        latest_row = df.iloc[-1]
        last_price = float(latest_row[close_col])

        if last_price > 0:
            units = amount_usd / last_price
            symbol_short = SYMBOL.split("-")[0]

            st.subheader("Investment Insight")

            c1, c2, c3 = st.columns(3)

            c1.metric("Investment (USD)", f"${amount_usd:,.2f}")
            c2.metric("Latest Price (USD)", f"{last_price:,.2f}")
            c3.metric(f"{symbol_short} Units", f"{units:.6f}")

            st.markdown(
                f"With **${amount_usd:,.2f}**, you can buy approximately "
                f"**{units:.6f} {symbol_short}** at the current price."
            )

        # ---------------------------------------------------
        # Table
        # ---------------------------------------------------
        st.subheader("Latest aggregates (tail)")
        st.dataframe(df.tail(20).reset_index(drop=True))

except Exception as e:
    st.error(f"DB read error: {e}")
    st.exception(e)


