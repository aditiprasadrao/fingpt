# demo_streamlit.py
# Single-file simulated Crypto Sentiment Dashboard for quick testing.
# Run: python -m streamlit run demo_streamlit.py

import streamlit as st
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta, timezone
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import threading

st.set_page_config(layout="wide", page_title="Demo Crypto Sentiment")

# In-memory data stores
TICK_DF = pd.DataFrame(columns=["ts","symbol","price","volume"])
POST_DF = pd.DataFrame(columns=["ts","subreddit","text","sentiment"])
ANALYZER = SentimentIntensityAnalyzer()

SYMBOL = st.sidebar.selectbox("Symbol", ["BTC-USD","ETH-USD"])
START_BUTTON = st.sidebar.button("Start Simulation")
STOP_BUTTON = st.sidebar.button("Stop Simulation")

# simulation controls
sim_rate = st.sidebar.slider("Tick interval (sec)", min_value=0.5, max_value=5.0, value=1.0, step=0.5)
post_rate = st.sidebar.slider("Post interval (sec)", min_value=5, max_value=60, value=15, step=5)

# simple price generator state
state = {"running": False, "price": {"BTC-USD": 50000.0, "ETH-USD": 3500.0}}

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

def score_text(text):
    return float(ANALYZER.polarity_scores(text)["compound"])

def simulate_tick_loop(interval):
    while state["running"]:
        ts = datetime.now(timezone.utc)
        for sym in ["BTC-USD","ETH-USD"]:
            base = state["price"][sym]
            drift = np.random.normal(loc=0.0, scale=0.2)
            new_price = max(0.1, base * (1 + drift/100.0))
            vol = max(0.0, np.random.exponential(scale=0.5))
            state["price"][sym] = new_price
            TICK_DF.loc[len(TICK_DF.index)] = [ts, sym, float(new_price), float(vol)]
        time.sleep(interval)

def simulate_posts_loop(interval):
    while state["running"]:
        ts = datetime.now(timezone.utc)
        txt = np.random.choice(SAMPLE_POSTS)
        sent = score_text(txt)
        POST_DF.loc[len(POST_DF.index)] = [ts, "CryptoCurrency", txt, float(sent)]
        time.sleep(interval)

def start_simulation(tick_interval, post_interval):
    if state["running"]:
        return
    state["running"] = True
    t1 = threading.Thread(target=simulate_tick_loop, args=(tick_interval,), daemon=True)
    t2 = threading.Thread(target=simulate_posts_loop, args=(post_interval,), daemon=True)
    t1.start()
    t2.start()

def stop_simulation():
    state["running"] = False

# control buttons
if START_BUTTON:
    start_simulation(sim_rate, post_rate)
if STOP_BUTTON:
    stop_simulation()

st.title("Demo Crypto Sentiment Dashboard (Simulated Data)")

# build aggregates every time UI refreshes
def build_aggregates(symbol, window_minutes=60):
    if TICK_DF.empty:
        return pd.DataFrame()
    df = TICK_DF.copy()
    df['ts'] = pd.to_datetime(df['ts'])
    df = df.set_index('ts')

    ticks = df[df['symbol']==symbol]['price'].resample('1T').agg(['first','last','max','min','count'])
    ticks = ticks.rename(columns={'first':'open','last':'close','max':'high','min':'low','count':'tick_count'})
    ticks['volume'] = df[df['symbol']==symbol]['volume'].resample('1T').sum().fillna(0)

    if not POST_DF.empty:
        p = POST_DF.copy()
        p['ts'] = pd.to_datetime(p['ts'])
        p = p.set_index('ts')
        s = p['sentiment'].resample('1T').mean().rename('avg_sentiment')
        c = p['sentiment'].resample('1T').count().rename('post_count')
        ticks = ticks.join(s, how='left')
        ticks = ticks.join(c, how='left')
    else:
        ticks['avg_sentiment'] = None
        ticks['post_count'] = 0

    ticks = ticks.fillna(method='ffill').fillna(0)
    return ticks.tail(window_minutes)

agg = build_aggregates(SYMBOL, window_minutes=120)

col1, col2 = st.columns([3,1])
with col1:
    if not agg.empty:
        import plotly.graph_objects as go
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=agg.index, y=agg['close'], name='Close Price'))
        fig.add_trace(go.Scatter(x=agg.index, y=agg['avg_sentiment'], name='Avg Sentiment', yaxis='y2'))
        fig.update_layout(
            yaxis=dict(title='Price'),
            yaxis2=dict(title='Sentiment', overlaying='y', side='right', range=[-1,1])
        )
        st.plotly_chart(fig, width="stretch")

    else:
        st.info("No data yet. Click Start Simulation.")

with col2:
    if not agg.empty:
        last = agg.iloc[-1]
        st.metric("Last close", f"{last['close']:.2f}")
        st.metric("Last sentiment", f"{last['avg_sentiment']:.3f}")
        st.metric("Posts (last min)", f"{int(last['post_count'])}")
    else:
        st.write("Waiting for simulated data...")

st.subheader("Recent simulated posts")
if not POST_DF.empty:
    dfp = POST_DF.copy()
    dfp['ts'] = pd.to_datetime(dfp['ts'])
    dfp = dfp.sort_values('ts', ascending=False).head(20)
    st.dataframe(dfp[['ts','subreddit','text','sentiment']])
else:
    st.write("No posts yet. Start simulation.")

st.caption("This demo simulates live ticks and reddit posts locally — no external APIs required.")
