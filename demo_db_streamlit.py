import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import uuid

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from sqlalchemy import insert

from src.storage import db as dbmod
from src.rag.retriever import retrieve_context
from src.rag.generator import generate_answer

# ---------------------------------------------------
# Page config
# ---------------------------------------------------
st.set_page_config(layout="wide", page_title="Crypto Sentiment Dashboard")
st.title("🚀 Crypto Sentiment & Market Insight Dashboard")

ANALYZER = SentimentIntensityAnalyzer()

# ---------------------------------------------------
# Sidebar
# ---------------------------------------------------
st.sidebar.header("Controls")

SYMBOL = st.sidebar.selectbox("Symbol", ["BTC-USD", "ETH-USD", "USDT-USD"])
amount_usd = st.sidebar.number_input(
    "Investment (USD)", value=2300.0, step=50.0
)

REFRESH = st.sidebar.button("Refresh data")

# ---------------------------------------------------
# Seed sentiment posts (safe, optional)
# ---------------------------------------------------
SAMPLE_POSTS = [
    "Bitcoin looks extremely bullish today",
    "Market crash incoming, panic selling",
    "Strong fundamentals, long-term buy",
    "ETH upgrade is very promising",
    "Whales manipulating prices",
]

if st.sidebar.button("Seed Sample Reddit Posts"):
    for _ in range(20):
        txt = np.random.choice(SAMPLE_POSTS)
        stmt = insert(dbmod.reddit_posts).values(
            id=str(uuid.uuid4()),
            subreddit="CryptoCurrency",
            text=txt,
            sentiment=ANALYZER.polarity_scores(txt)["compound"],
            created_utc=datetime.now(timezone.utc)
        )
        with dbmod.engine.begin() as conn:
            conn.execute(stmt)
    st.sidebar.success("Sample posts inserted")

# ---------------------------------------------------
# Load data (AGGREGATES preferred, fallback safe)
# ---------------------------------------------------
rows = dbmod.get_recent_aggregates(SYMBOL, limit=200)
df = pd.DataFrame(rows)

if df.empty:
    st.warning("No data available yet. Ensure ingestion + aggregator are running.")
    st.stop()

# Normalize timestamp
if "ts" in df.columns:
    df["ts"] = pd.to_datetime(df["ts"])
else:
    st.error("Timestamp column missing.")
    st.stop()

df = df.sort_values("ts").reset_index(drop=True)

# ---------------------------------------------------
# PRICE COLUMN DETECTION (CRITICAL FIX)
# ---------------------------------------------------
if "close_price" in df.columns:
    price_col = "close_price"
    data_mode = "Aggregated OHLC"
elif "price" in df.columns:
    price_col = "price"
    data_mode = "Live Ticker"
else:
    st.error(f"No usable price column found. Columns: {list(df.columns)}")
    st.stop()

# ---------------------------------------------------
# SENTIMENT COLUMN DETECTION (SAFE)
# ---------------------------------------------------
sent_col = None
for c in ["avg_sentiment", "sentiment", "sentiment_strength"]:
    if c in df.columns:
        sent_col = c
        break

# ---------------------------------------------------
# Info banner
# ---------------------------------------------------
st.caption(f"📊 Data mode: **{data_mode}**")

# ---------------------------------------------------
# Plot
# ---------------------------------------------------
import plotly.graph_objects as go

fig = go.Figure()

fig.add_trace(go.Scatter(
    x=df["ts"],
    y=df[price_col],
    name="Price",
    line=dict(width=2)
))

if sent_col:
    fig.add_trace(go.Scatter(
        x=df["ts"],
        y=df[sent_col],
        name="Sentiment",
        yaxis="y2",
        mode="lines+markers"
    ))

fig.update_layout(
    yaxis=dict(title="Price (USD)"),
    yaxis2=dict(
        title="Sentiment",
        overlaying="y",
        side="right",
        range=[-1, 1]
    ) if sent_col else {},
    height=500,
    template="plotly_dark"
)

st.plotly_chart(fig, width="stretch")

# ---------------------------------------------------
# Investment insight
# ---------------------------------------------------
last_price = float(df.iloc[-1][price_col])
units = amount_usd / last_price if last_price else 0.0

c1, c2, c3 = st.columns(3)
c1.metric("Investment", f"${amount_usd:,.2f}")
c2.metric("Latest Price", f"${last_price:,.2f}")
c3.metric("Units", f"{units:.6f} {SYMBOL.split('-')[0]}")

# ---------------------------------------------------
# Aggregates table (debug visibility)
# ---------------------------------------------------
st.subheader("Latest data (tail)")
st.dataframe(df.tail(30).reset_index(drop=True))

# ---------------------------------------------------
# RAG Chatbot
# ---------------------------------------------------
st.divider()
st.header("🧠 Ask the Market (RAG-powered)")

if "chat" not in st.session_state:
    st.session_state.chat = []

question = st.chat_input("Ask about price trends, sentiment, or market behavior")

if question:
    context = retrieve_context(question, symbol=SYMBOL.split("-")[0])
    answer = generate_answer(context, question)
    st.session_state.chat.append(("user", question))
    st.session_state.chat.append(("assistant", answer))

for role, msg in st.session_state.chat:
    with st.chat_message(role):
        st.markdown(msg)
