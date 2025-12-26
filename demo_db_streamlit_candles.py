# demo_db_streamlit_candles.py
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import timezone
from sqlalchemy import select
from src.storage import db as dbmod

st.set_page_config(layout="wide", page_title="Crypto Candles + Sentiment")

st.sidebar.title("Controls")
SYMBOL = st.sidebar.selectbox("Symbol", ["BTC-USD", "ETH-USD", "USDT-USD"])
minutes = st.sidebar.slider("Minutes (display)", min_value=15, max_value=720, value=120, step=15)
refresh = st.sidebar.button("Refresh now")
amount = st.sidebar.number_input("Enter amount (USD) to compute units", value=2300.00, step=10.0, format="%.2f")

st.title("Crypto — Candles & Sentiment")

def load_aggregates(symbol: str, minutes_display: int) -> pd.DataFrame:
    """
    Load recent aggregate rows for `symbol`.
    Prefer the project's helper; if that fails, query aggregates table directly.
    Returns a DataFrame with parsed datetimes and numeric columns when possible.
    """
    try:
        # prefer helper if available (returns sequence of dict-like rows)
        rows = dbmod.get_recent_aggregates(symbol, limit=minutes_display)
        df = pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception:
        # fallback direct select
        try:
            with dbmod.engine.connect() as conn:
                agg = dbmod.metadata.tables.get("aggregates")
                if agg is None:
                    return pd.DataFrame()
                q = select(agg).where(agg.c.symbol == symbol).order_by(agg.c.ts.desc()).limit(minutes_display)
                rows = conn.execute(q).all()
            df = pd.DataFrame([dict(r._mapping) for r in rows]) if rows else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    if df.empty:
        return df

    # Normalize and coerce types
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"])
    else:
        # try common alternatives (rare)
        for cand in ("timestamp", "time", "created_at"):
            if cand in df.columns:
                df["ts"] = pd.to_datetime(df[cand])
                break

    df = df.sort_values("ts").reset_index(drop=True)

    # Ensure numeric types for expected price columns
    for c in ("open_price", "high_price", "low_price", "close_price", "avg_sentiment", "volume"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df

if refresh:
    st.experimental_rerun()

df = load_aggregates(SYMBOL, minutes)

if df.empty:
    st.info("No aggregate rows found for this symbol yet. Ensure ingestion + aggregator are running, then click Refresh.")
else:
    # Ensure the required columns exist before plotting
    required_cols = ("open_price", "high_price", "low_price", "close_price")
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        st.error(f"Aggregates are present but missing required OHLC columns: {missing}. Available columns: {list(df.columns)}")
        st.subheader("Aggregates (tail) — raw")
        st.dataframe(df.tail(30))
    else:
        # Build candlestick figure
        fig = go.Figure()
        fig.add_trace(go.Candlestick(
            x=df["ts"],
            open=df["open_price"],
            high=df["high_price"],
            low=df["low_price"],
            close=df["close_price"],
            name="OHLC",
            increasing_line_color="lightblue",
            decreasing_line_color="royalblue",
        ))

        # If sentiment exists, plot as separate y-axis trace
        if "avg_sentiment" in df.columns:
            fig.add_trace(go.Scatter(
                x=df["ts"],
                y=df["avg_sentiment"],
                name="Avg Sentiment",
                yaxis="y2",
                mode="lines+markers",
                line=dict(width=2),
            ))

        fig.update_layout(
            xaxis=dict(type="date", rangeslider_visible=False),
            yaxis=dict(title="Price (USD)"),
            yaxis2=dict(title="Sentiment", overlaying="y", side="right", range=[-1, 1]) if "avg_sentiment" in df.columns else {},
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=50, r=50, t=80, b=50),
            template="plotly_dark",
            height=600,
        )

        # Use new style for container width (replaces deprecated use_container_width)
        st.plotly_chart(fig, width="stretch")

        # right column summary (latest row)
        last = df.iloc[-1]
        col_left, col_right = st.columns([3, 1])
        with col_right:
            try:
                last_close = float(last["close_price"])
                st.metric("Last close (USD)", f"{last_close:,.2f}")
            except Exception:
                st.metric("Last close (USD)", "N/A")

            if "avg_sentiment" in df.columns:
                try:
                    st.metric("Last avg sentiment", f"{float(last['avg_sentiment']):.3f}")
                except Exception:
                    st.metric("Last avg sentiment", "N/A")

            # units calculation (safe)
            try:
                units = (float(amount) / last_close) if last_close else 0.0
                # symbol short name (BTC-USD -> BTC)
                symbol_short = SYMBOL.split("-")[0]
                st.markdown(f"With **${amount:,.2f}** you can buy **{units:.6f} {symbol_short}** (approx.)")
            except Exception:
                st.write("Can't compute units (missing close price).")

        # Tail table for debugging / visibility
        st.subheader("Latest aggregates (tail)")
        tail_cols = ["ts", "open_price", "high_price", "low_price", "close_price", "avg_sentiment", "volume"]
        present = [c for c in tail_cols if c in df.columns]
        st.dataframe(df[present].tail(30).reset_index(drop=True))

        st.caption("Candlesticks use 1-minute aggregates. If candles look sparse, increase Minutes or ensure the aggregator is running and creating rows.")

