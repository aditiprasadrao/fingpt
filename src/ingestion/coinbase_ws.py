"""
Coinbase WebSocket ingestion client.

Run: python -m src.ingestion.coinbase_ws
or: python src/ingestion/coinbase_ws.py

It subscribes to the Coinbase public feed for ticker updates and writes rows
into your project's tickers table using src.storage.db.insert_ticker() if available,
or falling back to direct SQLAlchemy insert into dbmod.tickers.
"""
import json
import time
import threading
import os
from datetime import datetime, timezone
import traceback

# websocket-client (non-async)
import websocket

# local DB module
from src.storage import db as dbmod
from sqlalchemy import insert

# Settings: list the symbols you want to subscribe to here or via env var COINBASE_SYMBOLS
DEFAULT_SYMBOLS = ["BTC-USD", "ETH-USD", "USDT-USD"]
SYMBOLS = os.environ.get("COINBASE_SYMBOLS", ",".join(DEFAULT_SYMBOLS)).split(",")

WS_URL = os.environ.get("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")
# Channel we want
CHANNEL = os.environ.get("COINBASE_CHANNEL", "ticker")

# how long to wait between reconnect attempts (seconds)
RECONNECT_BASE = 1.0
RECONNECT_MAX = 60.0

# optional debug
DEBUG = bool(os.environ.get("COINBASE_WS_DEBUG", "0") in ("1", "true", "True"))

def log(*args, **kwargs):
    if DEBUG:
        print("[coinbase_ws]", *args, **kwargs)

def handle_message(msg):
    """
    Process a single JSON message from Coinbase WS.
    We care about 'ticker' messages which include price, product_id (symbol),
    time, and last_size/volume.
    """
    try:
        if isinstance(msg, bytes):
            msg = msg.decode("utf-8")
        data = json.loads(msg)
    except Exception:
        # ignore non-json or malformed messages
        log("failed to decode message:", msg)
        return

    typ = data.get("type")
    if typ != "ticker":
        # ignore other messages for now (subscriptions, heartbeat, etc.)
        return

    # Example ticker fields:
    # { "type":"ticker", "sequence": 12345, "time":"2020-08-21T18:46:09.874Z",
    #   "product_id":"BTC-USD", "price":"11679.01", "open_24h":"11500.00",
    #   "volume_24h":"1234.5", "low_24h":"11000.0", "high_24h":"12000.0",
    #   "side":"buy", "last_size":"0.001" }
    try:
        symbol = data.get("product_id")
        price = float(data.get("price", 0.0))
        # Prefer the explicit time field if present; else use now()
        ts_str = data.get("time")
        if ts_str:
            # coinbase times are ISO 8601 Z-suffixed -> parse to UTC
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        else:
            ts = datetime.now(timezone.utc)
        # volume field may be 'last_size' for the trade; fallback to size or 0
        vol = data.get("last_size") or data.get("size") or 0.0
        try:
            vol = float(vol)
        except Exception:
            vol = 0.0

        # write to DB using helper if available; else direct insert
        if hasattr(dbmod, "insert_ticker"):
            try:
                # insert_ticker should accept: symbol, price, volume, ts (datetime)
                dbmod.insert_ticker(symbol=symbol, price=float(price), volume=float(vol), ts=ts)
                log("inserted via insert_ticker:", symbol, price, vol, ts)
                return
            except Exception as e:
                log("insert_ticker failed, falling back to direct insert:", e)

        # fallback direct insert
        engine = dbmod.engine
        # adapt to table column names: tickers has columns (id, symbol, price, volume, ts)
        stmt = insert(dbmod.tickers).values(symbol=symbol, price=float(price), volume=float(vol), ts=ts)
        with engine.begin() as c:
            c.execute(stmt)
        log("inserted direct:", symbol, price, vol, ts)
    except Exception:
        print("Error handling message:", traceback.format_exc())

def on_open(ws):
    log("WebSocket opened. Subscribing to ticker channel for:", SYMBOLS)
    sub = {
        "type": "subscribe",
        "channels": [
            {
                "name": CHANNEL,
                "product_ids": SYMBOLS
            }
        ]
    }
    ws.send(json.dumps(sub))

def on_close(ws, close_status_code, close_msg):
    print(f"[coinbase_ws] WebSocket closed: code={close_status_code} msg={close_msg}")

def on_error(ws, error):
    print(f"[coinbase_ws] WebSocket error: {error}")

def on_message(ws, message):
    # route to handler in separate thread to avoid slowing WS event loop
    threading.Thread(target=handle_message, args=(message,), daemon=True).start()

def run_forever():
    backoff = RECONNECT_BASE
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            print(f"[coinbase_ws] Connecting to {WS_URL} (symbols={SYMBOLS})")
            # blocking run; when it returns, attempt reconnect
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except KeyboardInterrupt:
            print("[coinbase_ws] KeyboardInterrupt received, exiting.")
            return
        except Exception as e:
            print("[coinbase_ws] Exception in run_forever:", e)
            traceback.print_exc()

        # backoff sleep
        print(f"[coinbase_ws] Disconnected — reconnecting in {backoff:.1f}s ...")
        time.sleep(backoff)
        backoff = min(backoff * 2, RECONNECT_MAX)

if __name__ == "__main__":
    print("Starting coinbase websocket ingestion. CTRL+C to stop.")
    run_forever()
