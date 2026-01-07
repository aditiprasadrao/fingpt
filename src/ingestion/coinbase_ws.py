import json
import time
import threading
import os
from datetime import datetime, timezone
import traceback
import websocket
from sqlalchemy import insert
from src.storage import db as dbmod

WS_URL = "wss://ws-feed.exchange.coinbase.com"
SYMBOLS = ["BTC-USD", "ETH-USD", "USDT-USD"]
CHANNEL = "ticker"

def handle_message(msg):
    try:
        data = json.loads(msg)
        if data.get("type") != "ticker":
            return

        symbol = data["product_id"]
        price = float(data["price"])
        vol = float(data.get("last_size", 0.0))
        ts = datetime.fromisoformat(data["time"].replace("Z", "+00:00"))

        stmt = insert(dbmod.tickers).values(
            symbol=symbol,
            price=price,
            volume=vol,
            ts=ts
        )

        with dbmod.engine.begin() as conn:
            conn.execute(stmt)

    except Exception:
        print("handle_message error:", traceback.format_exc())

def start_ws(symbol):
    def on_open(ws):
        print(f"[coinbase_ws] Connected: {symbol}")
        ws.send(json.dumps({
            "type": "subscribe",
            "channels": [{
                "name": CHANNEL,
                "product_ids": [symbol]
            }]
        }))

    def on_message(ws, message):
        handle_message(message)

    def on_error(ws, error):
        print(f"[coinbase_ws] {symbol} error:", error)

    def on_close(ws, code, msg):
        print(f"[coinbase_ws] {symbol} closed — reconnecting")
        time.sleep(2)
        start_ws(symbol)

    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever(ping_interval=20, ping_timeout=10)

if __name__ == "__main__":
    print("Starting Coinbase ingestion (multi-connection mode)")
    for sym in SYMBOLS:
        threading.Thread(target=start_ws, args=(sym,), daemon=True).start()

    while True:
        time.sleep(60)
