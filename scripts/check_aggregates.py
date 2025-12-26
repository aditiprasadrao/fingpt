import sys
from datetime import datetime
from src.storage import db as dbmod

def q_count(conn, sql, params=None):
    try:
        r = conn.execute(sql) if params is None else conn.execute(sql, params)
        return r.fetchone()[0]
    except Exception as e:
        return f"ERR: {e}"

def q_rows(conn, sql, params=None):
    try:
        r = conn.execute(sql) if params is None else conn.execute(sql, params)
        return [tuple(row) for row in r.fetchall()]
    except Exception as e:
        return f"ERR: {e}"

def main():
    # args: SYMBOL, N
    sym = sys.argv[1] if len(sys.argv) > 1 else "BTC-USD"
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    print("=== quick DB sanity check ===")
    with dbmod.engine.connect() as c:
        print("\\n--- Aggregates ---")
        cnt = q_count(c, "SELECT COUNT(*) FROM aggregates WHERE symbol = ?", [sym])
        print("Aggregates count for", sym, ":", cnt)
        rows = q_rows(c, "SELECT ts, open_price, high_price, low_price, close_price, avg_sentiment, post_count FROM aggregates WHERE symbol = ? ORDER BY ts DESC LIMIT ?", [sym, n])
        print("Latest aggregates:")
        if isinstance(rows, str):
            print(rows)
        else:
            for r in rows:
                print(r)
        print("\\n--- Tickers (latest) ---")
        rows2 = q_rows(c, "SELECT ts, symbol, price FROM tickers WHERE symbol = ? ORDER BY ts DESC LIMIT ?", [sym, n])
        if isinstance(rows2, str):
            print(rows2)
        else:
            for r in rows2:
                print(r)
        print("\\n--- Reddit posts (latest) ---")
        rows3 = q_rows(c, "SELECT created_utc, subreddit, text, sentiment FROM reddit_posts ORDER BY created_utc DESC LIMIT ?", [n])
        if isinstance(rows3, str):
            print(rows3)
        else:
            for r in rows3:
                print(r)
    print("\\nDone.")

if __name__ == '__main__':
    main()
