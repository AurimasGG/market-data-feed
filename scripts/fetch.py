import json, yaml
from datetime import datetime, timezone
import pandas as pd

def load_universe(path="symbols.yaml"):
    with open(path, "r") as f:
        y = yaml.safe_load(f)
    return y["universe"]

def fetch_yfinance_one(ticker: str):
    import yfinance as yf
    t = yf.Ticker(ticker)
    try:
        df = t.history(period="1d", interval="1m")
    except Exception:
        df = t.history(period="5d", interval="5m")
    if df is None or df.empty:
        return None
    row = df.tail(1).iloc[0]
    return {
        "price": float(row["Close"]),
        "ts": df.tail(1).index[0].tz_convert("UTC").isoformat()
    }

def main():
    uni = load_universe()
    quotes = []
    for item in uni:
        res = fetch_yfinance_one(item["query"])
        if res:
            quotes.append({
                "id": item["id"],
                "asset_class": item["asset_class"],
                "source": item["source"],
                "query": item["query"],
                **res
            })
    out = {"generated_at": datetime.now(timezone.utc).isoformat(), "quotes": quotes}
    with open("data/quotes.json", "w") as f:
        json.dump(out, f, indent=2)

if __name__ == "__main__":
    main()
